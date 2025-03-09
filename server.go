// server.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/handlers"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/alphauslabs/pubsub/utils"
	"github.com/golang/glog"
	"github.com/google/uuid"

	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	*app.PubSub
	pb.UnimplementedPubSubServiceServer
}

const (
	MessagesTable = "Messages"
	TopicsTable   = "Topics"
	SubsTable     = "Subscriptions"
	notifleader   = 1
)

// Publish a message to a topic
func (s *server) Publish(ctx context.Context, in *pb.PublishRequest) (*pb.PublishResponse, error) {
	if in.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic must not be empty")
	}
	msgId := uuid.New().String()
	mutation := spanner.InsertOrUpdate(
		MessagesTable,
		[]string{"id", "name", "payload", "createdAt", "updatedAt", "visibilityTimeout", "processed"},
		[]interface{}{
			msgId,
			in.Topic,
			in.Payload,
			spanner.CommitTimestamp,
			spanner.CommitTimestamp,
			nil,   // Explicitly set visibilityTimeout as NULL
			false, // Default to unprocessed
		},
	)

	_, err := s.Client.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		glog.Infof("Error writing to Spanner: %v", err)
		return nil, err
	}

	m := storage.Message{
		Message: &pb.Message{
			Id:      msgId,
			Topic:   in.Topic,
			Payload: in.Payload,
		},
	}
	b, _ := json.Marshal(&m)

	// broadcast message
	bcastin := handlers.BroadCastInput{
		Type: handlers.Message,
		Msg:  b,
	}
	bin, _ := json.Marshal(bcastin)
	out := s.Op.Broadcast(ctx, bin)
	for _, v := range out {
		if v.Error != nil { // for us to know, then do necessary actions if frequent
			glog.Infof("[Publish] Error broadcasting message: %v", v.Error)
		}
	}
	glog.Infof("[Publish] Message successfully broadcasted and wrote to spanner with ID: %s", msgId)
	return &pb.PublishResponse{MessageId: msgId}, nil
}

// Subscribe to receive messages for a subscription
func (s *server) Subscribe(in *pb.SubscribeRequest, stream pb.PubSubService_SubscribeServer) error {
	glog.Infof("[Subscribe] New subscription request received - Topic: %s, Subscription: %s", in.Topic, in.Subscription)

	// Validate subscription exists for the topic
	err := utils.CheckIfTopicSubscriptionIsCorrect(in.Topic, in.Subscription)
	if err != nil {
		glog.Infof("[Subscribe] Error validating subscription: %v", err)
		return err
	}

	glog.Infof("[Subscribe] Starting subscription stream for ID: %s", in.Subscription)

	// track last message count to avoid duplicate logs
	lastMessageCount := 0

	// Continuous loop to stream messages
	for {
		select {
		// Check if client has disconnected
		case <-stream.Context().Done():
			glog.Infof("[Subscribe] Client disconnected, closing stream for subscription %s", in.Subscription)
			return nil
		default:
			// will get messages for the topic
			messages, err := storage.GetMessagesByTopic(in.Topic)
			if err != nil {
				glog.Infof("[Subscribe] Error getting messages: %v", err)
				time.Sleep(time.Second) // Back off on error
				continue
			}

			// If no messages, wait before checking again
			if len(messages) == 0 {
				glog.Infof("[Subscribe] No messages found for topic %s, waiting...", in.Topic)
				time.Sleep(time.Second)
				continue
			}

			// Only log if the number of messages has changed
			if len(messages) != lastMessageCount {
				glog.Infof("[Subscribe] Found %d messages for topic %s", len(messages), in.Topic)
				lastMessageCount = len(messages)
			}

			// Process each message
			for _, message := range messages {
				fmt.Printf("message: %v\n", *message)
				fmt.Printf("message.Subscriptions[in.Subscription]: %v\n", *message.Subscriptions[in.Subscription])
				if atomic.LoadInt32(&message.FinalDeleted) == 1 {
					continue // Message has been deleted
				}

				if message.Subscriptions[in.Subscription].IsDeleted() {
					continue // Message has been deleted for this subscription
				}

				if message.Subscriptions[in.Subscription].IsLocked() {
					continue // Message is already locked by another subscriber
				}

				// if !message.HasBeenProcessedBySubscription(in.Subscription) {
				// 	// First time for this subscription - send immediately
				// 	if err := stream.Send(message.Message); err != nil {
				// 		glog.Errorf("[Subscribe] Failed to send message %s to subscription %s: %v",
				// 			message.Id, in.Subscription, err)
				// 		continue
				// 	}

				// 	// Mark subscription as having received the message
				// 	message.MarkAsProcessedBySubscription(in.Subscription)
				// 	glog.Infof("[Subscribe] First delivery: sent message %s to subscription %s",
				// 		message.Id, in.Subscription)
				// 	continue
				// }

				// // If we get here, subscription has already received message
				// // Now check deleted and locked for load balancing
				// if atomic.LoadInt32(&message.Deleted) == 1 ||
				// 	atomic.LoadInt32(&message.Locked) == 1 {
				// 	continue
				// }

				// Lock it for this client
				// atomic.StoreInt32(&message.Locked, 1)

				// Broadcast lock status to other nodes
				broadcastData := handlers.BroadCastInput{
					Type: handlers.MsgEvent,
					Msg:  []byte(fmt.Sprintf("lock:%s:%s", message.Id, in.Subscription)),
				}
				bin, _ := json.Marshal(broadcastData)
				out := s.Op.Broadcast(stream.Context(), bin)
				for _, o := range out {
					if o.Error != nil {
						glog.Errorf("[Subscribe] Error broadcasting lock: %v", o.Error)
						return nil
					}
				}

				if err := stream.Send(message.Message); err != nil {
					glog.Errorf("[Subscribe] Failed to send message %s to subscription %s: %v", message.Id, in.Subscription, err)
					// Broadcast unlock on error
					broadcastData := handlers.BroadCastInput{
						Type: handlers.MsgEvent,
						Msg:  []byte(fmt.Sprintf("unlock:%s:%s", message.Id, in.Subscription)),
					}
					bin, _ := json.Marshal(broadcastData)
					out := s.Op.Broadcast(stream.Context(), bin)
					for _, o := range out {
						if o.Error != nil {
							glog.Errorf("[Subscribe] Error broadcasting unlock: %v", o.Error)
							return nil
						}
					}
				} else {
					glog.Infof("[Subscribe] sent message %s to subscription %s", message.Id, in.Subscription)
				}
			}
		}
	}
}

func (s *server) Acknowledge(ctx context.Context, in *pb.AcknowledgeRequest) (*pb.AcknowledgeResponse, error) {
	// Check if message exists in storage
	glog.Infof("[Acknowledge] Retrieving message %s from storage", in.Id)
	_, err := storage.GetMessage(in.Id)
	if err != nil {
		return nil, status.Error(codes.NotFound, "[Acknowledge] Message may have been removed after acknowledgment and cannot be found in storage. ")
	}

	// Update the processed status in Spanner
	if err := utils.UpdateMessageProcessedStatus(s.Client, in.Id); err != nil {
		return nil, status.Error(codes.Internal, "failed to update processed status in Spanner")
	}

	broadcastData := handlers.BroadCastInput{
		Type: handlers.MsgEvent,
		Msg:  []byte(fmt.Sprintf("delete:%s:%s", in.Id, in.Subscription)),
	}
	bin, _ := json.Marshal(broadcastData)
	out := s.Op.Broadcast(ctx, bin) // broadcast to set deleted
	for _, v := range out {
		if v.Error != nil {
			glog.Infof("[Acknowledge] Error broadcasting acknowledgment: %v", v.Error)
		}
	}

	glog.Infof("[Acknowledge] Successfully processed acknowledgment for message %s", in.Id)
	return &pb.AcknowledgeResponse{Success: true}, nil
}

// ModifyVisibilityTimeout resets the age of the message to extend visibility timeout
func (s *server) ModifyVisibilityTimeout(ctx context.Context, in *pb.ModifyVisibilityTimeoutRequest) (*pb.ModifyVisibilityTimeoutResponse, error) {
	glog.Infof("[Extend Visibility] Request to modify visibility for message: %s, Subscription: %s, New Timeout: %d seconds", in.Id, in.SubscriptionId, in.NewTimeout)

	msg, err := storage.GetMessage(in.Id)
	if err != nil {
		glog.Errorf("[Extend Visibility] Error retrieving message %s: %v", in.Id, err)
		return nil, err
	}
	if msg == nil {
		glog.Errorf("[Extend Visibility] Message %s not found", in.Id)
		return nil, fmt.Errorf("message not found")
	}

	// lock the message and reset Age
	msg.Mu.Lock()
	msg.Subscriptions[in.SubscriptionId].RenewAge()
	msg.Mu.Unlock()

	glog.Infof("[Extend Visibility] Visibility Timeout for message %s has been extended.", in.Id)
	return &pb.ModifyVisibilityTimeoutResponse{Success: true}, nil
}

// <<<<<<<< CRUD - TOPICS >>>>>>>>>>>>>
func (s *server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic name is required")
	}

	m := spanner.Insert(
		TopicsTable,
		[]string{"name", "createdAt", "updatedAt"},
		[]interface{}{req.Name, spanner.CommitTimestamp, spanner.CommitTimestamp},
	)

	_, err := s.Client.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create topic: %v", err)
	}

	topic := &pb.Topic{
		Name:      req.Name,
		CreatedAt: spanner.CommitTimestamp.Format(time.RFC3339),
		UpdatedAt: spanner.CommitTimestamp.Format(time.RFC3339),
	}

	s.notifyLeader(notifleader)

	return topic, nil
}

func (s *server) GetTopic(ctx context.Context, req *pb.GetTopicRequest) (*pb.Topic, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "topic ID is required")
	}

	stmt := spanner.Statement{
		SQL:    `SELECT name, createdAt, updatedAt FROM Topics WHERE name = @name LIMIT 1`,
		Params: map[string]interface{}{"name": req.Name},
	}

	iter := s.Client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return nil, status.Errorf(codes.NotFound, "topic with ID %q not found", req.Name)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query topic: %v", err)
	}

	var (
		name                 string
		createdAt, updatedAt spanner.NullTime
	)
	if err := row.Columns(&name, &createdAt, &updatedAt); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse topic data: %v", err)
	}

	return &pb.Topic{
		Name:      name,
		CreatedAt: createdAt.String(),
		UpdatedAt: createdAt.String(),
	}, nil
}

func (s *server) UpdateTopic(ctx context.Context, req *pb.UpdateTopicRequest) (*pb.Topic, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Current topic name is required")
	}
	if req.NewName == "" {
		return nil, status.Error(codes.InvalidArgument, "New topic name is required")
	}

	var updatedTopic *pb.Topic

	_, err := s.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// 1. Get the existing topic with its createdAt timestamp
		stmtGet := spanner.Statement{
			SQL: `SELECT createdAt FROM Topics WHERE name = @name`,
			Params: map[string]interface{}{
				"name": req.Name,
			},
		}

		var createdAt spanner.NullTime
		iter := txn.Query(ctx, stmtGet)
		defer iter.Stop()

		row, err := iter.Next()
		if err == iterator.Done {
			return status.Errorf(codes.NotFound, "Topic %q not found", req.Name)
		}
		if err != nil {
			return status.Errorf(codes.Internal, "Failed to fetch topic: %v", err)
		}
		if err := row.Columns(&createdAt); err != nil {
			return status.Errorf(codes.Internal, "Failed to parse topic data: %v", err)
		}

		// 2. Delete the old topic
		stmtDelete := spanner.Statement{
			SQL: `DELETE FROM Topics WHERE name = @name`,
			Params: map[string]interface{}{
				"name": req.Name,
			},
		}
		rowCount, err := txn.Update(ctx, stmtDelete)
		if err != nil {
			return status.Errorf(codes.Internal, "Failed to delete old topic: %v", err)
		}
		if rowCount == 0 {
			return status.Errorf(codes.NotFound, "Topic %q not found", req.Name)
		}

		// 3. Insert the new topic with the original createdAt
		mutation := spanner.Insert(
			"Topics",
			[]string{"name", "createdAt", "updatedAt"},
			[]interface{}{req.NewName, createdAt.Time, spanner.CommitTimestamp},
		)
		if err := txn.BufferWrite([]*spanner.Mutation{mutation}); err != nil {
			return status.Errorf(codes.Internal, "Failed to create new topic: %v", err)
		}

		// 4. Update all subscriptions referencing the old topic name
		stmtSubs := spanner.Statement{
			SQL: `UPDATE Subscriptions
                  SET topic = @newName,
                      updatedAt = PENDING_COMMIT_TIMESTAMP()
                  WHERE topic = @oldName`,
			Params: map[string]interface{}{
				"oldName": req.Name,
				"newName": req.NewName,
			},
		}
		_, err = txn.Update(ctx, stmtSubs)
		if err != nil {
			return status.Errorf(codes.Internal, "Failed to update subscriptions: %v", err)
		}

		// 5. Update messages referencing the old topic name
		stmtMsgs := spanner.Statement{
			SQL: `UPDATE Messages
                  SET topic = @newName,
                      updatedAt = PENDING_COMMIT_TIMESTAMP()
                  WHERE topic = @oldName`,
			Params: map[string]interface{}{
				"oldName": req.Name,
				"newName": req.NewName,
			},
		}
		_, err = txn.Update(ctx, stmtMsgs)
		if err != nil {
			return status.Errorf(codes.Internal, "Failed to update messages: %v", err)
		}

		updatedTopic = &pb.Topic{
			Name:      req.NewName,
			CreatedAt: createdAt.Time.Format(time.RFC3339),          //not yet in proto return
			UpdatedAt: spanner.CommitTimestamp.Format(time.RFC3339), //not yet in proto return
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Notify the leader or cluster if needed
	s.notifyLeader(notifleader)

	return updatedTopic, nil
}

func (s *server) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}

	_, err := s.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// 1) Check if topic exists
		checkStmt := spanner.Statement{
			SQL:    `SELECT name FROM Topics WHERE name = @name`, // (--THEN RETURN name) {but need to modify proto to return also the name}
			Params: map[string]interface{}{"name": req.Name},
		}
		iter := txn.Query(ctx, checkStmt)
		defer iter.Stop()

		_, err := iter.Next()
		if err == iterator.Done {
			return status.Errorf(codes.NotFound, "Topic %q does not exist", req.Name)
		}
		if err != nil {
			return status.Errorf(codes.Internal, "Failed to check topic existence: %v", err)
		}

		// 2) Delete the topic row
		topicMutation := spanner.Delete(TopicsTable, spanner.Key{req.Name})
		if err := txn.BufferWrite([]*spanner.Mutation{topicMutation}); err != nil {
			return status.Errorf(codes.Internal, "failed to delete topic: %v", err)
		}

		// 3) Delete all related subscriptions referencing this topic
		delSubs := spanner.Statement{
			SQL: `DELETE FROM Subscriptions WHERE topic = @Topic`,
			Params: map[string]interface{}{
				"Topic": req.Name,
			},
		}
		if _, err := txn.Update(ctx, delSubs); err != nil {
			return status.Errorf(codes.Internal, "failed to delete subscriptions: %v", err)
		}

		return nil
	})

	if err != nil {
		glog.Infof("Failed to delete topic and subscriptions: %v", err)
		return nil, err
	}

	// Remove from in-memory storage (if you maintain a local cache)
	if err := storage.RemoveTopic(req.Name); err != nil {
		glog.Infof("Failed to remove topic from memory: %v", err)
		// continuing so we can still broadcast the deletion
	}

	// Note: Might not be needed since we tell leader that a topic is delete.
	glog.Infof("Broadcasting topic deletion for %s", req.Name)
	broadcastData := handlers.BroadCastInput{
		Type: "topicdeleted",
		Msg:  []byte(req.Name),
	}
	bin, _ := json.Marshal(broadcastData)
	out := s.Op.Broadcast(ctx, bin)
	for _, o := range out {
		if o.Error != nil {
			glog.Errorf("Broadcast topic deleted error: %v", err)
		}
	}

	s.notifyLeader(notifleader) // Send flag=1 to indicate an update

	return &pb.DeleteTopicResponse{Success: true}, nil
}

func (s *server) ListTopics(ctx context.Context, _ *pb.Empty) (*pb.ListTopicsResponse, error) {
	stmt := spanner.Statement{SQL: `SELECT name, createdAt, updatedAt FROM Topics`}
	iter := s.Client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var topics []*pb.Topic
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to list topics: %v", err)
		}

		var (
			name                 string
			createdAt, updatedAt spanner.NullTime
		)
		if err := row.Columns(&name, &createdAt, &updatedAt); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to parse topic data: %v", err)
		}

		topics = append(topics, &pb.Topic{
			Name:      name,
			CreatedAt: createdAt.String(),
			UpdatedAt: createdAt.String(),
		})
	}

	return &pb.ListTopicsResponse{Topics: topics}, nil
}

func (s *server) CreateSubscription(ctx context.Context, req *pb.CreateSubscriptionRequest) (*pb.Subscription, error) {
	if req.Topic == "" || req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic and Subscription name are required")
	}

	// Default autoextend to false if not provided
	autoExtend := false
	if req.Autoextend != nil {
		autoExtend = *req.Autoextend
	}

	m := spanner.Insert(
		SubsTable,
		[]string{"name", "topic", "createdAt", "updatedAt", "autoextend"},
		[]interface{}{req.Name, req.Topic, spanner.CommitTimestamp, spanner.CommitTimestamp, autoExtend},
	)

	_, err := s.Client.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create subscription: %v", err)
	}

	glog.Infof("[CreateSubscription] Subscription %s created with AutoExtend: %v", req.Name, autoExtend)

	// todo: tell leader
	return &pb.Subscription{
		Name:       req.Name,
		Topic:      req.Topic,
		Autoextend: autoExtend,
	}, nil
}

func (s *server) GetSubscription(ctx context.Context, req *pb.GetSubscriptionRequest) (*pb.Subscription, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Subscription name is required")
	}

	stmt := spanner.Statement{
		SQL: `SELECT name, topic, autoextend FROM Subscriptions WHERE name = @name`,
		Params: map[string]interface{}{
			"name": req.Name,
		},
	}

	iter := s.Client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return nil, status.Errorf(codes.NotFound, "Subscription not found: %s", req.Name)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Query error: %v", err)
	}

	var name, topic string
	var autoExtend bool

	if err := row.Columns(&name, &topic, &autoExtend); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to parse subscription data: %v", err)
	}

	return &pb.Subscription{
		Name:       name,
		Topic:      topic,
		Autoextend: autoExtend,
	}, nil
}

func (s *server) UpdateSubscription(ctx context.Context, req *pb.UpdateSubscriptionRequest) (*pb.Subscription, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Subscription name is required")
	}

	// First get the existing subscription to preserve the topic
	existingSub, err := s.GetSubscription(ctx, &pb.GetSubscriptionRequest{Name: req.Name})
	if err != nil {
		return nil, err
	}

	// Update the subscription
	m := spanner.Update(
		SubsTable,
		[]string{"name", "visibility_timeout", "autoextend", "updatedAt"},
		[]interface{}{
			req.Name,
			req.ModifyVisibilityTimeout,
			req.Autoextend,
			spanner.CommitTimestamp,
		},
	)

	_, err = s.Client.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update subscription: %v", err)
	}

	// todo: tell leader
	return &pb.Subscription{
		Name:              req.Name,
		Topic:             existingSub.Topic,
		VisibilityTimeout: req.ModifyVisibilityTimeout,
		Autoextend:        req.GetAutoextend(),
	}, nil
}

func (s *server) DeleteSubscription(ctx context.Context, req *pb.DeleteSubscriptionRequest) (*pb.DeleteSubscriptionResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Subscription name is required")
	}

	m := spanner.Delete(SubsTable, spanner.Key{req.Name})
	_, err := s.Client.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete subscription: %v", err)
	}

	// todo: tell leader
	return &pb.DeleteSubscriptionResponse{
		Success:   true,
		DeletedAt: time.Now().Format(time.RFC3339),
	}, nil
}

func (s *server) ListSubscriptions(ctx context.Context, _ *pb.Empty) (*pb.ListSubscriptionsResponse, error) {
	stmt := spanner.Statement{
		SQL: `SELECT name, topic, visibility_timeout, autoextend FROM Subscriptions`,
	}

	iter := s.Client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var subscriptions []*pb.Subscription
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to iterate subscriptions: %v", err)
		}

		var (
			name              string
			topic             string
			visibilityTimeout int32
			autoextend        bool
		)

		if err := row.Columns(&name, &topic, &visibilityTimeout, &autoextend); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to parse subscription data: %v", err)
		}

		subscriptions = append(subscriptions, &pb.Subscription{
			Name:              name,
			Topic:             topic,
			VisibilityTimeout: visibilityTimeout,
			Autoextend:        autoextend,
		})
	}

	return &pb.ListSubscriptionsResponse{
		Subscriptions: subscriptions,
	}, nil
}

func (s *server) notifyLeader(flag byte) {
	// Create a simple payload with just the flag
	data := map[string]interface{}{
		"flag": flag,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		glog.Infof("Error marshaling data: %v", err)
		return
	}

	// Create SendInput with topicsubupdates type
	input := handlers.SendInput{
		Type: "topicsubupdates",
		Msg:  jsonData,
	}

	// Serialize the SendInput
	inputData, err := json.Marshal(input)
	if err != nil {
		glog.Infof("Error marshaling send input: %v", err)
		return
	}

	// Send to leader with timeout
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	_, err = s.Op.Send(timeoutCtx, inputData)
	if err != nil {
		glog.Infof("Failed to send to leader: %v", err)
	} else {
		glog.Infof("Successfully notified leader with flag: %d", flag)
	}
}

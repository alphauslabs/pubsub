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

	// Generate unique client ID for this subscription stream
	clientID := uuid.New().String()
	glog.Infof("[Subscribe] Assigned client ID %s for subscription %s", clientID, in.Subscription)

	// Validate if subscription exists for the given topic
	glog.Infof("[Subscribe] Checking if subscription exists for topic: %s", in.Topic)
	err := s.checkIfTopicSubscriptionIsCorrect(in.Topic, in.Subscription)
	if err != nil {
		glog.Infof("[Subscribe] Error validating subscription: %v", err)
		return err
	}

	glog.Infof("[Subscribe] Starting subscription stream for ID: %s", in.Subscription)

	// Track last message count to avoid duplicate logs
	lastMessageCount := 0

	// Continuous loop to stream messages
	for {
		select {
		// Check if client has disconnected
		case <-stream.Context().Done():
			glog.Infof("[Subscribe] Client disconnected, closing stream for subscription %s", in.Subscription)
			return nil
		default:
			// Get messages from local storage for the topic
			messages, err := storage.GetMessagesByTopic(in.Topic)
			if err != nil {
				glog.Infof("[Subscribe] Error getting messages: %v", err)
				time.Sleep(time.Second) // Back off on error
				continue
			}

			// If no messages, wait before checking again
			if len(messages) == 0 {
				glog.Infof("[Subscribe] No messages found for topic %s, waiting...", in.Topic)
				time.Sleep(time.Second) // todo: not sure if this is the best way
				continue
			}

			// Only log if the number of messages has changed
			if len(messages) != lastMessageCount {
				glog.Infof("[Subscribe] Found %d messages for topic %s", len(messages), in.Topic)
				lastMessageCount = len(messages)
			}

			// Process each message
			for _, message := range messages {
				message.Mu.Lock()
				// Initialize maps if needed
				if message.ProcessedBy == nil {
					message.ProcessedBy = make(map[string]bool)
				}
				if message.SentToSubs == nil {
					message.SentToSubs = make(map[string]bool)
				}
				message.Mu.Unlock()

				// Skip if this client has already processed this message
				if message.ProcessedBy[clientID] {
					continue
				}

				// Now check if message is deleted
				if atomic.LoadInt32(&message.Deleted) == 1 {
					continue
				}

				// Try to acquire lock for this message at subscription level
				if !message.LockForSubscription(in.Subscription) {
					glog.V(2).Infof("[Subscribe] Message %s is locked by another client in subscription %s, skipping...",
						message.Id, in.Subscription)
					continue
				}

				// Broadcast lock request to all nodes with subscription info
				broadcastData := handlers.BroadCastInput{
					Type: handlers.MsgEvent,
					Msg:  []byte(fmt.Sprintf("lock:%s:%s:%s:%s", message.Id, in.Topic, in.Subscription, clientID)),
				}
				bin, _ := json.Marshal(broadcastData)
				s.Op.Broadcast(context.Background(), bin)

				// Send the message to the client
				if err := stream.Send(message.Message); err != nil {
					// Release lock and broadcast unlock if sending fails
					message.UnlockForSubscription(in.Subscription)
					unlockData := handlers.BroadCastInput{
						Type: handlers.MsgEvent,
						Msg:  []byte(fmt.Sprintf("unlock:%s:%s:%s", message.Id, in.Subscription, clientID)),
					}
					unlockBin, _ := json.Marshal(unlockData)
					s.Op.Broadcast(context.Background(), unlockBin)
					glog.Errorf("[Subscribe] Failed to send message %s to client %s in subscription %s: %v",
						message.Id, clientID, in.Subscription, err)
				} else {
					glog.Infof("[Subscribe] Successfully sent message %s to client %s in subscription %s",
						message.Id, clientID, in.Subscription)
					// Mark this client as having processed the message
					message.ProcessedBy[clientID] = true
					// Release lock and broadcast unlock after successful send
					message.UnlockForSubscription(in.Subscription)
					unlockData := handlers.BroadCastInput{
						Type: handlers.MsgEvent,
						Msg:  []byte(fmt.Sprintf("unlock:%s:%s:%s", message.Id, in.Subscription, clientID)),
					}
					unlockBin, _ := json.Marshal(unlockData)
					s.Op.Broadcast(context.Background(), unlockBin)
				}
			}
		}
	}
}

func (s *server) Acknowledge(ctx context.Context, in *pb.AcknowledgeRequest) (*pb.AcknowledgeResponse, error) {
	glog.Infof("[Acknowledge] Received acknowledgment for message ID: %s", in.Id)
	// Check if message lock exists and is still valid (within 1 minute)
	// lockInfo, ok := s.MessageLocks.Load(in.Id)
	// if !ok {
	// 	glog.Infof("[Acknowledge] Error: Message lock not found for ID: %s", in.Id)
	// 	return nil, status.Error(codes.NotFound, "message lock not found")
	// }

	// info := lockInfo.(handlers.MessageLockInfo)
	// glog.Infof("[Acknowledge] Found lock info for message %s - Locked: %v, Timeout: %v, NodeID: %s", in.Id, info.Locked, info.Timeout, info.NodeID)

	// // Check if lock is valid and not timed out
	// if !info.Locked || time.Now().After(info.Timeout) {
	// 	return nil, status.Error(codes.FailedPrecondition, "message lock expired")
	// }

	// Check if message exists in storage
	glog.Infof("[Acknowledge] Retrieving message %s from storage", in.Id)
	msg, err := storage.GetMessage(in.Id)
	if err != nil {
		// Skip message
		return nil, status.Error(codes.NotFound, "[Acknowledge] Message may have been removed after acknowledgment and cannot be found in storage. ")
	}

	// Update the processed status in Spanner
	if err := utils.UpdateMessageProcessedStatus(s.Client, in.Id); err != nil {
		return nil, status.Error(codes.Internal, "failed to update processed status in Spanner")
	}

	// Log acknowledgment
	glog.Infof("Message acknowledged: %s, ID: %s", msg.Payload, in.Id)
	broadcastData := handlers.BroadCastInput{
		Type: handlers.MsgEvent,
		Msg:  []byte(fmt.Sprintf("delete:%s", in.Id)),
	}
	bin, _ := json.Marshal(broadcastData)
	s.Op.Broadcast(ctx, bin) // broadcast to set deleted

	// Clean up message (processed)
	// glog.Infof("[Acknowledge] Cleaning up message %s from local state", in.Id)
	// s.MessageLocks.Delete(in.Id)
	// if timer, ok := s.MessageTimer.Load(in.Id); ok {
	// 	glog.Infof("[Acknowledge] Stopping timer for message %s", in.Id)
	// 	timer.(*time.Timer).Stop()
	// 	s.MessageTimer.Delete(in.Id)
	// }

	// // Remove the message from in-memory storage
	// storage.RemoveMessage(in.Id, "") // RemoveMessage method from Storage

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
	msg.Age = time.Now().UTC()
	msg.Mu.Unlock()

	glog.Infof("[Extend Visibility] Visibility Timeout for message %s has been extended.", in.Id)
	return &pb.ModifyVisibilityTimeoutResponse{Success: true}, nil
}

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
		Name: req.Name,
	}

	// -----for testing only-----------//
	go func() {
		s.notifyLeader(notifleader) // Send flag=1 to indicate an update
	}()

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
		return nil, status.Error(codes.InvalidArgument, "Topic name is required")
	}
	if req.NewName == "" {
		return nil, status.Error(codes.InvalidArgument, "new topic name is required")
	}

	// todo: delete the old then insert new
	mutTopic := spanner.Update(TopicsTable,
		[]string{"name", "updatedAt"},
		[]interface{}{req.NewName, spanner.CommitTimestamp},
	)
	_, err := s.Client.Apply(ctx, []*spanner.Mutation{mutTopic})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update topic: %v", err)
	}

	// Update all subscriptions referencing the old topic name
	stmt := spanner.Statement{
		SQL: `UPDATE Subscriptions
			  SET topic = @newName,
			  updatedAt = PENDING_COMMIT_TIMESTAMP()
			  WHERE topic = @oldName`,
		Params: map[string]interface{}{
			"newName": req.NewName,
			"oldName": req.Name,
		},
	}
	_, err = s.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, stmt)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update subscriptions: %v", err)
	}

	updatedTopic := &pb.Topic{
		Name: req.NewName,
	}
	go func() {
		s.notifyLeader(notifleader) // Send flag=1 to indicate an update
	}()

	return updatedTopic, nil
}

func (s *server) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}

	_, err := s.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Delete topic
		topicMutation := spanner.Delete(TopicsTable, spanner.Key{req.Name})

		// Delete all related subscriptions
		stmt := spanner.Statement{
			SQL: `DELETE FROM Subscriptions WHERE topic = @Topic`,
			Params: map[string]interface{}{
				"Topic": req.Name,
			},
		}

		if err := txn.BufferWrite([]*spanner.Mutation{topicMutation}); err != nil {
			return status.Errorf(codes.Internal, "failed to delete topic: %v", err)
		}

		iter := txn.Query(ctx, stmt)
		defer iter.Stop()

		_, err := iter.Next()
		if err != nil && err != iterator.Done {
			return status.Errorf(codes.Internal, "failed to delete subscriptions: %v", err)
		}

		return nil
	})

	if err != nil {
		glog.Infof("Failed to delete topic and subscriptions: %v", err)
		return nil, err
	}

	// Remove from in-memory storage
	if err := storage.RemoveTopic(req.Name); err != nil {
		glog.Infof("Failed to remove topic from memory: %v", err) // Continue to broadcast the deletion
	}

	glog.Infof("Broadcasting topic deletion for %s", req.Name)
	broadcastData := handlers.BroadCastInput{
		Type: "topicdeleted",
		Msg:  []byte(req.Name),
	}
	bin, _ := json.Marshal(broadcastData)
	s.Op.Broadcast(ctx, bin)

	go func() {
		s.notifyLeader(notifleader) // Send flag=1 to indicate an update
	}()

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

func (s *server) notifyLeader(flag int) {
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

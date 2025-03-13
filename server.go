// server.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
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
	"google.golang.org/protobuf/types/known/emptypb"
)

type server struct {
	*app.PubSub
	pb.UnimplementedPubSubServiceServer
}

const (
	MessagesTable = "Messages"
	TopicsTable   = "Topics"
	SubsTable     = "Subscriptions"
)

// Publish a message to a topic
func (s *server) Publish(ctx context.Context, in *pb.PublishRequest) (*pb.PublishResponse, error) {
	if in.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic must not be empty")
	}

	attr := ""
	if in.Attributes != nil {
		b, _ := json.Marshal(in.Attributes)
		attr = string(b)
	}

	msgId := uuid.New().String()
	mutation := spanner.InsertOrUpdate(
		MessagesTable,
		[]string{"id", "topic", "payload", "attributes", "createdAt", "updatedAt", "processed"},
		[]any{
			msgId,
			in.Topic,
			in.Payload,
			attr,
			spanner.CommitTimestamp,
			spanner.CommitTimestamp,
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

	return &pb.PublishResponse{MessageId: msgId}, nil
}

// Subscribe to receive messages for a subscription
func (s *server) Subscribe(in *pb.SubscribeRequest, stream pb.PubSubService_SubscribeServer) error {
	glog.Infof("[Subscribe] New subscription request received for Topic=%s, Subscription=%s", in.Topic, in.Subscription)

	err := utils.CheckIfTopicSubscriptionIsCorrect(in.Topic, in.Subscription)
	if err != nil {
		glog.Infof("[Subscribe] Error validating subscription: %v", err)
		return err
	}

	glog.Infof("[Subscribe] Starting subscription stream for ID: %s", in.Subscription)
	for {
		select {
		// Check if client has disconnected
		case <-stream.Context().Done():
			glog.Infof("[Subscribe] Client disconnected, closing stream for subscription %s", in.Subscription)
			return nil
		default:
			msg, err := storage.GetMessagesByTopicSub(in.Topic, in.Subscription)
			if err != nil {
				glog.Info(err.Error())
				time.Sleep(time.Second) // Back off on error
				continue
			}

			// Broadcast lock status to other nodes
			broadcastData := handlers.BroadCastInput{
				Type: handlers.MsgEvent,
				Msg:  []byte(fmt.Sprintf("lock:%s:%s", msg.Id, in.Subscription)),
			}
			bin, _ := json.Marshal(broadcastData)
			out := s.Op.Broadcast(stream.Context(), bin)
			for _, o := range out {
				if o.Error != nil {
					glog.Errorf("[Subscribe] Error broadcasting lock: %v", o.Error)
					return nil
				}
			}

			if err := stream.Send(msg.Message); err != nil {
				glog.Errorf("[Subscribe] Failed to send message %s to subscription %s: %v", msg.Id, in.Subscription, err)
				// Broadcast unlock on error
				broadcastData := handlers.BroadCastInput{
					Type: handlers.MsgEvent,
					Msg:  []byte(fmt.Sprintf("unlock:%s:%s", msg.Id, in.Subscription)),
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
				// Wait for acknowledgement
				ch := make(chan struct{})
				go func() {
					defer close(ch) // Just close the channel

					ticker := time.NewTicker(100 * time.Millisecond)
					defer ticker.Stop()

					for {
						select {
						case <-ticker.C:
							m, err := storage.GetMessage(msg.Id)
							if err != nil {
								glog.Infof("[Subscribe] Error retrieving message %s: %v", msg.Id, err)
								return
							}
							switch {
							case m.IsFinalDeleted():
								glog.Infof("[Subscribe] Message %s has been deleted", m.Id)
								return
							case m.Subscriptions[in.Subscription].IsDeleted():
								glog.Infof("[Subscribe] Message %s has been deleted for subscription %s", m.Id, in.Subscription)
								return
							case !m.Subscriptions[in.Subscription].IsLocked():
								glog.Infof("[Subscribe] Message %s has been unlocked for subscription %s", m.Id, in.Subscription)
								return
							}
						case <-stream.Context().Done():
							glog.Infof("[Subscribe] Client context done while monitoring message %s", msg.Id)
							return
						}
					}
				}()
				<-ch
			}
		}
	}
}

func (s *server) Acknowledge(ctx context.Context, in *pb.AcknowledgeRequest) (*emptypb.Empty, error) {
	// Check if message exists in storage
	_, err := storage.GetMessage(in.Id)
	if err != nil {
		return nil, status.Error(codes.NotFound, "[Acknowledge] Message may have been removed after acknowledgment and cannot be found in storage. ")
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

	glog.Infof("[Acknowledge] Successfully processed acknowledgment for message=%v, sub=%v", in.Id, in.Subscription)
	return &emptypb.Empty{}, nil
}

func (s *server) ExtendVisibilityTimeout(ctx context.Context, in *pb.ExtendVisibilityTimeoutRequest) (*emptypb.Empty, error) {
	glog.Infof("[Extend Visibility] Request to modify visibility for message: %s, Subscription: %s", in.Id, in.Subscription)

	msg, err := storage.GetMessage(in.Id)
	if err != nil {
		glog.Errorf("[Extend Visibility] Error retrieving message %s: %v", in.Id, err)
		return nil, err
	}
	if msg == nil {
		glog.Errorf("[Extend Visibility] Message %s not found", in.Id)
		return nil, fmt.Errorf("message not found")
	}

	// Reset age
	msg.Mu.Lock()
	msg.Subscriptions[in.Subscription].RenewAge()
	msg.Mu.Unlock()

	glog.Infof("[Extend Visibility] Visibility Timeout for message %s has been extended.", in.Id)
	return &emptypb.Empty{}, nil
}

func (s *server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*emptypb.Empty, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic name is required")
	}

	m := spanner.Insert(
		TopicsTable,
		[]string{"name", "createdAt", "updatedAt"},
		[]any{req.Name, spanner.CommitTimestamp, spanner.CommitTimestamp},
	)

	_, err := s.Client.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create topic: %v", err)
	}

	s.notifyLeader(ctx)

	return &emptypb.Empty{}, nil
}

func (s *server) GetTopic(ctx context.Context, req *pb.GetTopicRequest) (*pb.GetTopicResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "topic ID is required")
	}

	stmt := spanner.Statement{
		SQL:    `SELECT name, createdAt, updatedAt FROM Topics WHERE name = @name LIMIT 1`,
		Params: map[string]any{"name": req.Name},
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

	return &pb.GetTopicResponse{
		Topic: &pb.Topic{
			Name:      name,
			CreatedAt: createdAt.String(),
			UpdatedAt: updatedAt.String(),
		},
	}, nil
}

func (s *server) UpdateTopic(ctx context.Context, req *pb.UpdateTopicRequest) (*pb.UpdateTopicResponse, error) {
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
			Params: map[string]any{
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
			Params: map[string]any{
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
			[]any{req.NewName, createdAt.Time, spanner.CommitTimestamp},
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
			Params: map[string]any{
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
			Params: map[string]any{
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
	s.notifyLeader(ctx)

	return &pb.UpdateTopicResponse{
		Topic: updatedTopic,
	}, nil
}

func (s *server) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*emptypb.Empty, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "topic is required")
	}

	_, err := s.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Check if topic exists
		checkStmt := spanner.Statement{
			SQL:    `SELECT name FROM Topics WHERE name = @name`, // (--THEN RETURN name) {but need to modify proto to return also the name}
			Params: map[string]any{"name": req.Name},
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

		// Delete the topic row
		topicMutation := spanner.Delete(TopicsTable, spanner.Key{req.Name})
		if err := txn.BufferWrite([]*spanner.Mutation{topicMutation}); err != nil {
			return status.Errorf(codes.Internal, "failed to delete topic: %v", err)
		}

		// Delete all related subscriptions referencing this topic
		delSubs := spanner.Statement{
			SQL: `DELETE FROM Subscriptions WHERE topic = @Topic`,
			Params: map[string]any{
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

	if err := storage.RemoveTopic(req.Name); err != nil {
		glog.Infof("Failed to remove topic from memory: %v", err)
		// continuing so we can still broadcast the deletion
	}

	// todo: Might not be needed since we tell leader that a topic is delete.
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

	s.notifyLeader(ctx)

	return &emptypb.Empty{}, nil
}

func (s *server) ListTopics(ctx context.Context, in *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
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

func (s *server) CreateSubscription(ctx context.Context, req *pb.CreateSubscriptionRequest) (*emptypb.Empty, error) {
	if req.Topic == "" || req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic and Subscription name are required")
	}

	autoExtend := true
	if req.NoAutoExtend {
		autoExtend = false
	}

	m := spanner.Insert(
		SubsTable,
		[]string{"name", "topic", "createdAt", "updatedAt", "autoextend"},
		[]any{req.Name, req.Topic, spanner.CommitTimestamp, spanner.CommitTimestamp, autoExtend},
	)

	_, err := s.Client.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create subscription: %v", err)
	}

	glog.Infof("[CreateSubscription] Subscription %s created with AutoExtend: %v", req.Name, autoExtend)

	s.notifyLeader(ctx)
	return &emptypb.Empty{}, nil
}

func (s *server) GetSubscription(ctx context.Context, req *pb.GetSubscriptionRequest) (*pb.GetSubscriptionResponse, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Subscription name is required")
	}

	stmt := spanner.Statement{
		SQL: `SELECT name, topic, autoextend FROM Subscriptions WHERE name = @name`,
		Params: map[string]any{
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

	return &pb.GetSubscriptionResponse{
		Subscription: &pb.Subscription{
			Name:       name,
			Topic:      topic,
			AutoExtend: autoExtend,
		},
	}, nil
}

func (s *server) UpdateSubscription(ctx context.Context, req *pb.UpdateSubscriptionRequest) (*pb.UpdateSubscriptionResponse, error) {
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
		[]any{
			req.Name,
			req.NoAutoExtend,
			spanner.CommitTimestamp,
		},
	)

	_, err = s.Client.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update subscription: %v", err)
	}

	s.notifyLeader(ctx)
	return &pb.UpdateSubscriptionResponse{
		Subscription: &pb.Subscription{
			Name:       req.Name,
			Topic:      existingSub.Subscription.Topic,
			AutoExtend: req.NoAutoExtend,
		},
	}, nil

}

func (s *server) DeleteSubscription(ctx context.Context, req *pb.DeleteSubscriptionRequest) (*emptypb.Empty, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Subscription name is required")
	}

	m := spanner.Delete(SubsTable, spanner.Key{req.Name})
	_, err := s.Client.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete subscription: %v", err)
	}

	s.notifyLeader(ctx)
	return &emptypb.Empty{}, nil
}

func (s *server) ListSubscriptions(ctx context.Context, in *pb.ListSubscriptionsRequest) (*pb.ListSubscriptionsResponse, error) {
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
			Name:       name,
			Topic:      topic,
			AutoExtend: autoextend,
		})
	}

	return &pb.ListSubscriptionsResponse{
		Subscriptions: subscriptions,
	}, nil
}

func (s *server) GetMessagesInQueue(ctx context.Context, in *pb.GetMessagesInQueueRequest) (*pb.GetMessagesInQueueResponse, error) {
	if in.Subscription == "triggercrash" {
		panic("trigger crash")
	}
	count, err := storage.GetSubscriptionQueueDepths()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to count messages: %v", err)
	}
	r := make([]*pb.InQueue, 0)
	for _, c := range count {
		r = append(r, &pb.InQueue{
			Subscription: c.Subscription,
			Total:        int32(c.Available),
		})
	}
	return &pb.GetMessagesInQueueResponse{
		InQueue: r,
	}, nil
}

func (s *server) notifyLeader(ctx context.Context) {
	input := handlers.SendInput{
		Type: "topicsubupdates",
		Msg:  []byte{},
	}

	inputData, err := json.Marshal(input)
	if err != nil {
		glog.Infof("Error marshaling send input: %v", err)
		return
	}

	_, err = s.Op.Send(ctx, inputData)
	if err != nil {
		glog.Infof("Failed to send to leader: %v", err)
	}
}

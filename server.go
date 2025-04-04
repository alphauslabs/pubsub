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

	subs, err := storage.GetSubscribtionsForTopic(in.Topic)
	if err != nil {
		glog.Errorf("Failed to get subscriptions for topic=%v", in.Topic)
		return nil, err
	}

	subStatus := make(map[string]bool)
	for _, s := range subs {
		subStatus[s.Name] = false
	}
	b, _ := json.Marshal(subStatus)

	msgId := uuid.New().String()
	mutation := spanner.InsertOrUpdate(
		MessagesTable,
		[]string{"id", "topic", "payload", "attributes", "subStatus", "createdAt", "updatedAt", "processed"},
		[]any{
			msgId,
			in.Topic,
			in.Payload,
			attr,
			string(b),
			spanner.CommitTimestamp,
			spanner.CommitTimestamp,
			false, // Default to unprocessed
		},
	)

	_, err = s.Client.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		glog.Errorf("Error writing to Spanner: %v", err)
		return nil, err
	}

	go func() {
		if in.Attributes != nil {
			if _, ok := in.Attributes["triggerpanic"]; ok {
				time.Sleep(500 * time.Millisecond)
				panic("simulated panic")
			}
		}
	}()

	return &pb.PublishResponse{MessageId: msgId}, nil
}

// Subscribe to receive messages for a subscription
func (s *server) Subscribe(in *pb.SubscribeRequest, stream pb.PubSubService_SubscribeServer) error {
	err := utils.CheckIfTopicSubscriptionIsCorrect(in.Topic, in.Subscription)
	if err != nil {
		glog.Errorf("[SubscribeHandler] Error validating subscription: %v", err)
		return err
	}

	glog.Infof("[SubscribeHandler] Starting subscription stream loop for topic=%v, sub=%v", in.Topic, in.Subscription)
	for {
		select {
		case <-stream.Context().Done():
			glog.Errorf("[SubscribeHandler] Client disconnected, closing stream for subscription %s", in.Subscription)
			return nil
		default:
			msg, err := storage.GetMessagesByTopicSub(in.Topic, in.Subscription)
			if err != nil {
				time.Sleep(2 * time.Second) // Back off on error
				continue
			}

			// Ask leader to lock this message for all nodes
			broadcastData := handlers.BroadCastInput{
				Type: handlers.MsgEvent,
				Msg:  []byte(fmt.Sprintf("lock:%s:%s:%s", msg.Id, in.Subscription, in.Topic)),
			}

			bin, _ := json.Marshal(broadcastData)
			outs := s.Op.Broadcast(stream.Context(), bin)
			for _, o := range outs {
				if o.Error != nil {
					glog.Errorf("[SubscribeHandler] Error broadcasting lock: %v", o.Error)
					return nil
				}
			}

			if err := stream.Send(msg.Message); err != nil {
				glog.Errorf("[SubscribeHandler] Failed to send message %s to subscription %s, err: %v", msg.Id, in.Subscription, err)
				// Broadcast unlock on error
				broadcastData := handlers.BroadCastInput{
					Type: handlers.MsgEvent,
					Msg:  []byte(fmt.Sprintf("unlock:%s:%s:%s", msg.Id, in.Subscription, in.Topic)),
				}
				bin, _ := json.Marshal(broadcastData)
				out := s.Op.Broadcast(stream.Context(), bin)
				for _, o := range out {
					if o.Error != nil {
						glog.Errorf("[SubscribeHandler] Error broadcasting unlock: %v", o.Error)
						return nil
					}
				}
			} else {
				// Wait for acknowledgement before doing another send.
				ch := make(chan struct{})
				go func() {
					defer close(ch)
					ticker := time.NewTicker(100 * time.Millisecond)
					defer ticker.Stop()

					for {
						select {
						case <-ticker.C:
							m, err := storage.GetMessage(msg.Id, in.Topic)
							if err != nil {
								glog.Errorf("[SubscribeHandler] Error retrieving message %s: %v", msg.Id, err)
								return
							}

							m.Mu.RLock()
							// Check for nil map or missing subscription
							if m.Subscriptions == nil {
								glog.Errorf("[SubscribeHandler] Message %s has nil Subscriptions map", m.Id)
								m.Mu.Unlock()
								return
							}

							subInfo, exists := m.Subscriptions[in.Subscription]
							if !exists {
								glog.Errorf("[SubscribeHandler] Subscription %s not found in message %s", in.Subscription, m.Id)
								m.Mu.RUnlock()
								return
							}
							m.Mu.RUnlock()

							switch {
							case m.IsFinalDeleted():
								glog.Errorf("[SubscribeHandler] Message %s has been final deleted", m.Id)
								return
							case subInfo.IsDeleted():
								glog.Errorf("[SubscribeHandler] Message %s has been deleted for subscription %s", m.Id, in.Subscription)
								return
							case !subInfo.IsLocked():
								glog.Errorf("[SubscribeHandler] Message %s has been unlocked for subscription %s", m.Id, in.Subscription)
								return
							}
						case <-stream.Context().Done():
							glog.Infof("[SubscribeHandler] Client context done while monitoring message %s", msg.Id)
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
	glog.Infof("[AcknowledgeHandler] Acknowledge request received for message:%v, sub:%v", in.Id, in.Subscription)
	broadcastData := handlers.BroadCastInput{
		Type: handlers.MsgEvent,
		Msg:  []byte(fmt.Sprintf("delete:%s:%s:%s", in.Id, in.Subscription, in.Topic)),
	}

	glog.Infof("[AcknowledgeHandler] Broadcasting acknowledgment for message:%v, sub:%v", in.Id, in.Subscription)
	bin, _ := json.Marshal(broadcastData)
	out := s.Op.Broadcast(ctx, bin) // broadcast to set deleted
	glog.Info("[AcknowledgeHandler] Broadcast response len=", len(out))
	for _, v := range out {
		if v.Error != nil {
			glog.Errorf("[AcknowledgeHandler] Error broadcasting acknowledgment for msg=%v, sub=%v, err=%v", in.Id, in.Subscription, v.Error)
			return nil, status.Errorf(codes.Internal, "failed to broadcast acknowledgment: %v", v.Error)
		}
	}

	glog.Infof("[AcknowledgeHandler] Successfully processed acknowledgment for message=%v, sub=%v", in.Id, in.Subscription)
	return &emptypb.Empty{}, nil
}

func (s *server) ExtendVisibilityTimeout(ctx context.Context, in *pb.ExtendVisibilityTimeoutRequest) (*emptypb.Empty, error) {
	broadcastData := handlers.BroadCastInput{
		Type: handlers.MsgEvent,
		Msg:  []byte(fmt.Sprintf("extend:%s:%s:%s", in.Id, in.Subscription, in.Topic)),
	}
	bin, _ := json.Marshal(broadcastData)
	out := s.Op.Broadcast(ctx, bin) // broadcast to set deleted
	for _, v := range out {
		if v.Error != nil {
			glog.Errorf("[Extend Visibility] Error in extending timeout for msg=%v, sub=%v, err=%v", in.Id, in.Subscription, v.Error)
		}
	}

	glog.Infof("[Extend Visibility] Visibility Timeout for msg=%s, sub=%v has been extended.", in.Id, in.Subscription)
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

		mutation := spanner.Insert(
			"Topics",
			[]string{"name", "createdAt", "updatedAt"},
			[]any{req.NewName, createdAt.Time, spanner.CommitTimestamp},
		)
		if err := txn.BufferWrite([]*spanner.Mutation{mutation}); err != nil {
			return status.Errorf(codes.Internal, "Failed to create new topic: %v", err)
		}

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
			CreatedAt: createdAt.Time.Format(time.RFC3339),
			UpdatedAt: spanner.CommitTimestamp.Format(time.RFC3339),
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	// Notify the leader
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
		glog.Errorf("Failed to delete topic and subscriptions: %v", err)
		return nil, err
	}

	if err := storage.RemoveTopic(req.Name); err != nil {
		glog.Errorf("Failed to remove topic from memory: %v", err)
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
		[]string{"name", "autoextend", "updatedAt"},
		[]any{
			req.Name,
			!req.NoAutoExtend,
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
			AutoExtend: !req.NoAutoExtend,
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
		glog.Errorf("Error marshaling send input: %v", err)
		return
	}

	_, err = s.Op.Send(ctx, inputData)
	if err != nil {
		glog.Errorf("Failed to send to leader: %v", err)
	}
}

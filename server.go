// server.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/broadcast"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	if in.TopicId == "" {
		return nil, status.Error(codes.InvalidArgument, "topic must not be empty")
	}

	b, _ := json.Marshal(in)

	l, _ := s.Op.HasLock()
	if l {
		log.Println("[Publish-leader] Received message:\n", string(b))
	} else {
		log.Printf("[Publish] Received message:\n%v", string(b))
	}

	messageID := uuid.New().String()
	mutation := spanner.InsertOrUpdate(
		MessagesTable,
		[]string{"id", "topic", "payload", "createdAt", "updatedAt", "visibilityTimeout", "processed"},
		[]interface{}{
			messageID,
			in.TopicId,
			in.Payload,
			spanner.CommitTimestamp,
			spanner.CommitTimestamp,
			nil,   // Explicitly set visibilityTimeout as NULL
			false, // Default to unprocessed
		},
	)

	_, err := s.Client.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		log.Printf("Error writing to Spanner: %v", err)
		return nil, err
	}

	// broadcast message
	bcastin := broadcast.BroadCastInput{
		Type: broadcast.Message,
		Msg:  b,
	}
	bin, _ := json.Marshal(bcastin)
	out := s.Op.Broadcast(ctx, bin)
	for _, v := range out {
		if v.Error != nil { // for us to know, then do necessary actions if frequent
			log.Printf("[Publish] Error broadcasting message: %v", v.Error)
		}
	}
	log.Printf("[Publish] Message successfully broadcasted and wrote to spanner with ID: %s", messageID)
	return &pb.PublishResponse{MessageId: messageID}, nil
}

// Subscribe to receive messages for a subscription
func (s *server) Subscribe(in *pb.SubscribeRequest, stream pb.PubSubService_SubscribeServer) error {
	// Validate if subscription exists for the given topic
	subs, err := s.Storage.GetSubscribtionsForTopic(in.TopicId)
	if err != nil {
		return status.Errorf(codes.NotFound, "Topic %s not found", in.TopicId)
	}

	// Check if the provided subscription ID exists in the topic's subscriptions
	found := false
	for _, sub := range subs {
		if sub == in.SubscriptionId {
			found = true
			break
		}
	}

	if !found {
		return status.Errorf(codes.NotFound, "Subscription %s not found", in.SubscriptionId)
	}

	log.Printf("[Subscribe] Starting subscription stream for ID: %s", in.SubscriptionId)

	// Continuous loop to stream messages
	for {
		select {
		// Check if client has disconnected
		case <-stream.Context().Done():
			return nil
		default:
			// Get messages from local storage for the topic
			messages, err := s.Storage.GetMessagesByTopic(in.TopicId)
			if err != nil {
				log.Printf("[Subscribe] Error getting messages: %v", err)
				time.Sleep(time.Second) // Back off on error
				continue
			}

			// If no messages, wait before checking again
			if len(messages) == 0 {
				time.Sleep(time.Second) // todo: not sure if this is the best way
				continue
			}

			// Process each message
			for _, message := range messages {
				// Skip if message is already locked by another subscriber
				if _, exists := s.MessageLocks.Load(message.Id); exists {
					continue
				}

				// Attempt to acquire distributed lock for the message
				// Default visibility timeout of 30 seconds
				if err := s.broadcastLock(stream.Context(), message.Id, in.SubscriptionId, 30*time.Second); err != nil {
					continue // Skip if unable to acquire lock
				}

				// Stream message to subscriber
				if err := stream.Send(message); err != nil {
					// Release lock if sending fails
					s.broadcastUnlock(stream.Context(), message.Id)
					return err // Return error to close stream
				}
			}
		}
	}
}

// Acknowledge a processed message
func (s *server) Acknowledge(ctx context.Context, in *pb.AcknowledgeRequest) (*pb.AcknowledgeResponse, error) {
	// Check if message lock exists and is still valid (within 1 minute)
	lockInfo, ok := s.MessageLocks.Load(in.Id)
	if !ok {
		return nil, status.Error(codes.NotFound, "message lock not found")
	}

	info := lockInfo.(broadcast.MessageLockInfo)
	// Check if lock is valid and not timed out
	if !info.Locked || time.Now().After(info.Timeout) {
		// Message already timed out - handled by handleMessageTimeout
		return nil, status.Error(codes.FailedPrecondition, "message lock expired")
	}

	// Get message processed in time
	msg, err := s.Storage.GetMessage(in.Id)
	if err != nil {
		return nil, status.Error(codes.NotFound, "message not found")
	}
	// Mark as processed since subscriber acknowledged in time
	msg.Processed = true
	if err := s.Storage.StoreMessage(msg); err != nil {
		return nil, status.Error(codes.Internal, "failed to update message")
	}

	// Broadcast successful processing
	broadcastData := broadcast.BroadCastInput{
		Type: broadcast.MsgEvent,
		Msg:  []byte(fmt.Sprintf("delete:%s", in.Id)),
	}
	bin, _ := json.Marshal(broadcastData)
	s.Op.Broadcast(ctx, bin)

	// Clean up message (processed)
	s.MessageLocks.Delete(in.Id)
	if timer, ok := s.MessageTimer.Load(in.Id); ok {
		timer.(*time.Timer).Stop()
		s.MessageTimer.Delete(in.Id)
	}

	return &pb.AcknowledgeResponse{Success: true}, nil
}

// ModifyVisibilityTimeout extends message lock timeout
func (s *server) ModifyVisibilityTimeout(ctx context.Context, in *pb.ModifyVisibilityTimeoutRequest) (*pb.ModifyVisibilityTimeoutResponse, error) {
	lockInfo, ok := s.MessageLocks.Load(in.Id)
	if !ok {
		return nil, status.Error(codes.NotFound, "message lock not found")
	}
	info := lockInfo.(broadcast.MessageLockInfo)
	if !info.Locked {
		return nil, status.Error(codes.FailedPrecondition, "message not locked")
	}

	// Check if this node owns the lock before extending
	if info.NodeID != s.Op.HostPort() {
		return nil, status.Error(codes.PermissionDenied, "only the lock owner can extend timeout")
	}

	// Broadcast new timeout
	broadcastData := broadcast.BroadCastInput{
		Type: broadcast.MsgEvent,
		Msg:  []byte(fmt.Sprintf("extend:%s:%d:%s", in.Id, in.NewTimeout, s.Op.HostPort())),
	}
	bin, _ := json.Marshal(broadcastData)
	s.Op.Broadcast(ctx, bin)

	// Update local timer
	if timer, ok := s.MessageTimer.Load(in.Id); ok {
		timer.(*time.Timer).Stop()
	}
	newTimer := time.NewTimer(time.Duration(in.NewTimeout) * time.Second)
	s.MessageTimer.Store(in.Id, newTimer)

	// Update lock info
	info.Timeout = time.Now().Add(time.Duration(in.NewTimeout) * time.Second)
	s.MessageLocks.Store(in.Id, info)

	go func() {
		<-newTimer.C
		s.handleMessageTimeout(in.Id)
	}()

	return &pb.ModifyVisibilityTimeoutResponse{Success: true}, nil
}

func (s *server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic name is required(JT)")
	}

	topicID := uuid.New().String()
	m := spanner.Insert(
		TopicsTable,
		[]string{"id", "name", "createdAt", "updatedAt"},
		[]interface{}{topicID, req.Name, spanner.CommitTimestamp, spanner.CommitTimestamp},
	)

	_, err := s.Client.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create topic: %v", err)
	}

	topic := &pb.Topic{
		Id:   topicID,
		Name: req.Name,
	}
	// -----for testing only-----------//
	if err := s.notifyLeader(ctx, 1); err != nil {
		log.Printf("Failed to notify leader: %v", err)
	}
	return topic, nil
}

func (s *server) GetTopic(ctx context.Context, req *pb.GetTopicRequest) (*pb.Topic, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "topic ID is required")
	}

	stmt := spanner.Statement{
		SQL:    `SELECT id, name, createdAt, updatedAt FROM Topics WHERE id = @id LIMIT 1`,
		Params: map[string]interface{}{"id": req.Id},
	}

	iter := s.Client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return nil, status.Errorf(codes.NotFound, "topic with ID %q not found", req.Id)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query topic: %v", err)
	}

	var (
		id, name             string
		createdAt, updatedAt spanner.NullTime
	)
	if err := row.Columns(&id, &name, &createdAt, &updatedAt); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse topic data: %v", err)
	}

	return &pb.Topic{
		Id:        id,
		Name:      name,
		CreatedAt: convertTime(createdAt),
		UpdatedAt: convertTime(updatedAt),
	}, nil
}

func (s *server) UpdateTopic(ctx context.Context, req *pb.UpdateTopicRequest) (*pb.Topic, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "topic ID is required")
	}
	if req.NewName == "" {
		return nil, status.Error(codes.InvalidArgument, "new topic name is required")
	}

	// 1. Check if the new name already exists
	exist, err := s.topicExists(ctx, req.NewName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check name availability: %v", err)
	}
	if exist {
		return nil, status.Errorf(codes.AlreadyExists, "topic name %q already exists", req.NewName)
	}

	// 2. Fetch the current topic to get its old name
	current, err := s.GetTopic(ctx, &pb.GetTopicRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}

	// 3. Perform both updates in a read-write transaction
	_, err = s.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// 3a. Update the topic row
		mutTopic := spanner.Update(TopicsTable,
			[]string{"id", "name", "updatedAt"},
			[]interface{}{current.Id, req.NewName, spanner.CommitTimestamp},
		)
		if err := txn.BufferWrite([]*spanner.Mutation{mutTopic}); err != nil {
			return err
		}

		// 3b. Update all subscriptions referencing the old topic name
		stmtSubs := spanner.Statement{
			SQL: `UPDATE Subscriptions
                  SET topic = @newName,
                      updatedAt = PENDING_COMMIT_TIMESTAMP()
                  WHERE topic = @oldName`,
			Params: map[string]interface{}{
				"newName": req.NewName,
				"oldName": current.Name,
			},
		}
		_, err2 := txn.Update(ctx, stmtSubs)
		return err2
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update topic: %v", err)
	}

	updatedTopic := &pb.Topic{
		Id:   current.Id,
		Name: req.NewName,
	}

	// optional: notify leader
	if err := s.notifyLeader(ctx, 1); err != nil {
		log.Printf("Failed to notify leader: %v", err)
	}

	return updatedTopic, nil
}

func (s *server) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "topic ID is required")
	}

	_, err := s.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Delete topic
		topicMutation := spanner.Delete(TopicsTable, spanner.Key{req.Id})

		// Delete all related subscriptions
		stmt := spanner.Statement{
			SQL: `DELETE FROM Subscriptions WHERE topic = @topicId`,
			Params: map[string]interface{}{
				"topicId": req.Id,
			},
		}

		// Execute delete operations
		if err := txn.BufferWrite([]*spanner.Mutation{topicMutation}); err != nil {
			return status.Errorf(codes.Internal, "failed to delete topic: %v", err)
		}

		// Execute subscription deletion
		iter := txn.Query(ctx, stmt)
		defer iter.Stop()

		_, err := iter.Next()
		if err != nil && err != iterator.Done {
			return status.Errorf(codes.Internal, "failed to delete subscriptions: %v", err)
		}

		return nil
	})

	if err != nil {
		log.Printf("Failed to delete topic and subscriptions: %v", err)
		return nil, err
	}

	// Notify leader of the deletion
	if err := s.notifyLeader(ctx, 1); err != nil {
		log.Printf("DeleteTopic notification failed: %v", err)
	}

	return &pb.DeleteTopicResponse{Success: true}, nil
}

func (s *server) ListTopics(ctx context.Context, _ *pb.Empty) (*pb.ListTopicsResponse, error) {
	stmt := spanner.Statement{SQL: `SELECT id, name, createdAt, updatedAt FROM Topics`}
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
			id, name             string
			createdAt, updatedAt spanner.NullTime
		)
		if err := row.Columns(&id, &name, &createdAt, &updatedAt); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to parse topic data: %v", err)
		}

		topics = append(topics, &pb.Topic{
			Id:        id,
			Name:      name,
			CreatedAt: convertTime(createdAt),
			UpdatedAt: convertTime(updatedAt),
		})
	}

	return &pb.ListTopicsResponse{Topics: topics}, nil
}

// Helper functions
func (s *server) topicExists(ctx context.Context, name string) (bool, error) {
	stmt := spanner.Statement{
		SQL:    "SELECT 1 FROM Topics WHERE name = @name LIMIT 1",
		Params: map[string]interface{}{"name": name},
	}

	iter := s.Client.Single().Query(ctx, stmt)
	defer iter.Stop()

	_, err := iter.Next()
	if err == iterator.Done {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("existence check failed: %w", err)
	}
	return true, nil
}

func convertTime(t spanner.NullTime) *timestamppb.Timestamp {
	if !t.Valid {
		return nil
	}
	return timestamppb.New(t.Time)
}

// not yet tested ----
func (s *server) notifyLeader(ctx context.Context, flag byte) error {
	//Send needs a slice byte
	//flag = 1 (when updates on topics occurred)
	flagged := map[string]interface{}{"flag": flag}
	jsonData, err := json.Marshal(flagged)
	if err != nil {
		return fmt.Errorf("failed to marshal flag: %w", err)
	}
	reply, err := s.PubSub.Op.Send(ctx, jsonData)
	if err != nil {
		return fmt.Errorf("failed to send to leader: %w", err)
	}
	//let see if there is a reply
	log.Printf("Leader notified with flag: %v, reply: %s", flag, string(reply))

	return nil
}

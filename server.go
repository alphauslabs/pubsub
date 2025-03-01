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
	notifleader   = 1
)

// Publish a message to a topic
func (s *server) Publish(ctx context.Context, in *pb.PublishRequest) (*pb.PublishResponse, error) {
	if in.TopicId == "" {
		return nil, status.Error(codes.InvalidArgument, "topic must not be empty")
	}

	b, _ := json.Marshal(in)

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
		glog.Infof("Error writing to Spanner: %v", err)
		return nil, err
	}

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
	glog.Infof("[Publish] Message successfully broadcasted and wrote to spanner with ID: %s", messageID)
	return &pb.PublishResponse{MessageId: messageID}, nil
}

// Subscribe to receive messages for a subscription
func (s *server) Subscribe(in *pb.SubscribeRequest, stream pb.PubSubService_SubscribeServer) error {
	glog.Infof("[Subscribe] New subscription request received - Topic: %s, Subscription: %s", in.TopicId, in.SubscriptionId)

	// Validate if subscription exists for the given topic
	glog.Infof("[Subscribe] Checking if subscription exists for topic: %s", in.TopicId)
	err := s.validateSubscription(in.TopicId, in.SubscriptionId)
	if err != nil {
		glog.Infof("[Subscribe] Error validating subscription: %v", err)
		return err
	}

	glog.Infof("[Subscribe] Starting subscription stream for ID: %s", in.SubscriptionId)

	// Continuous loop to stream messages
	for {
		select {
		// Check if client has disconnected
		case <-stream.Context().Done():
			glog.Infof("[Subscribe] Client disconnected, closing stream for subscription %s", in.SubscriptionId)
			return nil
		default:
			// Get messages from local storage for the topic
			messages, err := storage.GetMessagesByTopic(in.TopicId)
			if err != nil {
				glog.Infof("[Subscribe] Error getting messages: %v", err)
				time.Sleep(time.Second) // Back off on error
				continue
			}

			// If no messages, wait before checking again
			if len(messages) == 0 {
				glog.Infof("[Subscribe] No messages found for topic %s, waiting...", in.TopicId)
				time.Sleep(time.Second) // todo: not sure if this is the best way
				continue
			}

			glog.Infof("[Subscribe] Found %d messages for topic %s", len(messages), in.TopicId)

			// Process each message
			for _, message := range messages {
				// Skip if message is already locked by another subscriber
				if info, exists := s.MessageLocks.Load(message.Id); exists {
					inf := info.(handlers.MessageLockInfo)
					if inf.Locked {
						glog.Infof("[Subscribe] Message %s is already locked, skipping...", message.Id)
						continue
					}
				}

				// Attempt to acquire distributed lock for the message
				// Default visibility timeout of 30 seconds
				if err := s.broadcastLock(stream.Context(), message.Id, in.SubscriptionId, 30*time.Second); err != nil {
					glog.Infof("[Subscribe] Failed to acquire lock for message %s: %v", message.Id, err)
					continue // Skip if unable to acquire lock
				}
				glog.Infof("[Subscribe] Successfully acquired lock for message %s", message.Id)

				// Stream message to subscriber
				glog.Infof("[Subscribe] Sending message %s to subscriber %s", message.Id, in.SubscriptionId)
				if err := stream.Send(message); err != nil {
					// Release lock if sending fails
					glog.Infof("[Subscribe] Error sending message %s to subscriber: %v", message.Id, err)
					s.localUnlock(message.Id)
					glog.Infof("[Subscribe] Lock released due to send error for message %s", message.Id)
					return err // Return error to close stream
				}
				glog.Infof("[Subscribe] Successfully sent message %s to subscriber %s", message.Id, in.SubscriptionId)
			}
		}
	}
}

func (s *server) Acknowledge(ctx context.Context, in *pb.AcknowledgeRequest) (*pb.AcknowledgeResponse, error) {
	glog.Infof("[Acknowledge] Received acknowledgment for message ID: %s", in.Id)
	// Check if message lock exists and is still valid (within 1 minute)
	lockInfo, ok := s.MessageLocks.Load(in.Id)
	if !ok {
		glog.Infof("[Acknowledge] Error: Message lock not found for ID: %s", in.Id)
		return nil, status.Error(codes.NotFound, "message lock not found")
	}

	info := lockInfo.(handlers.MessageLockInfo)
	glog.Infof("[Acknowledge] Found lock info for message %s - Locked: %v, Timeout: %v, NodeID: %s", in.Id, info.Locked, info.Timeout, info.NodeID)

	// Check if lock is valid and not timed out
	if !info.Locked || time.Now().After(info.Timeout) {
		return nil, status.Error(codes.FailedPrecondition, "message lock expired")
	}

	// Get message processed in time
	glog.Infof("[Acknowledge] Retrieving message %s from storage", in.Id)
	msg, err := storage.GetMessage(in.Id)
	if err != nil {
		glog.Infof("[Acknowledge] Error: Message %s not found in storage: %v", in.Id, err)
		return nil, status.Error(codes.NotFound, "message not found")
	}

	// Mark as processed since subscriber acknowledged in time
	glog.Infof("[Acknowledge] Marking message %s as processed", in.Id)
	msg.Processed = true

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
	s.Op.Broadcast(ctx, bin)

	// Clean up message (processed)
	glog.Infof("[Acknowledge] Cleaning up message %s from local state", in.Id)
	s.MessageLocks.Delete(in.Id)
	if timer, ok := s.MessageTimer.Load(in.Id); ok {
		glog.Infof("[Acknowledge] Stopping timer for message %s", in.Id)
		timer.(*time.Timer).Stop()
		s.MessageTimer.Delete(in.Id)
	}

	// Remove the message from in-memory storage
	storage.RemoveMessage(in.Id) // RemoveMessage method from Storage

	glog.Infof("[Acknowledge] Successfully processed acknowledgment for message %s", in.Id)
	return &pb.AcknowledgeResponse{Success: true}, nil
}

// ModifyVisibilityTimeout extends message lock timeout
func (s *server) ModifyVisibilityTimeout(ctx context.Context, in *pb.ModifyVisibilityTimeoutRequest) (*pb.ModifyVisibilityTimeoutResponse, error) {
	glog.Infof("[ModifyVisibility] Request to modify visibility timeout for message %s to %d seconds", in.Id, in.NewTimeout)

	lockInfo, ok := s.MessageLocks.Load(in.Id)
	if !ok {
		glog.Infof("[ModifyVisibility] Error: Message lock not found for ID: %s", in.Id)
		return nil, status.Error(codes.NotFound, "message lock not found")
	}

	info := lockInfo.(handlers.MessageLockInfo)
	glog.Infof("[ModifyVisibility] Current lock info - Locked: %v, Timeout: %v, NodeID: %s",
		info.Locked, info.Timeout, info.NodeID)

	if !info.Locked {
		glog.Infof("[ModifyVisibility] Error: Message %s is not locked", in.Id)
		return nil, status.Error(codes.FailedPrecondition, "message not locked")
	}

	// Check if this node owns the lock before extending
	if info.NodeID != s.Op.HostPort() {
		glog.Infof("[ModifyVisibility] Error: Only lock owner can extend timeout. Current owner: %s, This node: %s",
			info.NodeID, s.Op.HostPort())
		return nil, status.Error(codes.PermissionDenied, "only the lock owner can extend timeout")
	}

	// Broadcast new timeout
	glog.Infof("[ModifyVisibility] Broadcasting timeout extension for message %s", in.Id)
	broadcastData := handlers.BroadCastInput{
		Type: handlers.MsgEvent,
		Msg:  []byte(fmt.Sprintf("extend:%s:%d:%s", in.Id, in.NewTimeout, s.Op.HostPort())),
	}
	bin, _ := json.Marshal(broadcastData)
	s.Op.Broadcast(ctx, bin)

	// Update local timer
	if timer, ok := s.MessageTimer.Load(in.Id); ok {
		glog.Infof("[ModifyVisibility] Stopping existing timer for message %s", in.Id)
		timer.(*time.Timer).Stop()
	}
	glog.Infof("[ModifyVisibility] Creating new timer for %d seconds", in.NewTimeout)
	newTimer := time.NewTimer(time.Duration(in.NewTimeout) * time.Second)
	s.MessageTimer.Store(in.Id, newTimer)

	// Update lock info
	newTimeout := time.Now().Add(time.Duration(in.NewTimeout) * time.Second)
	glog.Infof("[ModifyVisibility] Updating lock timeout from %v to %v", info.Timeout, newTimeout)
	info.Timeout = newTimeout
	s.MessageLocks.Store(in.Id, info)

	go func() {
		glog.Infof("[ModifyVisibility] Starting timeout handler for message %s", in.Id)
		<-newTimer.C
		glog.Infof("[ModifyVisibility] Timer expired for message %s, handling timeout", in.Id)
		s.handleMessageTimeout(in.Id)
	}()

	glog.Infof("[ModifyVisibility] Successfully extended visibility timeout for message %s", in.Id)
	return &pb.ModifyVisibilityTimeoutResponse{Success: true}, nil
}

func (s *server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic name is required")
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
	go func() {
		s.notifyLeader(1) // Send flag=1 to indicate an update
	}()

	return topic, nil
}

func (s *server) GetTopic(ctx context.Context, req *pb.GetTopicRequest) (*pb.Topic, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "topic ID is required")
	}

	stmt := spanner.Statement{
		SQL:    `SELECT id, name, createdAt, updatedAt FROM Topics WHERE name = @name LIMIT 1`,
		Params: map[string]interface{}{"name": req.Id},
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

	// Check if the new name already exists
	exist, err := s.topicExists(ctx, req.NewName)
	if exist {
		return nil, status.Errorf(codes.AlreadyExists, "topic name %q already exists, err: %v", req.NewName, err)
	}

	// Fetch the current topic to get its old name
	current, err := s.GetTopic(ctx, &pb.GetTopicRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}

	mutTopic := spanner.Update(TopicsTable,
		[]string{"id", "name", "updatedAt"},
		[]interface{}{current.Id, req.NewName, spanner.CommitTimestamp},
	)
	_, err = s.Client.Apply(ctx, []*spanner.Mutation{mutTopic})
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
			"oldName": current.Name,
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
		Id:   current.Id,
		Name: req.NewName,
	}
	go func() {
		s.notifyLeader(1) // Send flag=1 to indicate an update
	}()

	return updatedTopic, nil
}

func (s *server) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "topic ID is required")
	}

	// Delete topic
	topicMutation := spanner.Delete(TopicsTable, spanner.Key{req.Id})
	_, err := s.Client.Apply(ctx, []*spanner.Mutation{topicMutation})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete topic: %v", err)
	}

	_, err = s.Client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Delete all related subscriptions
		stmt := spanner.Statement{
			SQL: `DELETE FROM Subscriptions WHERE topic = @topicId`,
			Params: map[string]interface{}{
				"topicId": req.Id,
			},
		}
		_, err = txn.Update(ctx, stmt)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to delete subscriptions: %v", err)
		}

		return nil
	})

	if err != nil {
		glog.Infof("Failed to delete topic and subscriptions: %v", err)
		return nil, err
	}
	go func() {
		s.notifyLeader(1) // Send flag=1 to indicate an update
	}()

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
		return true, fmt.Errorf("existence check failed: %w", err)
	}
	return true, nil
}

func convertTime(t spanner.NullTime) *timestamppb.Timestamp {
	if !t.Valid {
		return nil
	}
	return timestamppb.New(t.Time)
}

func (s *server) notifyLeader(flag int) {
	// Create a simple payload with just the flag
	data := map[string]interface{}{
		"flag": flag,
	}

	// Serialize the data
	jsonData, err := json.Marshal(data)
	if err != nil {
		glog.Infof("Error marshaling data: %v", err)
		return
	}

	// Create SendInput with topicsubupdates type
	input := handlers.SendInput{
		Type: "topicsubupdates", // Use the constant defined in send.go
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

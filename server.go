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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	*app.PubSub
	pb.UnimplementedPubSubServiceServer
}

const (
	MessagesTable = "Messages"
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
		log.Printf("[Subscribe] Topic %s not found in storage", in.TopicId)
		return status.Errorf(codes.NotFound, "Topic %s not found", in.TopicId)
	}

	log.Printf("[Subscribe] Found subscriptions for topic %s: %v", in.TopicId, subs)

	// Check if the provided subscription ID exists in the topic's subscriptions
	found := false
	for _, sub := range subs {
		if sub == in.SubscriptionId {
			found = true
			break
		}
	}

	if !found {
		log.Printf("[Subscribe] Subscription %s not found in topic %s", in.SubscriptionId, in.TopicId)
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

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
	log.Printf("[Subscribe] New subscription request received - Topic: %s, Subscription: %s", in.TopicId, in.SubscriptionId)

	// Validate if subscription exists for the given topic
	log.Printf("[Subscribe] Checking if subscription exists for topic: %s", in.TopicId)
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
			log.Printf("[Subscribe] Subscription %s found in topic %s", in.SubscriptionId, in.TopicId)
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
			log.Printf("[Subscribe] Client disconnected, closing stream for subscription %s", in.SubscriptionId)
			return nil
		default:
			// Get messages from local storage for the topic
			log.Printf("[Subscribe] Checking for messages on topic: %s", in.TopicId)
			messages, err := s.Storage.GetMessagesByTopic(in.TopicId)
			if err != nil {
				log.Printf("[Subscribe] Error getting messages: %v", err)
				time.Sleep(time.Second) // Back off on error
				continue
			}

			// If no messages, wait before checking again
			if len(messages) == 0 {
				log.Printf("[Subscribe] No messages found for topic %s, waiting...", in.TopicId)
				time.Sleep(time.Second) // todo: not sure if this is the best way
				continue
			}

			log.Printf("[Subscribe] Found %d messages for topic %s", len(messages), in.TopicId)

			// Process each message
			for _, message := range messages {
				// Skip if message is already locked by another subscriber
				if _, exists := s.MessageLocks.Load(message.Id); exists {
					log.Printf("[Subscribe] Message %s already locked, skipping", message.Id)
					continue
				}

				// Attempt to acquire distributed lock for the message
				// Default visibility timeout of 30 seconds
				if err := s.broadcastLock(stream.Context(), message.Id, in.SubscriptionId, 30*time.Second); err != nil {
					log.Printf("[Subscribe] Failed to acquire lock for message %s: %v", message.Id, err)
					continue // Skip if unable to acquire lock
				}
				log.Printf("[Subscribe] Successfully acquired lock for message %s", message.Id)

				// Stream message to subscriber
				log.Printf("[Subscribe] Sending message %s to subscriber %s", message.Id, in.SubscriptionId)
				if err := stream.Send(message); err != nil {
					// Release lock if sending fails
					log.Printf("[Subscribe] Error sending message %s to subscriber: %v", message.Id, err)
					s.broadcastUnlock(stream.Context(), message.Id)
					log.Printf("[Subscribe] Lock released due to send error for message %s", message.Id)
					return err // Return error to close stream
				}
				log.Printf("[Subscribe] Successfully sent message %s to subscriber %s", message.Id, in.SubscriptionId)
			}
		}
	}
}

// Acknowledge a processed message
func (s *server) Acknowledge(ctx context.Context, in *pb.AcknowledgeRequest) (*pb.AcknowledgeResponse, error) {

	log.Printf("[Acknowledge] Received acknowledgment for message ID: %s", in.Id)
	// Check if message lock exists and is still valid (within 1 minute)
	lockInfo, ok := s.MessageLocks.Load(in.Id)
	if !ok {
		log.Printf("[Acknowledge] Error: Message lock not found for ID: %s", in.Id)
		return nil, status.Error(codes.NotFound, "message lock not found")
	}

	info := lockInfo.(broadcast.MessageLockInfo)
	log.Printf("[Acknowledge] Found lock info for message %s - Locked: %v, Timeout: %v, NodeID: %s", in.Id, info.Locked, info.Timeout, info.NodeID)

	// Check if lock is valid and not timed out
	if !info.Locked || time.Now().After(info.Timeout) {
		log.Printf("[Acknowledge] Error: Message lock expired for ID: %s, current time: %v", in.Id, time.Now())
		// Message already timed out - handled by handleMessageTimeout
		return nil, status.Error(codes.FailedPrecondition, "message lock expired")
	}

	// Get message processed in time
	log.Printf("[Acknowledge] Retrieving message %s from storage", in.Id)
	msg, err := s.Storage.GetMessage(in.Id)
	if err != nil {
		log.Printf("[Acknowledge] Error: Message %s not found in storage: %v", in.Id, err)
		return nil, status.Error(codes.NotFound, "message not found")
	}
	// Mark as processed since subscriber acknowledged in time
	log.Printf("[Acknowledge] Marking message %s as processed", in.Id)
	msg.Processed = true
	if err := s.Storage.StoreMessage(msg); err != nil {
		log.Printf("[Acknowledge] Error updating message %s in storage: %v", in.Id, err)
		return nil, status.Error(codes.Internal, "failed to update message")
	}
	log.Printf("[Acknowledge] Successfully marked message %s as processed", in.Id)

	// Broadcast successful processing
	log.Printf("[Acknowledge] Broadcasting deletion event for message %s", in.Id)
	broadcastData := broadcast.BroadCastInput{
		Type: broadcast.MsgEvent,
		Msg:  []byte(fmt.Sprintf("delete:%s", in.Id)),
	}
	bin, _ := json.Marshal(broadcastData)
	s.Op.Broadcast(ctx, bin)

	// Clean up message (processed)
	log.Printf("[Acknowledge] Cleaning up message %s from local state", in.Id)
	s.MessageLocks.Delete(in.Id)
	if timer, ok := s.MessageTimer.Load(in.Id); ok {
		log.Printf("[Acknowledge] Stopping timer for message %s", in.Id)
		timer.(*time.Timer).Stop()
		s.MessageTimer.Delete(in.Id)
	}

	log.Printf("[Acknowledge] Successfully processed acknowledgment for message %s", in.Id)
	return &pb.AcknowledgeResponse{Success: true}, nil
}

// ModifyVisibilityTimeout extends message lock timeout
func (s *server) ModifyVisibilityTimeout(ctx context.Context, in *pb.ModifyVisibilityTimeoutRequest) (*pb.ModifyVisibilityTimeoutResponse, error) {
	log.Printf("[ModifyVisibility] Request to modify visibility timeout for message %s to %d seconds", in.Id, in.NewTimeout)

	lockInfo, ok := s.MessageLocks.Load(in.Id)
	if !ok {
		log.Printf("[ModifyVisibility] Error: Message lock not found for ID: %s", in.Id)
		return nil, status.Error(codes.NotFound, "message lock not found")
	}

	info := lockInfo.(broadcast.MessageLockInfo)
	log.Printf("[ModifyVisibility] Current lock info - Locked: %v, Timeout: %v, NodeID: %s",
		info.Locked, info.Timeout, info.NodeID)

	if !info.Locked {
		log.Printf("[ModifyVisibility] Error: Message %s is not locked", in.Id)
		return nil, status.Error(codes.FailedPrecondition, "message not locked")
	}

	// Check if this node owns the lock before extending
	if info.NodeID != s.Op.HostPort() {
		log.Printf("[ModifyVisibility] Error: Only lock owner can extend timeout. Current owner: %s, This node: %s",
			info.NodeID, s.Op.HostPort())
		return nil, status.Error(codes.PermissionDenied, "only the lock owner can extend timeout")
	}

	// Broadcast new timeout
	log.Printf("[ModifyVisibility] Broadcasting timeout extension for message %s", in.Id)
	broadcastData := broadcast.BroadCastInput{
		Type: broadcast.MsgEvent,
		Msg:  []byte(fmt.Sprintf("extend:%s:%d:%s", in.Id, in.NewTimeout, s.Op.HostPort())),
	}
	bin, _ := json.Marshal(broadcastData)
	s.Op.Broadcast(ctx, bin)

	// Update local timer
	if timer, ok := s.MessageTimer.Load(in.Id); ok {
		log.Printf("[ModifyVisibility] Stopping existing timer for message %s", in.Id)
		timer.(*time.Timer).Stop()
	}
	log.Printf("[ModifyVisibility] Creating new timer for %d seconds", in.NewTimeout)
	newTimer := time.NewTimer(time.Duration(in.NewTimeout) * time.Second)
	s.MessageTimer.Store(in.Id, newTimer)

	// Update lock info
	newTimeout := time.Now().Add(time.Duration(in.NewTimeout) * time.Second)
	log.Printf("[ModifyVisibility] Updating lock timeout from %v to %v", info.Timeout, newTimeout)
	info.Timeout = newTimeout
	s.MessageLocks.Store(in.Id, info)

	go func() {
		log.Printf("[ModifyVisibility] Starting timeout handler for message %s", in.Id)
		<-newTimer.C
		log.Printf("[ModifyVisibility] Timer expired for message %s, handling timeout", in.Id)
		s.handleMessageTimeout(in.Id)
	}()

	log.Printf("[ModifyVisibility] Successfully extended visibility timeout for message %s", in.Id)
	return &pb.ModifyVisibilityTimeoutResponse{Success: true}, nil
}

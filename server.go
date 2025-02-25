package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
<<<<<<< HEAD
	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/broadcast"
=======
>>>>>>> origin/kate_branch
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
    "github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/broadcast"
)

type server struct {
<<<<<<< HEAD
	*app.PubSub
=======
	*PubSub
>>>>>>> origin/kate_branch
	pb.UnimplementedPubSubServiceServer
}

// Constant for table name and message types
const (
	MessagesTable = "Messages"
	// message       = "message"  // Match the constants in broadcast.go
	// topicsub      = "topicsub" // Match the constants in broadcast.go
)


// Publish a message to a topic
func (s *server) Publish(ctx context.Context, in *pb.PublishRequest) (*pb.PublishResponse, error) {
	if in.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic must not be empty")
	}
	b, _ := json.Marshal(in)
<<<<<<< HEAD

=======
>>>>>>> origin/kate_branch
	l, _ := s.Op.HasLock()
	if l {
		log.Println("[Publish-leader] Received message:\n", string(b))
	} else {
		log.Printf("[Publish] Received message:\n%v", string(b))
	}

	messageID := uuid.New().String()
	mutation := spanner.InsertOrUpdate(
<<<<<<< HEAD
    MessagesTable,
    []string{"id", "topic", "payload", "createdAt", "updatedAt", "visibilityTimeout", "processed"},
    []interface{}{
        messageID,
        in.Topic,
        in.Payload,
        spanner.CommitTimestamp,
        spanner.CommitTimestamp,
        nil,  // Explicitly set visibilityTimeout as NULL
        false, // Default to unprocessed
    },
)

=======
		MessagesTable,
		[]string{"id", "topic", "payload", "createdAt", "updatedAt", "visibilityTimeout", "processed"},
		[]interface{}{
			messageID,
			in.Topic,
			in.Payload,
			spanner.CommitTimestamp,
			spanner.CommitTimestamp,
			nil,   // Initial visibilityTimeout is NULL
			false, // Not processed yet
		},
	)
>>>>>>> origin/kate_branch
	_, err := s.Client.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		log.Printf("Error writing to Spanner: %v", err)
		return nil, err
	}

<<<<<<< HEAD
	// broadcast message
	bcastin := broadcast.BroadCastInput{
		Type: "message",
=======
	// broadcast message - using the correct message Type constant
	bcastin := broadCastInput{
		Type: message,
>>>>>>> origin/kate_branch
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
	// Validate subscription in memory first
	subscription, err := s.validateTopicSubscription(in.SubscriptionId)
	if err != nil {
		return err
	}
	log.Printf("[Subscribe] Starting subscription stream for ID: %s", in.SubscriptionId)
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			// Request message from the leader instead of querying directly
			message, err := s.requestMessageFromLeader(subscription.TopicId)
			if err != nil {
				log.Printf("[Subscribe] No available messages for subscription: %s", in.SubscriptionId)
				time.Sleep(time.Second) // Prevent CPU overuse
				continue
			}
			// Ensure it's not already locked by another node
			if _, exists := s.messageLocks.Load(message.Id); exists {
				continue // Skip locked messages
			}
			// Try to acquire distributed lock
			if err := s.broadcastLock(stream.Context(), message.Id, in.SubscriptionId, 30*time.Second); err != nil {
				continue
			}
			// Send message to subscriber
			if err := stream.Send(message); err != nil {
				s.broadcastUnlock(stream.Context(), message.Id)
				return err
			}
		}
	}
}

// Acknowledge a processed message
func (s *server) Acknowledge(ctx context.Context, in *pb.AcknowledgeRequest) (*pb.AcknowledgeResponse, error) {
	// Verify lock exists and is valid
	lockInfo, ok := s.messageLocks.Load(in.Id)
	if !ok {
		return nil, status.Error(codes.NotFound, "message lock not found")
	}
	info := lockInfo.(MessageLockInfo)
	if !info.Locked || time.Now().After(info.Timeout) {
		return nil, status.Error(codes.FailedPrecondition, "message lock expired")
	}
	// Update Spanner
	mutation := spanner.Update(
		MessagesTable,
		[]string{"id", "processed", "updatedAt"},
		[]interface{}{in.Id, true, spanner.CommitTimestamp},
	)
	_, err := s.Client.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		return nil, err
	}
	// Broadcast delete to all nodes - format matching handleBroadcastedMsg
	broadcastData := broadCastInput{
		Type: message,
		Msg:  []byte(fmt.Sprintf("delete:%s", in.Id)),
	}

	bin, _ := json.Marshal(broadcastData)
	s.Op.Broadcast(ctx, bin)
	// Clean up local state
	s.messageLocks.Delete(in.Id)
	if timer, ok := s.timeoutTimers.Load(in.Id); ok {
		timer.(*time.Timer).Stop()
		s.timeoutTimers.Delete(in.Id)
	}
	return &pb.AcknowledgeResponse{Success: true}, nil
}

// ModifyVisibilityTimeout extends message lock timeout
func (s *server) ModifyVisibilityTimeout(ctx context.Context, in *pb.ModifyVisibilityTimeoutRequest) (*pb.ModifyVisibilityTimeoutResponse, error) {
	lockInfo, ok := s.messageLocks.Load(in.Id)
	if !ok {
		return nil, status.Error(codes.NotFound, "message lock not found")
	}
	info := lockInfo.(MessageLockInfo)
	if !info.Locked {
		return nil, status.Error(codes.FailedPrecondition, "message not locked")
	}
	// Ensure the same node is extending the lock
	if info.NodeID != s.Op.ID() {
		return nil, status.Error(codes.PermissionDenied, "only the original lock holder can extend timeout")
	}
	// Broadcast new timeout - format matching handleBroadcastedMsg
	broadcastData := broadCastInput{
		Type: message,
		Msg:  []byte(fmt.Sprintf("extend:%s:%d", in.Id, in.NewTimeout)),
	}
	bin, _ := json.Marshal(broadcastData)
	s.Op.Broadcast(ctx, bin)
	// Update local timer
	if timer, ok := s.timeoutTimers.Load(in.Id); ok {
		timer.(*time.Timer).Stop()
	}
	newTimer := time.NewTimer(time.Duration(in.NewTimeout) * time.Second)
	s.timeoutTimers.Store(in.Id, newTimer)
	// Update lock info
	info.Timeout = time.Now().Add(time.Duration(in.NewTimeout) * time.Second)
	s.messageLocks.Store(in.Id, info)
	go func() {
		<-newTimer.C
		s.handleMessageTimeout(in.Id)
	}()
	return &pb.ModifyVisibilityTimeoutResponse{Success: true}, nil
}

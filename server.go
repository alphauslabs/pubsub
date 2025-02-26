//server.go
package main

import (
	"context"
	"encoding/json"
	"log"
	"time"
	"fmt"

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
        in.Topic,
        in.Payload,
        spanner.CommitTimestamp,
        spanner.CommitTimestamp,
        nil,  // Explicitly set visibilityTimeout as NULL
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
		Type: broadcast.message,
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
// Subscribe to receive messages for a subscription
func (s *server) Subscribe(in *pb.SubscribeRequest, stream pb.PubSubService_SubscribeServer) error {
    // Validate subscription using storage
    subs, err := s.Storage.GetSubscribtionsForTopic(in.TopicId)
    if err != nil {
        return status.Errorf(codes.NotFound, "Topic %s not found", in.TopicId)
    }

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
    
    for {
        select {
        case <-stream.Context().Done():
            return nil
        default:
            // Get messages from storage
            messages, err := s.Storage.GetMessagesByTopic(in.TopicId)
            if err != nil {
                log.Printf("[Subscribe] Error getting messages: %v", err)
                time.Sleep(time.Second)
                continue
            }

            if len(messages) == 0 {
                time.Sleep(time.Second)
                continue
            }

            for _, message := range messages {
                // Check if message is locked
                if _, exists := s.messageLocks.Load(message.Id); exists {
                    continue
                }

                // Try to acquire lock
                if err := s.broadcastLock(stream.Context(), message.Id, in.SubscriptionId, 30*time.Second); err != nil {
                    continue
                }

                // Send message
                if err := stream.Send(message); err != nil {
                    s.broadcastUnlock(stream.Context(), message.Id)
                    return err
                }
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

    info := lockInfo.(broadcast.MessageLockInfo)
    if !info.Locked || time.Now().After(info.Timeout) {
        return nil, status.Error(codes.FailedPrecondition, "message lock expired")
    }

    // Get message from storage
    msg, err := s.Storage.GetMessage(in.Id)
    if err != nil {
        return nil, status.Error(codes.NotFound, "message not found")
    }

    // Mark message as processed in storage
    msg.Processed = true
    if err := s.Storage.StoreMessage(msg); err != nil {
        return nil, status.Error(codes.Internal, "failed to update message")
    }

    // Broadcast acknowledgment
    broadcastData := broadcast.BroadCastInput{
        Type: broadcast.msgEvent,
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
    info := lockInfo.(broadcast.MessageLockInfo)
    if !info.Locked {
        return nil, status.Error(codes.FailedPrecondition, "message not locked")
    }
    
    // Check if this node owns the lock before extending
    if info.NodeID != s.Op.ID() {
        return nil, status.Error(codes.PermissionDenied, "only the lock owner can extend timeout")
    }
    
    // Broadcast new timeout
    broadcastData := broadcast.BroadCastInput{
        Type: broadcast.msgEvent,
        Msg:  []byte(fmt.Sprintf("extend:%s:%d:%s", in.Id, in.NewTimeout, s.Op.ID())),
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

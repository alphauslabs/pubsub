//helpers.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
    "github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/broadcast"
)


// validateTopicSubscription checks if subscription exists in memory
func (s *server) validateTopicSubscription(subscriptionID string) (*pb.Subscription, error) {
	if val, ok := s.subscriptions.Load(subscriptionID); ok {
		return val.(*pb.Subscription), nil
	}
	// Request subscription details from the leader - using topicsub message type
	broadcastData := broadcast.BroadCastInput{
		Type: broadcast.topicsub,
		Msg:  []byte(fmt.Sprintf("get:%s", subscriptionID)),
	}
	bin, _ := json.Marshal(broadcastData)
	resp, err := s.Op.Request(context.Background(), bin)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to request subscription from leader")
	}

	var subscription pb.Subscription
	if err := json.Unmarshal(resp, &subscription); err != nil {
		return nil, status.Error(codes.Internal, "failed to parse subscription response")
	}

	if subscription.Id == "" {
		return nil, status.Error(codes.NotFound, "subscription not found")
	}

	// Store it in memory to prevent duplicate lookups
	s.subscriptions.Store(subscriptionID, &subscription)
	return &subscription, nil
}

// broadcastLock sends lock request to all nodes and waits for acknowledgment
func (s *server) broadcastLock(ctx context.Context, messageID string, subscriberID string, timeout time.Duration) error {
    lockInfo := broadcast.MessageLockInfo{
        Timeout:      time.Now().Add(timeout),
        Locked:       true,
        NodeID:       s.Op.ID(),
        SubscriberID: subscriberID,
        LockHolders:  make(map[string]bool),
    }
    
    // Add this node as a lock holder
    lockInfo.LockHolders[s.Op.ID()] = true
    
    // Check if already locked by this node
    if val, loaded := s.messageLocks.LoadOrStore(messageID, lockInfo); loaded {
        existingInfo := val.(broadcast.MessageLockInfo)
        if existingInfo.NodeID != s.Op.ID() {
            return fmt.Errorf("message already locked by another node")
        }
        // Already locked by this node, just return success
        return nil
    }
    
    // Broadcast lock request to all nodes
    broadcastData := broadcast.BroadCastInput{
        Type: broadcast.msgEvent,
        Msg:  []byte(fmt.Sprintf("lock:%s:%d:%s:%s", messageID, int(timeout.Seconds()), subscriberID, s.Op.ID())),
    }
    
    bin, _ := json.Marshal(broadcastData)
    out := s.Op.Broadcast(ctx, bin)
    
    // Track which nodes acknowledged the lock
    successCount := 1 // Include self
    for i, v := range out {
        if v.Error == nil {
            successCount++
            // Track which node acknowledged
            lockInfo.LockHolders[fmt.Sprintf("node-%d", i)] = true
        }
    }
    
    // Need majority for consensus
    if successCount < (len(out)/2 + 1) {
        s.messageLocks.Delete(messageID)
        return fmt.Errorf("failed to acquire lock across majority of nodes")
    }
    
    // Update lock info with acknowledgments
    s.messageLocks.Store(messageID, lockInfo)
    
    // Start local timeout timer
    timer := time.NewTimer(timeout)
    s.timeoutTimers.Store(messageID, timer)
    
    go func() {
        <-timer.C
        s.handleMessageTimeout(messageID)
    }()
    
    return nil
}

// handleMessageTimeout ensures that if a node crashes while holding a lock, 
// other nodes can unlock the message and allow it to be processed again.
func (s *server) handleMessageTimeout(messageID string) {
    if lockInfo, ok := s.messageLocks.Load(messageID); ok {
        info := lockInfo.(broadcast.MessageLockInfo)
        
        // Only unlock if this node is the lock owner
        if info.NodeID == s.Op.ID() && info.Locked && time.Now().After(info.Timeout) {
            log.Printf("[Timeout] Node %s unlocking expired message: %s", s.Op.ID(), messageID)
            
            // Broadcast unlock
            s.broadcastUnlock(context.Background(), messageID)
            
            // Notify all nodes to retry processing this message
            broadcastData := broadcast.BroadCastInput{
                Type: broadcast.msgEvent,
                Msg:  []byte(fmt.Sprintf("retry:%s:%s", messageID, s.Op.ID())),
            }
            bin, _ := json.Marshal(broadcastData)
            s.Op.Broadcast(context.Background(), bin)
        }
    }
}

// broadcastUnlock ensures that only the leader node is responsible for broadcasting unlock requests
func (s *server) broadcastUnlock(ctx context.Context, messageID string) {

	// Any node can broadcast an unlock
	broadcastData := broadcast.BroadCastInput{
		Type: broadcast.msgEvent,
		Msg:  []byte(fmt.Sprintf("unlock:%s:%s", messageID, s.Op.ID())),
	}
	bin, _ := json.Marshal(broadcastData)
	s.Op.Broadcast(ctx, bin)
	// Clean up local state
	s.messageLocks.Delete(messageID)
	if timer, ok := s.timeoutTimers.Load(messageID); ok {
		timer.(*time.Timer).Stop()
		s.timeoutTimers.Delete(messageID)
	}
	log.Printf("[Unlock] Node %s unlocked message: %s", s.Op.ID(), messageID)
}


// requestMessageFromLeader asks the leader node for messages
func (s *server) requestMessageFromLeader(topicID string) (*pb.Message, error) {
	// Use the message type with proper format for requesting a message
	broadcastData := broadcast.BroadCastInput{
		Type: broadcast.msgEvent,
		Msg:  []byte(fmt.Sprintf("getmessage:%s", topicID)),
	}

	bin, _ := json.Marshal(broadcastData)
	resp, err := s.Op.Request(context.Background(), bin)
	if err != nil {
		return nil, err
	}

	if len(resp) == 0 {
		return nil, status.Error(codes.NotFound, "no messages available")
	}

	var message pb.Message
	if err := json.Unmarshal(resp, &message); err != nil {
		return nil, err
	}

	return &message, nil
}

// ExtendVisibilityTimeout extends the visibility timeout for a message
func (s *server) ExtendVisibilityTimeout(messageID string, subscriberID string, visibilityTimeout time.Duration) error {
    value, exists := s.messageLocks.Load(messageID)
    if !exists {
        return status.Error(codes.NotFound, "message not locked")
    }
    
    info, ok := value.(broadcast.MessageLockInfo)
    if !ok {
        return status.Error(codes.Internal, "invalid lock info")
    }
    
    // Check if this node owns the lock
    if info.NodeID != s.Op.ID() {
        return status.Error(codes.PermissionDenied, "only the lock owner can extend timeout")
    }
    
    // Check subscriber ID
    if info.SubscriberID != subscriberID {
        return status.Error(codes.PermissionDenied, "message locked by another subscriber")
    }
    
    // Extend visibility timeout
    newExpiresAt := time.Now().Add(visibilityTimeout)
    info.Timeout = newExpiresAt
    s.messageLocks.Store(messageID, info)
    
    // Create broadcast message
    broadcastData := broadcast.BroadCastInput{
        Type: broadcast.msgEvent,
        Msg:  []byte(fmt.Sprintf("extend:%s:%d:%s", messageID, int(visibilityTimeout.Seconds()), s.Op.ID())),
    }
    msgBytes, _ := json.Marshal(broadcastData)
    
    // Broadcast new timeout to all nodes
    s.Op.Broadcast(context.TODO(), msgBytes)
    log.Printf("[ExtendTimeout] Node %s extended timeout for message: %s", s.Op.ID(), messageID)
    
    return nil
}

// HandleBroadcastMessage processes broadcast messages received from other nodes
func (s *server) HandleBroadcastMessage(msgType string, msgData []byte) error {
	// This method would be called by your broadcast handler
	switch msgType {
	case "lock":
		parts := strings.Split(string(msgData), ":")
		if len(parts) < 3 {
			return fmt.Errorf("invalid lock message format")
		}
		messageID := parts[0]
		timeoutSecondsStr := parts[1]
		subscriberID := parts[2]

		timeoutSeconds, err := strconv.Atoi(timeoutSecondsStr)
		if err != nil {
			return err
		}

		// Store the lock locally
		lockInfo := MessageLockInfo{
			Timeout:      time.Now().Add(time.Duration(timeoutSeconds) * time.Second),
			Locked:       true,
			NodeID:       s.Op.ID(), // This is the current node
			SubscriberID: subscriberID,
			LockHolders:  make(map[string]bool),
		}
		s.messageLocks.Store(messageID, lockInfo)

	case "unlock":
		messageID := string(msgData)
		s.messageLocks.Delete(messageID)

	}

	return nil
}

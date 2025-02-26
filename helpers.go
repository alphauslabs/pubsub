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


// validateTopicSubscription checks if subscription exists in storage
func (s *server) validateTopicSubscription(subscriptionID string) error {
    subs, err := s.Storage.GetSubscribtionsForTopic(subscriptionID)
    if err != nil {
        return status.Errorf(codes.NotFound, "subscription not found")
    }

    found := false
    for _, sub := range subs {
        if sub == subscriptionID {
            found = true
            break
        }
    }

    if !found {
        return status.Errorf(codes.NotFound, "subscription not found")
    }

    return nil
}

// / broadcastLock handles distributed locking
func (s *server) broadcastLock(ctx context.Context, messageID string, subscriberID string, timeout time.Duration) error {
    lockInfo := broadcast.MessageLockInfo{
        Timeout:      time.Now().Add(timeout),
        Locked:       true,
        NodeID:       s.Op.ID(),
        SubscriberID: subscriberID,
        LockHolders:  make(map[string]bool),
    }

    // Check if message exists in storage
    _, err := s.Storage.GetMessage(messageID)
    if err != nil {
        return err
    }

    // Store lock information
    lockInfo.LockHolders[s.Op.ID()] = true
    s.messageLocks.Store(messageID, lockInfo)

    // Broadcast lock request
    broadcastData := broadcast.BroadCastInput{
        Type: broadcast.msgEvent,
        Msg:  []byte(fmt.Sprintf("lock:%s:%d:%s:%s", messageID, int(timeout.Seconds()), subscriberID, s.Op.ID())),
    }

    bin, _ := json.Marshal(broadcastData)
    out := s.Op.Broadcast(ctx, bin)

    // Track acknowledgments
    successCount := 1 // Include self
    for _, v := range out {
        if v.Error == nil {
            successCount++
        }
    }

    // Need majority for consensus
    if successCount < (len(out)/2 + 1) {
        s.messageLocks.Delete(messageID)
        return fmt.Errorf("failed to acquire lock across majority of nodes")
    }

    // Start timeout timer
    timer := time.NewTimer(timeout)
    s.timeoutTimers.Store(messageID, timer)

    go func() {
        <-timer.C
        s.handleMessageTimeout(messageID)
    }()

    return nil
}

func (s *server) handleMessageTimeout(messageID string) {
    if lockInfo, ok := s.messageLocks.Load(messageID); ok {
        info := lockInfo.(broadcast.MessageLockInfo)
        if info.NodeID == s.Op.ID() && info.Locked && time.Now().After(info.Timeout) {
            s.broadcastUnlock(context.Background(), messageID)
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

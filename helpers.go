package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
    pb "github.com/alphauslabs/pubsub-proto/v1"
    "google.golang.org/grpc/status"
    "google.golang.org/grpc/codes"
)


//HELPERFUNCTIONS//
// validateTopicSubscription checks if subscription exists in memory
func (s *server) validateTopicSubscription(subscriptionID string) (*pb.Subscription, error) {
    if val, ok := s.subscriptions.Load(subscriptionID); ok {
        return val.(*pb.Subscription), nil
    }

    // Request subscription details from the leader
    leaderSubscription, err := s.requestSubscriptionFromLeader(subscriptionID)
    if err != nil {
        return nil, status.Error(codes.NotFound, "subscription not found in memory or leader")
    }

       // Store it in memory to prevent duplicate lookups
       s.subscriptions.Store(subscriptionID, leaderSubscription)

    return leaderSubscription, nil // Do not store in-memory cache here
}

// broadcastLock sends lock request to all nodes and waits for acknowledgment
func (s *server) broadcastLock(ctx context.Context, messageID string, timeout time.Duration) error {
    lockInfo := MessageLockInfo{
        Timeout:     time.Now().Add(timeout),
        Locked:      true,
        NodeID:      s.Op.ID(),
        LockHolders: make(map[string]bool),
    }

    // Store initial lock info before broadcasting
    _, loaded := s.messageLocks.LoadOrStore(messageID, lockInfo)
    if loaded {
        return fmt.Errorf("message already locked by another node")
    }

    // Broadcast lock request
    broadcastData := broadCastInput{
        Type: message,
        Msg:  []byte(fmt.Sprintf("lock:%s:%d", messageID, timeout.Seconds())),
    }
    
    bin, _ := json.Marshal(broadcastData)
    out := s.Op.Broadcast(ctx, bin)

    // Ensure majority of nodes acknowledged
    successCount := 0
    for _, v := range out {
        if v.Error == nil {
            successCount++
        }
    }

    if successCount < (len(out)/2 + 1) {
        s.messageLocks.Delete(messageID)
        return fmt.Errorf("failed to acquire lock across majority of nodes")
    }

    // Start local timeout timer
    timer := time.NewTimer(timeout)
    s.timeoutTimers.Store(messageID, timer)
    
    go func() {
        <-timer.C
        s.handleMessageTimeout(messageID)
    }()

    return nil
}

//helper function -   function ensures that if a node crashes while holding a lock, other nodes can unlock the message and allow it to be processed again.
func (s *server) handleMessageTimeout(messageID string) {
    if lockInfo, ok := s.messageLocks.Load(messageID); ok {
        info := lockInfo.(MessageLockInfo)
        if info.Locked && time.Now().After(info.Timeout) {
            log.Printf("[Timeout] Unlocking expired message: %s", messageID)

            // Broadcast unlock
            s.BroadcastUnlock(context.Background(), messageID)

            // Remove lock entry
            s.messageLocks.Delete(messageID)

            // Notify all nodes to retry processing this message
            broadcastData := broadCastInput{
                Type: message,
                Msg:  []byte(fmt.Sprintf("retry:%s", messageID)),
            }

            bin, _ := json.Marshal(broadcastData)
            s.Op.Broadcast(context.Background(), bin)
        }
    }
}

//helper function - function ensures that only the leader node is responsible for broadcasting unlock requests:
func (s *server) broadcastUnlock(ctx context.Context, messageID string) {
    // Ensure only the leader sends the unlock request
    if !s.Op.IsLeader() {
        log.Printf("[Unlock] Skipping unlock broadcast. Only the leader handles unlocks.")
        return
    }

    broadcastData := broadCastInput{
        Type: message,
        Msg:  []byte(fmt.Sprintf("unlock:%s", messageID)),
    }

    bin, _ := json.Marshal(broadcastData)
    s.Op.Broadcast(ctx, bin)

    // Clean up local state
    s.messageLocks.Delete(messageID)
    if timer, ok := s.timeoutTimers.Load(messageID); ok {
        timer.(*time.Timer).Stop()
        s.timeoutTimers.Delete(messageID)
    }

    log.Printf("[Unlock] Leader node unlocked message: %s", messageID)
}

//helper function -  checks if the current node is the leader node in your pub/sub system.
func (s *server) IsLeader() bool {
    return s.Op.ID() == s.Op.GetLeaderID() // Compare current node ID with leader ID
}


//helper function - asks the leader node for messages
func (s *server) requestMessageFromLeader(topicID string) (*pb.Message, error) {
    // Simulated request to leader (replace with actual leader communication)
    log.Printf("[Leader] Requesting message for topic: %s", topicID)
    return nil, status.Error(codes.NotFound, "no messages available from leader")
}


func (s *server) ExtendVisibilityTimeout(messageID string, subscriberID string) error {
	// Non-leader nodes should not modify state directly
	if !s.IsLeader() {
		return status.Error(codes.PermissionDenied, "only the leader can extend visibility timeout")
	}

	value, exists := s.messageLocks.Load(messageID)
	if !exists {
		return status.Error(codes.NotFound, "message not locked")
	}

	info, ok := value.(VisibilityInfo)
	if !ok || info.SubscriberID != subscriberID {
		return status.Error(codes.PermissionDenied, "message locked by another subscriber")
	}

	// Leader extends visibility timeout
	newExpiresAt := time.Now().Add(visibilityTimeout)
	info.ExpiresAt = newExpiresAt
	s.messageLocks.Store(messageID, info)

	// Create broadcast message
	broadcastMsg := broadCastInput{
		Type: "extend",
		Msg:  fmt.Sprintf("%s:%d", messageID, visibilityTimeout.Seconds()),
	}
	msgBytes, _ := json.Marshal(broadcastMsg)

	// Leader broadcasts the new timeout
	s.Op.Broadcast(context.TODO(), msgBytes)

	log.Printf("[ExtendTimeout] Leader approved timeout extension for message: %s", messageID)
	return nil
}

//helper function - listen for the leader's broadcast and apply the timeout only when received.
func (s *server) HandleTimeoutExtension(msg broadCastInput) {
	// Parse message
	parts := strings.Split(string(msg.Msg), ":")
	if len(parts) != 2 {
		log.Println("[HandleTimeoutExtension] Invalid message format")
		return
	}

	messageID := parts[0]
	timeoutSeconds, err := strconv.Atoi(parts[1])
	if err != nil {
		log.Println("[HandleTimeoutExtension] Failed to parse timeout value")
		return
	}

	// Apply the extended timeout
	value, exists := s.messageLocks.Load(messageID)
	if !exists {
		log.Printf("[HandleTimeoutExtension] Message %s not found in locks", messageID)
		return
	}

	info, ok := value.(VisibilityInfo)
	if !ok {
		log.Println("[HandleTimeoutExtension] Invalid visibility info")
		return
	}

	info.ExpiresAt = time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
	s.messageLocks.Store(messageID, info)

	log.Printf("[HandleTimeoutExtension] Applied timeout extension for message: %s", messageID)
}



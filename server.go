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

// MessageLockInfo tracks lock state across nodes
type MessageLockInfo struct {
	Timeout      time.Time
	Locked       bool
	NodeID       string
	SubscriberID string            // Added to track which subscriber has the lock
	LockHolders  map[string]bool   // Track which nodes have acknowledged the lock
}

// validateTopicSubscription checks if subscription exists in memory
func (s *server) validateTopicSubscription(subscriptionID string) (*pb.Subscription, error) {
	if val, ok := s.subscriptions.Load(subscriptionID); ok {
		return val.(*pb.Subscription), nil
	}
	// Request subscription details from the leader - using topicsub message type
	broadcastData := broadCastInput{
		Type: topicsub,
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
	lockInfo := MessageLockInfo{
		Timeout:      time.Now().Add(timeout),
		Locked:       true,
		NodeID:       s.Op.ID(),
		SubscriberID: subscriberID,
		LockHolders:  make(map[string]bool),
	}
	// Store initial lock info before broadcasting
	_, loaded := s.messageLocks.LoadOrStore(messageID, lockInfo)
	if loaded {
		return fmt.Errorf("message already locked by another node")
	}
	// Broadcast lock request - format matching handleBroadcastedMsg
	broadcastData := broadCastInput{
		Type: message,
		Msg:  []byte(fmt.Sprintf("lock:%s:%d:%s", messageID, int(timeout.Seconds()), subscriberID)),
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

// handleMessageTimeout ensures that if a node crashes while holding a lock, 
// other nodes can unlock the message and allow it to be processed again.
func (s *server) handleMessageTimeout(messageID string) {
	if lockInfo, ok := s.messageLocks.Load(messageID); ok {
		info := lockInfo.(MessageLockInfo)
		if info.Locked && time.Now().After(info.Timeout) {
			log.Printf("[Timeout] Unlocking expired message: %s", messageID)
			// Broadcast unlock
			s.broadcastUnlock(context.Background(), messageID)
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

// broadcastUnlock ensures that only the leader node is responsible for broadcasting unlock requests
func (s *server) broadcastUnlock(ctx context.Context, messageID string) {
	// Ensure only the leader sends the unlock request
	if !s.IsLeader() {
		log.Printf("[Unlock] Skipping unlock broadcast. Only the leader handles unlocks.")
		return
	}
	// Format matching handleBroadcastedMsg
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

// IsLeader checks if the current node is the leader node in the pub/sub system
func (s *server) IsLeader() bool {
	return s.Op.ID() == s.Op.GetLeaderID() // Compare current node ID with leader ID
}

// requestMessageFromLeader asks the leader node for messages
func (s *server) requestMessageFromLeader(topicID string) (*pb.Message, error) {
	// Use the message type with proper format for requesting a message
	broadcastData := broadCastInput{
		Type: message,
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
	// Non-leader nodes should not modify state directly
	if !s.IsLeader() {
		return status.Error(codes.PermissionDenied, "only the leader can extend visibility timeout")
	}
	value, exists := s.messageLocks.Load(messageID)
	if !exists {
		return status.Error(codes.NotFound, "message not locked")
	}
	info, ok := value.(MessageLockInfo)
	if !ok || info.SubscriberID != subscriberID {
		return status.Error(codes.PermissionDenied, "message locked by another subscriber")
	}
	// Leader extends visibility timeout
	newExpiresAt := time.Now().Add(visibilityTimeout)
	info.Timeout = newExpiresAt
	s.messageLocks.Store(messageID, info)
	// Create broadcast message - format matching handleBroadcastedMsg
	broadcastData := broadCastInput{
		Type: message,
		Msg:  []byte(fmt.Sprintf("extend:%s:%d", messageID, int(visibilityTimeout.Seconds()))),
	}
	msgBytes, _ := json.Marshal(broadcastData)
	// Leader broadcasts the new timeout
	s.Op.Broadcast(context.TODO(), msgBytes)
	log.Printf("[ExtendTimeout] Leader approved timeout extension for message: %s", messageID)
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

		// Add other message types as needed
	}

	return nil
}

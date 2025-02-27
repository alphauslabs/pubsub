// helpers.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
		NodeID:       s.Op.HostPort(),
		SubscriberID: subscriberID,
		LockHolders:  make(map[string]bool),
	}

	// Check if message exists in storage
	_, err := s.Storage.GetMessage(messageID)
	if err != nil {
		return err
	}

	// Store lock information
	lockInfo.LockHolders[s.Op.HostPort()] = true
	s.MessageLocks.Store(messageID, lockInfo)

	// Broadcast lock request
	broadcastData := broadcast.BroadCastInput{
		Type: broadcast.MsgEvent,
		Msg:  []byte(fmt.Sprintf("lock:%s:%d:%s:%s", messageID, int(timeout.Seconds()), subscriberID, s.Op.HostPort())),
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
	// todo: Nice idea, but what if we have to be strict, like all nodes (instead of majority) must acknowledge the lock?
	// Check consensus mode from configuration (could be stored in server struct)
	consensusMode := s.ConsensusMode // Add this field to server struct: "majority" or "all"

	// Determine required acknowledgments based on mode
	requiredAcks := len(out)/2 + 1 // Default to majority
	if consensusMode == "all" {
		requiredAcks = len(out) // Require all nodes
	}

	// Check if we got enough acknowledgments
	if successCount < requiredAcks {
		s.MessageLocks.Delete(messageID)
		log.Printf("[Lock] Failed to acquire lock for message %s: got %d/%d required acknowledgments",
			messageID, successCount, requiredAcks)
		return fmt.Errorf("failed to acquire lock across %s of nodes (got %d/%d)",
			consensusMode, successCount, requiredAcks)
	}

	log.Printf("[Lock] Successfully acquired lock for message %s with %d/%d acknowledgments",
		messageID, successCount, len(out))

	// Start timeout timer
	timer := time.NewTimer(timeout)
	s.MessageTimer.Store(messageID, timer)

	go func() {
		<-timer.C
		s.handleMessageTimeout(messageID)
	}()

	return nil
}

// todo: not sure if we only allow the node that locked the message to unlock it, what if every node will just unlock it by themselves without broadcasting.
// todo: if the locker node will crash, no one will broadcast to unlock?
func (s *server) handleMessageTimeout(messageID string) {
	if lockInfo, ok := s.MessageLocks.Load(messageID); ok {
		info := lockInfo.(broadcast.MessageLockInfo)
		// Any node can unlock a message if its timeout has expired
		if info.Locked && time.Now().After(info.Timeout) {
			log.Printf("[Timeout] Node %s detected expired lock for message %s (owned by %s)",
				s.Op.HostPort(), messageID, info.NodeID)
			s.broadcastUnlock(context.Background(), messageID)
		}
	}
}

func (s *server) broadcastUnlock(ctx context.Context, messageID string) {
	// Get lock info before unlocking
	var originalOwner string
	if lockInfo, ok := s.MessageLocks.Load(messageID); ok {
		info := lockInfo.(broadcast.MessageLockInfo)
		originalOwner = info.NodeID
	}

	// Broadcast unlock with reason (timeout or explicit unlock)
	reason := "timeout"
	if originalOwner == s.Op.HostPort() {
		reason = "owner"
	}

	broadcastData := broadcast.BroadCastInput{
		Type: broadcast.MsgEvent,
		Msg:  []byte(fmt.Sprintf("unlock:%s:%s:%s", messageID, s.Op.HostPort(), reason)),
	}
	bin, _ := json.Marshal(broadcastData)
	s.Op.Broadcast(ctx, bin)

	// Clean up local state
	s.MessageLocks.Delete(messageID)
	if timer, ok := s.MessageTimer.Load(messageID); ok {
		timer.(*time.Timer).Stop()
		s.MessageTimer.Delete(messageID)
	}

	log.Printf("[Unlock] Node %s unlocked message: %s (original owner: %s, reason: %s)",
		s.Op.HostPort(), messageID, originalOwner, reason)
}

// ExtendVisibilityTimeout extends the visibility timeout for a message
func (s *server) ExtendVisibilityTimeout(messageID string, subscriberID string, visibilityTimeout time.Duration) error {
	value, exists := s.MessageLocks.Load(messageID)
	if !exists {
		return status.Error(codes.NotFound, "message not locked")
	}

	info, ok := value.(broadcast.MessageLockInfo)
	if !ok {
		return status.Error(codes.Internal, "invalid lock info")
	}

	// Check if this node owns the lock
	if info.NodeID != s.Op.HostPort() {
		return status.Error(codes.PermissionDenied, "only the lock owner can extend timeout")
	}

	// Check subscriber ID
	if info.SubscriberID != subscriberID { // todo: what does this mean?
		return status.Error(codes.PermissionDenied, "message locked by another subscriber")
	}

	// Extend visibility timeout
	newExpiresAt := time.Now().Add(visibilityTimeout)
	info.Timeout = newExpiresAt
	s.MessageLocks.Store(messageID, info)

	// Create broadcast message
	broadcastData := broadcast.BroadCastInput{
		Type: broadcast.MsgEvent,
		Msg:  []byte(fmt.Sprintf("extend:%s:%d:%s", messageID, int(visibilityTimeout.Seconds()), s.Op.HostPort())),
	}
	msgBytes, _ := json.Marshal(broadcastData)

	// Broadcast new timeout to all nodes
	s.Op.Broadcast(context.TODO(), msgBytes)
	log.Printf("[ExtendTimeout] Node %s extended timeout for message: %s", s.Op.HostPort(), messageID)

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
		lockInfo := broadcast.MessageLockInfo{
			Timeout:      time.Now().Add(time.Duration(timeoutSeconds) * time.Second),
			Locked:       true,
			NodeID:       s.Op.HostPort(), // This is the current node
			SubscriberID: subscriberID,
			LockHolders:  make(map[string]bool),
		}
		s.MessageLocks.Store(messageID, lockInfo)

	case "unlock":
		messageID := string(msgData)
		s.MessageLocks.Delete(messageID)

	}

	return nil
}

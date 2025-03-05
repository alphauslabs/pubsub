// helpers.go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/alphauslabs/pubsub/handlers"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/golang/glog"
)

// Checks if the topic subscription is correct, and exists in the memory
func (s *server) checkIfTopicSubscriptionIsCorrect(topicID, subscription string) error {
	glog.Infof("[Subscribe] Checking if subscription exists for topic: %s", topicID)
	subs, err := storage.GetSubscribtionsForTopic(topicID)

	if err != nil {
		glog.Infof("[Subscribe] Topic %s not found in storage", topicID)
		return status.Errorf(codes.NotFound, "Topic %s not found", topicID)
	}

	glog.Infof("[Subscribe] Found subscriptions for topic %s: %v", topicID, subs)

	// Check if the provided subscription ID exists in the topic's subscriptions
	found := false
	for _, sub := range subs {
		if sub.Name == subscription {
			found = true
			glog.Infof("[Subscribe] Subscription %s found in topic %s", subscription, topicID)
			break
		}
	}

	if !found {
		glog.Infof("[Subscribe] Subscription %s not found in topic %s", subscription, topicID)
		return status.Errorf(codes.NotFound, "Subscription %s not found", subscription)
	}

	return nil
}

// / broadcastLock handles distributed locking
func (s *server) broadcastLock(ctx context.Context, messageId, topicId string) error {
	// Check if message exists in storage
	_, err := storage.GetMessage(messageId)
	if err != nil {
		return err
	}

	// Broadcast lock request
	broadcastData := handlers.BroadCastInput{
		Type: handlers.MsgEvent,
		Msg:  []byte(fmt.Sprintf("lock:%s:%s", messageId, topicId)),
	}
	bin, _ := json.Marshal(broadcastData)
	out := s.Op.Broadcast(ctx, bin)

	for _, v := range out {
		if v.Error != nil {
			glog.Errorf("[Lock] Error broadcasting lock request: %v", v.Error)
		}
	}

	// Need majority for consensus
	// todo: Nice idea, but what if we have to be strict, like all nodes (instead of majority) must acknowledge the lock?
	// Check consensus mode from configuration (could be stored in server struct)
	// consensusMode := s.ConsensusMode // Add this field to server struct: "majority" or "all"

	// // Determine required acknowledgments based on mode
	// requiredAcks := len(out)/2 + 1 // Default to majority
	// if consensusMode == "all" {
	// 	requiredAcks = len(out) // Require all nodes
	// }

	// // Check if we got enough acknowledgments
	// if successCount < requiredAcks {
	// 	s.MessageLocks.Delete(messageID)
	// 	glog.Infof("[Lock] Failed to acquire lock for message %s: got %d/%d required acknowledgments",
	// 		messageID, successCount, requiredAcks)
	// 	return fmt.Errorf("failed to acquire lock across %s of nodes (got %d/%d)",
	// 		consensusMode, successCount, requiredAcks)
	// }

	// glog.Infof("[Lock] Successfully acquired lock for message %s with %d/%d acknowledgments",
	// 	messageID, successCount, len(out))

	// // Start timeout timer
	// timer := time.NewTimer(timeout)
	// s.MessageTimer.Store(messageID, timer)

	// go func() {
	// 	<-timer.C
	// 	s.handleMessageTimeout(messageID)
	// }()

	return nil
}

// todo: not sure if we only allow the node that locked the message to unlock it, what if every node will just unlock it by themselves without broadcasting.
// todo: if the locker node will crash, no one will broadcast to unlock?
func (s *server) handleMessageTimeout(messageID string) {
	if lockInfo, ok := s.MessageLocks.Load(messageID); ok {
		info := lockInfo.(handlers.MessageLockInfo)
		// Any node can unlock a message if its timeout has expired
		if info.Locked && time.Now().After(info.Timeout) {
			glog.Infof("[Timeout] Node %s detected expired lock for message %s (owned by %s)",
				s.Op.HostPort(), messageID, info.NodeID)
			//s.broadcastUnlock(context.Background(), messageID)
			s.localUnlock(messageID)
		}
	}
}
func (s *server) localUnlock(messageID string) {
	// Remove from local storage
	s.MessageLocks.Delete(messageID)

	// Stop and remove timer
	if timer, ok := s.MessageTimer.Load(messageID); ok {
		timer.(*time.Timer).Stop()
		s.MessageTimer.Delete(messageID)
	}

	glog.Infof("[Unlock] Node %s locally unlocked message %s", s.Op.HostPort(), messageID)
}

// func (s *server) broadcastUnlock(ctx context.Context, messageID string) {
// 	// Remove from local storage first
// 	s.MessageLocks.Delete(messageID)

// 	// Stop and remove timer
// 	if timer, ok := s.MessageTimer.Load(messageID); ok {
// 		timer.(*time.Timer).Stop()
// 		s.MessageTimer.Delete(messageID)
// 	}

// 	// Broadcast unlock request to all nodes
// 	reason := "request"

// 	broadcastData := handlers.BroadCastInput{
// 		Type: handlers.MsgEvent,
// 		Msg:  []byte(fmt.Sprintf("unlock:%s:%s:%s", messageID, s.Op.HostPort(), reason)),
// 	}
// 	bin, _ := json.Marshal(broadcastData)
// 	s.Op.Broadcast(ctx, bin)

// 	// Clean up local state
// 	s.MessageLocks.Delete(messageID)
// 	if timer, ok := s.MessageTimer.Load(messageID); ok {
// 		timer.(*time.Timer).Stop()
// 		s.MessageTimer.Delete(messageID)
// 	}

// 	glog.Infof("[Unlock] Node %s broadcasted unlock for message %s ", s.Op.HostPort(), messageID)
// }

// ExtendVisibilityTimeout extends the visibility timeout for a message
func (s *server) ExtendVisibilityTimeout(messageID string, subscriberID string, visibilityTimeout time.Duration) error {
	value, exists := s.MessageLocks.Load(messageID)
	if !exists {
		return status.Error(codes.NotFound, "message not locked")
	}

	info, ok := value.(handlers.MessageLockInfo)
	if !ok {
		return status.Error(codes.Internal, "invalid lock info")
	}

	// Retrieve subscription to check AutoExtend
	sub, err := s.GetSubscription(context.TODO(), &pb.GetSubscriptionRequest{Name: subscriberID})
	if err != nil {
		return status.Error(codes.NotFound, "subscription not found")
	}

	// Log AutoExtend status
	glog.Infof("[ExtendVisibility] Subscription %s has AutoExtend: %v", sub.Name, sub.Autoextend)

	// If AutoExtend is disabled, allow manual extension
	if !sub.Autoextend {
		glog.Infof("[ExtendVisibility] Autoextend is disabled. Allowing manual extension.")
	}

	// Extend visibility timeout
	newExpiresAt := time.Now().Add(visibilityTimeout)
	info.Timeout = newExpiresAt
	s.MessageLocks.Store(messageID, info)

	// Broadcast new timeout
	broadcastData := handlers.BroadCastInput{
		Type: handlers.MsgEvent,
		Msg:  []byte(fmt.Sprintf("extend:%s:%d:%s", messageID, int(visibilityTimeout.Seconds()), s.Op.HostPort())),
	}
	msgBytes, _ := json.Marshal(broadcastData)

	s.Op.Broadcast(context.TODO(), msgBytes)
	glog.Infof("[ExtendVisibility] Visibility timeout extended for message: %s by subscriber: %s", messageID, subscriberID)

	return nil
}

// AutoExtendTimeout automatically extends the visibility timeout if autoextend is enabled
// func (s *server) AutoExtendTimeout(messageID string, subscriberID string, visibilityTimeout time.Duration) {
// 	value, exists := s.MessageLocks.Load(messageID)
// 	if !exists {
// 		glog.Infof("[AutoExtend] Message %s not found or already processed", messageID)
// 		return
// 	}

// 	info, ok := value.(handlers.MessageLockInfo)
// 	if !ok {
// 		glog.Errorf("[AutoExtend] Invalid lock info for message %s", messageID)
// 		return
// 	}

// 	// Check if this node owns the lock
// 	if info.NodeID != s.Op.HostPort() {
// 		glog.Infof("[AutoExtend] Skipping extension for message %s (not owned by this node)", messageID)
// 		return
// 	}

// 	// Ensure only autoextend-enabled messages get extended

// 	sub, err := storage.GetSubscribtionsForTopic(subscriberID)
// 	if err != nil {
// 		glog.Errorf("[AutoExtend] Failed to fetch subscription %s: %v", subscriberID, err)
// 		return
// 	}

// 	if !sub[0].Autoextend {
// 		glog.Infof("[AutoExtend] Subscription %s does not have autoextend enabled", subscriberID)
// 		return
// 	}
// 	// sub, err := storage.GetSubscription(subscriberID) // todo: commented to fix errros, please uncomment if needed
// 	// if err != nil {
// 	// 	glog.Errorf("[AutoExtend] Failed to fetch subscription %s: %v", subscriberID, err)
// 	// 	return
// 	// }
// 	// if !sub.Autoextend {
// 	// 	glog.Infof("[AutoExtend] Subscription %s does not have autoextend enabled", subscriberID)
// 	// 	return
// 	//

// 	// Extend visibility timeout
// 	newExpiresAt := time.Now().Add(visibilityTimeout)
// 	info.Timeout = newExpiresAt
// 	s.MessageLocks.Store(messageID, info)

// 	// Broadcast new timeout
// 	broadcastData := handlers.BroadCastInput{
// 		Type: handlers.MsgEvent,
// 		Msg:  []byte(fmt.Sprintf("autoextend:%s:%d:%s", messageID, int(visibilityTimeout.Seconds()), s.Op.HostPort())),
// 	}
// 	msgBytes, _ := json.Marshal(broadcastData)

// 	// Send broadcast to all nodes
// 	s.Op.Broadcast(context.TODO(), msgBytes)
// 	glog.Infof("[AutoExtend] Node %s auto-extended timeout for message: %s", s.Op.HostPort(), messageID)
// }

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
		lockInfo := handlers.MessageLockInfo{
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

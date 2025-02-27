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

	"github.com/alphauslabs/pubsub/handlers"
	"github.com/alphauslabs/pubsub/storage"
)

// validateTopicSubscription checks if subscription exists in storage
func (s *server) validateTopicSubscription(subscriptionID string) error {
	subs, err := storage.GetSubscribtionsForTopic(subscriptionID)
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
	lockInfo := handlers.MessageLockInfo{
		Timeout:      time.Now().Add(timeout),
		Locked:       true,
		NodeID:       s.Op.HostPort(),
		SubscriberID: subscriberID,
		LockHolders:  make(map[string]bool),
	}

	// Check if message exists in storage
	_, err := storage.GetMessage(messageID)
	if err != nil {
		return err
	}

	// Store lock information
	lockInfo.LockHolders[s.Op.HostPort()] = true
	s.MessageLocks.Store(messageID, lockInfo)

	// Broadcast lock request
	broadcastData := handlers.BroadCastInput{
		Type: handlers.MsgEvent,
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
	if successCount < (len(out)/2 + 1) {
		s.MessageLocks.Delete(messageID)
		return fmt.Errorf("failed to acquire lock across majority of nodes")
	}

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
		info := lockInfo.(handlers.MessageLockInfo)
		if info.NodeID == s.Op.HostPort() && info.Locked && time.Now().After(info.Timeout) {
			s.broadcastUnlock(context.Background(), messageID)
		}
	}
}

// broadcastUnlock ensures that only the leader node is responsible for broadcasting unlock requests
func (s *server) broadcastUnlock(ctx context.Context, messageID string) {

	// Any node can broadcast an unlock
	broadcastData := handlers.BroadCastInput{
		Type: handlers.MsgEvent,
		Msg:  []byte(fmt.Sprintf("unlock:%s:%s", messageID, s.Op.HostPort())),
	}
	bin, _ := json.Marshal(broadcastData)
	s.Op.Broadcast(ctx, bin)
	// Clean up local state
	s.MessageLocks.Delete(messageID)
	if timer, ok := s.MessageLocks.Load(messageID); ok {
		timer.(*time.Timer).Stop()
		s.MessageTimer.Delete(messageID)
	}
	log.Printf("[Unlock] Node %s unlocked message: %s", s.Op.HostPort(), messageID)
}

// REQUEST MESSAGE USING BROADCAST METHOD
// func (s *server) requestMessageFromLeader(topicID string) (*pb.Message, error) {
//     broadcastData := broadcast.BroadCastInput{
//         Type: broadcast.MsgEvent,
//         Msg:  []byte(fmt.Sprintf("getmessage:%s", topicID)),
//     }

//     bin, _ := json.Marshal(broadcastData)
//     outputs := s.Op.Broadcast(context.Background(), bin)

//     // Process broadcast responses
//     for _, output := range outputs {
//         if output.Error != nil {
//             continue
//         }
//         if len(output.Reply) > 0 {
//             var message pb.Message
//             if err := json.Unmarshal(output.Reply, &message); err != nil {
//                 continue
//             }
//             return &message, nil
//         }
//     }

//     return nil, status.Error(codes.NotFound, "no messages available")
// }

//REQUEST MESSAGE USING REQUEST METHOD
// func (s *server) requestMessageFromLeader(topicID string) (*pb.Message, error) {
// 	// Use the message type with proper format for requesting a message
// 	broadcastData := broadcast.BroadCastInput{
// 		Type: broadcast.MsgEvent,
// 		Msg:  []byte(fmt.Sprintf("getmessage:%s", topicID)),
// 	}

// 	bin, _ := json.Marshal(broadcastData)
// 	resp, err := s.Op.Request(context.Background(), bin)
// 	if err != nil {
// 		return nil, err
// 	}

// 	if len(resp) == 0 {
// 		return nil, status.Error(codes.NotFound, "no messages available")
// 	}

// 	var message pb.Message
// 	if err := json.Unmarshal(resp, &message); err != nil {
// 		return nil, err
// 	}

// 	return &message, nil
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
	broadcastData := handlers.BroadCastInput{
		Type: handlers.MsgEvent,
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

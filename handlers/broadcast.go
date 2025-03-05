package handlers

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/golang/glog"
)

const (
	Message          = "message"
	Topicsub         = "topicsub"
	LeaderLiveliness = "leaderliveliness"
	MsgEvent         = "msgEvent"
	TopicDeleted     = "topicdeleted"

	// Message event types
	LockMsg   = "lock"
	UnlockMsg = "unlock"
	DeleteMsg = "delete"
	ExtendMsg = "extend"
	RetryMsg  = "retry"
)

type BroadCastInput struct {
	Type string
	Msg  []byte
}

// MessageLockInfo defines lock information structure
// Note: This should be consistent with the structure in helpers.go
type MessageLockInfo struct {
	Locked       bool
	Timeout      time.Time
	NodeID       string
	SubscriberID string          // Added to track which subscriber has the lock
	LockHolders  map[string]bool // Track which nodes have acknowledged the lock
}

var ctrlbroadcast = map[string]func(*app.PubSub, []byte) ([]byte, error){
	Message:          handleBroadcastedMsg,
	Topicsub:         handleBroadcastedTopicsub,
	MsgEvent:         handleMessageEvent, // Handles message locks, unlocks, deletes
	LeaderLiveliness: handleLeaderLiveliness,
	TopicDeleted:     handleTopicDeleted,
}

// Root handler for op.Broadcast()
func Broadcast(data any, msg []byte) ([]byte, error) {
	var in BroadCastInput
	appInstance := data.(*app.PubSub) // Ensure we're using an instance, not a type

	if err := json.Unmarshal(msg, &in); err != nil {
		return nil, err
	}

	return ctrlbroadcast[in.Type](appInstance, in.Msg)
}

func handleBroadcastedMsg(app *app.PubSub, msg []byte) ([]byte, error) {
	var message storage.Message
	if err := json.Unmarshal(msg, &message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	// Store in node queue/memory (not marking as processed yet)
	if err := storage.StoreMessage(&message); err != nil {
		return nil, fmt.Errorf("failed to store message: %w", err)
	}

	return nil, nil
}

// Handles topic-subscription updates
func handleBroadcastedTopicsub(app *app.PubSub, msg []byte) ([]byte, error) {
	var topicSubs map[string]map[string]*storage.Subscription
	if err := json.Unmarshal(msg, &topicSubs); err != nil {
		return nil, fmt.Errorf("failed to unmarshal topic-subscriptions: %w", err)
	}

	if err := storage.StoreTopicSubscriptions(topicSubs); err != nil {
		return nil, fmt.Errorf("failed to store topic-subscriptions: %w", err)
	}

	return nil, nil
}

// Handles lock/unlock/delete/extend operations separately
func handleMessageEvent(appInstance *app.PubSub, msg []byte) ([]byte, error) {
	glog.Info("[MessageEvent] Received message event:", string(msg))
	parts := strings.Split(string(msg), ":")
	if len(parts) < 2 {
		glog.Info("[Error] Invalid message event format:", msg)
		return nil, fmt.Errorf("invalid message event format")
	}

	messageType := parts[0]
	messageID := parts[1]

	eventHandlers := map[string]func(*app.PubSub, string, []string) ([]byte, error){
		LockMsg:   handleLockMsg,
		UnlockMsg: handleUnlockMsg,
		DeleteMsg: handleDeleteMsg,
		ExtendMsg: handleExtendMsg,
		RetryMsg:  handleRetryMsg,
	}

	handler, exists := eventHandlers[messageType]
	if !exists {
		glog.Info("[Error] Unknown message event type:", messageType)
		return nil, fmt.Errorf("unknown message event: %s", messageType)
	}

	glog.Info("[MessageEvent] Processing event:", messageType, "for message ID:", messageID)
	return handler(appInstance, messageID, parts[2:])
}

// Message event handlers
func handleLockMsg(app *app.PubSub, messageID string, params []string) ([]byte, error) {
	glog.Info("[Lock] Attempting to lock message:", messageID, "Params:", params)
	// Check if already locked
	// if existingLock, exists := app.MessageLocks.Load(messageID); exists {
	// 	info := existingLock.(MessageLockInfo)

	// 	// If lock is expired, allow new lock
	// 	if time.Now().After(info.Timeout) {
	// 		glog.Info("[Lock] Previous lock expired, allowing new lock.")
	// 		// Continue with new lock
	// 	} else if info.NodeID == requestingNodeID {
	// 		// Same node is refreshing its lock, allow it
	// 		info.LockHolders[app.NodeID] = true
	// 		app.MessageLocks.Store(messageID, info)
	// 		glog.Info("[Lock] Lock refreshed by same node:", requestingNodeID)
	// 		return nil, nil
	// 	} else {
	// 		// Different node has a valid lock, reject
	// 		glog.Info("[Lock] Message already locked by another node:", info.NodeID)
	// 		return nil, fmt.Errorf("message already locked by another node")
	// 	}
	// }

	if len(params) < 1 {
		glog.Error("[Lock] Subscription name missing in params")
		return nil, fmt.Errorf("subscription name missing in params")
	}
	subscriptionName := params[0]

	// retrieve the message from storage
	msg, err := storage.GetMessage(messageID)
	if err != nil {
		glog.Errorf("[Lock] Error retrieving message %s: %v", messageID, err)
		return nil, err
	}
	if msg == nil {
		glog.Errorf("[Lock] Message %s not found", messageID)
		return nil, fmt.Errorf("message not found")
	}

	// Retrieve the topic associated with the message
	if msg.Topic == "" {
		glog.Errorf("[Lock] Message %s does not have an associated topic", messageID)
		return nil, fmt.Errorf("message %s does not have an associated topic", messageID)
	}

	subscriptionsSlice, err := storage.GetSubscribtionsForTopic(msg.Topic)
	if err != nil {
		glog.Errorf("[Lock] Failed to retrieve subscriptions for topic %s: %v", msg.Topic, err)
		return nil, err
	}

	// Convert slice to map for quick lookup
	subscriptionsMap := make(map[string]*storage.Subscription)
	for _, sub := range subscriptionsSlice {
		subscriptionsMap[sub.Subscription.Name] = sub //
	}

	// Check if this subscription enabled autoextend
	autoExtend := false
	sub, exists := subscriptionsMap[subscriptionName]
	if exists && sub != nil && sub.Subscription != nil && sub.Subscription.Autoextend {
		autoExtend = true
	}

	// Lock the message atomically
	atomic.StoreInt32(&msg.Locked, 1)

	//  Update msg.Age and set autoextend only if the subscription has it enabled
	msg.Mu.Lock()
	msg.Age = time.Now().UTC()

	if autoExtend {
		atomic.StoreInt32(&msg.AutoExtend, 1) //
	} else {
		atomic.StoreInt32(&msg.AutoExtend, 0) //
	}

	msg.Mu.Unlock()

	// Each node maintains its own timer
	// Create new lock
	// lockInfo := MessageLockInfo{
	// 	Locked:       true,
	// 	Timeout:      time.Now().Add(time.Duration(timeoutSeconds) * time.Second),
	// 	NodeID:       requestingNodeID,
	// 	SubscriberID: subscriberID,
	// 	LockHolders:  make(map[string]bool),
	// }

	// // Mark this node as acknowledging the lock
	// lockInfo.LockHolders[app.NodeID] = true

	glog.Infof("[Lock] Message %s locked successfully for Subscription: %s with AutoExtend: %t", messageID, subscriptionName, autoExtend)
	// app.MessageLocks.Store(messageID, lockInfo)

	// // Set up a local timer to clear the lock when it expires
	// time.AfterFunc(time.Duration(timeoutSeconds)*time.Second, func() {
	// 	if lock, exists := app.MessageLocks.Load(messageID); exists {
	// 		info := lock.(MessageLockInfo)
	// 		if info.NodeID == requestingNodeID && time.Now().After(info.Timeout) {
	// 			app.MessageLocks.Delete(messageID)
	// 			glog.Infof("[Lock] Timer expired, node %s automatically released local lock for message: %s",
	// 				app.NodeID, messageID)
	// 		}
	// 	}
	// })
	// glog.Info("[Lock] Message locked successfully by node:", requestingNodeID)
	return nil, nil
}

func handleUnlockMsg(app *app.PubSub, messageID string, params []string) ([]byte, error) {
	glog.Info("[Unlock] Attempting to unlock message:", messageID)
	if len(params) < 2 {
		return nil, fmt.Errorf("invalid unlock parameters, expected at least [nodeID, reason]")
	}

	unlockingNodeID := params[0]
	unlockReason := params[1]

	// Check if the message is locked
	if lockInfo, exists := app.MessageLocks.Load(messageID); exists {
		info := lockInfo.(MessageLockInfo)

		// Allow unlock if:
		// 1. Lock has expired (any node can unlock)
		// 2. Unlock is requested (any node can request unlock)
		if time.Now().After(info.Timeout) || unlockReason == "request" {
			app.MessageLocks.Delete(messageID)
			// if timer, ok := app.MessageTimer.Load(messageID); ok {
			// 	timer.(*time.Timer).Stop()
			// 	app.MessageTimer.Delete(messageID)
			// }
			glog.Infof("[Unlock] Node %s acknowledged unlock for message: %s (reason: %s, node: %s)",
				app.NodeID, messageID, unlockReason, unlockingNodeID)
		} else {
			glog.Infof("[Unlock] Rejected unlock from node %s for message: %s - lock not expired yet (expires at: %v)",
				unlockingNodeID, messageID, info.Timeout)
			return nil, fmt.Errorf("lock not expired yet, current time: %v, expires at: %v",
				time.Now(), info.Timeout)
		}
	}

	return nil, nil
}

func handleDeleteMsg(app *app.PubSub, messageID string, _ []string) ([]byte, error) {
	glog.Info("[Delete] Removing message:", messageID)

	m, err := storage.GetMessage(messageID)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("message not found")
	}

	// Delete from storage
	atomic.StoreInt32(&m.Deleted, 1)

	glog.Info("[Delete] Message successfully removed:", messageID)
	return nil, nil
}

func handleExtendMsg(app *app.PubSub, messageID string, params []string) ([]byte, error) {
	if len(params) < 2 {
		return nil, fmt.Errorf("missing parameters for extend message")
	}

	timeoutSeconds, err := strconv.Atoi(params[0])
	if err != nil {
		return nil, err
	}

	extendingNodeID := params[1]

	if lockInfo, ok := app.MessageLocks.Load(messageID); ok {
		info := lockInfo.(MessageLockInfo)

		// Only update if the request comes from the lock owner
		if info.NodeID == extendingNodeID {
			info.Timeout = time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
			app.MessageLocks.Store(messageID, info)
			glog.Infof("[Extend] Message %s timeout extended by node %s", messageID, extendingNodeID)
		} else {
			glog.Infof("[Extend] Rejected extend from non-owner node %s for message: %s", extendingNodeID, messageID)
			return nil, fmt.Errorf("only lock owner can extend timeout")
		}
	}

	return nil, nil
}

func handleRetryMsg(app *app.PubSub, messageID string, params []string) ([]byte, error) {
	if len(params) < 1 {
		return nil, fmt.Errorf("invalid retry parameters")
	}

	retryNodeID := params[0]

	app.MessageLocks.Delete(messageID)
	glog.Infof("[Retry] Message %s is now available again (unlocked by node %s)", messageID, retryNodeID)

	return nil, nil
}

func handleLeaderLiveliness(app *app.PubSub, msg []byte) ([]byte, error) {
	// Handle leader liveliness messages
	m := string(msg)
	if strings.HasPrefix(m, "1") {
		app.LeaderActive.On()
	}
	return nil, nil
}

// Handle topic deletion
func handleTopicDeleted(app *app.PubSub, msg []byte) ([]byte, error) {
	topicName := string(msg)
	glog.Infof("[Delete] Received topic deletion notification for topic: %s", topicName)

	// Remove from memory
	if err := storage.RemoveTopic(topicName); err != nil {
		glog.Infof("[Delete] Error removing topic from memory: %v", err)
		return nil, fmt.Errorf("failed to remove topic from memory: %w", err)
	}

	glog.Infof("[Delete] Successfully removed topic %s from memory", topicName)
	return nil, nil
}

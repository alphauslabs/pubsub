package handlers

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"

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
)

type BroadCastInput struct {
	Type string
	Msg  []byte
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
	parts := strings.Split(string(msg), ":")
	if len(parts) < 3 {
		glog.Info("[Error] Invalid message event format:", msg)
		return nil, fmt.Errorf("invalid message event format")
	}

	msgType := parts[0]
	msgId := parts[1]
	subId := parts[2]

	eventHandlers := map[string]func(*app.PubSub, string, string) ([]byte, error){
		LockMsg:   handleLockMsg,
		UnlockMsg: handleUnlockMsg,
		DeleteMsg: handleDeleteMsg,
		ExtendMsg: handleExtendMsg,
	}

	handler, exists := eventHandlers[msgType]
	if !exists {
		glog.Info("[Error] Unknown message event type:", msgType)
		return nil, fmt.Errorf("unknown message event: %s", msgType)
	}

	return handler(appInstance, msgId, subId)
}

// Message event handlers
func handleLockMsg(app *app.PubSub, messageID string, subId string) ([]byte, error) {
	glog.Info("[Lock] Attempting to lock message:", messageID, " for subscription:", subId)

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

	// Retrieve subscriptions for the topic
	subscriptionsSlice, err := storage.GetSubscribtionsForTopic(msg.Topic)
	if err != nil {
		glog.Errorf("[Lock] Failed to retrieve subscriptions for topic %s: %v", msg.Topic, err)
		return nil, err
	}

	// Convert slice to map for quick lookup
	subscriptionsMap := make(map[string]*storage.Subscription)
	for _, sub := range subscriptionsSlice {
		subscriptionsMap[sub.Subscription.Name] = sub
	}

	// Check if this subscription enabled autoextend
	// autoExtend := false
	// if sub, exists := subscriptionsMap[subscriptionName]; exists && sub.Subscription.Autoextend {
	// 	autoExtend = true
	// }

	// todo: set autoextend or not
	msg.Subscriptions[subId].Lock()
	msg.Mu.Lock()
	msg.Subscriptions[subId].RenewAge()
	msg.Mu.Unlock()
	glog.Infof("[Lock] Message %s locked successfully for Subscription: %s", messageID, subId)

	return nil, nil
}

func handleUnlockMsg(app *app.PubSub, messageID, subId string) ([]byte, error) {
	// todo:
	return nil, nil
}

func handleDeleteMsg(app *app.PubSub, messageID string, subId string) ([]byte, error) {
	glog.Info("[Delete] Removing message:", messageID)

	m, err := storage.GetMessage(messageID)
	if err != nil {
		return nil, err
	}
	if m == nil {
		return nil, fmt.Errorf("message not found")
	}

	// Delete from storage
	atomic.StoreInt32(&m.Subscriptions[""].Deleted, 1)

	glog.Info("[Delete] Message successfully removed:", messageID)
	return nil, nil
}

func handleExtendMsg(app *app.PubSub, messageID string, subId string) ([]byte, error) {
	// todo:
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

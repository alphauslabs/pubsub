package handlers

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/alphauslabs/pubsub/utils"
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
	if len(parts) < 4 {
		glog.Info("[Error] Invalid message event format:", msg)
		return nil, fmt.Errorf("invalid message event format")
	}

	msgType := parts[0]
	msgId := parts[1]
	subId := parts[2]
	topic := parts[3]

	eventHandlers := map[string]func(*app.PubSub, string, string, string) ([]byte, error){
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

	return handler(appInstance, msgId, subId, topic)
}

func handleLockMsg(app *app.PubSub, messageID string, subId, topic string) ([]byte, error) {
	// retrieve the message from storage
	msg := storage.GetMessage(messageID, topic)
	if msg == nil {
		return nil, fmt.Errorf("message %s not found/available/locked", messageID)
	}

	msg.Mu.Lock()
	defer msg.Mu.Unlock()

	if msg.IsFinalDeleted() {
		return nil, fmt.Errorf("message %s already deleted", messageID)
	}

	msg.Subscriptions[subId].Mu.RLock()
	if msg.Subscriptions[subId].IsDeleted() {
		glog.Infof("[broadcast-handlelock] Message %s already deleted for sub=%s", messageID, subId)
		msg.Subscriptions[subId].Mu.RUnlock()
		return nil, fmt.Errorf("message %s already deleted for sub=%s", messageID, subId)
	}

	if msg.Subscriptions[subId].IsLocked() {
		glog.Infof("[broadcast-handlelock] Message %s already locked for sub=%s", messageID, subId)
		msg.Subscriptions[subId].Mu.RUnlock()
		return nil, fmt.Errorf("message %s already locked for sub=%s", messageID, subId)
	}

	msg.Subscriptions[subId].Mu.RUnlock()

	// Retrieve subscriptions for the topic
	subscriptionsSlice, err := storage.GetSubscribtionsForTopic(msg.Topic)
	if err != nil {
		glog.Errorf("[broadcast-handlelock] Failed to retrieve subscriptions for topic %s: %v", msg.Topic, err)
		return nil, err
	}

	// Convert slice to map for quick lookup
	subscriptionsMap := make(map[string]*storage.Subscription)
	for _, sub := range subscriptionsSlice {
		subscriptionsMap[sub.Subscription.Name] = sub
	}

	// Check if this subscription enabled autoextend
	autoExtend := false
	if sub, exists := subscriptionsMap[subId]; exists && sub.Subscription.AutoExtend {
		autoExtend = true
	}

	msg.Subscriptions[subId].SetAutoExtend(autoExtend)
	msg.Subscriptions[subId].Lock()
	msg.Subscriptions[subId].RenewAge()

	glog.Infof("[broadcast-handlelock] Message=%s locked successfully for sub=%s", messageID, subId)
	return nil, nil
}

func handleUnlockMsg(app *app.PubSub, messageID, subId, topic string) ([]byte, error) {
	// retrieve the message from storage
	m := storage.GetMessage(messageID, topic)
	if m == nil {
		return nil, nil
	}
	m.Mu.RLock()
	defer m.Mu.RUnlock()

	m.Subscriptions[subId].Unlock()
	m.Subscriptions[subId].ClearAge()

	return nil, nil
}

func handleDeleteMsg(app *app.PubSub, messageID string, subId, topic string) ([]byte, error) {
	glog.Infof("Handle delete message event called for msg:%v, sub:%v", messageID, subId)
	m := storage.GetMessage(messageID, topic)
	if m == nil {
		return nil, nil
	}

	// Delete for this subscription
	m.Subscriptions[subId].MarkAsDeleted()
	// Update the message status in Spanner
	err := utils.UpdateMessageProcessedStatusForSub(app.Client, messageID, subId)
	if err != nil {
		glog.Errorf("[Delete] Error updating message status for sub %s: %v", subId, err)
		return nil, err
	}

	glog.Infof("Message:%v set as deleted for sub:%v", messageID, subId)
	return nil, nil
}

func handleExtendMsg(app *app.PubSub, messageID string, subId, topic string) ([]byte, error) {
	m := storage.GetMessage(messageID, topic)
	if m == nil {
		return nil, nil
	}

	m.Mu.Lock()
	m.Subscriptions[subId].RenewAge()
	m.Mu.Unlock()

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
	// Remove from memory
	if err := storage.RemoveTopic(topicName); err != nil {
		glog.Errorf("[Delete] Error removing topic from memory: %v", err)
		return nil, fmt.Errorf("failed to remove topic from memory: %w", err)
	}

	glog.Infof("[Delete] Successfully removed topic %s from memory", topicName)
	return nil, nil
}

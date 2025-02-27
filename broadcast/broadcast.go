package broadcast

import (
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/alphauslabs/pubsub/app"
)

const (
	Message  = "message"
	Topicsub = "topicsub"
	MsgEvent = "msgEvent"

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
	Message:  handleBroadcastedMsg,
	Topicsub: handleBroadcastedTopicsub,
	MsgEvent: handleMessageEvent, // Handles message locks, unlocks, deletes
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
	log.Println("[Broadcast] Received message:\n", string(msg))
	var message pb.Message
	if err := json.Unmarshal(msg, &message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	// Store in node queue/memory (not marking as processed yet)
	if err := app.Storage.StoreMessage(&message); err != nil {
		return nil, fmt.Errorf("failed to store message: %w", err)
	}

	return nil, nil
}

// Handles topic-subscription updates
func handleBroadcastedTopicsub(app *app.PubSub, msg []byte) ([]byte, error) {
	log.Println("[Broadcast] Received topic-subscriptions:\n", string(msg))
	if err := app.Storage.StoreTopicSubscriptions(msg); err != nil {
		return nil, fmt.Errorf("failed to store topic-subscriptions: %w", err)
	}

	return nil, nil
}

// Handles lock/unlock/delete/extend operations separately
func handleMessageEvent(appInstance *app.PubSub, msg []byte) ([]byte, error) {
	log.Println("[MessageEvent] Received message event:", string(msg))
	parts := strings.Split(string(msg), ":")
	if len(parts) < 2 {
		log.Println("[Error] Invalid message event format:", msg)
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
		log.Println("[Error] Unknown message event type:", messageType)
		return nil, fmt.Errorf("unknown message event: %s", messageType)
	}

	log.Println("[MessageEvent] Processing event:", messageType, "for message ID:", messageID)
	return handler(appInstance, messageID, parts[2:])
}

// Message event handlers
func handleLockMsg(app *app.PubSub, messageID string, params []string) ([]byte, error) {
	log.Println("[Lock] Attempting to lock message:", messageID, "Params:", params)
	if len(params) < 3 {
		return nil, fmt.Errorf("invalid lock parameters")
	}

	timeoutSeconds, err := strconv.Atoi(params[0])
	if err != nil {
		return nil, err
	}
	subscriberID := params[1]
	requestingNodeID := params[2]

	app.Mutex.Lock() // todo: mutex lock and unlock, might remove this if no need
	defer app.Mutex.Unlock()

	// Check if already locked
	if existingLock, exists := app.MessageLocks.Load(messageID); exists {
		info := existingLock.(MessageLockInfo)

		// If lock is expired, allow new lock
		if time.Now().After(info.Timeout) {
			log.Println("[Lock] Previous lock expired, allowing new lock.")
			// Continue with new lock
		} else if info.NodeID == requestingNodeID {
			// Same node is refreshing its lock, allow it
			info.LockHolders[app.NodeID] = true
			app.MessageLocks.Store(messageID, info)
			log.Println("[Lock] Lock refreshed by same node:", requestingNodeID)
			return nil, nil
		} else {
			// Different node has a valid lock, reject
			log.Println("[Lock] Message already locked by another node:", info.NodeID)
			return nil, fmt.Errorf("message already locked by another node")
		}
	}

	// Each node maintains its own timer
	// Create new lock
	lockInfo := MessageLockInfo{
		Locked:       true,
		Timeout:      time.Now().Add(time.Duration(timeoutSeconds) * time.Second),
		NodeID:       requestingNodeID,
		SubscriberID: subscriberID,
		LockHolders:  make(map[string]bool),
	}

	// Mark this node as acknowledging the lock
	lockInfo.LockHolders[app.NodeID] = true

	app.MessageLocks.Store(messageID, lockInfo)
	log.Println("[Lock] Message locked successfully by node:", requestingNodeID)
	return nil, nil
}

func handleUnlockMsg(app *app.PubSub, messageID string, params []string) ([]byte, error) {
	log.Println("[Unlock] Attempting to unlock message:", messageID)
	if len(params) < 1 {
		return nil, fmt.Errorf("invalid unlock parameters")
	}

	unlockingNodeID := params[0]
	app.Mutex.Lock()
	defer app.Mutex.Unlock()

	// Check if the message is locked
	if lockInfo, exists := app.MessageLocks.Load(messageID); exists {
		info := lockInfo.(MessageLockInfo)

		// Only the lock owner can unlock
		if info.NodeID == unlockingNodeID {
			app.MessageLocks.Delete(messageID)
			log.Printf("[Unlock] Node %s acknowledged unlock for message: %s", app.NodeID, messageID)
		} else {
			log.Printf("[Unlock] Rejected unlock from non-owner node %s for message: %s", unlockingNodeID, messageID)
			return nil, fmt.Errorf("only lock owner can unlock")
		}
	}

	return nil, nil
}

func handleDeleteMsg(app *app.PubSub, messageID string, _ []string) ([]byte, error) {
	log.Println("[Delete] Removing message:", messageID)
	app.Mutex.Lock()
	defer app.Mutex.Unlock()

	app.MessageLocks.Delete(messageID)
	app.MessageTimer.Delete(messageID)
	log.Println("[Delete] Message successfully removed:", messageID)
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

	app.Mutex.Lock()
	defer app.Mutex.Unlock()

	if lockInfo, ok := app.MessageLocks.Load(messageID); ok {
		info := lockInfo.(MessageLockInfo)

		// Only update if the request comes from the lock owner
		if info.NodeID == extendingNodeID {
			info.Timeout = time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
			app.MessageLocks.Store(messageID, info)
			log.Printf("[Extend] Message %s timeout extended by node %s", messageID, extendingNodeID)
		} else {
			log.Printf("[Extend] Rejected extend from non-owner node %s for message: %s", extendingNodeID, messageID)
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

	// Make the message available again for processing
	app.Mutex.Lock()
	defer app.Mutex.Unlock()

	app.MessageLocks.Delete(messageID)
	log.Printf("[Retry] Message %s is now available again (unlocked by node %s)", messageID, retryNodeID)

	return nil, nil
}

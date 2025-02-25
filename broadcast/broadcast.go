package broadcast

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"strconv"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/alphauslabs/pubsub/app"
)

const (
	message  = "message"
	topicsub = "topicsub"
	msgEvent = "msgEvent"

	// Message event types
	lockMsg   = "lock"
	unlockMsg = "unlock"
	deleteMsg = "delete"
	extendMsg = "extend"
)

type BroadCastInput struct {
	Type string
	Msg  []byte
}

type MessageLockInfo struct {
	Locked  bool
	Timeout time.Time
	NodeID  string
	SubscriberID string            // Added to track which subscriber has the lock
	LockHolders  map[string]bool   // Track which nodes have acknowledged the lock
}

var ctrlbroadcast = map[string]func(*app.PubSub, []byte) ([]byte, error){
	message:  handleBroadcastedMsg,
	topicsub: handleBroadcastedTopicsub,
	msgEvent: handleMessageEvent,       // Handles message locks, unlocks, deletes
}

// Root handler for op.Broadcast()
func Broadcast(data any, msg []byte) ([]byte, error) {
var in BroadCastInput
	app := data.(*app.PubSub)
	if err := json.Unmarshal(msg, &in); err != nil {
		return nil, err
	}
	return ctrlbroadcast[in.Type](app, in.Msg)
}

func handleBroadcastedMsg(app *app.PubSub, msg []byte) ([]byte, error) {
	log.Println("Received message:\n", string(msg))
	var message pb.Message
	if err := json.Unmarshal(msg, &message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	if err := app.Storage.StoreMessage(&message); err != nil {
		return nil, fmt.Errorf("failed to store message: %w", err)
	}

	return nil, nil
}

	// Handles topic-subscription updates
func handleBroadcastedTopicsub(app *app.PubSub, msg []byte) ([]byte, error) {
	log.Println("Received topic-subscriptions:\n", string(msg))
	if err := app.Storage.StoreTopicSubscriptions(msg); err != nil {
		return nil, fmt.Errorf("failed to store topic-subscriptions: %w", err)
	}

	return nil, nil
}


	// Handles lock/unlock/delete/extend operations separately
func handleMessageEvent(app *app.PubSub, msg []byte) ([]byte, error) {
	parts := strings.Split(string(msg), ":")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid message event format")
	}

	messageType := parts[0]
	messageID := parts[1]

		// Map message event handlers
	eventHandlers := map[string]func(*app.PubSub, string, []string) ([]byte, error){
		lockMsg:   handleLockMsg,
		unlockMsg: handleUnlockMsg,
		deleteMsg: handleDeleteMsg,
		extendMsg: handleExtendMsg,
		retryMsg:  handleRetryMsg, 
	}

	handler, exists := eventHandlers[messageType]
	if !exists {
		return nil, fmt.Errorf("unknown message event: %s", messageType)
	}

	return handler(app, messageID, parts[2:])
}


	// Message event handlers
func handleLockMsg(app *app.PubSub, messageID string, params []string) ([]byte, error) {
	if len(params) < 2 {
	  return nil, fmt.Errorf("invalid lock parameters")
	}
	  
	timeoutSeconds, err := strconv.Atoi(params[0])
	if err != nil {
	  return nil, err
	}
	subscriberID := params[1]
  
	app.Mutex.Lock()
	defer app.Mutex.Unlock()
  
	if _, exists := app.MessageLocks.Load(messageID); exists {
	  return nil, nil // Already locked
	}
	  
	lockInfo := MessageLockInfo{
	  Locked:       true,
	  Timeout:      time.Now().Add(time.Duration(timeoutSeconds) * time.Second),
	  NodeID:       app.NodeID,
	  SubscriberID: subscriberID,
	  LockHolders:  make(map[string]bool),
	}
	app.MessageLocks.Store(messageID, lockInfo)
	  
	return nil, nil
  }
	  

  func handleUnlockMsg(app *app.PubSub, messageID string, _ []string) ([]byte, error) {
	if !app.IsLeader() {
	  return nil, nil // Only leader should handle unlocks
	}
	  
	app.Mutex.Lock()
	defer app.Mutex.Unlock()
	  
	app.MessageLocks.Delete(messageID)
	return nil, nil
  }

func handleDeleteMsg(app *app.PubSub, messageID string, _ []string) ([]byte, error) {
	app.Mutex.Lock()
	defer app.Mutex.Unlock()

	app.MessageLocks.Delete(messageID)
	app.MessageQueue.Delete(messageID)
	return nil, nil
}

func handleExtendMsg(app *app.PubSub, messageID string, params []string) ([]byte, error) {
	if len(params) < 1 {
		return nil, fmt.Errorf("missing timeout parameter for extend message")
	}

	timeoutSeconds, err := strconv.Atoi(params[0])
	if err != nil {
		return nil, err
	}

	app.Mutex.Lock()
	defer app.Mutex.Unlock()

	if lockInfo, ok := app.MessageLocks.Load(messageID); ok {
		// Update timeout
		info := lockInfo.(app.MessageLockInfo)
		info.Timeout = time.Now().Add(time.Duration(timeoutSeconds) * time.Second)
		app.MessageLocks.Store(messageID, info)
	}

	return nil, nil
} 

func handleRetryMsg(app *app.PubSub, messageID string, _ []string) ([]byte, error) {
	log.Printf("[Retry] Message %s is now available again", messageID)
	return nil, nil
  }
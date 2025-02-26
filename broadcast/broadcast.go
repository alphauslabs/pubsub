//broadcast.go
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
	retryMsg =  "retry"
)

type BroadCastInput struct {
	Type string
	Msg  []byte
}

// MessageLockInfo defines lock information structure
// Note: This should be consistent with the structure in helpers.go
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
	if len(params) < 3 {
		return nil, fmt.Errorf("invalid lock parameters")
	}
		
	timeoutSeconds, err := strconv.Atoi(params[0])
	if err != nil {
		return nil, err
	}
	subscriberID := params[1]
	requestingNodeID := params[2]
		
	app.Mutex.Lock()
	defer app.Mutex.Unlock()
		
	// Check if already locked
	if existingLock, exists := app.MessageLocks.Load(messageID); exists {
		info := existingLock.(MessageLockInfo)
		
		// If lock is expired, allow new lock
		if time.Now().After(info.Timeout) {
			// Continue with new lock
		} else if info.NodeID == requestingNodeID {
			// Same node is refreshing its lock, allow it
			info.LockHolders[app.NodeID] = true
			app.MessageLocks.Store(messageID, info)
			return nil, nil
		} else {
			// Different node has a valid lock, reject
			return nil, fmt.Errorf("message already locked by another node")
		}
	}
		
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
	
	return nil, nil
}
	  

func handleUnlockMsg(app *app.PubSub, messageID string, params []string) ([]byte, error) {
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
	app.Mutex.Lock()
	defer app.Mutex.Unlock()

	app.MessageLocks.Delete(messageID)
	app.MessageQueue.Delete(messageID)
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
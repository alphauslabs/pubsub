package main

import (
	"encoding/json"
)

const (
	message  = "message"
	topicsub = "topicsub"
)

type broadCastInput struct {
	Type string
	Msg  []byte
}

var ctrlbroadcast = map[string]func(*PubSub, []byte) ([]byte, error){
	message:  handleBroadcastedMsg,
	topicsub: handleBroadcastedTopicsub,
}

// Root handler for op.Broadcast() // do not change this
func broadcast(data any, msg []byte) ([]byte, error) {
	var in broadCastInput
	app := data.(*PubSub)
	if err := json.Unmarshal(msg, &in); err != nil {
		return nil, err
	}
	return ctrlbroadcast[in.Type](app, in.Msg)
}

func handleBroadcastedMsg(app *PubSub, msg []byte) ([]byte, error) {
    parts := strings.Split(string(msg), ":")
    switch parts[0] {
	case "lock":
		//if a node receives a "lock" request for a message it already has locked, it should reject duplicate locks.
		messageID := parts[1]
		if _, exists := app.messageLocks.Load(messageID); exists {
			return nil, nil // Already locked, ignore duplicate
		}
    case "unlock":
        // Handle unlock request
        messageID := parts[1]
		app.messageLocks.Delete(messageID)
        // Clean up locks and timers
	case "delete":
		messageID := parts[1]
		app.messageLocks.Delete(messageID)
		app.messageQueue.Delete(messageID)
    case "extend":
        // Handle timeout extension
        messageID := parts[1]
        newTimeout, _ := strconv.Atoi(parts[2])
		if lockInfo, ok := app.messageLocks.Load(messageID); ok {
			info := lockInfo.(MessageLockInfo)
			info.Timeout = time.Now().Add(time.Duration(newTimeout) * time.Second)
			app.messageLocks.Store(messageID, info)
		}
        // Update timeout and reset timer
    }
    return nil, nil
}

func handleBroadcastedTopicsub(app *PubSub, msg []byte) ([]byte, error) {
	return nil, nil
}

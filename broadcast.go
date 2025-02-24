package main

import (
	"encoding/json"
	"fmt"
	"log"

	storage "github.com/alphauslabs/pubsub/node_storage"
)

const (
	message  = "message"
	topicsub = "topicsub"
)

type broadCastInput struct {
	Type string
	Msg  []byte
}

func NewPubSub() *PubSub {
	return &PubSub{
		storage: storage.NewStorage(),
	}
}

var ctrlbroadcast = map[string]func(*PubSub, []byte) ([]byte, error){
	message:  handleBroadcastedMsg,
	topicsub: handleBroadcastedTopicsub,
}

// Root handler for op.Broadcast()
func broadcast(data any, msg []byte) ([]byte, error) {
	var in broadCastInput
	app := data.(*PubSub)
	if err := json.Unmarshal(msg, &in); err != nil {
		return nil, err
	}
	return ctrlbroadcast[in.Type](app, in.Msg)
}

func handleBroadcastedMsg(app *PubSub, msg []byte) ([]byte, error) {
	var queuedMsg storage.QueuedMessage
	if err := json.Unmarshal(msg, &queuedMsg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	if err := app.storage.QueueMessage(&queuedMsg); err != nil {
		if err == storage.ErrDuplicateMessage {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to queue message: %w", err)
	}

	go func() {
		if err := app.storage.ProcessQueue(); err != nil {
			log.Printf("Error processing queue: %v", err)
		}
	}()

	return nil, nil
}

func handleBroadcastedTopicsub(app *PubSub, msg []byte) ([]byte, error) {
	if err := app.storage.StoreTopicSubscriptions(msg); err != nil {
		return nil, fmt.Errorf("failed to store topic-subscriptions: %w", err)
	}
	return nil, nil
}

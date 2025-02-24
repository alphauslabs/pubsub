package main

import (
	"encoding/json"
	"fmt"

	pb "github.com/alphauslabs/pubsub-proto/v1"
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
	var message pb.Message
	if err := json.Unmarshal(msg, &message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	if err := app.storage.StoreMessage(&message); err != nil {
		return nil, fmt.Errorf("failed to store message: %w", err)
	}
	return nil, nil
}

func handleBroadcastedTopicsub(app *PubSub, msg []byte) ([]byte, error) {
	if err := app.storage.StoreTopicSubscriptions(msg); err != nil {
		return nil, fmt.Errorf("failed to store topic-subscriptions: %w", err)
	}
	return nil, nil
}

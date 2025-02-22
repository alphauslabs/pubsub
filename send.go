package main

import (
	"encoding/json"
)

const (
	topicsubupdates = "topicsubupdates"
)

type sendInput struct {
	Type string
	Msg  []byte
}

var ctrlsend = map[string]func(*PubSub, []byte) ([]byte, error){
	topicsubupdates: handleTopicSubUpdates,
}

// Root handler for broadcasted messages.
func send(data any, msg []byte) ([]byte, error) {
	var in sendInput
	app := data.(*PubSub)
	if err := json.Unmarshal(msg, &in); err != nil {
		return nil, err
	}
	return ctrlsend[in.Type](app, in.Msg)
}

// Handle topic subscription updates.
func handleTopicSubUpdates(app *PubSub, msg []byte) ([]byte, error) {
	return nil, nil
}

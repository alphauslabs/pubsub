package main

import (
	"encoding/json"

	"github.com/alphauslabs/pubsub/app"
)

const (
	topicsubupdates = "topicsubupdates"
)

type sendInput struct {
	Type string
	Msg  []byte
}

var ctrlsend = map[string]func(*app.PubSub, []byte) ([]byte, error){
	topicsubupdates: handleTopicSubUpdates,
}

// Root handler for op.Send()
func send(data any, msg []byte) ([]byte, error) {
	var in sendInput
	app := data.(*app.PubSub)
	if err := json.Unmarshal(msg, &in); err != nil {
		return nil, err
	}
	return ctrlsend[in.Type](app, in.Msg)
}

// Handle topic subscription updates.
func handleTopicSubUpdates(app *app.PubSub, msg []byte) ([]byte, error) {
	return nil, nil
}

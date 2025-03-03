package handlers

import (
	"context"
	"encoding/json"

	"github.com/alphauslabs/pubsub/app"
	"github.com/golang/glog"
)

const (
	topicsubupdates      = "topicsubupdates"
	checkleader          = "checkleader"
	initialTopicSubFetch = "initialtopicsubfetch"
)

type SendInput struct {
	Type string
	Msg  []byte
}

var ctrlsend = map[string]func(*app.PubSub, []byte) ([]byte, error){
	topicsubupdates:      handleTopicSubUpdates,
	checkleader:          handleCheckLeader,
	initialTopicSubFetch: handleInitializeTopicSub,
}

// Root handler for op.Send()
func Send(data any, msg []byte) ([]byte, error) {
	var in SendInput
	app := data.(*app.PubSub)

	if err := json.Unmarshal(msg, &in); err != nil {
		return nil, err
	}

	return ctrlsend[in.Type](app, in.Msg)
}

// Handle topic subscription updates.

func handleTopicSubUpdates(app *app.PubSub, msg []byte) ([]byte, error) {
	ctx := context.Background()
	client := app.Client // Spanner client
	op := app.Op

	FetchAndBroadcast(ctx, op, client, false) // trigger topic-subscription fetch and broadcast
	return nil, nil
}

func handleInitializeTopicSub(app *app.PubSub, msg []byte) ([]byte, error) {
	ctx := context.Background()
	client := app.Client // Spanner client

	topicsub := FetchAllTopicSubscriptions(ctx, client) // trigger topic-subscription fetch
	// Marshal topic-subscription data
	msgData, err := json.Marshal(topicsub)
	if err != nil {
		glog.Infof("STRUCT-Error marshalling topicSub: %v", err)
		return nil, err
	}

	return msgData, nil
}

func handleCheckLeader(app *app.PubSub, msg []byte) ([]byte, error) {
	if string(msg) == "PING" {
		return []byte("PONG"), nil
	}

	return nil, nil
}

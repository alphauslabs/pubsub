package handlers

import (
	"context"
	"encoding/json"
	"log"

	"github.com/alphauslabs/pubsub/app"
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
	initialTopicSubFetch: handleInitialize,
}

// Root handler for op.Send()
func Send(data any, msg []byte) ([]byte, error) {
	var in SendInput
	app := data.(*app.PubSub)

	log.Printf("[Send] Starting Send") //debugging_jose
	if err := json.Unmarshal(msg, &in); err != nil {

		log.Printf("[Send] error Send") //debugging_jose
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

func handleInitialize(app *app.PubSub, msg []byte) ([]byte, error) {
	ctx := context.Background()
	client := app.Client // Spanner client

	FetchAllTopicSubscriptions(ctx, client) // trigger topic-subscription fetch
	return nil, nil
}

func handleCheckLeader(app *app.PubSub, msg []byte) ([]byte, error) {
	if string(msg) == "PING" {
		return []byte("PONG"), nil
	}

	return nil, nil
}

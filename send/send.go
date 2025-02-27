package send

import (
	"context"
	"encoding/json"
	"log"

	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/broadcast"
)

const (
	topicsubupdates    = "topicsubupdates"
	checkleader        = "checkleader"
	crashTopicSubFetch = "crashtopicsubfetch"
)

type SendInput struct {
	Type string
	Msg  []byte
}

var ctrlsend = map[string]func(*app.PubSub, []byte) ([]byte, error){
	topicsubupdates:    handleTopicSubUpdates,
	crashTopicSubFetch: handleCrashTopicSubFetch,
	checkleader:        handleCheckLeader,
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
func handleCrashTopicSubFetch(app *app.PubSub, msg []byte) ([]byte, error) {
	ctx := context.Background()
	client := app.Client // Spanner client

	// query Spanner for the latest topic-subscription structure
	topicSub := broadcast.FetchAllTopicSubscriptions(ctx, client)

	// marshal the result into JSON format
	topicSubData, err := json.Marshal(topicSub)
	if err != nil {
		log.Printf("SEND-Error: Failed to marshal topic-subscription data: %v", err)
		return nil, err
	}

	// Return the topic-subscription structure as a response
	return topicSubData, nil
}

func handleTopicSubUpdates(app *app.PubSub, msg []byte) ([]byte, error) {
	ctx := context.Background()
	client := app.Client // Spanner client
	op := app.Op

	// Trigger immediate broadcast

	log.Printf("[handleTopicSubUpdates] Starting ImmediateBroadcast") //debugging_jose
	broadcast.ImmediateBroadcast(ctx, op, client)
	log.Printf("[handleTopicSubUpdates] ImmediateBroadcast Completed")

	return nil, nil
}

func handleCheckLeader(app *app.PubSub, msg []byte) ([]byte, error) {
	if string(msg) == "PING" {
		return []byte("PONG"), nil
	}

	return nil, nil
}

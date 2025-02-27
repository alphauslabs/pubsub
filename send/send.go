package send

import (
	"context"
	"encoding/json"
	"time"

	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/broadcast"
)

const (
	topicsubupdates = "topicsubupdates"
	checkleader     = "checkleader"
)

type SendInput struct {
	Type string
	Msg  []byte
}

var ctrlsend = map[string]func(*app.PubSub, []byte) ([]byte, error){
	topicsubupdates: handleTopicSubUpdates,
	checkleader:     handleCheckLeader,
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
	op := app.Op         // Hedge leader operation handler

	var lastChecked time.Time
	lastBroadcasted := make(map[string][]string)

	// Trigger immediate broadcast
	broadcast.ImmediateBroadcast(ctx, op, client, &lastBroadcasted, &lastChecked)

	return nil, nil
}

func handleCheckLeader(app *app.PubSub, msg []byte) ([]byte, error) {
	if string(msg) == "PING" {
		return []byte("PONG"), nil
	}

	return nil, nil
}

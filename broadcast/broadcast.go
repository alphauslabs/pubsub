package broadcast

import (
	"encoding/json"
	"fmt"
	"log"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/alphauslabs/pubsub/app"
)

const (
	message  = "message"
	topicsub = "topicsub"
)

type BroadCastInput struct {
	Type string
	Msg  []byte
}

var ctrlbroadcast = map[string]func(*app.PubSub, []byte) ([]byte, error){
	message:  handleBroadcastedMsg,
	topicsub: handleBroadcastedTopicsub,
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
	log.Println("[BROADCAST]: Received message:\n", string(msg))
	var message pb.Message
	if err := json.Unmarshal(msg, &message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	if err := app.Storage.StoreMessage(&message); err != nil {
		return nil, fmt.Errorf("failed to store message: %w", err)
	}

	return nil, nil
}

func handleBroadcastedTopicsub(app *app.PubSub, msg []byte) ([]byte, error) {
	log.Println("[BROADCAST]: Received topic-subscriptions:\n", string(msg))
	if err := app.Storage.StoreTopicSubscriptions(msg); err != nil {
		return nil, fmt.Errorf("failed to store topic-subscriptions: %w", err)
	}

	return nil, nil
}

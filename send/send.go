package send

import (
	"encoding/json"
	"log"

	"github.com/alphauslabs/pubsub/app"
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
	log.Println("[SEND]: Received message: ", string(in.Msg))
	return ctrlsend[in.Type](app, in.Msg)
}

// Handle topic subscription updates.
func handleTopicSubUpdates(app *app.PubSub, msg []byte) ([]byte, error) {
	return nil, nil
}

func handleCheckLeader(app *app.PubSub, msg []byte) ([]byte, error) {
	var in SendInput
	if err := json.Unmarshal(msg, &in); err != nil {
		log.Println("[ERROR]: Failed to unmarshal message")
		return nil, err
	}
	log.Println("Received message: ", string(in.Msg))
	if string(in.Msg) == "PING" {
		log.Println("returning PONG")
		return []byte("PONG"), nil
	}

	return nil, nil
}

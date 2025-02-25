package broadcast

import (
	"encoding/json"

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
	return nil, nil
}

func handleBroadcastedTopicsub(app *app.PubSub, msg []byte) ([]byte, error) {
	return nil, nil
}

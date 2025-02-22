package main

import (
	"encoding/json"
)

const (
	message  = "message"
	topicsub = "topicsub"
)

type broadCastInput struct {
	Type string
	Msg  []byte
}

var brdcst = map[string]func(*PubSub, []byte) ([]byte, error){
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
	return brdcst[in.Type](app, in.Msg)
}

func handleBroadcastedMsg(app *PubSub, msg []byte) ([]byte, error) {
	return nil, nil
}

func handleBroadcastedTopicsub(app *PubSub, msg []byte) ([]byte, error) {
	return nil, nil
}

package main

import (
	"encoding/json"
	"fmt"
	"log"
)

const (
	message  = "message"
	topicsub = "topicsub"
)

type broadCastInput struct {
	Type string
	Msg  []byte
}

// Root handler for broadcasted messages.
func broadcast(data any, msg []byte) ([]byte, error) {
	var in broadCastInput
	err := json.Unmarshal(msg, &in)
	if err != nil {
		return nil, err
	}

	switch in.Type {
	case message:
		log.Println("[broadcast] type message received:", string(in.Msg))
		return []byte("broadcast " + string(in.Msg)), nil
	case topicsub:
		log.Println("[broadcast] type topicsub received:", string(in.Msg))
		return []byte("broadcast " + string(in.Msg)), nil
	default:
		return nil, fmt.Errorf("unknown type: %v", in.Type)
	}
}

// You can add the message and topicsub handlers here

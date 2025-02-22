package main

import (
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
	switch data.(broadCastInput).Type {
	case message:
		log.Println("[broadcast] type message received:", string(msg))
		return []byte("broadcast " + string(msg)), nil
	case topicsub:
		log.Println("[broadcast] type topicsub received:", string(msg))
		return []byte("broadcast " + string(msg)), nil
	default:
		return nil, fmt.Errorf("unknown type: %v", data.(broadCastInput).Type)
	}
}

// You can add the message and topicsub handlers here

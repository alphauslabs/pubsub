package main

import (
	"fmt"
	"log"
)

const (
	topicsubupdates = "topicsubupdates"
)

type sendInput struct {
	Type string
	Msg  []byte
}

func send(data any, msg []byte) ([]byte, error) {
	switch data.(broadCastInput).Type {
	case topicsubupdates:
		log.Println("[send] type topicsubupdates received:", string(msg))
		return []byte("send " + string(msg)), nil
	default:
		return nil, fmt.Errorf("unknown type: %v", data.(broadCastInput).Type)
	}
}

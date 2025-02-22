package main

import (
	"encoding/json"
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

// Root send handler
func send(data any, msg []byte) ([]byte, error) {
	var in sendInput
	err := json.Unmarshal(msg, &in)
	if err != nil {
		return nil, err
	}

	switch in.Type {
	case topicsubupdates:
		log.Println("[send] type topicsubupdates received:", string(in.Msg))
		return []byte("send " + string(in.Msg)), nil
	default:
		return nil, fmt.Errorf("unknown type: %v", in.Type)
	}
}

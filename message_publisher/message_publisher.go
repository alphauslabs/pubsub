package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type Message struct {
	TopicID string `json:"id"`
	Payload string `json:"payload"`
}

const (
	numMessages = 20
)

var endpoints = []string{
        "http://localhost:8080",
	"http://localhost:8081",
	"http://localhost:8082",
}

func publishMessage(wg *sync.WaitGroup, id int, endpoint string) {
	defer wg.Done()

	// Generate unique topic ID based on message ID
	topicID := fmt.Sprintf("topic%d_sub%d", id%10, id%5)

	msg := Message{
		TopicID: topicID,
		Payload: fmt.Sprintf("Payload for message %d", id),
	}

	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("%s Message %d failed: %v", time.Now().Format(time.RFC3339), id, err)
		return
	}

	_, err = http.Post(endpoint, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("%s Message %d (Topic: %s) failed: %v", time.Now().Format(time.RFC3339), id, topicID, err)
		return
	}

	log.Printf("%s Message %d (ID: %s) published successfully", time.Now().Format(time.RFC3339), id, topicID)
}

func main() {
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		endpoint := endpoints[i%len(endpoints)] // Round-robin selection
		go publishMessage(&wg, i, endpoint)
	}

	wg.Wait()
	log.Printf("All messages published. Total: %d, Time: %.2f s, Message per second: %.2f", numMessages, time.Since(startTime).Seconds(), float64(numMessages)/time.Since(startTime).Seconds())
}

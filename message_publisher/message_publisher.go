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
	TopicID        string `json:"topic_id"`
	SubscriptionID string `json:"subscription_id"`
	Payload        string `json:"payload"`
}

const (
	numMessages = 10000
	concurrency = 50
	useMock     = true // set to false if virgil and kishea's endpoint is established
)

var endpointURL = func() string {
	if useMock {
		return "http://localhost:8080" // mock server URL
	}
	return "http://your-teammate-endpoint.com" // virgil-kitkat endpoint
}()

func publishMessage(wg *sync.WaitGroup, id int) {
	defer wg.Done()
	msg := Message{"test-topic", "test-subscription", fmt.Sprintf("Payload for message %d", id)}
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("%s Message %d failed: %v", time.Now().Format("2006/01/02 15:04:05"), id, err)
		return
	}
	_, err = http.Post(endpointURL, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("%s Message %d failed: %v", time.Now().Format("2006/01/02 15:04:05"), id, err)
		return
	}
	log.Printf("%s Message %d published successfully", time.Now().Format("2006/01/02 15:04:05"), id)
}

func main() {
	var wg sync.WaitGroup
	startTime := time.Now()
	for i := 0; i < numMessages; i++ {
		wg.Add(1)
		go publishMessage(&wg, i)
	}
	wg.Wait()
	log.Printf("All messages published. Total: %d, Time: %.2f s, Message per second: %.2f", numMessages, time.Since(startTime).Seconds(), float64(numMessages)/time.Since(startTime).Seconds())
}

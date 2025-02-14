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
    ID           string `json:"id"`
    Subsription string `json:"subsription"`
    Payload      string `json:"payload"`
}


const (
	numMessages = 10
)

var endpoints = []string{ 
"http://10.146.0.4:8080/write",
"http://10.146.0.8:8080/write",
"http://10.146.0.18:8080/write",
}

func publishMessage(wg *sync.WaitGroup, id int, endpoint string) {
	defer wg.Done()
	msg := Message{
		ID:           fmt.Sprintf("%d", id),
		Subsription: fmt.Sprintf("sub-%d", id),
		Payload:      fmt.Sprintf("MESSAGE TO NODE %d", id%len(endpoints)+1),
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

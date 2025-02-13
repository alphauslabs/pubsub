package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

var (
	isLeader = flag.Bool("leader", false, "Run this node as the leader for bulk writes")
	leaderURL = flag.String("leader-url", "http://localhost:8080", "URL of the leader node")
)

func main() {
	flag.Parse()

	if *isLeader {
		log.Println("Running as LEADER.")
		// Start listening for messages from the publisher.
		go startPublisherListener()

		// Start leader-specific HTTP server to accept forwarded messages from non-leader nodes.
		go startLeaderHTTPServer()

		// Process aggregated messages and perform bulk write.
		runBulkWriterAsLeader()
	} else {
		log.Println("Running as NON-LEADER.")
		// Start listening for messages from the publisher.
		go startPublisherListener()

		// Instead of writing to Spanner, forward the messages to the leader.
		runBulkWriterAsFollower()
	}

	// Prevent main from exiting.
	select {}
}

// startPublisherListener simulates listening for publisher messages.
func startPublisherListener() {
	// Pseudocode: Listen to publisher's HTTP endpoint.
	log.Println("Listening for publisher messages...")
	// Imagine this function continuously receives messages.
}

// startLeaderHTTPServer starts an HTTP server for receiving messages from followers.
func startLeaderHTTPServer() {
	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		var message map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&message)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Printf("Leader received message: %v\n", message)
		w.WriteHeader(http.StatusOK)
	})

	log.Println("Leader is now listening for forwarded messages from followers on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// runBulkWriterAsLeader handles bulk writing as a leader.
func runBulkWriterAsLeader() {
	// Pseudocode: Aggregate messages from publisher and followers.
	// Use batching logic, e.g., flush batch every N messages or every T seconds.
	for {
		// Simulate message aggregation.
		log.Println("Leader aggregating messages for bulk write...")
		time.Sleep(5 * time.Second)
		// Simulate bulk write to Spanner.
		log.Println("Leader performing bulk write to Spanner.")
	}
}

// runBulkWriterAsFollower forwards messages to the leader.
func runBulkWriterAsFollower() {
	// Pseudocode: Forward received messages to leader's HTTP endpoint.
	for {
		// Simulate receiving a message.
		message := map[string]interface{}{
			"content": "Sample message from non-leader node",
		}
		jsonData, err := json.Marshal(message)
		if err != nil {
			log.Printf("Error marshalling message: %v\n", err)
			continue
		}

		resp, err := http.Post(*leaderURL+"/receive", "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			log.Printf("Error forwarding message to leader: %v\n", err)
			continue
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Printf("Error reading response from leader: %v\n", err)
			continue
		}

		log.Printf("Non-leader NODE forwarded message to leader, response: %s\n", string(body))
		time.Sleep(3 * time.Second)
	}
}
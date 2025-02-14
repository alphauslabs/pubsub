package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
	"io"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
)

var (
	isLeader   = flag.Bool("leader", false, "Run this node as the leader for bulk writes")
	leaderURL  = flag.String("leader-url", "http://35.221.112.183:8080", "URL of the leader node")
	messages   = make(chan map[string]interface{}, 20000) // Buffered channel for messages
	mutex      sync.Mutex                                 // Mutex to protect concurrent access to messages
	batchSize  = 2000                                     // Number of messages per batch
)

func createClients(ctx context.Context, db string) (*spanner.Client, error) {
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("failed to create Spanner client: %v", err)
	}
	return client, nil
}

// WriteBatchUsingDML inserts a batch of messages into the Messages table using DML.
func WriteBatchUsingDML(w io.Writer, client *spanner.Client, batch []map[string]interface{}) error {
	ctx := context.Background()

	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		var stmts []spanner.Statement
		for _, message := range batch {
			id := uuid.New().String()
			stmt := spanner.Statement{
				SQL: `INSERT INTO Messages (id, subsription, payload, createdAt, updatedAt)
					  VALUES (@id, @subsription, @payload, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())`,
				Params: map[string]interface{}{
					"id":          id,
					"subsription": message["subsription"], // Note the typo here
					"payload":     message["payload"],
				},
			}
			stmts = append(stmts, stmt)
		}

		rowCount, err := txn.BatchUpdate(ctx, stmts)
		if err != nil {
			return fmt.Errorf("failed to execute batch DML: %v", err)
		}
		fmt.Fprintf(w, "%d record(s) inserted.\n", rowCount)
		return nil
	})
	return err
}

func main() {
	flag.Parse()

	if *isLeader {
		log.Println("Running as [LEADER].")
		// Start listening for messages from the publisher.
		go startPublisherListener()

		// Start leader-specific HTTP server to accept forwarded messages from follower nodes.
		go startLeaderHTTPServer()

		// Process aggregated messages and perform bulk write.
		go runBulkWriterAsLeader()
	} else {
		log.Println("Running as [FOLLOWER].")
		// Start listening for messages from the publisher.
		go startPublisherListener()

		// Instead of writing to Spanner, forward the messages to the leader.
		go runBulkWriterAsFollower()
	}

	select {}
}

// startPublisherListener simulates listening for publisher messages.
func startPublisherListener() {
	// Pseudocode: Listen to publisher's HTTP endpoint.
	log.Println("[LEADER] Listening for publisher messages...")
	// Imagine this function continuously receives messages.
}

// startLeaderHTTPServer starts an HTTP server for receiving messages from followers.
func startLeaderHTTPServer() {
	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}

		var message map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&message)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Log the received message.
		log.Printf("[LEADER] received message: %v\n", message)

		// Add the message to the messages channel.
		messages <- message

		// Respond with success.
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("[LEADER] Message received successfully"))
	})

	log.Println("[LEADER] now listening for forwarded messages from followers on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// runBulkWriterAsLeader handles bulk writing as a leader.
func runBulkWriterAsLeader() {
	// Replace with your actual database path
	db := "projects/labs-169405/instances/alphaus-dev/databases/main"

	// Create a context
	ctx := context.Background()

	// Create the Spanner client
	client, err := createClients(ctx, db)
	if err != nil {
		log.Fatalf("Failed to create clients: %v", err)
	}
	defer client.Close()

	var batch []map[string]interface{}
	for {
		select {
		case msg := <-messages:
			batch = append(batch, msg)
			if len(batch) >= batchSize {
				go func(batch []map[string]interface{}) {
					err := WriteBatchUsingDML(log.Writer(), client, batch)
					if err != nil {
						log.Printf("[LEADER] Error writing batch to Spanner: %v\n", err)
					} else {
						log.Printf("[LEADER] Successfully wrote batch of %d messages\n", len(batch))
					}
				}(batch)
				batch = nil
			}
		case <-time.After(400 * time.Millisecond): 
			if len(batch) > 0 {
				go func(batch []map[string]interface{}) {
					err := WriteBatchUsingDML(log.Writer(), client, batch)
					if err != nil {
						log.Printf("[LEADER] Error writing batch to Spanner: %v\n", err)
					} else {
						log.Printf("[LEADER] Successfully wrote batch of %d messages\n", len(batch))
					}
				}(batch)
				batch = nil
			}
		}
	}
}

// runBulkWriterAsFollower forwards messages to the leader.
func runBulkWriterAsFollower() {
	// Start listening for messages from the consumer.
	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}

		var message map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&message)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Log the received message.
		log.Printf("[FOLLOWER] Follower received message: %v\n", message)

		// Forward the message to the leader.
		jsonData, err := json.Marshal(message)
		if err != nil {
			log.Printf("[FOLLOWER] Error marshalling message: %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp, err := http.Post(*leaderURL+"/write", "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			log.Printf("[FOLLOWER] Error forwarding message to leader: %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("[FOLLOWER] Error reading response from leader: %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("[FOLLOWER] Leader response: %s\n", string(body)) // Log the response body
		// Respond to the consumer.
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("[FOLLOWER] Message forwarded to leader successfully"))
	})

	log.Println("[FOLLOWER] Follower is now listening for messages from the consumer...")
	log.Fatal(http.ListenAndServe(":8080", nil)) // Use a different port for the follower
}
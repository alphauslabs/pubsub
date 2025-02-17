package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
)

const (
	database   = "projects/labs-169405/instances/alphaus-dev/databases/main"
	table      = "Messages"
	numWorkers = 10 // concurrent message processing
)

// Global variables
var (
	spannerClient *spanner.Client
	messageQueue  chan Message // Buffered channel for queuing
)

type Message struct {
	ID           string    `json:"id"`
	Subscription string    `json:"subsription"`
	Payload      string    `json:"payload"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

func main() {
	ctx := context.Background()
	var err error

	spannerClient, err = spanner.NewClient(ctx, database)
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer spannerClient.Close()

	// Initialize message queue
	messageQueue = make(chan Message, 1000) // hold 1000 messages before blocking

	// worker pool
	for i := 0; i < numWorkers; i++ {
		go worker(ctx)
	}

	// Start HTTP server
	http.HandleFunc("/write", writeHandler)
	port := ":8080"
	fmt.Printf("Server listening on port %s\n", port)
	log.Fatal(http.ListenAndServe("0.0.0.0"+port, nil))
}

func writeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	// Generate unique ID
	msg.ID = uuid.New().String()

	// Truncate timestamp to seconds for Spanner compatibility
	now := time.Now().UTC().Truncate(time.Second)
	msg.CreatedAt = now
	msg.UpdatedAt = now

	fmt.Printf("Received published message: %+v\n", msg)

	// Send message to worker queue instead of writing immediately
	messageQueue <- msg

	// respond to client instead of waiting writing
	response := map[string]string{"message": "Received and queued for storage"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// function for processing messages from queue
func worker(ctx context.Context) {
	for msg := range messageQueue { // Continuous message fetch
		if err := insertMessage(ctx, msg); err != nil {
			log.Printf("Failed to write message to Spanner: %v", err)
		} else {
			log.Printf("Successfully wrote message to Spanner: %v", msg.ID)
		}
	}
}

// Write message to Spanner
func insertMessage(ctx context.Context, msg Message) error {
	mutation := spanner.InsertOrUpdate(
		table,
		[]string{"id", "subsription", "payload", "createdAt", "updatedAt"},
		[]interface{}{msg.ID, msg.Subscription, msg.Payload, msg.CreatedAt, msg.UpdatedAt},
	)

	_, err := spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		log.Printf("Error writing to Spanner: %v", err)
	} else {
		log.Printf("Successfully wrote message to Spanner")
	}
	return err
}

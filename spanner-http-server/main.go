package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

const (
	table = "Messages"
	db    = "projects/labs-169405/instances/alphaus-dev/databases/main"
)

// Message represents the structure of a message in Spanner
type Message struct {
	ID          string    `json:"id"`
	Subsription string    `json:"subsription"`
	Payload     string    `json:"payload,omitempty"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

// Spanner client
var spannerClient *spanner.Client

func main() {
	// Initialize Spanner client
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, db) // Replace with proper authentication
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer client.Close()
	spannerClient = client

	// Set up HTTP handlers
	http.HandleFunc("/publish", handlePublish) // POST request to insert message
	http.HandleFunc("/get", handleGetMessage)  // GET request to fetch message

	log.Println("Server listening on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// handlePublish inserts a message into Spanner
func handlePublish(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if msg.ID == "" || msg.Subsription == "" {
		http.Error(w, "Missing required fields", http.StatusBadRequest)
		return
	}

	// Insert into Spanner
	ctx := context.Background()
	now := time.Now()
	mutation := spanner.Insert(
		table,
		[]string{"id", "subsription", "payload", "createdAt", "updatedAt"},
		[]interface{}{msg.ID, msg.Subsription, msg.Payload, now, now},
	)

	_, err := spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to insert into Spanner: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "Message stored successfully")
}

// handleGetMessage retrieves a message from Spanner based on the ID
func handleGetMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	// Extract `id` from query parameter
	id := r.URL.Query().Get("id")
	if id == "" {
		http.Error(w, "Missing 'id' query parameter", http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	stmt := spanner.Statement{
		SQL:    "SELECT id, subsription, payload, createdAt, updatedAt FROM Messages WHERE id = @id",
		Params: map[string]interface{}{"id": id},
	}

	iter := spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	// Read first matching row
	row, err := iter.Next()
	if err == iterator.Done {
		http.Error(w, "Message not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, fmt.Sprintf("Query error: %v", err), http.StatusInternalServerError)
		return
	}

	var msg Message
	if err := row.Columns(&msg.ID, &msg.Subsription, &msg.Payload, &msg.CreatedAt, &msg.UpdatedAt); err != nil {
		http.Error(w, fmt.Sprintf("Failed to parse row: %v", err), http.StatusInternalServerError)
		return
	}

	// Return the message as JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(msg)
}

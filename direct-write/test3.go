package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/spanner"
)

const (
	database = "projects/labs-169405/instances/alphaus-dev/databases/main"
	table    = "Messages"
)

var spannerClient *spanner.Client

type Message struct {
	ID           string    `json:"id"`
	Subscription string    `json:"subsription"` // ✅ Kept as "subsription" to match DB
	Payload      string    `json:"payload"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

func main() {
	// ✅ Initialize Spanner client once
	ctx := context.Background()
	var err error
	spannerClient, err = spanner.NewClient(ctx, database)
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer spannerClient.Close()

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

	now := time.Now().UTC()
	msg.CreatedAt = now
	msg.UpdatedAt = now

	if err := insertMessage(context.Background(), msg); err != nil {
		http.Error(w, fmt.Sprintf("Failed to write to Spanner: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": "Data written to Spanner successfully"})
}

func insertMessage(ctx context.Context, msg Message) error {
	mutation := spanner.InsertOrUpdate(
		table,
		[]string{"id", "subsription", "payload", "createdAt", "updatedAt"}, // ✅ Kept "subsription"
		[]interface{}{msg.ID, msg.Subscription, msg.Payload, msg.CreatedAt, msg.UpdatedAt},
	)

	_, err := spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		log.Printf("Error writing to Spanner: %v", err) // ✅ Log for debugging
	}
	return err
}

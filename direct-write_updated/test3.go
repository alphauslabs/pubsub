package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid" //new import for UID
)

const (
	database = "projects/labs-169405/instances/alphaus-dev/databases/main"
	table    = "Messages"
)

var spannerClient *spanner.Client

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

	msg.ID = uuid.New().String() // Generate UUID

	now := time.Now().UTC().Truncate(time.Second) // Truncate to second for Spanner TIMESTAMP
	msg.CreatedAt = now
	msg.UpdatedAt = now

	fmt.Printf("Received published message: %+v\n", msg)

	if err := insertMessage(context.Background(), msg); err != nil {
		http.Error(w, fmt.Sprintf("Failed to write to Spanner: %v", err), http.StatusInternalServerError)
		return
	}

	response := map[string]string{"message": "Received and stored successfully"}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)

}

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

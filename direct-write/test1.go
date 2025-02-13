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
	db = "projects/labs-169405/instances/alphaus-dev/databases/main"
)

type Message struct {
	ID          string           `json:"id"`
	Subsription string           `json:"subsription"` // Match column name
	Payload     string           `json:"payload"`
	CreatedAt   spanner.NullTime `json:"createdAt"` // Change to NullTime
	UpdatedAt   spanner.NullTime `json:"updatedAt"` // Change to NullTime
}

// Create a Spanner client
func createClient() (*spanner.Client, error) {
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("failed to create client: %v", err)
	}
	return client, nil
}

// Insert data into Spanner
func insertMessage(w http.ResponseWriter, r *http.Request) {
	client, err := createClient()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer client.Close()

	var msg Message
	err = json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	// Set timestamps
	// Set timestamps correctly
	now := time.Now()
	msg.CreatedAt = spanner.NullTime{Time: now, Valid: true}
	msg.UpdatedAt = spanner.NullTime{Time: now, Valid: true}

	// Insert into Spanner
	mutation := spanner.Insert("Messages",
		[]string{"id", "subsription", "payload", "createdAt", "updatedAt"},
		[]interface{}{msg.ID, msg.Subsription, msg.Payload, msg.CreatedAt, msg.UpdatedAt},
	)

	_, err = client.Apply(context.Background(), []*spanner.Mutation{mutation})
	if err != nil {
		http.Error(w, fmt.Sprintf("Insert failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, "Inserted successfully")
}

// Query data from Spanner
func queryMessages(w http.ResponseWriter, r *http.Request) {
	client, err := createClient()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer client.Close()

	stmt := spanner.Statement{SQL: "SELECT * FROM Messages"}
	iter := client.Single().Query(context.Background(), stmt)

	var messages []Message

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("Query failed: %v", err), http.StatusInternalServerError)
			return
		}

		var msg Message
		err = row.Columns(&msg.ID, &msg.Subsription, &msg.Payload, &msg.CreatedAt, &msg.UpdatedAt) // Fixed field name
		if err != nil {
			http.Error(w, fmt.Sprintf("Row parsing failed: %v", err), http.StatusInternalServerError)
			return
		}
		messages = append(messages, msg)
	}

	json.NewEncoder(w).Encode(messages)
}

// Set up HTTP server
func main() {
	http.HandleFunc("/insert", insertMessage)
	http.HandleFunc("/query", queryMessages)

	log.Println("Server running on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

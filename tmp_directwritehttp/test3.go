package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"cloud.google.com/go/spanner"
	"github.com/golang/glog"
	"github.com/google/uuid"
)

const (
	database = "projects/labs-169405/instances/alphaus-dev/databases/main"
	table    = "Messages"
)

var spannerClient *spanner.Client

type Message struct {
	ID      string `json:"id"`
	Payload string `json:"payload"`
	Topic   string `json:"topic"`
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
	port := ":8085"
	fmt.Printf("Server listening on port %s\n", port)
	log.Fatal(http.ListenAndServe("10.146.0.4"+port, nil))
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

	msg.ID = uuid.New().String()

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
		[]string{"id", "topic", "createdAt", "updatedAt", "payload"},
		[]interface{}{msg.ID, msg.Topic, spanner.CommitTimestamp, spanner.CommitTimestamp, msg.Payload},
	)

	_, err := spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		glog.Infof("Error writing to Spanner: %v", err)
	} else {
		glog.Infof("Successfully wrote message to Spanner")
	}
	return err
}

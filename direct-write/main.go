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

var client *spanner.Client

func main() {
	var err error
	client, err = spanner.NewClient(context.Background(), db)
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer client.Close()

	http.HandleFunc("/insert", insertMessageHandler)
	http.HandleFunc("/query", queryMessagesHandler)

	log.Println("Server started on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// Insert message into Spanner via HTTP POST
func insertMessageHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	var requestData struct {
		Id           string `json:"id"`
		Subscription string `json:"subscription"`
		Payload      string `json:"payload"`
	}

	err := json.NewDecoder(r.Body).Decode(&requestData)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	currentTime := time.Now()
	cols := []string{"id", "subscription", "payload", "createdAt", "updatedAt"}
	vals := []interface{}{requestData.Id, requestData.Subscription, requestData.Payload, currentTime, currentTime}
	mutation := spanner.InsertOrUpdate(table, cols, vals)

	_, err = client.Apply(context.Background(), []*spanner.Mutation{mutation})
	if err != nil {
		http.Error(w, fmt.Sprintf("Insert failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("Message inserted successfully"))
}

// Query all messages from Spanner via HTTP GET
func queryMessagesHandler(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	stmt := spanner.Statement{SQL: fmt.Sprintf("SELECT * FROM %v", table)}
	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var results []map[string]interface{}
	for {
		var msg struct {
			Id           spanner.NullString
			Subscription spanner.NullString
			Payload      spanner.NullString
			CreatedAt    spanner.NullTime
			UpdatedAt    spanner.NullTime
		}

		err := iter.Next(&msg)
		if err == iterator.Done {
			break
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("Query failed: %v", err), http.StatusInternalServerError)
			return
		}

		results = append(results, map[string]interface{}{
			"id":           msg.Id.StringVal,
			"subscription": msg.Subscription.StringVal,
			"payload":      msg.Payload.StringVal,
			"createdAt":    msg.CreatedAt.Time,
			"updatedAt":    msg.UpdatedAt.Time,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

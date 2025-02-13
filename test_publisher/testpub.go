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

type SpannerData struct {
	ID           string    `json:"id"`
	Subscription string    `json:"subsription"`
	Payload      string    `json:"payload"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

func writeToSpanner(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Parse request body
	var data SpannerData
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Connect to Spanner (use emulator for local testing)
	ctx := context.Background()
	databaseName := "projects/labs-169405/instances/alphaus-dev/databases/main"

	client, err := spanner.NewClient(ctx, databaseName)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error connecting to Spanner: %v", err), http.StatusInternalServerError)
		return
	}
	defer client.Close()

	// Insert or update the data in Spanner
	_, err = client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdate("Messages",
			[]string{"id", "subsription", "payload", "createdAt", "updatedAt"},
			[]interface{}{data.ID, data.Subscription, data.Payload, data.CreatedAt, data.UpdatedAt},
		),
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("Error writing to Spanner: %v", err), http.StatusInternalServerError)
		return
	}

	// Respond with success message
	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, `{"message": "Data written to Spanner successfully"}`)
}

func main() {
	http.HandleFunc("/write", writeToSpanner)
	port := ":8080"
	fmt.Printf("Server listening on port %s\n", port)
	log.Fatal(http.ListenAndServe("0.0.0.0"+port, nil)) // Ensure it listens on all local interfaces
}

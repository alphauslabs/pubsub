// total_time_ms → Total request time, including parsing, writing, and commit retrieval.
// spanner_write_ms → Time taken for the Spanner Apply() operation.
// commit_retrieval_ms → Time spent retrieving commit timestamps from Spanner.
// commit_time → Actual Spanner commit timestamp.
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
	"google.golang.org/api/iterator"
)

const (
	database = "projects/labs-169405/instances/alphaus-dev/databases/main"
	table    = "Messages"
)

var spannerClient *spanner.Client

type Message struct {
	ID      string `json:"id"`
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
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
	log.Fatal(http.ListenAndServe("10.146.0.18"+port, nil))
}

func writeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
		return
	}

	startTotal := time.Now() // Start measuring total request time

	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}

	msg.ID = uuid.New().String()

	// Measure Spanner write time
	startSpanner := time.Now()
	err := insertMessage(context.Background(), msg)
	spannerDuration := time.Since(startSpanner)

	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to write to Spanner: %v", err), http.StatusInternalServerError)
		return
	}

	// Retrieve commit timestamp
	startCommit := time.Now()
	commitTime, err := getCommitTime(context.Background(), msg.ID)
	commitDuration := time.Since(startCommit)

	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to retrieve commit time: %v", err), http.StatusInternalServerError)
		return
	}

	totalDuration := time.Since(startTotal) // Total request processing time

	log.Printf("Write operation: Total=%v ms | SpannerWrite=%v ms | CommitRetrieval=%v ms",
		totalDuration.Milliseconds(), spannerDuration.Milliseconds(), commitDuration.Milliseconds())

	response := map[string]interface{}{
		"message":             "Received and stored successfully",
		"total_time_ms":       totalDuration.Milliseconds(),
		"spanner_write_ms":    spannerDuration.Milliseconds(),
		"commit_retrieval_ms": commitDuration.Milliseconds(),
		"commit_time":         commitTime,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func insertMessage(ctx context.Context, msg Message) error {
	mutation := spanner.InsertOrUpdate(
		table,
		[]string{"id", "topic", "payload", "createdAt", "updatedAt"},
		[]interface{}{msg.ID, msg.Topic, msg.Payload, spanner.CommitTimestamp, spanner.CommitTimestamp},
	)

	_, err := spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		log.Printf("Error writing to Spanner: %v", err)
	} else {
		log.Printf("Successfully wrote message to Spanner")
	}
	return err
}

func getCommitTime(ctx context.Context, id string) (string, error) {
	stmt := spanner.Statement{
		SQL:    "SELECT updatedAt FROM Messages WHERE id = @id",
		Params: map[string]interface{}{"id": id},
	}

	iter := spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var commitTime time.Time
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return "", err
		}
		if err := row.Columns(&commitTime); err != nil {
			return "", err
		}
	}

	return commitTime.Format(time.RFC3339Nano), nil
}

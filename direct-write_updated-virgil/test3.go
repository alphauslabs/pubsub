package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
)

const (
	database = "projects/labs-169405/instances/alphaus-dev/databases/main"
	table    = "Messages"
)

var spannerClient *spanner.Client

type Message struct {
	ID           string `json:"id"`
	Subscription string `json:"subsription"`
	Payload      string `json:"payload"`
	Topic        string `json:"topic"`
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
		[]string{"id", "payload", "createdAt", "updatedAt", "topic"},
		[]interface{}{msg.ID, msg.Topic, spanner.CommitTimestamp, spanner.CommitTimestamp, msg.Payload},
	)

	_, err := spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		log.Printf("Error writing to Spanner: %v", err)
	} else {
		log.Printf("Successfully wrote message to Spanner")
	}
	return err
}

// QUERY TO GET THE MESSAGE COUNTS PER SECOND AND AVERAGE MESSAGES PER SECOND
// WITH MessageCounts AS (
// 	SELECT
// 	  TIMESTAMP_TRUNC(createdAt, SECOND) AS second,
// 	  COUNT(*) AS message_count
// 	FROM
// 	  Messages
// 	WHERE
// 	  createdAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
// 	GROUP BY
// 	  second
//   ),
//   Summary AS (
// 	SELECT
// 	  COUNT(*) AS total_messages,
// 	  TIMESTAMP_DIFF(MAX(createdAt), MIN(createdAt), SECOND) AS total_time_seconds
// 	FROM
// 	  Messages
// 	WHERE
// 	  createdAt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)
//   )

//   SELECT
// 	second,
// 	message_count,
// 	(SELECT total_messages FROM Summary) AS total_message_count,
// 	(SELECT total_time_seconds FROM Summary) AS total_time_seconds,
// 	CASE
// 	  WHEN (SELECT total_time_seconds FROM Summary) > 0 THEN (SELECT total_messages FROM Summary) / (SELECT total_time_seconds FROM Summary)
// 	  ELSE 0
// 	END AS avg_messages_per_second
//   FROM
// 	MessageCounts
//   ORDER BY
// 	second DESC;

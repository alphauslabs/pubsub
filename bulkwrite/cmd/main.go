package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
	"io"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
)

var (
	isLeader      = flag.Bool("leader", false, "Run this node as the leader for bulk writes")
	leaderURL     = flag.String("leader-url", "http://35.221.112.183:8080", "URL of the leader node")
	batchSize     = flag.Int("batchsize", 5000, "Batch size for bulk writes")
	messagesBuffer= flag.Int("messagesbuffer", 100000, "Buffer size for messages channel")
	waitTime      = flag.Duration("waittime", 500*time.Millisecond, "Wait time before flushing the batch")
	numWorkers    = flag.Int("workers", 10, "Number of concurrent workers") // New flag for number of workers
	messages      = make(chan map[string]interface{}, *messagesBuffer) // Larger buffer
	mutex         sync.Mutex
)

type BatchStats struct {
	totalBatches int
	totalMessages int
	averageBatchSize float64
}

func createClients(ctx context.Context, db string) (*spanner.Client, error) {
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("failed to create Spanner client: %v", err)
	}
	return client, nil
}

// WriteBatchUsingDML inserts a batch of messages into the Messages table using DML.
func WriteBatchUsingDML(w io.Writer, client *spanner.Client, batch []map[string]interface{}) error {
	ctx := context.Background()

	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		var stmts []spanner.Statement
		for _, message := range batch {
			id := uuid.New().String()
			currentTime := time.Now().Format(time.RFC3339) // Use explicit timestamp
			stmt := spanner.Statement{
				SQL: `INSERT INTO Messages (id, subsription, payload, createdAt, updatedAt)
					  VALUES (@id, @subsription, @payload, @createdAt, @updatedAt)`,
				Params: map[string]interface{}{
					"id":          id,
					"subsription": message["subsription"], // Note the typo here
					"payload":     message["payload"],
					"createdAt":   currentTime,
					"updatedAt":   currentTime,
				},
			}
			stmts = append(stmts, stmt)
		}

		_, err := txn.BatchUpdate(ctx, stmts)
		if err != nil {
			return fmt.Errorf("failed to execute batch DML: %v", err)
		}
		fmt.Fprintf(w, "BATCH record() inserted.\n")
		return nil
	})
	return err
}

func main() {
	flag.Parse()

	// Reinitialize the messages channel with the configured buffer size
	messages = make(chan map[string]interface{}, *messagesBuffer)

	if *isLeader {
		log.Println("Running as [LEADER].")
		go startPublisherListener()
		go startLeaderHTTPServer()
		for i := 0; i < *numWorkers; i++ { // Use the flag value for number of workers
			go runBulkWriterAsLeader()
		}
	} else {
		log.Println("Running as [FOLLOWER].")
		go startPublisherListener()
		go runBulkWriterAsFollower()
	}

	select {}
}

func startPublisherListener() {
	log.Println("[LEADER] Listening for publisher messages...")
}

func startLeaderHTTPServer() {
	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}

		var message map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&message)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// log.Printf("[LEADER] received message: %v\n", message)
		messages <- message
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("[LEADER] Message received successfully"))
	})

	log.Println("[LEADER] now listening for forwarded messages from followers on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func runBulkWriterAsLeader() {
	db := "projects/labs-169405/instances/alphaus-dev/databases/main"
	ctx := context.Background()
	client, err := createClients(ctx, db)
	if err != nil {
		log.Fatalf("Failed to create clients: %v", err)
	}
	defer client.Close()

	var batch []map[string]interface{}
	stats := BatchStats{}

	for {
		select {
		case msg := <-messages:
			batch = append(batch, msg)
			if len(batch) >= *batchSize {
				stats.totalBatches++
				stats.totalMessages += len(batch)
				go func(batch []map[string]interface{}) {
					err := WriteBatchUsingDML(log.Writer(), client, batch)
					if err != nil {
						log.Printf("[LEADER] Error writing batch to Spanner: %v\n", err)
					} else {
						log.Printf("Successfully wrote batch of %d messages\n", len(batch))
					}
				}(batch)
				batch = nil
			}
		case <-time.After(*waitTime):
			if len(batch) > 0 {
				stats.totalBatches++
				stats.totalMessages += len(batch)
				go func(batch []map[string]interface{}) {
					err := WriteBatchUsingDML(log.Writer(), client, batch)
					if err != nil {
						log.Printf("[LEADER] Error writing batch to Spanner: %v\n", err)
					} else {
						log.Printf("Successfully wrote batch of %d messages\n", len(batch))
					}
				}(batch)
				batch = nil
			}
		}
	}

	// Calculate and log the average batch size
	if stats.totalBatches > 0 {
		stats.averageBatchSize = float64(stats.totalMessages) / float64(stats.totalBatches)
		log.Printf("Average batch size: %.2f\n", stats.averageBatchSize)
	}
}

func runBulkWriterAsFollower() {
	http.HandleFunc("/write", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Invalid request method", http.StatusMethodNotAllowed)
			return
		}

		var message map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&message)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		log.Printf("[FOLLOWER] Follower received message: %v\n", message)

		jsonData, err := json.Marshal(message)
		if err != nil {
			log.Printf("[FOLLOWER] Error marshalling message: %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		resp, err := http.Post(*leaderURL+"/write", "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			log.Printf("[FOLLOWER] Error forwarding message to leader: %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Printf("[FOLLOWER] Error reading response from leader: %v\n", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Printf("[FOLLOWER] Leader response: %s\n", string(body))
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("[FOLLOWER] Message forwarded to leader successfully"))
	})

	log.Println("[FOLLOWER] Follower is now listening for messages from the consumer...")
	log.Fatal(http.ListenAndServe(":8080", nil)) // Use a different port for the follower
}
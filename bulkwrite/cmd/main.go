package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
)

var (
	isLeader       = flag.Bool("leader", false, "Run this node as the leader for bulk writes")
	leaderURL      = flag.String("leader-url", "http://35.221.112.183:8080", "URL of the leader node")
	batchSize      = flag.Int("batchsize", 5000, "Batch size for bulk writes")
	messagesBuffer = flag.Int("messagesbuffer", 1000000, "Buffer size for messages channel") // Increased buffer size
	waitTime       = flag.Duration("waittime", 500*time.Millisecond, "Wait time before flushing the batch")
	numWorkers     = flag.Int("workers", 32, "Number of concurrent workers") // Increased worker count
	numShards      = 16                                                     // Number of shards for message channels
) 

type BatchStats struct {
	totalBatches     int
	totalMessages    int
	averageBatchSize float64
}

var (
	shardedChans = make([]chan map[string]interface{}, numShards) // Sharded channels for messages
	workerPool   = make(chan struct{}, *numWorkers)              // Worker pool semaphore
	wg           sync.WaitGroup
)

func init() {
	for i := 0; i < numShards; i++ {
		shardedChans[i] = make(chan map[string]interface{}, *messagesBuffer/numShards)
	}
}

func getShard(message map[string]interface{}) int {
	hash := fnv.New32a()
	hash.Write([]byte(message["subsription"].(string))) // Use a unique field for hashing
	return int(hash.Sum32()) % numShards
}

func createClients(ctx context.Context, db string) (*spanner.Client, error) {
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("failed to create Spanner client: %v", err)
	}
	return client, nil
}

func WriteBatchUsingDML(w io.Writer, client *spanner.Client, batch []map[string]interface{}) error {
	ctx := context.Background()

	mutations := make([]*spanner.Mutation, 0, len(batch))
	for _, message := range batch {
		id := uuid.New().String()
		currentTime := time.Now().Format(time.RFC3339)
		mutations = append(mutations, spanner.Insert(
			"Messages",
			[]string{"id", "subsription", "payload", "createdAt", "updatedAt"},
			[]interface{}{id, message["subsription"], message["payload"], currentTime, currentTime},
		))
	}

	_, err := client.Apply(ctx, mutations)
	if err != nil {
		return fmt.Errorf("failed to apply mutations: %v", err)
	}
	fmt.Fprintf(w, "BATCH record() inserted.\n")
	return nil
}

func main() {
	flag.Parse()

	if *isLeader {
		log.Println("Running as [LEADER].")
		go startPublisherListener()
		go startLeaderHTTPServer()
		for i := 0; i < *numWorkers; i++ {
			wg.Add(1)
			go runBulkWriterAsLeader(i)
		}
	} else {
		log.Println("Running as [FOLLOWER].")
		go startPublisherListener()
		go runBulkWriterAsFollower()
	}

	wg.Wait()
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

		// Send message to the appropriate shard
		shardedChans[getShard(message)] <- message
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("[LEADER] Message received successfully"))
	})

	log.Println("[LEADER] now listening for forwarded messages from followers on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func runBulkWriterAsLeader(workerID int) {
	defer wg.Done()

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
		case msg := <-shardedChans[workerID%numShards]: // Process messages from the assigned shard
			batch = append(batch, msg)
			if len(batch) >= *batchSize {
				stats.totalBatches++
				stats.totalMessages += len(batch)
				workerPool <- struct{}{} // Acquire worker
				go func(batch []map[string]interface{}) {
					defer func() { <-workerPool }() // Release worker
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
				workerPool <- struct{}{} // Acquire worker
				go func(batch []map[string]interface{}) {
					defer func() { <-workerPool }() // Release worker
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
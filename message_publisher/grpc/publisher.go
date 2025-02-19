package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
)

// Flags for customization
var (
	numMessages  = flag.Int("numMessages", 10000, "Number of messages to publish")
	useMock      = flag.Bool("mock", false, "Use localhost mock gRPC servers")
	useBulkWrite = flag.Bool("bulkwrite", false, "Use bulk write GCP gRPC endpoints")
	maxWorkers   = flag.Int("workers", 100, "Number of concurrent workers for publishing")
)

// gRPC endpoints
var (
	directWriteEndpoints = []string{"10.146.0.43:8085", "10.146.0.46:8085", "10.146.0.51:8085"}
	bulkWriteEndpoints   = []string{"10.146.0.43:8080", "10.146.0.46:8080", "10.146.0.51:8080"}
	mockEndpoints        = []string{"localhost:8080", "localhost:8081", "localhost:8082"}
	activeEndpoints      []string
	clients              []pb.PubSubServiceClient
)

// Publishes a message using gRPC
func publishMessage(id int, client pb.PubSubServiceClient) {
	topicID := fmt.Sprintf("topic-%d", id%1000)

	msg := &pb.Message{
		Id:      fmt.Sprintf("%d", id),
		Payload: fmt.Sprintf("MESSAGE %d", id),
		Topic:   topicID,
	}

	ctx := context.Background()

	_, err := client.Publish(ctx, msg)
	if err != nil {
		log.Printf("[ERROR] Message %d to %s failed: %v", id, topicID, err)
		return
	}

	log.Printf("[SUCCESS] Message %d published to %s", id, topicID)
}

func worker(id int, jobs <-chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	client := clients[id%len(clients)] // Use multiple gRPC clients
	for msgID := range jobs {
		publishMessage(msgID, client)
	}
}

// Connects to a gRPC server and returns a client
func connectToGRPC(endpoint string) (pb.PubSubServiceClient, error) {
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*10)), // Increase max message size
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v", endpoint, err)
	}
	return pb.NewPubSubServiceClient(conn), nil
}

func main() {
	flag.Parse()

	// Determine active endpoints
	if *useMock {
		activeEndpoints = mockEndpoints
		log.Println("Using mock gRPC servers (localhost)")
	} else if *useBulkWrite {
		activeEndpoints = bulkWriteEndpoints
		log.Println("Using bulk write GCP gRPC endpoints")
	} else {
		activeEndpoints = directWriteEndpoints
		log.Println("Using direct write GCP gRPC endpoints")
	}

	// gRPC clients
	for _, endpoint := range activeEndpoints {
		client, err := connectToGRPC(endpoint)
		if err != nil {
			log.Fatalf("Could not create gRPC client for %s: %v", endpoint, err)
		}
		clients = append(clients, client)
	}

	startTime := time.Now()

	// Creates a job queue
	jobs := make(chan int, *numMessages)
	var wg sync.WaitGroup

	for i := 0; i < *maxWorkers; i++ {
		wg.Add(1)
		go worker(i, jobs, &wg)
	}

	for i := 0; i < *numMessages; i++ {
		jobs <- i
	}
	close(jobs)

	wg.Wait()

	duration := time.Since(startTime)

	log.Printf("All messages published.")
	log.Printf("Total Messages: %d", *numMessages)
	log.Printf("Total Time: %.2f seconds", duration.Seconds())
	log.Printf("Throughput: %.2f messages/second", float64(*numMessages)/duration.Seconds())
}

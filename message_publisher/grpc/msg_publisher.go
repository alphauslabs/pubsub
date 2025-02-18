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
)


var (
	directWriteEndpoints = []string{
		"10.146.0.4:8085",
		"10.146.0.8:8085",
		"10.146.0.18:8085",
	}

	bulkWriteEndpoints = []string{
		"10.146.0.4:8080",
		"10.146.0.8:8080",
		"10.146.0.18:8080",
	}

	mockEndpoints = []string{
		"localhost:8080",
		"localhost:8081",
		"localhost:8082",
	}
)

var (
	activeEndpoints []string
	clients         []pb.PubSubServiceClient
)

// Publishes a message using gRPC
func publishMessage(wg *sync.WaitGroup, id int, client pb.PubSubServiceClient) {
	defer wg.Done()

	topicID := fmt.Sprintf("topic-%d", id%1000)

	msg := &pb.Message{
		Id:      fmt.Sprintf("%d", id),
		Payload: []byte(fmt.Sprintf("MESSAGE %d", id)),
		TopicId: topicID,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Publish(ctx, msg)
	if err != nil {
		log.Printf("[ERROR] Message %d to %s failed: %v", id, topicID, err)
		return
	}

	log.Printf("[SUCCESS] Message %d published to %s. Message ID: %s", id, topicID, resp.MessageId)
}

// Connects to a gRPC server and returns a client
func connectToGRPC(endpoint string) (pb.PubSubServiceClient, error) {
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure(), grpc.WithBlock())
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
		log.Println("Using mock gRPC servers (localhost:8080, localhost:8081, localhost:8082)")
	} else if *useBulkWrite {
		activeEndpoints = bulkWriteEndpoints
		log.Println("Using bulk write GCP gRPC endpoints (ports 8080)")
	} else {
		activeEndpoints = directWriteEndpoints
		log.Println("Using direct write GCP gRPC endpoints (ports 8085)")
	}

	for _, endpoint := range activeEndpoints {
		client, err := connectToGRPC(endpoint)
		if err != nil {
			log.Fatalf("Could not create gRPC client for %s: %v", endpoint, err)
		}
		clients = append(clients, client)
	}

	var wg sync.WaitGroup
	startTime := time.Now()

	// Publish messages using round-robin distribution
	for i := 0; i < *numMessages; i++ {
		wg.Add(1)
		client := clients[i%len(clients)]
		go publishMessage(&wg, i, client)
	}

	wg.Wait()
	duration := time.Since(startTime)


	log.Printf("All messages published.")
	log.Printf("Total Messages: %d", *numMessages)
	log.Printf("Total Time: %.2f seconds", duration.Seconds())
	log.Printf("Throughput: %.2f messages/second", float64(*numMessages)/duration.Seconds())
}

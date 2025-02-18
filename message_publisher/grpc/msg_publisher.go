package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	pb "github.com/alphauslabs/pubsub-proto"
	"google.golang.org/grpc"
)

// Flags for customization
var (
	numMessages  = flag.Int("numMessages", 10000, "Number of messages to publish")
	useMock      = flag.Bool("mock", false, "Use localhost mock gRPC servers")
	useBulkWrite = flag.Bool("bulkwrite", false, "Use bulk write GCP gRPC endpoints")
)

// gRPC endpoints for different scenarios
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

	activeEndpoints []string
	clients         []pb.PubSubServiceClient
)

func publishMessage(wg *sync.WaitGroup, id int, client pb.PubSubServiceClient) {
	defer wg.Done()

	msg := &pb.Message{
		Id:          fmt.Sprintf("%d", id),
		TopicId:     "test-topic",
		Subsription: fmt.Sprintf("sub-%d", id), // Intentionally using "subsription" typo
		Payload:     []byte(fmt.Sprintf("MESSAGE TO NODE %d", id%len(activeEndpoints)+1)),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.Publish(ctx, msg)
	if err != nil {
		log.Printf("[ERROR] Message %d failed to publish: %v", id, err)
		return
	}

	log.Printf("[SUCCESS] Message %d published. Server Message ID: %s", id, resp.MessageId)
}

func connectToGRPC(endpoint string) (pb.PubSubServiceClient, error) {
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v", endpoint, err)
	}
	return pb.NewPubSubServiceClient(conn), nil
}

func main() {
	flag.Parse()

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
			log.Fatalf("[FATAL] Could not create gRPC client: %v", err)
		}
		clients = append(clients, client)
	}

	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < *numMessages; i++ {
		wg.Add(1)
		client := clients[i%len(clients)] // Round-robin across clients
		go publishMessage(&wg, i, client)
	}

	wg.Wait()
	duration := time.Since(startTime)

	log.Printf("[SUMMARY] All messages published.")
	log.Printf("[SUMMARY] Total Messages: %d", *numMessages)
	log.Printf("[SUMMARY] Total Time: %.2f seconds", duration.Seconds())
	log.Printf("[SUMMARY] Throughput: %.2f messages/second", float64(*numMessages)/duration.Seconds())
}

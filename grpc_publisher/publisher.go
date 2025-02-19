package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"time"

	pubsubproto "github.com/alphauslabs/pubsub-proto/v1"
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
		"10.146.0.43:8085",
		"10.146.0.46:8085",
		"10.146.0.51:8085",
	}

	bulkWriteEndpoints = []string{
		"10.146.0.43:8080",
		"10.146.0.46:8080",
		"10.146.0.51:8080",
	}

	mockEndpoints = []string{
		"localhost:8080",
		"localhost:8081",
		"localhost:8082",
	}

	activeEndpoints []string
	clients         []pubsubproto.PubSubServiceClient
	connections     []*grpc.ClientConn
)

func publishMessage(wg *sync.WaitGroup, client pubsubproto.PubSubServiceClient, id int) {
	defer wg.Done()

	msg := &pubsubproto.Message{
		Id:      fmt.Sprintf("%d", id),
		Topic:   "test-topic",
		Payload: []byte(fmt.Sprintf("MESSAGE TO NODE %d", id%len(activeEndpoints)+1)),
	}

	ctx := context.Background()

	res, err := client.Publish(ctx, msg)
	if err != nil {
		log.Printf("Message %d failed to publish: %v", id, err)
		return
	}

	log.Printf("Message %d published successfully. Message ID: %s", id, res.MessageId)
}

func setActiveEndpoints() {
	if *useMock {
		activeEndpoints = mockEndpoints
	} else if *useBulkWrite {
		activeEndpoints = bulkWriteEndpoints
	} else {
		activeEndpoints = directWriteEndpoints
	}
}

func createClients() {
	clients = make([]pubsubproto.PubSubServiceClient, len(activeEndpoints))
	connections = make([]*grpc.ClientConn, len(activeEndpoints))

	for i, endpoint := range activeEndpoints {
		conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to connect to gRPC server %s: %v", endpoint, err)
		}

		connections[i] = conn
		clients[i] = pubsubproto.NewPubSubServiceClient(conn)
	}
}

func main() {
	flag.Parse()

	setActiveEndpoints()

	createClients()

	log.Printf("Active endpoints: %v", activeEndpoints)

	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < *numMessages; i++ {
		wg.Add(1)

		client := clients[i%len(clients)]

		go publishMessage(&wg, client, i)
	}

	wg.Wait()

	for _, conn := range connections {
		if err := conn.Close(); err != nil {
			log.Printf("Failed to close connection: %v", err)
		}
	}

	duration := time.Since(startTime)

	log.Printf("[SUMMARY] Total Messages: %d", *numMessages)
	log.Printf("[SUMMARY] Total Time: %.2f seconds", duration.Seconds())
	log.Printf("[SUMMARY] Throughput: %.2f messages/second", float64(*numMessages)/duration.Seconds())
}

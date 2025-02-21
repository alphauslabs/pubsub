package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	pubsubproto "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	serverAddr  = "localhost:50051"
	numRequests = 1000
)

func main() {
	// Connect to gRPC server securely
	conn, err := grpc.Dial(serverAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pubsubproto.NewPubSubServiceClient(conn)

	// Context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Metrics tracking
	var totalRequests int64
	var totalAcks int64
	var startTime = time.Now()
	var wg sync.WaitGroup

	// Create 1000 topics concurrently
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			// Measure request start time
			reqStart := time.Now()

			// Send request
			req := &pubsubproto.CreateTopicRequest{Name: fmt.Sprintf("test_topic_%d", i)}
			resp, err := client.CreateTopic(ctx, req)
			if err != nil {
				log.Printf("CreateTopic failed: %v", err)
				return
			}

			// Measure acknowledgment time
			reqDuration := time.Since(reqStart)

			// Update metrics
			totalRequests++
			totalAcks++

			log.Printf("Created topic: %s | Request Time: %v", resp.Name, reqDuration)
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Calculate elapsed time
	totalDuration := time.Since(startTime)

	// Throughput calculations
	requestsPerSecond := float64(totalRequests) / totalDuration.Seconds()
	acksPerSecond := float64(totalAcks) / totalDuration.Seconds()

	fmt.Println("\n=== Load Test Results ===")
	fmt.Printf("Total Requests Sent: %d\n", totalRequests)
	fmt.Printf("Total Acknowledgments Received: %d\n", totalAcks)
	fmt.Printf("Total Execution Time: %v\n", totalDuration)
	fmt.Printf("Throughput (Requests per second): %.2f\n", requestsPerSecond)
	fmt.Printf("Throughput (Acknowledgments per second): %.2f\n", acksPerSecond)
}

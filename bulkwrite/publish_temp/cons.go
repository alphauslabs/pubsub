package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	pubsubproto "bulkwrite/bulkwrite_proto"
)

var (
	serverAddrs = []string{
		"10.146.0.46:50051",
		"10.146.0.51:50050",
	}
	topicID  = flag.String("topic-id", "test-topic", "Topic ID to publish messages to")
	rowCount = flag.Int("row-count", 10000, "Number of rows to publish")
)

// workers  = flag.Int("workers", 10, "Number of concurrent workers to publish messages")

func main() {
	flag.Parse()

	// Create gRPC connections for each server address
	clients := make([]pubsubproto.PubSubServiceClient, len(serverAddrs))
	conns := make([]*grpc.ClientConn, len(serverAddrs))
	for i, addr := range serverAddrs {
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("Failed to connect to server %s: %v", addr, err)
		}
		defer conn.Close()
		conns[i] = conn
		clients[i] = pubsubproto.NewPubSubServiceClient(conn)
	}

	// Record the start time for metrics
	startTime := time.Now()

	// Round-robin index
	var index int32 = 0

	// Use a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup
	wg.Add(*rowCount)

	// Publish messages concurrently using goroutines
	for i := 0; i < *rowCount; i++ {
		go func(i int) {
			defer wg.Done()
			// Select a server using round-robin
			client := clients[atomic.AddInt32(&index, 1)%int32(len(clients))]

			// Create a message to publish
			message := &pubsubproto.Message{
				Id:        fmt.Sprintf("msg-%d", i),
				TopicId:   *topicID,
				Payload:   fmt.Sprintf("Bulkwrite_row_no_%d", i),
				CreatedAt: time.Now().UnixNano(),
				ExpiresAt: time.Now().Add(24 * time.Hour).UnixNano(), // Expires in 24 hours
			}

			// Call the Publish RPC
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			if _, err := client.Publish(ctx, message); err != nil {
				log.Printf("Failed to publish message: %v", err)
			}
		}(i)
	}

	// Wait for all goroutines to finish
	wg.Wait()

	// Record the end time for metrics
	endTime := time.Now()

	// Calculate and log the messages per second
	duration := endTime.Sub(startTime).Seconds()
	messagesPerSecond := float64(*rowCount) / duration
	log.Printf("Published %d messages in %.2f seconds (%.2f messages per second)\n", *rowCount, duration, messagesPerSecond)
}

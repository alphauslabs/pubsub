package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"

	pubsubproto "bulkwrite/bulkwrite_proto"
)

var (
	serverAddr = flag.String("server-addr", "10.146.0.46:50051", "Address of the gRPC server")
	topicID    = flag.String("topic-id", "test-topic", "Topic ID to publish messages to")
	rowCount   = flag.Int("row-count", 10000, "Number of rows to publish")
)

func main() {
	flag.Parse()

	// Set up a connection to the server
	conn, err := grpc.Dial(*serverAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	// Create a gRPC client
	client := pubsubproto.NewPubSubServiceClient(conn)

	// Record the start time for metrics
	startTime := time.Now()

	// Publish messages
	for i := 0; i < *rowCount; i++ {
		// Create a message to publish
		message := &pubsubproto.Message{
			Id:        fmt.Sprintf("msg-%d", i), // Unique message ID
			TopicId:   *topicID,
			Payload:   fmt.Sprintf("Bulkwrite_row_no_%d", i),
			CreatedAt: time.Now().UnixNano(),
			ExpiresAt: time.Now().Add(24 * time.Hour).UnixNano(), // Expires in 24 hours
		}

		// Call the Publish RPC
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := client.Publish(ctx, message)
		if err != nil {
			log.Fatalf("Failed to publish message: %v", err)
		}
	}

	// Record the end time for metrics
	endTime := time.Now()

	// Calculate and log the messages per second
	duration := endTime.Sub(startTime).Seconds()
	messagesPerSecond := float64(*rowCount) / duration
	log.Printf("Published %d messages in %.2f seconds (%.2f messages per second)\n", *rowCount, duration, messagesPerSecond)
}

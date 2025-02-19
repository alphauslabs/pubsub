package main

import (
	"context"
	"flag"
	"log"
	"time"

	"google.golang.org/grpc"

	pubsubproto "bulkwrite/bulkwrite_proto"
)

var (
	serverAddr = flag.String("server-addr", "10.146.0.46:50051", "Address of the gRPC server")
	topicID    = flag.String("topic-id", "test-topic", "Topic ID to publish messages to")
	payload    = flag.String("payload", "Hello, World!", "Payload of the message")
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

	// Create a message to publish
	message := &pubsubproto.Message{
		Id:        "msg-" + time.Now().Format("20060102-150405"), // Unique message ID
		TopicId:   *topicID,
		Payload:   *payload,
		CreatedAt: time.Now().UnixNano(),
		ExpiresAt: time.Now().Add(24 * time.Hour).UnixNano(), // Expires in 24 hours
	}

	// Call the Publish RPC
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	response, err := client.Publish(ctx, message)
	if err != nil {
		log.Fatalf("Failed to publish message: %v", err)
	}

	log.Printf("Message published successfully! Message ID: %s\n", response.MessageId)
}

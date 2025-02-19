package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pb "bulkwrite/bulkwrite_proto" // Replace with the correct import path

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
)

const (
	connectTimeout = 5 * time.Second // Timeout for connection setup
	rpcTimeout     = 3 * time.Second // Timeout for RPC calls
)

func main() {
	log.Println("Starting gRPC client...")

	// Define command-line flags for IP and port
	serverIP := flag.String("ip", "35.243.83.115", "IP address of the server")
	serverPort := flag.String("port", "50051", "Port number of the server")
	flag.Parse()

	// Create server address from IP and port
	serverAddr := fmt.Sprintf("%s:%s", *serverIP, *serverPort)

	// Configure connection options
	dialOptions := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithBlock(),
		grpc.WithTimeout(connectTimeout), // Ensures connection does not block forever
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff:           backoff.Config{MaxDelay: 2 * time.Second}, // Retry delay
			MinConnectTimeout: 2 * time.Second,
		}),
	}

	// Attempt to connect to the server
	log.Printf("Connecting to server at %s...\n", serverAddr)
	conn, err := grpc.Dial(serverAddr, dialOptions...)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	log.Println("Successfully connected to gRPC server.")

	client := pb.NewPubSubServiceClient(conn)

	// Create a message
	message := &pb.Message{
		Id:        "paultest",
		TopicId:   "topic-456",
		Payload:   []byte("Hello, World!"),
		CreatedAt: time.Now().Unix(),
		ExpiresAt: time.Now().Add(time.Hour).Unix(),
	}

	// Create a context with timeout for RPC call
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	log.Println("Sending message to gRPC server...")
	response, err := client.Publish(ctx, message)
	if err != nil {
		log.Fatalf("Publish failed: %v", err)
	}

	log.Printf("Message published successfully. ID: %s\n", response.MessageId)
}

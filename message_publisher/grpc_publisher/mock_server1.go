package main

import (
	"context"
	"fmt"
	"log"
	"net"

	pubsubproto "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
)

type server struct {
	pubsubproto.UnimplementedPubSubServiceServer
}

func (s *server) Publish(ctx context.Context, msg *pubsubproto.Message) (*pubsubproto.PublishResponse, error) {
	// Simulate processing of the message
	fmt.Printf("Received message on server 1: %s\n", msg.Payload)

	// Mock response (returning a dummy message ID)
	return &pubsubproto.PublishResponse{
		MessageId: "mock-message-id-1", // Simulate a generated message ID
	}, nil
}

func startMockServer() {
	// Start the mock gRPC server
	lis, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen on port 8080: %v", err)
	}

	s := grpc.NewServer()
	pubsubproto.RegisterPubSubServiceServer(s, &server{})

	log.Println("Mock server 1 started on port 8080")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func main() {
	startMockServer()
}

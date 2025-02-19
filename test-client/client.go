package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
)

const (
	serverAddr = "localhost:50051"
)

func main() {
	// Connect to the server
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(10*time.Second))
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewPubSubServiceClient(conn)

	// Create a new topic
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	topicReq := &pb.CreateTopicRequest{Name: "test-topic"}
	topicRes, err := client.CreateTopic(ctx, topicReq)
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	fmt.Printf("Created Topic: %v\n", topicRes)
}

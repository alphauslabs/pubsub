package main

import (
	"context"
	"log"
	"net"
	topic "tidy/crud-topic"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
)

const (
	projectID  = "labs-169405"
	instanceID = "alphaus-dev"
	databaseID = "main"
)

func main() {
	// Initialize Spanner client
	ctx := context.Background()
	dbPath := "projects/" + projectID + "/instances/" + instanceID + "/databases/" + databaseID
	client, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer client.Close()

	// Setup gRPC server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()

	// Register the TopicService
	topicService := &topic.TopicService{SpannerClient: client}
	pb.RegisterPubSubServiceServer(grpcServer, topicService)

	log.Println("Server is running on port 50051...")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

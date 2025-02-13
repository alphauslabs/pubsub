// cmd/server/main.go
package main

import (
	"cloud.google.com/go/spanner"
	"context"
	"github.com/alphauslabs/pubsub/internal/server"
	pb "github.com/alphauslabs/pubsub/protos/pubsub"
	"google.golang.org/grpc"
	"log"
	"net"
)

func main() {
	ctx := context.Background()

	// Initialize Spanner client
	client, err := spanner.NewClient(ctx, "projects/labs-169405/instances/alphaus-dev/databases/main")
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer client.Close()

	// Create gRPC server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterTopicServiceServer(s, server.NewTopicServer(client))

	log.Printf("Server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

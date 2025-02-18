package main

import (
	"context"
	"fmt"
	"log"
	"net"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

const (
	database = "projects/labs-169405/instances/alphaus-dev/databases/main"
	table    = "Messages"
)

var spannerClient *spanner.Client

type server struct {
	pb.UnimplementedPubSubServiceServer
}

func main() {
	ctx := context.Background()
	var err error
	spannerClient, err = spanner.NewClient(ctx, database)
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer spannerClient.Close()

	// Define port as a variable
	port := 8085

	// Start gRPC server
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterPubSubServiceServer(s, &server{})
	log.Printf("gRPC server is running on port %d", port)
	if err := s.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

func (s *server) Publish(ctx context.Context, msg *pb.Message) (*pb.PublishResponse, error) {
	messageID := uuid.New().String()

	mutation := spanner.InsertOrUpdate(
		table,
		[]string{"id", "payload", "createdAt", "updatedAt", "topic"},
		[]interface{}{messageID, msg.Payload, spanner.CommitTimestamp, spanner.CommitTimestamp, msg.TopicId},
	)

	_, err := spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		log.Printf("Error writing to Spanner: %v", err)
		return nil, err
	}

	log.Printf("Successfully wrote message to Spanner with ID: %s", messageID)
	return &pb.PublishResponse{MessageId: messageID}, nil
}

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
)

const (
	port     = ":8086"
	database = "projects/labs-169405/instances/alphaus-dev/databases/main"
	table    = "Messages"
)

var spannerClient *spanner.Client

type server struct {
	pb.UnimplementedPubSubServiceServer
}

// gRPC Publish Method - Writes Message to Spanner
func (s *server) Publish(ctx context.Context, msg *pb.Message) (*pb.PublishResponse, error) {
	msg.Id = uuid.New().String() // Assign unique ID

	// Perform Spanner write
	if err := insertMessage(ctx, msg); err != nil {
		return nil, fmt.Errorf("Spanner write failed: %v", err)
	}

	log.Printf("✅ Message successfully added to Spanner | ID: %s | Topic: %s", msg.Id, msg.Topic)

	return &pb.PublishResponse{MessageId: msg.Id}, nil
}

// Optimized direct Spanner write
func insertMessage(ctx context.Context, msg *pb.Message) error {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second) // Optimized timeout
	defer cancel()

	mutation := spanner.InsertOrUpdate(
		table,
		[]string{"id", "payload", "topic", "createdAt", "updatedAt"},
		[]interface{}{msg.Id, msg.Payload, msg.Topic, spanner.CommitTimestamp, spanner.CommitTimestamp},
	)

	_, err := spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
	return err
}

func main() {
	ctx := context.Background()
	var err error
	spannerClient, err = spanner.NewClient(ctx, database)
	if err != nil {
		log.Fatalf("❌ Failed to create Spanner client: %v", err)
	}
	defer spannerClient.Close()

	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("❌ Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterPubSubServiceServer(s, &server{})

	log.Printf(" gRPC server listening on %s", port)
	if err := s.Serve(listener); err != nil {
		log.Fatalf("❌ Failed to serve: %v", err)
	}
}

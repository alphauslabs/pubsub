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
	startTime := time.Now() // Start timing

	msg.Id = uuid.New().String()

	commitTime, err := insertMessage(ctx, msg)
	if err != nil {
		return nil, fmt.Errorf("Spanner write failed: %v", err)
	}

	// Calculate delay
	latency := commitTime.Sub(startTime)
	log.Printf("Message successfully written to Spanner | ID: %s | Latency: %v ms | CommitTime: %s", 
		msg.Id, latency.Milliseconds(), commitTime.Format(time.RFC3339Nano))

	return &pb.PublishResponse{MessageId: msg.Id}, nil
}


func getCommitTime(ctx context.Context, id string) (time.Time, error) {
	stmt := spanner.Statement{
		SQL:    "SELECT updatedAt FROM Messages WHERE id = @id",
		Params: map[string]interface{}{"id": id},
	}

	iter := spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var commitTime time.Time
	err := iter.Do(func(row *spanner.Row) error {
		return row.Columns(&commitTime)
	})

	return commitTime, err
}


// Optimized direct Spanner write
func insertMessage(ctx context.Context, msg *pb.Message) (time.Time, error) {
	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()

	mutation := spanner.InsertOrUpdate(
		table,
		[]string{"id", "payload", "topic", "createdAt", "updatedAt"},
		[]interface{}{msg.Id, msg.Payload, msg.Topic, spanner.CommitTimestamp, spanner.CommitTimestamp},
	)

	// Apply mutation
	err := spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		return time.Time{}, err
	}

	// Query the commit timestamp
	return getCommitTime(ctx, msg.Id)
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

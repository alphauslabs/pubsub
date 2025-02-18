package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	pb "github.com/alphauslabs/pubsub-proto/v1" // Import generated package
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
)

const (
	port     = ":8085"
	database = "projects/labs-169405/instances/alphaus-dev/databases/main"
	table    = "Messages"
)

var spannerClient *spanner.Client

type server struct {
	pb.UnimplementedPubSubServiceServer
}

// gRPC Publish Method - Writes Message to Spanner
func (s *server) Publish(ctx context.Context, msg *pb.Message) (*pb.PublishResponse, error) {
	startTotal := time.Now()

	msg.Id = uuid.New().String()
	msg.CreatedAt = time.Now().Unix()

	// Measure Spanner write time
	startSpanner := time.Now()
	err := insertMessage(ctx, msg)
	spannerDuration := time.Since(startSpanner)

	if err != nil {
		log.Printf("Error writing to Spanner: %v", err)
		return nil, fmt.Errorf("failed to write to Spanner: %v", err)
	}

	// Retrieve commit timestamp with timeout
	startCommit := time.Now()
	commitTime, err := getCommitTime(ctx, msg.Id)
	commitDuration := time.Since(startCommit)

	if err != nil {
		log.Printf("Error retrieving commit timestamp: %v", err)
		return nil, fmt.Errorf("failed to retrieve commit time: %v", err)
	}

	totalDuration := time.Since(startTotal)

	// Log performance metrics
	log.Printf("Write Operation Metrics: Total=%v ms | SpannerWrite=%v ms | CommitRetrieval=%v ms | CommitTime=%s",
		totalDuration.Milliseconds(), spannerDuration.Milliseconds(), commitDuration.Milliseconds(), commitTime)

	return &pb.PublishResponse{
		MessageId: msg.Id,
	}, nil
}

// Writes message directly to Spanner with a timeout
func insertMessage(ctx context.Context, msg *pb.Message) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second) // Timeout for DB write
	defer cancel()

	mutation := spanner.InsertOrUpdate(
		table,
		[]string{"id", "topic_id", "payload", "createdAt", "updatedAt"},
		[]interface{}{msg.Id, msg.TopicId, msg.Payload, spanner.CommitTimestamp, spanner.CommitTimestamp},
	)

	_, err := spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		log.Printf("Spanner write failed: %v", err)
	}
	return err
}

// Retrieves commit timestamp with a timeout
func getCommitTime(ctx context.Context, id string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second) // Timeout for DB read
	defer cancel()

	stmt := spanner.Statement{
		SQL:    "SELECT updatedAt FROM Messages WHERE id = @id",
		Params: map[string]interface{}{"id": id},
	}

	iter := spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var commitTime time.Time
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return "", err
		}
		if err := row.Columns(&commitTime); err != nil {
			return "", err
		}
	}

	return commitTime.Format(time.RFC3339Nano), nil
}

func main() {
	ctx := context.Background()
	var err error
	spannerClient, err = spanner.NewClient(ctx, database)
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer spannerClient.Close()

	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	pb.RegisterPubSubServiceServer(s, &server{})

	log.Printf("gRPC server listening on %s", port)
	if err := s.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

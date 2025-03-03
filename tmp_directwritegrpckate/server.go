package main

// import (
// 	"context"
// 	"fmt"
// 	"log"
// 	"net"
// 	"time"

// 	"cloud.google.com/go/spanner"
// 	pb "github.com/alphauslabs/pubsub-proto/v1"
// 	"github.com/golang/glog"
// 	"github.com/google/uuid"
// 	"google.golang.org/grpc"
// )

// const (
// 	port     = ":8086"
// 	database = "projects/labs-169405/instances/alphaus-dev/databases/main"
// 	table    = "Messages"
// )

// var spannerClient *spanner.Client

// type server struct {
// 	pb.UnimplementedPubSubServiceServer
// }

// // gRPC Publish Method - Writes Message to Spanner
// func (s *server) Publish(ctx context.Context, msg *pb.Message) (*pb.PublishResponse, error) {
// 	startTime := time.Now() // Start timing

// 	msg.Id = uuid.New().String()

// 	// Insert message into Spanner
// 	if err := insertMessage(ctx, msg); err != nil {
// 		return nil, fmt.Errorf("Spanner write failed: %v", err)
// 	}

// 	// Get commit timestamp
// 	commitTime, err := getCommitTime(ctx, msg.Id)
// 	if err != nil {
// 		return nil, fmt.Errorf("Failed to get commit timestamp: %v", err)
// 	}

// 	// Calculate delay
// 	latency := commitTime.Sub(startTime)
// 	glog.Infof("✅ Message written to Spanner | ID: %s | Latency: %v ms | CommitTime: %s",
// 		msg.Id, latency.Milliseconds(), commitTime.Format(time.RFC3339Nano))

// 	return &pb.PublishResponse{MessageId: msg.Id}, nil
// }

// // Inserts a message into Spanner
// func insertMessage(ctx context.Context, msg *pb.Message) error {
// 	ctx, cancel := context.WithTimeout(ctx, 120*time.Second)
// 	defer cancel()

// 	mutation := spanner.InsertOrUpdate(
// 		table,
// 		[]string{"id", "payload", "topic", "createdAt", "updatedAt"},
// 		[]interface{}{msg.Id, msg.Payload, msg.Topic, spanner.CommitTimestamp, spanner.CommitTimestamp},
// 	)

// 	// Apply mutation
// 	_, err := spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
// 	return err
// }

// // Queries Spanner to get the commit timestamp of the inserted message
// func getCommitTime(ctx context.Context, messageID string) (time.Time, error) {
// 	stmt := spanner.Statement{
// 		SQL:    "SELECT createdAt FROM Messages WHERE id = @id",
// 		Params: map[string]interface{}{"id": messageID},
// 	}

// 	iter := spannerClient.Single().Query(ctx, stmt)
// 	defer iter.Stop()

// 	row, err := iter.Next()
// 	if err != nil {
// 		return time.Time{}, err
// 	}

// 	var commitTime time.Time
// 	if err := row.Columns(&commitTime); err != nil {
// 		return time.Time{}, err
// 	}

// 	return commitTime, nil
// }

// func main() {
// 	ctx := context.Background()
// 	var err error
// 	spannerClient, err = spanner.NewClient(ctx, database)
// 	if err != nil {
// 		log.Fatalf("❌ Failed to create Spanner client: %v", err)
// 	}
// 	defer spannerClient.Close()

// 	listener, err := net.Listen("tcp", port)
// 	if err != nil {
// 		log.Fatalf("❌ Failed to listen: %v", err)
// 	}

// 	s := grpc.NewServer()
// 	pb.RegisterPubSubServiceServer(s, &server{})

// 	glog.Infof("🚀 gRPC server listening on %s", port)
// 	if err := s.Serve(listener); err != nil {
// 		log.Fatalf("❌ Failed to serve: %v", err)
// 	}
// }

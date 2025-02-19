package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	spanner "cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc"
)

const (
	spannerDB = "projects/labs-169405/instances/alphaus-dev/databases/main"
)

type pubSubServer struct {
	pb.UnimplementedPubSubServiceServer
	spannerClient *spanner.Client
}

func (s *pubSubServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	id := fmt.Sprintf("topic-%d", time.Now().UnixNano())
	mutation := spanner.Insert("Messages",
		[]string{"id", "payload", "createdAt", "updatedAt", "topic"},
		[]interface{}{id, "", spanner.CommitTimestamp, spanner.CommitTimestamp, req.Name})

	_, err := s.spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		log.Printf("Failed to insert into Spanner: %v", err) // Debugging log
		return nil, fmt.Errorf("failed to create topic: %v", err)
	}
	log.Println("Successfully created topic:", id) // Debugging log
	return &pb.Topic{TopicId: id, Name: req.Name}, nil
}

func (s *pubSubServer) GetTopic(ctx context.Context, req *pb.GetTopicRequest) (*pb.Topic, error) {
	row, err := s.spannerClient.Single().ReadRow(ctx, "Messages", spanner.Key{req.Id}, []string{"id", "topic"})
	if err != nil {
		return nil, fmt.Errorf("failed to get topic: %v", err)
	}
	var id, name string
	if err := row.Columns(&id, &name); err != nil {
		return nil, err
	}
	return &pb.Topic{TopicId: id, Name: name}, nil
}

func (s *pubSubServer) ListTopics(ctx context.Context, req *pb.Empty) (*pb.ListTopicsResponse, error) {
	stmt := spanner.Statement{SQL: "SELECT id, name FROM Messages"}
	iter := s.spannerClient.Single().Query(ctx, stmt)
	topics := []*pb.Topic{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var id, name string
		if err := row.Columns(&id, &name); err != nil {
			return nil, err
		}
		topics = append(topics, &pb.Topic{TopicId: id, Name: name})
	}
	return &pb.ListTopicsResponse{Topics: topics}, nil
}

// func newServer(spannerClient *spanner.Client) *topic.TopicService {
// 	return &topic.TopicService{SpannerClient: spannerClient}
// }

func main() {
	ctx := context.Background()

	client, err := spanner.NewClient(ctx, spannerDB)
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer client.Close()

	// Start gRPC server
	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	server := &pubSubServer{spannerClient: client}     // Create instance
	pb.RegisterPubSubServiceServer(grpcServer, server) // Register properly

	log.Println("gRPC server is running on port 50051")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

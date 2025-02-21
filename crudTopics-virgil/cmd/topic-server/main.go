package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "crud-api-topics/api/pubsub-proto-file" // TBA protobuff import path
	"crud-api-topics/internal/topic"           // Your internal topic package

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type server struct {
	pb.UnimplementedTopicServiceServer
	spannerClient *spanner.Client
}

func (s *server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	t := &topic.Topic{
		TopicID: req.Topic.TopicID,
		Name:    req.Topic.Name,
	}

	err := topic.CreateTopic(ctx, s.spannerClient, t)
	if err != nil {
		return &pb.CreateTopicResponse{Error: err.Error()}, err
	}

	pbTopic := &pb.Topic{
		TopicID:   t.TopicID,
		Name:      t.Name,
		CreatedAt: timestamppb.New(t.CreatedAt),
		UpdatedAt: timestamppb.New(t.UpdatedAt),
	}

	return &pb.CreateTopicResponse{Topic: pbTopic}, nil
}

func (s *server) GetTopic(ctx context.Context, req *pb.GetTopicRequest) (*pb.GetTopicResponse, error) {
	t, err := topic.GetTopic(ctx, s.spannerClient, req.TopicID)
	if err != nil {
		return &pb.GetTopicResponse{Error: err.Error()}, err
	}
	if t == nil {
		return &pb.GetTopicResponse{Error: "topic not found"}, nil
	}

	pbTopic := &pb.Topic{
		TopicID:   t.TopicID,
		Name:      t.Name,
		CreatedAt: timestamppb.New(t.CreatedAt),
		UpdatedAt: timestamppb.New(t.UpdatedAt),
	}

	return &pb.GetTopicResponse{Topic: pbTopic}, nil
}

func (s *server) UpdateTopic(ctx context.Context, req *pb.UpdateTopicRequest) (*pb.UpdateTopicResponse, error) {
	t := &topic.Topic{
		TopicID: req.Topic.TopicID,
		Name:    req.Topic.Name,
	}

	err := t.UpdateTopic(ctx, s.spannerClient)
	if err != nil {
		return &pb.UpdateTopicResponse{Error: err.Error()}, err
	}

	pbTopic := &pb.Topic{
		TopicID:   t.TopicID,
		Name:      t.Name,
		CreatedAt: timestamppb.New(t.CreatedAt),
		UpdatedAt: timestamppb.New(t.UpdatedAt),
	}

	return &pb.UpdateTopicResponse{Topic: pbTopic}, nil
}

func (s *server) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	err := topic.DeleteTopic(ctx, s.spannerClient, req.TopicID)
	if err != nil {
		return &pb.DeleteTopicResponse{Error: err.Error()}, err
	}

	return &pb.DeleteTopicResponse{}, nil
}

func (s *server) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	topics, err := topic.ListTopics(ctx, s.spannerClient)
	if err != nil {
		return &pb.ListTopicsResponse{Error: err.Error()}, err
	}

	var pbTopics []*pb.Topic
	for _, t := range topics {
		pbTopics = append(pbTopics, &pb.Topic{
			TopicID:   t.TopicID,
			Name:      t.Name,
			CreatedAt: timestamppb.New(t.CreatedAt),
			UpdatedAt: timestamppb.New(t.UpdatedAt),
		})
	}

	return &pb.ListTopicsResponse{Topics: pbTopics}, nil
}

func main() {
	// Initialize Spanner client
	ctx := context.Background()
	spannerClient, err := spanner.NewClient(ctx, "your-database")
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer spannerClient.Close()

	// Start gRPC server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterTopicServiceServer(s, &server{spannerClient: spannerClient})

	// Handle graceful shutdown
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		s.GracefulStop()
	}()

	log.Printf("Server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

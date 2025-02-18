package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	pb "github.com/alphauslabs/pubsub/v1"
	"google.golang.org/grpc"
)

// In-memory storage for topics (acts like a database)
type Server struct {
	pb.UnimplementedPubSubServiceServer
	mu     sync.Mutex
	topics map[string]*pb.Topic
}

// Create a new topic
func (s *Server) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Generate a new ID (just using len for simplicity)
	id := fmt.Sprintf("%d", len(s.topics)+1)

	topic := &pb.Topic{
		Id:   id,
		Name: req.Name,
	}

	s.topics[id] = topic
	return topic, nil
}

// Get a topic by ID
func (s *Server) GetTopic(ctx context.Context, req *pb.GetTopicRequest) (*pb.Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	topic, exists := s.topics[req.Id]
	if !exists {
		return nil, fmt.Errorf("topic not found")
	}

	return topic, nil
}

// Update a topic by ID
func (s *Server) UpdateTopic(ctx context.Context, req *pb.UpdateTopicRequest) (*pb.Topic, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	topic, exists := s.topics[req.Id]
	if !exists {
		return nil, fmt.Errorf("topic not found")
	}

	topic.Name = req.Name
	s.topics[req.Id] = topic
	return topic, nil
}

// Delete a topic by ID
func (s *Server) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.topics[req.Id]
	if !exists {
		return nil, fmt.Errorf("topic not found")
	}

	delete(s.topics, req.Id)
	return &pb.DeleteTopicResponse{Success: true}, nil
}

// List all topics
func (s *Server) ListTopics(ctx context.Context, req *pb.Empty) (*pb.ListTopicsResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var topics []*pb.Topic
	for _, topic := range s.topics {
		topics = append(topics, topic)
	}

	return &pb.ListTopicsResponse{Topics: topics}, nil
}

func main() {
	// Initialize the server
	server := &Server{
		topics: make(map[string]*pb.Topic),
	}

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPubSubServiceServer(grpcServer, server)

	log.Println("gRPC server listening on :50051")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

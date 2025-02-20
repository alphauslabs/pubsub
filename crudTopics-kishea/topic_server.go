// internal/server/topic_server.go
package server

import (
	"context"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "github.com/alphauslabs/pubsub/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TopicServer implements the TopicService gRPC interface.
type TopicServer struct {
	pb.UnimplementedTopicServiceServer
	spannerClient *spanner.Client
}

// NewTopicServer creates a new TopicServer instance.
func NewTopicServer(client *spanner.Client) *TopicServer {
	return &TopicServer{spannerClient: client}
}

// CreateTopic creates a new topic in Cloud Spanner.
func (s *TopicServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic name is required")
	}

	topicID := uuid.New().String()
	now := time.Now()
	mutation := spanner.Insert("Topics",
		[]string{"TopicID", "Name", "CreatedAt", "UpdatedAt"},
		[]interface{}{topicID, req.GetName(), now, now},
	)
	_, err := s.spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to create topic: %v", err)
	}

	return &pb.Topic{
		TopicId:   topicID,
		Name:      req.GetName(),
		CreatedAt: timestamppb.New(now),
		UpdatedAt: timestamppb.New(now),
	}, nil
}

// GetTopic retrieves a topic by its ID.
func (s *TopicServer) GetTopic(ctx context.Context, req *pb.GetTopicRequest) (*pb.Topic, error) {
	row, err := s.spannerClient.Single().ReadRow(ctx, "Topics",
		spanner.Key{req.GetTopicId()},
		[]string{"TopicID", "Name", "CreatedAt", "UpdatedAt"},
	)
	if err == spanner.ErrNoRows {
		return nil, status.Error(codes.NotFound, "Topic not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to read topic: %v", err)
	}

	var topic pb.Topic
	var createdAt, updatedAt time.Time
	if err := row.Columns(&topic.TopicId, &topic.Name, &createdAt, &updatedAt); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to scan topic row: %v", err)
	}

	topic.CreatedAt = timestamppb.New(createdAt)
	topic.UpdatedAt = timestamppb.New(updatedAt)
	return &topic, nil
}

// UpdateTopic updates an existing topic.
func (s *TopicServer) UpdateTopic(ctx context.Context, req *pb.UpdateTopicRequest) (*pb.Topic, error) {
	if req.GetTopicId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic ID is required")
	}
	if req.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic name is required")
	}

	now := time.Now()
	mutation := spanner.Update("Topics",
		[]string{"TopicID", "Name", "UpdatedAt"},
		[]interface{}{req.GetTopicId(), req.GetName(), now},
	)
	_, err := s.spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to update topic: %v", err)
	}

	return s.GetTopic(ctx, &pb.GetTopicRequest{TopicId: req.GetTopicId()})
}

// DeleteTopic deletes a topic.
func (s *TopicServer) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*emptypb.Empty, error) {
	if req.GetTopicId() == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic ID is required")
	}

	mutation := spanner.Delete("Topics", spanner.Key{req.GetTopicId()})
	_, err := s.spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to delete topic: %v", err)
	}
	return &emptypb.Empty{}, nil
}

// ListTopics retrieves all topics (simplified; pagination not fully implemented).
func (s *TopicServer) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	stmt := spanner.Statement{
		SQL: "SELECT TopicID, Name, CreatedAt, UpdatedAt FROM Topics",
	}
	iter := s.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var topics []*pb.Topic
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to iterate topics: %v", err)
		}
		var topic pb.Topic
		var createdAt, updatedAt time.Time
		if err := row.Columns(&topic.TopicId, &topic.Name, &createdAt, &updatedAt); err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to scan row: %v", err)
		}
		topic.CreatedAt = timestamppb.New(createdAt)
		topic.UpdatedAt = timestamppb.New(updatedAt)
		topics = append(topics, &topic)
	}

	return &pb.ListTopicsResponse{
		Topics: topics,
		// For simplicity, pagination is omitted; next_page_token remains empty.
		NextPageToken: "",
	}, nil
}

package topic

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TopicService implements the PubSubService server interface for topic management
type TopicService struct {
	pb.UnimplementedPubSubServiceServer
	SpannerClient *spanner.Client
}

// NewTopicService creates a new TopicService with the provided Spanner client
func NewTopicService(client *spanner.Client) *TopicService {
	return &TopicService{
		SpannerClient: client,
	}
}

// CreateTopic implements the CreateTopic RPC method
func (s *TopicService) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic name is required")
	}

	// Generate a unique ID for the topic
	topicID := uuid.New().String()

	// Current timestamp for created/updated fields
	now := time.Now()

	// Insert the topic record into the Messages table
	m := spanner.Insert("Messages",
		[]string{"id", "payload", "createdAt", "updatedAt", "topic"},
		[]interface{}{topicID, fmt.Sprintf("Topic created: %s", req.Name), now, now, req.Name})

	_, err := s.SpannerClient.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to create topic: %v", err)
	}

	// Return the created topic
	return &pb.Topic{
		Name: req.Name,
	}, nil
}

// GetTopic implements the GetTopic RPC method
func (s *TopicService) GetTopic(ctx context.Context, req *pb.GetTopicRequest) (*pb.Topic, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic ID is required")
	}

	// Query to find the topic
	stmt := spanner.Statement{
		SQL: `SELECT id, topic FROM Messages WHERE id = @id LIMIT 1`,
		Params: map[string]interface{}{
			"id": req.Id,
		},
	}

	iter := s.SpannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		if err == iterator.Done {
			return nil, status.Errorf(codes.NotFound, "Topic not found with ID: %s", req.Id)
		}
		return nil, status.Errorf(codes.Internal, "Failed to query topic: %v", err)
	}

	var id, name string
	if err := row.Columns(&id, &name); err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to parse topic data: %v", err)
	}

	return &pb.Topic{
		Name: name,
	}, nil
}

// UpdateTopic implements the UpdateTopic RPC method
func (s *TopicService) UpdateTopic(ctx context.Context, req *pb.UpdateTopicRequest) (*pb.Topic, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic ID is required")
	}

	if req.NewName == nil || *req.NewName == "" {
		return nil, status.Error(codes.InvalidArgument, "New topic name is required")
	}

	// Update timestamp
	now := time.Now()

	// Update the topic name
	m := spanner.Update("Messages",
		[]string{"id", "topic", "updatedAt"},
		[]interface{}{req.Id, *req.NewName, now})

	_, err := s.SpannerClient.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to update topic: %v", err)
	}

	return &pb.Topic{
		Name: *req.NewName,
	}, nil
}

// DeleteTopic implements the DeleteTopic RPC method
func (s *TopicService) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic ID is required")
	}

	// Delete the topic
	m := spanner.Delete("Messages", spanner.Key{req.Id})
	_, err := s.SpannerClient.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to delete topic: %v", err)
	}

	return &pb.DeleteTopicResponse{
		Success: true,
	}, nil
}

// ListTopics implements the ListTopics RPC method
func (s *TopicService) ListTopics(ctx context.Context, _ *pb.Empty) (*pb.ListTopicsResponse, error) {
	// Query to get all unique topics
	stmt := spanner.Statement{
		SQL: `SELECT id, topic FROM Messages GROUP BY id, topic`,
	}

	iter := s.SpannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var topics []*pb.Topic
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to list topics: %v", err)
		}

		var id, name string
		if err := row.Columns(&id, &name); err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to parse topic data: %v", err)
		}

		topics = append(topics, &pb.Topic{
			Name: name,
		})
	}

	return &pb.ListTopicsResponse{
		Topics: topics,
	}, nil
}

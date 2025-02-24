package topic

import (
	"context"

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

	// Insert the topic record into the Messages table
	m := spanner.Insert("Topics",
		[]string{"id", "name", "createdAt", "updatedAt"},
		[]interface{}{topicID, req.Name, spanner.CommitTimestamp, spanner.CommitTimestamp})

	_, err := s.SpannerClient.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to create topic: %v", err)
	}

	// Return the created topic
	return &pb.Topic{
		Id:   topicID, // Include ID
		Name: req.Name,
	}, nil
}

// GetTopic implements the GetTopic RPC method
// GetTopic retrieves a topic by name
func (s *TopicService) GetTopic(ctx context.Context, req *pb.GetTopicRequest) (*pb.Topic, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic name is required")
	}

	stmt := spanner.Statement{
		SQL:    `SELECT id, name FROM Topics WHERE id = @id LIMIT 1`,
		Params: map[string]interface{}{"name": req.Id},
	}

	iter := s.SpannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		if err == iterator.Done {
			return nil, status.Errorf(codes.NotFound, "Topic %s not found", req.Id)
		}
		return nil, status.Errorf(codes.Internal, "Query failed: %v", err)
	}

	var id, topic string
	if err := row.Columns(&id, &topic); err != nil {
		return nil, status.Errorf(codes.Internal, "Data parse error: %v", err)
	}

	return &pb.Topic{Id: id, Name: topic}, nil
}

// UpdateTopic implements the UpdateTopic RPC method
func (s *TopicService) UpdateTopic(ctx context.Context, req *pb.UpdateTopicRequest) (*pb.Topic, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic ID is required")
	}

	if req.NewName == "" {
		return nil, status.Error(codes.InvalidArgument, "New topic name is required")
	}

	// Update the topic name
	m := spanner.Update("Topics",
		[]string{"id", "name", "updatedAt"},                         // Include primary key
		[]interface{}{req.Id, req.NewName, spanner.CommitTimestamp}) // Use ID from request

	_, err := s.SpannerClient.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to update topic: %v", err)
	}

	return &pb.Topic{
		Id:   req.Id,
		Name: req.NewName,
	}, nil
}

// DeleteTopic implements the DeleteTopic RPC method
func (s *TopicService) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "Topic name is required")
	}

	// Delete the topic
	m := spanner.Delete("Topics", spanner.Key{req.Id})
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
		SQL: `SELECT id, name FROM Topics`,
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
			Id:   id,
			Name: name,
		})

	}

	return &pb.ListTopicsResponse{
		Topics: topics,
	}, nil
}

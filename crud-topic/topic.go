package topic

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/flowerinthenight/hedge/v2"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TopicService struct {
	pb.UnimplementedPubSubServiceServer
	SpannerClient *spanner.Client
	HedgeOp       *hedge.Op
}

func NewTopicService(client *spanner.Client, op *hedge.Op) *TopicService {
	return &TopicService{
		SpannerClient: client,
		HedgeOp:       op,
	}
}

func (s *TopicService) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "topic name is required")
	}

	// Check for existing topic name
	exist, err := s.topicExists(ctx, req.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check topic existence: %v", err)
	}
	if exist {
		return nil, status.Errorf(codes.AlreadyExists, "topic name %q already exists", req.Name)
	}

	topicID := uuid.New().String()
	m := spanner.Insert("Topics",
		[]string{"id", "name", "createdAt", "updatedAt"},
		[]interface{}{topicID, req.Name, spanner.CommitTimestamp, spanner.CommitTimestamp},
	)

	_, err = s.SpannerClient.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create topic: %v", err)
	}

	topic := &pb.Topic{
		Id:   topicID,
		Name: req.Name,
	}

	if err := s.notifyLeader(ctx, 1); err != nil {
		log.Printf("Failed to notify leader: %v", err)
	}
	return topic, nil
}

func (s *TopicService) GetTopic(ctx context.Context, req *pb.GetTopicRequest) (*pb.Topic, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "topic ID is required")
	}

	stmt := spanner.Statement{
		SQL:    `SELECT id, name, createdAt, updatedAt FROM Topics WHERE id = @id LIMIT 1`,
		Params: map[string]interface{}{"id": req.Id},
	}

	iter := s.SpannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return nil, status.Errorf(codes.NotFound, "topic with ID %q not found", req.Id)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query topic: %v", err)
	}

	var (
		id, name             string
		createdAt, updatedAt spanner.NullTime
	)
	if err := row.Columns(&id, &name, &createdAt, &updatedAt); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse topic data: %v", err)
	}

	return &pb.Topic{
		Id:        id,
		Name:      name,
		CreatedAt: convertTime(createdAt),
		UpdatedAt: convertTime(updatedAt),
	}, nil
}

func (s *TopicService) UpdateTopic(ctx context.Context, req *pb.UpdateTopicRequest) (*pb.Topic, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "topic ID is required")
	}
	if req.NewName == "" {
		return nil, status.Error(codes.InvalidArgument, "new topic name is required")
	}

	// Check if new name already exists
	exist, err := s.topicExists(ctx, req.NewName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check name availability: %v", err)
	}
	if exist {
		return nil, status.Errorf(codes.AlreadyExists, "topic name %q already exists", req.NewName)
	}

	// Get existing topic to verify existence
	current, err := s.GetTopic(ctx, &pb.GetTopicRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}

	m := spanner.Update("Topics",
		[]string{"id", "name", "updatedAt"},
		[]interface{}{current.Id, req.NewName, spanner.CommitTimestamp},
	)

	_, err = s.SpannerClient.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update topic: %v", err)
	}

	updatedTopic := &pb.Topic{
		Id:   current.Id,
		Name: req.NewName,
	}

	if err := s.notifyLeader(ctx, 1); err != nil {
		log.Printf("Failed to notify leader: %v", err)
	}

	return updatedTopic, nil
}

func (s *TopicService) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "topic ID is required")
	}

	// Verify topic exists before deletion
	if _, err := s.GetTopic(ctx, &pb.GetTopicRequest{Id: req.Id}); err != nil {
		return nil, err
	}

	m := spanner.Delete("Topics", spanner.Key{req.Id})
	_, err := s.SpannerClient.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete topic: %v", err)
	}
	return &pb.DeleteTopicResponse{Success: true}, nil
}

func (s *TopicService) ListTopics(ctx context.Context, _ *pb.Empty) (*pb.ListTopicsResponse, error) {
	stmt := spanner.Statement{SQL: `SELECT id, name, createdAt, updatedAt FROM Topics`}
	iter := s.SpannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var topics []*pb.Topic
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to list topics: %v", err)
		}

		var (
			id, name             string
			createdAt, updatedAt spanner.NullTime
		)
		if err := row.Columns(&id, &name, &createdAt, &updatedAt); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to parse topic data: %v", err)
		}

		topics = append(topics, &pb.Topic{
			Id:        id,
			Name:      name,
			CreatedAt: convertTime(createdAt),
			UpdatedAt: convertTime(updatedAt),
		})
	}

	return &pb.ListTopicsResponse{Topics: topics}, nil
}

// Helper functions
func (s *TopicService) topicExists(ctx context.Context, name string) (bool, error) {
	stmt := spanner.Statement{
		SQL:    "SELECT 1 FROM Topics WHERE name = @name LIMIT 1",
		Params: map[string]interface{}{"name": name},
	}

	iter := s.SpannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	_, err := iter.Next()
	if err == iterator.Done {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("existence check failed: %w", err)
	}
	return true, nil
}

func convertTime(t spanner.NullTime) *timestamppb.Timestamp {
	if !t.Valid {
		return nil
	}
	return timestamppb.New(t.Time)
}

func (s *TopicService) notifyLeader(ctx context.Context, flag int) error {
	data := map[string]interface{}{
		"flag": flag,
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	resp := s.HedgeOp.Broadcast(ctx, jsonData)
	for _, r := range resp {
		if r.Error != nil {
			return fmt.Errorf("failed to broadcast to %s: %w", r.Id, r.Error)
		}
	}
	return nil
}

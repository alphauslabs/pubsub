package subscription

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

const (
	projectID  = "labs-169405"
	instanceID = "alphaus-dev"
	databaseID = "main"
)

type SubscriptionService struct {
	SpannerClient *spanner.Client
	HedgeOp       *hedge.Op
	pb.UnimplementedPubSubServiceServer
}

func NewSubscriptionService(client *spanner.Client, op *hedge.Op) *SubscriptionService {
	return &SubscriptionService{
		SpannerClient: client,
		HedgeOp:       op,
	}
}

func (s *SubscriptionService) CreateSubscription(ctx context.Context, req *pb.CreateSubscriptionRequest) (*pb.Subscription, error) {
	if req.TopicId == "" {
		return nil, status.Error(codes.InvalidArgument, "topic ID is required")
	}

	// Generate a new subscription name
	subscriptionName := "sub-" + uuid.New().String()

	// Check if subscription name already exists
	exist, err := s.subscriptionExists(ctx, subscriptionName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check name availability: %v", err)
	}
	if exist {
		return nil, status.Errorf(codes.AlreadyExists, "subscription name %q already exists", subscriptionName)
	}

	subscriptionID := uuid.New().String()
	m := spanner.Insert("Subscriptions_beta",
		[]string{"id", "name", "topic", "createdAt", "updatedAt"},
		[]interface{}{subscriptionID, subscriptionName, req.TopicId, spanner.CommitTimestamp, spanner.CommitTimestamp},
	)

	_, err = s.SpannerClient.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create subscription: %v", err)
	}

	subscription := &pb.Subscription{
		Id:      subscriptionID,
		TopicId: req.TopicId,
	}

	if err := s.notifyLeader(ctx, 1); err != nil {
		log.Printf("Failed to notify leader: %v", err)
	}
	return subscription, nil
}

func (s *SubscriptionService) GetSubscription(ctx context.Context, req *pb.GetSubscriptionRequest) (*pb.Subscription, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "subscription ID is required")
	}

	stmt := spanner.Statement{
		SQL:    `SELECT id, name, topic, createdAt, updatedAt FROM Subscriptions_beta WHERE id = @id LIMIT 1`,
		Params: map[string]interface{}{"id": req.Id},
	}

	iter := s.SpannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return nil, status.Errorf(codes.NotFound, "subscription with ID %q not found", req.Id)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to query subscription: %v", err)
	}

	var (
		id, name, topic      string
		createdAt, updatedAt spanner.NullTime
	)
	if err := row.Columns(&id, &name, &topic, &createdAt, &updatedAt); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse subscription data: %v", err)
	}

	return &pb.Subscription{
		Id:      id,
		TopicId: topic,
	}, nil
}


//add missing fields to protofile
func (s *SubscriptionService) UpdateSubscription(ctx context.Context, req *pb.UpdateSubscriptionRequest) (*pb.Subscription, error) {
	if req.Id == "" || req.NewName == "" || req.TopicId == "" {
		return nil, status.Error(codes.InvalidArgument, "subscription ID, new name, and topic ID are required")
	}

	// Check if new name already exists
	exist, err := s.subscriptionExists(ctx, req.NewName)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check name availability: %v", err)
	}
	if exist {
		return nil, status.Errorf(codes.AlreadyExists, "subscription name %q already exists", req.NewName)
	}

	// Get existing subscription to verify existence
	current, err := s.GetSubscription(ctx, &pb.GetSubscriptionRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}

	m := spanner.Update("Subscriptions_beta",
		[]string{"id", "name", "topic", "updatedAt"},
		[]interface{}{current.Id, req.NewName, req.TopicId, spanner.CommitTimestamp},
	)

	_, err = s.SpannerClient.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update subscription: %v", err)
	}

	updatedSubscription := &pb.Subscription{
		Id:      current.Id,
		Name:    req.NewName,
		TopicId: req.TopicId,
	}

	if err := s.notifyLeader(ctx, 1); err != nil {
		log.Printf("Failed to notify leader: %v", err)
	}

	return updatedSubscription, nil
}

func (s *SubscriptionService) DeleteSubscription(ctx context.Context, req *pb.DeleteSubscriptionRequest) (*pb.DeleteSubscriptionResponse, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "subscription ID is required")
	}

	// Verify subscription exists before deletion
	if _, err := s.GetSubscription(ctx, &pb.GetSubscriptionRequest{Id: req.Id}); err != nil {
		return nil, err
	}

	m := spanner.Delete("Subscriptions_beta", spanner.Key{req.Id})
	_, err := s.SpannerClient.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete subscription: %v", err)
	}
	if err := s.notifyLeader(ctx, 1); err != nil {
		log.Printf("DeleteSubscription notification failed: %v", err)
	}
	return &pb.DeleteSubscriptionResponse{Success: true}, nil
}

func (s *SubscriptionService) ListSubscriptions(ctx context.Context, _ *pb.Empty) (*pb.ListSubscriptionsResponse, error) {
	stmt := spanner.Statement{SQL: `SELECT id, name, topic, createdAt, updatedAt FROM Subscriptions_beta`}
	iter := s.SpannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var subscriptions []*pb.Subscription
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to list subscriptions: %v", err)
		}

		var (
			id, name, topic      string
			createdAt, updatedAt spanner.NullTime
		)
		if err := row.Columns(&id, &name, &topic, &createdAt, &updatedAt); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to parse subscription data: %v", err)
		}

		subscriptions = append(subscriptions, &pb.Subscription{
			Id:      id,
			TopicId: topic,
		})
	}

	return &pb.ListSubscriptionsResponse{Subscriptions: subscriptions}, nil
}

// Helper functions
func (s *SubscriptionService) subscriptionExists(ctx context.Context, name string) (bool, error) {
	stmt := spanner.Statement{
		SQL:    "SELECT 1 FROM Subscriptions_beta WHERE name = @name LIMIT 1",
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

func (s *SubscriptionService) notifyLeader(ctx context.Context, flag int) error {
	data := map[string]interface{}{
		"operation": flag,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}
	if isLeader, _ := s.HedgeOp.HasLock(); !isLeader {
		resp := s.HedgeOp.Broadcast(ctx, jsonData)
		for _, r := range resp {
			if r.Error != nil {
				log.Printf("Failed to notify node %s: %v", r.Id, r.Error)
			}
		}
	}
	return nil
}

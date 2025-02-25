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

type SubscriptionService struct {
	pb.UnimplementedPubSubServiceServer
	SpannerClient *spanner.Client
	HedgeOp       *hedge.Op
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

	subscriptionID := uuid.New().String()
	m := spanner.Insert("Subscriptions",
		[]string{"subscriptionID", "TopicID", "createdAt", "updatedAt"},
		[]interface{}{subscriptionID, req.TopicId, spanner.CommitTimestamp, spanner.CommitTimestamp},
	)

	_, err := s.SpannerClient.Apply(ctx, []*spanner.Mutation{m})
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
		SQL:    `SELECT subscriptionID, TopicID, createdAt, updatedAt FROM Subscriptions WHERE subscriptionID = @id LIMIT 1`,
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
		id, topicID          string
		createdAt, updatedAt spanner.NullTime
	)
	if err := row.Columns(&id, &topicID, &createdAt, &updatedAt); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse subscription data: %v", err)
	}

	return &pb.Subscription{
		Id:        id,
		TopicId:   topicID,
		CreatedAt: convertTime(createdAt),
		UpdatedAt: convertTime(updatedAt),
	}, nil
}

func (s *SubscriptionService) UpdateSubscription(ctx context.Context, req *pb.UpdateSubscriptionRequest) (*pb.Subscription, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "subscription ID is required")
	}
	if req.TopicId == "" {
		return nil, status.Error(codes.InvalidArgument, "new topic ID is required")
	}

	// Get existing subscription to verify existence
	current, err := s.GetSubscription(ctx, &pb.GetSubscriptionRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}

	m := spanner.Update("Subscriptions",
		[]string{"subscriptionID", "TopicID", "updatedAt"},
		[]interface{}{current.Id, req.TopicId, spanner.CommitTimestamp},
	)

	_, err = s.SpannerClient.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update subscription: %v", err)
	}

	updatedSubscription := &pb.Subscription{
		Id:      current.Id,
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

	m := spanner.Delete("Subscriptions", spanner.Key{req.Id})
	_, err := s.SpannerClient.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to delete subscription: %v", err)
	}
	return &pb.DeleteSubscriptionResponse{Success: true}, nil
}

func (s *SubscriptionService) ListSubscriptions(ctx context.Context, _ *pb.Empty) (*pb.ListSubscriptionsResponse, error) {
	stmt := spanner.Statement{SQL: `SELECT subscriptionID, TopicID, createdAt, updatedAt FROM Subscriptions`}
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
			id, topicID          string
			createdAt, updatedAt spanner.NullTime
		)
		if err := row.Columns(&id, &topicID, &createdAt, &updatedAt); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to parse subscription data: %v", err)
		}

		subscriptions = append(subscriptions, &pb.Subscription{
			Id:        id,
			TopicId:   topicID,
			CreatedAt: convertTime(createdAt),
			UpdatedAt: convertTime(updatedAt),
		})
	}

	return &pb.ListSubscriptionsResponse{Subscriptions: subscriptions}, nil
}

// Helper functions
func convertTime(t spanner.NullTime) *timestamppb.Timestamp {
	if !t.Valid {
		return nil
	}
	return timestamppb.New(t.Time)
}

func (s *SubscriptionService) notifyLeader(ctx context.Context, flag int) error {
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

package topic

import (
	"context"
	"fmt"

	spanner "cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/flowerinthenight/hedge/v2"
	"github.com/google/uuid"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type SubscriptionService struct {
	client        pb.PubSubServiceClient
	SpannerClient *spanner.Client
}

func NewSubscriptionService(conn *grpc.ClientConn, db *spanner.Client) *SubscriptionService {
	client := pb.NewPubSubServiceClient(conn)
	return &SubscriptionService{
		client: 		client, 
		SpannerClient: 	db,
	}


}


//update the CreateSubscription function to include subscription id
func (s *SubscriptionService) CreateSubscription(ctx context.Context, req *pb.CreateSubscriptionRequest) (*pb.Subscription, error) {
	if req.id == "" || req.TopicId == "" {
		return nil, fmt.Errorf("invalid request: Topic ID and Subscription ID are required")
	}

	//check if subscription exists
	exist, err := s.SubscriptionExists(ctx, req.id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check subscription existence: %v", err)
	}
	if exist {
		return nil, status.Errorf(codes.AlreadyExists, "subscription name %q already exists", req.	id)
	}
	
	stmt := spanner.Statement{
		SQL:    "INSERT INTO Subscriptions (subscriptionID, TopciID) VALUES (@id, @topic_id)",
		Params: map[string]interface{}{"subscriptionID": req.id, "TopicID": *&req.TopicId},
	}

	_, err = s.SpannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err = txn.Update(ctx, stmt)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create subscription: %v", err)
	}

	return &pb.Subscription{Id: req.id, TopicId: *&req.TopicId}, nil
}

//modify the GetSubscription function to include subscription id and done
func (s *SubscriptionService) GetSubscription(ctx context.Context, req *pb.GetSubscriptionRequest) (*pb.Subscription, error) {
	if req.Id == "" {
		return nil, fmt.Errorf("invalid request: exisitng subscription required")
	}
	
	stmt := spanner.Statement{
		SQL:    "SELECT subscriptionID FROM Subscriptions WHERE subscriptionID = @id LIMIT 1",
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
		TopciID, SubscriptionID string
		createdAt, updatedAt 	spanner.NullTime
	)
	if err := row.Columns(&TopciID, &SubscriptionID, &createdAt, &updatedAt); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to parse topic data: %v", err)
	}

	return &pb.Subscription{
		Id:        	  SubscriptionID,
		TopicId:      TopciID,
	}, nil
}

//modify the GetSubscription function to include topic id and done
func (s *SubscriptionService) UpdateSubscription(ctx context.Context, req *pb.UpdateSubscriptionRequest) (*pb.Subscription, error) {
	if req.Id == "" {
		return nil, status.Error(codes.InvalidArgument, "subscription ID is required")
	}
	if req.TopicId == "" {
		return nil, status.Error(codes.InvalidArgument, "attached topicId is required")
	}

	// Check if new Subscription already exists
	exist, err := s.SubscriptionExists(ctx, req.Id)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check ID availability: %v", err)
	}
	if exist {
		return nil, status.Errorf(codes.AlreadyExists, "Subscription ID %q already exists", req.Id)
	}

	// Get existing subscription to verify existence
	current, err := s.GetSubscription(ctx, &pb.GetSubscriptionRequest{Id: req.Id})
	if err != nil {
		return nil, err
	}

	m := spanner.Update("Subsscriptions",
		[]string{"subscriptionID", "TopicID", "updatedAt"},
		[]interface{}{current.Id, req.TopicID, spanner.CommitTimestamp},
	)

	_, err = s.SpannerClient.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update topic: %v", err)
	}

	updatedTopic := &pb.Topic{
		Id:   current.Id,
		SubscriptionID: req.Id,
	}

	if err := s.notifyLeader(ctx, 1); err != nil {
		log.Printf("Failed to notify leader: %v", err)
	}

	return updatedTopic, nil
}

func (s *SubscriptionService) DeleteSubscription(ctx context.Context, req *pb.DeleteSubscriptionRequest) (*pb.DeleteSubscriptionResponse, error) {
	response, err := s.client.DeleteSubscription(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to delete subscription: %v", err)
	}
	// to be configured x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0xx0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x
	return response, nil
}

func (s *SubscriptionService) ListSubscriptions(ctx context.Context, req *pb.Empty) (*pb.ListSubscriptionsResponse, error) {
	subscriptions, err := s.client.ListSubscriptions(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to list subscriptions: %v", err)
	}
	// to be configured x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0xx0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x0x
	return subscriptions, nil
}

//Additional functions
func (s *TopicService) SubscriptionExists(ctx context.Context, name string) (bool, error) {
	stmt := spanner.Statement{
		SQL:    "SELECT 1 FROM Subscription WHERE subscriptionID = @subscriptionID LIMIT 1",
		Params: map[string]interface{}{"subscriptionID": subscriptionID},
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
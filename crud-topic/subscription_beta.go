package topic

import (
	"context"
	"fmt"

	spanner "cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/api/iterator"
)

type SubscriptionService struct {
	client        pb.PubSubServiceClient
	SpannerClient *spanner.Client
}

func NewSubscriptionService(conn *grpc.ClientConn, db *spanner.Client) *SubscriptionService {
	client := pb.NewPubSubServiceClient(conn)
	return &SubscriptionService{client: client, SpannerClient: db}


}


//update the CreateSubscription function to include subscription id
func (s *SubscriptionService) CreateSubscription(ctx context.Context, req *pb.CreateSubscriptionRequest) (*pb.Subscription, error) {
	_, err := s.client.CreateSubscription(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to create subscription: %v", err)
	}

	if req.id == "" || req.TopicId == "" {
		return nil, fmt.Errorf("invalid request: Topic ID and Subscription ID are required")
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

//somewhat done
func (s *SubscriptionService) GetSubscription(ctx context.Context, req *pb.GetSubscriptionRequest) (*pb.Subscription, error) {
	_, err := s.client.GetSubscription(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to get subscription: %v", err)
	}
	
	if req.Id == "" {
		return nil, fmt.Errorf("invalid request: exisitng subscription required")
	}
	
	stmt := spanner.Statement{
		SQL:    "SELECT subscriptionID FROM Subscriptions WHERE subscriptionID = @id",
		Params: map[string]interface{}{"id": req.Id},
	}

	_, err = s.SpannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err = txn.Update(ctx, stmt)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to update topic: %v", err)
	}

	return &pb.Subscription{TopicId: req.Id}, nil
}

//somewhat done
func (s *SubscriptionService) UpdateSubscription(ctx context.Context, req *pb.UpdateSubscriptionRequest) (*pb.Subscription, error) {
	subscription, err := s.client.UpdateSubscription(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to update subscription: %v", err)
	}
	








	
	return subscription, nil
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

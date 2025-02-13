package server

import (
	"cloud.google.com/go/spanner"
	"context"
	"fmt"
	pb "github.com/alphauslabs/pubsub/protos/pubsub"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

type TopicServer struct {
	pb.UnimplementedTopicServiceServer
	client *spanner.Client
}

func NewTopicServer(client *spanner.Client) *TopicServer {
	return &TopicServer{client: client}
}

func (s *TopicServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.Topic, error) {
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "topic name is required")
	}

	// Check if topic with same name exists
	stmt := spanner.NewStatement(`
        SELECT COUNT(*) FROM Topics 
        WHERE Name = @name
    `)
	stmt.Params["name"] = req.Name

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to check topic existence: %v", err)
	}

	var count int64
	if err := row.Columns(&count); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to read count: %v", err)
	}

	if count > 0 {
		return nil, status.Error(codes.AlreadyExists, "topic with this name already exists")
	}

	topicID := uuid.New().String()
	now := time.Now()

	mutation := []*spanner.Mutation{
		spanner.Insert("Topics",
			[]string{"TopicID", "Name", "CreatedAt", "UpdatedAt"},
			[]interface{}{topicID, req.Name, now, now},
		),
	}

	err = s.client.Apply(ctx, mutation)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create topic: %v", err)
	}

	return &pb.Topic{
		TopicId:   topicID,
		Name:      req.Name,
		CreatedAt: timestamppb.New(now),
		UpdatedAt: timestamppb.New(now),
	}, nil
}

func (s *TopicServer) GetTopic(ctx context.Context, req *pb.GetTopicRequest) (*pb.Topic, error) {
	if req.TopicId == "" {
		return nil, status.Error(codes.InvalidArgument, "topic_id is required")
	}

	// Query both topic details and subscription count
	stmt := spanner.NewStatement(`
        SELECT t.TopicID, t.Name, t.CreatedAt, t.UpdatedAt,
               (SELECT COUNT(*) FROM Subscriptions s WHERE s.TopicID = t.TopicID) as SubCount
        FROM Topics t
        WHERE t.TopicID = @topic_id
    `)
	stmt.Params["topic_id"] = req.TopicId

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return nil, status.Error(codes.NotFound, "topic not found")
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to get topic: %v", err)
	}

	var topic pb.Topic
	var createdAt, updatedAt time.Time
	var subCount int32
	err = row.Columns(&topic.TopicId, &topic.Name, &createdAt, &updatedAt, &subCount)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to scan topic data: %v", err)
	}

	topic.CreatedAt = timestamppb.New(createdAt)
	topic.UpdatedAt = timestamppb.New(updatedAt)
	topic.SubscriptionCount = subCount

	return &topic, nil
}

func (s *TopicServer) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = 50 // default page size
	}

	stmt := spanner.NewStatement(`
        SELECT t.TopicID, t.Name, t.CreatedAt, t.UpdatedAt,
               (SELECT COUNT(*) FROM Subscriptions s WHERE s.TopicID = t.TopicID) as SubCount
        FROM Topics t
        ORDER BY t.CreatedAt DESC
        LIMIT @limit
        OFFSET @offset
    `)

	offset := 0
	if req.PageToken != "" {
		if _, err := fmt.Sscanf(req.PageToken, "%d", &offset); err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid page token")
		}
	}

	stmt.Params = map[string]interface{}{
		"limit":  pageSize,
		"offset": offset,
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var topics []*pb.Topic
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to iterate topics: %v", err)
		}

		var topic pb.Topic
		var createdAt, updatedAt time.Time
		var subCount int32
		err = row.Columns(&topic.TopicId, &topic.Name, &createdAt, &updatedAt, &subCount)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "failed to scan topic data: %v", err)
		}

		topic.CreatedAt = timestamppb.New(createdAt)
		topic.UpdatedAt = timestamppb.New(updatedAt)
		topic.SubscriptionCount = subCount
		topics = append(topics, &topic)
	}

	var nextPageToken string
	if len(topics) == int(pageSize) {
		nextPageToken = fmt.Sprintf("%d", offset+len(topics))
	}

	return &pb.ListTopicsResponse{
		Topics:        topics,
		NextPageToken: nextPageToken,
	}, nil
}

func (s *TopicServer) UpdateTopic(ctx context.Context, req *pb.UpdateTopicRequest) (*pb.Topic, error) {
	if req.TopicId == "" || req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "topic_id and name are required")
	}

	// Check if topic exists
	exists, err := s.topicExists(ctx, req.TopicId)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, status.Error(codes.NotFound, "topic not found")
	}

	now := time.Now()
	mutation := []*spanner.Mutation{
		spanner.Update("Topics",
			[]string{"TopicID", "Name", "UpdatedAt"},
			[]interface{}{req.TopicId, req.Name, now},
		),
	}

	err = s.client.Apply(ctx, mutation)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to update topic: %v", err)
	}

	return s.GetTopic(ctx, &pb.GetTopicRequest{TopicId: req.TopicId})
}

func (s *TopicServer) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*emptypb.Empty, error) {
	if req.TopicId == "" {
		return nil, status.Error(codes.InvalidArgument, "topic_id is required")
	}

	// Start a ReadWrite transaction
	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Check if topic exists
		exists, err := s.topicExistsInTxn(ctx, txn, req.TopicId)
		if err != nil {
			return err
		}
		if !exists {
			return status.Error(codes.NotFound, "topic not found")
		}

		mutations := []*spanner.Mutation{
			spanner.Delete("Topics", spanner.Key{req.TopicId}),
		}

		// If deleteSubscriptions is true, delete all associated subscriptions
		if req.DeleteSubscriptions {
			subsMutation := spanner.Delete("Subscriptions", spanner.KeyRange{
				Start: spanner.Key{req.TopicId},
				End:   spanner.Key{req.TopicId}.AsPrefix(),
			})
			mutations = append(mutations, subsMutation)
		}

		return txn.BufferWrite(mutations)
	})

	if err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// Helper functions

func (s *TopicServer) topicExists(ctx context.Context, topicID string) (bool, error) {
	row, err := s.client.Single().ReadRow(ctx, "Topics", spanner.Key{topicID}, []string{"TopicID"})
	if err == spanner.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, status.Errorf(codes.Internal, "failed to check topic existence: %v", err)
	}
	return true, nil
}

func (s *TopicServer) topicExistsInTxn(ctx context.Context, txn *spanner.ReadWriteTransaction, topicID string) (bool, error) {
	row, err := txn.ReadRow(ctx, "Topics", spanner.Key{topicID}, []string{"TopicID"})
	if err == spanner.ErrNoRows {
		return false, nil
	}
	if err != nil {
		return false, status.Errorf(codes.Internal, "failed to check topic existence: %v", err)
	}
	return true, nil
}

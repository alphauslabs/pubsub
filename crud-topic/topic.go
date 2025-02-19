package topic

import (
	"context"
	"fmt"

	spanner "cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/api/iterator"
)

type TopicService struct {
	pb.UnimplementedPubSubServiceServer
	SpannerClient *spanner.Client
}

// UpdateTopic updates a topic’s name by its ID.
func (s *TopicService) UpdateTopic(ctx context.Context, req *pb.UpdateTopicRequest) (*pb.Topic, error) {
	if req.Id == "" || req.NewName == nil {
		return nil, fmt.Errorf("invalid request: ID and new name are required")
	}

	stmt := spanner.Statement{
		SQL:    "UPDATE topics SET name = @new_name WHERE id = @id",
		Params: map[string]interface{}{"id": req.Id, "new_name": *req.NewName},
	}

	_, err := s.SpannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, stmt)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to update topic: %v", err)
	}

	return &pb.Topic{TopicId: req.Id, Name: *req.NewName}, nil
}

// DeleteTopic deletes a topic by its ID.
func (s *TopicService) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	if req.Id == "" {
		return nil, fmt.Errorf("invalid request: ID is required")
	}

	stmt := spanner.Statement{
		SQL:    "DELETE FROM topics WHERE id = @id",
		Params: map[string]interface{}{"id": req.Id},
	}

	_, err := s.SpannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, stmt)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("failed to delete topic: %v", err)
	}

	return &pb.DeleteTopicResponse{Success: true}, nil
}

// ListTopics fetches all topics.
func (s *TopicService) ListTopics(ctx context.Context, req *pb.Empty) (*pb.ListTopicsResponse, error) {
	stmt := spanner.Statement{SQL: "SELECT id, name FROM Messages"}
	iter := s.SpannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var topics []*pb.Topic
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var id, name string
		if err := row.Columns(&id, &name); err != nil {
			return nil, err
		}
		topics = append(topics, &pb.Topic{TopicId: id, Name: name})
	}

	return &pb.ListTopicsResponse{Topics: topics}, nil
}

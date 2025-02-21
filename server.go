package main

import (
	"context"
	"encoding/json"
	"log"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/flowerinthenight/hedge/v2"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	client *spanner.Client
	op     *hedge.Op
	pb.UnimplementedPubSubServiceServer
}

const (
	MessagesTable = "Messages"
)

func (s *server) Publish(ctx context.Context, in *pb.PublishRequest) (*pb.PublishResponse, error) {
	if in.TopicId == "" {
		return nil, status.Error(codes.InvalidArgument, "topic must not be empty")
	}

	l, _ := s.op.HasLock()
	if l {
		log.Println("[Publish] I'm the leader")
	} else {
		log.Println("[Publish] I'm not the leader")
	}

	b, _ := json.Marshal(in)
	log.Printf("[Publish] Received message:\n%v", string(b))

	messageID := uuid.New().String()
	mutation := spanner.InsertOrUpdate(
		MessagesTable,
		[]string{"id", "topic", "payload", "createdAt", "updatedAt"},
		[]interface{}{
			messageID,
			in.TopicId,
			in.Payload,
			spanner.CommitTimestamp,
			spanner.CommitTimestamp,
		},
	)

	_, err := s.client.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		log.Printf("Error writing to Spanner: %v", err)
		return nil, err
	}

	log.Printf("[Publish] Message successfully wrote to spanner with ID: %s", messageID)
	return &pb.PublishResponse{MessageId: messageID}, nil
}

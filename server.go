package main

import (
	"context"
	"log"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/google/uuid"
)

type server struct {
	client *spanner.Client
	pb.UnimplementedPubSubServiceServer
}

const (
	MessagesTable = "Messages"
)

func (s *server) Publish(ctx context.Context, in *pb.PublishRequest) (*pb.PublishResponse, error) {
	log.Println("Received message: ", in)
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

	log.Printf("Message successfully wrote message to Spanner with ID: %s", messageID)

	return &pb.PublishResponse{MessageId: messageID}, nil
}

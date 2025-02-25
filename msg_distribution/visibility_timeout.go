package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

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
	messageLocks sync.Map // Tracks message locks with expiration times
}

const (
	MessagesTable = "Messages"
	VisibilityTimeout = time.Minute // 1-minute lock
)

func (s *server) Publish(ctx context.Context, in *pb.PublishRequest) (*pb.PublishResponse, error) {
	if in.TopicId == "" {
		return nil, status.Error(codes.InvalidArgument, "topic must not be empty")
	}

	b, _ := json.Marshal(in)
	l, _ := s.op.HasLock()
	if l {
		log.Println("[Publish-leader] Received message:\n", string(b))
	} else {
		log.Printf("[Publish] Received message:\n%v", string(b))
	}

	messageID := uuid.New().String()
	mutation := spanner.InsertOrUpdate(
		MessagesTable,
		[]string{"id", "topic", "payload", "createdAt", "updatedAt", "visibilityTimeout", "processed"},
		[]interface{}{
			messageID,
			in.TopicId,
			in.Payload,
			spanner.CommitTimestamp,
			spanner.CommitTimestamp,
			time.Now().Add(VisibilityTimeout),
			false,
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

func (s *server) Subscribe(req *pb.SubscribeRequest, stream pb.PubSubService_SubscribeServer) error {
	ctx := stream.Context()
	subscriberID := req.SubscriptionId

	for {
		stmt := spanner.Statement{
			SQL: `SELECT id, payload, topic FROM Messages WHERE processed = FALSE AND visibilityTimeout <= CURRENT_TIMESTAMP()`,
		}
		iter := s.client.Single().Query(ctx, stmt)

		for {
			row, err := iter.Next()
			if err != nil {
				return err
			}

			var msg pb.Message
			if err := row.Columns(&msg.Id, &msg.Payload, &msg.Topic); err != nil {
				return err
			}

			if _, exists := s.messageLocks.Load(msg.Id); exists {
				continue // Skip locked messages
			}

			s.messageLocks.Store(msg.Id, time.Now().Add(VisibilityTimeout))

			if err := stream.Send(&msg); err != nil {
				s.messageLocks.Delete(msg.Id)
				return err
			}
		}
	}
}

func (s *server) Acknowledge(ctx context.Context, req *pb.AcknowledgeRequest) (*pb.AcknowledgeResponse, error) {
	_, err := s.client.Apply(ctx, []*spanner.Mutation{
		spanner.Update(MessagesTable, []string{"id", "processed"}, []interface{}{req.Id, true}),
	})
	if err != nil {
		return nil, err
	}

	s.messageLocks.Delete(req.Id)
	return &pb.AcknowledgeResponse{Success: true}, nil
}


//update code

package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

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
	messageLocks sync.Map // Tracks message locks with expiration times
}

const (
	MessagesTable     = "Messages"
	VisibilityTimeout = time.Minute // 1-minute lock
)

// Publish a message to Spanner
func (s *server) Publish(ctx context.Context, in *pb.PublishRequest) (*pb.PublishResponse, error) {
	if in.TopicId == "" {
		return nil, status.Error(codes.InvalidArgument, "topic must not be empty")
	}

	b, _ := json.Marshal(in)
	l, _ := s.op.HasLock()
	if l {
		log.Println("[Publish-leader] Received message:\n", string(b))
	} else {
		log.Printf("[Publish] Received message:\n%v", string(b))
	}

	messageID := uuid.New().String()
	mutation := spanner.InsertOrUpdate(
		MessagesTable,
		[]string{"id", "topic", "payload", "createdAt", "updatedAt", "visibilityTimeout", "processed"},
		[]interface{}{
			messageID,
			in.TopicId,
			in.Payload,
			spanner.CommitTimestamp,
			spanner.CommitTimestamp,
			time.Now(), // Initially visible
			false,
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

// Assigns a message to a subscriber by updating its visibility timeout
func (s *server) AssignMessage(ctx context.Context) (*pb.Message, error) {
	stmt := spanner.Statement{
		SQL: `SELECT id, payload, topic FROM Messages 
			  WHERE processed = FALSE AND visibilityTimeout <= CURRENT_TIMESTAMP() 
			  ORDER BY createdAt ASC 
			  LIMIT 1`,
	}

	row, err := s.client.Single().Query(ctx, stmt).Next()
	if err != nil {
		return nil, err
	}

	var msg pb.Message
	if err := row.Columns(&msg.Id, &msg.Payload, &msg.Topic); err != nil {
		return nil, err
	}

	// Lock message in Spanner by updating visibility timeout
	updateMutation := spanner.Update(
		MessagesTable,
		[]string{"id", "visibilityTimeout"},
		[]interface{}{msg.Id, time.Now().Add(VisibilityTimeout)},
	)
	_, err = s.client.Apply(ctx, []*spanner.Mutation{updateMutation})
	if err != nil {
		return nil, err
	}

	s.messageLocks.Store(msg.Id, time.Now().Add(VisibilityTimeout))
	return &msg, nil
}

// Subscribe to messages
func (s *server) Subscribe(req *pb.SubscribeRequest, stream pb.PubSubService_SubscribeServer) error {
	ctx := stream.Context()
	subscriberID := req.SubscriptionId

	for {
		msg, err := s.AssignMessage(ctx)
		if err != nil {
			time.Sleep(time.Second) // Prevent excessive Spanner queries
			continue
		}

		if err := stream.Send(msg); err != nil {
			s.messageLocks.Delete(msg.Id)
			return err
		}
	}
}

// Acknowledge message processing completion
func (s *server) Acknowledge(ctx context.Context, req *pb.AcknowledgeRequest) (*pb.AcknowledgeResponse, error) {
	_, err := s.client.Apply(ctx, []*spanner.Mutation{
		spanner.Update(MessagesTable, []string{"id", "processed"}, []interface{}{req.Id, true}),
	})
	if err != nil {
		return nil, err
	}

	s.messageLocks.Delete(req.Id)
	return &pb.AcknowledgeResponse{Success: true}, nil
}

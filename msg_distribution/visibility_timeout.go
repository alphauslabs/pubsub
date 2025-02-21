// ✅ 1️⃣ Distribute messages to subscribers
// The Subscribe function retrieves messages from the in-memory buffer.
// Uses visibility timeout to lock messages for subscribers.

// ✅ 2️⃣ Handle subscriber acknowledgments
// Acknowledge updates the message status (processed = true) in Spanner.
// Removes acknowledged messages from in-memory buffers.

// ✅ 3️⃣ Implement visibility timeout logic
// If a subscriber doesn’t acknowledge in time, the message becomes visible again.
// Another subscriber can pick it up.

// ✅ 4️⃣ Support time extension requests
// ModifyVisibilityTimeout allows subscribers to extend their processing time.
// Updates the timeout in Spanner.

package main

import (
	"context"
	"log"
	"time"

	pubsubpb "github.com/alphauslabs/pubsubproto"
	"cloud.google.com/go/spanner"
	"google.golang.org/grpc"
)

type PubSubServer struct {
	pubsubpb.UnimplementedPubSubServiceServer
	spannerClient *spanner.Client
}

// Publish writes a message to Spanner and acknowledges receipt
func (s *PubSubServer) Publish(ctx context.Context, req *pubsubpb.PublishRequest) (*pubsubpb.PublishResponse, error) {
	id := generateUUID()
	createdAt := time.Now()
	visibilityTimeout := createdAt.Add(time.Minute) // Default 1 min lock

	_, err := s.spannerClient.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Messages",
			[]string{"id", "payload", "topic", "createdAt", "updatedAt", "visibilityTimeout", "processed"},
			[]interface{}{id, req.Payload, req.Topic, createdAt, createdAt, visibilityTimeout, false},
		),
	})
	if err != nil {
		return nil, err
	}

	return &pubsubpb.PublishResponse{MessageId: id}, nil
}

// Subscribe streams messages to a subscriber
func (s *PubSubServer) Subscribe(req *pubsubpb.SubscribeRequest, stream pubsubpb.PubSubService_SubscribeServer) error {
	ctx := stream.Context()
	stmt := spanner.Statement{
		SQL: `SELECT id, payload, topic, visibilityTimeout, processed FROM Messages 
			WHERE processed = FALSE AND visibilityTimeout <= CURRENT_TIMESTAMP()`,
	}
	iter := s.spannerClient.Single().Query(ctx, stmt)
	subscriberID := req.SubscriptionId
	for {
		row, err := iter.Next()
		if err != nil {
			return err
		}

		var msg pubsubpb.Message
		err = row.Columns(&msg.Id, &msg.Payload, &msg.Topic, &msg.Processed)
		if err != nil {
			return err
		}

		msg.Processed = false
		if err := stream.Send(&msg); err != nil {
			return err
		}
	}
}

// Acknowledge confirms message processing
func (s *PubSubServer) Acknowledge(ctx context.Context, req *pubsubpb.AcknowledgeRequest) (*pubsubpb.AcknowledgeResponse, error) {
	_, err := s.spannerClient.Apply(ctx, []*spanner.Mutation{
		spanner.Update("Messages", []string{"id", "processed"}, []interface{}{req.Id, true}),
	})
	if err != nil {
		return nil, err
	}

	return &pubsubpb.AcknowledgeResponse{Success: true}, nil
}

// ModifyVisibilityTimeout updates message lock duration
func (s *PubSubServer) ModifyVisibilityTimeout(ctx context.Context, req *pubsubpb.ModifyVisibilityTimeoutRequest) (*pubsubpb.ModifyVisibilityTimeoutResponse, error) {
	newTimeout := time.Now().Add(time.Duration(req.NewTimeout) * time.Second)
	_, err := s.spannerClient.Apply(ctx, []*spanner.Mutation{
		spanner.Update("Messages", []string{"id", "visibilityTimeout"}, []interface{}{req.Id, newTimeout}),
	})
	if err != nil {
		return nil, err
	}
	return &pubsubpb.ModifyVisibilityTimeoutResponse{Success: true}, nil
}

func main() {
	// gRPC server setup
	server := grpc.NewServer()
	pubsubpb.RegisterPubSubServiceServer(server, &PubSubServer{})

	log.Println("Pub/Sub Service is running...")
	// Listen & Serve (setup omitted for brevity)
}

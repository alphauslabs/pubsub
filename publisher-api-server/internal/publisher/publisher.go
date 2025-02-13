//ALL PLACEHOLDERS ATM - NON FUNCTIONAL
package publisher

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	"publisher-api-server/pkg/protos" // Adjust to your actual module path

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// PublisherServer implements the PubSubService gRPC server.
type PublisherServer struct {
	protos.UnimplementedPubSubServiceServer
	// You can include fields such as a Spanner client or configuration if needed.
}

// NewPublisherServer creates a new instance of PublisherServer.
func NewPublisherServer() *PublisherServer {
	return &PublisherServer{}
}

// Publish implements the Publish RPC.
func (ps *PublisherServer) Publish(ctx context.Context, req *protos.PublishRequest) (*protos.PublishResponse, error) {
	// Basic validation.
	if req.Topic == "" {
		return nil, status.Errorf(codes.InvalidArgument, "topic is required")
	}
	if req.Payload == "" {
		return nil, status.Errorf(codes.InvalidArgument, "payload cannot be empty")
	}

	// Generate a unique message ID.
	messageID := uuid.New().String()

	// Prepare the message to store in Spanner.
	message := Message{
		ID:      messageID,
		Topic:   req.Topic,
		Payload: req.Payload,
		Created: time.Now(),
	}

	// Persist the message to Spanner.
	if err := storeMessage(ctx, message); err != nil {
		log.Printf("Failed to store message: %v", err)
		return nil, status.Errorf(codes.Internal, "failed to store message")
	}

	// Return a response containing the message ID.
	return &protos.PublishResponse{
		MessageId: messageID,
	}, nil
}

// Message represents a message to be stored in Spanner.
type Message struct {
	ID      string
	Topic   string
	Payload string
	Created time.Time
}

// storeMessage simulates writing the message to Spanner.
// In your actual implementation, you would use the Spanner client library to perform the write.
func storeMessage(ctx context.Context, msg Message) error {
	// Simulate a database write delay.
	time.Sleep(10 * time.Millisecond)
	// In a real implementation, write to Cloud Spanner here.
	log.Printf("Message stored: %+v", msg)
	return nil
}

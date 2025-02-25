package main

import (
	"context"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	pb "github.com/alphauslabs/pubsub-proto/v1"
)

// SubscriberHandler manages subscriber connections and keep-alive functionality.
type SubscriberHandler struct {
	pb.UnimplementedPubSubServiceServer // Embed the unimplemented server for forward compatibility

	mu          sync.Mutex
	subscribers map[string]chan *pb.Message // Map of subscription IDs to message channels
}

// NewSubscriberHandler initializes a new SubscriberHandler.
func NewSubscriberHandler() *SubscriberHandler {
	return &SubscriberHandler{
		subscribers: make(map[string]chan *pb.Message),
	}
}

// Subscribe implements the gRPC streaming API for subscriptions. (server)
func (s *SubscriberHandler) Subscribe(req *pb.SubscribeRequest, stream pb.PubSubService_SubscribeServer) error {
	subscriptionID := req.SubscriptionId
	log.Printf("New subscriber connected for subscription: %s", subscriptionID)

	// Create a message channel for this subscription
	msgChan := make(chan *pb.Message, 100)
	s.mu.Lock()
	s.subscribers[subscriptionID] = msgChan
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.subscribers, subscriptionID)
		close(msgChan)
		s.mu.Unlock()
		log.Printf("Subscriber disconnected for subscription: %s", subscriptionID)
	}()

	// Keep the connection alive and stream messages
	for {
		select {
		case msg := <-msgChan:
			// Deliver message to subscriber
			if err := stream.Send(msg); err != nil {
				log.Printf("Failed to send message to subscriber %s: %v", subscriptionID, err)
				return err
			}

		case <-stream.Context().Done():
			// Handle disconnection
			log.Printf("Subscriber for subscription %s disconnected", subscriptionID)
			return nil
		}
	}
}

// Acknowledge handles message acknowledgments from subscribers.
// This is a no-op in this implementation - no visibility timeout handler over here for now
func (s *SubscriberHandler) Acknowledge(ctx context.Context, req *pb.AcknowledgeRequest) (*pb.AcknowledgeResponse, error) {
	log.Printf("Received acknowledgment for message: %s from subscription: %s", req.Id, req.SubscriptionId)
	return &pb.AcknowledgeResponse{Success: true}, nil
}

func main() {
	// Initialize the subscriber handler
	handler := NewSubscriberHandler()

	// Start the gRPC server with verbose keep-alive logging
	grpcServer := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    10 * time.Second, // Send keepalive pings every 10 seconds
			Timeout: 20 * time.Second, // Wait 20 seconds for a ping ack before closing the connection
		}),
		grpc.ChainStreamInterceptor(func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			// Log keep-alive pings
			md, _ := metadata.FromIncomingContext(ss.Context())
			log.Printf("Keep-alive ping received: %v", md)
			return handler(srv, ss)
		}),
	)
	pb.RegisterPubSubServiceServer(grpcServer, handler)

	listener, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	log.Println("Server is running on port 50051")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

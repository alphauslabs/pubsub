package main

import (
	"context"
	"log"
	"net"

	pb "github.com/alphauslabs/pubsub-proto" // Import directly
	"google.golang.org/grpc"
)

// server is used to implement pb.PubSubServiceServer.
type server struct {
	pb.UnimplementedPubSubServiceServer
}

// Publish implements the gRPC Publish method
func (s *server) Publish(ctx context.Context, msg *pb.Message) (*pb.PublishResponse, error) {
	log.Printf("[Mock Server 8080] Received message: ID=%s, Subsription=%s, Payload=%s",
		msg.Id, msg.Subsription, string(msg.Payload))
	return &pb.PublishResponse{MessageId: msg.Id}, nil
}

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("[FATAL] Failed to listen on port 8080: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPubSubServiceServer(grpcServer, &server{})

	log.Println("[INFO] Mock gRPC Server is running on port 8080")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("[FATAL] Failed to serve gRPC server: %v", err)
	}
}

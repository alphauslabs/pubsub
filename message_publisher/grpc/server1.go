package main

import (
	"context"
	"log"
	"net"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedPubSubServiceServer
}

func (s *server) Publish(ctx context.Context, msg *pb.Message) (*pb.PublishResponse, error) {
	log.Printf("[Mock Server 8081] Received message:\n   ID: %s\n   Payload: %s\n   Topic: %s",
		msg.Id, string(msg.Payload), msg.TopicId)
	return &pb.PublishResponse{MessageId: msg.Id}, nil
}

func main() {
	listener, err := net.Listen("tcp", ":8081")
	if err != nil {
		log.Fatalf("[FATAL] Failed to listen on port 8081: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterPubSubServiceServer(grpcServer, &server{})

	log.Println("Mock gRPC Server is running on port 8081")
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("[FATAL] Failed to serve gRPC server: %v", err)
	}
}

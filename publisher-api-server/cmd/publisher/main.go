//ALL PLACEHOLDERS ATM - NON FUNCTIONAL

package main

import (
	"log"
	"net"

	"google.golang.org/grpc"
	"project-root/internal/publisher"
	pb "project-root/pkg/protos"
)

func main() {
	//50051 RECONFIGURABLE AS PER REQUEST
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()

	// Register the publisher service.
	pb.RegisterPubSubServiceServer(grpcServer, publisher.NewPublisherServer())

	log.Println("Publish API server is running on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

package main

import (
	"context"
	"flag"
	"log"
	"net"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var port = flag.String("port", ":50051", "Main gRPC server port")

func main() {
	lis, err := net.Listen("tcp", *port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
		return
	}

	spannerClient, err := spanner.NewClient(context.Background(), "projects/labs-169405/instances/alphaus-dev/databases/main")
	if err != nil {
		log.Fatalf("failed to create Spanner client: %v", err)
		return
	}

	defer spannerClient.Close()

	s := grpc.NewServer()
	reflection.Register(s)
	pb.RegisterPubSubServiceServer(s, &server{client: spannerClient})
	log.Printf("Server listening on :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

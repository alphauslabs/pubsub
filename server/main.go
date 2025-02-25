package main

import (
	"log"
	"net"

	"google.golang.org/grpc"
	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	subscription "path/to/subscription-gamma"
	"github.com/flowerinthenight/hedge/v2"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}

	s := grpc.NewServer()
	spannerClient, err := spanner.NewClient(context.Background(), "your-spanner-database")
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	hedgeOp := hedge.NewOp()

	subscriptionService := subscription.NewSubscriptionService(spannerClient, hedgeOp)
	pb.RegisterPubSubServiceServer(s, subscriptionService)

	log.Println("Server is running on port :50051")
	if err := s.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

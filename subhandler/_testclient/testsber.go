package main

import (
	"context"
	"log"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
)

func main() {
	// Connect to the gRPC server
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := pb.NewPubSubServiceClient(conn)

	// Subscribe to a specific subscription ID
	subscriptionID := "test-subscription"
	stream, err := client.Subscribe(context.Background(), &pb.SubscribeRequest{SubscriptionId: subscriptionID})
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	log.Printf("Subscribed to subscription ID: %s", subscriptionID)

	// Listen for messages
	for {
		msg, err := stream.Recv()
		if err != nil {
			log.Fatalf("Failed to receive message: %v", err)
		}

		log.Printf("Received message: %s", msg.Payload)
	}
}

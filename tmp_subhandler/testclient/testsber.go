package main

// import (
// 	"context"
// 	"log"
// 	"time"

// 	pb "github.com/alphauslabs/pubsub-proto/v1"
// 	"google.golang.org/grpc"
// )

// func main() {
// 	// Connect to the gRPC serve
// 	conn, err := grpc.Dial("10.146.0.43:50051", grpc.WithInsecure(), grpc.WithBlock())
// 	if err != nil {
// 		log.Fatalf("Failed to connect to server: %v", err)
// 	}
// 	defer conn.Close()

// 	client := pb.NewPubSubServiceClient(conn)

// 	// Subscribe to a specific subscription ID
// 	subscriptionID := "test-subscription"
// 	stream, err := client.Subscribe(context.Background(), &pb.SubscribeRequest{SubscriptionId: subscriptionID})
// 	if err != nil {
// 		log.Fatalf("Failed to subscribe: %v", err)
// 	}

// 	glog.Infof("Subscribed to subscription ID: %s", subscriptionID)

// 	// Start the timer
// 	startTime := time.Now()

// 	// Listen for messages
// 	for {
// 		msg, err := stream.Recv()
// 		if err != nil {
// 			log.Fatalf("Failed to receive message: %v", err)
// 		}

// 		// Calculate the elapsed time
// 		elapsedTime := time.Since(startTime)

// 		// Print the received message along with the elapsed time on the same line
// 		glog.Infof("Received message: %s [Connection Time: %v]", msg.Payload, elapsedTime)
// 	}
// }

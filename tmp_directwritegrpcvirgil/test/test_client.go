package main

// import (
// 	"context"
// 	"log"
// 	"time"

// 	pb "github.com/alphauslabs/pubsub-proto/v1"
// 	"github.com/golang/glog"
// 	"google.golang.org/grpc"
// 	"google.golang.org/grpc/credentials/insecure"
// )

// func main() {
// 	// Connect to the server
// 	conn, err := grpc.Dial("localhost:8085", grpc.WithTransportCredentials(insecure.NewCredentials()))
// 	if err != nil {
// 		log.Fatalf("Failed to connect: %v", err)
// 	}
// 	defer conn.Close()

// 	// Create a client
// 	client := pb.NewPubSubServiceClient(conn)

// 	// Create a test message
// 	message := &pb.Message{
// 		Topic:   "test-topic-virgil-new",
// 		Payload: []byte("Hello, this is a test message!"),
// 	}

// 	// Send the message
// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	defer cancel()

// 	response, err := client.Publish(ctx, message)
// 	if err != nil {
// 		log.Fatalf("Failed to publish message: %v", err)
// 	}

// 	glog.Infof("Successfully published message. Message ID: %s", response.MessageId)
// }

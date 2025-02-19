package main

import (
	"context"
	"log"
	"time"

	pb "bulkwrite/bulkwrite_proto" // Replace with the correct import path

	"google.golang.org/grpc"
)

func main() {
	// Set up a connection to the server.
	conn, err := grpc.Dial("http://35.243.83.115:50050", grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewPubSubServiceClient(conn)

	// Create a context with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Create a Message to publish
	message := &pb.Message{
		Id:        "msg-123",
		TopicId:   "topic-456",
		Payload:   []byte("Hello, World!"),
		CreatedAt: time.Now().Unix(),
		ExpiresAt: time.Now().Add(time.Hour * 1).Unix(),
	}

	// Send the Publish request
	response, err := client.Publish(ctx, message)
	if err != nil {
		log.Fatalf("could not publish: %v", err)
	}

	// Print the response
	log.Printf("Message published with ID: %s", response.MessageId)
}

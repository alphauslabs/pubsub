package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	pb "github.com/alphauslabs/pubsub/proto"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewTopicServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	// Create a topic.
	topic, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{
		Name: "test-topic",
	})
	if err != nil {
		log.Fatalf("Failed to create topic: %v", err)
	}
	log.Printf("Created topic: %+v", topic)

	// Get the created topic.
	fetched, err := client.GetTopic(ctx, &pb.GetTopicRequest{
		TopicId: topic.TopicId,
	})
	if err != nil {
		log.Fatalf("Failed to get topic: %v", err)
	}
	log.Printf("Fetched topic: %+v", fetched)

	// Update the topic.
	updated, err := client.UpdateTopic(ctx, &pb.UpdateTopicRequest{
		TopicId: topic.TopicId,
		Name:    "updated-topic-name",
	})
	if err != nil {
		log.Fatalf("Failed to update topic: %v", err)
	}
	log.Printf("Updated topic: %+v", updated)

	// List topics.
	listResp, err := client.ListTopics(ctx, &pb.ListTopicsRequest{})
	if err != nil {
		log.Fatalf("Failed to list topics: %v", err)
	}
	log.Printf("Listed topics: %+v", listResp.Topics)

	// Delete the topic.
	_, err = client.DeleteTopic(ctx, &pb.DeleteTopicRequest{
		TopicId: topic.TopicId,
	})
	if err != nil {
		log.Fatalf("Failed to delete topic: %v", err)
	}
	log.Println("Deleted topic successfully")
}

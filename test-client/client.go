package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	pubsubproto "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
)

const serverAddr = "localhost:50051" // Change if your server is running elsewhere

func main() {
	// Command-line flags for different API calls
	createTopic := flag.String("create", "", "Create a new topic (usage: --create 'topic_name')")
	getTopic := flag.String("get", "", "Get a topic by ID (usage: --get 'topic_id')")
	updateTopic := flag.String("update", "", "Update a topic name (usage: --update 'topic_id --newname 'new_topic_name'')")
	newName := flag.String("newname", "", "New topic name for update")
	deleteTopic := flag.String("delete", "", "Delete a topic by ID (usage: --delete 'topic_id')")
	listTopics := flag.Bool("list", false, "List all topics")

	flag.Parse()

	// Connect to gRPC server
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	client := pubsubproto.NewPubSubServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Handle API calls
	switch {
	case *createTopic != "":
		req := &pubsubproto.CreateTopicRequest{Name: *createTopic}
		resp, err := client.CreateTopic(ctx, req)
		if err != nil {
			log.Fatalf("CreateTopic failed: %v", err)
		}
		fmt.Printf("Created topic: %s\n", resp.Name)

	case *getTopic != "":
		req := &pubsubproto.GetTopicRequest{Id: *getTopic}
		resp, err := client.GetTopic(ctx, req)
		if err != nil {
			log.Fatalf("GetTopic failed: %v", err)
		}
		fmt.Printf("Topic: %s\n", resp.Name)

	case *updateTopic != "" && *newName != "":
		req := &pubsubproto.UpdateTopicRequest{
			Id:      *updateTopic,
			NewName: new(string),
		}
		*req.NewName = *newName
		resp, err := client.UpdateTopic(ctx, req)
		if err != nil {
			log.Fatalf("UpdateTopic failed: %v", err)
		}
		fmt.Printf("Updated topic: %s\n", resp.Name)

	case *deleteTopic != "":
		req := &pubsubproto.DeleteTopicRequest{Id: *deleteTopic}
		resp, err := client.DeleteTopic(ctx, req)
		if err != nil {
			log.Fatalf("DeleteTopic failed: %v", err)
		}
		fmt.Printf("Deleted topic: %v\n", resp.Success)

	case *listTopics:
		req := &pubsubproto.Empty{}
		resp, err := client.ListTopics(ctx, req)
		if err != nil {
			log.Fatalf("ListTopics failed: %v", err)
		}
		fmt.Println("Topics:")
		for _, topic := range resp.Topics {
			fmt.Printf("- %s\n", topic.Name)
		}

	default:
		fmt.Println("Usage:")
		fmt.Println("  --create 'topic_name'    Create a new topic")
		fmt.Println("  --get 'topic_id'         Get a topic by ID")
		fmt.Println("  --update 'topic_id' --newname 'new_name'  Update a topic name")
		fmt.Println("  --delete 'topic_id'      Delete a topic by ID")
		fmt.Println("  --list                   List all topics")
	}
}

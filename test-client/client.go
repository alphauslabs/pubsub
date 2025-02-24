package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	create := flag.String("create", "", "Create a new topic with specified name")
	get := flag.String("get", "", "Get a topic by name")
	update := flag.Bool("update", false, "Update a topic name (follow with old and new names)")
	del := flag.String("delete", "", "Delete a topic by name")
	list := flag.Bool("list", false, "List all topics")

	flag.Parse()

	if hasMultipleCommands() {
		fmt.Println("Error: Specify only one command at a time")
		os.Exit(1)
	}

	conn, err := grpc.Dial(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Connection failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewPubSubServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch {
	case *create != "":
		createTopic(ctx, client, *create)
	case *get != "":
		getTopic(ctx, client, *get)
	case *update:
		handleUpdate(ctx, client)
	case *del != "":
		deleteTopic(ctx, client, *del)
	case *list:
		listTopics(ctx, client)
	default:
		fmt.Println("Valid commands: --create, --get, --update, --delete, --list")
		os.Exit(1)
	}
}

func hasMultipleCommands() bool {
	count := len(flag.Args())
	for _, f := range []string{"create", "get", "update", "delete", "list"} {
		if flag.Lookup(f) != nil && flag.Lookup(f).Value.String() != "" {
			count--
		}
	}
	return count > 1
}

func createTopic(ctx context.Context, client pb.PubSubServiceClient, name string) {
	resp, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{Name: name})
	if err != nil {
		log.Fatalf("Create failed: %v", err)
	}
	fmt.Printf("Created:\nID: %s\nName: %s\n", resp.Id, resp.Name)
}

func getTopic(ctx context.Context, client pb.PubSubServiceClient, name string) {
	resp, err := client.GetTopic(ctx, &pb.GetTopicRequest{Id: name})
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}
	fmt.Printf("Topic:\nID: %s\nName: %s\n", resp.Id, resp.Name)
}

func handleUpdate(ctx context.Context, client pb.PubSubServiceClient) {
	args := flag.Args()
	if len(args) < 2 {
		fmt.Println("Update requires: --update 'old-name' 'new-name'")
		os.Exit(1)
	}

	oldName, newName := args[0], args[1]
	resp, err := client.UpdateTopic(ctx, &pb.UpdateTopicRequest{
		Id:      oldName,
		NewName: newName,
	})
	if err != nil {
		log.Fatalf("Update failed: %v", err)
	}
	fmt.Printf("Updated:\nOld Name: %s\nNew Name: %s\n", oldName, resp.Name)
}

func deleteTopic(ctx context.Context, client pb.PubSubServiceClient, name string) {
	resp, err := client.DeleteTopic(ctx, &pb.DeleteTopicRequest{Id: name})
	if err != nil {
		log.Fatalf("Delete failed: %v", err)
	}
	if resp.Success {
		fmt.Printf("Deleted: %s\n", name)
	}
}

func listTopics(ctx context.Context, client pb.PubSubServiceClient) {
	resp, err := client.ListTopics(ctx, &pb.Empty{})
	if err != nil {
		log.Fatalf("List failed: %v", err)
	}

	fmt.Println("Topics:")
	for _, t := range resp.Topics {
		fmt.Printf("• %-20s (ID: %s)\n", t.Id, t.Id)
	}
}

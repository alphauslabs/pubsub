package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"text/tabwriter"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	var (
		createTopic   = flag.String("create", "", "Create topic with specified name")
		getTopic      = flag.String("get", "", "Get topic by ID")
		updateTopicID = flag.String("update", "", "Topic ID to update")
		newTopicName  = flag.String("name", "", "New name for the topic")
		deleteTopicID = flag.String("delete", "", "Delete topic by ID")
		listTopics    = flag.Bool("list", false, "List all topics")
	)

	flag.Parse()

	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Connection failed: %v", err)
	}
	defer conn.Close()

	client := pb.NewPubSubServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch {
	case *createTopic != "":
		createTopicHandler(ctx, client, *createTopic)
	case *getTopic != "":
		getTopicHandler(ctx, client, *getTopic)
	case *updateTopicID != "":
		if *newTopicName == "" {
			fmt.Println("Both --update and --name are required")
			os.Exit(1)
		}
		updateTopicHandler(ctx, client, *updateTopicID, *newTopicName)
	case *deleteTopicID != "":
		deleteTopicHandler(ctx, client, *deleteTopicID)
	case *listTopics:
		listTopicsHandler(ctx, client)
	default:
		fmt.Println("See -help for usage")
		os.Exit(1)
	}
}

func createTopicHandler(ctx context.Context, client pb.PubSubServiceClient, name string) {
	resp, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{Name: name})
	if err != nil {
		log.Fatalf("Create failed!: %v", err)
	}

	fmt.Printf("Created topic:\nID: %s\nName: %s\n", resp.Id, resp.Name)
}

func getTopicHandler(ctx context.Context, client pb.PubSubServiceClient, id string) {
	resp, err := client.GetTopic(ctx, &pb.GetTopicRequest{Id: id})
	if err != nil {
		log.Fatalf("Get failed: %v", err)
	}

	fmt.Printf("Topic details:\nID: %s\nName: %s\nCreated: %v\nUpdated: %v\n",
		resp.Id,
		resp.Name,
		resp.CreatedAt.AsTime().Format(time.RFC3339),
		resp.UpdatedAt.AsTime().Format(time.RFC3339),
	)
}

func updateTopicHandler(ctx context.Context, client pb.PubSubServiceClient, id, newName string) {
	resp, err := client.UpdateTopic(ctx, &pb.UpdateTopicRequest{
		Id:      id,
		NewName: newName,
	})
	if err != nil {
		log.Fatalf("Update failed: %v", err)
	}

	fmt.Printf("Updated topic:\nID: %s\nNew Name: %s\n", resp.Id, resp.Name)
}

func deleteTopicHandler(ctx context.Context, client pb.PubSubServiceClient, id string) {
	resp, err := client.DeleteTopic(ctx, &pb.DeleteTopicRequest{Id: id})
	if err != nil {
		log.Fatalf("Delete failed: %v", err)
	}

	if resp.Success {
		fmt.Printf("Deleted topic: %s\n", id)
	}
}

func listTopicsHandler(ctx context.Context, client pb.PubSubServiceClient) {
	resp, err := client.ListTopics(ctx, &pb.Empty{})
	if err != nil {
		log.Fatalf("List failed: %v", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tCREATED\tUPDATED")
	for _, t := range resp.Topics {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
			t.Id,
			t.Name,
			t.CreatedAt.AsTime().Format(time.RFC3339),
			t.UpdatedAt.AsTime().Format(time.RFC3339),
		)
	}
	w.Flush()
}

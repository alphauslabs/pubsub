package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	pb "github.com/alphauslabs/pubsub-proto/v1"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := pb.NewPubSubServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// Test CreateSubscription
	createReq := &pb.CreateSubscriptionRequest{
		TopicId: "test-topic-id",
	}
	createResp, err := client.CreateSubscription(ctx, createReq)
	if err != nil {
		log.Fatalf("CreateSubscription failed: %v", err)
	}
	log.Printf("CreateSubscription response: %v", createResp)

	// Test GetSubscription
	getReq := &pb.GetSubscriptionRequest{
		Id: createResp.Id,
	}
	getResp, err := client.GetSubscription(ctx, getReq)
	if err != nil {
		log.Fatalf("GetSubscription failed: %v", err)
	}
	log.Printf("GetSubscription response: %v", getResp)

	// Test UpdateSubscription
	updateReq := &pb.UpdateSubscriptionRequest{
		Id:      createResp.Id,
		TopicId: "new-test-topic-id",
	}
	updateResp, err := client.UpdateSubscription(ctx, updateReq)
	if err != nil {
		log.Fatalf("UpdateSubscription failed: %v", err)
	}
	log.Printf("UpdateSubscription response: %v", updateResp)

	// Test DeleteSubscription
	deleteReq := &pb.DeleteSubscriptionRequest{
		Id: createResp.Id,
	}
	deleteResp, err := client.DeleteSubscription(ctx, deleteReq)
	if err != nil {
		log.Fatalf("DeleteSubscription failed: %v", err)
	}
	log.Printf("DeleteSubscription response: %v", deleteResp)

	// Test ListSubscriptions
	listReq := &pb.Empty{}
	listResp, err := client.ListSubscriptions(ctx, listReq)
	if err != nil {
		log.Fatalf("ListSubscriptions failed: %v", err)
	}
	log.Printf("ListSubscriptions response: %v", listResp)
}

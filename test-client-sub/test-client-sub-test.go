// This file is a test client for interacting with the CRUD operations of the SubscriptionService.
// To use this file, run `go run test-client-sub-test.go` and follow the prompts to perform CRUD operations.

package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
)

const (
	address    = "localhost:50051"
	projectID  = "labs-169405"
	instanceID = "alphaus-dev"
	databaseID = "main"
)

func main() {
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := pb.NewPubSubServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	// Example: Create a new subscription
	createReq := &pb.CreateSubscriptionRequest{
		Name:    "test-subscription",
		TopicId: "test-topic",
	}
	subscription, err := client.CreateSubscription(ctx, createReq)
	if err != nil {
		log.Fatalf("could not create subscription: %v", err)
	}
	fmt.Printf("Created subscription: %v\n", subscription)

	// Example: Get the subscription
	getReq := &pb.GetSubscriptionRequest{Id: subscription.Id}
	subscription, err = client.GetSubscription(ctx, getReq)
	if err != nil {
		log.Fatalf("could not get subscription: %v", err)
	}
	fmt.Printf("Got subscription: %v\n", subscription)

	// Example: Update the subscription
	updateReq := &pb.UpdateSubscriptionRequest{
		Id:      subscription.Id,
		NewName: "updated-subscription",
		Topic:   "updated-topic",
	}
	subscription, err = client.UpdateSubscription(ctx, updateReq)
	if err != nil {
		log.Fatalf("could not update subscription: %v", err)
	}
	fmt.Printf("Updated subscription: %v\n", subscription)

	// Example: Delete the subscription
	deleteReq := &pb.DeleteSubscriptionRequest{Id: subscription.Id}
	deleteResp, err := client.DeleteSubscription(ctx, deleteReq)
	if err != nil {
		log.Fatalf("could not delete subscription: %v", err)
	}
	fmt.Printf("Deleted subscription: %v\n", deleteResp)

	// Example: List all subscriptions
	listReq := &pb.Empty{}
	listResp, err := client.ListSubscriptions(ctx, listReq)
	if err != nil {
		log.Fatalf("could not list subscriptions: %v", err)
	}
	fmt.Printf("List of subscriptions: %v\n", listResp.Subscriptions)
}

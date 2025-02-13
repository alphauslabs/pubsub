package main

import (
    "context"
    "log"
    "time"
    "google.golang.org/grpc"
    pb "github.com/yourdomain/pubsub/proto"
)

func main() {
    conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
    if err != nil {
        log.Fatalf("Failed to connect: %v", err)
    }
    defer conn.Close()

    client := pb.NewTopicServiceClient(conn)
    ctx, cancel := context.WithTimeout(context.Background(), time.Second)
    defer cancel()

    // Test CreateTopic
    topic, err := client.CreateTopic(ctx, &pb.CreateTopicRequest{
        Name: "test-topic",
    })
    if err != nil {
        log.Fatalf("Could not create topic: %v", err)
    }
    log.Printf("Created topic: %v", topic)

    // Add more test calls...
}

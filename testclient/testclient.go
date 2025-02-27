package main

import (
	"context"
	"flag"
	"io"
	"log"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var method = flag.String("method", "", "gRPC method to call")

func main() {
	flag.Parse()
	log.Printf("[Test] method: %v", *method)

	conn, err := grpc.NewClient("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewPubSubServiceClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch *method {
	case "publish":
		r, err := c.Publish(ctx, &pb.PublishRequest{TopicId: "topic1", Payload: "Hello World"})
		if err != nil {
			log.Fatalf("Publish failed: %v", err)
		}
		log.Println(r.MessageId)
	case "subscribe":
		r, err := c.Subscribe(ctx, &pb.SubscribeRequest{TopicId: "topic1", SubscriptionId: "sub1"})
		if err != nil {
			log.Fatalf("Subscribe failed: %v", err)
		}

		for {
			rec, err := r.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				log.Printf("Error: %v", err)
				break
			}

			log.Printf("rec.Payload: %v\n", rec.Payload)
			time.Sleep(20 * time.Second) // simulate processing
			ackres, err := c.Acknowledge(ctx, &pb.AcknowledgeRequest{Id: rec.Id, SubscriptionId: "sub1"})
			if err != nil {
				log.Fatalf("Acknowledge failed: %v", err)
			}
			log.Printf("Acknowledge Response: %v\n", ackres)
		}
	default:
		log.Printf("Unsupported method: %s", *method)
	}
}

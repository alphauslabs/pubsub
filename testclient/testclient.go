package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	method = flag.String("method", "", "gRPC method to call")
	host   = flag.String("host", "localhost", "gRPC server host")
	input  = flag.String("input", "", "input data: fmt: {topicId}|{subId}|{payload}|{newtopicname} , Please leave empty if not needed, don't remove | separator")
)

func main() {
	flag.Parse()
	log.Printf("[Test] method: %v", *method)
	ins := strings.Split(*input, "|")
	if len(ins) != 4 {
		log.Fatalf("Invalid input: %v", *input)
	}
	topic, sub, payload, newtopicname := ins[0], ins[1], ins[2], ins[3]
	conn, err := grpc.NewClient(fmt.Sprintf("%v:50051", *host), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewPubSubServiceClient(conn)

	// Contact the server and print out its response.

	switch *method {
	case "publish":
		r, err := c.Publish(context.Background(), &pb.PublishRequest{TopicId: topic, Payload: payload})
		if err != nil {
			log.Fatalf("Publish failed: %v", err)
		}
		log.Printf("Message Published!\nID: %s", r.MessageId)

	case "listtopics":
		r, err := c.ListTopics(context.Background(), &pb.Empty{})
		if err != nil {
			log.Fatalf("Listing failed: %v", err)
		}
		fmt.Printf("r.Topics: %v\n", r.Topics)
	case "deletetopic":
		r, err := c.DeleteTopic(context.Background(), &pb.DeleteTopicRequest{Id: topic})
		if err != nil {
			log.Fatalf("Delete failed: %v", err)
		}
		if r.Success {
			log.Printf("Topic ID: %s deleted sucessfully", topic)
		} else {
			log.Printf("Topic ID: %s not found", topic)
		}
	case "updatetopic":
		r, err := c.UpdateTopic(context.Background(), &pb.UpdateTopicRequest{
			Id:      topic,
			NewName: newtopicname,
		})
		if err != nil {
			log.Fatalf("Update Failed: %v", err)
		}
		log.Printf("Updated!\nID: %s\nPrevious Name:\nNew Name:%s\n", r.Id, r.Name)
	case "gettopic":
		r, err := c.GetTopic(context.Background(), &pb.GetTopicRequest{Id: topic})
		if err != nil {
			log.Fatalf("Read Failed: %v", err)
		}
		log.Printf("Topic Found!\nID: %s\nName: %s\n", r.Id, r.Name)
	case "createtopic":
		r, err := c.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: topic})
		if err != nil {
			log.Fatalf("Create Failed: %v", err)
		}
		log.Printf("Topic Created!\nID: %s\nName: %s\n", r.Id, r.Name)
	case "subscribe":
		r, err := c.Subscribe(context.Background(), &pb.SubscribeRequest{TopicId: topic, SubscriptionId: sub})
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
			ackres, err := c.Acknowledge(context.Background(), &pb.AcknowledgeRequest{Id: rec.Id, SubscriptionId: sub})
			if err != nil {
				log.Fatalf("Acknowledge failed: %v", err)
			}
			log.Printf("Acknowledge Response: %v\n", ackres)
		}

	default:
		fmt.Println("Invalid method, try again...")
	}
}

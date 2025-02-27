package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	"os"
	"text/tabwriter"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	method := flag.String("method", "", "gRPC method to call")
	topicName := flag.String("name", "", "Topic name for create")
	topicID := flag.String("id", "", "Topic ID for create")
	newTopicName := flag.String("newName", "", "New topic name")
	payload := flag.String("payload", "", "Payload for create")

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
		/* --- making it dynamic ------
		r, err := c.Publish(ctx, &pb.PublishRequest{Topic: "topic1", Payload: "Hello World"})
		if err != nil {
			log.Fatalf("Publish failed: %v", err)
		}
		log.Println(r.MessageId)
		*/
		if *topicID == "" || *payload == "" {
			if *topicID == "" {
				log.Fatal("Topic name required!")
			}
			if *payload == "" {
				n, _ := rand.Int(rand.Reader, big.NewInt(10000))
				*payload = fmt.Sprintf("Hello World: %d", n)
			}
		}
		r, err := c.Publish(ctx, &pb.PublishRequest{TopicId: *topicID, Payload: *payload})
		if err != nil {
			log.Fatalf("Publish failed: %v", err)
		}
		log.Printf("Message Published!\nID: %s", r.MessageId)

	case "list":
		r, err := c.ListTopics(ctx, &pb.Empty{})
		if err != nil {
			log.Fatalf("Listing failed: %v", err)
		}
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "ID\tNAME\tCREATED\tUPDATED")
		for _, t := range r.Topics {
			fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
				t.Id,
				t.Name,
				t.CreatedAt.AsTime().Format(time.RFC3339),
				t.UpdatedAt.AsTime().Format(time.RFC3339))
		}
		w.Flush()
	case "delete":
		if *topicID == "" {
			log.Fatal("ID of the Topic is Required!")
		}
		r, err := c.DeleteTopic(ctx, &pb.DeleteTopicRequest{Id: *topicID})
		if err != nil {
			log.Fatalf("Delete failed: %v", err)
		}
		if r.Success {
			log.Printf("Topic ID: %s deleted sucessfully", *topicID)
		}
	case "update":
		if *topicID == "" || *newTopicName == "" {
			if *topicID == "" {
				log.Fatal("ID of existing topic is required")
			}
			if *newTopicName == " " {
				log.Fatal("New name of that topic is required")
			}
		}
		r, err := c.UpdateTopic(ctx, &pb.UpdateTopicRequest{
			Id:      *topicID,
			NewName: *newTopicName,
		})
		if err != nil {
			log.Fatalf("Update Failed: %v", err)
		}
		log.Printf("Updated!\nID: %s\nPrevious Name:\nNew Name:%s\n", r.Id, r.Name)
	case "get":
		if *topicID == "" {
			log.Fatal("ID is required")
		}
		r, err := c.GetTopic(ctx, &pb.GetTopicRequest{Id: *topicID})
		if err != nil {
			log.Fatalf("Read Failed: %v", err)
		}
		log.Printf("-----Topic details----\nID: %s\nName:: %s\nCraated: %s\nUpdated: %s\n",
			r.Id,
			r.Name,
			r.CreatedAt.AsTime().Format(time.RFC3339),
			r.UpdatedAt.AsTime().Format(time.RFC3339),
		)
	case "create":
		if *topicName == "" {
			log.Fatal("Topic name is required")
		}
		r, err := c.CreateTopic(ctx, &pb.CreateTopicRequest{Name: *topicName})
		if err != nil {
			log.Fatalf("Create Failed: %v", err)
		}
		log.Printf("Created Sucessfully\n ID: %v\nName:%s",
			r.Id, r.Name)
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
		}

	default:
		fmt.Println("Available methods:")
		fmt.Println("  create  : --method=create --name=<topic_name>")
		fmt.Println("  get     : --method=get --id=<topic_id>")
		fmt.Println("  update  : --method=update --id=<topic_id> --newName=<new_name>")
		fmt.Println("  delete  : --method=delete --id=<topic_id>")
		fmt.Println("  list    : --method=list")
		fmt.Println("  publish : --method=publish --id=<topic_id> --payload=<message>")
		fmt.Println("  subscribe : --method=subscribe")
		os.Exit(1)
	}
}

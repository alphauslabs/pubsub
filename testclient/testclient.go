package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/golang/glog"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	method = flag.String("method", "", "gRPC method to call")
	host   = flag.String("host", "localhost", "gRPC server host")
	input  = flag.String("input", "", "input data: fmt: {topicName}|{SubscriptionName}|{payload}|{newtopicname}|{extendVisibility} , Please leave empty if not needed, don't remove | separator")
)

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()
	defer glog.Flush()
	glog.Infof("[Test] method: %v", *method)
	ins := strings.Split(*input, "|")
	if len(ins) != 5 {
		log.Fatalf("Invalid input: %v", *input)
	}
	topic, sub, payload, newtopicname, extendVisibility := ins[0], ins[1], ins[2], ins[3], ins[4]
	conn, err := grpc.NewClient(fmt.Sprintf("%v:50051", *host), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewPubSubServiceClient(conn)

	// Contact the server and print out its response.

	switch *method {
	case "publish":
		r, err := c.Publish(context.Background(), &pb.PublishRequest{Topic: topic, Payload: payload})
		if err != nil {
			log.Fatalf("Publish failed: %v", err)
		}
		glog.Infof("Message Published!\nID: %s", r.MessageId)

	case "listtopics":
		r, err := c.ListTopics(context.Background(), &pb.Empty{})
		if err != nil {
			log.Fatalf("Listing failed: %v", err)
		}
		fmt.Printf("r.Topics: %v\n", r.Topics)
	case "deletetopic":
		r, err := c.DeleteTopic(context.Background(), &pb.DeleteTopicRequest{Name: topic})
		if err != nil {
			log.Fatalf("Delete failed: %v", err)
		}
		if r.Success {
			glog.Infof("Topic ID: %s deleted sucessfully", topic)
		} else {
			glog.Infof("Topic ID: %s not found", topic)
		}
	case "updatetopic":
		_, err := c.UpdateTopic(context.Background(), &pb.UpdateTopicRequest{
			Name:    topic,
			NewName: newtopicname,
		})
		if err != nil {
			log.Fatalf("Update Failed: %v", err)
		}
		glog.Infof("Updated!\nID: %s\nPrevious Name:\nNew Name:%s\n", topic, newtopicname)
	case "gettopic":
		_, err := c.GetTopic(context.Background(), &pb.GetTopicRequest{Name: topic})
		if err != nil {
			log.Fatalf("Read Failed: %v", err)
		}
		glog.Infof("Topic Found!\nID: %s\nName: %s\n", topic)
	case "createtopic":
		_, err := c.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: topic})
		if err != nil {
			log.Fatalf("Create Failed: %v", err)
		}
		glog.Infof("Topic Created!\nID: %s\nName: %s\n", topic)
	case "subscribe":
		r, err := c.Subscribe(context.Background(), &pb.SubscribeRequest{Topic: topic, Subscription: sub})
		if err != nil {
			log.Fatalf("Subscribe failed: %v", err)
		}

		for {
			rec, err := r.Recv()
			if err == io.EOF {
				break
			}

			if err != nil {
				glog.Infof("Error: %v", err)
				break
			}

			glog.Infof("rec.Payload: %v\n", rec.Payload)
			ackres, err := c.Acknowledge(context.Background(), &pb.AcknowledgeRequest{Id: rec.Id, Subscription: sub})
			if err != nil {
				log.Fatalf("Acknowledge failed: %v", err)
			}
			glog.Infof("Acknowledge Response: %v\n", ackres)
		}

	case "extendvisibility":
	    if payload == "" {
	        log.Fatalf("ExtendVisibilityTimeout requires a valid message ID in the payload field")
	    }
	
	    // Convert the extendVisibility input (new timeout) to an integer
	    newTimeout, err := strconv.Atoi(newtopicname) // Assuming newtopicname holds the timeout value
	    if err != nil {
	        log.Fatalf("Invalid timeout value: %v", err)
	    }
	
	    // Ensure the subscription ID is provided
	    if sub == "" {
	        log.Fatalf("ExtendVisibilityTimeout requires a valid subscription ID.")
	    }
	
	    // Call ModifyVisibilityTimeout with the correct parameters
	    r, err := c.ModifyVisibilityTimeout(context.Background(), &pb.ModifyVisibilityTimeoutRequest{
	        Id:              payload,           
	        NewTimeout:      int32(newTimeout), 
	        SubscriptionId:  sub,               
	    })
	    if err != nil {
	        log.Fatalf("ExtendVisibilityTimeout failed: %v", err)
	    }
	
	    log.Printf("Visibility Timeout Extended! Success = %v", r.Success)


	case "createsubscription":
		if topic == "" || sub == "" {
			log.Fatalf("CreateSubscription requires topic and subscription name")
		}

		autoExtend := false
		if newtopicname != "" {
			parsedAutoExtend, err := strconv.ParseBool(newtopicname)
			if err != nil {
				log.Fatalf("Invalid autoextend value (must be true or false): %v", err)
			}
			autoExtend = parsedAutoExtend
		}

		r, err := c.CreateSubscription(context.Background(), &pb.CreateSubscriptionRequest{
			Topic:      topic,
			Name:       sub,
			Autoextend: &autoExtend,
		})
		if err != nil {
			log.Fatalf("Create Subscription Failed: %v", err)
		}

		glog.Infof("Subscription Created! Name: %s, Topic: %s, AutoExtend: %v", r.Name, r.Topic, r.Autoextend)

	case "getsubscription":
		r, err := c.GetSubscription(context.Background(), &pb.GetSubscriptionRequest{Name: sub})
		if err != nil {
			log.Fatalf("Get Subscription Failed: %v", err)
		}
		glog.Infof("Subscription Found! Name: %s, Topic: %s, AutoExtend: %v", r.Name, r.Topic, r.Autoextend)

	default:
		fmt.Println("Invalid method, try again...")
	}
}

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
	topic, sub, payload, newtopicname := ins[0], ins[1], ins[2], ins[3]
	conn, err := grpc.NewClient(fmt.Sprintf("%v:50051", *host), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewPubSubServiceClient(conn)

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
		glog.Infof("Topic Found!\nName: %s\n", topic)
	case "createtopic":
		_, err := c.CreateTopic(context.Background(), &pb.CreateTopicRequest{Name: topic})
		if err != nil {
			log.Fatalf("Create Failed: %v", err)
		}
		glog.Infof("Topic Created!\nName: %s\n", topic)
	case "subscribe":
		processingTime := flag.Int("processingTime", 10, "Simulated message processing time in seconds")
		extendVisibility := flag.Bool("extendVisibility", false, "Enable manual visibility extension for non-autoextend subscriptions")
		flag.Parse()

		// Check if subscription is autoextend
		subDetails, err := c.GetSubscription(context.Background(), &pb.GetSubscriptionRequest{Name: sub})
		if err != nil {
			log.Fatalf("Failed to get subscription details: %v", err)
		}
		isAutoExtend := subDetails.Autoextend // check if the subscription has autoextend enabled

		glog.Infof("[Subscribe] Subscription: %s, AutoExtend: %t, ExtendVisibilityFlag: %t", sub, isAutoExtend, *extendVisibility)

		r, err := c.Subscribe(context.Background(), &pb.SubscribeRequest{Topic: topic, Subscription: sub})
		if err != nil {
			log.Fatalf("Subscribe failed: %v", err)
		}

		ackCount := 0 //counter for mssges

	messageLoop:
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

			if *processingTime > 0 { // set processingTime=0 for instant acknowledgment
				startTime := time.Now()
				ticker := time.NewTicker(5 * time.Second)
				extendThreshold := 20 * time.Second // When to request extension
				processingDone := time.After(time.Duration(*processingTime) * time.Second)

				stopExtension := make(chan bool, 1)

				// Handle visibility extension for non-autoextend subscriptions
				if !isAutoExtend && *extendVisibility {
					go func() {
						defer glog.Infof("[Extension] Stopped extension requests for message %s", rec.Id)

						for {
							select {
							case <-time.After(extendThreshold):
								glog.Infof("Requesting visibility extension for message %s", rec.Id)
								_, err := c.ModifyVisibilityTimeout(context.Background(), &pb.ModifyVisibilityTimeoutRequest{
									Id:             rec.Id,
									SubscriptionId: sub,
								})
								if err != nil {
									glog.Errorf("Failed to extend visibility for message %s: %v", rec.Id, err)
								}
							case <-stopExtension:
								return // Stop requesting visibility extension once processing is done
							}
						}
					}()
				}

				// Processing loop
				for {
					select {
					case <-ticker.C:
						elapsed := time.Since(startTime).Seconds()
						glog.Infof("[Processing] Message %v processing... elapsed time: %.2f seconds", rec.Id, elapsed)

					case <-processingDone:
						ticker.Stop()
						glog.Infof("[Processing] Completed message %v processing after %d seconds", rec.Id, *processingTime)

						if !isAutoExtend && *extendVisibility {
							select {
							case stopExtension <- true:
							default:
							}
							close(stopExtension)
						}
						break messageLoop
					}
				}
			}
			//Acknowledge the message
			ackres, err := c.Acknowledge(context.Background(), &pb.AcknowledgeRequest{Id: rec.Id, Subscription: sub})
			if err != nil {
				log.Fatalf("Acknowledge failed: %v", err)
			}
			glog.Infof("Acknowledge Response: %v\n", ackres)
			ackCount++ //increment
		}
		glog.Infof("Total Messages Acknowledged: %v\n", ackCount)

	case "createsubscription":

	case "getsubscription":

	case "updatesubscription":
		_, err := c.UpdateSubscription(context.Background(), &pb.UpdateSubscriptionRequest{
			Name:                    sub,
			ModifyVisibilityTimeout: 60,
			//			Autoextend:              true,
		})
		if err != nil {
			log.Fatalf("UpdateSubscription failed: %v", err)
		}
		glog.Infof("Subscription Updated!\nID: %s\n", sub)

	case "deletesubscription":
		r, err := c.DeleteSubscription(context.Background(), &pb.DeleteSubscriptionRequest{Name: sub})
		if err != nil {
			log.Fatalf("DeleteSubscription failed: %v", err)
		}
		if r.Success {
			glog.Infof("Subscription ID: %s deleted successfully", sub)
		} else {
			glog.Infof("Subscription ID: %s not found", sub)
		}

	case "listsubscriptions":
		r, err := c.ListSubscriptions(context.Background(), &pb.Empty{})
		if err != nil {
			log.Fatalf("ListSubscriptions failed: %v", err)
		}
		fmt.Printf("r.Subscriptions: %v\n", r.Subscriptions)

	default:
		fmt.Println("Invalid method, try again...")
	}
}

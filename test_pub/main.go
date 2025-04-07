package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/golang/glog"
	"google.golang.org/grpc"
)

var (
	numMessages  = flag.Int("numMessages", 10000, "Number of messages to publish")
	host         = flag.String("host", "localhost", "gRPC server host")
	triggerPanic = flag.Bool("triggerpanic", false, "Trigger panic in the server")
	topics       = flag.String("topics", "", "Topics to publish messages to, fmt: {topic1},{topic2}")
)

func publishMessage(wg *sync.WaitGroup, id int, topic string, ch chan int, client pb.PubSubServiceClient) {
	defer wg.Done()
	msg := &pb.PublishRequest{
		Payload: fmt.Sprintf("Message %d for topic=%v", id, topic),
		Topic:   topic,
		Attributes: map[string]string{
			fmt.Sprintf("key%v", id): fmt.Sprintf("value%v", id),
		},
	}
	if *triggerPanic {
		if id%3 == 0 {
			msg.Attributes["triggerpanic"] = "yes"
		}
	}

	ctx := context.Background()
	resp, err := client.Publish(ctx, msg)
	if err != nil {
		glog.Errorf("[ERROR] Message %d to %s failed: %v", id, topic, err)
		ch <- 1
		return
	}

	glog.Infof("[SUCCESS] Message %d published to %s. Message ID: %s", id, topic, resp.MessageId)
}

func connectToGRPC(endpoint string) (pb.PubSubServiceClient, error) {
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %v", endpoint, err)
	}
	return pb.NewPubSubServiceClient(conn), nil
}

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()

	client, err := connectToGRPC(fmt.Sprintf("%v:50051", *host))
	if err != nil {
		glog.Errorf("Failed to connect to gRPC server: %v", err)
		return
	}
	var wg sync.WaitGroup
	startTime := time.Now()
	counterr := make(chan int, *numMessages)

	ts := strings.Split(*topics, ",")
	for _, t := range ts {
		for i := range *numMessages {
			wg.Add(1)
			go publishMessage(&wg, i, t, counterr, client)
		}
	}
	wg.Wait()
	close(counterr)
	c := 0
	for range counterr {
		c++
	}

	duration := time.Since(startTime)
	glog.Infof("Total Messages published %d", *numMessages-c)
	glog.Infof("Total Time: %.2f seconds", duration.Seconds())
	glog.Infof("Throughput: %.2f messages/second", float64(*numMessages)/duration.Seconds())
}

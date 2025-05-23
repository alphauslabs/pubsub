package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	pbsb "github.com/alphauslabs/pubsub-sdk-go"
	"github.com/golang/glog"
)

var (
	numMessages  = flag.Int("numMessages", 10000, "Number of messages to publish")
	host         = flag.String("host", "localhost", "gRPC server host")
	triggerPanic = flag.Bool("triggerpanic", false, "Trigger panic in the server")
	topics       = flag.String("topics", "", "Topics to publish messages to, fmt: {topic1},{topic2}")
)

func publishMessage(id int, topic string, ch chan int, client *pbsb.PubSubClient) {
	msg := &pbsb.PublishRequest{
		Message: fmt.Sprintf("Message %d for topic=%v", id, topic),
		Topic:   topic,
		Attributes: map[string]string{
			fmt.Sprintf("key%v", id): fmt.Sprintf("value%v", id),
		},
		RetryLimit: 30,
	}
	if *triggerPanic {
		if id%3 == 0 {
			msg.Attributes["triggerpanic"] = "yes"
		}
	}

	ctx := context.Background()

	err := client.Publish(ctx, msg)
	if err != nil {
		glog.Errorf("[ERROR] Failed to publish message %d to topic %s: %v", id, topic, err)
		ch <- 1
		return
	}

	glog.Infof("[SUCCESS] Message %v publshed, topic=%s", id, topic)
}

func main() {
	flag.Set("logtostderr", "true")
	flag.Parse()

	var wg sync.WaitGroup
	startTime := time.Now()
	counterr := make(chan int, *numMessages)

	cclient, err := pbsb.New()
	if err != nil {
		glog.Errorf("Failed to create PubSub client: %v", err)
		return
	}

	defer cclient.Close()

	limit := make(chan struct{}, 100)
	ts := strings.Split(*topics, ",")
	for _, t := range ts {
		for i := range *numMessages {
			limit <- struct{}{}
			wg.Add(1)
			go func() {
				defer func() {
					<-limit
					wg.Done()
				}()
				publishMessage(i, t, counterr, cclient)
			}()
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

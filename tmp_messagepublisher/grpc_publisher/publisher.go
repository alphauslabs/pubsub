package main

import (
	"flag"
	"log"
	"sync"
	"time"

	pubsubproto "github.com/alphauslabs/pubsub-proto/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

var (
	numMessages  = flag.Int("numMessages", 10000, "Number of messages to publish")
	useMock      = flag.Bool("mock", false, "Use localhost mock gRPC servers")
	useBulkWrite = flag.Bool("bulkwrite", false, "Use bulk write GCP gRPC endpoints")
)

var (
	directWriteEndpoints = []string{
		"10.146.0.43:8085",
		"10.146.0.46:8085",
		"10.146.0.51:8085",
	}

	bulkWriteEndpoints = []string{
		"10.146.0.43:8080",
		"10.146.0.46:8080",
		"10.146.0.51:8080",
	}

	mockEndpoints = []string{
		"localhost:8080",
		"localhost:8081",
		"localhost:8082",
	}

	activeEndpoints []string
	clients         []pubsubproto.PubSubServiceClient
	connections     []*grpc.ClientConn
)

const maxWorkers = 100

// func publishMessage(client pubsubproto.PubSubServiceClient, id int) {

// 	topicID := fmt.Sprintf("topic-%d", id%1000)

// 	msg := &pubsubproto.Message{
// 		Id:      fmt.Sprintf("%d", id),
// 		Topic:   topicID,
// 		Payload: []byte(fmt.Sprintf("[MESSAGE TO NODE %d]", id%len(activeEndpoints)+1)),
// 	}

// 	ctx := context.Background()

// 	for retry := 0; retry < 3; retry++ {
// 		res, err := client.Publish(ctx, msg)
// 		if err == nil {
// 			log.Printf("[INFO] Message %d published successfully: ID-%s", id, res.MessageId)
// 			return
// 		}
// 		log.Printf("[ERROR] Message %d failed to publish: %v", id, err)
// 		time.Sleep(200 * time.Millisecond)
// 	}
// }

func setActiveEndpoints() {
	if *useMock {
		activeEndpoints = mockEndpoints
	} else if *useBulkWrite {
		activeEndpoints = bulkWriteEndpoints
	} else {
		activeEndpoints = directWriteEndpoints
	}
}

func createClients() {
	clients = make([]pubsubproto.PubSubServiceClient, len(activeEndpoints))
	connections = make([]*grpc.ClientConn, len(activeEndpoints))

	for i, endpoint := range activeEndpoints {
		conn, err := grpc.Dial(endpoint,
			grpc.WithInsecure(),
			grpc.WithBlock(),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                10 * time.Second,
				Timeout:             5 * time.Second,
				PermitWithoutStream: true,
			}),
		)
		if err != nil {
			log.Fatalf("Failed to connect to gRPC server %s: %v", endpoint, err)
		}

		connections[i] = conn
		clients[i] = pubsubproto.NewPubSubServiceClient(conn)
	}
}

func main() {
	flag.Parse()
	setActiveEndpoints()
	createClients()

	log.Printf("Active endpoints: %v", activeEndpoints)

	taskCh := make(chan int, *numMessages)
	wg := sync.WaitGroup{}

	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// for id := range taskCh {
			// 	// client := clients[id%len(clients)]
			// 	// publishMessage(client, id)
			// }
		}()
	}

	startTime := time.Now()

	for i := 0; i < *numMessages; i++ {
		taskCh <- i
	}
	close(taskCh)

	wg.Wait()

	for _, conn := range connections {
		if err := conn.Close(); err != nil {
			log.Printf("Failed to close connection: %v", err)
		}
	}

	duration := time.Since(startTime)

	log.Printf("[SUMMARY] Total Messages: %d", *numMessages)
	log.Printf("[SUMMARY] Total Time: %.2f seconds", duration.Seconds())
	log.Printf("[SUMMARY] Throughput: %.2f messages/second", float64(*numMessages)/duration.Seconds())
}

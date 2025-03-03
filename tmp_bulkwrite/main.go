package main

import (
	"context"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pubsubproto "bulkwrite/bulkwrite_proto"
)

var (
	isLeader       = flag.Bool("leader", false, "Run this node as the leader for bulk writes")
	leaderURL      = flag.String("leader-url", "10.146.0.43:50050", "URL of the leader node")
	followerPort   = flag.String("follower-port", "50051", "Port for the follower node") // New flag for follower port
	batchSize      = flag.Int("batchsize", 5000, "Batch size for bulk writes")
	messagesBuffer = flag.Int("messagesbuffer", 1000000, "Buffer size for messages channel")
	waitTime       = flag.Duration("waittime", 500*time.Millisecond, "Wait time before flushing the batch")
	numWorkers     = flag.Int("workers", 32, "Number of concurrent workers")
	numShards      = 16
)

type BatchStats struct {
	totalBatches  int
	totalMessages int
}

var (
	shardedChans = make([]chan *pubsubproto.PublishRequest, numShards) // Sharded channels for messages
	workerPool   = make(chan struct{}, *numWorkers)                    // Worker pool semaphore
	wg           sync.WaitGroup
)

func init() {
	for i := 0; i < numShards; i++ {
		shardedChans[i] = make(chan *pubsubproto.PublishRequest, *messagesBuffer/numShards)
	}
}

func getShard(message *pubsubproto.PublishRequest) int {
	hash := fnv.New32a()
	hash.Write([]byte(message.TopicId)) // Use topic ID for hashing
	return int(hash.Sum32()) % numShards
}

func createClients(ctx context.Context, db string) (*spanner.Client, error) {
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		return nil, fmt.Errorf("failed to create Spanner client: %v", err)
	}
	return client, nil
}

func WriteBatchUsingDML(w io.Writer, client *spanner.Client, batch []*pubsubproto.PublishRequest) error {
	ctx := context.Background()

	mutations := make([]*spanner.Mutation, 0, len(batch))
	for _, message := range batch {
		id := uuid.New().String()
		// currentTime := time.Now().Format(time.RFC3339)
		mutations = append(mutations, spanner.Insert(
			"Messages",
			[]string{"id", "topic", "payload", "createdAt", "updatedAt"},
			[]interface{}{id, message.TopicId, message.Payload, spanner.CommitTimestamp, spanner.CommitTimestamp},
		))
	}

	_, err := client.Apply(ctx, mutations)
	if err != nil {
		return fmt.Errorf("failed to apply mutations: %v", err)
	}
	fmt.Fprintf(w, "BATCH record() inserted.\n")
	return nil
}

func startPublisherListener() {
	glog.Info("[FO] Listening for publisher messages...")
}

func runBulkWriterAsLeader(workerID int) {
	defer wg.Done()

	db := "projects/labs-169405/instances/alphaus-dev/databases/main"
	ctx := context.Background()
	client, err := createClients(ctx, db)
	if err != nil {
		log.Fatalf("Failed to create clients: %v", err)
	}
	defer client.Close()

	var batch []*pubsubproto.PublishRequest
	stats := BatchStats{}

	for {
		select {
		case msg := <-shardedChans[workerID%numShards]:
			batch = append(batch, msg)
			if len(batch) >= *batchSize {
				stats.totalBatches++
				stats.totalMessages += len(batch)
				workerPool <- struct{}{} // Acquire worker
				go func(batch []*pubsubproto.PublishRequest) {
					defer func() { <-workerPool }() // Release worker
					err := WriteBatchUsingDML(log.Writer(), client, batch)
					if err != nil {
						glog.Infof("[LEADER] Error writing batch to Spanner: %v\n", err)
					} else {
						glog.Infof("Successfully wrote batch of %d messages\n", len(batch))
					}
				}(batch)
				batch = nil
			}
		case <-time.After(*waitTime):
			if len(batch) > 0 {
				stats.totalBatches++
				stats.totalMessages += len(batch)
				workerPool <- struct{}{} // Acquire worker
				go func(batch []*pubsubproto.PublishRequest) {
					defer func() { <-workerPool }() // Release worker
					err := WriteBatchUsingDML(log.Writer(), client, batch)
					if err != nil {
						glog.Infof("[LEADER] Error writing batch to Spanner: %v\n", err)
					} else {
						glog.Infof("Successfully wrote batch of %d messages\n", len(batch))
					}
				}(batch)
				batch = nil
			}
		}
	}
}
func runBulkWriterAsFollower() {
	// Connect to the leader's gRPC server

	conn, err := grpc.Dial(
		*leaderURL,
		grpc.WithInsecure(),
		grpc.WithTimeout(10*time.Second), // Increase timeout
	)
	if err != nil {
		log.Fatalf("[FOLLOWER] Failed to connect to leader: %v", err)
	}
	defer conn.Close()

	client := pubsubproto.NewPubSubServiceClient(conn)

	// Start the gRPC server for the follower
	lis, err := net.Listen("tcp", ":"+*followerPort) // Use the followerPort flag
	if err != nil {
		log.Fatalf("[FOLLOWER] Failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pubsubproto.RegisterPubSubServiceServer(s, &server{client: client}) // Pass the client to the server
	glog.Infof("[FOLLOWER] gRPC server is running on :%s\n", *followerPort)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("[FOLLOWER] Failed to serve: %v", err)
		}
	}()

	// Handle incoming messages via gRPC
	glog.Info("[FOLLOWER] Follower is now listening for messages from the consumer...")
	select {}
}

// BatchWrite implements the BatchWrite RPC method.
func (s *server) BatchWrite(ctx context.Context, req *pubsubproto.BatchWriteRequest) (*pubsubproto.BatchWriteResponse, error) {
	db := "projects/labs-169405/instances/alphaus-dev/databases/main"
	client, err := createClients(ctx, db)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to create Spanner client: %v", err)
	}
	defer client.Close()

	err = WriteBatchUsingDML(log.Writer(), client, req.Messages)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "failed to write batch: %v", err)
	}

	return &pubsubproto.BatchWriteResponse{Success: true}, nil
}

func (s *server) Subscribe(req *pubsubproto.SubscribeRequest, stream pubsubproto.PubSubService_SubscribeServer) error {
	return status.Errorf(codes.Unimplemented, "method Subscribe not implemented")
}

// Acknowledge implements the Acknowledge RPC method.
func (s *server) Acknowledge(ctx context.Context, req *pubsubproto.AcknowledgeRequest) (*pubsubproto.AcknowledgeResponse, error) {
	// Placeholder implementation
	return nil, status.Errorf(codes.Unimplemented, "method Acknowledge not implemented")
}

// ModifyVisibilityTimeout implements the ModifyVisibilityTimeout RPC method.
func (s *server) ModifyVisibilityTimeout(ctx context.Context, req *pubsubproto.ModifyVisibilityTimeoutRequest) (*pubsubproto.ModifyVisibilityTimeoutResponse, error) {
	// Placeholder implementation
	return nil, status.Errorf(codes.Unimplemented, "method ModifyVisibilityTimeout not implemented")
}

// LeaderHealthCheck implements the LeaderHealthCheck RPC method.
func (s *server) LeaderHealthCheck(ctx context.Context, req *pubsubproto.LeaderHealthCheckRequest) (*pubsubproto.LeaderHealthCheckResponse, error) {
	// Placeholder implementation
	return &pubsubproto.LeaderHealthCheckResponse{IsLeaderAlive: true}, nil
}

// LeaderElection implements the LeaderElection RPC method.
func (s *server) LeaderElection(ctx context.Context, req *pubsubproto.LeaderElectionRequest) (*pubsubproto.LeaderElectionResponse, error) {
	// Placeholder implementation
	return &pubsubproto.LeaderElectionResponse{IsElected: true}, nil
}

type server struct {
	pubsubproto.UnimplementedPubSubServiceServer
	client pubsubproto.PubSubServiceClient // Add this field
}

// Publish handles messages from clients or followers.
func (s *server) Publish(ctx context.Context, req *pubsubproto.PublishRequest) (*pubsubproto.PublishResponse, error) { // Forward the message to the leader
	shardID := getShard(req)
	forwardReq := &pubsubproto.ForwardMessageRequest{
		Message: req,
		ShardId: int32(shardID),
	}

	_, err := s.client.ForwardMessage(ctx, forwardReq)
	if err != nil {
		glog.Infof("[FOLLOWER] Error forwarding message to leader: %v\n", err)
		return nil, status.Errorf(codes.Internal, "failed to forward message to leader: %v", err)
	}

	glog.Infof("[FOLLOWER] Message forwarded to leader successfully: %v\n", req)
	return &pubsubproto.PublishResponse{MessageId: "00"}, nil
}

// ForwardMessage handles messages forwarded from followers to the leader.
func (s *server) ForwardMessage(ctx context.Context, req *pubsubproto.ForwardMessageRequest) (*pubsubproto.ForwardMessageResponse, error) {
	shardedChans[req.ShardId] <- req.Message
	return &pubsubproto.ForwardMessageResponse{Success: true}, nil
}

func main() {
	flag.Parse()

	// Start the gRPC server
	lis, err := net.Listen("tcp", ":50050")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()

	if *isLeader {
		glog.Info("Running as [LEADER].")
		pubsubproto.RegisterPubSubServiceServer(s, &server{}) // Leader doesn't need a client
		go startPublisherListener()

		// Start the leader's bulk writer workers
		for i := 0; i < *numWorkers; i++ {
			wg.Add(1)
			go runBulkWriterAsLeader(i)
		}

		// Also start the follower logic on the leader
		glog.Info("Leader is also running as a follower.")
		wg.Add(1)
		go func() {
			defer wg.Done()
			runBulkWriterAsFollower()
		}()
	} else {
		glog.Info("Running as [FOLLOWER].")
		conn, err := grpc.Dial(*leaderURL, grpc.WithInsecure())
		if err != nil {
			log.Fatalf("[FOLLOWER] Failed to connect to leader: %v", err)
		}
		defer conn.Close()

		client := pubsubproto.NewPubSubServiceClient(conn)
		pubsubproto.RegisterPubSubServiceServer(s, &server{client: client}) // Pass the client to the server
		go startPublisherListener()
		wg.Add(1) // Add the follower to the WaitGroup
		go func() {
			defer wg.Done()
			runBulkWriterAsFollower()
		}()
	}

	glog.Infof("(MAIN) gRPC server is running on :50050\n")
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	wg.Wait()
}

package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/broadcast"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/flowerinthenight/hedge/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

var port = flag.String("port", ":50051", "Main gRPC server port")

func main() {
	flag.Parse()
	go serveHealthChecks() // handle health checks from our LB

	spannerClient, err := spanner.NewClient(context.Background(), "projects/labs-169405/instances/alphaus-dev/databases/main")
	if err != nil {
		log.Fatalf("failed to create Spanner client: %v", err)
		return
	}

	  // Initialize storage with message tracking capabilities
	  storageInstance := storage.NewStorage()
  

	 // Initialize app with all necessary components for distributed message handling
	 app := &app.PubSub{
        Client:        spannerClient,
        Storage:       storageInstance,
        MessageLocks:  sync.Map{},    // For distributed message locking
        MessageQueue:  sync.Map{},     // For message tracking
        Mutex:         sync.Mutex{},   // For concurrency control
        TimeoutTimers: sync.Map{},     // Add this for tracking message timeouts across nodes
    }

	log.Println("[STORAGE]: Storage initialized")

	// Configure timeout settings
    const (
        defaultVisibilityTimeout = 30 * time.Second
        timeoutCheckInterval    = 5 * time.Second
    )

	op := hedge.New(
		spannerClient,
		":50052", // addr will be resolved internally
		"locktable",
		"pubsublock",
		"logtable",
		hedge.WithLeaderHandler( // if leader only, handles Send()
			app,
			send,
		),
		hedge.WithBroadcastHandler( // handles Broadcast()
			app,
			broadcast.Broadcast,
		),
	)

	app.Op = op
	app.NodeID = op.ID() // Important for tracking which node handles which message

	ctx, cancel := context.WithCancel(context.Background())

	// Start timeout monitor for all nodes
    go monitorMessageTimeouts(ctx, app, defaultVisibilityTimeout)

    // Start subscription validator
    go validateSubscriptions(ctx, app)

	go func() {
		if err := run(ctx, &server{PubSub: app}); err != nil {
			log.Fatalf("failed to run: %v", err)
		}
	}()

	done := make(chan error, 1) // optional wait
	go op.Run(ctx, done)

	// Start our fetching and broadcast routine for topic-subscription structure.
	go broadcast.StartDistributor(ctx, op, spannerClient)
	// Start our fetching and broadcast routine for unprocessed messages.
	go broadcast.FetchAndBroadcastUnprocessedMessage(ctx, op, spannerClient)

	sigCh := make(chan os.Signal, 1)

	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	cancel()
	<-done
}

func run(ctx context.Context, serverconf *server) error {
	lis, err := net.Listen("tcp", *port)
	if err != nil {
		log.Printf("failed to listen: %v", err)
		return err
	}

	s := grpc.NewServer()
	reflection.Register(s)
	pb.RegisterPubSubServiceServer(s, serverconf)
	log.Printf("gRPC Server listening on :50051")

	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()

	return s.Serve(lis)
}

func serveHealthChecks() {
	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatalf("failed to accept: %v", err)
		}
		conn.Close()
	}
}

// Monitor timeouts across all nodes
func monitorMessageTimeouts(ctx context.Context, app *app.PubSub, defaultTimeout time.Duration) {
    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            app.MessageLocks.Range(func(key, value interface{}) bool {
                messageID := key.(string)
                lockInfo := value.(broadcast.MessageLockInfo)

                // Check if message lock has expired
                if time.Now().After(lockInfo.Timeout) {
                    // Broadcast unlock message to all nodes
                    broadcastData := broadcast.BroadCastInput{
                        Type: broadcast.msgEvent,
                        Msg:  []byte(fmt.Sprintf("unlock:%s:%s", messageID, app.NodeID)),
                    }
                    bin, _ := json.Marshal(broadcastData)
                    app.Op.Broadcast(ctx, bin)

                    // Clean up local state
                    app.MessageLocks.Delete(messageID)
                    if timer, ok := app.TimeoutTimers.Load(messageID); ok {
                        timer.(*time.Timer).Stop()
                        app.TimeoutTimers.Delete(messageID)
                    }
                }
                return true
            })
        }
    }
}

// Validate subscriptions periodically
func validateSubscriptions(ctx context.Context, app *app.PubSub) {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            // Request latest subscription data from leader
            broadcastData := broadcast.BroadCastInput{
                Type: broadcast.topicsub,
                Msg:  []byte("refresh"),
            }
            bin, _ := json.Marshal(broadcastData)
            if _, err := app.Op.Request(ctx, bin); err != nil {
                log.Printf("Error refreshing subscriptions: %v", err)
            }
        }
    }
}
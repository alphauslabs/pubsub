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

	app := &app.PubSub{
		Client:  spannerClient,
		Storage: storage.NewStorage(),
	}

	log.Println("[STORAGE]: Storage initialized")

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
	ctx, cancel := context.WithCancel(context.Background())
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

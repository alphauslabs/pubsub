package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/broadcast"
	"github.com/alphauslabs/pubsub/send"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/alphauslabs/pubsub/utils"
	"github.com/flowerinthenight/hedge"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

var port = flag.String("port", ":50051", "Main gRPC server port")

func main() {
	flag.Parse()
	go serveHealthChecks() // _handle health checks from our LB

	spannerClient, err := spanner.NewClient(context.Background(), "projects/labs-169405/instances/alphaus-dev/databases/main")
	if err != nil {
		log.Fatalf("failed to create Spanner client: %v", err)
		return
	}
	defer spannerClient.Close()

	app := &app.PubSub{
		Client:        spannerClient,
		Storage:       storage.NewStorage(),
		ConsensusMode: "majority",
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
			send.Send,
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

	// Wait for leader availability
	func() {
		var m string
		defer func(l *string, t time.Time) {
			log.Printf("%v: %v", *l, time.Since(t))
		}(&m, time.Now())
		log.Println("Waiting for leader to be active...")
		ok, err := utils.EnsureLeaderActive(op, ctx)
		switch {
		case !ok:
			m = fmt.Sprintf("failed: %v, no leader after ", err)
		default:
			m = "leader active after "
		}
	}()

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

	kaep := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}

	kasp := keepalive.ServerParameters{
		MaxConnectionIdle:     0,                // Never close the connection due to inactivity
		MaxConnectionAge:      0,                // Never close the connection regardless of duration
		MaxConnectionAgeGrace: 0,                // No grace period needed since no forced closure
		Time:                  30 * time.Second, // Send keepalive ping per interval to maintain NAT/firewall state
		Timeout:               20 * time.Second, // Wait 20 seconds for a ping response before considering the connection dead
	}
	s := grpc.NewServer(
		grpc.KeepaliveEnforcementPolicy(kaep),
		grpc.KeepaliveParams(kasp),
	)
	reflection.Register(s)
	pb.RegisterPubSubServiceServer(s, serverconf)
	log.Printf("gRPC Server listening on %s", *port)

	go func() {
		<-ctx.Done()
		s.GracefulStop()
	}()

	return s.Serve(lis)
}

func serveHealthChecks() {
	l, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("failed to listen for health checks: %v", err)
	}

	for {
		conn, err := l.Accept()
		if err != nil {
			log.Fatalf("failed to accept health check connection: %v", err)
		}
		conn.Close()
	}
}

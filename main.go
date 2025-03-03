package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/handlers"
	"github.com/alphauslabs/pubsub/leader"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/alphauslabs/pubsub/sweep"
	"github.com/alphauslabs/pubsub/utils"
	"github.com/flowerinthenight/hedge"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

var (
	port = flag.String("port", ":50051", "Main gRPC server port")
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			glog.Infof("[Panic] Recovered in main: %v", r)
		}
	}()
	flag.Set("logtostderr", "true")
	flag.Parse()
	defer glog.Flush()

	go serveHealthChecks() // _handle health checks from our LB

	spconf := spanner.ClientConfig{
		SessionPoolConfig: spanner.SessionPoolConfig{
			TrackSessionHandles: true,
			InactiveTransactionRemovalOptions: spanner.InactiveTransactionRemovalOptions{
				ActionOnInactiveTransaction: spanner.WarnAndClose,
			},
		},
		Logger: log.New(os.Stdout, "spanner-client: ", log.Lshortfile),
	}
	spannerClient, err := spanner.NewClientWithConfig(context.Background(), "projects/labs-169405/instances/alphaus-dev/databases/main", spconf)
	if err != nil {
		log.Fatalf("failed to create Spanner client: %v", err)
		return
	}
	defer spannerClient.Close()
	app := &app.PubSub{
		Client:        spannerClient,
		ConsensusMode: "all",
	}

	go storage.MonitorActivity()

	op := hedge.New(
		spannerClient,
		":50052", // addr will be resolved internally
		"locktable",
		"pubsublock",
		"logtable",
		hedge.WithGroupSyncInterval(2*time.Second),
		hedge.WithLeaderCallback(2, func(data interface{}, msg []byte) {
			glog.Infof("Leader callback: %v", string(msg))
			s := strings.Split(string(msg), "")
			v, err := strconv.Atoi(s[0])
			if err != nil {
				log.Fatalf("failed to convert string to int: %v", err)
			}
			atomic.StoreInt32(&leader.IsLeader, int32(v))
		}),
		hedge.WithLeaderHandler( // if leader only, handles Send()
			app,
			handlers.Send,
		),
		hedge.WithBroadcastHandler( // handles Broadcast()
			app,
			handlers.Broadcast,
		),
		hedge.WithLogger(log.New(io.Discard, "", 0)), // silence
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
			glog.Infof("%v: %v", *l, time.Since(t))
		}(&m, time.Now())
		glog.Info("Waiting for leader to be active...")
		ok, err := utils.EnsureLeaderActive(op, ctx)
		switch {
		case !ok:
			m = fmt.Sprintf("failed: %v, no leader after ", err)
		default:
			m = "leader active after "
		}
	}()
	// Start our sweeper goroutine to check if message is expired, if so, then it unlocks it.
	go sweep.RunCheckForExpired()
	// Start our sweeper goroutine to check if message is deleted, if so, then it deletes it.
	go sweep.RunCheckForDeleted()
	// Start our fetching and broadcast routine for topic-subscription structure.
	go handlers.StartDistributor(ctx, op, spannerClient)
	// Start our fetching and broadcast routine for unprocessed messages.
	go handlers.FetchAndBroadcastUnprocessedMessage(ctx, op, spannerClient)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	cancel()
	<-done
}

func run(ctx context.Context, serverconf *server) error {
	lis, err := net.Listen("tcp", *port)
	if err != nil {
		glog.Infof("failed to listen: %v", err)
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
	glog.Infof("gRPC Server listening on %s", *port)

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

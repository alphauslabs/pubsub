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
	"runtime/debug"
	"syscall"
	"time"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/auth"
	"github.com/alphauslabs/pubsub/handlers"
	"github.com/alphauslabs/pubsub/leader"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/alphauslabs/pubsub/sweep"
	"github.com/alphauslabs/pubsub/utils"
	"github.com/flowerinthenight/hedge/v2"
	"github.com/flowerinthenight/timedoff"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"
)

var (
	port = flag.String("port", ":50051", "Main gRPC server port")
	env  = flag.String("env", "dev", "Environment: dev, prod")
)

func main() {
	client, err := createLoggingClient(*env)
	if err != nil {
		glog.Fatalf("Failed to create Google Cloud Logging client: %v", err)
	}
	defer client.Close()

	logger := client.Logger("pubsub-crash-")
	defer func() {
		if r := recover(); r != nil {
			logger.Log(logging.Entry{
				Severity: logging.Critical, // Ensure it's an error-level log
				Payload: map[string]interface{}{
					"message": fmt.Sprintf("Recovered from panic: %v", r),
				},
			})
		}
	}()
	flag.Set("logtostderr", "true")
	flag.Parse()
	defer glog.Flush()

	go serveHealthChecks() // _handle health checks from our LB

	spannerClient, err := createSpannerClient(*env)
	if err != nil {
		glog.Fatalf("Failed to create Spanner client: %v", err)
	}

	defer spannerClient.Close()
	ap := &app.PubSub{
		Client: spannerClient,
		LeaderActive: timedoff.New(60*time.Second, &timedoff.CallbackT{
			Callback: func(i any) {
				glog.Errorf("Warning! No leader active after 1 minute")
			},
		}),
	}

	op := hedge.New(
		spannerClient,
		":50052", // addr will be resolved internally
		"pubsub_lock",
		"pubsublock",
		"pubsub_log",
		hedge.WithDuration(5000),
		hedge.WithGroupSyncInterval(2*time.Second),
		hedge.WithLeaderCallback(
			ap,
			leader.LeaderCallBack,
		),
		hedge.WithLeaderHandler(
			ap,
			handlers.Send,
		),
		hedge.WithBroadcastHandler(
			ap,
			handlers.Broadcast,
		),
		hedge.WithLogger(log.New(io.Discard, "", 0)), // silence
	)

	ap.Op = op
	ctx, cancel := context.WithCancel(context.Background())
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

	go func() {
		defer func() {
			if r := recover(); r != nil {
				logger.Log(logging.Entry{
					Severity: logging.Critical, // Ensure it's an error-level log
					Payload: map[string]any{
						"message":    fmt.Sprintf("Recovered from panic: %v", r),
						"stacktrace": string(debug.Stack()), // Ensure stack trace is included
					},
				})
			}
		}()
		if err := run(ctx, &server{PubSub: ap}); err != nil {
			log.Fatalf("failed to run: %v", err)
		}
	}()

	go storage.MonitorActivity(ctx)
	// Start our sweeper goroutine to check if message is expired, if so, then it unlocks it.
	go sweep.RunCheckForExpired(ctx)
	// Start our sweeper goroutine to check if message is deleted, if so, then it deletes it.
	go sweep.RunCheckForDeleted(ctx, ap)
	// Start our fetching and broadcast routine for topic-subscription structure.
	go handlers.StartBroadcastTopicSub(ctx, ap)

	time.Sleep(2 * time.Second) // to be improve later
	// Start our fetching and broadcast routine for unprocessed messages.
	go handlers.StartBroadcastMessages(ctx, ap)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	cancel()
	<-done
}

func run(ctx context.Context, serverconf *server) error {
	lis, err := net.Listen("tcp", *port)
	if err != nil {
		glog.Errorf("failed to listen: %v", err)
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

	var opts []grpc.ServerOption
	opts = append(opts, grpc.KeepaliveEnforcementPolicy(kaep))
	opts = append(opts, grpc.KeepaliveParams(kasp))
	opts = append(opts, grpc.ChainUnaryInterceptor(
		grpc.UnaryServerInterceptor(auth.UnaryInterceptor),
	))
	opts = append(opts, grpc.ChainStreamInterceptor(
		grpc.StreamServerInterceptor(auth.StreamInterceptor),
	))
	s := grpc.NewServer(
		opts...,
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
		glog.Info("Health check connection accepted, ", conn.RemoteAddr())
		conn.Close()
	}
}

func createSpannerClient(env string) (*spanner.Client, error) {
	if env == "dev" {
		return spanner.NewClientWithConfig(context.Background(), "projects/labs-169405/instances/alphaus-dev/databases/main", spanner.ClientConfig{
			SessionPoolConfig: spanner.SessionPoolConfig{
				TrackSessionHandles: true,
				InactiveTransactionRemovalOptions: spanner.InactiveTransactionRemovalOptions{
					ActionOnInactiveTransaction: spanner.WarnAndClose,
				},
			},
			Logger: log.New(os.Stdout, "spanner-client: ", log.Lshortfile),
		})
	} else if env == "prod" {
		return spanner.NewClientWithConfig(context.Background(), "projects/mobingi-main/instances/alphaus-prod/databases/main", spanner.ClientConfig{
			SessionPoolConfig: spanner.SessionPoolConfig{
				TrackSessionHandles: true,
				InactiveTransactionRemovalOptions: spanner.InactiveTransactionRemovalOptions{
					ActionOnInactiveTransaction: spanner.WarnAndClose,
				},
			},
			Logger: log.New(os.Stdout, "spanner-client: ", log.Lshortfile),
		})
	}
	return nil, fmt.Errorf("invalid environment: %s", env)
}

func createLoggingClient(env string) (*logging.Client, error) {
	if env == "dev" {
		return logging.NewClient(context.Background(), "labs-169405")
	} else if env == "prod" {
		return logging.NewClient(context.Background(), "mobingi-main")
	}

	return nil, fmt.Errorf("invalid environment: %s", env)
}

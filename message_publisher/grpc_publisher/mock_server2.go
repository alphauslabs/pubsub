package main

// type server struct {
// 	pubsubproto.UnimplementedPubSubServiceServer
// 	messagePool sync.Pool
// }

// func newServer() *server {
// 	return &server{
// 		messagePool: sync.Pool{
// 			New: func() interface{} {
// 				return fmt.Sprintf("%d", rand.Int63())
// 			},
// 		},
// 	}
// }

// func (s *server) Publish(ctx context.Context, msg *pubsubproto.Message) (*pubsubproto.PublishResponse, error) {

// 	fmt.Printf("Received message on server 2: %s\n", msg.Payload)

// 	messageID := s.messagePool.Get().(string)
// 	defer s.messagePool.Put(messageID)

// 	if rand.Intn(100) < 1 {
// 		time.Sleep(time.Millisecond * 10)
// 	}

// 	return &pubsubproto.PublishResponse{
// 		MessageId: "02",
// 	}, nil
// }

// func startMockServer() {
// 	lis, err := net.Listen("tcp", ":8081")
// 	if err != nil {
// 		log.Fatalf("Failed to listen on port 8081: %v", err)
// 	}

// 	grpcServer := grpc.NewServer(
// 		grpc.KeepaliveParams(keepalive.ServerParameters{
// 			MaxConnectionIdle:     10 * time.Minute,
// 			Timeout:               20 * time.Second,
// 			MaxConnectionAgeGrace: 5 * time.Minute,
// 		}),
// 	)

// 	pubsubproto.RegisterPubSubServiceServer(grpcServer, newServer())

// 	log.Println("Mock server started on port 8081")
// 	if err := grpcServer.Serve(lis); err != nil {
// 		log.Fatalf("Failed to serve: %v", err)
// 	}
// }

// func main() {
// 	rand.Seed(time.Now().UnixNano())
// 	startMockServer()
// }

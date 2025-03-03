// gRPC Server: Initializes the Spanner client, registers your service, and listens for connections.
package main

// func main() {
// 	ctx := context.Background()

// 	// Initialize Spanner client.
// 	client, err := spanner.NewClient(ctx, "projects/labs-169405/instances/alphaus-dev/databases/main")
// 	if err != nil {
// 		log.Fatalf("Failed to create Spanner client: %v", err)
// 	}
// 	defer client.Close()

// 	// Create gRPC server.
// 	lis, err := net.Listen("tcp", ":50051")
// 	if err != nil {
// 		log.Fatalf("Failed to listen: %v", err)
// 	}

// 	grpcServer := grpc.NewServer()
// 	pb.RegisterTopicServiceServer(grpcServer, server.NewTopicServer(client))

// 	glog.Infof("Server listening at %v", lis.Addr())
// 	if err := grpcServer.Serve(lis); err != nil {
// 		log.Fatalf("Failed to serve: %v", err)
// 	}
// }

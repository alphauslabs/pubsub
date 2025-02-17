// server.go
package main

import (
    "cloud.google.com/go/spanner"
    "context"
    "fmt"
    "log"
    "net"
    "time"

    "github.com/google/uuid"
    "github.com/alphauslabs/pubsub/proto"  // Adjust to your actual module path
    "google.golang.org/grpc"
    "google.golang.org/grpc/codes"
    "google.golang.org/grpc/status"
    "google.golang.org/api/iterator"
    "google.golang.org/protobuf/types/known/emptypb"
    "google.golang.org/protobuf/types/known/timestamppb"
)

// TopicServer implements the proto.TopicServiceServer interface.
type TopicServer struct {
    proto.UnimplementedTopicServiceServer
    spannerClient *spanner.Client
}

// StartServer initializes the Spanner client, sets up the gRPC server, and listens for requests.
func StartServer() error {
    ctx := context.Background()

    // 1. Create Spanner client. Adjust connection string to match your GCP project/instance/db.
    client, err := spanner.NewClient(ctx, "projects/YOUR_PROJECT_ID/instances/YOUR_INSTANCE/databases/YOUR_DB")
    if err != nil {
        return fmt.Errorf("failed to create Spanner client: %w", err)
    }

    // We’ll close the client when the server shuts down. In a real app, you might handle this differently.
    defer client.Close()

    // 2. Create gRPC server
    lis, err := net.Listen("tcp", ":50051")
    if err != nil {
        return fmt.Errorf("failed to listen on port 50051: %w", err)
    }

    grpcServer := grpc.NewServer()

    // 3. Register our TopicServer
    topicServer := &TopicServer{spannerClient: client}
    proto.RegisterTopicServiceServer(grpcServer, topicServer)

    log.Printf("Server listening on :50051")
    // 4. Start serving
    return grpcServer.Serve(lis)
}

// --------------------
// CRUD Methods
// --------------------

// CreateTopic inserts a new topic into Spanner.
func (s *TopicServer) CreateTopic(ctx context.Context, req *proto.CreateTopicRequest) (*proto.Topic, error) {
    if req.GetName() == "" {
        return nil, status.Error(codes.InvalidArgument, "topic name is required")
    }

    topicID := uuid.New().String()
    now := time.Now()

    mutation := spanner.Insert("Topics",
        []string{"TopicID", "Name", "CreatedAt", "UpdatedAt"},
        []interface{}{topicID, req.GetName(), now, now},
    )
    _, err := s.spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
    if err != nil {
        return nil, status.Errorf(codes.Internal, "failed to create topic: %v", err)
    }

    return &proto.Topic{
        TopicId:   topicID,
        Name:      req.GetName(),
        CreatedAt: timestamppb.New(now),
        UpdatedAt: timestamppb.New(now),
    }, nil
}

// GetTopic fetches a topic by its ID.
func (s *TopicServer) GetTopic(ctx context.Context, req *proto.GetTopicRequest) (*proto.Topic, error) {
    row, err := s.spannerClient.Single().ReadRow(ctx, "Topics",
        spanner.Key{req.GetTopicId()},
        []string{"TopicID", "Name", "CreatedAt", "UpdatedAt"},
    )
    if err == spanner.ErrNoRows {
        return nil, status.Error(codes.NotFound, "topic not found")
    }
    if err != nil {
        return nil, status.Errorf(codes.Internal, "failed to read topic: %v", err)
    }

    var t proto.Topic
    var createdAt, updatedAt time.Time
    if err := row.Columns(&t.TopicId, &t.Name, &createdAt, &updatedAt); err != nil {
        return nil, status.Errorf(codes.Internal, "failed to scan topic row: %v", err)
    }

    t.CreatedAt = timestamppb.New(createdAt)
    t.UpdatedAt = timestamppb.New(updatedAt)
    return &t, nil
}

// ListTopics retrieves all topics (basic version, no pagination).
func (s *TopicServer) ListTopics(ctx context.Context, req *proto.ListTopicsRequest) (*proto.ListTopicsResponse, error) {
    stmt := spanner.Statement{
        SQL: `SELECT TopicID, Name, CreatedAt, UpdatedAt FROM Topics`,
    }
    iter := s.spannerClient.Single().Query(ctx, stmt)
    defer iter.Stop()

    var topics []*proto.Topic
    for {
        row, err := iter.Next()
        if err == iterator.Done {
            break
        }
        if err != nil {
            return nil, status.Errorf(codes.Internal, "failed to query topics: %v", err)
        }

        var t proto.Topic
        var createdAt, updatedAt time.Time
        if err := row.Columns(&t.TopicId, &t.Name, &createdAt, &updatedAt); err != nil {
            return nil, status.Errorf(codes.Internal, "failed to scan row: %v", err)
        }

        t.CreatedAt = timestamppb.New(createdAt)
        t.UpdatedAt = timestamppb.New(updatedAt)
        topics = append(topics, &t)
    }

    return &proto.ListTopicsResponse{
        Topics: topics,
        // next_page_token is empty for this basic example
    }, nil
}

// UpdateTopic updates an existing topic's name.
func (s *TopicServer) UpdateTopic(ctx context.Context, req *proto.UpdateTopicRequest) (*proto.Topic, error) {
    if req.GetTopicId() == "" {
        return nil, status.Error(codes.InvalidArgument, "topic ID is required")
    }
    if req.GetName() == "" {
        return nil, status.Error(codes.InvalidArgument, "new topic name is required")
    }

    now := time.Now()
    mutation := spanner.Update("Topics",
        []string{"TopicID", "Name", "UpdatedAt"},
        []interface{}{req.GetTopicId(), req.GetName(), now},
    )
    _, err := s.spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
    if err != nil {
        return nil, status.Errorf(codes.Internal, "failed to update topic: %v", err)
    }

    // Return the updated topic by reusing GetTopic
    return s.GetTopic(ctx, &proto.GetTopicRequest{TopicId: req.GetTopicId()})
}

// DeleteTopic removes a topic by ID.
func (s *TopicServer) DeleteTopic(ctx context.Context, req *proto.DeleteTopicRequest) (*emptypb.Empty, error) {
    if req.GetTopicId() == "" {
        return nil, status.Error(codes.InvalidArgument, "topic ID is required")
    }

    mutation := spanner.Delete("Topics", spanner.Key{req.GetTopicId()})
    _, err := s.spannerClient.Apply(ctx, []*spanner.Mutation{mutation})
    if err != nil {
        return nil, status.Errorf(codes.Internal, "failed to delete topic: %v", err)
    }

    return &emptypb.Empty{}, nil
}

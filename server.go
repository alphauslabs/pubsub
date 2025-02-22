package main

import (
	"context"
	"encoding/json"
	"log"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/flowerinthenight/hedge/v2"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	client *spanner.Client
	op     *hedge.Op
	pb.UnimplementedPubSubServiceServer

	visibilityTimeouts sync.Map // messageID -> VisibilityInfo
    lockMu            sync.RWMutex

    messageQueue   map[string][]*pb.Message // topic -> messages
    messageQueueMu sync.RWMutex
}

type broadCastInput struct {
    Type string      `json:"type"`
    Msg  interface{} `json:"msg"`
}

type VisibilityInfo struct {
    MessageID    string    `json:"messageId"`
    SubscriberID string    `json:"subscriberId"`
    ExpiresAt    time.Time `json:"expiresAt"`
    NodeID       string    `json:"nodeId"`
}

const (
	MessagesTable = "Messages"
	visibilityTimeout = 5 * time.Minute
    cleanupInterval   = 30 * time.Second
)

func NewServer(client *spanner.Client, op *hedge.Op) *server {
    s := &server{
        client:       client,
        op:           op,
        messageQueue: make(map[string][]*pb.Message),
    }
    go s.startVisibilityCleanup()
    return s
}

func (s *server) Publish(ctx context.Context, in *pb.PublishRequest) (*pb.PublishResponse, error) {
	if in.Topic == "" {
		return nil, status.Error(codes.InvalidArgument, "topic must not be empty")
	}

	b, _ := json.Marshal(in)
	l, _ := s.op.HasLock()
	if l {
		log.Println("[Publish-leader] Received message:\n", string(b))
	} else {
		log.Printf("[Publish] Received message:\n%v", string(b))
	}

	messageID := uuid.New().String()
	mutation := spanner.InsertOrUpdate(
    MessagesTable,
    []string{"id", "topic", "payload", "createdAt", "updatedAt", "visibilityTimeout", "processed"},
    []interface{}{
        messageID,
        in.Topic,
        in.Payload,
        spanner.CommitTimestamp,
        spanner.CommitTimestamp,
        nil,  // Explicitly set visibilityTimeout as NULL
        false, // Default to unprocessed
    },
)

	_, err := s.client.Apply(ctx, []*spanner.Mutation{mutation})
	if err != nil {
		log.Printf("Error writing to Spanner: %v", err)
		return nil, err
	}

	// broadcast message
	bcastin := broadCastInput{
		Type: message,
		Msg:  b,
	}

	bin, _ := json.Marshal(bcastin)
	out := s.op.Broadcast(ctx, bin)
	for _, v := range out {
		if v.Error != nil { // for us to know, then do necessary actions if frequent
			log.Printf("[Publish] Error broadcasting message: %v", v.Error)
		}
	}

	log.Printf("[Publish] Message successfully broadcasted and wrote to spanner with ID: %s", messageID)
	return &pb.PublishResponse{MessageId: messageID}, nil
}

func (s *server) Subscribe(req *pb.SubscribeRequest, stream pb.PubSubService_SubscribeServer) error {
    subscriberID := uuid.New().String()
    ctx := stream.Context()

    log.Printf("[Subscribe] New subscriber: %s for topic: %s", subscriberID, req.Topic)
    go s.keepAliveSubscriber(ctx, stream)

    for {
        select {
        case <-ctx.Done():
            s.cleanupSubscriberLocks(subscriberID)
            return nil
        default:
            s.messageQueueMu.RLock()
            msgs, exists := s.messageQueue[req.Topic]
            s.messageQueueMu.RUnlock()

            if !exists || len(msgs) == 0 {
                time.Sleep(100 * time.Millisecond)
                continue
            }
            // Check visibility timeout before sending
            info, exists := s.visibilityTimeouts.Load(msg.Id)
            if exists && time.Now().Before(info.(VisibilityInfo).ExpiresAt) {
              continue // Skip locked messages
            }
  
            s.messageQueueMu.Lock()
            msg := msgs[0]
            s.messageQueue[req.Topic] = msgs[1:]
            s.messageQueueMu.Unlock()

         
            locked, err := s.tryLockMessage(msg.Id, subscriberID)
            if err != nil || !locked {
                continue
            }

            if err := stream.Send(msg); err != nil {
                s.releaseMessageLock(msg.Id, subscriberID)
                return err
            }
        }
    }
}

func (s *server) tryLockMessage(messageID, subscriberID string) (bool, error) {
    s.lockMu.Lock()
    defer s.lockMu.Unlock()

    if _, exists := s.visibilityTimeouts.Load(messageID); exists {
        return false, nil
    }

    visInfo := VisibilityInfo{
        MessageID:    messageID,
        SubscriberID: subscriberID,
        ExpiresAt:    time.Now().Add(visibilityTimeout),
        NodeID:       uuid.New().String(),
    }

    s.visibilityTimeouts.Store(messageID, visInfo)
    return true, s.broadcastVisibilityUpdate("lock", visInfo)
}

func (s *server) Acknowledge(ctx context.Context, req *pb.AcknowledgeRequest) (*pb.AcknowledgeResponse, error) {
    s.messageQueueMu.Lock()
    defer s.messageQueueMu.Unlock()

    if err := s.releaseMessageLock(req.Id, req.SubscriberId); err != nil {
        log.Printf("Error releasing message lock: %v", err)
    }

    mutation := spanner.Update(
        MessagesTable,
        []string{"id", "processed", "updatedAt"},
        []interface{}{req.Id, true, spanner.CommitTimestamp},
    )

    _, err := s.client.Apply(ctx, []*spanner.Mutation{mutation})
    if err != nil {
        return nil, err
    }

    s.messageQueue[req.Topic] = s.messageQueue[req.Topic][1:]

    bcastin := broadCastInput{
        Type: "ack",
        Msg: map[string]string{
            "messageId": req.Id,
            "topic":    req.Topic,
        },
    }
    if err := s.broadcastAck(bcastin); err != nil {
        log.Printf("Error broadcasting ack: %v", err)
    }

    return &pb.AcknowledgeResponse{Success: true}, nil
}

func (s *server) releaseMessageLock(messageID, subscriberID string) error {
    s.lockMu.Lock()
    defer s.lockMu.Unlock()

    if info, exists := s.visibilityTimeouts.Load(messageID); exists {
        visInfo := info.(VisibilityInfo)
        if visInfo.SubscriberID == subscriberID {
            s.visibilityTimeouts.Delete(messageID)
            return s.broadcastVisibilityUpdate("unlock", visInfo)
        }
    }
    return nil
}

func (s *server) ExtendVisibilityTimeout(ctx context.Context, req *pb.ExtendTimeoutRequest) (*pb.ExtendTimeoutResponse, error) {
    s.lockMu.Lock()
    defer s.lockMu.Unlock()

    info, exists := s.visibilityTimeouts.Load(req.MessageId)
    if !exists {
        return nil, status.Error(codes.NotFound, "Message lock not found")
    }

    visInfo := info.(VisibilityInfo)
    if visInfo.SubscriberID != req.SubscriberId {
        return nil, status.Error(codes.PermissionDenied, "Not allowed to extend timeout for this message")
    }

    newExpiry := time.Now().Add(time.Duration(req.ExtensionSeconds) * time.Second)
    visInfo.ExpiresAt = newExpiry
    s.visibilityTimeouts.Store(req.MessageId, visInfo)

    // Update Spanner to reflect the new timeout
   go func() {
    mutation := spanner.Update(
        MessagesTable,
        []string{"id", "visibilityTimeout", "updatedAt"},
        []interface{}{req.MessageId, newExpiry, spanner.CommitTimestamp},
    )
    _, err := s.client.Apply(ctx, []*spanner.Mutation{mutation})
    if err != nil {
        log.Printf("Spanner update error: %v", err)
    }
}()

    // Broadcast new timeout info
    _ = s.broadcastVisibilityUpdate("extend", visInfo)

    return &pb.ExtendTimeoutResponse{Success: true}, nil
}


func (s *server) broadcastVisibilityUpdate(cmdType string, info VisibilityInfo) error {
    bcastin := broadCastInput{
        Type: "visibility",
        Msg: struct {
            Command string        `json:"command"`
            Info    VisibilityInfo `json:"info"`
        }{
            Command: cmdType,
            Info:    info,
        },
    }

    data, err := json.Marshal(bcastin)
    if err != nil {
        return err
    }

    results := s.op.Broadcast(context.Background(), data)
    for _, result := range results {
        if result.Error != nil {
            log.Printf("Broadcast error to node %s: %v", result.NodeID, result.Error)
        }
    }

    return nil
}

func (s *server) startVisibilityCleanup() {
    ticker := time.NewTicker(cleanupInterval)
    for range ticker.C {
        s.cleanupExpiredLocks()
    }
}

func (s *server) cleanupExpiredLocks() {
    now := time.Now()
    s.lockMu.Lock()
    defer s.lockMu.Unlock()
    
    s.visibilityTimeouts.Range(func(key, value interface{}) bool {
        visInfo := value.(VisibilityInfo)
        if now.After(visInfo.ExpiresAt) {
            // Double-check before deleting
            if info, exists := s.visibilityTimeouts.Load(key); exists {
                if time.Now().Before(info.(VisibilityInfo).ExpiresAt) {
                    return true // Another node extended it
                }
            }
            s.visibilityTimeouts.Delete(key)
            s.broadcastVisibilityUpdate("unlock", visInfo)
        }
        return true
    })
}


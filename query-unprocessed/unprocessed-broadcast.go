package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/flowerinthenight/hedge/v2"
	"google.golang.org/api/iterator"
)

type QueuedMessage struct {
	Id      string `json:"id"`
	Topic   string `json:"topic"`
	Payload string `json:"payload"`
}

type MessageQueue struct {
	messages map[string]*QueuedMessage // changed slices to map with message ID as key
	mu       sync.RWMutex
}

func NewMessageQueue() *MessageQueue {
	return &MessageQueue{
		messages: make(map[string]*QueuedMessage),
	}
}

func ProcessUnprocessedMessages(ctx context.Context, op *hedge.Op, spannerClient *spanner.Client) {
	queue := NewMessageQueue()

	// used ticker instead of sleep
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l, _ := op.HasLock()
			if !l {
				log.Println("Not leader, skipping")
				continue
			}

			// Query unprocessed messages
			stmt := spanner.Statement{
				SQL: `SELECT id, topic, payload 
                      FROM Messages
                      WHERE processed = FALSE`,
			}

			iter := spannerClient.Single().Query(ctx, stmt)
			defer iter.Stop()

			for {
				row, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					log.Printf("Error reading message: %v", err)
					continue
				}

				var msg pb.Message
				if err := row.Columns(&msg.Id, &msg.Topic, &msg.Payload); err != nil {
					log.Printf("Error scanning message: %v", err)
					continue
				}

				// Store in map using message ID as key
				queue.mu.Lock()
				queue.messages[msg.Id] = &QueuedMessage{
					Id:      msg.Id,
					Topic:   msg.Topic,
					Payload: msg.Payload,
				}
				queue.mu.Unlock()

				messageInfo := struct {
					ID      string `json:"id"`
					Topic   string `json:"topic"`
					Payload string `json:"payload"`
				}{
					ID:      msg.Id,
					Topic:   msg.Topic,
					Payload: msg.Payload,
				}

				data, err := json.Marshal(messageInfo)
				if err != nil {
					log.Printf("Error marshalling message: %v", err)
					continue
				}

				// broadcast to all VMs including leader
				if err := op.Broadcast(ctx, data); err != nil {
					log.Printf("Error broadcasting message: %v", err)
					continue
				}

				log.Printf("Successfully broadcast message: %s", msg.Id)
			}
		}
	}
}

package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"

    "cloud.google.com/go/spanner"
    pb "github.com/alphauslabs/pubsub-proto/v1"
    "github.com/flowerinthenight/hedge/v2"
)

const (
    MaxQueueSize = 2000  // Queue size limit
)

// represent message stored in VM queue
type QueuedMessage struct {
    ID               string    `json:"id"`
    Topic            string    `json:"topic"`
    Payload          string    `json:"payload"`
    VisibilityTimeout time.Time `json:"visibilityTimeout"`
}

// hold messages for each VM
type MessageQueue struct {
    messages []*QueuedMessage
    maxSize  int
}

func main() {
    ctx := context.Background()

    // Initialize queue VM with size limit
    queue := &MessageQueue{
        messages: make([]*QueuedMessage, 0),
        maxSize:  MaxQueueSize,
    }

    // Initialize Spanner client
    spannerClient, err := spanner.NewClient(ctx, "projects/labs-169405/instances/alphaus-dev/databases/main")
    if err != nil {
        log.Fatalf("Failed to create Spanner client: %v", err)
    }
    defer spannerClient.Close()

    // Initialize hedge with broadcast handler
    op := hedge.New(
        spannerClient,
        ":50052",
        "locktable",
        "pubsublock",
        "logtable",
        hedge.WithBroadcastHandler(
            nil,
            func(data interface{}, msg []byte) ([]byte, error) {
                // Unmarshal received broadcast message
                var receivedMsg struct {
                    ID      string `json:"id"`
                    Topic   string `json:"topic"`
                    Payload string `json:"payload"`
                }
                if err := json.Unmarshal(msg, &receivedMsg); err != nil {
                    log.Printf("Error unmarshalling message: %v", err)
                    return nil, err
                }

                // Check if queue is full
                if len(queue.messages) >= queue.maxSize {
                    log.Printf("Queue is full (size: %d). Message %s not added", queue.maxSize, receivedMsg.ID)
                    return nil, fmt.Errorf("queue is full")
                }

                // Create queued message with visibility timeout
                queuedMsg := &QueuedMessage{
                    ID:               receivedMsg.ID,
                    Topic:           receivedMsg.Topic,
                    Payload:         receivedMsg.Payload,
                    VisibilityTimeout: time.Now().Add(time.Minute),
                }

                // Add to queue
                queue.messages = append(queue.messages, queuedMsg)
                log.Printf("Stored message %s in queue. Queue size: %d/%d", 
                    queuedMsg.ID, len(queue.messages), queue.maxSize)

                return []byte("stored"), nil
            },
        ),
    )

    // Start hedge
    done := make(chan error, 1)
    go op.Run(ctx, done)

    // Process messages every second
    for {
        // Check if leader
        l, _ := op.HasLock()
        if !l {
            log.Println("Not leader, skipping")
            time.Sleep(1 * time.Second) // Wait 1 second before next check
            continue
        }

        // Query unprocessed messages
        stmt := spanner.Statement{
            SQL: `SELECT id, topic, payload 
                  FROM Messages 
                  WHERE processed = FALSE`
        }

        iter := spannerClient.Single().Query(ctx, stmt)
        defer iter.Stop()

        // Process message
        for {
            row, err := iter.Next()
            if err == iterator.Done {
                break
            }
            if err != nil {
                log.Printf("Error reading message: %v", err)
                continue
            }

            // Get message details
            var msg pb.Message
            if err := row.Columns(&msg.Id, &msg.Topic, &msg.Payload); err != nil {
                log.Printf("Error scanning message: %v", err)
                continue
            }

            // Prepare message for broadcasting
            messageInfo := struct {
                ID      string `json:"id"`
                Topic   string `json:"topic"`
                Payload string `json:"payload"`
            }{
                ID:      msg.Id,
                Topic:   msg.Topic,
                Payload: msg.Payload,
            }

            // Marshal message for broadcast
            data, err := json.Marshal(messageInfo)
            if err != nil {
                log.Printf("Error marshalling message: %v", err)
                continue
            }

            // Broadcast message to all VMs
            if err := op.Broadcast(ctx, data); err != nil {
                log.Printf("Error broadcasting message: %v", err)
                continue
            }

            // Mark message as processed
            mutation := spanner.InsertOrUpdate(
                "Messages",
                []string{"id", "topic", "payload", "processed", "updatedAt", "visibilityTimeout"},
                []interface{}{
                    msg.Id,
                    msg.Topic,
                    msg.Payload,
                    true,
                    spanner.CommitTimestamp,
                    time.Now().Add(time.Minute),
                },
            )

            if _, err := spannerClient.Apply(ctx, []*spanner.Mutation{mutation}); err != nil {
                log.Printf("Error marking message as processed: %v", err)
                continue
            }

            log.Printf("Successfully processed message: %s", msg.Id)
        }

        time.Sleep(1 * time.Second) // Wait 1 second before next query
    }
}

package broadcast

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/flowerinthenight/hedge/v2"
	"google.golang.org/api/iterator"
)

// Todo: Add status to messages table if a message is broadcasted, this is to prevent re-broadcasting
// and will lessen unnecessary network calls.
// Currently, all messages are broadcasted to all node every tick.
func FetchAndBroadcastUnprocessedMessage(ctx context.Context, op *hedge.Op, spannerClient *spanner.Client) {
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

				// define message structure
				messageInfo := struct {
					ID      string `json:"id"`
					Topic   string `json:"topic"`
					Payload string `json:"payload"`
				}{ // fill values
					ID:      msg.Id,
					Topic:   msg.Topic,
					Payload: msg.Payload,
				}

				// conv structured msg to JSON
				data, err := json.Marshal(messageInfo)
				if err != nil {
					log.Printf("Error marshalling message: %v", err)
					continue
				}

				//broadcast JSON msg to all Nodes
				resp := op.Broadcast(ctx, data)
				for _, r := range resp {
					if r.Error != nil {
						log.Printf("Error broadcasting message: %v", r.Error)
					}
				}

				log.Printf("Successfully broadcast message: %s", msg.Id)
			}
		}
	}
}

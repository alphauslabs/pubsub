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

func FetchAndBroadcastUnprocessedMessage(ctx context.Context, op *hedge.Op, spannerClient *spanner.Client) {
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

				// Structure the message
				messageInfo := struct {
					ID      string `json:"id"`
					Topic   string `json:"topic"`
					Payload string `json:"payload"`
				}{
					ID:      msg.Id,
					Topic:   msg.Topic,
					Payload: msg.Payload,
				}

				// Marshal message info
				data, err := json.Marshal(messageInfo)
				if err != nil {
					log.Printf("Error marshalling message: %v", err)
					continue
				}

				// Create broadcast input
				broadcastInput := BroadCastInput{
					Type: message, // Using const from same package
					Msg:  data,
				}

				// Marshal broadcast input
				broadcastData, err := json.Marshal(broadcastInput)
				if err != nil {
					log.Printf("Error marshalling broadcast input: %v", err)
					continue
				}

				// Broadcast
				if err := op.Broadcast(ctx, broadcastData); err != nil {
					log.Printf("Error broadcasting message: %v", err)
					continue
				}

				log.Printf("Successfully broadcast message: %s", msg.Id)
			}
		}
	}
}

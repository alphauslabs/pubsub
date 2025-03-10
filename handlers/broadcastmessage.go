package handlers

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/alphauslabs/pubsub/leader"
	"github.com/flowerinthenight/hedge"
	"github.com/golang/glog"
	"google.golang.org/api/iterator"
)

func FetchAndBroadcastUnprocessedMessage(ctx context.Context, op *hedge.Op, spannerClient *spanner.Client) {
	isFirst := true
	ticker := time.NewTicker(2 * time.Second) // check every 2 seconds
	defer ticker.Stop()
	var lastQueryTime time.Time

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if atomic.LoadInt32(&leader.IsLeader) != 1 { // Check if leader
				continue
			}

			func() {
				var stmt spanner.Statement

				if isFirst { // 1st query, fetch all unprocessed
					stmt = spanner.Statement{
						SQL: `SELECT id, topic, payload 
							  FROM Messages
							  WHERE processed = FALSE`,
					}
					isFirst = false // fasle after 1st query
				} else {
					// next queries fetch new messages after lastQueryTime
					stmt = spanner.Statement{
						SQL: `SELECT id, topic, payload 
							  FROM Messages
							  WHERE processed = FALSE AND createdAt > @lastQueryTime`,
						Params: map[string]interface{}{"lastQueryTime": lastQueryTime},
					}
				}

				iter := spannerClient.Single().Query(ctx, stmt)
				defer iter.Stop()

				count := 0 // counter for unprocessed messages
				for {
					row, err := iter.Next()
					if err == iterator.Done {
						break
					}
					if err != nil {
						glog.Infof("[BroadcastMessage] Error reading message: %v", err)
						continue
					}
					count++

					var msg pb.Message
					if err := row.Columns(&msg.Id, &msg.Topic, &msg.Payload); err != nil {
						glog.Infof("[BroadcastMessage] Error reading message columns: %v", err)
						continue
					}

					// Structure
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
						glog.Infof("[BroadcastMessage] Error marshalling message: %v", err)
						continue
					}

					// Create broadcast input
					broadcastInput := BroadCastInput{
						Type: Message,
						Msg:  data,
					}

					// Marshal broadcast input
					broadcastData, err := json.Marshal(broadcastInput)
					if err != nil {
						glog.Infof("[BroadcastMessage] Error marshalling broadcast input: %v", err)
						continue
					}

					// Broadcast
					responses := op.Broadcast(ctx, broadcastData)
					for _, response := range responses {
						if response.Error != nil {
							glog.Infof("[BroadcastMessage] Error broadcasting message: %v", response.Error)
						}
					}
				}

				// Update lastQueryTime
				if count > 0 {
					lastQueryTime = time.Now().UTC()
				}

				if count == 0 {
					glog.Info("[BroadcastMessage] No new unprocessed messages found.")
				}
			}()
		}
	}
}

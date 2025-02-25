package broadcast

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge/v2"
)

// fetchAndBroadcast fetches updated topic-subscription data and broadcasts it if there are updates.
func fetchAndBroadcast(op *hedge.Op, client *spanner.Client, lastChecked *time.Time) {
	ctx := context.Background()
	stmt := spanner.Statement{
		SQL: `SELECT topic, ARRAY_AGG(name) AS subscriptions
              FROM Subscriptions
              WHERE updatedAt > @last_checked_time
              GROUP BY topic`,
		Params: map[string]interface{}{"last_checked_time": *lastChecked},
	}

	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	topicSub := make(map[string][]string)
	for {
		row, err := iter.Next()
		if err != nil {
			break
		}
		var topic string
		var subscriptions []string
		if err := row.Columns(&topic, &subscriptions); err != nil {
			log.Printf("Error reading row: %v", err)
			continue
		}
		topicSub[topic] = subscriptions
	}
	*lastChecked = time.Now()
	log.Println("[Leader] Fetched topic subscriptions:", topicSub)

	if len(topicSub) == 0 {
		log.Println("[Leader] No new updates, skipping broadcast.")
		return
	}

	data, err := json.Marshal(topicSub)
	if err != nil {
		log.Printf("Error marshalling topicSub: %v", err)
		return
	}

	for _, r := range op.Broadcast(ctx, data) {
		if r.Error != nil {
			log.Printf("Error broadcasting to %s: %v", r.Id, r.Error)
		}
	}
	log.Println("[Leader] Topic-sub structure broadcast completed.")
}

// StartDistributor initializes the distributor that periodically checks for updates.
func StartDistributor(op *hedge.Op, client *spanner.Client) {
	lastChecked := time.Now().Add(-10 * time.Second)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if hasLock, _ := op.HasLock(); hasLock {
			log.Println("Leader: Processing updates...")
			fetchAndBroadcast(op, client, &lastChecked)
		} else {
			log.Println("Follower: No action needed.")
		}
	}
}

/* leader broadcasts topic-subscription to all nodes (even if no changes/updates happened)
func broadcastTopicSubStruct(op *hedge.Op, topicSub map[string][]string) {
data, err := json.Marshal(topicSub)
if err != nil {
log.Printf("Error marshalling topic-subscription: %v", err)
return
}
op.Broadcast(context.Background(), data)
log.Println("Leader: Broadcasted topic-subscription structure to all nodes")
}
*/

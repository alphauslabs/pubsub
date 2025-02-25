package broadcaststruct

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge/v2"
	"google.golang.org/api/iterator"
)

// fetchAndBroadcast fetches updated topic-subscription data and broadcasts it if there are updates.
func fetchAndBroadcast(op *hedge.Op, client *spanner.Client, lastChecked *time.Time) {
	ctx := context.Background()
	queryTime := time.Now()

	/*
	   	stmt := spanner.Statement{
	   		SQL: `SELECT topic_id, ARRAY_AGG(subscription) AS subscriptions
	                 FROM Subscriptions
	                 WHERE updatedAt > @last_checked_time
	                 GROUP BY topic_id`,
	   		Params: map[string]interface{}{"last_checked_time": *lastChecked},
	   	}
	*/

	stmt := spanner.Statement{
		SQL: `SELECT t.name AS topic_name, ARRAY_AGG(s.subscriber) AS subscriptions
			FROM Subscriptions s
			JOIN Topics t ON s.topic_id = t.id
			WHERE s.updatedAt > @last_checked_time
			GROUP BY t.name`,
		Params: map[string]interface{}{
			"last_checked_time": *lastChecked,
		},
	}

	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	topicSub := make(map[string][]string)

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Printf("Error reading row: %v", err)
			continue
		}

		var topicName string
		var subscriptions []string
		if err := row.Columns(&topicName, &subscriptions); err != nil {
			log.Printf("Error parsing row: %v", err)
			continue
		}

		topicSub[topicName] = subscriptions
	}

	*lastChecked = queryTime
	log.Println("Leader: Fetched topic-subscriptions structure:", topicSub)

	if len(topicSub) == 0 {
		log.Println("Leader: No new updates, skipping broadcast.")
		return
	}

	// Convert the data to JSON for broadcasting
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
	log.Println("Leader: Broadcast completed.")
}

// StartDistributor initializes the distributor that periodically checks for updates.
func StartDistributor(op *hedge.Op, client *spanner.Client) {
	lastChecked := time.Now().Add(-10 * time.Second)
	ticker := time.NewTicker(10 * time.Second)
	go func() {
		defer ticker.Stop()
		for range ticker.C {
			if hasLock, _ := op.HasLock(); hasLock {
				log.Println("Leader: Processing updates...")
				fetchAndBroadcast(op, client, &lastChecked)
			} else {
				log.Println("Follower: Not retrieving topic-subscription structure.")
			}
		}
	}()
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

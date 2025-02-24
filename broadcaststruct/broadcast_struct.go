package broadcaststruct

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge/v2"
)

// query Spanner for only updated topics and subscriptions
func fetchUpdatedTopicSub(client *spanner.Client, lastCheckedTime *time.Time) map[string][]string {
	ctx := context.Background()

	stmt := spanner.Statement{
		SQL: `SELECT topic_id, ARRAY_AGG(subscription) AS subscriptions 
              FROM Subscriptions 
              WHERE updatedAt > @last_checked_time 
              GROUP BY topic_id`,
		Params: map[string]interface{}{
			"last_checked_time": *lastCheckedTime,
		},
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
			log.Printf("Error reading Spanner row (topic-subscription query): %v", err)
			continue
		}
		topicSub[topic] = subscriptions
	}

	// update last query time
	*lastCheckedTime = time.Now()
	log.Println("Leader: Queried latest topic-subscription structure:", topicSub)
	return topicSub
}

// leader broadcasts topic-subscription structure if an update happened
func broadcastTopicSubStruct(op *hedge.Op, topicSub map[string][]string) {
	if len(topicSub) == 0 {
		log.Println("Leader: No new updates, skipping broadcast.")
		return
	}

	data, err := json.Marshal(topicSub)
	if err != nil {
		log.Printf("Error marshalling topic-subscription: %v", err)
		return
	}

	// Broadcast the data
	resp := op.Broadcast(context.Background(), data)

	// Track errors
	hasError := false
	for _, r := range resp {
		if r.Error != nil {
			log.Printf("Error broadcasting to %s: %v", r.Id, r.Error)
			hasError = true
		}
	}

	// Log final status
	if hasError {
		log.Println("Leader: Broadcast completed with errors.")
	} else {
		log.Println("Leader: Broadcast successfully completed without errors.")
	}
}

// StartDistributor initializes and starts the topic-subscription distributor
func StartDistributor(op *hedge.Op, spannerClient *spanner.Client) {
	lastCheckedTime := time.Now().Add(-10 * time.Second) // Start 10 seconds earlier
	go distributeStruct(op, spannerClient, &lastCheckedTime)
}

// leader fetches and broadcasts topic-subs updates every 10 seconds
func distributeStruct(op *hedge.Op, spannerClient *spanner.Client, lastCheckedTime *time.Time) {
	ticker := time.NewTicker(10 * time.Second) // ticker for periodic execution
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			// check if this node is the leader
			l, _ := op.HasLock()
			if l {
				log.Println("Leader: Fetching and broadcasting topic-subscription updates...")
				topicSub := fetchUpdatedTopicSub(spannerClient, lastCheckedTime)
				broadcastTopicSubStruct(op, topicSub)
			} else {
				log.Println("Follower: No action needed.")
			}
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

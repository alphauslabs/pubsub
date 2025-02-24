package main

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

	//capture response and error from broadcast
	resp, err := op.Broadcast(context.Background(), data)
	if err != nil {
		log.Printf("Error in broadcast: %v", err)
	} else {
		log.Printf("Broadcast response: %s", string(resp))
	}

	log.Println("Leader: Broadcasted topic-subscription structure to all nodes")
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

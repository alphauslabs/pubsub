package main

import (
	"context"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge/v2"
)

var lastCheckedTime time.Time // track the last Spanner query time to fetch only new updates

func main() {
	ctx := context.Background()

	// spanner Client
	spannerClient, err := spanner.NewClient(ctx, "projects/labs-169405/instances/alphaus-dev/databases/main")
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
		return
	}
	defer spannerClient.Close()

	// leader election
	op := hedge.New(
		spannerClient,
		":50053",
		"locktable",
		"pubsublock",
		"logtable",
	)

	done := make(chan error, 1)
	go op.Run(ctx, done)

	// distributre topic-sub struct
	go distributeStruct(op, spannerClient)

	<-done
}

// leader fetches and broadcasts topic-subs updates every 10 seconds
func distributeStruct(op *hedge.Hedge, spannerClient *spanner.Client) {
	lastCheckedTime = time.Now().Add(-10 * time.Second) // Start 10 seconds earlier
	ticker := time.NewTicker(10 * time.Second)          // ticker for periodic execution
	defer ticker.Stop()

	for range ticker.C {
		// check if this node is the leader
		l, _ := op.HasLock()
		if l {
			log.Println("Leader: Fetching and broadcasting topic-subscription updates...")
			topicSub := fetchUpdatedTopicSub(spannerClient, &lastCheckedTime)
			broadcastTopicSubStruct(op, topicSub)
		} else {
			log.Println("Follower: No action needed.")
		}
	}
}

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
			log.Printf("Error reading row: %v", err)
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
func broadcastTopicSubStruct(op *hedge.Hedge, topicSub map[string][]string) {
	if len(topicSub) == 0 {
		log.Println("Leader: No new updates, skipping broadcast.")
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

// leader broadcasts topic-subscription to all nodes (even if no changes/updates happened)
//func broadcastTopicSubStruct(op *hedge.Hedge, topicSub map[string][]string) {
//data, err := json.Marshal(topicSub)
//if err != nil {
//log.Printf("Error marshalling topic-subscription: %v", err)
//return
//}
//op.Broadcast(context.Background(), data)
//log.Println("Leader: Broadcasted topic-subscription structure to all nodes")
//}

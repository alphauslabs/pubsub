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
func fetchAndBroadcast(op *hedge.Op, client *spanner.Client, lastChecked *time.Time, lastBroadcasted *map[string][]string) {
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
		if err == spanner.Done {
			break
		}
		if err != nil {
			log.Printf("Error iterating rows: %v", err)
			return
		}

		var topic string
		var subscriptions []string
		if err := row.Columns(&topic, &subscriptions); err != nil {
			log.Printf("Error reading row: %v", err)
			continue
		}

		// ensure subscriptions is not nil
		if subscriptions == nil {
			subscriptions = []string{}
		}

		topicSub[topic] = subscriptions
	}

		// if there are no new updates, log and return
	if len(topicSub) == 0 {
		log.Println("Leader: No new updates, skipping broadcast.")
		if len(*lastBroadcasted) > 0 {
			log.Println("Leader: Subscription topic structure is still:", *lastBroadcasted)
		} else {
			log.Println("Leader: No previous topic-subscription structure available.")
		}
		return
	}

	// compare topicSub with lastBroadcasted to check if they are exactly the same
	same := true
	for key, subs := range topicSub {
		if lastSubs, exists := (*lastBroadcasted)[key]; !exists || !equalStringSlices(subs, lastSubs) {
			same = false
			break
		}
	}

	if same {
		log.Println("Leader: No new updates, skipping broadcast.")
		log.Println("Leader: Subscription topic structure is still:", *lastBroadcasted)
		return
	}

	log.Println("Leader: Fetched topic subscriptions:", topicSub)


	// marshal topic subscription data
	msgData, err := json.Marshal(topicSub)
	if err != nil {
		log.Printf("Error marshalling topicSub: %v", err)
		return
	}

	broadcastMsg := BroadCastInput{
		Type: topicsub,
		Msg:  msgData,
	}

	// marshal BroadCastInput
	broadcastData, err := json.Marshal(broadcastMsg)
	if err != nil {
		log.Printf("Error marshalling BroadCastInput: %v", err)
		return
	}

	// broadcast message
	for _, r := range op.Broadcast(ctx, broadcastData) {
		if r.Error != nil {
			log.Printf("Error broadcasting to %s: %v", r.Id, r.Error)
		}
	}

	*lastChecked = time.Now()
	*lastBroadcasted = topicSub
	log.Println("Leader: Topic-subscription structure broadcast completed.")
}

/ StartDistributor initializes the distributor that periodically checks for updates.
func StartDistributor(op *hedge.Op, client *spanner.Client) {
	lastChecked := time.Now().Add(-10 * time.Second)
	lastBroadcasted := make(map[string][]string) // Stores the last known structure
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		if hasLock, _ := op.HasLock(); hasLock {
			log.Println("Leader: Processing updates...")
			fetchAndBroadcast(op, client, &lastChecked, &lastBroadcasted)
		} else {
			log.Println("Follower: No action needed.")
		}
	}
}

// helper function to compare two string slices (ignoring order)
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	exists := make(map[string]bool)
	for _, val := range a {
		exists[val] = true
	}
	for _, val := range b {
		if !exists[val] {
			return false
		}
	}
	return true
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

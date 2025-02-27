package handlers

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/flowerinthenight/hedge"
	"google.golang.org/api/iterator"
)

// Global variables to track last broadcast state
var (
	lastBroadcasted = make(map[string][]string)
)

func FetchAllTopicSubscriptions(ctx context.Context, client *spanner.Client) map[string][]string {
	stmt := spanner.Statement{
		SQL: `SELECT topic, ARRAY_AGG(name) AS subscriptions FROM Subscriptions WHERE name IS NOT NULL GROUP BY topic`,
	}

	log.Println("STRUCT-Leader: Running full topic-subscription query as lastBroadcasted is empty.")

	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	topicSub := make(map[string][]string)

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("STRUCT-Fatal error iterating Spanner rows: %v", err)
		}

		var topic string
		var subscriptions []string
		if err := row.Columns(&topic, &subscriptions); err != nil {
			log.Printf("STRUCT-Error reading row: %v", err)
			continue
		}

		subscriptions = append([]string{}, subscriptions...)
		topicSub[topic] = subscriptions
	}

	return topicSub
}

func FetchAndBroadcast(ctx context.Context, op *hedge.Op, client *spanner.Client, isStartup bool) {
	var latest map[string][]string
	if isStartup {
		requestTopicSubFetch(ctx, op)
		return
	}

	latest = FetchAllTopicSubscriptions(ctx, client)
	if AreTopicSubscriptionsEqual(latest, lastBroadcasted) {
		log.Println("STRUCT-Leader: No changes detected in topic-subscription structure.")
		return
	}

	// Marshal topic-subscription data
	msgData, err := json.Marshal(latest)
	if err != nil {
		log.Printf("STRUCT-Error marshalling topicSub: %v", err)
		return
	}

	broadcastMsg := BroadCastInput{
		Type: Topicsub,
		Msg:  msgData,
	}

	// Marshal BroadCastInput
	broadcastData, err := json.Marshal(broadcastMsg)
	if err != nil {
		log.Printf("STRUCT-Error marshalling BroadCastInput: %v", err)
		return
	}

	// Broadcast message
	for _, r := range op.Broadcast(ctx, broadcastData) {
		if r.Error != nil {
			log.Printf("STRUCT-Error broadcasting to %s: %v", r.Id, r.Error)
		} else {
			lastBroadcasted = latest
		}
	}

	log.Println("STRUCT-Debug: Updated lastBroadcasted")
	log.Println("STRUCT-Leader: Topic-subscription structure broadcast completed.")
}

// initializes the distributor that periodically checks for updates.
func StartDistributor(ctx context.Context, op *hedge.Op, client *spanner.Client) {
	ticker := time.NewTicker(10 * time.Second)
	defer func() {
		ticker.Stop()
		log.Println("STRUCT-Leader: Distributor ticker stopped.")
	}()

	// perform an initial broadcast of all topic-subscription structures
	log.Println("STRUCT: Running initial startup query and broadcasting structure.")
	FetchAndBroadcast(ctx, op, client, true) // run startup broadcast

	for {
		select {
		case <-ctx.Done():
			log.Println("STRUCT-Leader: Context canceled, stopping distributor.")
			return
		case <-ticker.C:
			hasLock, _ := op.HasLock()
			if hasLock {
				FetchAndBroadcast(ctx, op, client, false)
			}
		}
	}
}

// Immediate broadcast function to send topic-subscription updates instantly.
// func ImmediateBroadcast(ctx context.Context, op *hedge.Op, client *spanner.Client) {
// 	log.Println("STRUCT-Leader: Immediate broadcast triggered.")

// 	// Ensure this node is the leader before broadcasting
// 	hasLock, _ := op.HasLock()
// 	if !hasLock {
// 		log.Println("STRUCT-Leader: Skipping immediate broadcast because this node is not the leader.")
// 		return
// 	}

// 	// Fetch latest topic-subscription data
// 	newBroadcasted := FetchAllTopicSubscriptions(ctx, client)
// 	if len(newBroadcasted) == 0 {
// 		log.Println("STRUCT-Leader: No updated topic-subscription data found, skipping immediate broadcast.")
// 		return
// 	}

// 	// Update last broadcasted structure
// 	lastBroadcasted = newBroadcasted

// 	// Marshal topic-subscription data
// 	msgData, err := json.Marshal(lastBroadcasted)
// 	if err != nil {
// 		log.Printf("STRUCT-Error marshalling topicSub: %v", err)
// 		return
// 	}

// 	broadcastMsg := BroadCastInput{
// 		Type: Topicsub,
// 		Msg:  msgData,
// 	}

// 	// Marshal BroadCastInput
// 	broadcastData, err := json.Marshal(broadcastMsg)
// 	if err != nil {
// 		log.Printf("STRUCT-Error marshalling BroadCastInput: %v", err)
// 		return
// 	}

// 	// Broadcast the message
// 	for _, r := range op.Broadcast(ctx, broadcastData) {
// 		if r.Error != nil {
// 			log.Printf("STRUCT-Error broadcasting to %s: %v", r.Id, r.Error)
// 		}
// 	}

// 	log.Println("STRUCT-Leader: Immediate topic-subscription structure broadcast completed.")
// }

func requestTopicSubFetch(ctx context.Context, op *hedge.Op) {
	// Send a request to leader to fetch the latest topic-subscription structure
	broadcastMsg := SendInput{
		Type: initialTopicSubFetch,
		Msg:  []byte{},
	}

	bin, _ := json.Marshal(broadcastMsg)
	out, err := op.Send(ctx, bin)
	if err != nil {
		log.Printf("STRUCT-Error sending request to leader: %v", err)
		return
	}

	err = storage.StoreTopicSubscriptions(out)
	if err != nil {
		log.Printf("STRUCT-Error storing topic-subscription data: %v", err)
	}
}

// Compare two topic-subscription maps for equality.
func AreTopicSubscriptionsEqual(current, last map[string][]string) bool {
	// First check if maps have the same number of keys
	if len(current) != len(last) {
		return false
	}

	// Check each topic and its subscriptions
	for topic, subs1 := range last {
		// Check if the topic exists in map2
		subs2, exists := current[topic]
		if !exists {
			return false
		}

		// Check if slices have the same length
		if len(subs1) != len(subs2) {
			return false
		}

		// Create frequency maps to compare elements regardless of order
		freq1 := make(map[string]int)
		for _, sub := range subs1 {
			freq1[sub]++
		}

		freq2 := make(map[string]int)
		for _, sub := range subs2 {
			freq2[sub]++
		}

		// Compare frequency maps
		for sub, count := range freq1 {
			if freq2[sub] != count {
				return false
			}
		}
	}

	return true
}

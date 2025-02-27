package broadcast

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge"
	"google.golang.org/api/iterator"
)

// fetchAllTopicSubscriptions fetches all topic-subscription mappings when lastBroadcasted is empty.
func fetchAllTopicSubscriptions(ctx context.Context, client *spanner.Client) map[string][]string {
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

// fetches updated topic-subscription data and broadcasts it
func fetchAndBroadcast(ctx context.Context, op *hedge.Op, client *spanner.Client, lastChecked *time.Time, lastBroadcasted *map[string][]string, isStartup bool) {
	if isStartup {
		// On startup, fetch all topic-subscription structure
		*lastBroadcasted = fetchAllTopicSubscriptions(ctx, client)
		log.Println("STRUCT-Leader: Startup detected. Broadcasting full topic-subscription data.")
	} else {
		// Check if any topic has been updated since lastChecked
		updateCheckStmt := spanner.Statement{
			SQL:    `SELECT COUNT(*) FROM Subscriptions WHERE updatedAt > @last_checked_time`,
			Params: map[string]interface{}{"last_checked_time": *lastChecked},
		}

		iter := client.Single().Query(ctx, updateCheckStmt)
		defer iter.Stop()

		var updateCount int64
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Printf("STRUCT-Error checking for updates: %v", err)
				return
			}
			if err := row.Columns(&updateCount); err != nil {
				log.Printf("STRUCT-Error reading update count: %v", err)
				return
			}
		}

		// If updates exist, fetch the topic-subscription structure
		if updateCount > 0 {
			log.Println("STRUCT-Leader: Changes detected. Fetching full topic-subscription structure.")
			*lastBroadcasted = fetchAllTopicSubscriptions(ctx, client)
		} else {
			return
		}
	}

	// If lastBroadcasted is empty, re-fetch all topic-subscription data
	if len(*lastBroadcasted) == 0 {
		log.Println("STRUCT-Leader: lastBroadcasted is empty! Running a full query to re-fetch topic-subscription data.")
		*lastBroadcasted = fetchAllTopicSubscriptions(ctx, client)
	}

	// If no updates, log and return
	if len(*lastBroadcasted) == 0 {
		log.Println("STRUCT-Leader: No updated topic-subscription data found, skipping broadcast.")
		return
	}

	log.Println("STRUCT-Leader: Fetched topic subscriptions.")
	//log.Println("STRUCT-Leader: Fetched topic subscriptions:", *lastBroadcasted)

	// Marshal topic-subscription data
	msgData, err := json.Marshal(*lastBroadcasted)
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
		}
	}

	*lastChecked = time.Now()

	log.Println("STRUCT-Debug: Updated lastBroadcasted")
	//log.Println("STRUCT-Debug: Updated lastBroadcasted with:", *lastBroadcasted)

	log.Println("STRUCT-Leader: Topic-subscription structure broadcast completed.")
}

// initializes the distributor that periodically checks for updates.
func StartDistributor(ctx context.Context, op *hedge.Op, client *spanner.Client) {
	lastChecked := time.Now()
	lastBroadcasted := make(map[string][]string)
	ticker := time.NewTicker(10 * time.Second)

	defer func() {
		ticker.Stop()
		log.Println("STRUCT-Leader: Distributor ticker stopped.")
	}()

	// perform an initial broadcast of all topic-subscription structures if it's the leader
	if hasLock, _ := op.HasLock(); hasLock {
		log.Println("STRUCT-Leader: Running initial startup query and broadcasting structure.")
		fetchAndBroadcast(ctx, op, client, &lastChecked, &lastBroadcasted, true) // run startup broadcast
	} else {
		log.Println("STRUCT-Follower: Skipping startup query.")
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("STRUCT-Leader: Context canceled, stopping distributor.")
			return
		case <-ticker.C:
			hasLock, _ := op.HasLock()

			if hasLock {
				log.Println("STRUCT-Leader: Processing updates...")
				fetchAndBroadcast(ctx, op, client, &lastChecked, &lastBroadcasted, false)
			} else {
				log.Println("STRUCT-Follower: No action needed. Skipping fetchAndBroadcast.")
			}
		}
	}
}

func ImmediateBroadcast(ctx context.Context, op *hedge.Op, client *spanner.Client, lastBroadcasted *map[string][]string, lastChecked *time.Time) {
	log.Println("STRUCT-Leader: Immediate broadcast triggered.")

	// Fetch latest topic-subscription data
	newBroadcasted := fetchAllTopicSubscriptions(ctx, client)
	if len(newBroadcasted) == 0 {
		log.Println("STRUCT-Leader: No updated topic-subscription data found, skipping immediate broadcast.")
		return
	}

	*lastBroadcasted = newBroadcasted

	// Marshal topic-subscription data
	msgData, err := json.Marshal(*lastBroadcasted)
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

	// Broadcast the message
	for _, r := range op.Broadcast(ctx, broadcastData) {
		if r.Error != nil {
			log.Printf("STRUCT-Error broadcasting to %s: %v", r.Id, r.Error)
		}
	}

	*lastChecked = time.Now()
	log.Println("STRUCT-Leader: Immediate topic-subscription structure broadcast completed.")
}

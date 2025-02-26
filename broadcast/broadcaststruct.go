package broadcast

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge/v2"
	"google.golang.org/api/iterator"
)

// fetches updated topic-subscription data and broadcasts it
func fetchAndBroadcast(ctx context.Context, op *hedge.Op, client *spanner.Client, lastChecked *time.Time, lastBroadcasted *map[string][]string, isStartup bool) {
	stmt := spanner.Statement{}

	if isStartup {
		// on startup, fetch all topic-subscription structure (ignore updatedAt)
		stmt.SQL = `SELECT topic, ARRAY_AGG(name) AS subscriptions FROM Subscriptions GROUP BY topic`
		log.Println("Leader: Startup detected. Broadcasting full topic-subscription data.")
	} else {
		// check if any topic has been updated since lastChecked
		updateCheckStmt := spanner.Statement{
			SQL:    `SELECT COUNT(*) FROM Subscriptions WHERE updatedAt > @last_checked_time`,
			Params: map[string]interface{}{"last_checked_time": *lastChecked},
		}

		iter := client.Single().Query(ctx, updateCheckStmt)
		defer iter.Stop()

		var updateCount int64 = 0 // Ensure updateCount is initialized
		row, err := iter.Next()
		if err == nil {
			if err := row.Columns(&updateCount); err != nil {
				log.Printf("Error checking for updates: %v", err)
				return
			}
		}

		// if updates exist, fetch the topic-subscription structure
		if updateCount > 0 {
			log.Println("Leader: Changes detected! Fetching full topic-subscription structure.")
			stmt.SQL = `SELECT topic, ARRAY_AGG(name) AS subscriptions FROM Subscriptions GROUP BY topic`
		} else {
			return
		}
	}

	log.Println("Debug: Executing Spanner query...")

	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	topicSub := make(map[string][]string)

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("Fatal error iterating Spanner rows: %v", err)
		}

		var topic string
		var subscriptions []string
		if err := row.Columns(&topic, &subscriptions); err != nil {
			log.Printf("Error reading row: %v", err)
			continue
		}

		// ensure subscriptions is a valid empty slice if nil
		subscriptions = append([]string{}, subscriptions...)

		topicSub[topic] = subscriptions
	}

	// debug log to check retrieved data
	log.Println("Debug: Retrieved topic-subscription structure from Spanner:", topicSub)

	// If no updates and it's not startup, check if lastBroadcasted is empty
	if len(topicSub) == 0 {
		log.Println("Leader: No new updates, skipping broadcast.")

		// If lastBroadcasted is empty, force a re-query and broadcast
		if len(*lastBroadcasted) == 0 {
			log.Println("Leader: lastBroadcasted is empty! Running a full query to re-fetch topic-subscription data.")

			// Stop previous iterator before reassigning a new query
			iter.Stop()
			stmt.SQL = `SELECT topic, ARRAY_AGG(name) AS subscriptions FROM Subscriptions GROUP BY topic`
			iter = client.Single().Query(ctx, stmt)
			defer iter.Stop()

			// Fetch all topics again
			topicSub = make(map[string][]string)
			for {
				row, err := iter.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					log.Fatalf("Fatal error iterating Spanner rows: %v", err)
				}

				var topic string
				var subscriptions []string
				if err := row.Columns(&topic, &subscriptions); err != nil {
					log.Printf("Error reading row: %v", err)
					continue
				}

				subscriptions = append([]string{}, subscriptions...)
				topicSub[topic] = subscriptions
			}

			// If it's still empty after the query, log an error to prevent infinite loops
			if len(topicSub) == 0 {
				log.Println("Leader: No topic-subscription data found, skipping broadcast.")
				return
			}

			log.Println("Leader: Re-query successful. Broadcasting all topic-subscription data.")
		}

		// If lastBroadcasted is still populated, log the existing structure
		log.Println("Leader: Subscription topic structure is still:", *lastBroadcasted)
		return
	}

	log.Println("Leader: Fetched topic subscriptions:", topicSub)

	// Marshal topic-subscription data
	msgData, err := json.Marshal(topicSub)
	if err != nil {
		log.Printf("Error marshalling topicSub: %v", err)
		return
	}

	broadcastMsg := BroadCastInput{
		Type: topicsub,
		Msg:  msgData,
	}

	// Marshal BroadCastInput
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
	if isStartup {
		log.Println("Debug: Storing first broadcasted topic-subscription structure.")
	}

	*lastBroadcasted = topicSub
	log.Println("Debug: Updated lastBroadcasted with:", *lastBroadcasted)
	log.Println("Leader: Topic-subscription structure broadcast completed.")
}

// initializes the distributor that periodically checks for updates.
func StartDistributor(ctx context.Context, op *hedge.Op, client *spanner.Client) {
	lastChecked := time.Now().Add(-24 * time.Hour) // Ensure older updates are included
	lastBroadcasted := make(map[string][]string)   // Ensure it's an initialized empty map
	ticker := time.NewTicker(10 * time.Second)

	// ensure ticker is stopped when the function exits
	defer func() {
		ticker.Stop()
		log.Println("Leader: Distributor ticker stopped.")
	}()

	// perform an initial broadcast of all topic-subscription structures if it's the leader
	if hasLock, _ := op.HasLock(); hasLock {
		log.Println("Leader: Running initial startup query and broadcasting structure.")
		fetchAndBroadcast(ctx, op, client, &lastChecked, &lastBroadcasted, true) // run startup broadcast
	} else {
		log.Println("Follower: Skipping startup query.")
	}

	for {
		select {
		case <-ctx.Done():
			log.Println("Leader: Context canceled, stopping distributor.")
			return
		case <-ticker.C:
			hasLock, _ := op.HasLock()

			if hasLock {
				log.Println("Leader: Processing updates...")
				fetchAndBroadcast(ctx, op, client, &lastChecked, &lastBroadcasted, false)
			} else {
				log.Println("Follower: No action needed. Skipping fetchAndBroadcast.")
			}
		}
	}
}

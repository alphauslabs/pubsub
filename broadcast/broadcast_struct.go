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

// fetchAndBroadcast fetches updated topic-subscription data and broadcasts it if there are updates.
func fetchAndBroadcast(ctx context.Context, op *hedge.Op, client *spanner.Client, lastChecked *time.Time, lastBroadcasted *map[string][]string, isStartup bool) {
	stmt := spanner.Statement{}

	if isStartup {
		// on startup, fetch all topic-subscription mappings (ignore updatedAt)
		stmt.SQL = `SELECT topic, ARRAY_AGG(name) AS subscriptions FROM Subscriptions GROUP BY topic`
		log.Println("Leader: Startup detected. Broadcasting full topic-subscription data.")
	} else {
		// fetch only updated subscriptions
		stmt.SQL = `SELECT topic, ARRAY_AGG(name) AS subscriptions 
                    FROM Subscriptions 
                    WHERE updatedAt > @last_checked_time 
                    GROUP BY topic`
		stmt.Params = map[string]interface{}{"last_checked_time": *lastChecked}
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
			log.Printf("Error iterating rows: %v", err)
			return
		}

		var topic string
		var subscriptions []string
		if err := row.Columns(&topic, &subscriptions); err != nil {
			log.Printf("Error reading row: %v", err)
			continue
		}

		// Ensure subscriptions is not nil
		if subscriptions == nil {
			subscriptions = []string{}
		}

		topicSub[topic] = subscriptions
	}

	// if no updates and it's not startup, skip broadcasting
	if len(topicSub) == 0 && !isStartup {
		log.Println("Leader: No new updates, skipping broadcast.")
		if len(*lastBroadcasted) > 0 {
			log.Println("Leader: Subscription topic structure is still:", *lastBroadcasted)
		} else {
			log.Println("Leader: No previous topic-subscription structure available.")
		}
		return
	}

	log.Println("Leader: Fetched topic subscriptions:", topicSub)

	// marshal topic-subscription data
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

	// update last checked time and last broadcasted structure
	*lastChecked = time.Now()
	*lastBroadcasted = topicSub
	log.Println("Leader: Topic-subscription structure broadcast completed.")
}

// StartDistributor initializes the distributor that periodically checks for updates.
func StartDistributor(ctx context.Context, op *hedge.Op, client *spanner.Client) {
	lastChecked := time.Now().Add(-10 * time.Second)
	lastBroadcasted := make(map[string][]string) // Stores the last known structure
	ticker := time.NewTicker(10 * time.Second)

	// Ensure ticker is stopped when the function exits
	defer func() {
		ticker.Stop()
		log.Println("Leader: Distributor ticker stopped.")
	}()

	// Perform an initial broadcast of all topic-subscription structures only if this instance is the leader
	if hasLock, _ := op.HasLock(); hasLock {
		log.Println("Leader: Startup detected. Broadcasting full topic-subscription data.")
		fetchAndBroadcast(ctx, op, client, &lastChecked, &lastBroadcasted, true) // run during startup
	} else {
		log.Println("Follower: Skipping startup broadcast.")
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
				fetchAndBroadcast(ctx, op, client, &lastChecked, &lastBroadcasted, false) // Run only if leader
			} else {
				log.Println("Follower: No action needed. Skipping fetchAndBroadcast.")
			}
		}
	}
}

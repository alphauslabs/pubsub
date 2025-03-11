package handlers

import (
	"context"
	"encoding/json"
	"log"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/leader"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/flowerinthenight/hedge"
	"github.com/golang/glog"
	"google.golang.org/api/iterator"
)

var (
	lastBroadcasted = make(map[string]map[string]*storage.Subscription)
)

func FetchAllTopicSubscriptions(ctx context.Context, client *spanner.Client) map[string]map[string]*storage.Subscription {
	stmt := spanner.Statement{
		SQL: `SELECT topic, name AS subscription, autoextend FROM Subscriptions WHERE name IS NOT NULL ORDER BY topic, subscription`,
	}

	iter := client.Single().Query(ctx, stmt)
	defer iter.Stop()

	topicSub := make(map[string]map[string]*storage.Subscription)

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			log.Fatalf("STRUCT-Fatal error iterating Spanner rows: %v", err)
		}
		var topic string
		var subName string
		var autoExtend bool
		if err := row.Columns(&topic, &subName, &autoExtend); err != nil {
			glog.Infof("STRUCT-Error reading row: %v", err)
			continue
		}

		// Ensure the topic exists in the map
		if _, exists := topicSub[topic]; !exists {
			topicSub[topic] = make(map[string]*storage.Subscription)
		}

		// Store topic-subscription with autoextend structure
		topicSub[topic][subName] = &storage.Subscription{
			Subscription: &pb.Subscription{
				Name:       subName,
				Topic:      topic,
				Autoextend: autoExtend,
			},
		}
	}

	return topicSub
}

func FetchAndBroadcast(ctx context.Context, app *app.PubSub, isStartup bool) {
	var latest map[string]map[string]*storage.Subscription
	if isStartup {
		requestTopicSubFetch(ctx, app.Op) // request to the current leader
		return
	}

	latest = FetchAllTopicSubscriptions(ctx, app.Client)
	if AreTopicSubscriptionsEqual(latest, lastBroadcasted) {
		glog.Info("STRUCT-Leader: No changes detected in topic-subscription structure.")
		return
	}

	//log message to see the broadcasted message
	glog.Info("STRUCT-Leader: Broadcasting updated topic-subscription structure with AutoExtend.")
	for topic, subs := range latest {
		for subName, sub := range subs {
			glog.Infof("STRUCT-Leader: %s -> %s (AutoExtend: %t)", topic, subName, sub.Subscription.Autoextend)
		}
	}

	// Marshal topic-subscription data into JSON
	msgData, err := json.Marshal(latest)
	if err != nil {
		glog.Infof("STRUCT-Error marshalling topicSub: %v", err)
		return
	}

	broadcastMsg := BroadCastInput{
		Type: Topicsub,
		Msg:  msgData,
	}

	// Marshal BroadCastInput
	broadcastData, err := json.Marshal(broadcastMsg)
	if err != nil {
		glog.Infof("STRUCT-Error marshalling BroadCastInput: %v", err)
		return
	}

	// Broadcast message
	out := app.Op.Broadcast(ctx, broadcastData)
	for _, r := range out {
		if r.Error != nil {
			glog.Infof("STRUCT-Error broadcasting to %s: %v", r.Id, r.Error)
		} else {
			lastBroadcasted = latest
		}
	}

	glog.Info("STRUCT-Leader: Topic-subscription structure broadcast completed.")
}

func StartBroadcastTopicSub(ctx context.Context, app *app.PubSub) {
	glog.Info("[STRUCT] Starting distribution of topic-sub scription structure...")
	ticker := time.NewTicker(10 * time.Second) // will adjust to lower interval later
	defer func() {
		ticker.Stop()
		glog.Info("STRUCT-Leader: Distributor ticker stopped.")
	}()

	// perform an initial broadcast of all topic-subscription structures
	FetchAndBroadcast(ctx, app, true) // run startup broadcast

	for {
		select {
		case <-ctx.Done():
			glog.Info("STRUCT-Leader: Context canceled, stopping distributor...")
			return
		case <-ticker.C:
			if atomic.LoadInt32(&leader.IsLeader) == 1 {
				FetchAndBroadcast(ctx, app, false)
			}
		}
	}
}

func requestTopicSubFetch(ctx context.Context, op *hedge.Op) {
	// Send a request to leader to fetch the latest topic-subscription structure
	broadcastMsg := SendInput{
		Type: initialTopicSubFetch,
		Msg:  []byte{},
	}

	bin, _ := json.Marshal(broadcastMsg)
	out, err := op.Send(ctx, bin)
	if err != nil {
		glog.Infof("STRUCT-Error sending request to leader: %v", err)
		return
	}
	var d map[string]map[string]*storage.Subscription
	err = json.Unmarshal(out, &d)
	if err != nil {
		glog.Infof("STRUCT-Error unmarshalling topic-subscription data: %v", err)
		return
	}

	err = storage.StoreTopicSubscriptions(d)
	if err != nil {
		glog.Infof("STRUCT-Error storing topic-subscription data: %v", err)
	}
}

// Compare two topic-subscription maps for equality.
func AreTopicSubscriptionsEqual(current, last map[string]map[string]*storage.Subscription) bool {
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
		for subName := range subs1 {
			freq1[subName]++
		}

		freq2 := make(map[string]int)
		for subName := range subs2 {
			freq2[subName]++
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

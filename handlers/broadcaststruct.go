package handlers

import (
	"context"
	"encoding/json"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/leader"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/alphauslabs/pubsub/utils"
	"github.com/flowerinthenight/hedge/v2"
	"github.com/golang/glog"
	"google.golang.org/api/iterator"
)

var (
	lastBroadcasted = make(map[string]map[string]*storage.Subscription)
)

func FetchAllTopicSubscriptions(ctx context.Context, client *spanner.Client) map[string]map[string]*storage.Subscription {
	stmt := spanner.Statement{
		SQL: `SELECT topic, name AS subscription, autoextend FROM pubsub_subscriptions WHERE name IS NOT NULL ORDER BY topic, subscription`,
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
			glog.Errorf("STRUCT-Error reading row: %v", err)
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
				AutoExtend: autoExtend,
			},
		}
	}

	return topicSub
}

func FetchAndBroadcast(ctx context.Context, app *app.PubSub, isStartup bool) {
	if isStartup {
		requestTopicSubFetch(ctx, app.Op) // request to the current leader
		return
	}

	latest := FetchAllTopicSubscriptions(ctx, app.Client)
	if AreTopicSubscriptionsEqual(latest, lastBroadcasted) {
		return
	}

	for k, v := range storage.RecordMap {
		grouped := utils.CreateGrouping(latest, v)
		msgData, err := json.Marshal(grouped)
		if err != nil {
			glog.Errorf("STRUCT-Error marshalling topicSub: %v", err)
			return
		}

		broadcastMsg := BroadCastInput{
			Type: Topicsub,
			Msg:  msgData,
		}

		// Marshal BroadCastInput
		broadcastData, err := json.Marshal(broadcastMsg)
		if err != nil {
			glog.Errorf("STRUCT-Error marshalling BroadCastInput: %v", err)
			return
		}

		k := strings.Split(k, ":")[0]
		k = k + ":" + "50051"

		glog.Infof("STRUCT-Broadcasting topic-subscription data to %s: %v", k, string(broadcastData))

		out := app.Op.Broadcast(ctx, broadcastData, hedge.BroadcastArgs{
			OnlySendTo: []string{k}, // only send to this node
		})
		for _, r := range out {
			if r.Error != nil {
				glog.Errorf("STRUCT-Error broadcasting to %s: %v", r.Id, r.Error)
			} else {
				lastBroadcasted = latest
			}
		}
	}
}

func StartBroadcastTopicSub(ctx context.Context, app *app.PubSub) {
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
	me := op.Name()
	me = strings.Split(me, ":")[0]
	me = me + ":" + "50051"
	broadcastMsg := SendInput{
		Type: initialTopicSubFetch,
		Msg:  []byte(me),
	}

	bin, _ := json.Marshal(broadcastMsg)
	out, err := op.Send(ctx, bin)
	if err != nil {
		glog.Errorf("STRUCT-Error sending request to leader: %v", err)
		return
	}
	var d map[string]map[string]*storage.Subscription
	err = json.Unmarshal(out, &d)
	if err != nil {
		glog.Errorf("STRUCT-Error unmarshalling topic-subscription data: %v", err)
		return
	}

	err = storage.StoreTopicSubscriptions(d)
	if err != nil {
		glog.Errorf("STRUCT-Error storing topic-subscription data: %v", err)
	}

	glog.Infof("[RequestStructFromLeader] topic-subscription data from leader: %v", string(out))
}

// AreTopicSubscriptionsEqual compares two topic-subscription maps for equality,
// including both structure and property values of subscriptions
func AreTopicSubscriptionsEqual(current, last map[string]map[string]*storage.Subscription) bool {
	// First check if maps have the same number of topics
	if len(current) != len(last) {
		glog.Infof("STRUCT-Compare: Different number of topics: current=%d, last=%d",
			len(current), len(last))
		return false
	}

	// Check each topic and its subscriptions
	for topic, lastSubs := range last {
		// Check if the topic exists in current map
		currentSubs, exists := current[topic]
		if !exists {
			glog.Infof("STRUCT-Compare: Topic %s exists in last but not in current", topic)
			return false
		}

		// Check if subscriptions have the same length
		if len(lastSubs) != len(currentSubs) {
			glog.Infof("STRUCT-Compare: Different number of subscriptions for topic %s: current=%d, last=%d",
				topic, len(currentSubs), len(lastSubs))
			return false
		}

		// Check each subscription
		for subName, lastSub := range lastSubs {
			// Check if subscription exists in current map
			currentSub, exists := currentSubs[subName]
			if !exists {
				glog.Infof("STRUCT-Compare: Subscription %s exists in last but not in current for topic %s",
					subName, topic)
				return false
			}

			// Compare subscription properties
			if !areSubscriptionsEqual(currentSub, lastSub) {
				glog.Infof("STRUCT-Compare: Properties differ for subscription %s in topic %s",
					subName, topic)
				return false
			}
		}

		// Check if current has any subscriptions not in last
		for subName := range currentSubs {
			if _, exists := lastSubs[subName]; !exists {
				glog.Infof("STRUCT-Compare: Subscription %s exists in current but not in last for topic %s",
					subName, topic)
				return false
			}
		}
	}

	// Check if current has any topics not in last
	for topic := range current {
		if _, exists := last[topic]; !exists {
			glog.Infof("STRUCT-Compare: Topic %s exists in current but not in last", topic)
			return false
		}
	}

	return true
}

// areSubscriptionsEqual compares two subscription objects for property equality
func areSubscriptionsEqual(sub1, sub2 *storage.Subscription) bool {
	// Check for nil values
	if sub1 == nil || sub2 == nil {
		return sub1 == sub2 // Both should be nil or both should be non-nil
	}

	// Check if both subscription objects have valid pb.Subscription fields
	if sub1.Subscription == nil || sub2.Subscription == nil {
		return sub1.Subscription == sub2.Subscription // Both should be nil or both should be non-nil
	}

	// Compare Name property
	if sub1.Subscription.Name != sub2.Subscription.Name {
		glog.Infof("STRUCT-Compare: Subscription name changed from %s to %s",
			sub2.Subscription.Name, sub1.Subscription.Name)
		return false
	}

	// Compare Topic property
	if sub1.Subscription.Topic != sub2.Subscription.Topic {
		glog.Infof("STRUCT-Compare: Subscription topic changed from %s to %s for subscription %s",
			sub2.Subscription.Topic, sub1.Subscription.Topic, sub1.Subscription.Name)
		return false
	}

	// Compare AutoExtend property
	if sub1.Subscription.AutoExtend != sub2.Subscription.AutoExtend {
		glog.Infof("STRUCT-Compare: AutoExtend setting changed from %v to %v for subscription %s",
			sub2.Subscription.AutoExtend, sub1.Subscription.AutoExtend, sub1.Subscription.Name)
		return false
	}

	return true
}

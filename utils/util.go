package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/flowerinthenight/hedge/v2"
	"github.com/golang/glog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	MessagesTable = "pubsub_messages"
)

func EnsureLeaderActive(op *hedge.Op, ctx context.Context) (bool, error) {
	msg := struct {
		Type string
		Msg  []byte
	}{
		Type: "checkleader",
		Msg:  []byte("PING"),
	}

	b, _ := json.Marshal(msg)
	r, err := hedge.SendToLeader(ctx, op, b, &hedge.SendToLeaderArgs{Retries: 50})
	if err != nil {
		return false, err
	}

	switch {
	case string(r) == "PONG":
		return true, nil
	default:
		return false, nil
	}
}

func UpdateMessageProcessedStatus(spannerClient *spanner.Client, id string) error {
	if id == "" {
		glog.Error("[Acknowledge]: Received invalid message ID")
		return nil
	}

	// Update the message processed status in Spanner
	_, err := spannerClient.Apply(context.Background(), []*spanner.Mutation{
		spanner.Update(MessagesTable, []string{"id", "processed", "updatedAt"}, []any{id, true, spanner.CommitTimestamp}),
	})
	if err != nil {
		glog.Errorf("[Acknowledge]: Failed to update message processed status in Spanner: %v", err)
		return err
	}

	glog.Infof("[Acknowledge]: Updated message processed status in Spanner for ID: %s, Processed: %v", id, true)
	return nil
}

func UpdateMessageProcessedStatusForSub(spannerClient *spanner.Client, id, sub string) error {
	if id == "" || sub == "" {
		glog.Error("[Acknowledge]: Received invalid message or subscription ID")
		return fmt.Errorf("invalid message ID or subscription ID")
	}

	_, err := spannerClient.ReadWriteTransaction(context.Background(), func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Get current status within transaction
		row, err := txn.ReadRow(ctx, MessagesTable, spanner.Key{id}, []string{"subStatus"})
		if err != nil {
			glog.Errorf("[Acknowledge]: Error reading message %s: %v", id, err)
			return err
		}

		var temp string
		if err := row.Column(0, &temp); err != nil {
			return err
		}

		var subStatus map[string]bool
		if err := json.Unmarshal([]byte(temp), &subStatus); err != nil {
			return err
		}

		// Update status
		subStatus[sub] = true
		b, err := json.Marshal(subStatus)
		if err != nil {
			return err
		}

		// Update within transaction
		txn.BufferWrite([]*spanner.Mutation{
			spanner.Update(MessagesTable, []string{"id", "subStatus", "updatedAt"},
				[]any{id, string(b), spanner.CommitTimestamp}),
		})
		return nil
	})

	if err != nil {
		glog.Errorf("[Acknowledge]: Failed to update message sub status in Spanner: %v", err)
		return err
	}

	glog.Infof("[Acknowledge]: Successfully updated message status for ID: %s, Sub: %s", id, sub)
	return nil
}

func CheckIfTopicSubscriptionIsCorrect(topicID, subscription string) error {
	subs, err := storage.GetSubscribtionsForTopic(topicID)

	if err != nil {
		glog.Errorf("[Subscribe] Topic %s not found in storage", topicID)
		return status.Errorf(codes.NotFound, "Topic %s not found", topicID)
	}

	// Check if the provided subscription ID exists in the topic's subscriptions
	found := false
	for _, sub := range subs {
		if sub.Name == subscription {
			found = true
			glog.Infof("[Subscribe] Subscription %s found in topic %s", subscription, topicID)
			break
		}
	}

	if !found {
		glog.Infof("[Subscribe] Subscription %s not found in topic %s", subscription, topicID)
		return status.Errorf(codes.NotFound, "Subscription %s not found", subscription)
	}

	return nil
}

func CreateRecordMapping(app *app.PubSub) map[string][]string {
	record := map[string][]string{} // Maps node IDs to subscription prefixes
	all := app.Op.Members()
	if len(all) > 0 {
		alphabet := "abcdefghijklmnopqrstuvwxyz"
		charsPerNode := len(alphabet) / len(all)
		remainder := len(alphabet) % len(all)

		start := 0
		for i, nodeID := range all {
			nodeID = strings.Split(nodeID, ":")[0]
			nodeID = nodeID + ":" + "50052"
			end := start + charsPerNode
			if i < remainder {
				// Distribute remainder characters evenly
				end++
			}

			// Assign a range of letters to this node
			if end > len(alphabet) {
				end = len(alphabet)
			}

			charRange := alphabet[start:end]
			record[nodeID] = []string{charRange}
			glog.Infof("Node %s assigned subscription prefixes: %s", nodeID, charRange)

			start = end
		}
	}
	return record
}

// Find the correct nodeId for this subscription prefix
func WhatNode(pre string, record map[string][]string) string {
	for nodeID, prefixes := range record {
		for _, prefix := range prefixes {
			if pre == prefix {
				return nodeID
			}
		}
	}
	return ""
}

func BroadcastRecord(app *app.PubSub, record map[string][]string) error {
	data, err := json.Marshal(record)
	if err != nil {
		glog.Errorf("Error marshalling record: %v", err)
		return err
	}

	broadcastMsg := struct {
		Type string
		Msg  []byte
	}{
		Type: "recordmap",
		Msg:  data,
	}

	broadcastData, err := json.Marshal(broadcastMsg)
	if err != nil {
		glog.Errorf("Error marshalling BroadCastInput: %v", err)
		return err
	}

	out := app.Op.Broadcast(context.Background(), broadcastData)
	for _, r := range out {
		if r.Error != nil {
			glog.Errorf("Error broadcasting to %s: %v", r.Id, r.Error)
			return r.Error
		}
	}
	return nil
}

func GetSamePrefixSubscriptions(subs map[string]map[string]*storage.Subscription, topic string) map[string]map[string]*storage.Subscription {
	samePrefixSubs := make(map[string]map[string]*storage.Subscription)
	for k, v := range subs {
		if k == topic {
			samePrefixSubs[k] = v
		}
	}
	return samePrefixSubs
}

func CreateGrouping(ts map[string]map[string]*storage.Subscription, grp []string) map[string]map[string]*storage.Subscription {
	grouped := make(map[string]map[string]*storage.Subscription)
	for topic, subs := range ts {
		for subName, sub := range subs {
			if IsPresent(subName, grp) {
				grouped[topic][subName] = sub
			}
		}
	}
	return grouped
}

// IsPresent check if s prefix is present in arr
// Example: s = "apple", a is present in arr = ["a", "b", "c"] so this return true
func IsPresent(s string, arr []string) bool {
	for _, v := range arr {
		if strings.HasPrefix(s, v) {
			return true
		}
	}
	return false
}

func CheckIfSubscriptionIsCorrect(sub string, nodeId string) (bool, string) {
	recs := storage.RecordMap[nodeId]
	if IsPresent(sub, recs) {
		return true, ""
	}

	for name, v := range storage.RecordMap {
		for _, rec := range v {
			if strings.HasPrefix(sub, rec) {
				return false, name
			}
		}
	}

	return false, ""
}

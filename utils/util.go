package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"cloud.google.com/go/spanner"
	pubsubproto "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/flowerinthenight/hedge/v2"
	"github.com/golang/glog"
	"google.golang.org/api/iterator"
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
	f := make([]string, 0, len(all))
	// Get their external IP
	for _, nodeID := range all {
		inp := struct {
			Type string
			Msg  []byte
		}{
			Type: "getexternalip",
			Msg:  []byte(""),
		}
		broadcastData, _ := json.Marshal(inp)
		resp := app.Op.Broadcast(context.Background(), broadcastData, hedge.BroadcastArgs{
			OnlySendTo: []string{nodeID},
		})
		for _, r := range resp {
			if r.Error == nil {
				f = append(f, string(r.Reply)) // Add the node ID to the list
			}
		}
	}
	if len(f) > 0 {
		alphabet := "abcdefghijklmnopqrstuvwxyz"
		charsPerNode := len(alphabet) / len(f)
		remainder := len(alphabet) % len(f)

		start := 0
		for i, nodeID := range f {
			nodeID = strings.Split(nodeID, ":")[0]
			end := start + charsPerNode
			if i < remainder {
				// Distribute remainder characters evenly
				end++
			}

			if end > len(alphabet) {
				end = len(alphabet)
			}

			// Convert character range to individual letters
			charRange := alphabet[start:end]
			individualLetters := make([]string, len(charRange))
			for j, c := range charRange {
				individualLetters[j] = string(c)
			}
			internalIp := strings.Split(all[i], ":")[0]
			record[nodeID+"|"+internalIp] = individualLetters
			glog.Infof("Node %s assigned subscription prefixes: %v", nodeID, individualLetters)

			start = end
		}
	}
	return record
}

// Find the correct nodeId for this subscription prefix
func WhatNode(pre string, record map[string][]string) string {
	storage.RecordMapMu.RLock()
	defer storage.RecordMapMu.RUnlock()

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
	storage.RecordMapMu.RLock()
	defer storage.RecordMapMu.RUnlock()

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

	out := app.Op.Broadcast(context.Background(), broadcastData, hedge.BroadcastArgs{
		SkipSelf: true,
	})
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

// Returns a list of node IDs that have subscriptions matching any of the provided prefixes.
// pre is list of prefixes, record is our record map
func GetSubNodeHandlers(pre []string, record map[string][]string) []string {
	storage.RecordMapMu.RLock()
	defer storage.RecordMapMu.RUnlock()

	handlers := make([]string, 0)
	temp := make(map[string]struct{})
	for _, p := range pre {
		for nodeID, prefixes := range record {
			if IsPresent(p, prefixes) {
				if _, exists := temp[nodeID]; !exists {
					temp[nodeID] = struct{}{} // Ensure unique node IDs
				}
			}
		}
	}
	for nodeID := range temp {
		handlers = append(handlers, nodeID)
	}

	return handlers
}

func CreateGrouping(ts map[string]map[string]*storage.Subscription, grp []string) map[string]map[string]*storage.Subscription {
	grouped := make(map[string]map[string]*storage.Subscription)
	for topic, subs := range ts {
		grouped[topic] = make(map[string]*storage.Subscription)
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

// Verify if subscription is in correct node, if not, it wil return the correct one.
func CheckIfSubscriptionIsCorrect(sub string, nodeId string) (bool, string) {
	storage.RecordMapMu.RLock()
	defer storage.RecordMapMu.RUnlock()

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

func GetSubMembers(record map[string][]string, nodeId string, search []string) []string {
	storage.RecordMapMu.RLock()
	defer storage.RecordMapMu.RUnlock()

	members := make([]string, 0)
	mem, ok := storage.RecordMap[nodeId]
	if ok {
		members = append(members, mem...)
	}

	tmp := make(map[string]struct{})
	res := make([]string, 0)
	for _, mem := range members {
		for _, s := range search {
			if strings.HasPrefix(s, mem) {
				if _, exists := tmp[mem]; !exists {
					tmp[s] = struct{}{} // Ensure unique members
				}
			}
		}
	}

	for m := range tmp {
		res = append(res, m)
	}

	glog.Infof("GetSubMembers: %v", res)
	return res
}

func GetAllSubscriptionsForTopic(topic string, client *spanner.Client) ([]*storage.Subscription, error) {
	stmt := spanner.Statement{
		SQL: `SELECT name, topic, autoextend FROM pubsub_subscriptions WHERE topic = @topic ORDER BY name`,
		Params: map[string]interface{}{
			"topic": topic,
		},
	}

	iter := client.Single().Query(context.Background(), stmt)
	defer iter.Stop()

	var subscriptions []*storage.Subscription
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("error fetching subscriptions for topic %s: %v", topic, err)
		}

		sub := storage.Subscription{
			Subscription: &pubsubproto.Subscription{},
		}
		if err := row.Columns(&sub.Subscription.Name, &sub.Subscription.Topic, &sub.Subscription.AutoExtend); err != nil {
			return nil, fmt.Errorf("error reading subscription row: %v", err)
		}
		subscriptions = append(subscriptions, &sub)
	}

	return subscriptions, nil
}

// GetMyExternalIp retrieves the external IP address of the current node.
func GetMyExternalIp(op *hedge.Op) (string, error) {
	client := &http.Client{}
	req, err := http.NewRequest("GET", "http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip", nil)
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Add("Metadata-Flavor", "Google")
	resp, err := client.Do(req)
	if err != nil {
		glog.Errorf("[GetExternalIp] Error fetching external IP: %v", err)
		return "", fmt.Errorf("failed to fetch external IP: %w", err)
	}
	defer resp.Body.Close()

	ip, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}
	return string(ip), nil
}

// Generates Ip:port format for internal comms (communication between nodes)
func AddrForInternal(record string) string {
	s := strings.Split(record, "|")
	if len(s) >= 2 {
		return fmt.Sprintf("%s:50052", s[1])
	}

	return fmt.Sprintf("%s:50052", s[0])
}

// Generates Ip:port format for external comms (for pubsub clients)
func AddrForExternal(record string) string {
	s := strings.Split(record, "|")
	return fmt.Sprintf("%s:50051", s[0])
}

func NotifyLeaderForTopicSubBroadcast(ctx context.Context, op *hedge.Op) {
	input := struct {
		Type string
		Msg  []byte
	}{
		Type: "topicsubupdates",
		Msg:  []byte{},
	}

	inputData, err := json.Marshal(input)
	if err != nil {
		glog.Errorf("Error marshaling send input: %v", err)
		return
	}

	_, err = op.Send(ctx, inputData)
	if err != nil {
		glog.Errorf("Failed to send to leader: %v", err)
	}
}

func NotifyLeaderForAllMessageBroadcast(ctx context.Context, op *hedge.Op) {
	input := struct {
		Type string
		Msg  []byte
	}{
		Type: "allmessages",
		Msg:  []byte{},
	}

	inputData, err := json.Marshal(input)
	if err != nil {
		glog.Errorf("Error marshaling send input: %v", err)
		return
	}

	_, err = op.Send(ctx, inputData)
	if err != nil {
		glog.Errorf("Failed to send to leader: %v", err)
	}
}

package utils

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/spanner"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/flowerinthenight/hedge"
	"github.com/golang/glog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		glog.Info("[Acknowledge]: Received invalid message ID")
		return nil
	}

	// Update the message processed status in Spanner
	_, err := spannerClient.Apply(context.Background(), []*spanner.Mutation{
		spanner.Update("Messages", []string{"id", "processed", "updatedAt"}, []any{id, true, spanner.CommitTimestamp}),
	})
	if err != nil {
		glog.Infof("[Acknowledge]: Failed to update message processed status in Spanner: %v", err)
		return err
	}

	glog.Infof("[Acknowledge]: Updated message processed status in Spanner for ID: %s, Processed: %v", id, true)
	return nil
}

func UpdateMessageProcessedStatusForSub(spannerClient *spanner.Client, id, sub string) error {
	if id == "" {
		glog.Info("[Acknowledge]: Received invalid message ID")
		return nil
	}

	// Query first to get the current status
	query := spanner.NewStatement("SELECT subStatus FROM Messages WHERE id = @id and processed = false")
	query.Params["id"] = id
	iter := spannerClient.Single().Query(context.Background(), query)
	defer iter.Stop()
	var subStatus map[string]bool
	var temp string

	for {
		row, err := iter.Next()
		if err != nil {
			if spanner.ErrCode(err) == codes.NotFound {
				glog.Errorf("[Acknowledge]: Message with ID %s not found in Spanner", id)
				return nil
			}
			glog.Infof("[Acknowledge]: Error querying message status: %v", err)
			return err
		}

		if err := row.Columns(&temp); err != nil {
			glog.Errorf("[Acknowledge]: Error reading message status: %v", err)
			return err
		}

		if err := json.Unmarshal([]byte(temp), &subStatus); err != nil {
			glog.Errorf("[Acknowledge]: Error unmarshalling message status: %v", err)
			return err
		}
		break
	}

	subStatus[sub] = true
	b, _ := json.Marshal(subStatus)

	// Update the message processed status in Spanner
	_, err := spannerClient.Apply(context.Background(), []*spanner.Mutation{
		spanner.Update("Messages", []string{"id", "processed", "subStatus", "updatedAt"}, []any{id, true, string(b), spanner.CommitTimestamp}),
	})
	if err != nil {
		glog.Errorf("[Acknowledge]: Failed to update message sub status in Spanner: %v", err)
		return err
	}

	glog.Infof("[Acknowledge]: Updated message sub status in spanner with msgId=%s, subStatus=%v", id, string(b))
	return nil
}

func CheckIfTopicSubscriptionIsCorrect(topicID, subscription string) error {
	glog.Infof("[Subscribe] Checking if subscription exists for topic: %s", topicID)
	subs, err := storage.GetSubscribtionsForTopic(topicID)

	if err != nil {
		glog.Infof("[Subscribe] Topic %s not found in storage", topicID)
		return status.Errorf(codes.NotFound, "Topic %s not found", topicID)
	}

	glog.Infof("[Subscribe] Found subscriptions for topic %s: %v", topicID, subs)

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

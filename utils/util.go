package utils

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/spanner"
	"github.com/alphauslabs/pubsub/handlers"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/flowerinthenight/hedge"
	"github.com/golang/glog"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func EnsureLeaderActive(op *hedge.Op, ctx context.Context) (bool, error) {
	msg := handlers.SendInput{
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

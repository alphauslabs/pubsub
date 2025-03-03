package utils

import (
	"context"
	"encoding/json"

	"cloud.google.com/go/spanner"
	"github.com/alphauslabs/pubsub/handlers"
	"github.com/flowerinthenight/hedge"
	"github.com/golang/glog"
)

func EnsureLeaderActive(op *hedge.Op, ctx context.Context) (bool, error) {
	msg := handlers.SendInput{
		Type: "checkleader",
		Msg:  []byte("PING"),
	}

	b, _ := json.Marshal(msg)
	r, err := hedge.SendToLeader(ctx, op, b, &hedge.SendToLeaderArgs{Retries: 15})
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
		glog.Info("[ERROR]: Received invalid message ID")
		return nil
	}

	// Update the message processed status in Spanner
	_, err := spannerClient.Apply(context.Background(), []*spanner.Mutation{
		spanner.Update("Messages", []string{"id", "processed", "updatedAt"}, []interface{}{id, true, spanner.CommitTimestamp}),
	})
	if err != nil {
		glog.Infof("[ERROR]: Failed to update message processed status in Spanner: %v", err)
		return err
	}

	glog.Infof("[STORAGE]: Updated message processed status in Spanner for ID: %s, Processed: %v", id, true)
	return nil
}

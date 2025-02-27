package utils

import (
	"context"
	"encoding/json"
	"log"

	"cloud.google.com/go/spanner"
	"github.com/alphauslabs/pubsub/send"
	"github.com/flowerinthenight/hedge"
)

func EnsureLeaderActive(op *hedge.Op, ctx context.Context) (bool, error) {
	msg := send.SendInput{
		Type: "checkleader",
		Msg:  []byte("PING"),
	}

	b, _ := json.Marshal(msg)
	r, err := hedge.SendToLeader(ctx, op, b)
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

// NEW METHOD TO UPDATE MESSAGE PROCESSED STATUS IN SPANNER			//transferred to utils from storage
func UpdateMessageProcessedStatus(spannerClient *spanner.Client, id string, processed bool) error {

	// Check for valid ID
	if id == "" {
		log.Println("[ERROR]: Received invalid message ID")
		return nil
	}

	// Update the message processed status in Spanner
	_, err := spannerClient.Apply(context.Background(), []*spanner.Mutation{
		spanner.Update("Messages", []string{"Id", "Processed"}, []interface{}{id, processed}),
	})
	if err != nil {
		log.Printf("[ERROR]: Failed to update message processed status in Spanner: %v", err)
		return err
	}

	log.Printf("[STORAGE]: Updated message processed status in Spanner for ID: %s, Processed: %v", id, processed)

	return nil
}

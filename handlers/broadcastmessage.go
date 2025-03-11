package handlers

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/leader"
	"github.com/golang/glog"
	"google.golang.org/api/iterator"
)

func BroadcastAllMessages(ctx context.Context, app *app.PubSub) {
	stmt := spanner.Statement{
		SQL: `SELECT id, topic, payload 
			  FROM Messages where processed = false`,
	}

	iter := app.Client.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			glog.Infof("[AllMessages] Error reading message: %v", err)
			continue
		}

		var msg pb.Message
		if err := row.Columns(&msg.Id, &msg.Topic, &msg.Payload); err != nil {
			glog.Infof("[AllMessages] Error reading message columns: %v", err)
			continue
		}
		// Structure
		messageInfo := struct {
			ID      string `json:"id"`
			Topic   string `json:"topic"`
			Payload string `json:"payload"`
		}{
			ID:      msg.Id,
			Topic:   msg.Topic,
			Payload: msg.Payload,
		}

		// Marshal message info
		data, err := json.Marshal(messageInfo)
		if err != nil {
			glog.Infof("[BroadcastMessage] Error marshalling message: %v", err)
			continue
		}

		// Create broadcast input
		broadcastInput := BroadCastInput{
			Type: Message,
			Msg:  data,
		}

		// Marshal broadcast input
		broadcastData, err := json.Marshal(broadcastInput)
		if err != nil {
			glog.Infof("[BroadcastMessage] Error marshalling broadcast input: %v", err)
			continue
		}

		// Broadcast
		responses := app.Op.Broadcast(ctx, broadcastData)
		for _, response := range responses {
			if response.Error != nil {
				glog.Infof("[BroadcastMessage] Error broadcasting message: %v", response.Error)
			}
		}
	}

	glog.Info("[BroadcastMessage] All messages broadcast completed.")
}

func LatestMessages(ctx context.Context, app *app.PubSub, t *time.Time) {
	stmt := spanner.Statement{
		SQL: `SELECT id, topic, payload 
							  FROM Messages
							  WHERE processed = FALSE AND createdAt > @lastQueryTime`,
		Params: map[string]interface{}{"lastQueryTime": t},
	}

	iter := app.Client.Single().Query(ctx, stmt)
	defer iter.Stop()
	*t = time.Now().UTC()

	count := 0 // counter for unprocessed messages
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			glog.Infof("[BroadcastMessage] Error reading message: %v", err)
			continue
		}
		count++

		var msg pb.Message
		if err := row.Columns(&msg.Id, &msg.Topic, &msg.Payload); err != nil {
			glog.Infof("[BroadcastMessage] Error reading message columns: %v", err)
			continue
		}

		// Structure
		messageInfo := struct {
			ID      string `json:"id"`
			Topic   string `json:"topic"`
			Payload string `json:"payload"`
		}{
			ID:      msg.Id,
			Topic:   msg.Topic,
			Payload: msg.Payload,
		}

		// Marshal message info
		data, err := json.Marshal(messageInfo)
		if err != nil {
			glog.Infof("[BroadcastMessage] Error marshalling message: %v", err)
			continue
		}

		// Create broadcast input
		broadcastInput := BroadCastInput{
			Type: Message,
			Msg:  data,
		}

		// Marshal broadcast input
		broadcastData, err := json.Marshal(broadcastInput)
		if err != nil {
			glog.Infof("[BroadcastMessage] Error marshalling broadcast input: %v", err)
			continue
		}

		// Broadcast
		responses := app.Op.Broadcast(ctx, broadcastData)
		for _, response := range responses {
			if response.Error != nil {
				glog.Infof("[BroadcastMessage] Error broadcasting message: %v", response.Error)
			}
		}
	}
	if count == 0 {
		glog.Info("[BroadcastMessage] No new unprocessed messages found.")
	}
}

func StartBroadcastMessages(ctx context.Context, app *app.PubSub) {
	tick := time.NewTicker(2 * time.Second) // check every 2 seconds
	defer tick.Stop()

	broadcastMsg := SendInput{
		Type: initialmsgsfetch,
		Msg:  []byte{},
	}

	// Ask the leader to trigger a broadcast of all messages
	bin, _ := json.Marshal(broadcastMsg)
	_, err := app.Op.Send(ctx, bin)
	if err != nil {
		glog.Errorf("[Broadcast messages] sending request to leader: %v", err)
		return
	}

	lastQueryTime := time.Now().UTC()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if atomic.LoadInt32(&leader.IsLeader) == 1 { // Check if leader
				LatestMessages(ctx, app, &lastQueryTime)
			}
		}
	}
}

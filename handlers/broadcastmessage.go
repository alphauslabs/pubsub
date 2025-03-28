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
	"github.com/alphauslabs/pubsub/storage"
	"github.com/golang/glog"
	"google.golang.org/api/iterator"
)

type Raw struct {
	Id         string             `spanner:"id"`
	Topic      string             `spanner:"topic"`
	Payload    string             `spanner:"payload"`
	Attributes spanner.NullString `spanner:"attributes"`
	SubStatus  spanner.NullString `spanner:"subStatus"`
}

func BroadcastAllMessages(ctx context.Context, app *app.PubSub) {
	stmt := spanner.Statement{
		SQL: `SELECT id, topic, payload, attributes, subStatus
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
			glog.Errorf("[AllMessages] Error reading message: %v", err)
			continue
		}

		var msg Raw

		err = row.ToStruct(&msg)
		if err != nil {
			glog.Error(err)
			continue
		}

		attr := make(map[string]string)
		if msg.Attributes.Valid && msg.Attributes.StringVal != "" {
			err = json.Unmarshal([]byte(msg.Attributes.StringVal), &attr)
			if err != nil {
				glog.Errorf("[BroadcastMessage] Error unmarshalling attributes: %v", err)
				continue
			}
		}

		subStatus := make(map[string]bool)
		if msg.SubStatus.Valid && msg.SubStatus.StringVal != "" {
			err = json.Unmarshal([]byte(msg.SubStatus.StringVal), &subStatus)
			if err != nil {
				glog.Errorf("[BroadcastMessage] Error unmarshalling subStatus: %v", err)
				continue
			}
		}

		subss := make(map[string]*storage.Subs)
		for k, v := range subStatus {
			var done int32
			if v {
				done = 1
			}
			subss[k] = &storage.Subs{
				SubscriptionID: k,
				Deleted:        done,
			}
		}

		m := storage.Message{
			Message: &pb.Message{
				Id:         msg.Id,
				Topic:      msg.Topic,
				Payload:    msg.Payload,
				Attributes: attr,
			},
			Subscriptions: subss,
		}

		// Marshal message info
		data, err := json.Marshal(&m)
		if err != nil {
			glog.Errorf("[BroadcastMessage] Error marshalling message: %v", err)
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
			glog.Errorf("[BroadcastMessage] Error marshalling broadcast input: %v", err)
			continue
		}

		// Broadcast
		responses := app.Op.Broadcast(ctx, broadcastData)
		for _, response := range responses {
			if response.Error != nil {
				glog.Errorf("[BroadcastMessage] Error broadcasting message: %v", response.Error)
			}
		}
	}

	glog.Info("[BroadcastMessage] All messages broadcast completed.")
}

func LatestMessages(ctx context.Context, app *app.PubSub, t *time.Time) {
	current := time.Now().UTC()
	stmt := spanner.Statement{
		SQL: `SELECT id, topic, payload, attributes, subStatus
			  FROM Messages
			  WHERE processed = FALSE AND createdAt > @lastQueryTime`,
		Params: map[string]any{"lastQueryTime": t},
	}

	iter := app.Client.Single().Query(ctx, stmt)
	defer iter.Stop()

	count := 0 // counter for unprocessed messages
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			glog.Errorf("[BroadcastMessage] Error reading message: %v", err)
			continue
		}
		count++

		var msg Raw
		err = row.ToStruct(&msg)
		if err != nil {
			glog.Error(err)
			continue
		}

		attr := make(map[string]string)
		if msg.Attributes.Valid && msg.Attributes.StringVal != "" {
			err = json.Unmarshal([]byte(msg.Attributes.StringVal), &attr)
			if err != nil {
				glog.Errorf("[BroadcastMessage] Error unmarshalling attributes: %v", err)
				continue
			}
		}

		subStatus := make(map[string]bool)
		if msg.SubStatus.Valid && msg.SubStatus.StringVal != "" {
			err = json.Unmarshal([]byte(msg.SubStatus.StringVal), &subStatus)
			if err != nil {
				glog.Errorf("[BroadcastMessage] Error unmarshalling subStatus: %v", err)
				continue
			}
		}

		subss := make(map[string]*storage.Subs)
		for k, v := range subStatus {
			var done int32
			if v {
				done = 1
			}
			subss[k] = &storage.Subs{
				SubscriptionID: k,
				Deleted:        done,
			}
		}

		m := storage.Message{
			Message: &pb.Message{
				Id:         msg.Id,
				Topic:      msg.Topic,
				Payload:    msg.Payload,
				Attributes: attr,
			},
			Subscriptions: subss,
		}

		// Marshal message info
		data, err := json.Marshal(&m)
		if err != nil {
			glog.Errorf("[BroadcastMessage] Error marshalling message: %v", err)
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
			glog.Errorf("[BroadcastMessage] Error marshalling broadcast input: %v", err)
			continue
		}

		// Broadcast
		responses := app.Op.Broadcast(ctx, broadcastData)
		for _, response := range responses {
			if response.Error != nil {
				glog.Errorf("[BroadcastMessage] Error broadcasting message: %v", response.Error)
			}
		}
	}
	if count > 0 {
		*t = current
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

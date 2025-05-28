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
	"github.com/alphauslabs/pubsub/utils"
	"github.com/flowerinthenight/hedge/v2"
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
			  FROM pubsub_messages where processed = false`,
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

		subss := make(map[string]*storage.MsgSub)
		for k, v := range subStatus {
			var done int32
			if v {
				done = 1
			}
			subss[k] = &storage.MsgSub{
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
		node := utils.WhatNode(string(msg.Id[0]), storage.RecordMap)
		broadcastData, err := json.Marshal(broadcastInput)
		if err != nil {
			glog.Errorf("[BroadcastMessage] Error marshalling broadcast input: %v", err)
			continue
		}

		outs := app.Op.Broadcast(ctx, broadcastData, hedge.BroadcastArgs{
			OnlySendTo: []string{node}, // only send to this node
		})
		for _, out := range outs {
			if out.Error != nil {
				glog.Errorf("[BroadcastMessage] Error broadcasting message: %v", out.Error)
			}
		}
	}

	glog.Info("[BroadcastMessage] All messages broadcast completed.")
}

func LatestMessages(ctx context.Context, app *app.PubSub, t *time.Time) {
	current := time.Now().UTC()
	stmt := spanner.Statement{
		SQL: `SELECT id, topic, payload, attributes, subStatus
			  FROM pubsub_messages
			  WHERE processed = FALSE AND createdAt > @lastQueryTime
			  ORDER BY createdAt ASC`,
		Params: map[string]any{"lastQueryTime": *t},
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

		pres := make([]string, 0)
		subss := make(map[string]*storage.MsgSub)
		for k, v := range subStatus {
			var done int32
			if v {
				done = 1
			}
			subss[k] = &storage.MsgSub{
				SubscriptionID: k,
				Deleted:        done,
			}
			pres = append(pres, string(k[0]))
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

		n := utils.GetSubNodeHandlers(pres, storage.RecordMap)
		// Broadcast
		responses := app.Op.Broadcast(ctx, broadcastData, hedge.BroadcastArgs{
			OnlySendTo: n, // only send to these nodes
		})
		for _, response := range responses {
			if response.Error != nil {
				glog.Errorf("[BroadcastMessage] Error broadcasting message: %v", response.Error)
			}
		}
	}

	// Always update the timestamp regardless of whether messages were found
	// This ensures we don't repeatedly query the same time range
	*t = current
	glog.Infof("[BroadcastMessage] count: %v", count)

	if count > 0 {
		glog.Infof("[BroadcastMessage] Processed %d new messages", count)
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

// func placeholder() {
// 	for _, msg := range storage.TopicMessages {
// 		all := msg.GetAll()
// 		for _, m := range all {
// 			// send m to all the requesting node
// 		}
// 	}
// }

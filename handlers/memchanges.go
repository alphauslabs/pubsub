package handlers

import (
	"context"
	"time"

	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/alphauslabs/pubsub/utils"
	"github.com/golang/glog"
)

func MemberChanges(data any, msg []byte) ([]byte, error) {
	ap := data.(*app.PubSub)
	res := utils.CreateRecordMapping(ap)
	storage.SetRecordMap(res)
	err := utils.BroadcastRecord(ap, storage.RecordMap)
	if err != nil {
		glog.Errorf("BroadcastRecord error: %v", err)
		return nil, err
	}
	utils.NotifyLeaderForTopicSubBroadcast(context.Background(), ap.Op)
	time.Sleep(2 * time.Second)
	utils.NotifyLeaderForAllMessageBroadcast(context.Background(), ap.Op)
	return nil, nil
}

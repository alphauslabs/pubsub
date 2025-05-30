package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang/glog"
)

func MonitorMessages(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	do := func() {
		var topicsubMsgCounts = make(map[string]int)
		var topicSubDetails = make(map[string]int)

		TopicMsgMu.RLock()
		for topic, msgs := range TopicMessages {
			for _, msg := range msgs.GetAll() {
				if msg.IsFinalDeleted() {
					continue
				}
				for _, sub := range msg.Subscriptions {
					k := fmt.Sprintf("%s_%s", topic, sub.SubscriptionID)
					if sub.IsDeleted() {
						continue
					}
					if m, ok := topicsubMsgCounts[k]; !ok {
						topicsubMsgCounts[k] = 1
					} else {
						topicsubMsgCounts[k] = m + 1
					}
				}

			}

		}
		TopicMsgMu.RUnlock()

		topicSubsMu.RLock()
		for topic, subs := range topicSubs {
			topicSubDetails[topic] = len(subs)
		}
		topicSubsMu.RUnlock()

		if len(topicSubDetails) != 0 {
			b, _ := json.Marshal(topicSubDetails)
			glog.Infof("[Storage Monitor] Topic-Subscription data: %s", string(b))
		}
		if len(topicsubMsgCounts) != 0 {
			b, _ := json.Marshal(topicsubMsgCounts)
			glog.Infof("[Storage Monitor] Topic_sub-Messages data: %s", string(b))
		}
	}

	do() // trigger first do
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			do()
		}
	}
}

func MonitorRecordMap(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	do := func() {
		RecordMapMu.RLock()
		defer RecordMapMu.RUnlock()

		if len(RecordMap) == 0 {
			glog.Info("[Storage Monitor] RecordMap is empty")
		} else {
			for k, v := range RecordMap {
				glog.Infof("[Storage Monitor] RecordMap entry: %s -> %s", k, v)
			}
		}
	}

	do() // trigger first do
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			do()
		}
	}
}

package storage

import (
	"context"
	"encoding/json"
	"time"

	"github.com/golang/glog"
)

func MonitorActivity(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	do := func() {
		var topicMsgCounts = make(map[string]int)
		var topicSubDetails = make(map[string]int)

		TopicMsgMu.RLock()
		for topic, msgs := range TopicMessages {
			count := 0
			for _, msg := range msgs.GetAll() {
				if !msg.IsFinalDeleted() {
					count++
				}
			}
			topicMsgCounts[topic] = count
		}
		TopicMsgMu.RUnlock()

		topicSubsMu.RLock()
		for topic, subs := range topicSubs {
			topicSubDetails[topic] = len(subs)
		}
		topicSubsMu.RUnlock()

		if len(topicSubDetails) == 0 {
			glog.Info("[Storage Monitor] No topic-subscription data available")
		} else {
			b, _ := json.Marshal(topicSubDetails)
			glog.Infof("[Storage Monitor] Topic-Subscription data: %s", string(b))
		}

		if len(topicMsgCounts) == 0 {
			glog.Info("[Storage Monitor] No Messages available")
		} else {
			b, _ := json.Marshal(topicMsgCounts)
			glog.Infof("[Storage Monitor] Topic-Messages data: %s", string(b))
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

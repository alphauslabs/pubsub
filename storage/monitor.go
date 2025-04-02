package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/golang/glog"
)

func MonitorActivity(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	do := func() {
		var topicsubMsgCountsdeleted = make(map[string]int)
		var topicsubMsgCountslocked = make(map[string]int)
		var topicSubDetails = make(map[string]int)

		TopicMsgMu.RLock()
		for topic, msgs := range TopicMessages {
			count := 0
			count1 := 0
			for _, msg := range msgs.GetAll() {
				if msg.IsFinalDeleted() {
					continue
				}
				for _, sub := range msg.Subscriptions {
					if sub.IsDeleted() {
						count++
					}
					if sub.IsLocked() {
						count1++
					}
					k := fmt.Sprintf("%s_%s", topic, sub.SubscriptionID)
					topicsubMsgCountsdeleted[k] = count
					topicsubMsgCountslocked[k] = count1
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

		if len(topicsubMsgCountsdeleted) != 0 || len(topicsubMsgCountslocked) != 0 {
			b, _ := json.Marshal(topicsubMsgCountsdeleted)
			glog.Infof("[Storage Monitor] Topic-sub-Messages data (deleted): %s", string(b))
			b, _ = json.Marshal(topicsubMsgCountslocked)
			glog.Infof("[Storage Monitor] Topic_sub-Messages data (deleted): %s", string(b))
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

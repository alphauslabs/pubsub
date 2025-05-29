package sweep

import (
	"context"
	"time"

	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/alphauslabs/pubsub/utils"
	"github.com/golang/glog"
)

func RunCheckForExpired(ctx context.Context) {
	glog.Info("[sweep] check for expired messages started")
	sweep := func() {
		storage.TopicMsgMu.RLock()
		defer storage.TopicMsgMu.RUnlock()
		for _, v := range storage.TopicMessages {
			v.Mu.RLock()
			for _, v1 := range v.Messages {
				if !v1.IsFinalDeleted() {
					v1.Mu.RLock()
					count := 0
					for _, t := range v1.Subscriptions {
						if t.IsDeleted() {
							count++
							continue
						}

						if t.Age.IsZero() {
							continue
						}
						switch {
						case time.Since(t.Age).Seconds() >= 30 && !t.IsAutoExtend():
							glog.Infof("[sweep] unlocked message: %v, sub: %v", v1.Id, t.SubscriptionID)
							t.Unlock()
							t.ClearAge()
						case time.Since(t.Age).Seconds() >= 30 && t.IsAutoExtend():
							glog.Infof("[sweep] auto-extend message: %v, sub: %v", v1.Id, t.SubscriptionID)
							t.RenewAge()
						}
					}
					if count == len(v1.Subscriptions) {
						v1.MarkAsFinalDeleted()
						glog.Info("[sweep] set to final deleted message:", v1.Id)
					}
					v1.Mu.RUnlock()
				}
			}
			v.Mu.RUnlock()
		}
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}

func RunCheckForDeleted(ctx context.Context, app *app.PubSub) {
	glog.Info("[sweep] check for deleted messages started")
	sweep := func() {
		storage.TopicMsgMu.RLock()
		defer storage.TopicMsgMu.RUnlock()
		for _, v := range storage.TopicMessages {
			tobedeleted := make(map[string]struct{})
			v.Mu.Lock()
			for _, v1 := range v.Messages {
				if v1.IsFinalDeleted() {
					tobedeleted[v1.Id] = struct{}{}
				}
			}

			for k := range tobedeleted {
				if err := utils.UpdateMessageProcessedStatus(app.Client, k); err != nil {
					glog.Errorf("[sweep] error updating message %s processed status: %v", k, err)
				} else {
					delete(v.Messages, k)
					glog.Infof("[sweep] removed from memory message: %v", k)
				}
			}
			v.Mu.Unlock()
		}
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}

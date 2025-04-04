package sweep

import (
	"context"
	"sync/atomic"
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
						case time.Since(t.Age).Seconds() >= 30 && atomic.LoadInt32(&t.AutoExtend) == 0:
							t.Unlock()
							t.ClearAge()
						case time.Since(t.Age).Seconds() >= 30 && atomic.LoadInt32(&t.AutoExtend) == 1:
							t.RenewAge()
						}
					}
					if count == len(v1.Subscriptions) {
						v1.MarkAsFinalDeleted()
						glog.Info("[sweep] set to final deleted message:", v1.Id)
					} else {

						glog.Infof("[sweep] message=%v is not final deleted, count=%v, subs=%v", v1.Id, count, len(v1.Subscriptions))
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
			v.Mu.RLock()
			for _, v1 := range v.Messages {
				if v1.IsFinalDeleted() {
					// Update the processed status in Spanner
					if err := utils.UpdateMessageProcessedStatus(app.Client, v1.Id); err != nil {
						glog.Errorf("[sweep] error updating message %s processed status: %v", v1.Id, err)
					}

					delete(v.Messages, v1.Id)
					glog.Info("[sweep] deleted message:", v1.Id)
				}
			}
			v.Mu.RUnlock()
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

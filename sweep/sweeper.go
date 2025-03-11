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
	glog.Info("[sweep] run check for expired messages started")
	sweep := func() {
		for _, v := range storage.TopicMessages {
			v.Mu.Lock()
			for _, v1 := range v.Messages {
				if atomic.LoadInt32(&v1.FinalDeleted) == 0 {
					v1.Mu.Lock()
					count := 0
					for s, t := range v1.Subscriptions {
						if atomic.LoadInt32(&t.Deleted) == 1 {
							count++
							continue
						}
						if t.Age.IsZero() {
							continue
						}
						switch {
						case time.Duration(time.Since(t.Age).Seconds()) >= 30*time.Second && atomic.LoadInt32(&t.AutoExtend) == 0:
							glog.Infof("[sweep] message %s subscription %s expired. Unlocking...", v1.Id, s)
							t.Unlock()
							t.ClearAge()
						case time.Duration(time.Since(t.Age).Seconds()) >= 30*time.Second && atomic.LoadInt32(&t.AutoExtend) == 1:
							t.RenewAge()
						}
					}
					if count == len(v1.Subscriptions) {
						atomic.StoreInt32(&v1.FinalDeleted, 1)
						glog.Info("[sweep] set to final deleted message:", v1.Id)
					}
					v1.Mu.Unlock()
				}
			}
			v.Mu.Unlock()
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
	glog.Info("[sweep] run check for deleted messages started")
	sweep := func() {
		for _, v := range storage.TopicMessages {
			for _, v1 := range v.Messages {
				if atomic.LoadInt32(&v1.FinalDeleted) == 1 {
					// Update the processed status in Spanner
					if err := utils.UpdateMessageProcessedStatus(app.Client, v1.Id); err != nil {
						glog.Errorf("[sweep] error updating message %s processed status: %v", v1.Id, err)
					}

					delete(v.Messages, v1.Id)
					glog.Info("[sweep] deleted message:", v1.Id)
				}
			}
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

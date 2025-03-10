package sweep

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/alphauslabs/pubsub/storage"
	"github.com/golang/glog"
)

func RunCheckForExpired(ctx context.Context) {
	glog.Info("[sweep] run check for expired messages started")
	sweep := func() {
		for _, v := range storage.TopicMessages {
			for _, v1 := range v.Messages {
				if atomic.LoadInt32(&v1.FinalDeleted) == 0 {
					v1.Mu.Lock()
					count := 0
					for _, t := range v1.Subscriptions {
						if atomic.LoadInt32(&t.Deleted) == 1 {
							count++
							continue
						}
						if t.Age.IsZero() {
							continue
						}
						switch {
						case time.Since(t.Age) >= 30*time.Second && atomic.LoadInt32(&t.AutoExtend) == 0:
							t.Reset()
						case time.Since(t.Age) >= 30*time.Second && atomic.LoadInt32(&t.AutoExtend) == 1:
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

func RunCheckForDeleted(ctx context.Context) {
	glog.Info("[sweep] run check for deleted messages started")
	sweep := func() {
		for _, v := range storage.TopicMessages {
			for _, v1 := range v.Messages {
				if atomic.LoadInt32(&v1.FinalDeleted) == 1 {
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

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
				if atomic.LoadInt32(&v1.Deleted) == 0 && atomic.LoadInt32(&v1.Locked) == 1 {
					v1.Mu.Lock()
					switch {
					case time.Since(v1.Age) >= 30*time.Second && atomic.LoadInt32(&v1.AutoExtend) == 0:
						atomic.StoreInt32(&v1.Locked, 0) // release lock
					case time.Since(v1.Age) >= 30*time.Second && atomic.LoadInt32(&v1.AutoExtend) == 1:
						v1.Age = time.Now().UTC() // extend lock
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
				if atomic.LoadInt32(&v1.Deleted) == 1 {
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

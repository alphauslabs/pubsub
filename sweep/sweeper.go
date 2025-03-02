package sweep

import (
	"sync/atomic"
	"time"

	"github.com/alphauslabs/pubsub/storage"
	"github.com/golang/glog"
)

func Run() {
	sweep := func() {
		for _, v := range storage.TopicMessages {
			for _, v1 := range v.Messages {
				if atomic.LoadInt32(&v1.Locked) == 1 {
					switch {
					case time.Since(v1.Age) > 30*time.Second && atomic.LoadInt32(&v1.AutoExtend) == 0:
						atomic.StoreInt32(&v1.Locked, 0) // release lock
					case time.Since(v1.Age) > 30*time.Second && atomic.LoadInt32(&v1.AutoExtend) == 1:
						v1.Age = time.Now().UTC() // extend lock
					}
				}
			}
		}
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case t := <-ticker.C:
			sweep()
			glog.Infof("[sweep] time: %v", t.Second())
		}
	}
}

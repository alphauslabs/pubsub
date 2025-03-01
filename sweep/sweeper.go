package sweep

import (
	"sync/atomic"
	"time"

	"github.com/golang/glog"
)

type message struct { // sample only
	Payload    string
	Time       time.Time
	Topic      string
	Locked     int32
	AutoExtend int32
}

var m = make(map[string][]*message)

func Run() {
	sweep := func() {
		for _, v := range m {
			for _, v1 := range v {
				if atomic.LoadInt32(&v1.Locked) == 1 {
					switch {
					case time.Since(v1.Time) > 30*time.Second && atomic.LoadInt32(&v1.AutoExtend) == 0:
						atomic.StoreInt32(&v1.Locked, 0) // release lock
					case time.Since(v1.Time) > 30*time.Second && atomic.LoadInt32(&v1.AutoExtend) == 1:
						v1.Time = time.Now().UTC() // extend lock
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

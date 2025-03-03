package leader

import (
	"context"
	"log"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/alphauslabs/pubsub/app"
	"github.com/golang/glog"
)

var IsLeader int32

func LeaderCallBack(d interface{}, msg []byte) {
	o := d.(*app.PubSub)
	s := strings.Split(string(msg), "")
	v, err := strconv.Atoi(s[0])
	if err != nil {
		log.Fatalf("failed to convert string to int: %v", err)
	}
	atomic.StoreInt32(&IsLeader, int32(v))
	res := o.Op.Broadcast(context.Background(), msg)
	for _, r := range res {
		if r.Error != nil {
			glog.Errorf("Error broadcasting to %s: %v", r.Id, r.Error)
		}
	}
}

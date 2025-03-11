package app

import (
	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge"
	"github.com/flowerinthenight/timedoff"
)

type PubSub struct {
	Op           *hedge.Op
	Client       *spanner.Client
	LeaderActive *timedoff.TimedOff
}

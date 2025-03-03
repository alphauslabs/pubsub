package app

import (
	"sync"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge"
	"github.com/flowerinthenight/timedoff"
)

type PubSub struct {
	Op            *hedge.Op
	Client        *spanner.Client
	NodeID        string
	MessageLocks  sync.Map // messageID -> MessageLockInfo
	MessageTimer  sync.Map // messageID -> *time.Timer
	ConsensusMode string
	LeaderActive  *timedoff.TimedOff
}

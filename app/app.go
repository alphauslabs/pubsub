package app

import (
	"sync"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge"
)

type PubSub struct {
	Op           *hedge.Op
	Client       *spanner.Client
	NodeID       string
	MessageLocks sync.Map   // messageID -> MessageLockInfo
	MessageTimer sync.Map   // messageID -> *time.Timer
	Mutex        sync.Mutex // app level mutex
}

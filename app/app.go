package app

import (
	"sync"

	"cloud.google.com/go/spanner"
	storage "github.com/alphauslabs/pubsub/storage"
	"github.com/flowerinthenight/hedge"
)

type PubSub struct {
	Op            *hedge.Op
	Client        *spanner.Client
	Storage       *storage.Storage
	NodeID        string
	MessageLocks  sync.Map   // messageID -> MessageLockInfo
	MessageTimer  sync.Map   // messageID -> *time.Timer
	Mutex         sync.Mutex // app level mutex
	ConsensusMode string
}

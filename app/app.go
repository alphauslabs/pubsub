package app

import (
	"cloud.google.com/go/spanner"
	storage "github.com/alphauslabs/pubsub/storage"
	"github.com/flowerinthenight/hedge/v2"
	"sync"
)

type PubSub struct {
	Op      *hedge.Op
	Client  *spanner.Client
	Storage *storage.Storage
	NodeID       string
	MessageLocks sync.Map
	MessageQueue sync.Map
	Mutex        sync.Mutex
}

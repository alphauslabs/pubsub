package app

import (
	"cloud.google.com/go/spanner"
	storage "github.com/alphauslabs/pubsub/storage"
	"github.com/flowerinthenight/hedge"
)

type PubSub struct {
	Op      *hedge.Op
	Client  *spanner.Client
	Storage *storage.Storage
}

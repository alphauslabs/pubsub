package app

import (
	"cloud.google.com/go/spanner"
	storage "github.com/alphauslabs/pubsub/node_storage"
	"github.com/flowerinthenight/hedge/v2"
)

type PubSub struct {
	Op      *hedge.Op
	Client  *spanner.Client
	storage *storage.Storage
}

package main

import (
	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge/v2"
)

type PubSub struct {
	Op     *hedge.Op
	Client *spanner.Client
}

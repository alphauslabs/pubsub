package main

import (
	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge/v2"
	"sync"
)

type PubSub struct {
	Op     *hedge.Op
	Client *spanner.Client
	 // Message handling
	 messageLocks    sync.Map    // messageID -> MessageLockInfo
	 messageQueue    sync.Map    // topic -> []*pb.Message
	 subscriptions  sync.Map    // subscriptionID -> *pb.Subscription
	 
	 // Timer tracking
	 timeoutTimers  sync.Map    // messageID -> *time.Timer
	 storage         *storage.Storage // jansen's storage
}

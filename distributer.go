package main

import (
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/flowerinthenight/hedge/v2"
)

var lastCheckedTime time.Time // track the last Spanner query time to fetch only new updates

// StartDistributor initializes and starts the topic-subscription distributor
func StartDistributor(op *hedge.Op, spannerClient *spanner.Client) {
	go distributeStruct(op, spannerClient)
}

// leader fetches and broadcasts topic-subs updates every 10 seconds
func distributeStruct(op *hedge.Op, spannerClient *spanner.Client) {
	lastCheckedTime = time.Now().Add(-10 * time.Second) // Start 10 seconds earlier
	ticker := time.NewTicker(10 * time.Second)          // ticker for periodic execution
	defer ticker.Stop()

	for range ticker.C {
		// check if this node is the leader
		l, _ := op.HasLock()
		if l {
			log.Println("Leader: Fetching and broadcasting topic-subscription updates...")
			topicSub := fetchUpdatedTopicSub(spannerClient, &lastCheckedTime)
			broadcastTopicSubStruct(op, topicSub)
		} else {
			log.Println("Follower: No action needed.")
		}
	}
}

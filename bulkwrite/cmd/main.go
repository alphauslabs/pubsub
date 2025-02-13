package main

import (
	"flag"
	"log"
	"time"
)

var (
	isLeader = flag.Bool("leader", false, "Run this node as the leader for bulk writes")
)

func main() {
	flag.Parse()

	if *isLeader {
		log.Println("Running as LEADER.")
		// Start listening for messages from the publisher.
		go startPublisherListener()

		// Start leader-specific listener to accept forwarded messages from non-leader nodes.
		go startLeaderMessageListener()

		// Process aggregated messages and perform bulk write.
		runBulkWriterAsLeader()
	} else {
		log.Println("Running as NON-LEADER.")
		// Start listening for messages from the publisher.
		go startPublisherListener()

		// Instead of writing to Spanner, forward the messages to the leader.
		runBulkWriterAsFollower()
	}
	
	// Prevent main from exiting.
	select {}
}

// startPublisherListener simulates listening for publisher messages.
func startPublisherListener() {
	// Pseudocode: Listen to publisher's gRPC endpoint.
	log.Println("Listening for publisher messages...")
	// Imagine this function continuously receives messages.
}

// startLeaderMessageListener simulates a gRPC server for receiving messages from followers.
func startLeaderMessageListener() {
	// Pseudocode: Start a gRPC server on a designated port for follower nodes.
	log.Println("Leader is now listening for forwarded messages from followers...")
	// Implement gRPC endpoint here.
}

// runBulkWriterAsLeader handles bulk writing as a leader.
func runBulkWriterAsLeader() {
	// Pseudocode: Aggregate messages from publisher and followers.
	// Use batching logic, e.g., flush batch every N messages or every T seconds.
	for {
		// Simulate message aggregation.
		log.Println("Leader aggregating messages for bulk write...")
		time.Sleep(5 * time.Second)
		// Simulate bulk write to Spanner.
		log.Println("Leader performing bulk write to Spanner.")
	}
}

// runBulkWriterAsFollower forwards messages to the leader.
func runBulkWriterAsFollower() {
	// Pseudocode: Forward received messages to leader's gRPC endpoint.
	for {
		// Simulate receiving a message.
		log.Println("Non-leader received a message from publisher, forwarding to leader...")
		// Forward logic: Dial leader's gRPC endpoint and send the message.
		time.Sleep(3 * time.Second)
	}
}

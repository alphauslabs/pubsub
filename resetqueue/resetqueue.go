package main

import (
	"log"

	"github.com/alphauslabs/pubsub/storage" // Replace with your actual module path
)

func main() {
	log.Println("Resetting message queue...")
	storage.ClearAllMessages() // Call the reset function
}

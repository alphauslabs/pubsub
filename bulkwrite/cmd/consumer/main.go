package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"time"
	"fmt"
)

var (
	nodeURL = flag.String("node-url", "http://localhost:8081", "URL of the node to send messages to")
)

func main() {
	flag.Parse()

	for i := 0; i < 10; i++ {
		// Simulate a message based on the table schema.
		message := map[string]interface{}{
			"id":          "-", // Unique message ID
			"subsription": "bulkwrite-sample", // Subscription ID
			"payload":     fmt.Sprintf("BULKWRITE PAYLOAD NO. %d out of 10 (20ms per message - 1 second to aggregate from [LEADER] NODE )", i), // Payload with index
			"createdAt":   time.Now().Format(time.RFC3339), // Current timestamp
			"updatedAt":   time.Now().Format(time.RFC3339), // Current timestamp
		}

		// Convert the message to JSON.
		jsonData, err := json.Marshal(message)
		if err != nil {
			log.Printf("Error marshalling message: %v\n", err)
			continue
		}

		// Send the message to the node.
		resp, err := http.Post(*nodeURL+"/receive", "application/json", bytes.NewBuffer(jsonData))
		if err != nil {
			log.Printf("Error sending message to node: %v\n", err)
			continue
		}
		defer resp.Body.Close()

		// Log the response.
		log.Printf("Message sent to node, response: %s\n", resp.Status)

		// Wait before sending the next message.
		time.Sleep(20 * time.Millisecond)
	}
}
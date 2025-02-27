package common

// import (
// 	"context"
// 	"fmt"
// 	"log"
// 	"time"

// 	"cloud.google.com/go/spanner"
// )

// const (
// 	database = "projects/labs-169405/instances/alphaus-dev/databases/main"
// 	table    = "Messages"
// )

// func main() {

// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	defer cancel()

// 	// Initialize Spanner client
// 	client, err := spanner.NewClient(ctx, database)
// 	if err != nil {
// 		log.Fatalf("Failed to create Spanner client: %v", err)
// 	}
// 	defer client.Close()

// 	log.Println("Connected to Spanner successfully.")

// 	// Start data insertion
// 	insertData(ctx, client)
// }

// func insertData(ctx context.Context, client *spanner.Client) {

// 	mutations := []*spanner.Mutation{}

// 	// Track message counts
// 	topicCounter := make(map[string]int)
// 	processedCounter := make(map[string]int)

// 	// Create 100 messages
// 	for i := 1; i <= 100; i++ {
// 		// Cycle through topics 1-10
// 		topicNum := ((i - 1) % 10) + 1
// 		topic := fmt.Sprintf("topic%d", topicNum)

// 		// First 80 messages unprocessed, last 20 processed
// 		processed := i > 80

// 		// Create mutation
// 		mutations = append(mutations, spanner.Insert(table,
// 			// Column names
// 			[]string{
// 				"id",                // Message ID
// 				"payload",           // Message content
// 				"topic",             // Topic name
// 				"processed",         // Processing status
// 				"createdAt",         // Creation timestamp
// 				"updatedAt",         // Update timestamp
// 				"visibilityTimeout", // Visibility timeout
// 			},
// 			// Values for each column
// 			[]interface{}{
// 				fmt.Sprintf("msg%d", i),                       // Unique message ID (msg1, msg2, etc.)
// 				fmt.Sprintf("test payload for message %d", i), // Test content
// 				topic,      // Topic name (topic1, topic2, etc.)
// 				processed,
// 				time.Now(),
// 				time.Now(),
// 				nil,
// 			}))

// 		// Update counters
// 		topicCounter[topic]++
// 		if processed {
// 			processedCounter[topic]++
// 		}
// 	}

// 	log.Printf("Prepared %d mutations for insertion.", len(mutations))

// 	// Apply mutations
// 	_, err := client.Apply(ctx, mutations)
// 	if err != nil {
// 		log.Fatalf("Failed to insert data: %v", err)
// 	}

// 	log.Println("Sample data inserted successfully into Messages table.")

// 	// Print summary of messages per topic
// 	for topic, total := range topicCounter {
// 		processed := processedCounter[topic]
// 		unprocessed := total - processed
// 		log.Printf("%s: %d total messages (%d unprocessed, %d processed)",
// 			topic, total, unprocessed, processed)
// 	}
// }

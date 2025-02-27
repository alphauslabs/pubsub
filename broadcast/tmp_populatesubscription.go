package broadcast

// import (
// 	"context"
// 	"flag"
// 	"fmt"
// 	"log"
// 	"time"

// 	"cloud.google.com/go/spanner"
// )

// const (
// 	database = "projects/labs-169405/instances/alphaus-dev/databases/main"
// 	table    = "Subscriptions"
// )

// func main() {
// 	// Define flags for number of runs and interval between runs
// 	runs := flag.Int("runs", 1, "Number of times to insert unique subscriptions into Spanner")
// 	interval := flag.Int("interval", 5, "Interval (in seconds) between runs")
// 	flag.Parse()

// 	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 	defer cancel()

// 	client, err := spanner.NewClient(ctx, database)
// 	if err != nil {
// 		log.Fatalf("Failed to create Spanner client: %v", err)
// 	}
// 	defer client.Close()

// 	log.Println("Connected to Spanner successfully.")

// 	// Track unique subscription names across multiple runs
// 	uniqueNames := make(map[string]bool)

// 	// Track the last used sequential ID
// 	lastID := getLastID(ctx, client) // Fetches the highest existing ID from Spanner

// 	for run := 1; run <= *runs; run++ {
// 		log.Printf("Run %d/%d: Inserting data...\n", run, *runs)
// 		lastID = insertData(ctx, client, uniqueNames, lastID)

// 		// Sleep between runs, except after the last run
// 		if run < *runs {
// 			log.Printf("Waiting for %d seconds before next run...\n", *interval)
// 			time.Sleep(time.Duration(*interval) * time.Second)
// 		}
// 	}

// 	log.Println("All runs completed successfully.")
// }

// // Retrieves the highest ID from the database to continue sequential numbering
// func getLastID(ctx context.Context, client *spanner.Client) int {
// 	stmt := spanner.Statement{
// 		SQL: "SELECT MAX(id) FROM " + table,
// 	}

// 	iter := client.Single().Query(ctx, stmt)
// 	defer iter.Stop()

// 	var lastID int64
// 	err := iter.Do(func(row *spanner.Row) error {
// 		return row.Columns(&lastID)
// 	})

// 	if err != nil {
// 		log.Printf("Error retrieving last ID, starting from 0: %v", err)
// 		return 0
// 	}

// 	log.Printf("Last ID in database: %d", lastID)
// 	return int(lastID)
// }

// func insertData(ctx context.Context, client *spanner.Client, uniqueNames map[string]bool, lastID int) int {
// 	mutations := []*spanner.Mutation{}
// 	topicCounter := make(map[string]int)

// 	for t := 1; t <= 10; t++ {
// 		topic := fmt.Sprintf("topic%d", t)
// 		subCount := (t%3)*2 + 1 // Alternates between 1, 3, and 5 subscriptions per topic

// 		for j := 0; j < subCount; j++ {
// 			lastID++ // Increment ID sequentially
// 			name := fmt.Sprintf("subscription%d", lastID)

// 			// Ensure unique names
// 			if uniqueNames[name] {
// 				log.Printf("Skipping duplicate name: %s", name)
// 				continue
// 			}
// 			uniqueNames[name] = true

// 			mutations = append(mutations, spanner.Insert(table,
// 				[]string{"id", "name", "topic", "createdAt", "updatedAt"},
// 				[]interface{}{lastID, name, topic, time.Now(), time.Now()}))

// 			topicCounter[topic]++
// 		}
// 	}

// 	log.Printf("Prepared %d mutations for insertion.", len(mutations))

// 	// Apply mutations in a single batch
// 	_, err := client.Apply(ctx, mutations)
// 	if err != nil {
// 		log.Fatalf("Failed to insert data: %v", err)
// 	}

// 	log.Println("Sample data inserted successfully into Subscriptions table.")

// 	for topic, count := range topicCounter {
// 		log.Printf("%s: %d subscriptions", topic, count)
// 	}

// 	return lastID // Return the updated last ID
// }

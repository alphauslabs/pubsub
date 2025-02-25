package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/option"
)

const (
	database        = "projects/labs-169405/instances/alphaus-dev/databases/main"
	table           = "Subscriptions"
	credentialsFile = "/home/jennifertongco/service-account.json"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client, err := spanner.NewClient(ctx, database, option.WithCredentialsFile(credentialsFile))
	if err != nil {
		log.Fatalf("Failed to create Spanner client: %v", err)
	}
	defer client.Close()

	log.Println("Connected to Spanner successfully.")

	insertData(ctx, client)
}

func insertData(ctx context.Context, client *spanner.Client) {
	mutations := []*spanner.Mutation{}
	topicCounter := make(map[string]int)

	for i := 1; i <= 100; {
		for t := 1; t <= 10 && i <= 100; t++ {
			topic := fmt.Sprintf("topic%d", t)
			subCount := (t%3)*2 + 1 // Alternates between 1, 3, and 5 subscriptions per topic
			for j := 0; j < subCount && i <= 100; j++ {
				subscription := fmt.Sprintf("subscription%d", i)
				mutations = append(mutations, spanner.Insert(table,
					[]string{"id", "name", "topic", "createdAt", "updatedAt"},
					[]interface{}{fmt.Sprintf("%d", i), subscription, topic, time.Now(), time.Now()}))
				topicCounter[topic]++
				i++
			}
		}
	}

	log.Printf("Prepared %d mutations for insertion.", len(mutations))

	_, err := client.Apply(ctx, mutations)
	if err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}

	log.Println("Sample data inserted successfully into Subscriptions table.")

	for topic, count := range topicCounter {
		log.Printf("%s: %d subscriptions", topic, count)
	}
}

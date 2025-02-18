package topic

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// TABLE STRUCTURE IN SPANNER
type Topic struct {
	TopicID   string    `spanner:"TopicID"`
	Name      string    `spanner:"Name"`
	CreatedAt time.Time `spanner:"CreatedAt"`
	UpdatedAt time.Time `spanner:"UpdatedAt"`
}

// Insert new topic into Topics table
func CreateTopic(ctx context.Context, spannerClient *spanner.Client, topic *Topic) error {
	_, err := spannerClient.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("Topics", []string{"TopicID", "Name", "CreatedAt", "UpdatedAt"},
			[]interface{}{topic.TopicID, topic.Name, spanner.CommitTimestamp, spanner.CommitTimestamp}),
	})
	if err != nil {
		return fmt.Errorf("creating topic: %w", err)
	}
	return nil
}

// Get topic from Topics table
func GetTopic(ctx context.Context, spannerClient *spanner.Client, topicID string) (*Topic, error) {
	stmt := spanner.Statement{
		SQL:    "SELECT TopicID, Name, CreatedAt, UpdatedAt FROM Topics WHERE TopicID = @topicID",
		Params: map[string]interface{}{"topicID": topicID},
	}
	iter := spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return nil, nil // IF No result found
	}
	if err != nil {
		return nil, fmt.Errorf("querying topic: %w", err)
	}

	var topic Topic
	if err := row.ToStruct(&topic); err != nil {
		return nil, fmt.Errorf("unmarshaling topic: %w", err)
	}
	return &topic, nil
}

// updates existing topic's name and updates UpdatedAt timestamp.
func (t *Topic) UpdateTopic(ctx context.Context, spannerClient *spanner.Client) error {
	_, err := spannerClient.Apply(ctx, []*spanner.Mutation{
		spanner.Update("Topics", []string{"TopicID", "Name", "UpdatedAt"},
			[]interface{}{t.TopicID, t.Name, spanner.CommitTimestamp}),
	})
	if err != nil {
		return fmt.Errorf("updating topic: %w", err)
	}
	return nil
}

// remove topic from the database by TopicID.
func DeleteTopic(ctx context.Context, spannerClient *spanner.Client, topicID string) error {
	key := spanner.Key{topicID}
	_, err := spannerClient.Apply(ctx, []*spanner.Mutation{
		spanner.Delete("Topics", spanner.KeySetFromKeys(key)),
	})
	if err != nil {
		return fmt.Errorf("deleting topic: %w", err)
	}
	return nil
}

// List all topics from Topics table
func ListTopics(ctx context.Context, spannerClient *spanner.Client) ([]*Topic, error) {
	sql := "SELECT TopicID, Name, CreatedAt, UpdatedAt FROM Topics"
	stmt := spanner.Statement{SQL: sql}
	iter := spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var topics []*Topic
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing topics: %w", err)
		}
		var topic Topic
		if err := row.ToStruct(&topic); err != nil {
			return nil, fmt.Errorf("unmarshaling topic: %w", err)
		}
		topics = append(topics, &topic)
	}
	return topics, nil
}

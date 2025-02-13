// spanner_operations.go
package spannerops

import (
	"context"
	"fmt"
	"io"

	"cloud.google.com/go/spanner"
	"github.com/google/uuid"
)

// WriteUsingDML inserts a new topic into the Topics table using DML.
func WriteUsingDML(w io.Writer, client *spanner.Client, topicName string) error {
	ctx := context.Background()

	// Generate a unique ID for the topic
	topicID := uuid.New().String()

	// Use the client to perform a DML operation
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL: `INSERT INTO Topics (TopicID, Name, CreatedAt, UpdatedAt)
				  VALUES (@topicID, @name, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())`,
			Params: map[string]interface{}{
				"topicID": topicID,
				"name":    topicName,
			},
		}
		rowCount, err := txn.Update(ctx, stmt)
		if err != nil {
			return fmt.Errorf("failed to execute DML: %v", err)
		}
		fmt.Fprintf(w, "%d record(s) inserted.\n", rowCount)
		return nil
	})
	return err
}

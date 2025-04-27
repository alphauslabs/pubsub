package sweep

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/alphauslabs/pubsub/app"
	"github.com/alphauslabs/pubsub/storage"
	"github.com/alphauslabs/pubsub/utils"
	"github.com/golang/glog"
)

func RunCheckForExpired(ctx context.Context) {
	glog.Info("[sweep] check for expired messages started")
	sweep := func() {
		storage.TopicMsgMu.RLock()
		defer storage.TopicMsgMu.RUnlock()

		for _, topic := range storage.TopicMessages {
			topic.Mu.RLock()

			// First pass: identify messages that need final deletion
			var toMarkDeleted []string
			for msgID, msg := range topic.Messages {
				if msg.IsFinalDeleted() {
					continue
				}

				msg.Mu.RLock()
				deletedCount := 0

				// Check each subscription
				for _, sub := range msg.Subscriptions {
					if sub.IsDeleted() {
						deletedCount++
						continue
					}

					if !sub.Age.IsZero() {
						ageSeconds := time.Since(sub.Age).Seconds()
						if ageSeconds >= 30 {
							if atomic.LoadInt32(&sub.AutoExtend) == 0 {
								sub.ClearAge()
								sub.Unlock()
							} else if atomic.LoadInt32(&sub.AutoExtend) == 1 {
								sub.RenewAge()
							}
						}
					}
				}

				// If all subscriptions are deleted, mark this message
				if deletedCount == len(msg.Subscriptions) {
					toMarkDeleted = append(toMarkDeleted, msgID)
				}

				msg.Mu.RUnlock()
			}

			topic.Mu.RUnlock()

			// Second pass: mark messages as deleted (with proper write lock)
			if len(toMarkDeleted) > 0 {
				topic.Mu.Lock()
				for _, msgID := range toMarkDeleted {
					if msg, exists := topic.Messages[msgID]; exists {
						msg.MarkAsFinalDeleted()
						glog.Info("[sweep] set to final deleted message:", msg.Id)
					}
				}
				topic.Mu.Unlock()
			}
		}
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}

func RunCheckForDeleted(ctx context.Context, app *app.PubSub) {
	glog.Info("[sweep] check for deleted messages started")
	sweep := func() {
		storage.TopicMsgMu.RLock()
		defer storage.TopicMsgMu.RUnlock()

		for _, v := range storage.TopicMessages {
			// Create a list of messages to delete first
			var toDelete []string

			v.Mu.Lock()
			for msgID, msg := range v.Messages {
				if msg.IsFinalDeleted() {
					toDelete = append(toDelete, msgID)
				}
			}

			// Then delete them after identifying all candidates
			for _, msgID := range toDelete {
				// Update the processed status in Spanner
				if err := utils.UpdateMessageProcessedStatus(app.Client, msgID); err != nil {
					glog.Errorf("[sweep] error updating message %s processed status: %v", msgID, err)
				}

				delete(v.Messages, msgID)
				glog.Info("[sweep] deleted message:", msgID)
			}
			v.Mu.Unlock()
		}
	}

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep()
		}
	}
}

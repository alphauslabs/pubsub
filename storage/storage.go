package storage

import (
	"encoding/json"
	"sync"
	"time"

	//added spanner client
	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/golang/glog"
)

type Message struct {
	*pb.Message
	Locked     int32
	AutoExtend int32
	Age        time.Time
	mu         sync.Mutex
}

type Subscriptions struct {
	*pb.Subscription
}

var (
	// Map for topic subscriptions
	topicSubs   = make(map[string]map[string]*Subscriptions)
	topicSubsMu sync.RWMutex

	// Map for topic messages
	TopicMessages = make(map[string]map[string]*Message)
	topicMsgMu    sync.RWMutex
)

func StoreMessage(msg *Message) error {
	if msg == nil || msg.Id == "" {
		glog.Info("[ERROR]: Received invalid message")
		return ErrInvalidMessage
	}

	// Lock at message level first for internal operations
	msg.mu.Lock()
	defer msg.mu.Unlock()

	// Lock the topic messages map for writing
	topicMsgMu.Lock()
	defer topicMsgMu.Unlock()

	if _, exists := TopicMessages[msg.Topic]; !exists {
		TopicMessages[msg.Topic] = make(map[string]*Message)
	}
	TopicMessages[msg.Topic][msg.Id] = msg

	glog.Infof("[STORAGE]: Stored messages:ID = %s, Topic = %s", msg.Id, msg.Topic)

	return nil
}

func StoreTopicSubscriptions(data []byte) error {
	// Lock internal subscription data
	topicSubsMu.Lock()
	defer topicSubsMu.Unlock()

	// Unmarshal the data into the topic subscriptions map
	if err := json.Unmarshal(data, &topicSubs); err != nil {
		glog.Errorf("[ERROR]: Failed to unmarshal topic subscriptions: %v", err)
		return err
	}

	glog.Infof("[STORAGE]: Stored topic-subscription data: %d", len(topicSubs))

	return nil
}

func MonitorActivity() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		var topicMsgCounts = make(map[string]int)
		var topicSubDetails = make(map[string]int)

		topicMsgMu.RLock()
		for topic, msgs := range TopicMessages {
			topicMsgCounts[topic] = len(msgs)
		}
		topicMsgMu.RUnlock()

		topicSubsMu.RLock()
		for topic, subs := range topicSubs {
			topicSubDetails[topic] = len(subs)
		}
		topicSubsMu.RUnlock()

		if len(topicSubDetails) == 0 {
			glog.Info("[Storage monitor] No topic-subscription data available")
		} else {
			for topic, subCount := range topicSubDetails {
				glog.Infof("[Storage monitor] Topic: %s - Subscriptions: %d", topic, subCount)
			}
		}

		if len(topicMsgCounts) == 0 {
			glog.Info("[Storage monitor] No messages available")
		} else {
			for topic, count := range topicMsgCounts {
				glog.Infof("[Storage monitor] Topic: %s - Messages: %d", topic, count)
			}
		}
	}
}

func GetMessage(id string) (*Message, error) {
	topicMsgMu.RLock()
	defer topicMsgMu.RUnlock()

	// Since we don't know which topic this message belongs to,
	// we need to search all topics
	for _, msgs := range TopicMessages {
		if msg, exists := msgs[id]; exists {
			return msg, nil
		}
	}

	return nil, ErrMessageNotFound
}

func GetMessagesByTopic(topicID string) ([]*Message, error) {
	topicMsgMu.RLock()
	defer topicMsgMu.RUnlock()

	topicMsgs, exists := TopicMessages[topicID]
	if !exists {
		return nil, nil
	}

	messages := make([]*Message, 0, len(topicMsgs))
	for _, msg := range topicMsgs {
		messages = append(messages, msg)
	}
	return messages, nil
}

func GetSubscribtionsForTopic(topicID string) ([]*Subscriptions, error) {
	topicSubsMu.RLock()
	defer topicSubsMu.RUnlock()

	subs, exists := topicSubs[topicID]
	if !exists {
		return nil, ErrTopicNotFound
	}
	// Convert map to slice
	subList := make([]*Subscriptions, 0, len(subs))
	for _, sub := range subs {
		subList = append(subList, sub)
	}

	return subList, nil
}

// RemoveMessage removes a message from storage
func RemoveMessage(id string, topicID string) error {
	topicMsgMu.Lock()
	defer topicMsgMu.Unlock()

	// If topicID is provided, we can directly check that topic
	if topicID != "" {
		if topicMsgs, exists := TopicMessages[topicID]; exists {
			if _, found := topicMsgs[id]; found {
				delete(topicMsgs, id)
				return nil
			}
		}
		return ErrMessageNotFound
	}

	// If topicID is not provided, search all topics
	for _, msgs := range TopicMessages {
		if _, exists := msgs[id]; exists {
			delete(msgs, id)
			return nil
		}
	}

	return ErrMessageNotFound
}

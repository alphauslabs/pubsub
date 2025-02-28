package storage

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/spanner" //added spanner client
	pb "github.com/alphauslabs/pubsub-proto/v1"
)

var (
	mu            sync.RWMutex
	messages      = make(map[string]*pb.Message)
	topicSubs     = make(map[string][]string)
	topicMessages = make(map[string]map[string]*pb.Message)
	lastActivity  = time.Now()
	spannerClient *spanner.Client //included spanner client
)

// func NewStorage() *Storage {
// 	s := &Storage{
// 		messages:      make(map[string]*pb.Message),
// 		topicSubs:     make(map[string][]string),
// 		topicMessages: make(map[string]map[string]*pb.Message),
// 		lastActivity:  time.Now(),
// 	}

// 	go s.monitorActivity()

// 	return s
// }

func StoreMessage(msg *pb.Message) error {
	if msg == nil || msg.Id == "" {
		log.Println("[ERROR]: Received invalid message")
		return ErrInvalidMessage
	}

	mu.Lock()
	defer mu.Unlock()

	messages[msg.Id] = msg

	if _, exists := topicMessages[msg.Topic]; !exists {
		topicMessages[msg.Topic] = make(map[string]*pb.Message)
	}
	topicMessages[msg.Topic][msg.Id] = msg

	lastActivity = time.Now()
	log.Printf("[STORAGE]: Stored messages:ID = %s, Topic = %s", msg.Id, msg.Topic)

	return nil
}

func StoreTopicSubscriptions(data []byte) error {
	if len(data) == 0 {
		log.Println("[ERROR]: Received empty topic-subscription data")
		return ErrInvalidTopicSub
	}

	var recv map[string][]string
	if err := json.Unmarshal(data, &recv); err != nil {
		return err
	}

	mu.Lock()
	defer mu.Unlock()

	topicSubs = recv // always replace

	lastActivity = time.Now()

	topicCount := len(topicSubs)
	totalSubs := 0
	for _, subs := range topicSubs {
		totalSubs += len(subs)
	}
	log.Printf("[STORAGE]: Stored topic-subscription data: %d topics, %d total subscriptions", topicCount, totalSubs)

	return nil
}

func MonitorActivity() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		mu.RLock()
		elapsed := time.Since(lastActivity)
		msgCount := len(messages)
		topicCount := len(topicSubs)

		topicMsgCounts := make(map[string]int)
		for topic, msgs := range topicMessages {
			topicMsgCounts[topic] = len(msgs)
		}

		topicSubDetails := make(map[string]int)
		for topic, subs := range topicSubs {
			topicSubDetails[topic] = len(subs)
		}

		mu.RUnlock()

		log.Printf("[STORAGE] Status: %d messages, %d topics, last activity: %v ago", msgCount, topicCount, elapsed.Round(time.Second))
		log.Printf("[STORAGE] Topic-Subscription Structure:")
		if len(topicSubDetails) == 0 {
			log.Println("[STORAGE]    No topic-subscription data available")
		} else {
			for topic, subCount := range topicSubDetails {
				log.Printf("[STORAGE]    Topic: %s - Subscriptions: %d", topic, subCount)
			}
		}

		log.Println("[STORAGE] Message Distribution:")
		if len(topicMsgCounts) == 0 {
			log.Println("[STORAGE]    No messages available")
		} else {
			for topic, count := range topicMsgCounts {
				log.Printf("[STORAGE]    Topic: %s - Messages: %d", topic, count)
			}
		}

		if elapsed > 10*time.Minute {
			log.Printf("[STORAGE] No activity detected in the last %v", elapsed.Round(time.Second))
		}
	}
}

func GetMessage(id string) (*pb.Message, error) {
	mu.RLock()
	defer mu.RUnlock()

	msg, exists := messages[id]
	if !exists {
		return nil, ErrMessageNotFound
	}
	return msg, nil
}

func GetMessagesByTopic(topicID string) ([]*pb.Message, error) {
	mu.RLock()
	defer mu.RUnlock()

	topicMsgs, exists := topicMessages[topicID]
	if !exists {
		return nil, nil
	}

	messages := make([]*pb.Message, 0, len(topicMsgs))
	for _, msg := range topicMsgs {
		messages = append(messages, msg)
	}
	return messages, nil
}

func GetSubscribtionsForTopic(topicID string) ([]string, error) {
	mu.RLock()
	defer mu.RUnlock()

	subs, exists := topicSubs[topicID]
	if !exists {
		return nil, ErrTopicNotFound
	}

	return subs, nil
}

// NEW METHOD TO REMOVE MESSAGES IN QUEUE AFTER ACKNOWLEDGE
func RemoveMessage(id string) {
	mu.Lock()
	defer mu.Unlock()
	delete(messages, id)
}

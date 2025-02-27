package storage

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	"cloud.google.com/go/spanner" //added spanner client
	pb "github.com/alphauslabs/pubsub-proto/v1"
)

type Storage struct {
	mu            sync.RWMutex
	messages      map[string]*pb.Message
	topicSubs     map[string][]string
	topicMessages map[string]map[string]*pb.Message
	lastActivity  time.Time
	spannerClient *spanner.Client //included spanner client
}

func NewStorage() *Storage {
	s := &Storage{
		messages:      make(map[string]*pb.Message),
		topicSubs:     make(map[string][]string),
		topicMessages: make(map[string]map[string]*pb.Message),
		lastActivity:  time.Now(),
	}

	go s.monitorActivity()

	return s
}

func (s *Storage) StoreMessage(msg *pb.Message) error {
	if msg == nil || msg.Id == "" {
		log.Println("[ERROR]: Received invalid message")
		return ErrInvalidMessage
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.messages[msg.Id] = msg

	if _, exists := s.topicMessages[msg.Topic]; !exists {
		s.topicMessages[msg.Topic] = make(map[string]*pb.Message)
	}
	s.topicMessages[msg.Topic][msg.Id] = msg

	s.lastActivity = time.Now()
	log.Printf("[STORAGE]: Stored messages:ID = %s, Topic = %s", msg.Id, msg.Topic)

	return nil
}

func (s *Storage) StoreTopicSubscriptions(data []byte) error {
	if len(data) == 0 {
		log.Println("[ERROR]: Received empty topic-subscription data")
		return ErrInvalidTopicSub
	}

	var topicSubs map[string][]string
	if err := json.Unmarshal(data, &topicSubs); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.topicSubs = topicSubs
	s.lastActivity = time.Now()

	topicCount := len(topicSubs)
	totalSubs := 0
	for _, subs := range topicSubs {
		totalSubs += len(subs)
	}
	log.Printf("[STORAGE]: Stored topic-subscription data: %d topics, %d total subscriptions", topicCount, totalSubs)

	return nil
}

func (s *Storage) monitorActivity() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.RLock()
		elapsed := time.Since(s.lastActivity)
		msgCount := len(s.messages)
		topicCount := len(s.topicSubs)

		topicMsgCounts := make(map[string]int)
		for topic, msgs := range s.topicMessages {
			topicMsgCounts[topic] = len(msgs)
		}

		topicSubDetails := make(map[string]int)
		for topic, subs := range s.topicSubs {
			topicSubDetails[topic] = len(subs)
		}

		s.mu.RUnlock()

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

func (s *Storage) GetMessage(id string) (*pb.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	msg, exists := s.messages[id]
	if !exists {
		return nil, ErrMessageNotFound
	}
	return msg, nil
}

func (s *Storage) GetMessagesByTopic(topicID string) ([]*pb.Message, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	topicMsgs, exists := s.topicMessages[topicID]
	if !exists {
		return nil, nil
	}

	messages := make([]*pb.Message, 0, len(topicMsgs))
	for _, msg := range topicMsgs {
		messages = append(messages, msg)
	}
	return messages, nil
}

func (s *Storage) GetSubscribtionsForTopic(topicID string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	subs, exists := s.topicSubs[topicID]
	if !exists {
		return nil, ErrTopicNotFound
	}

	return subs, nil
}

// NEW METHOD TO REMOVE MESSAGES IN QUEUE AFTER ACKNOWLEDGE
func (s *Storage) RemoveMessage(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.messages, id)
}

// NEW METHOD TO UPDATE MESSAGE PROCESSED STATUS IN SPANNER
func (s *Storage) UpdateMessageProcessedStatus(id string, processed bool) error {
	// Check for valid ID
	if id == "" {
		log.Println("[ERROR]: Received invalid message ID")
		return ErrInvalidMessage
	}

	// Update the message processed status in Spanner
	_, err := s.spannerClient.Apply(context.Background(), []*spanner.Mutation{
		spanner.Update("Messages", []string{"Id", "Processed"}, []interface{}{id, processed}),
	})
	if err != nil {
		log.Printf("[ERROR]: Failed to update message processed status in Spanner: %v", err)
		return err
	}

	log.Printf("[STORAGE]: Updated message processed status in Spanner for ID: %s, Processed: %v", id, processed)

	return nil
}

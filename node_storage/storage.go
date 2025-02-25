package storage

import (
	"encoding/json"
	"sync"

	pb "github.com/alphauslabs/pubsub-proto/v1"
)

type Storage struct {
	mu            sync.RWMutex
	messages      map[string]*pb.Message
	topicSubs     map[string][]string
	topicMessages map[string]map[string]*pb.Message
}

func NewStorage() *Storage {
	return &Storage{
		messages:      make(map[string]*pb.Message),
		topicSubs:     make(map[string][]string),
		topicMessages: make(map[string]map[string]*pb.Message),
	}
}

func (s *Storage) StoreMessage(msg *pb.Message) error {
	if msg == nil || msg.Id == "" {
		return ErrInvalidMessage
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.messages[msg.Id] = msg

	if _, exists := s.topicMessages[msg.Topic]; !exists {
		s.topicMessages[msg.Topic] = make(map[string]*pb.Message)
	}
	s.topicMessages[msg.Topic][msg.Id] = msg

	return nil
}

func (s *Storage) StoreTopicSubscriptions(data []byte) error {
	if len(data) == 0 {
		return ErrInvalidTopicSub
	}

	var topicSubs map[string][]string
	if err := json.Unmarshal(data, &topicSubs); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.topicSubs = topicSubs
	return nil
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

	result := make([]string, len(subs))
	copy(result, subs)
	return result, nil
}

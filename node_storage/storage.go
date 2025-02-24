package main

import (
	"sync"

	pb "github.com/alphauslabs/pubsub-proto/v1"
)

type Storage struct {
	mu            sync.RWMutex
	messages      map[string]*pb.Message
	topics        map[string]*pb.Topic
	subscriptions map[string]*pb.Subscription
	topicMessages map[string]map[string]*pb.Message
}

func NewStorage() *Storage {
	return &Storage{
		messages:      make(map[string]*pb.Message),
		topics:        make(map[string]*pb.Topic),
		subscriptions: make(map[string]*pb.Subscription),
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

	if _, exists := s.topicMessages[msg.TopicId]; !exists {
		s.topicMessages[msg.TopicId] = make(map[string]*pb.Message)
	}
	s.topicMessages[msg.TopicId][msg.Id] = msg

	return nil
}

func (s *Storage) StoreTopic(topic *pb.Topic) error {
	if topic == nil || topic.TopicId == "" {
		return ErrInvalidTopic
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.topics[topic.TopicId] = topic
	return nil
}

func (s *Storage) StoreSubscription(sub *pb.Subscription) error {
	if sub == nil || sub.Id == "" {
		return ErrInvalidSubscription
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.subscriptions[sub.Id] = sub
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

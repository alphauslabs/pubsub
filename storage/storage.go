package storage

import (
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
	Deleted    int32
	Age        time.Time
	Mu         sync.Mutex
}

type MessageMap struct {
	Messages map[string]*Message
	mu       sync.RWMutex
}

// NewMessageMap creates a new message map
func NewMessageMap() *MessageMap {
	return &MessageMap{
		Messages: make(map[string]*Message),
	}
}

// Get retrieves a message by ID
func (mm *MessageMap) Get(id string) (*Message, bool) {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	msg, exists := mm.Messages[id]
	return msg, exists
}

// Put adds or updates a message
func (mm *MessageMap) Put(id string, msg *Message) {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	mm.Messages[id] = msg
}

// Delete removes a message
func (mm *MessageMap) Delete(id string) bool {
	mm.mu.Lock()
	defer mm.mu.Unlock()

	_, exists := mm.Messages[id]
	if exists {
		delete(mm.Messages, id)
	}
	return exists
}

// GetAll returns a copy of all Messages
func (mm *MessageMap) GetAll() []*Message {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	result := make([]*Message, 0, len(mm.Messages))
	for _, msg := range mm.Messages {
		result = append(result, msg)
	}
	return result
}

// Count returns the number of Messages
func (mm *MessageMap) Count() int {
	mm.mu.RLock()
	defer mm.mu.RUnlock()

	return len(mm.Messages)
}

type Subscription struct {
	*pb.Subscription
}

var (
	// Map for topic subscriptions
	topicSubs   = make(map[string]map[string]*Subscription)
	topicSubsMu sync.RWMutex

	// Map for topic Messages
	TopicMessages = make(map[string]*MessageMap)
	topicMsgMu    sync.RWMutex
)

func StoreMessage(msg *Message) error {
	if msg == nil || msg.Id == "" {
<<<<<<< HEAD
		glog.Info("[ERROR]: Received invalid message")
=======
		log.Println("[ERROR] Received invalid message")
>>>>>>> aa8fdf48 (Added changes)
		return ErrInvalidMessage
	}

	// Lock the topic Messages map for writing
	topicMsgMu.Lock()
	defer topicMsgMu.Unlock()

	if _, exists := TopicMessages[msg.Topic]; !exists {
		TopicMessages[msg.Topic] = NewMessageMap()
	}
	TopicMessages[msg.Topic].Put(msg.Id, msg)

<<<<<<< HEAD
	glog.Infof("[STORAGE] Stored message with ID = %s, Topic = %s", msg.Id, msg.Topic)
	return nil
}

func StoreTopicSubscriptions(d map[string]map[string]*Subscription) error {
	// Lock internal subscription data
	topicSubsMu.Lock()
	defer topicSubsMu.Unlock()

	topicSubs = d // replaces everytime
	glog.Infof("[STORAGE] Stored topic-subscription data with len %d", len(topicSubs))
=======
	s.lastActivity = time.Now()
	log.Printf("[STORAGE] Stored messages:ID = %s, Topic = %s", msg.Id, msg.Topic)
>>>>>>> aa8fdf48 (Added changes)

	return nil
}

<<<<<<< HEAD
func MonitorActivity() {
	ticker := time.NewTicker(2 * time.Minute)
=======
func (s *Storage) StoreTopicSubscriptions(data []byte) error {
	if len(data) == 0 {
		log.Println("[ERROR] Received empty topic-subscription data")
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
	log.Printf("[STORAGE] Stored topic-subscription data: %d topics, %d total subscriptions", topicCount, totalSubs)

	return nil
}

func (s *Storage) monitorActivity() {
	ticker := time.NewTicker(5 * time.Minute)
>>>>>>> aa8fdf48 (Added changes)
	defer ticker.Stop()

	for range ticker.C {
		var topicMsgCounts = make(map[string]int)
		var topicSubDetails = make(map[string]int)

		topicMsgMu.RLock()
		for topic, msgs := range TopicMessages {
			topicMsgCounts[topic] = msgs.Count()
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
			glog.Info("[Storage monitor] No Messages available")
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
		if msg, exists := msgs.Get(id); exists {
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

	return topicMsgs.GetAll(), nil
}

func GetSubscribtionsForTopic(topicID string) ([]*Subscription, error) {
	topicSubsMu.RLock()
	defer topicSubsMu.RUnlock()

	subs, exists := topicSubs[topicID]
	if !exists {
		return nil, ErrTopicNotFound
	}
	// Convert map to slice
	subList := make([]*Subscription, 0, len(subs))
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
			if topicMsgs.Delete(id) {
				return nil
			}
		}
		return ErrMessageNotFound
	}

	// If topicID is not provided, search all topics
	for _, msgs := range TopicMessages {
		if msgs.Delete(id) {
			return nil
		}
	}

	return ErrMessageNotFound
}

package storage

import (
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/golang/glog"
)

type Message struct {
	*pb.Message
	Mu            sync.Mutex
	Subscriptions map[string]*Subs
	FinalDeleted  int32
}

type Subs struct {
	SubscriptionID string
	Age            time.Time
	Deleted        int32
	Locked         int32
	AutoExtend     int32
	Mu             sync.Mutex // lock
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
		glog.Info("[ERROR]: Received invalid message")
		return ErrInvalidMessage
	}

	// Lock the topic Messages map for writing
	topicMsgMu.Lock()
	defer topicMsgMu.Unlock()

	if _, exists := TopicMessages[msg.Topic]; !exists {
		TopicMessages[msg.Topic] = NewMessageMap()
	}

	subs, err := GetSubscribtionsForTopic(msg.Topic)
	if err != nil {
		glog.Errorf("[STORAGE] Error retrieving subscriptions for topic %s: %v", msg.Topic, err)
		return err
	}

	subss := make(map[string]*Subs)
	for _, sub := range subs {
		subss[sub.Name] = &Subs{
			SubscriptionID: sub.Name,
		}
	}

	msg.Subscriptions = subss
	TopicMessages[msg.Topic].Put(msg.Id, msg)

	glog.Infof("[STORAGE] Stored message with ID = %s, Topic = %s", msg.Id, msg.Topic)
	return nil
}

func StoreTopicSubscriptions(d map[string]map[string]*Subscription) error {
	// Lock internal subscription data
	topicSubsMu.Lock()
	defer topicSubsMu.Unlock()

	topicSubs = d // replaces everytime
	glog.Infof("[STORAGE] Stored topic-subscription data with len %d", len(topicSubs))

	return nil
}

func MonitorActivity() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		var topicMsgCounts = make(map[string]int)
		var topicSubDetails = make(map[string]int)

		topicMsgMu.RLock()
		for topic, msgs := range TopicMessages {
			count := 0
			for _, msg := range msgs.GetAll() {
				if atomic.LoadInt32(&msg.FinalDeleted) == 0 {
					count++
				}
			}
			topicMsgCounts[topic] = count
		}
		topicMsgMu.RUnlock()

		topicSubsMu.RLock()
		for topic, subs := range topicSubs {
			topicSubDetails[topic] = len(subs)
		}
		topicSubsMu.RUnlock()

		if len(topicSubDetails) == 0 {
			glog.Info("[Storage Monitor] No topic-subscription data available")
		} else {
			for topic, subCount := range topicSubDetails {
				glog.Infof("[Storage Monitor] Topic: %s - Subscriptions: %d", topic, subCount)
			}
		}

		if len(topicMsgCounts) == 0 {
			glog.Info("[Storage Monitor] No Messages available")
		} else {
			for topic, count := range topicMsgCounts {
				glog.Infof("[Storage Monitor] Topic: %s - Messages: %d", topic, count)
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
			// check if marked deleted
			if atomic.LoadInt32(&msg.FinalDeleted) == 1 {
				return nil, ErrMessageNotFound
			}
			return msg, nil
		}
	}

	return nil, ErrMessageNotFound
}

func GetMessagesByTopic(topicName, sub string) ([]*Message, error) {
	topicMsgMu.RLock()
	defer topicMsgMu.RUnlock()

	topicMsgs, exists := TopicMessages[topicName]
	if !exists {
		return nil, nil
	}
	allMsgs := topicMsgs.GetAll()

	// filter messages marked dleted
	activeMsgs := make([]*Message, 0, len(allMsgs))
	for _, msg := range allMsgs {
		if atomic.LoadInt32(&msg.FinalDeleted) == 1 { // filter
			continue
		}

		if msg.Subscriptions[sub].IsDeleted() {
			continue
		}

		if msg.Subscriptions[sub].IsLocked() {
			continue
		}

		activeMsgs = append(activeMsgs, msg)
	}

	return activeMsgs, nil //ret filtered mssges
}

func GetSubscribtionsForTopic(topicName string) ([]*Subscription, error) {
	topicSubsMu.RLock()
	defer topicSubsMu.RUnlock()

	subs, exists := topicSubs[topicName]
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

// RemoveTopic removes a topic from storage
func RemoveTopic(topicName string) error {
	topicMsgMu.Lock()
	defer topicMsgMu.Unlock()

	if _, exists := TopicMessages[topicName]; !exists {
		return ErrTopicNotFound
	}

	delete(TopicMessages, topicName)
	delete(topicSubs, topicName)

	return nil
}

// RemoveMessage removes a message from storage
func RemoveMessage(id string, topicName string) error {
	topicMsgMu.Lock()
	defer topicMsgMu.Unlock()

	// If topicName is provided, we can directly check that topic
	if topicName != "" {
		if topicMsgs, exists := TopicMessages[topicName]; exists {
			if topicMsgs.Delete(id) {
				return nil
			}
		}
		return ErrMessageNotFound
	}

	// If topicName is not provided, search all topics
	for _, msgs := range TopicMessages {
		if msgs.Delete(id) {
			return nil
		}
	}

	return ErrMessageNotFound
}

// HasBeenProcessedBySubscription checks if this subscription has received the message (fanout check)
// func (m *Message) HasBeenProcessedBySubscription(subscriptionID string) bool {
// 	m.Mu.Lock()
// 	defer m.Mu.Unlock()

// 	if m.ProcessedSubs == nil {
// 		m.ProcessedSubs = make(map[string]struct{})
// 	}

// 	_, exists := m.ProcessedSubs[subscriptionID]
// 	return exists
// }

// HasBeenProcessedByClient checks if a client has processed the message (load balance check -client side )
// func (m *Message) HasBeenProcessedByClient(subscriptionID, clientID string) bool {
// 	m.Mu.Lock()
// 	defer m.Mu.Unlock()

// 	if m.ProcessedSubs == nil {
// 		return false
// 	}

// 	_, processed := m.ProcessedSubs[subscriptionID]
// 	return processed
// }

// MarkAsProcessedByClient marks message as processed by client and creates subscription entry if needed
// func (m *Message) MarkAsProcessedByClient(subscriptionID, clientID string) {
// 	m.Mu.Lock()
// 	defer m.Mu.Unlock()

// 	if m.ProcessedSubs == nil {
// 		m.ProcessedSubs = make(map[string]struct{})
// 	}

// 	m.ProcessedSubs[subscriptionID] = struct{}{}
// }

// IsLockedBySubscription checks if message is locked within a specific subscription
func (m *Message) IsLockedBySubscription(subscriptionID string) bool {
	return atomic.LoadInt32(&m.Subscriptions[subscriptionID].Locked) == 1
}

// // MarkAsProcessedBySubscription marks message as processed by subscription
// func (m *Message) MarkAsProcessedBySubscription(subscriptionID string) {
// 	m.Mu.Lock()
// 	defer m.Mu.Unlock()

// 	atomic.StoreInt32(&m.Subscriptions[subscriptionID].Dispatched, 1)
// }

func (s *Subs) RenewAge() {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Age = time.Now().UTC()
}

func (s *Subs) Lock() {
	atomic.StoreInt32(&s.Locked, 1)
}

func (s *Subs) Unlock() {
	atomic.StoreInt32(&s.Locked, 0)
}

func (s *Subs) IsLocked() bool {
	return atomic.LoadInt32(&s.Locked) == 1
}

func (s *Subs) IsDeleted() bool {
	return atomic.LoadInt32(&s.Deleted) == 1
}

func (s *Subs) MarkAsDeleted() {
	atomic.StoreInt32(&s.Deleted, 1)
}

func (s *Subs) SetAutoExtend(autoExtend bool) {
	if autoExtend {
		atomic.StoreInt32(&s.AutoExtend, 1)
	} else {
		atomic.StoreInt32(&s.AutoExtend, 0)
	}
}

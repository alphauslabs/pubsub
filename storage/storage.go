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
	Locked         int32
	AutoExtend     int32
	Deleted        int32
	Age            time.Time
	ProcessedCount int32    // count of subscriptions that processed this message (fanout)
	ProcessedBy    map[string]struct{} // track which clients have processed this message (load balance)
	ProcessingBy   string   // track which client is currently processing this message
	Mu             sync.Mutex
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
	ticker := time.NewTicker(2 * time.Minute)
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
			if atomic.LoadInt32(&msg.Deleted) == 1 {
				return nil, ErrMessageNotFound
			}
			return msg, nil
		}
	}

	return nil, ErrMessageNotFound
}

func GetMessagesByTopic(topicName string) ([]*Message, error) {
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
		if atomic.LoadInt32(&msg.Deleted) == 0 { // filter
			activeMsgs = append(activeMsgs, msg)
		}
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

// HasBeenProcessedBySubscription checks if a message has been sent to any subscription (fanout)
func (m *Message) HasBeenProcessedBySubscription() bool {
	return atomic.LoadInt32(&m.ProcessedCount) > 0
}

// MarkAsProcessedBySubscription marks a message as sent to a subscription (fanout)
func (m *Message) MarkAsProcessedBySubscription() {
	atomic.AddInt32(&m.ProcessedCount, 1)
}

// IsBeingProcessed checks if a message is currently being processed by any client
func (m *Message) IsBeingProcessed() bool {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	return m.ProcessingBy != ""
}

// StartProcessing marks a message as being processed by a specific client
func (m *Message) StartProcessing(clientID string) bool {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	
	// If message is already being processed, return false
	if m.ProcessingBy != "" {
		return false
	}
	
	m.ProcessingBy = clientID
	return true
}

// EndProcessing marks a message as no longer being processed
func (m *Message) EndProcessing(clientID string) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	
	if m.ProcessingBy == clientID {
		m.ProcessingBy = ""
	}
}
// HasBeenProcessedByClient checks if a message has been processed by a specific client (load balance)
func (m *Message) HasBeenProcessedByClient(clientID string) bool {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	
	if m.ProcessedBy == nil {
		m.ProcessedBy = make(map[string]struct{})
	}
	
	_, exists := m.ProcessedBy[clientID]
	return exists
}

// MarkAsProcessedByClient marks a message as processed by a specific client (load balance)
func (m *Message) MarkAsProcessedByClient(clientID string) {
	m.Mu.Lock()
	defer m.Mu.Unlock()
	
	if m.ProcessedBy == nil {
		m.ProcessedBy = make(map[string]struct{})
	}
	
	m.ProcessedBy[clientID] = struct{}{}
	m.ProcessingBy = "" // Clear processing state when marking as processed
}


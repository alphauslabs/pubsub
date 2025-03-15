package storage

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/alphauslabs/pubsub-proto/v1"
	"github.com/golang/glog"
)

var (
	ErrMessageNotFound = errors.New("message not found")
	ErrInvalidMessage  = errors.New("invalid message")
	ErrTopicNotFound   = errors.New("topic not found")
	ErrInvalidTopicSub = errors.New("invalid topic-subscription structure")
)

// Notes: Lock acquisition order to prevent deadlocks
// Always acquire locks in this order:
// 1. topicSubsMu
// 2. topicMsgMu
// 3. MessageMap.Mu
// 4. Message.Mu
// 5. Subs.Mu
type Subscription struct {
	*pb.Subscription
}

var (
	// Map for topic subscriptions
	topicSubs   = make(map[string]map[string]*Subscription)
	topicSubsMu sync.RWMutex

	// Map for topic Messages
	TopicMessages = make(map[string]*MessageMap)
	TopicMsgMu    sync.RWMutex
)

type MessageMap struct {
	Messages map[string]*Message
	Mu       sync.RWMutex
}
type Message struct {
	*pb.Message
	Mu            sync.Mutex
	Subscriptions map[string]*Subs
	FinalDeleted  int32
}

func (m *Message) MarkAsFinalDeleted() {
	atomic.StoreInt32(&m.FinalDeleted, 1)
}

func (m *Message) IsFinalDeleted() bool {
	return atomic.LoadInt32(&m.FinalDeleted) == 1
}

type Subs struct {
	SubscriptionID string
	Age            time.Time
	Deleted        int32
	Locked         int32
	AutoExtend     int32
	Mu             sync.Mutex // lock
}

// NewMessageMap creates a new message map
func NewMessageMap() *MessageMap {
	return &MessageMap{
		Messages: make(map[string]*Message),
	}
}

// Get retrieves a message by ID
func (mm *MessageMap) Get(id string) (*Message, bool) {
	mm.Mu.RLock()
	defer mm.Mu.RUnlock()

	msg, exists := mm.Messages[id]
	return msg, exists
}

// Put adds a message, no op if exists
func (mm *MessageMap) Put(id string, msg *Message) {
	mm.Mu.Lock()
	defer mm.Mu.Unlock()

	_, exists := mm.Messages[id]
	if !exists {
		mm.Messages[id] = msg
	} else {
		glog.Infof("[STORAGE] Message %s already exists, no op", id)
	}
}

// GetAll returns a copy of all Messages
func (mm *MessageMap) GetAll() []*Message {
	mm.Mu.RLock()
	defer mm.Mu.RUnlock()

	result := make([]*Message, 0, len(mm.Messages))
	for _, msg := range mm.Messages {
		result = append(result, msg)
	}
	return result
}

func StoreMessage(msg *Message) error {
	if msg == nil || msg.Id == "" {
		glog.Info("[ERROR]: Received invalid message")
		return ErrInvalidMessage
	}

	topicSubsMu.RLock()
	_, exists := topicSubs[msg.Topic]
	topicSubsMu.RUnlock()

	if !exists {
		glog.Errorf("[STORAGE] topic %s not found in storage", msg.Topic)
		return ErrTopicNotFound
	}

	TopicMsgMu.Lock()
	defer TopicMsgMu.Unlock()

	if _, exists := TopicMessages[msg.Topic]; !exists {
		TopicMessages[msg.Topic] = NewMessageMap()
	}
	TopicMessages[msg.Topic].Put(msg.Id, msg)
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

func GetMessage(id string) (*Message, error) {
	TopicMsgMu.RLock()
	defer TopicMsgMu.RUnlock()

	// Since we don't know which topic this message belongs to,
	// we need to search all topics
	for _, msgs := range TopicMessages {
		if msg, exists := msgs.Get(id); exists {
			// check if marked deleted
			if msg.IsFinalDeleted() {
				return nil, ErrMessageNotFound
			}
			return msg, nil
		}
	}

	return nil, ErrMessageNotFound
}

func GetMessagesByTopicSub(topicName, sub string) (*Message, error) {
	TopicMsgMu.RLock()
	defer TopicMsgMu.RUnlock()

	topicMsgs, exists := TopicMessages[topicName]
	if !exists {
		return nil, fmt.Errorf("[Subscribe] no active messages found for topic=%s and sub=%s", topicName, sub)
	}
	allMsgs := topicMsgs.GetAll()

	for _, msg := range allMsgs {
		if msg.IsFinalDeleted() {
			continue
		}

		if msg.Subscriptions[sub].IsDeleted() {
			continue
		}

		if msg.Subscriptions[sub].IsLocked() {
			continue
		}

		return msg, nil
	}

	return nil, fmt.Errorf("[Subscribe] no active messages found for topic=%s and sub=%s", topicName, sub)
}

func GetSubscribtionsForTopic(topicName string) ([]*Subscription, error) {
	topicSubsMu.RLock()
	defer topicSubsMu.RUnlock()

	subs, exists := topicSubs[topicName]
	if !exists {
		glog.Errorf("[STORAGE] topic %s not found in storage, current in mem=%v", topicName, getTopicKeys())
		return nil, ErrTopicNotFound
	}
	// Convert map to slice
	subList := make([]*Subscription, 0, len(subs))
	for _, sub := range subs {
		if sub == nil {
			glog.Errorf("[STORAGE] found nil subscription for topic %s", topicName)
			continue
		}
		subList = append(subList, sub)
	}

	return subList, nil
}

func getTopicKeys() []string {
	keys := make([]string, 0, len(topicSubs))
	for k := range topicSubs {
		keys = append(keys, k)
	}
	return keys
}

// RemoveTopic removes a topic from storage
func RemoveTopic(topicName string) error {
	TopicMsgMu.Lock()
	defer TopicMsgMu.Unlock()

	if _, exists := TopicMessages[topicName]; !exists {
		return ErrTopicNotFound
	}

	delete(TopicMessages, topicName)
	delete(topicSubs, topicName)

	return nil
}

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

func (s *Subs) ClearAge() {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Age = time.Time{}
}

type InQueue struct {
	Subscription string `json:"subscription"`
	Total        int    `json:"total"`
	Available    int    `json:"available"`
	Locked       int    `json:"locked"`
}

// MessageCount represents the count of messages for a subscription
type MessageCount struct {
	Topic        string `json:"topic"`
	Subscription string `json:"subscription"`
	Total        int    `json:"total"`     // Total messages
	Locked       int    `json:"locked"`    // Messages locked
	Available    int    `json:"available"` // Messages available for processing
	Deleted      int    `json:"deleted"`   // Messages marked as deleted
}

// GetMessagesCountBySubscription returns counts of messages for all subscriptions
// The results can be filtered by topic and/or subscription if provided
func GetMessagesCountBySubscription(filterTopic, filterSubscription string) ([]MessageCount, error) {
	topicSubsMu.RLock()
	defer topicSubsMu.RUnlock()
	TopicMsgMu.RLock()
	defer TopicMsgMu.RUnlock()

	var results []MessageCount

	// Initialize counters for all topic-subscription pairs
	counters := make(map[string]map[string]*MessageCount)

	for topic, subscriptions := range topicSubs {
		// Skip if filtering by topic and this isn't the requested topic
		if filterTopic != "" && filterTopic != topic {
			continue
		}

		counters[topic] = make(map[string]*MessageCount)

		for subName := range subscriptions {
			// Skip if filtering by subscription and this isn't the requested one
			if filterSubscription != "" && filterSubscription != subName {
				continue
			}

			counters[topic][subName] = &MessageCount{
				Topic:        topic,
				Subscription: subName,
				Total:        0,
				Locked:       0,
				Available:    0,
				Deleted:      0,
			}
		}
	}

	// Now count messages for each topic-subscription pair
	for topic, msgMap := range TopicMessages {
		// Skip if filtering by topic and this isn't the requested topic
		if filterTopic != "" && filterTopic != topic {
			continue
		}

		// Skip if this topic has no subscriptions we're tracking
		topicCounters, exists := counters[topic]
		if !exists {
			continue
		}

		// Get all messages for this topic
		allMsgs := msgMap.GetAll()

		// Count each message for relevant subscriptions
		for _, msg := range allMsgs {
			// Skip if the message is marked as final deleted
			if msg.IsFinalDeleted() {
				continue
			}

			// Check each subscription for this message
			for subName, sub := range msg.Subscriptions {
				// Skip if filtering by subscription and this isn't the requested one
				if filterSubscription != "" && filterSubscription != subName {
					continue
				}

				// Skip if we're not tracking this subscription
				counter, exists := topicCounters[subName]
				if !exists {
					continue
				}

				// Count this message
				counter.Total++

				// Update specific counters based on message state
				if sub.IsDeleted() {
					counter.Deleted++
				} else if sub.IsLocked() {
					counter.Locked++
				} else {
					counter.Available++
				}
			}
		}
	}

	// Convert the map to a flat slice
	for _, topicCounters := range counters {
		for _, counter := range topicCounters {
			results = append(results, *counter)
		}
	}

	return results, nil
}

// GetMessagesStatsByTopic returns aggregated statistics grouped by topic
func GetMessagesStatsByTopic() (map[string]struct {
	Subscriptions     int `json:"subscriptions"`
	TotalMessages     int `json:"total_messages"`
	AvailableMessages int `json:"available_messages"`
}, error) {
	counts, err := GetMessagesCountBySubscription("", "")
	if err != nil {
		return nil, err
	}

	// Group by topic
	stats := make(map[string]struct {
		Subscriptions     int `json:"subscriptions"`
		TotalMessages     int `json:"total_messages"`
		AvailableMessages int `json:"available_messages"`
	})

	// Count unique subscriptions per topic
	subsByTopic := make(map[string]map[string]bool)

	for _, count := range counts {
		if _, exists := stats[count.Topic]; !exists {
			stats[count.Topic] = struct {
				Subscriptions     int `json:"subscriptions"`
				TotalMessages     int `json:"total_messages"`
				AvailableMessages int `json:"available_messages"`
			}{
				Subscriptions:     0,
				TotalMessages:     0,
				AvailableMessages: 0,
			}
			subsByTopic[count.Topic] = make(map[string]bool)
		}

		// Count unique subscriptions
		subsByTopic[count.Topic][count.Subscription] = true

		// Update stats
		entry := stats[count.Topic]
		entry.TotalMessages += count.Total
		entry.AvailableMessages += count.Available
		stats[count.Topic] = entry
	}

	// Set subscription counts
	for topic, subs := range subsByTopic {
		entry := stats[topic]
		entry.Subscriptions = len(subs)
		stats[topic] = entry
	}

	return stats, nil
}

// GetSubscriptionQueueDepths returns queue depths for monitoring purposes
func GetSubscriptionQueueDepths() ([]InQueue, error) {
	counts, err := GetMessagesCountBySubscription("", "")
	if err != nil {
		return nil, err
	}

	results := make([]InQueue, 0, len(counts))
	for _, count := range counts {
		results = append(results, InQueue{
			Subscription: count.Topic + ":" + count.Subscription,
			Total:        count.Total,
			Available:    count.Available,
			Locked:       count.Locked,
		})
	}

	return results, nil
}

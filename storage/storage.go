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
// 5. MsgSub.Mu
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

	// Record Map for node subscriptions
	RecordMapMu sync.RWMutex
	// Key format: externalIp|internalIp, value is the list of members (initials of the subscriptions the node is currently serving)
	RecordMap = make(map[string][]string)
)

// Replaces current record map
func SetRecordMap(v map[string][]string) {
	RecordMapMu.Lock()
	defer RecordMapMu.Unlock()
	RecordMap = v
}

func GetMembersFromRecordMap(r map[string][]string, node string) []string {
	RecordMapMu.RLock()
	defer RecordMapMu.RUnlock()

	mem, ok := r[node]
	if !ok {
		return nil
	}

	return mem
}

type MessageMap struct {
	Messages map[string]*Message
	Mu       sync.RWMutex
}
type Message struct {
	*pb.Message
	Mu            sync.RWMutex
	Subscriptions map[string]*MsgSub
	FinalDeleted  int32
}

func (m *Message) MarkAsFinalDeleted() {
	atomic.StoreInt32(&m.FinalDeleted, 1)
}

func (m *Message) IsFinalDeleted() bool {
	return atomic.LoadInt32(&m.FinalDeleted) == 1
}

type MsgSub struct {
	SubscriptionID string
	Age            time.Time
	Deleted        int32
	Locked         int32
	AutoExtend     int32
	Mu             sync.RWMutex // lock
}

// NewMessageMap creates a new message map
func NewMessageMap() *MessageMap {
	return &MessageMap{
		Messages: make(map[string]*Message),
	}
}

// Get retrieves a message by ID
func (mm *MessageMap) Get(id string) *Message {
	mm.Mu.RLock()
	defer mm.Mu.RUnlock()

	msg, exists := mm.Messages[id]
	if !exists {
		return nil
	}
	if msg.IsFinalDeleted() {
		return nil
	}
	return msg
}

// Put adds a message, no op if exists
func (mm *MessageMap) Put(id string, msg *Message) {
	mm.Mu.Lock()
	defer mm.Mu.Unlock()

	_, exists := mm.Messages[id]
	if !exists {
		mm.Messages[id] = msg
	} else {
		glog.Infof("[Storage] Message %s already exists, updating it", id)
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
		glog.Error("[Storage] Received invalid message, msg is nil or id is empty")
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
	topicSubsMu.Lock()
	defer topicSubsMu.Unlock()

	topicSubs = d // replaces everytime
	glog.Infof("[STORAGE] Stored topic-subscription data with len %d", len(topicSubs))

	return nil
}

func GetMessage(id, topic string) *Message {
	TopicMsgMu.RLock()
	defer TopicMsgMu.RUnlock()

	msg, ok := TopicMessages[topic]
	if !ok {
		glog.Errorf("[STORAGE] Topic %s not found in storage, returning nil...", topic)
		return nil
	}

	m := msg.Get(id)
	if m == nil {
		glog.Errorf("[STORAGE] Message %s not found in topic %s, returning nil...", id, topic)
		return nil
	}

	if m.IsFinalDeleted() {
		glog.Errorf("[STORAGE] Message %s already marked as final deleted in topic %s, returning nil...", id, topic)
		return nil
	}

	return m
}

func GetMessagesByTopicSub(topicName, sub string) (*Message, error) {
	TopicMsgMu.RLock()
	defer TopicMsgMu.RUnlock()

	topicMsgs, exists := TopicMessages[topicName]
	if !exists {
		return nil, fmt.Errorf("[Subscribe] topic %s not found in TopicMessages", topicName)
	}

	allMsgs := topicMsgs.GetAll()
	for _, msg := range allMsgs {
		if msg.IsFinalDeleted() {
			continue
		}

		// Check status
		msg := func() *Message {
			msg.Mu.RLock()
			defer msg.Mu.RUnlock()

			if msg.Subscriptions[sub].IsDeleted() {
				return nil
			}

			if msg.Subscriptions[sub].IsLocked() {
				return nil
			}

			msg.Subscriptions[sub].Lock()
			msg.Subscriptions[sub].RenewAge()
			return msg
		}()
		if msg == nil {
			continue
		}
		return msg, nil
	}

	return nil, fmt.Errorf("[Subscribe] no available messages found for topic=%s sub=%s", topicName, sub)
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

func (s *MsgSub) RenewAge() {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Age = time.Now().UTC()
}

func (s *MsgSub) Lock() {
	atomic.StoreInt32(&s.Locked, 1)
}

func (s *MsgSub) Unlock() {
	atomic.StoreInt32(&s.Locked, 0)
}

func (s *MsgSub) IsLocked() bool {
	return atomic.LoadInt32(&s.Locked) == 1
}

func (s *MsgSub) IsDeleted() bool {
	return atomic.LoadInt32(&s.Deleted) == 1
}

func (s *MsgSub) MarkAsDeleted() {
	atomic.StoreInt32(&s.Deleted, 1)
	s.ClearAge()
}

func (s *MsgSub) SetAutoExtend(autoExtend bool) {
	if autoExtend {
		atomic.StoreInt32(&s.AutoExtend, 1)
	} else {
		atomic.StoreInt32(&s.AutoExtend, 0)
	}
}

func (s *MsgSub) IsAutoExtend() bool {
	return atomic.LoadInt32(&s.AutoExtend) == 1
}

func (s *MsgSub) ClearAge() {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.Age = time.Time{}
}

type InQueue struct {
	Subscription string
	Total        int
	Available    int
	Locked       int
}

// MessageCount represents the count of messages for a subscription
type MessageCount struct {
	Topic        string
	Subscription string
	Total        int // Total messages
	Locked       int // Messages locked
	Available    int // Messages available for processing
	Deleted      int // Messages marked as deleted
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

			msg.Mu.RLock()
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

				if sub.IsDeleted() {
					continue
				}

				if sub.IsLocked() {
					counter.Locked++ // in flight
				} else {
					counter.Available++ // available for processing
				}
			}
			msg.Mu.RUnlock()
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
// in could be topic, subscription or both. fmt: in[0] = topic, in[1] = subscription
func GetSubscriptionQueueDepths(in ...string) ([]InQueue, error) {
	var topic, sub string
	if len(in) == 1 {
		topic = in[0]
	} else if len(in) == 2 {
		topic = in[0]
		sub = in[1]
	}
	counts, err := GetMessagesCountBySubscription(topic, sub)
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

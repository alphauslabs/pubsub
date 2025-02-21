package storage

import "errors"

var (
	ErrMessageNotFound      = errors.New("message not found")
	ErrTopicNotFound        = errors.New("topic not found")
	ErrSubscriptionNotFound = errors.New("subscription not found")
	ErrInvalidMessage       = errors.New("invalid message")
	ErrInvalidTopic         = errors.New("invalid topic")
	ErrInvalidSubscription  = errors.New("invalid subscription")
)

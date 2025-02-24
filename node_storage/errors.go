package storage

import "errors"

var (
	ErrMessageNotFound  = errors.New("message not found")
	ErrInvalidMessage   = errors.New("invalid message")
	ErrDuplicateMessage = errors.New("duplicate message")
	ErrTopicNotFound    = errors.New("topic not found")
	ErrInvalidTopicSub  = errors.New("invalid topic-subscription structure")
)

package utils

import (
    "log"
)

// LogError logs an error message to the console.
func LogError(err error) {
    if err != nil {
        log.Printf("Error: %v", err)
    }
}

// ParseMessage will be the placeholder function for parsing messages.
func ParseMessage(message []byte) (interface{}, error) {
    // TODO: Implement message parsing logic
    return nil, nil
}
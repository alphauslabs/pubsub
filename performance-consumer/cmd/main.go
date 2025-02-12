package main

import (
    "log"
    "performance-consumer/internal/config"
    "performance-consumer/internal/consumer"
)

func main() {
    // Load application configuration
    cfg, err := config.LoadConfig()
    if err != nil {
        log.Fatalf("Error loading configuration: %v", err)
    }

    // Consumer Init
    c, err := consumer.NewConsumer(cfg)
    if err != nil {
        log.Fatalf("Error initializing consumer: %v", err)
    }

    // Start consumer and handling of errors
    if err := c.Start(); err != nil {
        log.Fatalf("Error starting consumer: %v", err)
    }

    log.Println("Consumer started successfully")
}
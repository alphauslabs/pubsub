# performance-consumer

## Overview
Performance-consumer worktree - task is to efficiently consume messages from a messaging system, process them, and acknowledge their receipt; pseudo benchmarking the raw performance of the whole system whilst assumedly ignoring the system's VT and Beyond VT that covers actual business logic. 
## Project Structure
```
performance-consumer
├── cmd
│   └── main.go          # Entry point for the application
├── internal
│   ├── consumer
│   │   └── consumer.go  # Core consumer logic
│   └── config
│       └── config.go    # Configuration settings
├── pkg
│   └── utils
│       └── utils.go     # Utility functions
├── go.mod               # Module dependencies and Go version
└── README.md            # documentation
```

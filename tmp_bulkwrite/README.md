# BulkWrite Pub/Sub System

This repository contains a distributed Pub/Sub system designed for high-throughput message processing and bulk writing to a database. The system is built using Go and leverages gRPC for communication between nodes. It supports a leader-follower architecture for distributed message handling and batch processing for efficient database writes.

## Table of Contents

*   [Overview](#overview)
*   [Architecture](#architecture)
*   [Features](#features)
*   [Getting Started](#getting-started)
    *   [Prerequisites](#prerequisites)
    *   [Installation](#installation)
    *   [Running the System](#running-the-system)
    *   [Configuration](#configuration)
*   [API Documentation](#api-documentation)
*   [Contributing](#contributing)
*   [License](#license)

## Overview

The BulkWrite Pub/Sub system is designed to handle high volumes of messages efficiently. It uses a leader-follower architecture to distribute the load across multiple nodes. The system supports batch processing of messages, which are then written to a database in bulk to optimize performance.

## Architecture

The system is composed of the following components:

*   **Leader Node:** Responsible for coordinating the followers, handling message routing, and managing batch writes to the database.
*   **Follower Nodes:** Handle incoming messages, forward them to the leader, and assist in batch processing.
*   **gRPC Services:** Used for communication between the leader and followers.
*   **Database:** Stores the processed messages. The system is designed to work with Google Cloud Spanner, but it can be adapted to other databases.

## Features

*   **Distributed Architecture:** Leader-follower model for scalable message processing.
*   **Batch Processing:** Messages are processed in batches to optimize database writes.
*   **gRPC Communication:** Efficient and reliable communication between nodes.
*   **Customizable Batch Size:** Adjustable batch size and buffer size for messages.
*   **Concurrent Workers:** Multiple workers can process messages concurrently.

## Getting Started

### Prerequisites

*   Go 1.23.1 or higher
*   Google Cloud Spanner (or another database if adapted)
*   gRPC and Protocol Buffers (protobuf)

### Installation

1.  Clone the repository:

    ```bash
    git clone https://github.com/alphauslabs/pubsub.git
    ```

2.  Install dependencies:

    ```bash
    go mod download
    ```

3.  Generate protobuf files:

    ```bash
    protoc --go_out=. --go-grpc_out=. bulkwrite_proto/pubsub.proto
    ```

### Running the System

1.  Run the Leader Node:

    ```bash
    go run main.go -leader=true -leader-url="ip-here:50050"
    ```

2.  Run Follower Nodes:

    ```bash
    go run main.go -leader=false -follower-port="50051"
    ```

3.  Configure the Database:

    Ensure that your Google Cloud Spanner instance is configured and accessible. Update the database connection details in the code if necessary.
    

### Configuration

The system can be configured using command-line flags:

*   `-leader`: Set to true to run the node as a leader.
*   `-leader-url`: URL of the leader node (used by followers).
*   `-follower-port`: Port for the follower node.
*   `-batchsize`: Batch size for bulk writes (default: 5000).
*   `-messagesbuffer`: Buffer size for messages channel (default: 1000000).
*   `-waittime`: Wait time before flushing the batch (default: 500ms).
*   `-workers`: Number of concurrent workers (default: 32).

## API Documentation

The system provides the following gRPC services:

*   **PubSubService**
    *   `ForwardMessage`: Forwards a message to the leader node.
    *   `LeaderHealthCheck`: Checks the health of the leader node.
    *   `LeaderElection`: Handles leader election in case of leader failure.
    *   `BatchWrite`: Writes a batch of messages to the database.
    *   `CreateTopic`: Creates a new topic.
    *   `GetTopic`: Retrieves a topic by ID.
    *   `UpdateTopic`: Updates a topic's name.
    *   `DeleteTopic`: Deletes a topic.
    *   `ListTopics`: Lists all topics.
    *   `CreateSubscription`: Creates a new subscription.
    *   `GetSubscription`: Retrieves a subscription by ID.
    *   `UpdateSubscription`: Updates a subscription's visibility timeout.
    *   `DeleteSubscription`: Deletes a subscription.
    *   `ListSubscriptions`: Lists all subscriptions.
    *   `Publish`: Publishes a message to a topic.
    *   `Subscribe`: Subscribes to a topic and receives messages.
    *   `Acknowledge`: Acknowledges the receipt of a message.
    *   `ModifyVisibilityTimeout`: Modifies the visibility timeout of a message.

For detailed API definitions, refer to the `pubsub.proto` file.
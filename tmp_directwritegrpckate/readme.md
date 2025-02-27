# gRPC Pub/Sub Service with Google Spanner

## Overview
This project implements a gRPC-based Pub/Sub service that writes messages directly to Google Spanner.

## Features

### gRPC Server
- Listens on port `8086`.
- Implements a `Publish` method to store messages in Spanner.

### Google Spanner Integration
- Each message is assigned a unique ID (`UUID`).
- Uses `InsertOrUpdate` to store messages in the `Messages` table.
- Includes logging for successful message writes.

### Performance Optimizations
- Direct writes to Spanner for efficiency.
- Timeout of **120 seconds** for database operations to prevent hanging.

### Logging
- Logs when the gRPC server starts.
- Logs errors if Spanner operations fail.
- Logs when a message is successfully written to Spanner, including **commit time and latency**.

## Performance Measurement

The system measures how long it takes for a message to be written to Spanner using **commit timestamp retrieval**.

### Formula for Latency Calculation

```
Latency = CommitTime - StartTime
```

### Where:
- StartTime = Time when the gRPC server receives the request.
- CommitTime = Time when Google Spanner commits the transaction.
- The commit time is retrieved directly from Spanner, ensuring a more accurate measure of when the message was stored in the database.

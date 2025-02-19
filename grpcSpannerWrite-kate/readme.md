# gRPC Pub/Sub Service with Google Spanner

This project implements a gRPC-based Pub/Sub service that writes messages directly to Google Spanner.

## Features

- **gRPC Server**
  - Listens on port `8085`
  - Implements a `Publish` method to store messages in Spanner

- **Google Spanner Integration**
  - Each message is assigned a unique ID (`UUID`)
  - Uses `InsertOrUpdate` to store messages in the `Messages` table
  - Includes logging for successful message writes

- **Performance Optimizations**
  - Direct writes to Spanner for efficiency
  - Timeout of 120 seconds for database operations to prevent hanging

- **Logging**
  - Logs when the gRPC server starts
  - Logs errors if Spanner operations fail
  - Logs when a message is successfully written to Spanner

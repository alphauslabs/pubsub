**Key Features:**


*gRPC Server*

-Listens on port 8085.

-Implements a Publish method to accept and store messages.


*Message Writing to Spanner*


-Each message gets a unique ID (UUID).

-Uses InsertOrUpdate to store messages in the Messages table.


*Performance Metrics Logged*

-Total operation time (end-to-end).

-Spanner write time (how long the DB write took).

-Commit retrieval time (fetching the commit timestamp).


*Timeouts for Reliability*

-5-second timeout for writing to Spanner to prevent hanging.

-5-second timeout for reading commit timestamps.


*Spanner Commit Timestamp*

-Uses spanner.CommitTimestamp to track when the message was stored.


*Error Handling & Logging*

-Logs failures if writing or reading from Spanner fails.

-Returns errors if Spanner operations don't complete successfully.

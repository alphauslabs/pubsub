**Structure**

- **proto/v1/**: Contains the protocol buffer definitions and generated code.
  - `pubsub.proto`: Protocol buffer definitions.
  - `pubsub.pb.go`: Generated Go code from the protocol buffer definitions.
  - `pubsub_grpc.pb.go`: Generated gRPC code from the protocol buffer definitions.
- **server.go**: gRPC server implementation.
- **testclient.go**: gRPC test client.
- **main.go**: Entry point of the application.
- **go.mod**: Go module file.
- **go.sum**: Go dependencies file.
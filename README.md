


You need to install few things to start contributing:

- The [protoc](https://protobuf.dev/installation/) tool, a protocol buffer compiler.

- The following compiler plugins:

```bash
$ go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
$ go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

To update generated files:

```bash
protoc \
--go_out=.  \   
--go_opt=paths=source_relative \
--go-grpc_out=.  \   
--go-grpc_opt=paths=source_relative \
v1/pubsub.proto
```

to update the go libs

```bash
go get -u google.golang.org/grpc
```
it should fade the errors in pubsub_grpc.pb.go and pubsub.pb.go

To cleanup go.mod run this:
```bash
go mod tidy
```


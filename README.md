I've created this branch just in case the original branch won't work.
The generated file on the other branch is based on the pubsub-proto repo
This branch package is based on this repo


You need to install few things to start contributing:

- The [protoc](https://protobuf.dev/installation/) tool, a protocol buffer compiler.

- The following compiler plugins:

```bash
$ go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
$ go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

To update generated files:

```bash
make generate_grpc_code
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
if error persists in go.mod
delete go.sum and rerun go mod tidy

export GO_PATH=~/go
export PATH=$PATH:/$GO_PATH/bin
source ~/.bash_profile
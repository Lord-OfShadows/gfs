# gfs

A small GFS-inspired distributed file system in Go: one coordinator holds file metadata, several chunkservers store replicated chunks, and a CLI client talks to both over gRPC.

**Status:** early development. Only the project scaffold and gRPC contracts (milestone M0) exist so far. See [docs/PRD.md](docs/PRD.md) for the full specification.

## Layout

```
cmd/coordinator   coordinator binary (metadata)
cmd/chunkserver   chunkserver binary (chunk bytes)
cmd/gfs           CLI client
internal/...      implementation packages
proto/            .proto contracts; generated Go code in proto/gfspb
test/             integration and failure tests
docs/             design documents
```

## Prerequisites

- Go 1.27+
- `protoc` (Protocol Buffers compiler), only needed to regenerate code
- `protoc-gen-go` and `protoc-gen-go-grpc`, only needed to regenerate code:
  ```
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
  ```

## Build

Generated code is committed, so a plain build needs only Go:

```
go build ./...
```

With `make` (Linux/macOS, or Windows with GNU make):

```
make proto   # regenerate proto/gfspb from proto/*.proto
make build   # compile everything and write binaries to bin/
make test
make clean
```

On Windows without `make`, use the PowerShell script with the same targets:

```
.\make.ps1 proto
.\make.ps1 build
```

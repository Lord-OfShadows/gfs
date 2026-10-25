# gfs

A small GFS-inspired distributed file system in Go: one coordinator holds file metadata, several chunkservers store replicated chunks, and a CLI client talks to both over gRPC.

**Status:** Core system working: chunked upload/download with SHA-256 integrity checks, atomic chunk writes, 3-way replication via a write pipeline, replica-fallback reads, idempotent file creation, and a Docker Compose cluster (1 coordinator + 4 chunkservers). Next (M4, not started): heartbeats, failure detection, and automatic re-replication. See [docs/PRD.md](docs/PRD.md) for the full design.

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

## Running with Docker

Requires Docker with Compose v2. The cluster is one coordinator and four chunkservers (`cs1`–`cs4`), each chunkserver with its own named volume.

```
docker compose build          # build the image (all three binaries)
docker compose up -d          # start coordinator + cs1..cs4 in the background
```

The CLI runs as a one-off container on the cluster's network. Files go in and out through the `./transfer` folder, which is mounted at `/transfer`:

```
docker compose run --rm gfs upload /transfer/report.pdf
docker compose run --rm gfs ls
docker compose run --rm gfs download report.pdf /transfer/report-copy.pdf
docker compose run --rm gfs rm report.pdf
```

Watch the logs (one JSON line per RPC):

```
docker compose logs -f
```

Simulate a node failure and restore it:

```
docker compose stop cs2
docker compose run --rm gfs download report.pdf /transfer/copy-while-cs2-down.pdf
docker compose start cs2
```

Stop everything and delete all stored data:

```
docker compose down -v
```

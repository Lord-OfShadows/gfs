MODULE := github.com/Lord-OfShadows/gfs
PROTOS := proto/common.proto proto/coordinator.proto proto/chunkserver.proto

.PHONY: proto build test clean

# Regenerate Go code from .proto files into proto/gfspb/.
proto:
	protoc -I proto \
		--go_out=. --go_opt=module=$(MODULE) \
		--go-grpc_out=. --go-grpc_opt=module=$(MODULE) \
		$(PROTOS)

# Type-check every package, then write the three binaries to bin/.
build:
	go build ./...
	go build -o bin/ ./cmd/...

test:
	go test ./...

# Remove build output. Generated proto code is committed, so it is kept.
clean:
	go clean
	rm -rf bin

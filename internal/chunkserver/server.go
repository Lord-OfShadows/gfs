package chunkserver

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Lord-OfShadows/gfs/proto/gfspb"
)

// Server implements gfspb.ChunkServerServiceServer on top of a Store.
type Server struct {
	// Embedded by value: supplies an "Unimplemented" answer for every RPC we
	// have not written yet (e.g. CopyChunk), and is required by the generated code.
	gfspb.UnimplementedChunkServerServiceServer

	nodeID string
	store  *Store
}

// NewServer returns a Server that identifies itself as nodeID.
func NewServer(nodeID string, store *Store) *Server {
	return &Server{nodeID: nodeID, store: store}
}

// WriteChunk receives a header message followed by data frames, verifies the
// checksum, and persists the chunk.
func (s *Server) WriteChunk(stream grpc.ClientStreamingServer[gfspb.WriteChunkRequest, gfspb.WriteChunkResponse]) error {
	// TODO(arjun): implement.
	//   1. stream.Recv() the first message; it must be a header (else InvalidArgument).
	//   2. Recv() data frames until io.EOF, appending bytes and enforcing header.chunk.length.
	//   3. Verify SHA-256 against header.chunk.checksum (mismatch -> DataLoss or InvalidArgument).
	//   4. s.store.Write, then stream.SendAndClose(&gfspb.WriteChunkResponse{...}).
	return status.Error(codes.Unimplemented, "WriteChunk not implemented")
}

// ReadChunk streams a stored chunk: a header message, then data frames.
func (s *Server) ReadChunk(req *gfspb.ReadChunkRequest, stream grpc.ServerStreamingServer[gfspb.ReadChunkResponse]) error {
	// TODO(arjun): implement.
	//   1. s.store.Read(req.GetChunkId()); map ErrChunkNotFound -> NotFound.
	//   2. stream.Send a header with id, length, and SHA-256 checksum.
	//   3. stream.Send the bytes in fixed-size frames (e.g. 64 KiB).
	return status.Error(codes.Unimplemented, "ReadChunk not implemented")
}

// DeleteChunk removes a chunk from local disk. Deleting a missing chunk succeeds.
func (s *Server) DeleteChunk(ctx context.Context, req *gfspb.DeleteChunkRequest) (*gfspb.DeleteChunkResponse, error) {
	if err := s.store.Delete(req.GetChunkId()); err != nil {
		return nil, toStatus(err)
	}
	return &gfspb.DeleteChunkResponse{}, nil
}

// GetHealth reports this node's ID and how many chunks/bytes it stores.
func (s *Server) GetHealth(ctx context.Context, req *gfspb.GetHealthRequest) (*gfspb.GetHealthResponse, error) {
	count, used, err := s.store.Stats()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read store stats: %v", err)
	}
	return &gfspb.GetHealthResponse{
		NodeId: s.nodeID,
		Stats: &gfspb.NodeStats{
			ChunkCount: count,
			UsedBytes:  used,
			// CapacityBytes stays 0 until we add an OS-specific free-space check.
		},
	}, nil
}

// toStatus maps Store errors to gRPC status codes so clients can tell
// "bad request" from "not found" from "server problem".
func toStatus(err error) error {
	switch {
	case errors.Is(err, ErrInvalidChunkID):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, ErrChunkNotFound):
		return status.Error(codes.NotFound, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

package chunkserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Lord-OfShadows/gfs/proto/gfspb"
)

const maxChunkSize = 64 << 20 // 64 MiB

const frameSize = 64 << 10 // 64 KiB per data message

const reportTimeout = 5 * time.Second // deadline for each ReportChunk call to the coordinator

// Server implements gfspb.ChunkServerServiceServer on top of a Store.
type Server struct {
	// Embedded by value: supplies an "Unimplemented" answer for every RPC we
	// have not written yet (e.g. CopyChunk), and is required by the generated code.
	gfspb.UnimplementedChunkServerServiceServer

	nodeID string
	store  *Store
	coord  gfspb.CoordinatorServiceClient // nil = no coordinator (standalone mode, tests)
}

// NewServer returns a Server that identifies itself as nodeID.
func NewServer(nodeID string, store *Store) *Server {
	return &Server{nodeID: nodeID, store: store}
}

// SetCoordinator sets the coordinator this chunkserver reports stored chunks
// to. Call it before the gRPC server starts serving: handlers read s.coord
// without a lock, which is only safe if it never changes while they run.
func (s *Server) SetCoordinator(c gfspb.CoordinatorServiceClient) {
	s.coord = c
}

// WriteChunk receives a header message followed by data frames, verifies the
// checksum, and persists the chunk.
func (s *Server) WriteChunk(stream grpc.ClientStreamingServer[gfspb.WriteChunkRequest, gfspb.WriteChunkResponse]) error {

	// First Message recieved must be the header
	first, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "recieve header: %v", err)
	}

	header := first.GetHeader()
	if header == nil || header.GetChunk() == nil {
		return status.Error(codes.InvalidArgument, "First message must be a header with chunk info")
	}

	info := header.GetChunk()
	want := info.GetLength()
	if want < 0 || want > maxChunkSize {
		return status.Errorf(codes.InvalidArgument, "invalid chunk length %d", want)
	}

	// Recieve data untill client says "done"
	buf := make([]byte, 0, want)
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break // Client finished sending data
		}
		if err != nil {
			return err // connection broke or client cancelled
		}

		if msg.GetHeader() != nil {
			return status.Error(codes.InvalidArgument, "unexpected 2nd header")
		}
		data := msg.GetData()
		if int64(len(buf)+len(data)) > want {
			return status.Errorf(codes.InvalidArgument, "recieved more data than %d bytes", want)
		}

		buf = append(buf, data...)
	}

	// We get exactly what was expected
	if int64(len(buf)) != want {
		return status.Errorf(codes.InvalidArgument, "recieved %d bytes, expected %d", len(buf), want)
	}

	sum := sha256.Sum256(buf)
	if !bytes.Equal(sum[:], info.GetChecksum()) {
		return status.Error(codes.DataLoss, "checksum mismatch ;  Data corrupted in transit")
	}

	// Save to Disk; Then Reply
	if err := s.store.Write(info.GetChunkId(), buf); err != nil {
		return toStatus(err)
	}

	// Tell the coordinator we have this chunk
	if s.coord != nil {
		ctx, cancel := context.WithTimeout(stream.Context(), reportTimeout)
		defer cancel()
		_, err := s.coord.ReportChunk(ctx, &gfspb.ReportChunkRequest{
			NodeId: s.nodeID,
			Chunk: &gfspb.ChunkInfo{
				ChunkId:  info.GetChunkId(),
				Length:   int64(len(buf)),
				Checksum: sum[:],
			},
		})
		if err != nil {
			return status.Errorf(status.Code(err), "stored chunk but could not report it to coordinator: %v", err)
		}
	}

	return stream.SendAndClose(&gfspb.WriteChunkResponse{
		ChunkId:      info.GetChunkId(),
		BytesWritten: int64(len(buf)),
	})
}

// ReadChunk streams a stored chunk: a header message, then data frames.
func (s *Server) ReadChunk(req *gfspb.ReadChunkRequest, stream grpc.ServerStreamingServer[gfspb.ReadChunkResponse]) error {
	// Load the chunk from disk
	data, err := s.store.Read(req.GetChunkId())
	if err != nil {
		return toStatus(err)
	}

	// Send Header First
	sum := sha256.Sum256(data)
	header := &gfspb.ReadChunkResponse{
		Payload: &gfspb.ReadChunkResponse_Header{
			Header: &gfspb.ChunkInfo{
				ChunkId:  req.GetChunkId(),
				Length:   int64(len(data)),
				Checksum: sum[:],
			},
		},
	}
	if err := stream.Send(header); err != nil {
		return err
	}

	// Send Data in 64Kb pieces/frames
	for start := 0; start < len(data); start += frameSize {
		end := min(start+frameSize, len(data))
		msg := &gfspb.ReadChunkResponse{
			Payload: &gfspb.ReadChunkResponse_Data{Data: data[start:end]},
		}
		if err := stream.Send(msg); err != nil {
			return err
		}
	}
	return nil
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

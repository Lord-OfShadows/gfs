package chunkserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/Lord-OfShadows/gfs/proto/gfspb"
)

const maxChunkSize = 64 << 20           // 64 MiB
const frameSize = 64 << 10              // 64 KiB per data message
const reportTimeout = 5 * time.Second   // deadline for each ReportChunk call to the coordinator
const forwardTimeout = 30 * time.Second // deadline for forwarding one chunk down the replica chain

// Server implements gfspb.ChunkServerServiceServer on top of a Store.
type Server struct {
	// Embedded by value: supplies an "Unimplemented" answer for every RPC we
	// have not written yet (e.g. CopyChunk), and is required by the generated code.
	gfspb.UnimplementedChunkServerServiceServer

	nodeID string
	store  *Store
	coord  gfspb.CoordinatorServiceClient // nil = no coordinator (standalone mode, tests)

	// dial opens a connection to a peer chunkserver. Tests replace it (SetDial)
	// to use in-memory (bufconn) connections.
	dial func(addr string) (*grpc.ClientConn, error)

	mu    sync.Mutex                  // protects conns
	conns map[string]*grpc.ClientConn // peer address -> cached connection
}

// NewServer returns a Server that identifies itself as nodeID.
func NewServer(nodeID string, store *Store) *Server {
	return &Server{
		nodeID: nodeID,
		store:  store,
		dial: func(addr string) (*grpc.ClientConn, error) {
			return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		},
		conns: make(map[string]*grpc.ClientConn),
	}
}

// SetDial replaces how peer chunkservers are dialed (tests use bufconn).
// Call it before the gRPC server starts serving, like SetCoordinator.
func (s *Server) SetDial(d func(addr string) (*grpc.ClientConn, error)) {
	s.dial = d
}

// peer returns a client for the chunkserver at addr, reusing a cached
// connection if there is one. Safe to call from concurrent handlers.
func (s *Server) peer(addr string) (gfspb.ChunkServerServiceClient, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	conn, ok := s.conns[addr]
	if !ok {
		// Dialing under the lock is fine: grpc.NewClient does no network I/O,
		// it only builds the connection object.
		var err error
		conn, err = s.dial(addr)
		if err != nil {
			return nil, fmt.Errorf("connect to peer %s: %w", addr, err)
		}
		s.conns[addr] = conn
	}
	return gfspb.NewChunkServerServiceClient(conn), nil
}

// Close closes every cached peer connection. Call it on shutdown, after the
// gRPC server has stopped.
func (s *Server) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	for addr, conn := range s.conns {
		errs = append(errs, conn.Close())
		delete(s.conns, addr)
	}
	return errors.Join(errs...)
}

// forwardChunk sends a chunk this server has already verified and stored to
// the next replica in the chain, asking it to forward to the rest.
// An empty chain means this server is the last replica.
func (s *Server) forwardChunk(ctx context.Context, info *gfspb.ChunkInfo, data []byte, chain []*gfspb.ReplicaLocation) error {
	if len(chain) == 0 {
		return nil
	}
	next, rest := chain[0], chain[1:]

	// Connect to the next server in the chain
	peer, err := s.peer(next.GetAddress())
	if err != nil {
		return fmt.Errorf("Connect to %s: %w", next.GetAddress(), err)
	}
	stream, err := peer.WriteChunk(ctx)
	if err != nil {
		return fmt.Errorf("Open Stream to %s: %w", next.GetAddress(), err)
	}

	// Header: Same Chunk info , and "you forward it to the REST of the chain"
	header := &gfspb.WriteChunkRequest{
		Payload: &gfspb.WriteChunkRequest_Header{
			Header: &gfspb.WriteChunkHeader{
				Chunk:     info,
				ForwardTo: rest,
			},
		},
	}

	if err := stream.Send(header); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("Send Header to %s: %w", next.GetAddress(), err)
	}

	// Data in 64Kb pieces
	for start := 0; start < len(data); start += frameSize {
		end := min(start+frameSize, len(data))
		msg := &gfspb.WriteChunkRequest{
			Payload: &gfspb.WriteChunkRequest_Data{Data: data[start:end]},
		}

		if err := stream.Send(msg); err != nil {
			if errors.Is(err, io.EOF) {
				break // next server hung up; Real error comes from CloseAndRecv
			}
			return fmt.Errorf("Send Data to %s: %w", next.GetAddress(), err)
		}
	}

	// Wait for the next server answer (includes everyone after it)
	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("forward to %s: %w", next.GetAddress(), err)
	}
	return nil
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

	// Pass the chunk down the pipeline (if there is anyone left)
	if chain := header.GetForwardTo(); len(chain) > 0 {
		ctx, cancel := context.WithTimeout(stream.Context(), forwardTimeout)
		defer cancel()
		if err := s.forwardChunk(ctx, info, buf, chain); err != nil {
			return status.Errorf(codes.Unavailable, "stored locally but forwarding failed: %v", err)
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

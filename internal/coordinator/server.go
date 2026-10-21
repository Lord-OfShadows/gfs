package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
	"unicode"
	"unicode/utf8"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/Lord-OfShadows/gfs/proto/gfspb"
)

const (
	maxPathLen          = 1024
	sha256Len           = 32
	heartbeatIntervalMs = 1000
	chunkDeleteTimeout  = 5 * time.Second
)

// Server is the gRPC face of the coordinator: it validates requests, calls
// Metadata, and converts between protobuf messages and Go types.
type Server struct {
	gfspb.UnimplementedCoordinatorServiceServer

	md           *Metadata
	chunkservers []string // addresses new chunks may be placed on

	// dial opens a connection to a chunkserver. Tests replace it to use an
	// in-memory (bufconn) connection.
	dial func(addr string) (*grpc.ClientConn, error)
}

// NewServer returns a Server that stores metadata in md and places chunks on
// the given chunkserver addresses.
func NewServer(md *Metadata, chunkservers []string) *Server {
	return &Server{
		md:           md,
		chunkservers: chunkservers,
		dial: func(addr string) (*grpc.ClientConn, error) {
			return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		},
	}
}

// CreateFile validates the request, registers a pending file, and returns
// where each chunk should be written.
func (s *Server) CreateFile(ctx context.Context, req *gfspb.CreateFileRequest) (*gfspb.CreateFileResponse, error) {
	if err := validatePath(req.GetPath()); err != nil {
		return nil, err
	}

	specs := make([]ChunkSpec, len(req.GetChunks()))
	var total int64
	for i, c := range req.GetChunks() {
		if c.GetLength() <= 0 {
			return nil, status.Errorf(codes.InvalidArgument, "chunk %d: length must be positive, got %d", i, c.GetLength())
		}
		if len(c.GetChecksum()) != sha256Len {
			return nil, status.Errorf(codes.InvalidArgument, "chunk %d: checksum must be %d bytes (SHA-256), got %d", i, sha256Len, len(c.GetChecksum()))
		}
		specs[i] = ChunkSpec{Length: c.GetLength(), Checksum: c.GetChecksum()}
		total += c.GetLength()
	}
	if total != req.GetSizeBytes() {
		return nil, status.Errorf(codes.InvalidArgument, "size_bytes is %d but chunk lengths add up to %d", req.GetSizeBytes(), total)
	}

	f, err := s.md.CreateFile(req.GetOperationId(), req.GetPath(), specs, s.chunkservers)
	if err != nil {
		return nil, toStatus(err)
	}

	resp := &gfspb.CreateFileResponse{FileId: f.ID}
	for _, c := range f.Chunks {
		resp.Chunks = append(resp.Chunks, toPlacement(c))
	}
	return resp, nil
}

// GetFileMetadata returns a visible file's chunks, in order, with where to read them.
func (s *Server) GetFileMetadata(ctx context.Context, req *gfspb.GetFileMetadataRequest) (*gfspb.GetFileMetadataResponse, error) {
	f, err := s.md.GetFile(req.GetPath())
	if err != nil {
		return nil, toStatus(err)
	}

	file := &gfspb.FileMetadata{
		Path:            f.Path,
		FileId:          f.ID,
		SizeBytes:       f.Size,
		CreatedAtUnixMs: f.CreatedAt.UnixMilli(),
		// Version stays 0: files are immutable and Metadata does not track versions yet.
	}
	for _, c := range f.Chunks {
		file.Chunks = append(file.Chunks, toPlacement(c))
	}
	return &gfspb.GetFileMetadataResponse{File: file}, nil
}

// ListFiles returns visible files under a prefix, sorted by path.
func (s *Server) ListFiles(ctx context.Context, req *gfspb.ListFilesRequest) (*gfspb.ListFilesResponse, error) {
	resp := &gfspb.ListFilesResponse{}
	for _, f := range s.md.ListFiles(req.GetPrefix()) {
		resp.Files = append(resp.Files, &gfspb.FileSummary{
			Path:            f.Path,
			SizeBytes:       f.Size,
			CreatedAtUnixMs: f.CreatedAt.UnixMilli(),
		})
	}
	return resp, nil
}

// DeleteFile removes the file from the namespace, then asks chunkservers to
// delete its chunks. The metadata change is what counts: once it succeeds the
// file is gone for clients, so chunk cleanup failures are logged, not returned.
func (s *Server) DeleteFile(ctx context.Context, req *gfspb.DeleteFileRequest) (*gfspb.DeleteFileResponse, error) {
	// TODO: honor req.expected_version once Metadata tracks versions.
	chunks, err := s.md.DeleteFile(req.GetPath())
	if err != nil {
		return nil, toStatus(err)
	}

	// The metadata lock has been released by now, so slow chunkservers cannot
	// block other RPCs.
	s.deleteChunks(chunks)
	return &gfspb.DeleteFileResponse{}, nil
}

// deleteChunks makes a best-effort DeleteChunk call for every chunk on every
// address it was assigned to. It opens one connection per chunkserver.
//
// TODO(M4): chunks whose deletion fails become orphans on disk. Garbage
// collection (comparing chunkserver inventories with metadata) cleans them up.
func (s *Server) deleteChunks(chunks []*Chunk) {
	byAddr := map[string][]string{} // address -> chunk IDs
	for _, c := range chunks {
		for _, addr := range c.Assigned {
			byAddr[addr] = append(byAddr[addr], c.ID)
		}
	}

	for addr, ids := range byAddr {
		conn, err := s.dial(addr)
		if err != nil {
			slog.Warn("chunk cleanup: cannot connect", "addr", addr, "chunks", len(ids), "err", err)
			continue
		}
		cs := gfspb.NewChunkServerServiceClient(conn)
		for _, id := range ids {
			ctx, cancel := context.WithTimeout(context.Background(), chunkDeleteTimeout)
			_, err := cs.DeleteChunk(ctx, &gfspb.DeleteChunkRequest{ChunkId: id})
			cancel()
			if err != nil {
				slog.Warn("chunk cleanup: DeleteChunk failed", "addr", addr, "chunk_id", id, "err", err)
			}
		}
		conn.Close()
	}
}

// ReportChunk records that a chunkserver has stored a chunk.
func (s *Server) ReportChunk(ctx context.Context, req *gfspb.ReportChunkRequest) (*gfspb.ReportChunkResponse, error) {
	if req.GetNodeId() == "" || req.GetChunk().GetChunkId() == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and chunk.chunk_id are required")
	}
	err := s.md.ReportChunk(req.GetNodeId(), req.GetChunk().GetChunkId(), req.GetChunk().GetChecksum())
	if err != nil {
		return nil, toStatus(err)
	}
	return &gfspb.ReportChunkResponse{}, nil
}

// Heartbeat acknowledges a chunkserver.
//
// TODO(M4): record last-heartbeat time per node, run the
// HEALTHY -> SUSPECT -> UNAVAILABLE state machine, and return the real state.
func (s *Server) Heartbeat(ctx context.Context, req *gfspb.HeartbeatRequest) (*gfspb.HeartbeatResponse, error) {
	return &gfspb.HeartbeatResponse{
		State:               gfspb.NodeState_NODE_STATE_HEALTHY,
		HeartbeatIntervalMs: heartbeatIntervalMs,
	}, nil
}

// toPlacement converts a Chunk to the protobuf form sent to clients.
//
// TODO(M4): replicas come from the chunk's Assigned addresses. Once
// heartbeats build a node registry (node ID -> address, state), return the
// nodes in Chunk.Replicas with their real state instead.
func toPlacement(c *Chunk) *gfspb.ChunkPlacement {
	p := &gfspb.ChunkPlacement{
		Chunk: &gfspb.ChunkInfo{
			ChunkId:  c.ID,
			Index:    uint32(c.Index),
			Length:   c.Length,
			Checksum: c.Checksum,
		},
	}
	for _, addr := range c.Assigned {
		p.Replicas = append(p.Replicas, &gfspb.ReplicaLocation{
			Address: addr,
			State:   gfspb.NodeState_NODE_STATE_UNSPECIFIED, // unknown until M4
		})
	}
	return p
}

// validatePath rejects empty, overlong, or control-character paths.
func validatePath(p string) error {
	switch {
	case p == "":
		return status.Error(codes.InvalidArgument, "path is empty")
	case len(p) > maxPathLen:
		return status.Errorf(codes.InvalidArgument, "path is %d bytes, max %d", len(p), maxPathLen)
	case !utf8.ValidString(p):
		return status.Error(codes.InvalidArgument, "path is not valid UTF-8")
	}
	for _, r := range p {
		if unicode.IsControl(r) {
			return status.Errorf(codes.InvalidArgument, "path contains control character %U", r)
		}
	}
	return nil
}

// toStatus maps Metadata errors to gRPC status codes.
func toStatus(err error) error {
	code := codes.Internal
	switch {
	case errors.Is(err, ErrFileExists):
		code = codes.AlreadyExists
	case errors.Is(err, ErrFileNotFound), errors.Is(err, ErrChunkNotFound):
		code = codes.NotFound
	case errors.Is(err, ErrChecksumMismatch):
		code = codes.FailedPrecondition
	case errors.Is(err, ErrNoChunkservers):
		code = codes.Unavailable
	}
	return status.Error(code, fmt.Sprint(err))
}

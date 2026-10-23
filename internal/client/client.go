package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Lord-OfShadows/gfs/proto/gfspb"
)

// ErrChunkCorrupted means a downloaded chunk didn't match its expected checksum.
var ErrChunkCorrupted = errors.New("chunk corrupted")

const (
	ChunkSize = 4 << 20  // 4 MiB: files are split into chunks of this size
	frameSize = 64 << 10 // 64 KiB: bytes per WriteChunk data message
)

// errNotImplemented is returned by the stubs below until they are written.
var errNotImplemented = errors.New("not implemented")

// ChunkRef describes one chunk of a file and where it is stored.
type ChunkRef struct {
	ID       string
	Index    int      // position in the file, starting at 0
	Length   int64    // bytes in this chunk
	Checksum []byte   // SHA-256 recorded by the coordinator at upload time
	Addrs    []string // chunkserver addresses holding (or assigned) this chunk
}

// FileInfo describes a file stored in the cluster.
type FileInfo struct {
	Path   string
	Size   int64
	Chunks []ChunkRef // in file order; empty for List results
}

// Client talks to the coordinator for metadata and to chunkservers for bytes.
type Client struct {
	coord     gfspb.CoordinatorServiceClient
	coordConn *grpc.ClientConn

	// dial opens a connection to a chunkserver address. Tests replace it to
	// use in-memory (bufconn) connections.
	dial func(addr string) (*grpc.ClientConn, error)

	mu    sync.Mutex                  // protects conns
	conns map[string]*grpc.ClientConn // chunkserver address -> cached connection
}

// New returns a Client for the coordinator at coordAddr (e.g. "localhost:7000").
// No network traffic happens here; gRPC connects lazily on the first RPC.
func New(coordAddr string) (*Client, error) {
	conn, err := grpc.NewClient(coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("create gRPC client for coordinator %s: %w", coordAddr, err)
	}
	return newWithConn(conn, func(addr string) (*grpc.ClientConn, error) {
		return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}), nil
}

// newWithConn builds a Client from an existing coordinator connection and a
// custom chunkserver dial function. Tests use it with bufconn connections.
func newWithConn(coordConn *grpc.ClientConn, dial func(addr string) (*grpc.ClientConn, error)) *Client {
	return &Client{
		coord:     gfspb.NewCoordinatorServiceClient(coordConn),
		coordConn: coordConn,
		dial:      dial,
		conns:     make(map[string]*grpc.ClientConn),
	}
}

// chunkserver returns a client for the chunkserver at addr, reusing a cached
// connection if there is one. Safe to call from multiple goroutines.
func (c *Client) chunkserver(addr string) (gfspb.ChunkServerServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	conn, ok := c.conns[addr]
	if !ok {
		// Dialing under the lock is fine: grpc.NewClient does no network I/O,
		// it only builds the connection object.
		var err error
		conn, err = c.dial(addr)
		if err != nil {
			return nil, fmt.Errorf("connect to chunkserver %s: %w", addr, err)
		}
		c.conns[addr] = conn
	}
	return gfspb.NewChunkServerServiceClient(conn), nil
}

// Close closes the coordinator connection and every cached chunkserver connection.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	errs := []error{c.coordConn.Close()}
	for addr, conn := range c.conns {
		errs = append(errs, conn.Close())
		delete(c.conns, addr)
	}
	return errors.Join(errs...)
}

// refFromPlacement converts the coordinator's description of a chunk to a ChunkRef.
func refFromPlacement(p *gfspb.ChunkPlacement) ChunkRef {
	info := p.GetChunk()
	ref := ChunkRef{
		ID:       info.GetChunkId(),
		Index:    int(info.GetIndex()),
		Length:   info.GetLength(),
		Checksum: info.GetChecksum(),
	}
	for _, r := range p.GetReplicas() {
		ref.Addrs = append(ref.Addrs, r.GetAddress())
	}
	return ref
}

// newOperationID returns a random 128-bit ID as 32 hex characters, for
// CreateFile's operation_id (reuse the same ID when retrying).
func newOperationID() string {
	b := make([]byte, 16)
	rand.Read(b) // never fails since Go 1.24; it crashes the program instead
	return hex.EncodeToString(b)
}

// uploadChunk sends one chunk to the chunkserver with WriteChunk.
func (c *Client) uploadChunk(ctx context.Context, cs gfspb.ChunkServerServiceClient, ref ChunkRef, data []byte) error {
	// Open a stream to the chunkserver
	stream, err := cs.WriteChunk(ctx)
	if err != nil {
		return fmt.Errorf("Open Write Stream: %w", err)
	}

	// Everyone after the first server: the primary forwards to them
	var forward []*gfspb.ReplicaLocation
	if len(ref.Addrs) > 1 {
		for _, addr := range ref.Addrs[1:] {
			forward = append(forward, &gfspb.ReplicaLocation{Address: addr})
		}
	}

	header := &gfspb.WriteChunkRequest{
		Payload: &gfspb.WriteChunkRequest_Header{
			Header: &gfspb.WriteChunkHeader{
				Chunk: &gfspb.ChunkInfo{
					ChunkId:  ref.ID,
					Index:    uint32(ref.Index),
					Length:   ref.Length,
					Checksum: ref.Checksum,
				},
				ForwardTo: forward,
			},
		},
	}
	if err := stream.Send(header); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("Send Header: %w", err)
	}

	// Send the data in 64Kb chunks
	for start := 0; start < len(data); start += frameSize {
		end := min(start+frameSize, len(data))
		msg := &gfspb.WriteChunkRequest{
			Payload: &gfspb.WriteChunkRequest_Data{Data: data[start:end]},
		}

		if err := stream.Send(msg); err != nil {
			if errors.Is(err, io.EOF) {
				break // Server hung up/closed the stream
			}
			return fmt.Errorf("Send Data: %w", err)
		}
	}

	// Close Stream and Get Response
	if _, err := stream.CloseAndRecv(); err != nil {
		return fmt.Errorf("Write Chunk %s: %w", ref.ID, err)
	}

	return nil
}

// Upload splits the local file into ChunkSize chunks, registers it with the
// coordinator as remotePath, writes each chunk to its assigned chunkserver(s),
// and returns the stored file's description.
func (c *Client) Upload(ctx context.Context, localPath, remotePath string) (*FileInfo, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("Open %s: %w", localPath, err)
	}
	defer f.Close()

	// Pass 1. CheckSum every chunk (don't keep the bytes)
	var specs []*gfspb.ChunkSpec
	var size int64
	buf := make([]byte, ChunkSize)
	for {
		n, err := io.ReadFull(f, buf)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("read %s: %w", localPath, err)
		}
		sum := sha256.Sum256(buf[:n])
		specs = append(specs, &gfspb.ChunkSpec{Length: int64(n), Checksum: sum[:]})
		size += int64(n)
		if errors.Is(err, io.ErrUnexpectedEOF) {
			break // Partial chunk was sent
		}
	}

	// Ask the coordinator for a plan
	resp, err := c.coord.CreateFile(ctx, &gfspb.CreateFileRequest{
		Path:        remotePath,
		SizeBytes:   size,
		Chunks:      specs,
		OperationId: newOperationID(),
	})

	if err != nil {
		return nil, fmt.Errorf("Create File %s: %w", remotePath, err)
	}

	// Rewind and upload each chunk where the coordinator said

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("Rewind %s: %w", localPath, err)
	}

	info := &FileInfo{Path: remotePath, Size: size}
	for _, p := range resp.GetChunks() {
		ref := refFromPlacement(p)
		data := buf[:ref.Length]
		if _, err := io.ReadFull(f, data); err != nil {
			return nil, fmt.Errorf("re-read chunk %d: %w", ref.Index, err)
		}
		if len(ref.Addrs) == 0 {
			return nil, fmt.Errorf("coordinator gave no chunkserver for chunk %d", ref.Index)
		}
		cs, err := c.chunkserver(ref.Addrs[0])
		if err != nil {
			return nil, err
		}
		if err := c.uploadChunk(ctx, cs, ref, data); err != nil {
			return nil, err
		}
		info.Chunks = append(info.Chunks, ref)
	}
	return info, nil
}

// downloadChunk reads one chunk with ReadChunk and checks it against ref.
func (c *Client) downloadChunk(ctx context.Context, cs gfspb.ChunkServerServiceClient, ref ChunkRef) ([]byte, error) {
	// Ask Server for the chunk
	stream, err := cs.ReadChunk(ctx, &gfspb.ReadChunkRequest{ChunkId: ref.ID})
	if err != nil {
		return nil, fmt.Errorf("Open Read Stream %w", err)
	}

	// First message should be a header
	first, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("Read Header %s : %w", ref.ID, err)
	}

	header := first.GetHeader()
	if header == nil {
		return nil, fmt.Errorf("Read Chunk %s : first message was not header", ref.ID)
	}

	if header.GetLength() != ref.Length {
		return nil, fmt.Errorf("%w: chunk %s is %d bytes , expected %d", ErrChunkCorrupted, ref.ID, header.GetLength(), ref.Length)
	}

	// Collect data pieces until Server is done sending
	buf := make([]byte, 0, ref.Length)
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("Read chunk %s : %w", ref.ID, err)
		}
		data := msg.GetData()
		if int64(len(buf)+len(data)) > ref.Length {
			return nil, fmt.Errorf("%w: chunk %s received more data than %d bytes", ErrChunkCorrupted, ref.ID, ref.Length)
		}
		buf = append(buf, data...)
	}

	// Verify against what we recorded at upload time
	if int64(len(buf)) != ref.Length {
		return nil, fmt.Errorf("%w: chunk %s received %d bytes, expected %d", ErrChunkCorrupted, ref.ID, len(buf), ref.Length)
	}
	sum := sha256.Sum256(buf)
	if !bytes.Equal(sum[:], ref.Checksum) {
		return nil, fmt.Errorf("%w: chunk %s checksum mismatch", ErrChunkCorrupted, ref.ID)
	}
	return buf, nil
}

// fetchChunk downloads one chunk from any chunkserver that holds it.
func (c *Client) fetchChunk(ctx context.Context, ref ChunkRef) ([]byte, error) {
	lastErr := fmt.Errorf("chunk %s has no replicas", ref.ID)
	for _, addr := range ref.Addrs {
		cs, err := c.chunkserver(addr)
		if err != nil {
			lastErr = err
			continue // can't connect: try the next copy
		}
		data, err := c.downloadChunk(ctx, cs, ref)
		if err == nil {
			return data, nil // got a good copy
		}
		lastErr = fmt.Errorf("replica %s: %w", addr, err)
	}
	return nil, lastErr
}

// Download fetches remotePath's chunks in order and writes the file to outPath.
func (c *Client) Download(ctx context.Context, remotePath, outPath string) error {
	// 1. Ask the coordinator what the file is made of
	resp, err := c.coord.GetFileMetadata(ctx, &gfspb.GetFileMetadataRequest{Path: remotePath})
	if err != nil {
		return fmt.Errorf("get metadata for %s: %w", remotePath, err)
	}
	meta := resp.GetFile()

	// 2. Temp file next to the final one, cleaned up on failure
	f, err := os.CreateTemp(filepath.Dir(outPath), filepath.Base(outPath)+"-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmp := f.Name()
	done := false
	defer func() {
		if !done {
			f.Close()
			os.Remove(tmp)
		}
	}()

	// 3. Fetch each chunk in order, verify, append
	var written int64
	for i, p := range meta.GetChunks() {
		ref := refFromPlacement(p)
		if ref.Index != i {
			return fmt.Errorf("metadata out of order: position %d has chunk index %d", i, ref.Index)
		}
		data, err := c.fetchChunk(ctx, ref)
		if err != nil {
			return err
		}
		if _, err := f.Write(data); err != nil {
			return fmt.Errorf("write %s: %w", tmp, err)
		}
		written += int64(len(data))
	}
	if written != meta.GetSizeBytes() {
		return fmt.Errorf("wrote %d bytes, coordinator says %d", written, meta.GetSizeBytes())
	}

	// 4. Durable + atomic, same as before
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, outPath); err != nil {
		return fmt.Errorf("rename to %s: %w", outPath, err)
	}
	done = true
	return nil
}

// List returns the files whose path starts with prefix, sorted by path.
func (c *Client) List(ctx context.Context, prefix string) ([]FileInfo, error) {
	resp, err := c.coord.ListFiles(ctx, &gfspb.ListFilesRequest{Prefix: prefix})
	if err != nil {
		return nil, fmt.Errorf("list files: %w", err)
	}
	files := make([]FileInfo, 0, len(resp.GetFiles()))
	for _, f := range resp.GetFiles() {
		files = append(files, FileInfo{Path: f.GetPath(), Size: f.GetSizeBytes()})
	}
	return files, nil
}

// Delete removes path from the cluster.
func (c *Client) Delete(ctx context.Context, path string) error {
	if _, err := c.coord.DeleteFile(ctx, &gfspb.DeleteFileRequest{Path: path}); err != nil {
		return fmt.Errorf("delete %s: %w", path, err)
	}
	return nil
}

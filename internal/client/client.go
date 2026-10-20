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

// Client uploads and downloads files by talking to a chunkserver over gRPC.
type Client struct {
	cs   gfspb.ChunkServerServiceClient
	conn *grpc.ClientConn
}

// New returns a Client for the chunkserver at addr (e.g. "localhost:7001").
// No network traffic happens here; gRPC connects lazily on the first RPC.
func New(addr string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("create gRPC client for %s: %w", addr, err)
	}
	return newWithConn(conn), nil
}

// newWithConn wraps an existing connection. Tests use it to pass an in-memory
// (bufconn) connection instead of a real network address.
func newWithConn(conn *grpc.ClientConn) *Client {
	return &Client{cs: gfspb.NewChunkServerServiceClient(conn), conn: conn}
}

// Close releases the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

// newChunkID returns a random 128-bit ID as 32 hex characters, which only
// uses [0-9a-f] and so passes the chunkserver's chunk ID whitelist.
func newChunkID() string {
	b := make([]byte, 16)
	rand.Read(b) // never fails since Go 1.24; it crashes the program instead
	return hex.EncodeToString(b)
}

// uploadChunk sends one chunk to the chunkserver with WriteChunk.
func (c *Client) uploadChunk(ctx context.Context, ref ChunkRef, data []byte) error {
	// Open a stream to the chunkserver
	stream, err := c.cs.WriteChunk(ctx)
	if err != nil {
		return fmt.Errorf("Open Write Stream: %w", err)
	}

	// Send The header
	header := &gfspb.WriteChunkRequest{
		Payload: &gfspb.WriteChunkRequest_Header{
			Header: &gfspb.WriteChunkHeader{
				Chunk: &gfspb.ChunkInfo{
					ChunkId:  ref.ID,
					Index:    uint32(ref.Index),
					Length:   ref.Length,
					Checksum: ref.Checksum,
				},
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

// Upload splits the file at localPath into ChunkSize chunks, uploads each one,
// and returns a Manifest describing them in order.
func (c *Client) Upload(ctx context.Context, localPath string) (*Manifest, error) {
	// Open the local file
	f, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", localPath, err)
	}
	defer f.Close() // closes automatically when Upload returns

	m := &Manifest{Name: filepath.Base(localPath)}
	buf := make([]byte, ChunkSize) // one 4MB buffer , reused for every chunk

	for index := 0; ; index++ {
		// Read up to 4MB
		n, err := io.ReadFull(f, buf)
		if errors.Is(err, io.EOF) {
			break // done reading file
		}

		if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("read %s: %w", localPath, err)
		}
		data := buf[:n]

		// Fingerprint the chunk , give it an ID, Upload it
		sum := sha256.Sum256(data)
		ref := ChunkRef{
			ID:       newChunkID(),
			Index:    index,
			Length:   int64(len(data)),
			Checksum: sum[:],
		}

		if err := c.uploadChunk(ctx, ref, data); err != nil {
			return nil, err
		}

		// Record it in manifest
		m.Chunks = append(m.Chunks, ref)
		m.Size += int64(n)

		if errors.Is(err, io.ErrUnexpectedEOF) {
			break // partial chunk
		}
	}
	return m, nil
}

// downloadChunk reads one chunk with ReadChunk and checks it against ref.
func (c *Client) downloadChunk(ctx context.Context, ref ChunkRef) ([]byte, error) {
	// Ask Server for the chunk
	stream, err := c.cs.ReadChunk(ctx, &gfspb.ReadChunkRequest{ChunkId: ref.ID})
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

// Download fetches every chunk listed in m, in order, and writes the file to outPath.
func (c *Client) Download(ctx context.Context, m *Manifest, outPath string) error {
	// Create a Temp File next to where the final file will go
	f, err := os.CreateTemp(filepath.Dir(outPath), filepath.Base(outPath)+"-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmp := f.Name()

	// if anything fails below, clean up the temp file automatically
	done := false
	defer func() {
		if !done {
			f.Close()
			os.Remove(tmp)
		}
	}()

	// Download each chunk in order and append it
	var written int64
	for i, ref := range m.Chunks {
		if ref.Index != i {
			return fmt.Errorf("manifest out of order: chunk %d has index %d", i, ref.Index)
		}
		data, err := c.downloadChunk(ctx, ref)
		if err != nil {
			return err
		}
		if _, err := f.Write(data); err != nil {
			return fmt.Errorf("Write %s: %w", tmp, err)
		}
		written += int64(len(data))
	}

	if written != m.Size {
		return fmt.Errorf("wrote %d bytes, manifest says %d ", written, m.Size)
	}

	// Make it durable, then automatically put it in place
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

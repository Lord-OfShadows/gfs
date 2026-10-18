package chunkserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Lord-OfShadows/gfs/proto/gfspb"
)

// startTestServer runs a real gRPC chunkserver over an in-memory connection
// and returns a client connected to it plus the Store behind it.
func startTestServer(t *testing.T) (gfspb.ChunkServerServiceClient, *Store) {
	t.Helper()

	store, err := NewStore(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	lis := bufconn.Listen(1 << 20) // 1 MiB in-memory buffer
	srv := grpc.NewServer()
	gfspb.RegisterChunkServerServiceServer(srv, NewServer("test-node", store))
	go srv.Serve(lis)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}

	t.Cleanup(func() {
		conn.Close()
		srv.Stop()
	})
	return gfspb.NewChunkServerServiceClient(conn), store
}

// headerMsg builds the first message of a WriteChunk stream.
func headerMsg(id string, length int64, checksum []byte) *gfspb.WriteChunkRequest {
	return &gfspb.WriteChunkRequest{Payload: &gfspb.WriteChunkRequest_Header{
		Header: &gfspb.WriteChunkHeader{
			OperationId: "op-" + id,
			Chunk:       &gfspb.ChunkInfo{ChunkId: id, Length: length, Checksum: checksum},
		},
	}}
}

// dataMsgs splits data into frames of at most frameSize bytes.
func dataMsgs(data []byte, frameSize int) []*gfspb.WriteChunkRequest {
	var msgs []*gfspb.WriteChunkRequest
	for start := 0; start < len(data); start += frameSize {
		end := min(start+frameSize, len(data))
		msgs = append(msgs, &gfspb.WriteChunkRequest{
			Payload: &gfspb.WriteChunkRequest_Data{Data: data[start:end]},
		})
	}
	return msgs
}

func sha(data []byte) []byte {
	sum := sha256.Sum256(data)
	return sum[:]
}

// sendWrite sends msgs on a WriteChunk stream and returns the server's reply.
// If the server rejects the stream early, Send starts returning io.EOF; the
// real error is then delivered by CloseAndRecv, so we stop sending and ask.
func sendWrite(t *testing.T, client gfspb.ChunkServerServiceClient, msgs []*gfspb.WriteChunkRequest) (*gfspb.WriteChunkResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.WriteChunk(ctx)
	if err != nil {
		t.Fatalf("open WriteChunk stream: %v", err)
	}
	for _, m := range msgs {
		if err := stream.Send(m); err != nil {
			break
		}
	}
	return stream.CloseAndRecv()
}

func TestWriteChunk(t *testing.T) {
	data := pattern(200 << 10) // 200 KiB: four frames at 64 KiB
	const frame = 64 << 10

	// concat joins a header with data frames into one message list.
	concat := func(h *gfspb.WriteChunkRequest, rest []*gfspb.WriteChunkRequest) []*gfspb.WriteChunkRequest {
		return append([]*gfspb.WriteChunkRequest{h}, rest...)
	}

	tests := []struct {
		name     string
		id       string
		msgs     []*gfspb.WriteChunkRequest
		wantCode codes.Code
	}{
		{
			name:     "happy path in several frames",
			id:       "chunk-ok",
			msgs:     concat(headerMsg("chunk-ok", int64(len(data)), sha(data)), dataMsgs(data, frame)),
			wantCode: codes.OK,
		},
		{
			name:     "checksum mismatch",
			id:       "chunk-badsum",
			msgs:     concat(headerMsg("chunk-badsum", int64(len(data)), sha([]byte("other bytes"))), dataMsgs(data, frame)),
			wantCode: codes.DataLoss,
		},
		{
			name:     "more bytes than header length",
			id:       "chunk-long",
			msgs:     concat(headerMsg("chunk-long", int64(len(data))-1, sha(data)), dataMsgs(data, frame)),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "fewer bytes than header length",
			id:       "chunk-short",
			msgs:     concat(headerMsg("chunk-short", int64(len(data))+1, sha(data)), dataMsgs(data, frame)),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "first message is data, not header",
			id:       "chunk-noheader",
			msgs:     dataMsgs(data, frame),
			wantCode: codes.InvalidArgument,
		},
		{
			name:     "invalid chunk ID",
			id:       "../evil",
			msgs:     concat(headerMsg("../evil", int64(len(data)), sha(data)), dataMsgs(data, frame)),
			wantCode: codes.InvalidArgument,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, store := startTestServer(t)

			resp, err := sendWrite(t, client, tc.msgs)
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("WriteChunk code = %v (err: %v), want %v", got, err, tc.wantCode)
			}

			if tc.wantCode != codes.OK {
				// A rejected write must leave nothing behind.
				if _, err := store.Read(tc.id); !errors.Is(err, ErrChunkNotFound) && !errors.Is(err, ErrInvalidChunkID) {
					t.Errorf("after rejected write, store.Read(%q) error = %v, want not found", tc.id, err)
				}
				return
			}

			if resp.GetChunkId() != tc.id || resp.GetBytesWritten() != int64(len(data)) {
				t.Errorf("response = {%q, %d}, want {%q, %d}", resp.GetChunkId(), resp.GetBytesWritten(), tc.id, len(data))
			}
			got, err := store.Read(tc.id)
			if err != nil {
				t.Fatalf("store.Read(%q): %v", tc.id, err)
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("stored %d bytes, want the %d bytes that were sent", len(got), len(data))
			}
		})
	}
}

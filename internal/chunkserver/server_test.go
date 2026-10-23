package chunkserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
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

// readChunk calls ReadChunk and collects the whole response stream: every
// message in order, until the server ends the stream. A non-nil error is the
// server's gRPC status (e.g. NotFound) or a transport failure.
func readChunk(t *testing.T, client gfspb.ChunkServerServiceClient, id string) ([]*gfspb.ReadChunkResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.ReadChunk(ctx, &gfspb.ReadChunkRequest{ChunkId: id})
	if err != nil {
		return nil, err
	}
	var msgs []*gfspb.ReadChunkResponse
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return msgs, nil // server finished normally
		}
		if err != nil {
			return msgs, err // server returned an error status
		}
		msgs = append(msgs, msg)
	}
}

func TestReadChunk(t *testing.T) {
	const maxFrame = 64 << 10 // the protocol's per-message data limit

	t.Run("round trip", func(t *testing.T) {
		sizes := []struct {
			name string
			size int
		}{
			{name: "empty", size: 0},
			{name: "one byte", size: 1},
			{name: "exactly 64 KiB", size: 64 << 10},
			{name: "64 KiB plus 1", size: 64<<10 + 1},
			{name: "4 MiB", size: 4 << 20},
		}

		for _, tc := range sizes {
			t.Run(tc.name, func(t *testing.T) {
				client, store := startTestServer(t)
				const id = "chunk-read"
				data := pattern(tc.size)
				if err := store.Write(id, data); err != nil {
					t.Fatalf("store.Write: %v", err)
				}

				msgs, err := readChunk(t, client, id)
				if err != nil {
					t.Fatalf("ReadChunk: %v", err)
				}
				if len(msgs) == 0 {
					t.Fatal("ReadChunk sent no messages, want a header first")
				}

				// First message: header describing the whole chunk.
				header := msgs[0].GetHeader()
				if header == nil {
					t.Fatalf("first message is %T, want a header", msgs[0].GetPayload())
				}
				if header.GetChunkId() != id {
					t.Errorf("header chunk_id = %q, want %q", header.GetChunkId(), id)
				}
				if header.GetLength() != int64(len(data)) {
					t.Errorf("header length = %d, want %d", header.GetLength(), len(data))
				}
				if !bytes.Equal(header.GetChecksum(), sha(data)) {
					t.Errorf("header checksum = %x, want SHA-256 %x", header.GetChecksum(), sha(data))
				}

				// Every following message: a data frame of at most 64 KiB.
				var got []byte
				for i, m := range msgs[1:] {
					frame, ok := m.GetPayload().(*gfspb.ReadChunkResponse_Data)
					if !ok {
						t.Fatalf("message %d is %T, want data", i+1, m.GetPayload())
					}
					if len(frame.Data) > maxFrame {
						t.Errorf("message %d has %d bytes, want at most %d", i+1, len(frame.Data), maxFrame)
					}
					got = append(got, frame.Data...)
				}

				if !bytes.Equal(got, data) {
					t.Fatalf("reassembled %d bytes, want the %d bytes that were stored", len(got), len(data))
				}
			})
		}
	})

	errorCases := []struct {
		name     string
		id       string
		wantCode codes.Code
	}{
		{name: "missing chunk", id: "does-not-exist", wantCode: codes.NotFound},
		{name: "invalid chunk ID", id: "../evil", wantCode: codes.InvalidArgument},
	}
	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			client, _ := startTestServer(t)

			msgs, err := readChunk(t, client, tc.id)
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("ReadChunk(%q) code = %v (err: %v), want %v", tc.id, got, err, tc.wantCode)
			}
			if len(msgs) != 0 {
				t.Errorf("ReadChunk(%q) sent %d messages before failing, want 0", tc.id, len(msgs))
			}
		})
	}
}

// peerNode is one chunkserver in a multi-server test.
type peerNode struct {
	addr  string
	store *Store
	srv   *grpc.Server
	lis   *bufconn.Listener
}

// startPeers runs n chunkservers over bufconn. Each one's dial function maps
// the other servers' addresses ("cs1:7001", ...) to their in-memory listeners,
// so they can forward chunks to each other.
func startPeers(t *testing.T, n int) []*peerNode {
	t.Helper()

	nodes := make([]*peerNode, n)
	listeners := map[string]*bufconn.Listener{}
	for i := range nodes {
		addr := fmt.Sprintf("cs%d:7001", i+1)
		nodes[i] = &peerNode{addr: addr, lis: bufconn.Listen(1 << 20)}
		listeners[addr] = nodes[i].lis
	}
	dial := func(addr string) (*grpc.ClientConn, error) {
		lis, ok := listeners[addr]
		if !ok {
			return nil, fmt.Errorf("unknown test peer %s", addr)
		}
		return grpc.NewClient("passthrough:///"+addr,
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}

	for i, node := range nodes {
		store, err := NewStore(filepath.Join(t.TempDir(), fmt.Sprintf("data%d", i+1)))
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}
		s := NewServer(fmt.Sprintf("cs%d", i+1), store)
		s.SetDial(dial) // before serving
		srv := grpc.NewServer()
		gfspb.RegisterChunkServerServiceServer(srv, s)
		go srv.Serve(node.lis)
		node.store, node.srv = store, srv
		t.Cleanup(func() {
			srv.Stop()
			s.Close()
		})
	}
	return nodes
}

// clientFor returns a client connected to one test node.
func clientFor(t *testing.T, node *peerNode) gfspb.ChunkServerServiceClient {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+node.addr,
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return node.lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return gfspb.NewChunkServerServiceClient(conn)
}

// forwardingWrite builds a WriteChunk stream for data that asks the receiving
// server to forward it to the given addresses.
func forwardingWrite(id string, data []byte, forwardTo ...string) []*gfspb.WriteChunkRequest {
	header := &gfspb.WriteChunkHeader{
		OperationId: "op-" + id,
		Chunk:       &gfspb.ChunkInfo{ChunkId: id, Length: int64(len(data)), Checksum: sha(data)},
	}
	for _, addr := range forwardTo {
		header.ForwardTo = append(header.ForwardTo, &gfspb.ReplicaLocation{Address: addr})
	}
	msgs := []*gfspb.WriteChunkRequest{{Payload: &gfspb.WriteChunkRequest_Header{Header: header}}}
	return append(msgs, dataMsgs(data, 64<<10)...)
}

func TestWriteChunkForwardsToReplicas(t *testing.T) {
	nodes := startPeers(t, 3)
	data := pattern(200 << 10) // several frames
	const id = "chunk-fwd"

	_, err := sendWrite(t, clientFor(t, nodes[0]), forwardingWrite(id, data, nodes[1].addr, nodes[2].addr))
	if err != nil {
		t.Fatalf("WriteChunk to %s forwarding to [%s %s]: %v", nodes[0].addr, nodes[1].addr, nodes[2].addr, err)
	}

	for _, node := range nodes {
		got, err := node.store.Read(id)
		if err != nil {
			t.Errorf("%s: chunk not stored: %v", node.addr, err)
			continue
		}
		if !bytes.Equal(got, data) {
			t.Errorf("%s: stored %d bytes, want the %d bytes that were sent", node.addr, len(got), len(data))
		}
	}
}

func TestWriteChunkFailsWhenReplicaDown(t *testing.T) {
	nodes := startPeers(t, 3)
	nodes[2].srv.Stop() // the last replica in the chain is unreachable

	data := pattern(100 << 10)
	_, err := sendWrite(t, clientFor(t, nodes[0]), forwardingWrite("chunk-down", data, nodes[1].addr, nodes[2].addr))
	if err == nil {
		t.Fatalf("WriteChunk succeeded although replica %s is down, want an error", nodes[2].addr)
	}
}

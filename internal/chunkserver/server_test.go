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
	"sync"
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
	return startPeersWithCoordinator(t, n, nil)
}

// startPeersWithCoordinator is startPeers with every server reporting stored
// chunks to coord (nil = no coordinator).
func startPeersWithCoordinator(t *testing.T, n int, coord gfspb.CoordinatorServiceClient) []*peerNode {
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
		if coord != nil {
			s.SetCoordinator(coord)
		}
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

// reportRecorder is a fake coordinator that records ReportChunk calls.
// Embedding the interface satisfies the methods we do not need.
type reportRecorder struct {
	gfspb.CoordinatorServiceClient
	mu      sync.Mutex
	reports []*gfspb.ReportChunkRequest
}

func (f *reportRecorder) ReportChunk(ctx context.Context, req *gfspb.ReportChunkRequest, _ ...grpc.CallOption) (*gfspb.ReportChunkResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reports = append(f.reports, req)
	return &gfspb.ReportChunkResponse{}, nil
}

// reportsFrom returns the reports node made, in order.
func (f *reportRecorder) reportsFrom(node string) []*gfspb.ReportChunkRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*gfspb.ReportChunkRequest
	for _, r := range f.reports {
		if r.GetNodeId() == node {
			out = append(out, r)
		}
	}
	return out
}

func copyReq(id string, data []byte, source string) *gfspb.CopyChunkRequest {
	return &gfspb.CopyChunkRequest{
		Chunk:  &gfspb.ChunkInfo{ChunkId: id, Length: int64(len(data)), Checksum: sha(data)},
		Source: &gfspb.ReplicaLocation{Address: source},
	}
}

func rpcCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func wantNotStored(t *testing.T, node *peerNode, id string) {
	t.Helper()
	if _, err := node.store.Read(id); !errors.Is(err, ErrChunkNotFound) {
		t.Errorf("%s: Read(%q) error = %v, want ErrChunkNotFound", node.addr, id, err)
	}
}

func TestCopyChunk(t *testing.T) {
	coord := &reportRecorder{}
	nodes := startPeersWithCoordinator(t, 2, coord)
	src, dst := nodes[0], nodes[1]
	data := pattern(200 << 10) // several frames
	const id = "chunk-copy"
	if err := src.store.Write(id, data); err != nil {
		t.Fatalf("seed %s: %v", src.addr, err)
	}

	resp, err := clientFor(t, dst).CopyChunk(rpcCtx(t), copyReq(id, data, src.addr))
	if err != nil {
		t.Fatalf("CopyChunk: %v", err)
	}
	if resp.GetChunkId() != id || resp.GetBytesCopied() != int64(len(data)) {
		t.Errorf("CopyChunk = {%q, %d}, want {%q, %d}", resp.GetChunkId(), resp.GetBytesCopied(), id, len(data))
	}

	got, err := dst.store.Read(id)
	if err != nil {
		t.Fatalf("%s: chunk not stored: %v", dst.addr, err)
	}
	if !bytes.Equal(got, data) {
		t.Errorf("%s: stored %d bytes that differ from the source's %d", dst.addr, len(got), len(data))
	}

	reports := coord.reportsFrom("cs2")
	if len(reports) != 1 {
		t.Fatalf("coordinator got %d reports from cs2, want 1", len(reports))
	}
	if c := reports[0].GetChunk(); c.GetChunkId() != id || c.GetLength() != int64(len(data)) || !bytes.Equal(c.GetChecksum(), sha(data)) {
		t.Errorf("report = {%q, %d, %x}, want {%q, %d, %x}", c.GetChunkId(), c.GetLength(), c.GetChecksum(), id, len(data), sha(data))
	}
}

func TestCopyChunkWrongChecksum(t *testing.T) {
	coord := &reportRecorder{}
	nodes := startPeersWithCoordinator(t, 2, coord)
	data := pattern(100 << 10)
	const id = "chunk-badsum"
	if err := nodes[0].store.Write(id, data); err != nil {
		t.Fatalf("seed: %v", err)
	}

	req := copyReq(id, data, nodes[0].addr)
	req.Chunk.Checksum = sha([]byte("something else"))
	_, err := clientFor(t, nodes[1]).CopyChunk(rpcCtx(t), req)
	if got := status.Code(err); got != codes.DataLoss {
		t.Fatalf("CopyChunk with wrong checksum: code = %v (err: %v), want DataLoss", got, err)
	}
	wantNotStored(t, nodes[1], id)
	if n := len(coord.reportsFrom("cs2")); n != 0 {
		t.Errorf("cs2 sent %d reports for a rejected copy, want 0", n)
	}
}

func TestCopyChunkSourceMissing(t *testing.T) {
	nodes := startPeersWithCoordinator(t, 2, &reportRecorder{})
	const id = "chunk-nowhere"

	_, err := clientFor(t, nodes[1]).CopyChunk(rpcCtx(t), copyReq(id, []byte("data"), nodes[0].addr))
	if got := status.Code(err); got != codes.Unavailable {
		t.Fatalf("CopyChunk from a source without the chunk: code = %v (err: %v), want Unavailable", got, err)
	}
	wantNotStored(t, nodes[1], id)
}

func TestCopyChunkValidation(t *testing.T) {
	nodes := startPeers(t, 2)
	client := clientFor(t, nodes[1])

	noID := copyReq("x", []byte("data"), nodes[0].addr)
	noID.Chunk.ChunkId = ""
	noSource := copyReq("chunk-x", []byte("data"), nodes[0].addr)
	noSource.Source = nil

	for name, req := range map[string]*gfspb.CopyChunkRequest{"missing chunk id": noID, "missing source": noSource} {
		t.Run(name, func(t *testing.T) {
			_, err := client.CopyChunk(rpcCtx(t), req)
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Fatalf("code = %v (err: %v), want InvalidArgument", got, err)
			}
		})
	}
}

// WriteChunk reports through reportChunk too; no other test has a coordinator.
func TestWriteChunkReportsToCoordinator(t *testing.T) {
	coord := &reportRecorder{}
	nodes := startPeersWithCoordinator(t, 1, coord)
	data := pattern(100 << 10)

	if _, err := sendWrite(t, clientFor(t, nodes[0]), forwardingWrite("chunk-rep", data)); err != nil {
		t.Fatalf("WriteChunk: %v", err)
	}
	reports := coord.reportsFrom("cs1")
	if len(reports) != 1 || reports[0].GetChunk().GetChunkId() != "chunk-rep" || !bytes.Equal(reports[0].GetChunk().GetChecksum(), sha(data)) {
		t.Fatalf("reports from cs1 = %v, want one for chunk-rep with its checksum", reports)
	}
}

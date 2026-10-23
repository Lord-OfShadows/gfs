package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Lord-OfShadows/gfs/internal/chunkserver"
	"github.com/Lord-OfShadows/gfs/internal/coordinator"
	"github.com/Lord-OfShadows/gfs/proto/gfspb"
)

// testChunkserverAddr is the address the coordinator hands out for the
// in-memory chunkserver. The client's test dial function maps it to bufconn.
// The coordinator's own connections (used only for chunk cleanup in
// DeleteFile) dial it for real; port 1 refuses immediately, so cleanup fails
// fast and is just logged. Chunk cleanup is tested in the coordinator package.
const testChunkserverAddr = "127.0.0.1:1"

// cluster is an in-memory coordinator plus one chunkserver.
type cluster struct {
	coordLis *bufconn.Listener
	csLis    *bufconn.Listener
	store    *chunkserver.Store // the chunkserver's disk, for tampering in tests
}

func serve(t *testing.T, register func(*grpc.Server)) *bufconn.Listener {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	register(srv)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis
}

func dialBufconn(lis *bufconn.Listener) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

// startCluster runs a coordinator and a chunkserver that reports to it.
func startCluster(t *testing.T) *cluster {
	t.Helper()

	coordLis := serve(t, func(s *grpc.Server) {
		gfspb.RegisterCoordinatorServiceServer(s,
			coordinator.NewServer(coordinator.NewMetadata(), []string{testChunkserverAddr}))
	})

	store, err := chunkserver.NewStore(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	csToCoord, err := dialBufconn(coordLis)
	if err != nil {
		t.Fatalf("dial coordinator: %v", err)
	}
	t.Cleanup(func() { csToCoord.Close() })
	cs := chunkserver.NewServer("cs1", store)
	cs.SetCoordinator(gfspb.NewCoordinatorServiceClient(csToCoord))
	csLis := serve(t, func(s *grpc.Server) { gfspb.RegisterChunkServerServiceServer(s, cs) })

	return &cluster{coordLis: coordLis, csLis: csLis, store: store}
}

// newClient returns a Client connected to the cluster. dial lets a test
// override how chunkserver addresses are reached (nil = the in-memory one).
func (cl *cluster) newClient(t *testing.T, dial func(addr string) (*grpc.ClientConn, error)) *Client {
	t.Helper()
	coordConn, err := dialBufconn(cl.coordLis)
	if err != nil {
		t.Fatalf("dial coordinator: %v", err)
	}
	if dial == nil {
		dial = func(addr string) (*grpc.ClientConn, error) {
			if addr != testChunkserverAddr {
				return nil, errors.New("unknown chunkserver " + addr)
			}
			return dialBufconn(cl.csLis)
		}
	}
	c := newWithConn(coordConn, dial)
	t.Cleanup(func() { c.Close() })
	return c
}

// randomBytes returns n pseudo-random bytes, the same for the same n. Random
// (not repeating) content means a bug that swaps or repeats chunks changes
// the output and is caught by the byte comparison.
func randomBytes(n int) []byte {
	var seed [32]byte
	seed[0], seed[1], seed[2] = byte(n), byte(n>>8), byte(n>>16)
	b := make([]byte, n)
	rand.NewChaCha8(seed).Read(b)
	return b
}

func writeTempFile(t *testing.T, name string, data []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, data, 0o644); err != nil {
		t.Fatalf("write input file: %v", err)
	}
	return p
}

func testContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func listPaths(t *testing.T, c *Client, prefix string) []string {
	t.Helper()
	files, err := c.List(testContext(t), prefix)
	if err != nil {
		t.Fatalf("List(%q): %v", prefix, err)
	}
	var paths []string
	for _, f := range files {
		paths = append(paths, f.Path)
	}
	return paths
}

func TestUploadDownloadRoundTrip(t *testing.T) {
	tests := []struct {
		name       string
		size       int
		wantChunks int
	}{
		{name: "empty", size: 0, wantChunks: 0},
		{name: "one byte", size: 1, wantChunks: 1},
		{name: "exactly one chunk", size: ChunkSize, wantChunks: 1},
		{name: "one chunk plus 1 byte", size: ChunkSize + 1, wantChunks: 2},
		{name: "10 MiB", size: 10 << 20, wantChunks: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := startCluster(t).newClient(t, nil)
			ctx := testContext(t)
			data := randomBytes(tc.size)
			in := writeTempFile(t, "input.bin", data)

			info, err := c.Upload(ctx, in, "input.bin")
			if err != nil {
				t.Fatalf("Upload: %v", err)
			}
			if info.Path != "input.bin" || info.Size != int64(tc.size) || len(info.Chunks) != tc.wantChunks {
				t.Fatalf("FileInfo = {path %q, size %d, %d chunks}, want {%q, %d, %d chunks}",
					info.Path, info.Size, len(info.Chunks), "input.bin", tc.size, tc.wantChunks)
			}
			for i, ref := range info.Chunks {
				if ref.Index != i || len(ref.Addrs) == 0 {
					t.Errorf("chunk %d = {Index %d, Addrs %v}, want index %d and an address", i, ref.Index, ref.Addrs, i)
				}
			}

			out := filepath.Join(t.TempDir(), "output.bin")
			if err := c.Download(ctx, "input.bin", out); err != nil {
				t.Fatalf("Download: %v", err)
			}
			got, err := os.ReadFile(out)
			if err != nil {
				t.Fatalf("read downloaded file: %v", err)
			}
			if !bytes.Equal(got, data) {
				t.Fatalf("downloaded %d bytes, want the %d bytes that were uploaded", len(got), len(data))
			}
		})
	}
}

func TestDownloadDetectsCorruptedChunk(t *testing.T) {
	cl := startCluster(t)
	c := cl.newClient(t, nil)
	ctx := testContext(t)
	in := writeTempFile(t, "input.bin", randomBytes(ChunkSize+1)) // two chunks

	info, err := c.Upload(ctx, in, "input.bin")
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if len(info.Chunks) == 0 {
		t.Fatal("Upload returned no chunks")
	}

	// Simulate disk corruption: same length, one bit flipped. The chunkserver
	// serves these bytes with a header checksum computed from them, so only
	// the client's check against the coordinator's checksum can catch it.
	id := info.Chunks[0].ID
	stored, err := cl.store.Read(id)
	if err != nil {
		t.Fatalf("store.Read(%q): %v", id, err)
	}
	stored[0] ^= 0x01
	if err := cl.store.Write(id, stored); err != nil {
		t.Fatalf("store.Write(%q): %v", id, err)
	}

	out := filepath.Join(t.TempDir(), "output.bin")
	err = c.Download(ctx, "input.bin", out)
	if !errors.Is(err, ErrChunkCorrupted) {
		t.Fatalf("Download of a corrupted chunk: error = %v, want ErrChunkCorrupted", err)
	}
	if _, statErr := os.Stat(out); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("failed Download left a file at %s (stat error = %v), want none", out, statErr)
	}
}

func TestFailedUploadIsNotListed(t *testing.T) {
	cl := startCluster(t)
	ctx := testContext(t)
	in := writeTempFile(t, "input.bin", randomBytes(1000))

	// A client that cannot reach any chunkserver: CreateFile succeeds, every
	// chunk write fails, so the file must stay pending (invisible).
	broken := cl.newClient(t, func(addr string) (*grpc.ClientConn, error) {
		return nil, errors.New("chunkserver unreachable")
	})
	if _, err := broken.Upload(ctx, in, "failed.bin"); err == nil {
		t.Fatal("Upload with unreachable chunkservers succeeded, want an error")
	} else if errors.Is(err, errNotImplemented) {
		t.Fatalf("Upload returned %v; implement it first", err)
	}

	c := cl.newClient(t, nil)
	if paths := listPaths(t, c, ""); slices.Contains(paths, "failed.bin") {
		t.Errorf("List = %v, want the failed upload excluded", paths)
	}
	err := c.Download(ctx, "failed.bin", filepath.Join(t.TempDir(), "out.bin"))
	if status.Code(err) != codes.NotFound {
		t.Errorf("Download(failed upload): error = %v, want gRPC NotFound", err)
	}
}

func TestListAfterUpload(t *testing.T) {
	c := startCluster(t).newClient(t, nil)
	ctx := testContext(t)

	for _, name := range []string{"docs-b.txt", "docs-a.txt", "img.png"} {
		in := writeTempFile(t, name, randomBytes(100+len(name)))
		if _, err := c.Upload(ctx, in, name); err != nil {
			t.Fatalf("Upload(%s): %v", name, err)
		}
	}

	if got, want := listPaths(t, c, ""), []string{"docs-a.txt", "docs-b.txt", "img.png"}; !slices.Equal(got, want) {
		t.Errorf("List(\"\") = %v, want %v", got, want)
	}
	if got, want := listPaths(t, c, "docs-"), []string{"docs-a.txt", "docs-b.txt"}; !slices.Equal(got, want) {
		t.Errorf("List(\"docs-\") = %v, want %v", got, want)
	}
}

func TestDeleteThenDownloadIsNotFound(t *testing.T) {
	c := startCluster(t).newClient(t, nil)
	ctx := testContext(t)
	in := writeTempFile(t, "input.bin", randomBytes(5000))

	if _, err := c.Upload(ctx, in, "input.bin"); err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if err := c.Delete(ctx, "input.bin"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	err := c.Download(ctx, "input.bin", filepath.Join(t.TempDir(), "out.bin"))
	if status.Code(err) != codes.NotFound {
		t.Fatalf("Download after Delete: error = %v, want gRPC NotFound", err)
	}
	if paths := listPaths(t, c, ""); len(paths) != 0 {
		t.Errorf("List after Delete = %v, want empty", paths)
	}
	if err := c.Delete(ctx, "input.bin"); status.Code(err) != codes.NotFound {
		t.Errorf("second Delete: error = %v, want gRPC NotFound", err)
	}
}

// csNode is one chunkserver in a replicated test cluster.
type csNode struct {
	addr  string
	store *chunkserver.Store
	srv   *grpc.Server
}

// replicatedCluster is an in-memory coordinator plus several chunkservers
// that report to it and can forward chunks to each other.
type replicatedCluster struct {
	coordLis *bufconn.Listener
	nodes    []*csNode
	dial     func(addr string) (*grpc.ClientConn, error) // maps node addresses to bufconn
}

// startReplicatedCluster runs a coordinator and n chunkservers. Node addresses
// are 127.0.0.1:1, :2, ... so the coordinator's own cleanup connections (it
// cannot use our bufconn dialer) are refused immediately instead of hanging.
func startReplicatedCluster(t *testing.T, n int) *replicatedCluster {
	t.Helper()

	nodes := make([]*csNode, n)
	addrs := make([]string, n)
	listeners := map[string]*bufconn.Listener{}
	for i := range nodes {
		addrs[i] = fmt.Sprintf("127.0.0.1:%d", i+1)
		nodes[i] = &csNode{addr: addrs[i]}
		listeners[addrs[i]] = bufconn.Listen(1 << 20)
	}
	dial := func(addr string) (*grpc.ClientConn, error) {
		lis, ok := listeners[addr]
		if !ok {
			return nil, errors.New("unknown chunkserver " + addr)
		}
		return dialBufconn(lis)
	}

	coordLis := serve(t, func(s *grpc.Server) {
		gfspb.RegisterCoordinatorServiceServer(s, coordinator.NewServer(coordinator.NewMetadata(), addrs))
	})

	for i, node := range nodes {
		store, err := chunkserver.NewStore(filepath.Join(t.TempDir(), fmt.Sprintf("data%d", i+1)))
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}
		toCoord, err := dialBufconn(coordLis)
		if err != nil {
			t.Fatalf("dial coordinator: %v", err)
		}
		cs := chunkserver.NewServer(fmt.Sprintf("cs%d", i+1), store)
		cs.SetCoordinator(gfspb.NewCoordinatorServiceClient(toCoord)) // both before serving
		cs.SetDial(dial)

		srv := grpc.NewServer()
		gfspb.RegisterChunkServerServiceServer(srv, cs)
		go srv.Serve(listeners[node.addr])
		node.store, node.srv = store, srv
		t.Cleanup(func() {
			srv.Stop()
			cs.Close()
			toCoord.Close()
		})
	}
	t.Cleanup(func() {
		for _, lis := range listeners {
			lis.Close()
		}
	})
	return &replicatedCluster{coordLis: coordLis, nodes: nodes, dial: dial}
}

func (rc *replicatedCluster) newClient(t *testing.T) *Client {
	t.Helper()
	coordConn, err := dialBufconn(rc.coordLis)
	if err != nil {
		t.Fatalf("dial coordinator: %v", err)
	}
	c := newWithConn(coordConn, rc.dial)
	t.Cleanup(func() { c.Close() })
	return c
}

func (rc *replicatedCluster) node(addr string) *csNode {
	for _, n := range rc.nodes {
		if n.addr == addr {
			return n
		}
	}
	return nil
}

func TestReplicatedUploadSurvivesNodeLoss(t *testing.T) {
	rc := startReplicatedCluster(t, 3)
	c := rc.newClient(t)
	ctx := testContext(t)
	data := randomBytes(2*ChunkSize + 100) // three chunks
	in := writeTempFile(t, "input.bin", data)

	info, err := c.Upload(ctx, in, "input.bin")
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if paths := listPaths(t, c, ""); !slices.Equal(paths, []string{"input.bin"}) {
		t.Fatalf("List after Upload = %v, want [input.bin] (file should be visible)", paths)
	}

	// Every chunk: 3 distinct assigned servers, and the bytes on all 3 disks.
	for _, ref := range info.Chunks {
		if len(ref.Addrs) != 3 || ref.Addrs[0] == ref.Addrs[1] || ref.Addrs[1] == ref.Addrs[2] || ref.Addrs[0] == ref.Addrs[2] {
			t.Errorf("chunk %d assigned to %v, want 3 different chunkservers", ref.Index, ref.Addrs)
		}
		for _, node := range rc.nodes {
			got, err := node.store.Read(ref.ID)
			if err != nil {
				t.Errorf("chunk %d missing on %s: %v", ref.Index, node.addr, err)
			} else if int64(len(got)) != ref.Length {
				t.Errorf("chunk %d on %s has %d bytes, want %d", ref.Index, node.addr, len(got), ref.Length)
			}
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	// Stop the primary of chunk 0: reads must fall back to the other replicas.
	victim := rc.node(info.Chunks[0].Addrs[0])
	victim.srv.Stop()

	out := filepath.Join(t.TempDir(), "output.bin")
	if err := c.Download(ctx, "input.bin", out); err != nil {
		t.Fatalf("Download with %s down: %v", victim.addr, err)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read downloaded file: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("downloaded %d bytes with %s down, want the %d bytes that were uploaded", len(got), victim.addr, len(data))
	}
}

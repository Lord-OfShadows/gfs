package coordinator

import (
	"context"
	"crypto/sha256"
	"errors"
	"net"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Lord-OfShadows/gfs/internal/chunkserver"
	"github.com/Lord-OfShadows/gfs/proto/gfspb"
)

const testChunkserverAddr = "chunkserver-1"

// bufconnClient connects to lis over an in-memory connection.
func bufconnClient(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// serve runs srv on a new bufconn listener and stops it when the test ends.
func serve(t *testing.T, register func(*grpc.Server)) *bufconn.Listener {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	register(srv)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	return lis
}

// startCoordinator runs a coordinator with one in-memory chunkserver behind it
// and returns a coordinator client plus that chunkserver's Store.
func startCoordinator(t *testing.T) (gfspb.CoordinatorServiceClient, *chunkserver.Store) {
	t.Helper()

	store, err := chunkserver.NewStore(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	csLis := serve(t, func(s *grpc.Server) {
		gfspb.RegisterChunkServerServiceServer(s, chunkserver.NewServer("cs1", store))
	})

	coord := NewServer(NewMetadata(), []string{testChunkserverAddr})
	coord.dial = func(addr string) (*grpc.ClientConn, error) {
		if addr != testChunkserverAddr {
			return nil, errors.New("unknown test chunkserver " + addr)
		}
		return grpc.NewClient("passthrough:///"+addr,
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return csLis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}
	coordLis := serve(t, func(s *grpc.Server) { gfspb.RegisterCoordinatorServiceServer(s, coord) })

	return gfspb.NewCoordinatorServiceClient(bufconnClient(t, coordLis)), store
}

func rpcContext(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// createReq builds a CreateFileRequest with one chunk per content string.
func createReq(opID, path string, contents ...string) *gfspb.CreateFileRequest {
	req := &gfspb.CreateFileRequest{Path: path, OperationId: opID}
	for _, s := range contents {
		sum := sha256.Sum256([]byte(s))
		req.Chunks = append(req.Chunks, &gfspb.ChunkSpec{Length: int64(len(s)), Checksum: sum[:]})
		req.SizeBytes += int64(len(s))
	}
	return req
}

// createVisible creates a file and reports every chunk, so it becomes visible.
func createVisible(t *testing.T, c gfspb.CoordinatorServiceClient, req *gfspb.CreateFileRequest) *gfspb.CreateFileResponse {
	t.Helper()
	ctx := rpcContext(t)
	resp, err := c.CreateFile(ctx, req)
	if err != nil {
		t.Fatalf("CreateFile(%q): %v", req.GetPath(), err)
	}
	for _, p := range resp.GetChunks() {
		if _, err := c.ReportChunk(ctx, &gfspb.ReportChunkRequest{NodeId: "cs1", Chunk: p.GetChunk()}); err != nil {
			t.Fatalf("ReportChunk(%q): %v", p.GetChunk().GetChunkId(), err)
		}
	}
	return resp
}

func wantCode(t *testing.T, what string, err error, want codes.Code) {
	t.Helper()
	if got := status.Code(err); got != want {
		t.Fatalf("%s: code = %v (err: %v), want %v", what, got, err, want)
	}
}

func TestServerCreateReportGet(t *testing.T) {
	c, _ := startCoordinator(t)
	ctx := rpcContext(t)
	req := createReq("op-1", "/docs/a.txt", "first chunk", "second")

	created := createVisible(t, c, req)
	if created.GetFileId() == "" || len(created.GetChunks()) != 2 {
		t.Fatalf("CreateFile = {file_id %q, %d chunks}, want an ID and 2 chunks", created.GetFileId(), len(created.GetChunks()))
	}
	for i, p := range created.GetChunks() {
		if p.GetChunk().GetIndex() != uint32(i) || len(p.GetReplicas()) == 0 || p.GetReplicas()[0].GetAddress() != testChunkserverAddr {
			t.Errorf("chunk %d placement = %v, want index %d on %s", i, p, i, testChunkserverAddr)
		}
	}

	resp, err := c.GetFileMetadata(ctx, &gfspb.GetFileMetadataRequest{Path: "/docs/a.txt"})
	if err != nil {
		t.Fatalf("GetFileMetadata: %v", err)
	}
	f := resp.GetFile()
	if f.GetFileId() != created.GetFileId() || f.GetSizeBytes() != req.GetSizeBytes() || f.GetCreatedAtUnixMs() == 0 {
		t.Errorf("file = {id %q, size %d, created %d}, want {%q, %d, non-zero}",
			f.GetFileId(), f.GetSizeBytes(), f.GetCreatedAtUnixMs(), created.GetFileId(), req.GetSizeBytes())
	}
	for i, p := range f.GetChunks() {
		want := created.GetChunks()[i].GetChunk()
		if p.GetChunk().GetChunkId() != want.GetChunkId() || p.GetChunk().GetIndex() != uint32(i) {
			t.Errorf("chunk %d = %q (index %d), want %q in order", i, p.GetChunk().GetChunkId(), p.GetChunk().GetIndex(), want.GetChunkId())
		}
	}
}

func TestServerGetPendingFileIsNotFound(t *testing.T) {
	c, _ := startCoordinator(t)
	ctx := rpcContext(t)

	if _, err := c.CreateFile(ctx, createReq("op-1", "/pending.txt", "data")); err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	_, err := c.GetFileMetadata(ctx, &gfspb.GetFileMetadataRequest{Path: "/pending.txt"})
	wantCode(t, "GetFileMetadata(pending)", err, codes.NotFound)
}

func TestServerCreateFileValidation(t *testing.T) {
	c, _ := startCoordinator(t)

	badSize := createReq("op-1", "/a.txt", "hello")
	badSize.SizeBytes = 999

	badChecksum := createReq("op-2", "/b.txt", "hello")
	badChecksum.Chunks[0].Checksum = []byte{1, 2, 3}

	tests := []struct {
		name string
		req  *gfspb.CreateFileRequest
	}{
		{name: "size_bytes does not match chunks", req: badSize},
		{name: "empty path", req: createReq("op-3", "", "x")},
		{name: "path too long", req: createReq("op-4", "/"+strings.Repeat("a", maxPathLen), "x")},
		{name: "control character in path", req: createReq("op-5", "/bad\nname", "x")},
		{name: "checksum not SHA-256 sized", req: badChecksum},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := c.CreateFile(rpcContext(t), tc.req)
			wantCode(t, "CreateFile", err, codes.InvalidArgument)
		})
	}
}

func TestServerDuplicateVisiblePath(t *testing.T) {
	c, _ := startCoordinator(t)
	createVisible(t, c, createReq("op-1", "/a.txt", "one"))

	_, err := c.CreateFile(rpcContext(t), createReq("op-2", "/a.txt", "two"))
	wantCode(t, "CreateFile(existing visible path)", err, codes.AlreadyExists)
}

func TestServerListFiles(t *testing.T) {
	c, _ := startCoordinator(t)
	ctx := rpcContext(t)
	createVisible(t, c, createReq("op-1", "/docs/b.txt", "b"))
	createVisible(t, c, createReq("op-2", "/docs/a.txt", "a"))
	createVisible(t, c, createReq("op-3", "/img/x.png", "x"))
	if _, err := c.CreateFile(ctx, createReq("op-4", "/docs/pending.txt", "p")); err != nil {
		t.Fatalf("CreateFile: %v", err)
	}

	resp, err := c.ListFiles(ctx, &gfspb.ListFilesRequest{Prefix: "/docs/"})
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	var got []string
	for _, f := range resp.GetFiles() {
		got = append(got, f.GetPath())
	}
	if want := []string{"/docs/a.txt", "/docs/b.txt"}; !slices.Equal(got, want) {
		t.Fatalf("ListFiles(/docs/) = %v, want %v", got, want)
	}
}

func TestServerDeleteFile(t *testing.T) {
	c, store := startCoordinator(t)
	ctx := rpcContext(t)
	created := createVisible(t, c, createReq("op-1", "/a.txt", "one", "two"))

	// Put the chunk bytes on the chunkserver, as a real upload would have.
	for _, p := range created.GetChunks() {
		if err := store.Write(p.GetChunk().GetChunkId(), []byte("bytes")); err != nil {
			t.Fatalf("store.Write: %v", err)
		}
	}

	if _, err := c.DeleteFile(ctx, &gfspb.DeleteFileRequest{Path: "/a.txt"}); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}

	_, err := c.GetFileMetadata(ctx, &gfspb.GetFileMetadataRequest{Path: "/a.txt"})
	wantCode(t, "GetFileMetadata after delete", err, codes.NotFound)

	for _, p := range created.GetChunks() {
		id := p.GetChunk().GetChunkId()
		if _, err := store.Read(id); !errors.Is(err, chunkserver.ErrChunkNotFound) {
			t.Errorf("chunk %s still on chunkserver after DeleteFile (Read error = %v)", id, err)
		}
	}

	_, err = c.DeleteFile(ctx, &gfspb.DeleteFileRequest{Path: "/a.txt"})
	wantCode(t, "second DeleteFile", err, codes.NotFound)
}

// serverWithDeadNode returns a Server whose three static nodes all sent
// heartbeats, with cs3 then going silent past UnavailableAfter.
func serverWithDeadNode() *Server {
	s := NewServer(NewMetadata(), []string{"cs1:7001", "cs2:7001", "cs3:7001"})
	r := s.Registry()
	r.Heartbeat("cs1", "cs1:7001", t0)
	r.Heartbeat("cs2", "cs2:7001", t0)
	r.Heartbeat("cs3", "cs3:7001", t0)

	later := t0.Add(UnavailableAfter + 500*time.Millisecond)
	r.Heartbeat("cs1", "cs1:7001", later)
	r.Heartbeat("cs2", "cs2:7001", later)
	r.Sweep(later)
	return s
}

func TestCreateFileNeverAssignsDeadNode(t *testing.T) {
	s := serverWithDeadNode()

	resp, err := s.CreateFile(rpcContext(t), createReq("op-1", "/a.txt", "one", "two", "three", "four"))
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	for i, p := range resp.GetChunks() {
		var got []string
		for _, r := range p.GetReplicas() {
			got = append(got, r.GetAddress())
		}
		slices.Sort(got)
		if want := []string{"cs1:7001", "cs2:7001"}; !slices.Equal(got, want) {
			t.Errorf("chunk %d replicas = %v, want %v", i, got, want)
		}
	}
}

func TestCreateFileAllNodesDead(t *testing.T) {
	s := NewServer(NewMetadata(), []string{"cs1:7001"})
	s.Registry().Heartbeat("cs1", "cs1:7001", t0)
	s.Registry().Sweep(t0.Add(UnavailableAfter + 500*time.Millisecond))

	_, err := s.CreateFile(rpcContext(t), createReq("op-1", "/a.txt", "one"))
	wantCode(t, "CreateFile with every node dead", err, codes.Unavailable)
}

func TestPlacementFallsBackToStaticListBeforeFirstHeartbeat(t *testing.T) {
	static := []string{"cs1:7001", "cs2:7001"}
	s := NewServer(NewMetadata(), static)
	if got := s.placementNodes(); !slices.Equal(got, static) {
		t.Fatalf("placementNodes = %v, want %v", got, static)
	}
}

func TestHeartbeatHandler(t *testing.T) {
	s := NewServer(NewMetadata(), nil)
	ctx := rpcContext(t)

	for _, req := range []*gfspb.HeartbeatRequest{
		{Address: "cs1:7001"},
		{NodeId: "cs1"},
	} {
		_, err := s.Heartbeat(ctx, req)
		wantCode(t, "Heartbeat with a missing field", err, codes.InvalidArgument)
	}

	resp, err := s.Heartbeat(ctx, &gfspb.HeartbeatRequest{NodeId: "cs1", Address: "cs1:7001"})
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if resp.GetState() != gfspb.NodeState_NODE_STATE_HEALTHY || resp.GetHeartbeatIntervalMs() != 1000 {
		t.Errorf("Heartbeat = {state %v, interval %d}, want {HEALTHY, 1000}", resp.GetState(), resp.GetHeartbeatIntervalMs())
	}
}

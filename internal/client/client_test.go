package client

import (
	"bytes"
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Lord-OfShadows/gfs/internal/chunkserver"
	"github.com/Lord-OfShadows/gfs/proto/gfspb"
)

// startChunkserver runs a real chunkserver over an in-memory connection and
// returns a Client connected to it plus the server's Store, so tests can
// inspect or tamper with what is on "disk".
func startChunkserver(t *testing.T) (*Client, *chunkserver.Store) {
	t.Helper()

	store, err := chunkserver.NewStore(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	gfspb.RegisterChunkServerServiceServer(srv, chunkserver.NewServer("test-node", store))
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

	c := newWithConn(conn)
	t.Cleanup(func() {
		c.Close()
		srv.Stop()
	})
	return c, store
}

// randomBytes returns n pseudo-random bytes, the same for the same n. Random
// (not repeating) content means a bug that swaps or repeats chunks changes
// the output and is caught by the byte comparison.
func randomBytes(n int) []byte {
	var seed [32]byte
	seed[0] = byte(n)
	seed[1] = byte(n >> 8)
	seed[2] = byte(n >> 16)
	b := make([]byte, n)
	rand.NewChaCha8(seed).Read(b)
	return b
}

// writeTempFile creates a file with the given contents and returns its path.
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
			c, _ := startChunkserver(t)
			ctx := testContext(t)
			data := randomBytes(tc.size)
			in := writeTempFile(t, "input.bin", data)

			m, err := c.Upload(ctx, in)
			if err != nil {
				t.Fatalf("Upload: %v", err)
			}
			if m.Name != "input.bin" || m.Size != int64(tc.size) || len(m.Chunks) != tc.wantChunks {
				t.Fatalf("manifest = {name %q, size %d, %d chunks}, want {%q, %d, %d chunks}",
					m.Name, m.Size, len(m.Chunks), "input.bin", tc.size, tc.wantChunks)
			}
			for i, ref := range m.Chunks {
				if ref.Index != i {
					t.Errorf("chunk %d has Index %d, want %d", i, ref.Index, i)
				}
			}

			out := filepath.Join(t.TempDir(), "output.bin")
			if err := c.Download(ctx, m, out); err != nil {
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
	c, store := startChunkserver(t)
	ctx := testContext(t)
	data := randomBytes(ChunkSize + 1) // two chunks
	in := writeTempFile(t, "input.bin", data)

	m, err := c.Upload(ctx, in)
	if err != nil {
		t.Fatalf("Upload: %v", err)
	}
	if len(m.Chunks) == 0 {
		t.Fatal("Upload returned no chunks")
	}

	// Simulate disk corruption: same length, one bit flipped. The server will
	// happily serve these bytes with a header checksum computed from them, so
	// only the client's check against the manifest can catch it.
	id := m.Chunks[0].ID
	stored, err := store.Read(id)
	if err != nil {
		t.Fatalf("store.Read(%q): %v", id, err)
	}
	stored[0] ^= 0x01
	if err := store.Write(id, stored); err != nil {
		t.Fatalf("store.Write(%q): %v", id, err)
	}

	out := filepath.Join(t.TempDir(), "output.bin")
	err = c.Download(ctx, m, out)
	if err == nil {
		t.Fatal("Download of a corrupted chunk succeeded, want an error")
	}
	if errors.Is(err, errNotImplemented) {
		t.Fatalf("Download returned %v; implement it first", err)
	}
}

func TestManifestSaveLoad(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "manifests")
	want := &Manifest{
		Name: "report.pdf",
		Size: 5,
		Chunks: []ChunkRef{
			{ID: "abc123", Index: 0, Length: 5, Checksum: []byte{0xde, 0xad, 0xbe, 0xef}},
		},
	}

	if err := SaveManifest(dir, want); err != nil {
		t.Fatalf("SaveManifest: %v", err)
	}
	got, err := LoadManifest(dir, "report.pdf")
	if err != nil {
		t.Fatalf("LoadManifest: %v", err)
	}
	if got.Name != want.Name || got.Size != want.Size || len(got.Chunks) != 1 ||
		got.Chunks[0].ID != "abc123" || !bytes.Equal(got.Chunks[0].Checksum, want.Chunks[0].Checksum) {
		t.Fatalf("loaded %+v, want %+v", got, want)
	}

	if _, err := LoadManifest(dir, "missing.txt"); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("LoadManifest(missing) error = %v, want os.ErrNotExist", err)
	}
}

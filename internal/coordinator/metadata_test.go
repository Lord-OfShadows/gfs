package coordinator

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
)

var testNodes = []string{"localhost:7001"}

// specsFor returns one ChunkSpec per string, with the string's length and SHA-256.
func specsFor(contents ...string) []ChunkSpec {
	specs := make([]ChunkSpec, len(contents))
	for i, s := range contents {
		sum := sha256.Sum256([]byte(s))
		specs[i] = ChunkSpec{Length: int64(len(s)), Checksum: sum[:]}
	}
	return specs
}

func mustCreate(t *testing.T, m *Metadata, opID, path string, specs []ChunkSpec) *File {
	t.Helper()
	f, err := m.CreateFile(opID, path, specs, testNodes)
	if err != nil {
		t.Fatalf("CreateFile(%q, %q): %v", opID, path, err)
	}
	return f
}

// reportAll reports every chunk of f as stored on node "cs1".
func reportAll(t *testing.T, m *Metadata, f *File) {
	t.Helper()
	for _, c := range f.Chunks {
		if err := m.ReportChunk("cs1", c.ID, c.Checksum); err != nil {
			t.Fatalf("ReportChunk(%q): %v", c.ID, err)
		}
	}
}

func TestCreateFile(t *testing.T) {
	m := NewMetadata()
	specs := specsFor("aaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbb", "ccccc") // 10 + 20 + 5 bytes

	f := mustCreate(t, m, "op-1", "/docs/a.txt", specs)

	if f.ID == "" || f.Path != "/docs/a.txt" {
		t.Errorf("file = {ID %q, Path %q}, want a non-empty ID and path /docs/a.txt", f.ID, f.Path)
	}
	if f.Size != 35 {
		t.Errorf("Size = %d, want 35", f.Size)
	}
	if f.Visible {
		t.Error("new file with chunks is Visible, want pending until chunks are reported")
	}
	if len(f.Chunks) != 3 {
		t.Fatalf("got %d chunks, want 3", len(f.Chunks))
	}

	seen := map[string]bool{}
	for i, c := range f.Chunks {
		if c.ID == "" || seen[c.ID] {
			t.Errorf("chunk %d has empty or duplicate ID %q", i, c.ID)
		}
		seen[c.ID] = true
		if c.FileID != f.ID || c.Index != i {
			t.Errorf("chunk %d = {FileID %q, Index %d}, want {%q, %d}", i, c.FileID, c.Index, f.ID, i)
		}
		if c.Length != specs[i].Length || !bytes.Equal(c.Checksum, specs[i].Checksum) {
			t.Errorf("chunk %d length/checksum do not match its spec", i)
		}
		if len(c.Assigned) == 0 {
			t.Errorf("chunk %d has no Assigned chunkservers", i)
		}
	}
}

func TestCreateFileEmptyIsVisibleImmediately(t *testing.T) {
	m := NewMetadata()

	f := mustCreate(t, m, "op-1", "/empty.txt", nil)
	if !f.Visible || f.Size != 0 || len(f.Chunks) != 0 {
		t.Fatalf("empty file = {Visible %v, Size %d, %d chunks}, want {true, 0, 0}", f.Visible, f.Size, len(f.Chunks))
	}
	if _, err := m.GetFile("/empty.txt"); err != nil {
		t.Fatalf("GetFile(empty file): %v", err)
	}
}

func TestCreateFileSameOpIDIsIdempotent(t *testing.T) {
	m := NewMetadata()
	specs := specsFor("hello", "world")

	first := mustCreate(t, m, "op-retry", "/a.txt", specs)
	second := mustCreate(t, m, "op-retry", "/a.txt", specs)

	if second.ID != first.ID {
		t.Fatalf("retry returned file ID %q, want the original %q", second.ID, first.ID)
	}
	for i := range first.Chunks {
		if second.Chunks[i].ID != first.Chunks[i].ID {
			t.Errorf("retry chunk %d ID = %q, want the original %q", i, second.Chunks[i].ID, first.Chunks[i].ID)
		}
	}
}

func TestCreateFileOverExistingPath(t *testing.T) {
	t.Run("visible file -> ErrFileExists", func(t *testing.T) {
		m := NewMetadata()
		f := mustCreate(t, m, "op-1", "/a.txt", specsFor("one"))
		reportAll(t, m, f)

		_, err := m.CreateFile("op-2", "/a.txt", specsFor("two"), testNodes)
		if !errors.Is(err, ErrFileExists) {
			t.Fatalf("CreateFile over visible file: error = %v, want ErrFileExists", err)
		}
	})

	t.Run("pending file -> replaced", func(t *testing.T) {
		m := NewMetadata()
		old := mustCreate(t, m, "op-1", "/a.txt", specsFor("abandoned upload"))

		replacement := mustCreate(t, m, "op-2", "/a.txt", specsFor("new upload"))
		if replacement.ID == old.ID {
			t.Fatal("replacement has the same file ID as the abandoned upload")
		}

		// The abandoned upload's chunks must be forgotten, not leaked.
		if err := m.ReportChunk("cs1", old.Chunks[0].ID, old.Chunks[0].Checksum); !errors.Is(err, ErrChunkNotFound) {
			t.Errorf("ReportChunk(abandoned chunk) error = %v, want ErrChunkNotFound", err)
		}

		reportAll(t, m, replacement)
		got, err := m.GetFile("/a.txt")
		if err != nil {
			t.Fatalf("GetFile after replacing: %v", err)
		}
		if got.ID != replacement.ID {
			t.Errorf("GetFile returned file %q, want the replacement %q", got.ID, replacement.ID)
		}
	})
}

func TestGetFileVisibility(t *testing.T) {
	m := NewMetadata()
	f := mustCreate(t, m, "op-1", "/a.txt", specsFor("part one", "part two"))

	if _, err := m.GetFile("/a.txt"); !errors.Is(err, ErrFileNotFound) {
		t.Fatalf("GetFile(pending) error = %v, want ErrFileNotFound", err)
	}

	// Reporting only some chunks keeps the file hidden.
	if err := m.ReportChunk("cs1", f.Chunks[0].ID, f.Chunks[0].Checksum); err != nil {
		t.Fatalf("ReportChunk: %v", err)
	}
	if _, err := m.GetFile("/a.txt"); !errors.Is(err, ErrFileNotFound) {
		t.Fatalf("GetFile(half reported) error = %v, want ErrFileNotFound", err)
	}

	reportAll(t, m, f)
	got, err := m.GetFile("/a.txt")
	if err != nil {
		t.Fatalf("GetFile after all chunks reported: %v", err)
	}
	if !got.Visible {
		t.Error("file not Visible after every chunk was reported")
	}
	if !got.Chunks[0].Replicas["cs1"] {
		t.Errorf("chunk 0 Replicas = %v, want cs1 recorded", got.Chunks[0].Replicas)
	}
}

func TestReportChunkErrors(t *testing.T) {
	m := NewMetadata()
	f := mustCreate(t, m, "op-1", "/a.txt", specsFor("data"))

	wrong := sha256.Sum256([]byte("different data"))
	if err := m.ReportChunk("cs1", f.Chunks[0].ID, wrong[:]); !errors.Is(err, ErrChecksumMismatch) {
		t.Errorf("ReportChunk(wrong checksum) error = %v, want ErrChecksumMismatch", err)
	}
	if err := m.ReportChunk("cs1", "no-such-chunk", wrong[:]); !errors.Is(err, ErrChunkNotFound) {
		t.Errorf("ReportChunk(unknown chunk) error = %v, want ErrChunkNotFound", err)
	}
	if _, err := m.GetFile("/a.txt"); !errors.Is(err, ErrFileNotFound) {
		t.Errorf("file became visible after a rejected report: GetFile error = %v", err)
	}
}

func TestListFiles(t *testing.T) {
	m := NewMetadata()
	for i, p := range []string{"/docs/b.txt", "/img/x.png", "/docs/a.txt", "/docs/c.txt"} {
		f := mustCreate(t, m, fmt.Sprintf("op-%d", i), p, specsFor(p))
		if p != "/docs/c.txt" { // c.txt stays pending
			reportAll(t, m, f)
		}
	}

	paths := func(list []FileSummary) []string {
		var out []string
		for _, s := range list {
			out = append(out, s.Path)
		}
		return out
	}

	if got, want := paths(m.ListFiles("/docs/")), []string{"/docs/a.txt", "/docs/b.txt"}; !slices.Equal(got, want) {
		t.Errorf("ListFiles(/docs/) = %v, want %v", got, want)
	}
	if got, want := paths(m.ListFiles("")), []string{"/docs/a.txt", "/docs/b.txt", "/img/x.png"}; !slices.Equal(got, want) {
		t.Errorf("ListFiles(\"\") = %v, want %v", got, want)
	}
	if got := m.ListFiles("/nothing/"); len(got) != 0 {
		t.Errorf("ListFiles(/nothing/) = %v, want empty", paths(got))
	}
}

func TestDeleteFile(t *testing.T) {
	m := NewMetadata()
	f := mustCreate(t, m, "op-1", "/a.txt", specsFor("one", "two"))
	reportAll(t, m, f)

	deleted, err := m.DeleteFile("/a.txt")
	if err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	if len(deleted) != 2 || deleted[0].ID != f.Chunks[0].ID || deleted[1].ID != f.Chunks[1].ID {
		t.Fatalf("DeleteFile returned %d chunks, want the file's 2 chunks in order", len(deleted))
	}

	if _, err := m.GetFile("/a.txt"); !errors.Is(err, ErrFileNotFound) {
		t.Errorf("GetFile after delete: error = %v, want ErrFileNotFound", err)
	}
	if err := m.ReportChunk("cs1", f.Chunks[0].ID, f.Chunks[0].Checksum); !errors.Is(err, ErrChunkNotFound) {
		t.Errorf("ReportChunk after delete: error = %v, want ErrChunkNotFound (chunk should be removed)", err)
	}
	if _, err := m.DeleteFile("/a.txt"); !errors.Is(err, ErrFileNotFound) {
		t.Errorf("second DeleteFile: error = %v, want ErrFileNotFound", err)
	}
}

func TestGetFileReturnsCopy(t *testing.T) {
	m := NewMetadata()
	f := mustCreate(t, m, "op-1", "/a.txt", specsFor("data"))
	reportAll(t, m, f)

	got, err := m.GetFile("/a.txt")
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	wantChecksum := bytes.Clone(got.Chunks[0].Checksum)

	// Scribble over every level of the returned value.
	got.Size = 999
	got.Path = "/hacked"
	got.Chunks[0].Checksum[0] ^= 0xFF
	got.Chunks[0].Assigned[0] = "evil:1"
	got.Chunks[0].Replicas["evil-node"] = true
	got.Chunks[0] = &Chunk{ID: "fake"}

	again, err := m.GetFile("/a.txt")
	if err != nil {
		t.Fatalf("GetFile again: %v", err)
	}
	c := again.Chunks[0]
	switch {
	case again.Size != 4 || again.Path != "/a.txt":
		t.Errorf("stored file changed: Size %d, Path %q", again.Size, again.Path)
	case c.ID != f.Chunks[0].ID:
		t.Errorf("stored chunk replaced: ID %q, want %q", c.ID, f.Chunks[0].ID)
	case !bytes.Equal(c.Checksum, wantChecksum):
		t.Error("stored checksum changed through the returned copy")
	case c.Assigned[0] != testNodes[0]:
		t.Errorf("stored Assigned changed: %v", c.Assigned)
	case c.Replicas["evil-node"]:
		t.Error("stored Replicas map changed through the returned copy")
	}
}

// TestConcurrentAccess hammers Metadata from many goroutines. It mostly exists
// for `go test -race`, which reports any unsynchronized access to shared state.
func TestConcurrentAccess(t *testing.T) {
	m := NewMetadata()
	const workers = 50

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			path := fmt.Sprintf("/load/file-%02d", i)

			f, err := m.CreateFile(fmt.Sprintf("op-%d", i), path, specsFor("x", "yy"), testNodes)
			if err != nil {
				t.Errorf("CreateFile(%s): %v", path, err) // t.Errorf is safe from goroutines; t.Fatalf is not
				return
			}
			for _, c := range f.Chunks {
				if err := m.ReportChunk("cs1", c.ID, c.Checksum); err != nil {
					t.Errorf("ReportChunk(%s): %v", c.ID, err)
					return
				}
			}
			if _, err := m.GetFile(path); err != nil {
				t.Errorf("GetFile(%s): %v", path, err)
			}
			m.ListFiles("/load/")
		}()
	}
	wg.Wait()

	if got := len(m.ListFiles("/load/")); got != workers {
		t.Errorf("ListFiles after concurrent creates: %d files, want %d", got, workers)
	}
}

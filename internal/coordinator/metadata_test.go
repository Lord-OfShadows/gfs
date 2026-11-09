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

var repairNodes = []string{"a", "b", "c"}

// mustCreateOn creates a pending file whose chunks are placed on nodes.
func mustCreateOn(t *testing.T, m *Metadata, path string, nodes []string, contents ...string) *File {
	t.Helper()
	f, err := m.CreateFile("op-"+path, path, specsFor(contents...), nodes)
	if err != nil {
		t.Fatalf("CreateFile(%q): %v", path, err)
	}
	return f
}

// visibleOn creates a file on repairNodes and reports every chunk from a
// distinct node per replica, so it becomes visible.
func visibleOn(t *testing.T, m *Metadata, path string, contents ...string) *File {
	t.Helper()
	f := mustCreateOn(t, m, path, repairNodes, contents...)
	for _, c := range f.Chunks {
		for _, addr := range c.Assigned {
			if err := m.ReportChunk("node-"+addr, c.ID, c.Checksum); err != nil {
				t.Fatalf("ReportChunk(%q from %s): %v", c.ID, addr, err)
			}
		}
	}
	if _, err := m.GetFile(path); err != nil {
		t.Fatalf("GetFile(%q) after reporting every replica: %v", path, err)
	}
	return f
}

// without returns addrs minus every address in drop, keeping order.
func without(addrs []string, drop map[string]bool) []string {
	var out []string
	for _, a := range addrs {
		if !drop[a] {
			out = append(out, a)
		}
	}
	return out
}

func TestUnderReplicated(t *testing.T) {
	tests := []struct {
		name    string
		down    map[string]bool
		want    int
		pending bool // leave the file pending (not visible)
		tasks   bool // expect one task per chunk
	}{
		{name: "all nodes up", down: map[string]bool{}, want: 3},
		{name: "one replica down", down: map[string]bool{"b": true}, want: 3, tasks: true},
		{name: "every replica down", down: map[string]bool{"a": true, "b": true, "c": true}, want: 3},
		{name: "pending file", down: map[string]bool{"b": true}, want: 3, pending: true},
		{name: "enough live copies for want=2", down: map[string]bool{"c": true}, want: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewMetadata()
			var f *File
			if tc.pending {
				f = mustCreateOn(t, m, "/a.bin", repairNodes, "one", "two", "three")
			} else {
				f = visibleOn(t, m, "/a.bin", "one", "two", "three")
			}

			got := m.UnderReplicated(tc.down, tc.want)
			if !tc.tasks {
				if len(got) != 0 {
					t.Fatalf("UnderReplicated = %+v, want no tasks", got)
				}
				return
			}

			if len(got) != len(f.Chunks) {
				t.Fatalf("got %d tasks, want one per chunk (%d)", len(got), len(f.Chunks))
			}
			byID := map[string]RepairTask{}
			for _, task := range got {
				byID[task.ChunkID] = task
			}
			for _, c := range f.Chunks {
				task, ok := byID[c.ID]
				if !ok {
					t.Errorf("no task for chunk %d (%s)", c.Index, c.ID)
					continue
				}
				if task.Length != c.Length || !bytes.Equal(task.Checksum, c.Checksum) {
					t.Errorf("chunk %d task = {Length %d, Checksum %x}, want {%d, %x}", c.Index, task.Length, task.Checksum, c.Length, c.Checksum)
				}
				// Placement rotates per chunk, so compare against this chunk's own order.
				if want := without(c.Assigned, tc.down); !slices.Equal(task.Live, want) {
					t.Errorf("chunk %d Live = %v, want %v", c.Index, task.Live, want)
				}
				if !slices.Equal(task.Assigned, c.Assigned) {
					t.Errorf("chunk %d Assigned = %v, want %v", c.Index, task.Assigned, c.Assigned)
				}
				if s := slices.Sorted(slices.Values(task.Live)); !slices.Equal(s, []string{"a", "c"}) {
					t.Errorf("chunk %d Live = %v, want a and c", c.Index, task.Live)
				}
				if s := slices.Sorted(slices.Values(task.Assigned)); !slices.Equal(s, repairNodes) {
					t.Errorf("chunk %d Assigned = %v, want a, b and c", c.Index, task.Assigned)
				}
			}
		})
	}
}

func TestUnderReplicatedReturnsCopies(t *testing.T) {
	m := NewMetadata()
	visibleOn(t, m, "/a.bin", "data")
	down := map[string]bool{"b": true}

	tasks := m.UnderReplicated(down, 3)
	if len(tasks) != 1 {
		t.Fatalf("got %d tasks, want 1", len(tasks))
	}
	before, err := m.GetFile("/a.bin")
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}

	// Scribble over every slice the task carries.
	tasks[0].Checksum[0] ^= 0xFF
	tasks[0].Live[0] = "evil"
	tasks[0].Assigned[0] = "evil"

	after, err := m.GetFile("/a.bin")
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	b, a := before.Chunks[0], after.Chunks[0]
	if !bytes.Equal(a.Checksum, b.Checksum) {
		t.Error("stored Checksum changed through the returned task")
	}
	if !slices.Equal(a.Assigned, b.Assigned) {
		t.Errorf("stored Assigned = %v, want %v", a.Assigned, b.Assigned)
	}
	again := m.UnderReplicated(down, 3)
	if want := without(b.Assigned, down); len(again) != 1 || !slices.Equal(again[0].Live, want) {
		t.Errorf("UnderReplicated after mutation = %+v, want one task with Live %v", again, want)
	}
}

func TestAddReplica(t *testing.T) {
	tests := []struct {
		name  string
		adds  []string
		extra []string // addresses expected after the original Assigned
	}{
		{name: "new address", adds: []string{"d"}, extra: []string{"d"}},
		{name: "same address twice", adds: []string{"d", "d"}, extra: []string{"d"}},
		{name: "already assigned", adds: []string{"a"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			m := NewMetadata()
			f := visibleOn(t, m, "/a.bin", "data")
			c := f.Chunks[0]

			for _, addr := range tc.adds {
				if err := m.AddReplica(c.ID, addr); err != nil {
					t.Fatalf("AddReplica(%q): %v", addr, err)
				}
			}

			got, err := m.GetFile("/a.bin")
			if err != nil {
				t.Fatalf("GetFile: %v", err)
			}
			want := append(slices.Clone(c.Assigned), tc.extra...)
			if !slices.Equal(got.Chunks[0].Assigned, want) {
				t.Errorf("Assigned = %v, want %v", got.Chunks[0].Assigned, want)
			}
		})
	}
}

func TestAddReplicaUnknownChunk(t *testing.T) {
	m := NewMetadata()
	if err := m.AddReplica("no-such-chunk", "d"); !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("AddReplica(unknown) error = %v, want ErrChunkNotFound", err)
	}
}

func TestAddReplicaResolvesUnderReplication(t *testing.T) {
	m := NewMetadata()
	f := visibleOn(t, m, "/a.bin", "one", "two")
	down := map[string]bool{"b": true}

	if got := m.UnderReplicated(down, 3); len(got) != 2 {
		t.Fatalf("before repair: %d tasks, want 2", len(got))
	}
	if err := m.AddReplica(f.Chunks[0].ID, "d"); err != nil {
		t.Fatalf("AddReplica: %v", err)
	}

	got := m.UnderReplicated(down, 3)
	if len(got) != 1 || got[0].ChunkID != f.Chunks[1].ID {
		t.Fatalf("after repairing chunk 0: tasks = %+v, want only chunk 1 (%s)", got, f.Chunks[1].ID)
	}
}

// TestConcurrentRepair runs UnderReplicated, AddReplica and GetFile from many
// goroutines at once. It mostly exists for `go test -race`.
func TestConcurrentRepair(t *testing.T) {
	m := NewMetadata()
	f := visibleOn(t, m, "/a.bin", "one", "two", "three", "four")
	down := map[string]bool{"b": true}
	const workers, newAddrs = 50, 5 // 50 >= 4*5, so every chunk gets all 5 new addresses

	var wg sync.WaitGroup
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for _, task := range m.UnderReplicated(down, 3) {
				task.Live[0] = "scribble" // writing our own copy must not race with anyone
			}
			c := f.Chunks[i%len(f.Chunks)]
			if err := m.AddReplica(c.ID, fmt.Sprintf("new-%d", i%newAddrs)); err != nil {
				t.Errorf("AddReplica: %v", err)
			}
			if _, err := m.GetFile("/a.bin"); err != nil {
				t.Errorf("GetFile: %v", err)
			}
		}()
	}
	wg.Wait()

	if got := m.UnderReplicated(down, 3); len(got) != 0 {
		t.Errorf("after every chunk got new replicas: %d tasks left, want 0", len(got))
	}
	got, err := m.GetFile("/a.bin")
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	for _, c := range got.Chunks {
		if want := len(repairNodes) + newAddrs; len(c.Assigned) != want {
			t.Errorf("chunk %d has %d Assigned addresses, want %d", c.Index, len(c.Assigned), want)
		}
	}
}

package chunkserver

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// newTestStore returns a Store rooted in a fresh temp dir that Go deletes
// automatically when the test ends.
func newTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	return s
}

// pattern returns n deterministic, non-repeating-looking bytes.
func pattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i % 251) // 251 is prime, so the pattern doesn't align with powers of two
	}
	return b
}

func TestStoreWriteReadRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		id   string
		data []byte
	}{
		{name: "empty", id: "chunk-empty", data: []byte{}},
		{name: "one byte", id: "chunk-one", data: []byte{0x42}},
		{name: "4 MiB", id: "chunk-4mib", data: pattern(4 << 20)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)

			if err := s.Write(tc.id, tc.data); err != nil {
				t.Fatalf("Write(%q): %v", tc.id, err)
			}
			got, err := s.Read(tc.id)
			if err != nil {
				t.Fatalf("Read(%q): %v", tc.id, err)
			}
			if !bytes.Equal(got, tc.data) {
				t.Fatalf("Read(%q) returned %d bytes, want %d identical bytes", tc.id, len(got), len(tc.data))
			}
		})
	}
}

func TestStoreReadMissing(t *testing.T) {
	s := newTestStore(t)

	_, err := s.Read("does-not-exist")
	if !errors.Is(err, ErrChunkNotFound) {
		t.Fatalf("Read(missing) error = %v, want ErrChunkNotFound", err)
	}
}

func TestStoreRejectsBadChunkIDs(t *testing.T) {
	tests := []struct {
		name string
		id   string
	}{
		{name: "parent traversal", id: "../evil"},
		{name: "empty", id: ""},
		{name: "forward slash", id: "a/b"},
		{name: "backslash", id: `a\b`}, // a path separator on Windows
		{name: "dot dot", id: ".."},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t)

			if err := s.Write(tc.id, []byte("x")); !errors.Is(err, ErrInvalidChunkID) {
				t.Errorf("Write(%q) error = %v, want ErrInvalidChunkID", tc.id, err)
			}
			if _, err := s.Read(tc.id); !errors.Is(err, ErrInvalidChunkID) {
				t.Errorf("Read(%q) error = %v, want ErrInvalidChunkID", tc.id, err)
			}
			if err := s.Delete(tc.id); !errors.Is(err, ErrInvalidChunkID) {
				t.Errorf("Delete(%q) error = %v, want ErrInvalidChunkID", tc.id, err)
			}

			// Nothing may be created outside the store's root.
			if _, err := os.Stat(filepath.Join(s.root, "..", "evil")); err == nil {
				t.Errorf("Write(%q) created a file outside the store root", tc.id)
			}
		})
	}
}

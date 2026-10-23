package coordinator

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// Errors returned by Metadata. Callers compare with errors.Is.
var (
	ErrFileExists       = errors.New("file already exists")
	ErrFileNotFound     = errors.New("file not found")
	ErrChunkNotFound    = errors.New("chunk not found")
	ErrChecksumMismatch = errors.New("checksum mismatch")
	ErrNoChunkservers   = errors.New("no chunkservers available")
)

// How many copies of each chunk we try to keep
const ReplicationFactor = 3

// ChunkSpec is what the client tells us about a chunk before uploading it.
type ChunkSpec struct {
	Length   int64
	Checksum []byte // SHA-256
}

// Chunk is the coordinator's record of one chunk of a file.
type Chunk struct {
	ID       string
	FileID   string
	Index    int
	Length   int64
	Checksum []byte          // SHA-256 recorded at CreateFile; the source of truth
	Assigned []string        // chunkserver addresses the client was told to write to
	Replicas map[string]bool // node IDs that confirmed storing this chunk
}

// File is the coordinator's record of one file in the namespace.
type File struct {
	ID        string
	Path      string
	Size      int64
	CreatedAt time.Time
	Visible   bool     // false until every chunk has been reported stored
	Chunks    []*Chunk // in file order
}

// FileSummary is the short form of a File returned by ListFiles.
type FileSummary struct {
	Path      string
	Size      int64
	CreatedAt time.Time
}

// Metadata is the coordinator's in-memory namespace and chunk map.
// All fields are protected by mu; never return internal pointers to callers.
type Metadata struct {
	mu     sync.RWMutex
	files  map[string]*File  // path -> file
	chunks map[string]*Chunk // chunk ID -> chunk (same *Chunk as in File.Chunks)
	ops    map[string]string // operation ID -> path, for idempotent CreateFile retries
}

// NewMetadata returns an empty Metadata.
func NewMetadata() *Metadata {
	return &Metadata{
		files:  make(map[string]*File),
		chunks: make(map[string]*Chunk),
		ops:    make(map[string]string),
	}
}

// newID returns a random 128-bit ID as 32 hex characters.
func newID() string {
	b := make([]byte, 16)
	rand.Read(b) // never fails since Go 1.24; it crashes the program instead
	return hex.EncodeToString(b)
}

// clone returns a deep copy of f.
//
// Why: Metadata's maps hold pointers, and its methods are only safe while mu
// is held. If GetFile returned the stored *File itself, the caller would keep
// using it after the lock is released: reading it while another goroutine's
// ReportChunk writes Replicas is a data race, and a caller changing Size or a
// Checksum would silently corrupt the coordinator's metadata. A shallow copy
// is not enough, because the copy would still share the same *Chunk values,
// Checksum/Assigned backing arrays, and Replicas map (Go slices and maps are
// references, like a struct holding a pointer). So every level is copied.
// Call clone while holding mu (read lock is enough).
func (f *File) clone() *File {
	out := *f // copies the plain fields (ID, Path, Size, ...)
	out.Chunks = make([]*Chunk, len(f.Chunks))
	for i, c := range f.Chunks {
		cc := *c
		cc.Checksum = bytes.Clone(c.Checksum)
		cc.Assigned = slices.Clone(c.Assigned)
		cc.Replicas = maps.Clone(c.Replicas)
		out.Chunks[i] = &cc
	}
	return &out
}

// pickNodes return upto N different nodes for chunk i. The starting node
// rotates with i, so chunks (and which server is primary) spread evenly.
func pickNodes(nodes []string, i, n int) []string {
	if n > len(nodes) {
		n = len(nodes) // fewer servers than copies wanted; we use all of them
	}

	out := make([]string, 0, n)
	for j := 0; j < n; j++ {
		out = append(out, nodes[(i+j)%len(nodes)])
	}
	return out
}

// CreateFile registers a pending (not yet Visible) file at path with one chunk
// per spec, assigns each chunk a new ID and the chunkservers to write to, and
// returns a copy of the file. A file with no chunks is Visible immediately.
//
// Retrying with the same opID returns the original file instead of creating a
// second one. Creating over a Visible file fails with ErrFileExists; creating
// over a pending (abandoned) upload replaces it.
func (m *Metadata) CreateFile(opID, path string, specs []ChunkSpec, nodes []string) (*File, error) {
	m.mu.Lock()         // We are changing the maps: exclusive lock
	defer m.mu.Unlock() // Released when the function returns, however it returns

	// Is this a retry of a request we already handled? Return the same answer
	if opID != "" {
		if prevPath, seen := m.ops[opID]; seen {
			if f, ok := m.files[prevPath]; ok {
				return f.clone(), nil
			}
		}
	}

	// Chunks need somewhere to go; an empty file needs no chunkserver at all
	if len(specs) > 0 && len(nodes) == 0 {
		return nil, ErrNoChunkservers
	}

	// Is the path taken?
	if old, exists := m.files[path]; exists {
		if old.Visible {
			return nil, fmt.Errorf("%w: %s", ErrFileExists, path)
		}
		for _, c := range old.Chunks {
			delete(m.chunks, c.ID)
		}
	}

	// Build the file and give every chunk an ID and a chunkserver
	f := &File{
		ID:        newID(),
		Path:      path,
		CreatedAt: time.Now(),
	}
	for i, spec := range specs {
		c := &Chunk{
			ID:       newID(),
			FileID:   f.ID,
			Index:    i,
			Length:   spec.Length,
			Checksum: bytes.Clone(spec.Checksum),             // own copy: the caller may reuse its slice
			Assigned: pickNodes(nodes, i, ReplicationFactor), // spread chunks across nodes (Round Robin)
			Replicas: map[string]bool{},
		}
		f.Chunks = append(f.Chunks, c)
		f.Size += spec.Length
		m.chunks[c.ID] = c
	}
	f.Visible = len(f.Chunks) == 0 // An Empty file has nothing to wait for

	// Save the file
	m.files[path] = f
	if opID != "" {
		m.ops[opID] = path
	}
	return f.clone(), nil
}

// GetFile returns a copy of the Visible file at path, or ErrFileNotFound.
func (m *Metadata) GetFile(path string) (*File, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	f, ok := m.files[path]
	if !ok || !f.Visible {
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, path)
	}
	return f.clone(), nil
}

// ListFiles returns the Visible files whose path starts with prefix, sorted by path.
func (m *Metadata) ListFiles(prefix string) []FileSummary {
	m.mu.RLock() // only readings: shared Lock
	defer m.mu.RUnlock()

	var out []FileSummary
	for path, f := range m.files {
		if !f.Visible || !strings.HasPrefix(path, prefix) {
			continue // skip pending files and ones that don't match
		}

		out = append(out, FileSummary{
			Path:      path,
			Size:      f.Size,
			CreatedAt: f.CreatedAt,
		})
	}

	// Go Maps have no order. So we sort them out for stable output
	sort.Slice(out, func(i, j int) bool {
		return out[i].Path < out[j].Path
	})
	return out
}

// DeleteFile removes the file at path and its chunks from the metadata and
// returns those chunks (no longer reachable from the maps), so the caller can
// tell chunkservers to delete the bytes after releasing the lock.
func (m *Metadata) DeleteFile(path string) ([]*Chunk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	f, ok := m.files[path]
	if !ok || !f.Visible {
		return nil, fmt.Errorf("%w: %s", ErrFileNotFound, path)
	}

	// Forget the file and all its chunks
	delete(m.files, path)
	removed := make([]*Chunk, 0, len(f.Chunks))
	for _, c := range f.Chunks {
		delete(m.chunks, c.ID)
		removed = append(removed, c)
	}

	// Return the chunks so the caller can tell chunkservers to delete the bytes
	return removed, nil
}

// ReportChunk records that nodeID has stored chunkID with the given checksum.
// When every chunk of a file has at least one replica, the file becomes Visible.
func (m *Metadata) ReportChunk(nodeID, chunkID string, checksum []byte) error {
	m.mu.Lock() // we change Replicas and maybe visible; Exclusive Lock
	defer m.mu.Unlock()

	// Do we know this chunk?
	c, ok := m.chunks[chunkID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrChunkNotFound, chunkID)
	}

	// Does the node have RIGHT bytes?
	if !bytes.Equal(c.Checksum, checksum) {
		return fmt.Errorf("%w: chunk %s from node %s", ErrChecksumMismatch, chunkID, nodeID)
	}

	// Record that this node holds a copy
	c.Replicas[nodeID] = true

	// Find the chunk's file. If every chunk now has a copy, make it visible
	for _, f := range m.files {
		if f.ID != c.FileID {
			continue
		}
		if f.Visible {
			return nil // already visible. nothing to do
		}

		for _, fc := range f.Chunks {
			if len(fc.Replicas) < len(fc.Assigned) {
				return nil // some copies of this chunk are still missing; Keep Waiting
			}
		}
		f.Visible = true // all chunks stored; the upload is complete
		return nil
	}

	return nil
}

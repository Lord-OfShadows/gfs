package chunkserver

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Errors returned by Store. Callers compare with errors.Is, e.g.
// errors.Is(err, ErrChunkNotFound), so wrapped errors still match.
var (
	ErrChunkNotFound  = errors.New("chunk not found")
	ErrInvalidChunkID = errors.New("invalid chunk ID")
)

// errNotImplemented is returned by the stubs below until they are written.
var errNotImplemented = errors.New("not implemented")

// Store keeps chunk bytes as files under a root directory, one file per chunk.
type Store struct {
	root string
}

// NewStore returns a Store rooted at dir, creating the directory if needed.
func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir %q: %w", dir, err)
	}
	return &Store{root: dir}, nil
}

// Write durably stores data under chunkID.
// It must return ErrInvalidChunkID for an unsafe or empty ID.
func (s *Store) Write(chunkID string, data []byte) error {
	// TODO(arjun): implement. Things to decide:
	//   - write to a temp file, fsync, then rename, so a crash never leaves a half-written chunk
	//   - what to do if the chunk already exists (chunks are immutable)
	return errNotImplemented
}

// Read returns the bytes stored under chunkID.
// It must return ErrChunkNotFound if the chunk does not exist and
// ErrInvalidChunkID for an unsafe or empty ID.
func (s *Store) Read(chunkID string) ([]byte, error) {
	// TODO(arjun): implement.
	return nil, errNotImplemented
}

// Delete removes the chunk. Deleting a chunk that does not exist returns nil,
// so retries are safe. It must return ErrInvalidChunkID for an unsafe or empty ID.
func (s *Store) Delete(chunkID string) error {
	// TODO(arjun): implement.
	return errNotImplemented
}

// path maps a chunk ID to its file path under s.root, rejecting IDs that are
// empty or could escape the root (e.g. "../evil", "a/b", `a\b`).
func (s *Store) path(chunkID string) (string, error) {
	// TODO(arjun): implement. Validate first, then join with s.root.
	return "", errNotImplemented
}

// Stats returns the number of stored files and their total size in bytes.
func (s *Store) Stats() (chunkCount, usedBytes int64, err error) {
	err = filepath.WalkDir(s.root, func(_ string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !d.Type().IsRegular() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		chunkCount++
		usedBytes += info.Size()
		return nil
	})
	return chunkCount, usedBytes, err
}

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

	// Name is safe and get the final file path.
	p, err := s.path(chunkID)
	if err != nil {
		return err
	}

	// Create a temp file in the same folder
	f, err := os.CreateTemp(s.root, chunkID+"-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tmp := f.Name()

	// Write data/bytes into temp file
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("Write Chunk %s : %w", chunkID, err)
	}

	// Force the data to disk (Not just remain in RAM)
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("Sync Chunk %s : %w", chunkID, err)
	}

	// Close the file
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("Close Chunk %s : %w", chunkID, err)
	}

	// Rename the temp file (Commit data)
	if err := os.Rename(tmp, p); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("Rename Chunk %s : %w", chunkID, err)
	}
	return nil
}

// Read returns the bytes stored under chunkID.
// It must return ErrChunkNotFound if the chunk does not exist and
// ErrInvalidChunkID for an unsafe or empty ID.
func (s *Store) Read(chunkID string) ([]byte, error) {

	// Check Name and get file path
	p, err := s.path(chunkID)
	if err != nil {
		return nil, err
	}

	// Read file into memory
	data, err := os.ReadFile(p)

	// if file doesn't exist ,  return "chunk not found" error (our error)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %q", ErrChunkNotFound, chunkID)
	}

	// any other error (disk error etc)
	if err != nil {
		return nil, fmt.Errorf("Read Chunk %s : %w", chunkID, err)
	}

	return data, nil
}

// Delete removes the chunk. Deleting a chunk that does not exist returns nil,
// so retries are safe. It must return ErrInvalidChunkID for an unsafe or empty ID.
func (s *Store) Delete(chunkID string) error {
	// Check Name and get file path
	p, err := s.path(chunkID)
	if err != nil {
		return err
	}

	// Delete file
	err = os.Remove(p)

	// File Already Deleted (or never existed) is not an error
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}

	// Any other error (disk error etc)
	if err != nil {
		return fmt.Errorf("Delete Chunk %s : %w", chunkID, err)
	}

	return nil
}

// path maps a chunk ID to its file path under s.root, rejecting IDs that are
// empty or could escape the root (e.g. "../evil", "a/b", `a\b`).
func (s *Store) path(chunkID string) (string, error) {
	if chunkID == "" {
		return "", fmt.Errorf("%w: Empty Chunk ID", ErrInvalidChunkID)
	}

	for _, c := range chunkID {
		isLetter := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		isDigit := (c >= '0' && c <= '9')
		if !isLetter && !isDigit && c != '-' && c != '_' {
			return "", fmt.Errorf("%w: %q", ErrInvalidChunkID, chunkID)
		}
	}
	return filepath.Join(s.root, chunkID+".chunk"), nil
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

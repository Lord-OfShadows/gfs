package client

// The manifest is a TEMPORARY stand-in for the coordinator. In M1 there is no
// metadata service, so after an upload the client saves "which chunks make up
// this file" as a local JSON file. In M2 the coordinator owns this information
// and this file goes away.

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// ChunkRef describes one stored chunk of a file.
type ChunkRef struct {
	ID       string `json:"id"`
	Index    int    `json:"index"`    // position in the file, starting at 0
	Length   int64  `json:"length"`   // bytes in this chunk
	Checksum []byte `json:"checksum"` // SHA-256; encoded as base64 in JSON
}

// Manifest lists the chunks that make up an uploaded file, in order.
type Manifest struct {
	Name   string     `json:"name"`
	Size   int64      `json:"size"`
	Chunks []ChunkRef `json:"chunks"`
}

// manifestPath returns dir/<base of name>.json. filepath.Base strips any
// directories, so a name like "../../x" cannot write outside dir.
func manifestPath(dir, name string) (string, error) {
	base := filepath.Base(name)
	if base == "." || base == ".." || base == string(filepath.Separator) {
		return "", fmt.Errorf("invalid manifest name %q", name)
	}
	return filepath.Join(dir, base+".json"), nil
}

// SaveManifest writes m to dir/<name>.json, creating dir if needed. The write
// is atomic (temp file + rename), so a crash never leaves a half-written manifest.
func SaveManifest(dir string, m *Manifest) error {
	p, err := manifestPath(dir, m.Name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create manifest dir: %w", err)
	}

	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}

	f, err := os.CreateTemp(dir, filepath.Base(p)+"-*.tmp")
	if err != nil {
		return fmt.Errorf("create temp manifest: %w", err)
	}
	tmp := f.Name()
	_, werr := f.Write(data)
	serr := f.Sync()
	cerr := f.Close()
	if err := errors.Join(werr, serr, cerr); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("write manifest: %w", err)
	}
	if err := os.Rename(tmp, p); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("save manifest: %w", err)
	}
	return nil
}

// LoadManifest reads dir/<name>.json. A missing manifest returns an error that
// matches errors.Is(err, os.ErrNotExist).
func LoadManifest(dir, name string) (*Manifest, error) {
	p, err := manifestPath(dir, name)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, fmt.Errorf("read manifest for %q: %w", name, err)
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("decode manifest %s: %w", p, err)
	}
	return &m, nil
}

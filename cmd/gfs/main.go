// Command gfs is the CLI client.
//
//	gfs upload <file> [--addr localhost:7001] [--manifest-dir .gfs/manifests]
//	gfs download <name> <out-path> [--addr localhost:7001] [--manifest-dir .gfs/manifests]
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Lord-OfShadows/gfs/internal/client"
)

const (
	defaultAddr        = "localhost:7001"
	defaultManifestDir = ".gfs/manifests"
	opTimeout          = 60 * time.Second
)

const usage = `usage:
  gfs upload <file> [--addr localhost:7001] [--manifest-dir .gfs/manifests]
  gfs download <name> <out-path> [--addr localhost:7001] [--manifest-dir .gfs/manifests]
`

// errUsage marks errors caused by bad command-line arguments (exit code 2).
var errUsage = errors.New("bad usage")

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	var err error
	switch os.Args[1] {
	case "upload":
		err = runUpload(os.Args[2:])
	case "download":
		err = runDownload(os.Args[2:])
	case "help", "-h", "--help":
		fmt.Print(usage)
		return
	default:
		err = fmt.Errorf("%w: unknown command %q", errUsage, os.Args[1])
	}

	if err != nil {
		fmt.Fprintln(os.Stderr, "gfs:", err)
		if errors.Is(err, errUsage) {
			fmt.Fprint(os.Stderr, usage)
			os.Exit(2)
		}
		os.Exit(1)
	}
}

func runUpload(args []string) error {
	fs := flag.NewFlagSet("upload", flag.ContinueOnError)
	addr := fs.String("addr", defaultAddr, "chunkserver address")
	manifestDir := fs.String("manifest-dir", defaultManifestDir, "where manifests are stored")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 1 {
		return fmt.Errorf("%w: upload takes exactly 1 argument, got %d", errUsage, len(pos))
	}
	path := pos[0]

	c, err := client.New(*addr)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()

	start := time.Now()
	m, err := c.Upload(ctx, path)
	if err != nil {
		return fmt.Errorf("upload %s: %w", path, err)
	}
	if err := client.SaveManifest(*manifestDir, m); err != nil {
		return err
	}
	fmt.Printf("uploaded %s: %d chunks, %d bytes in %s\n",
		m.Name, len(m.Chunks), m.Size, time.Since(start).Round(time.Millisecond))
	return nil
}

func runDownload(args []string) error {
	fs := flag.NewFlagSet("download", flag.ContinueOnError)
	addr := fs.String("addr", defaultAddr, "chunkserver address")
	manifestDir := fs.String("manifest-dir", defaultManifestDir, "where manifests are stored")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return err
	}
	if len(pos) != 2 {
		return fmt.Errorf("%w: download takes exactly 2 arguments, got %d", errUsage, len(pos))
	}
	name, outPath := pos[0], pos[1]

	m, err := client.LoadManifest(*manifestDir, name)
	if err != nil {
		return err
	}

	c, err := client.New(*addr)
	if err != nil {
		return err
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	defer cancel()

	start := time.Now()
	if err := c.Download(ctx, m, outPath); err != nil {
		return fmt.Errorf("download %s: %w", name, err)
	}
	fmt.Printf("downloaded %s -> %s: %d chunks, %d bytes in %s\n",
		m.Name, outPath, len(m.Chunks), m.Size, time.Since(start).Round(time.Millisecond))
	return nil
}

// parseArgs parses flags that may appear before, between, or after the
// positional arguments. Go's flag package stops at the first non-flag
// argument, so `gfs upload a.txt --addr x` would otherwise treat "--addr" as
// a file name. We parse, take one positional, and parse the rest again.
func parseArgs(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for {
		if err := fs.Parse(args); err != nil {
			return nil, fmt.Errorf("%w: %v", errUsage, err)
		}
		rest := fs.Args()
		if len(rest) == 0 {
			return positional, nil
		}
		positional = append(positional, rest[0])
		args = rest[1:]
	}
}

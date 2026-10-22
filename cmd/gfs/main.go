// Command gfs is the CLI client.
//
//	gfs upload <file>              [--coordinator localhost:7000]
//	gfs download <name> <out-path> [--coordinator localhost:7000]
//	gfs ls [prefix]                [--coordinator localhost:7000]
//	gfs rm <name>                  [--coordinator localhost:7000]
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Lord-OfShadows/gfs/internal/client"
)

const (
	defaultCoordinator = "localhost:7000"
	opTimeout          = 60 * time.Second
)

const usage = `usage:
  gfs upload <file>              [--coordinator localhost:7000]
  gfs download <name> <out-path> [--coordinator localhost:7000]
  gfs ls [prefix]                [--coordinator localhost:7000]
  gfs rm <name>                  [--coordinator localhost:7000]
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
	case "ls":
		err = runList(os.Args[2:])
	case "rm":
		err = runRemove(os.Args[2:])
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

// command holds what every subcommand needs: a connected client and a deadline.
type command struct {
	client *client.Client
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *command) close() {
	c.cancel()
	c.client.Close()
}

// setup parses flags for a subcommand, checks the number of positional
// arguments is between minArgs and maxArgs, and connects to the coordinator.
func setup(name string, args []string, minArgs, maxArgs int) (*command, []string, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	coordAddr := fs.String("coordinator", defaultCoordinator, "coordinator address")
	pos, err := parseArgs(fs, args)
	if err != nil {
		return nil, nil, err
	}
	if len(pos) < minArgs || len(pos) > maxArgs {
		return nil, nil, fmt.Errorf("%w: %s takes %s, got %d", errUsage, name, argCount(minArgs, maxArgs), len(pos))
	}

	c, err := client.New(*coordAddr)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), opTimeout)
	return &command{client: c, ctx: ctx, cancel: cancel}, pos, nil
}

func argCount(minArgs, maxArgs int) string {
	if minArgs == maxArgs {
		return fmt.Sprintf("exactly %d argument(s)", minArgs)
	}
	return fmt.Sprintf("%d to %d arguments", minArgs, maxArgs)
}

func runUpload(args []string) error {
	cmd, pos, err := setup("upload", args, 1, 1)
	if err != nil {
		return err
	}
	defer cmd.close()

	localPath := pos[0]
	start := time.Now()
	info, err := cmd.client.Upload(cmd.ctx, localPath, filepath.Base(localPath))
	if err != nil {
		return fmt.Errorf("upload %s: %w", localPath, err)
	}
	fmt.Printf("uploaded %s: %d chunks, %d bytes in %s\n",
		info.Path, len(info.Chunks), info.Size, time.Since(start).Round(time.Millisecond))
	return nil
}

func runDownload(args []string) error {
	cmd, pos, err := setup("download", args, 2, 2)
	if err != nil {
		return err
	}
	defer cmd.close()

	name, outPath := pos[0], pos[1]
	start := time.Now()
	if err := cmd.client.Download(cmd.ctx, name, outPath); err != nil {
		return fmt.Errorf("download %s: %w", name, err)
	}
	var size int64
	if st, err := os.Stat(outPath); err == nil {
		size = st.Size()
	}
	fmt.Printf("downloaded %s -> %s: %d bytes in %s\n",
		name, outPath, size, time.Since(start).Round(time.Millisecond))
	return nil
}

func runList(args []string) error {
	cmd, pos, err := setup("ls", args, 0, 1)
	if err != nil {
		return err
	}
	defer cmd.close()

	prefix := ""
	if len(pos) == 1 {
		prefix = pos[0]
	}
	files, err := cmd.client.List(cmd.ctx, prefix)
	if err != nil {
		return err
	}
	for _, f := range files {
		fmt.Printf("%12d  %s\n", f.Size, f.Path)
	}
	fmt.Printf("%d file(s)\n", len(files))
	return nil
}

func runRemove(args []string) error {
	cmd, pos, err := setup("rm", args, 1, 1)
	if err != nil {
		return err
	}
	defer cmd.close()

	if err := cmd.client.Delete(cmd.ctx, pos[0]); err != nil {
		return err
	}
	fmt.Printf("deleted %s\n", pos[0])
	return nil
}

// parseArgs parses flags that may appear before, between, or after the
// positional arguments. Go's flag package stops at the first non-flag
// argument, so `gfs upload a.txt --coordinator x` would otherwise treat
// "--coordinator" as a file name. We parse, take one positional, and repeat.
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

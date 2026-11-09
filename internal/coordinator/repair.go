package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Lord-OfShadows/gfs/proto/gfspb"
)

const (
	RepairEvery   = 2 * time.Second  // how often Run looks for under-replicated chunks
	RepairWorkers = 4                // at most this many CopyChunk calls at once
	copyTimeout   = 30 * time.Second // deadline for one CopyChunk call
)

// Repairer restores the replica count of chunks that lost a copy to an
// Unavailable node, by asking a healthy node without the chunk to copy it
// from a live replica (CopyChunk).
type Repairer struct {
	md      *Metadata
	reg     *Registry
	workers int // bound on concurrent repairs

	// dial opens a connection to a chunkserver. Tests replace it (SetDial)
	// to use in-memory (bufconn) connections.
	dial func(addr string) (*grpc.ClientConn, error)

	mu    sync.Mutex                  // protects conns
	conns map[string]*grpc.ClientConn // chunkserver address -> cached connection
}

// NewRepairer returns a Repairer that reads chunk metadata from md and node
// liveness from reg, running at most workers repairs at once.
func NewRepairer(md *Metadata, reg *Registry, workers int) *Repairer {
	return &Repairer{
		md:      md,
		reg:     reg,
		workers: workers,
		dial: func(addr string) (*grpc.ClientConn, error) {
			return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		},
		conns: make(map[string]*grpc.ClientConn),
	}
}

// SetDial replaces how chunkservers are dialed (tests use bufconn).
// Call it before Run or RunOnce.
func (r *Repairer) SetDial(d func(addr string) (*grpc.ClientConn, error)) {
	r.dial = d
}

// chunkserver returns a client for the chunkserver at addr, reusing a cached
// connection if there is one. Safe to call from concurrent workers.
func (r *Repairer) chunkserver(addr string) (gfspb.ChunkServerServiceClient, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	conn, ok := r.conns[addr]
	if !ok {
		// Dialing under the lock is fine: grpc.NewClient does no network I/O,
		// it only builds the connection object.
		var err error
		conn, err = r.dial(addr)
		if err != nil {
			return nil, fmt.Errorf("connect to chunkserver %s: %w", addr, err)
		}
		r.conns[addr] = conn
	}
	return gfspb.NewChunkServerServiceClient(conn), nil
}

// Close closes every cached chunkserver connection. Call it on shutdown,
// after Run has returned.
func (r *Repairer) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	var errs []error
	for addr, conn := range r.conns {
		errs = append(errs, conn.Close())
		delete(r.conns, addr)
	}
	return errors.Join(errs...)
}

// pickSource returns a live replica that is also healthy, or "" if none is.
func pickSource(live []string, healthy map[string]bool) string {
	for _, addr := range live {
		if healthy[addr] {
			return addr
		}
	}
	return "" // no healthy source right now: try again next round
}

// pickDestination returns a healthy node that doesn't already hold the chunk,
// or "" if there is none. It starts at a random position so repairs spread
// across nodes instead of all landing on the first one.
func pickDestination(healthy []string, assigned []string) string {
	if len(healthy) == 0 {
		return ""
	}
	start := rand.IntN(len(healthy))
	for i := 0; i < len(healthy); i++ {
		addr := healthy[(start+i)%len(healthy)]
		if !slices.Contains(assigned, addr) {
			return addr
		}
	}
	return ""
}

// repairOne asks dst to copy the chunk from src, then records the new replica.
func (r *Repairer) repairOne(ctx context.Context, t RepairTask, src, dst string) error {
	cs, err := r.chunkserver(dst)
	if err != nil {
		return fmt.Errorf("connect to %s: %w", dst, err)
	}

	cctx, cancel := context.WithTimeout(ctx, copyTimeout)
	defer cancel()
	_, err = cs.CopyChunk(cctx, &gfspb.CopyChunkRequest{
		Chunk: &gfspb.ChunkInfo{
			ChunkId:  t.ChunkID,
			Length:   t.Length,
			Checksum: t.Checksum,
		},
		Source: &gfspb.ReplicaLocation{Address: src},
	})
	if err != nil {
		return fmt.Errorf("copy %s from %s to %s: %w", t.ChunkID, src, dst, err)
	}

	return r.md.AddReplica(t.ChunkID, dst)
}

// RunOnce runs one repair round: it finds under-replicated chunks and copies
// them, at most r.workers at a time. It returns how many chunks it repaired.
func (r *Repairer) RunOnce(ctx context.Context) int {
	down := r.reg.Unavailable()
	if len(down) == 0 {
		return 0 // nobody is dead: nothing to repair
	}

	healthy := r.reg.Healthy()
	healthySet := make(map[string]bool, len(healthy))
	for _, addr := range healthy {
		healthySet[addr] = true
	}

	tasks := r.md.UnderReplicated(down, ReplicationFactor)

	sem := make(chan struct{}, r.workers) // at most r.workers copies at once
	var wg sync.WaitGroup
	var mu sync.Mutex
	repaired := 0

	for _, t := range tasks {
		src := pickSource(t.Live, healthySet)
		dst := pickDestination(healthy, t.Assigned)
		if src == "" || dst == "" {
			continue // can't fix this chunk right now; try next round
		}

		wg.Add(1)
		sem <- struct{}{} // take a slot (waits if all are busy)
		go func() {
			defer wg.Done()
			defer func() { <-sem }() // give the slot back

			if err := r.repairOne(ctx, t, src, dst); err != nil {
				slog.Warn("repair failed", "chunk_id", t.ChunkID, "src", src, "dst", dst, "err", err)
				return
			}
			slog.Info("chunk re-replicated", "chunk_id", t.ChunkID, "src", src, "dst", dst)
			mu.Lock()
			repaired++
			mu.Unlock()
		}()
	}

	wg.Wait() // wait for every copy in this round to finish
	return repaired
}

// Run calls RunOnce every interval until ctx is cancelled.
func (r *Repairer) Run(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n := r.RunOnce(ctx); n > 0 {
				slog.Info("repair round finished", "repaired", n)
			}
		}
	}
}

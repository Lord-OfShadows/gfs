package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"net"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Lord-OfShadows/gfs/internal/chunkserver"
	"github.com/Lord-OfShadows/gfs/proto/gfspb"
)

func TestRegistryUnavailable(t *testing.T) {
	r := NewRegistry()
	if got := r.Unavailable(); len(got) != 0 {
		t.Fatalf("Unavailable on an empty registry = %v, want empty", got)
	}

	now := t0.Add(3500 * time.Millisecond)
	r.Heartbeat("cs1", "cs1:7001", now)                           // fresh: Healthy
	r.Heartbeat("cs2", "cs2:7001", t0.Add(1500*time.Millisecond)) // silent 2s: Suspect
	r.Heartbeat("cs3", "cs3:7001", t0)                            // silent 3.5s: Unavailable
	r.Sweep(now)

	if got, want := r.Unavailable(), map[string]bool{"cs3:7001": true}; !maps.Equal(got, want) {
		t.Fatalf("Unavailable = %v, want %v", got, want)
	}
}

func TestPickSource(t *testing.T) {
	live := []string{"a", "b", "c"}
	tests := []struct {
		name    string
		healthy map[string]bool
		want    []string // acceptable answers; empty means ""
	}{
		{name: "one healthy replica", healthy: map[string]bool{"c": true}, want: []string{"c"}},
		{name: "several healthy", healthy: map[string]bool{"a": true, "c": true}, want: []string{"a", "c"}},
		{name: "healthy node without the chunk", healthy: map[string]bool{"d": true}},
		{name: "nothing healthy", healthy: map[string]bool{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pickSource(live, tc.healthy)
			if len(tc.want) == 0 {
				if got != "" {
					t.Fatalf("pickSource = %q, want \"\"", got)
				}
				return
			}
			if !slices.Contains(tc.want, got) {
				t.Fatalf("pickSource = %q, want one of %v", got, tc.want)
			}
		})
	}
}

func TestPickDestination(t *testing.T) {
	tests := []struct {
		name     string
		healthy  []string
		assigned []string
		want     []string // acceptable answers; empty means ""
	}{
		{name: "one candidate", healthy: []string{"a", "b", "c", "d"}, assigned: []string{"a", "b", "c"}, want: []string{"d"}},
		{name: "several candidates", healthy: []string{"a", "d", "e"}, assigned: []string{"a", "b", "c"}, want: []string{"d", "e"}},
		{name: "every healthy node has it", healthy: []string{"a", "c"}, assigned: []string{"a", "b", "c"}},
		{name: "no healthy nodes", healthy: nil, assigned: []string{"a", "b", "c"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pickDestination(tc.healthy, tc.assigned)
			if len(tc.want) == 0 {
				if got != "" {
					t.Fatalf("pickDestination = %q, want \"\"", got)
				}
				return
			}
			if !slices.Contains(tc.want, got) {
				t.Fatalf("pickDestination = %q, want one of %v (never one of assigned %v)", got, tc.want, tc.assigned)
			}
		})
	}
}

// repairCluster is n real chunkservers over bufconn ("cs1:7001", ...) plus a
// Metadata, Registry and Repairer wired to them.
type repairCluster struct {
	md     *Metadata
	reg    *Registry
	rep    *Repairer
	stores map[string]*chunkserver.Store // address -> store
	dials  atomic.Int32                  // how many times the Repairer dialed
}

func startRepairCluster(t *testing.T, n int) *repairCluster {
	t.Helper()
	c := &repairCluster{md: NewMetadata(), reg: NewRegistry(), stores: map[string]*chunkserver.Store{}}

	listeners := map[string]*bufconn.Listener{}
	for i := 1; i <= n; i++ {
		listeners[fmt.Sprintf("cs%d:7001", i)] = bufconn.Listen(1 << 20)
	}
	dial := func(addr string) (*grpc.ClientConn, error) {
		lis, ok := listeners[addr]
		if !ok {
			return nil, fmt.Errorf("unknown test chunkserver %s", addr)
		}
		return grpc.NewClient("passthrough:///"+addr,
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
				return lis.DialContext(ctx)
			}),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}

	for i := 1; i <= n; i++ {
		addr := fmt.Sprintf("cs%d:7001", i)
		store, err := chunkserver.NewStore(filepath.Join(t.TempDir(), fmt.Sprintf("data%d", i)))
		if err != nil {
			t.Fatalf("NewStore: %v", err)
		}
		cs := chunkserver.NewServer(fmt.Sprintf("cs%d", i), store)
		cs.SetDial(dial) // the destination pulls from the source over bufconn
		srv := grpc.NewServer()
		gfspb.RegisterChunkServerServiceServer(srv, cs)
		go srv.Serve(listeners[addr])
		t.Cleanup(func() {
			srv.Stop()
			cs.Close()
		})
		c.stores[addr] = store
	}

	c.rep = NewRepairer(c.md, c.reg, RepairWorkers)
	c.rep.SetDial(func(addr string) (*grpc.ClientConn, error) {
		c.dials.Add(1)
		return dial(addr)
	})
	t.Cleanup(func() { c.rep.Close() })
	return c
}

// visibleFile creates a file on nodes, writes every chunk's bytes into the
// stores of its assigned nodes, and reports them so the file is visible.
// It returns the file and each chunk's bytes by chunk ID.
func (c *repairCluster) visibleFile(t *testing.T, path string, nodes []string, contents ...string) (*File, map[string][]byte) {
	t.Helper()
	f, err := c.md.CreateFile("op-"+path, path, specsFor(contents...), nodes)
	if err != nil {
		t.Fatalf("CreateFile: %v", err)
	}
	data := map[string][]byte{}
	for i, ch := range f.Chunks {
		data[ch.ID] = []byte(contents[i])
		for _, addr := range ch.Assigned {
			if err := c.stores[addr].Write(ch.ID, data[ch.ID]); err != nil {
				t.Fatalf("write chunk %d to %s: %v", i, addr, err)
			}
			if err := c.md.ReportChunk(strings.TrimSuffix(addr, ":7001"), ch.ID, ch.Checksum); err != nil {
				t.Fatalf("ReportChunk: %v", err)
			}
		}
	}
	if _, err := c.md.GetFile(path); err != nil {
		t.Fatalf("file not visible after reporting every replica: %v", err)
	}
	return f, data
}

// heartbeatAll marks every node Healthy at t0, then lets the nodes in dead go
// silent until they are Unavailable.
func (c *repairCluster) heartbeatAll(dead ...string) {
	later := t0.Add(UnavailableAfter + 500*time.Millisecond)
	for addr := range c.stores {
		id := strings.TrimSuffix(addr, ":7001")
		c.reg.Heartbeat(id, addr, t0)
		if !slices.Contains(dead, addr) {
			c.reg.Heartbeat(id, addr, later)
		}
	}
	c.reg.Sweep(later)
}

var threeOfFour = []string{"cs1:7001", "cs2:7001", "cs3:7001"}

// checkRepaired verifies every chunk of f that was on cs2 now has a copy on a
// node that did not have it before, with identical bytes, recorded in Metadata.
func (c *repairCluster) checkRepaired(t *testing.T, f *File, data map[string][]byte) {
	t.Helper()
	got, err := c.md.GetFile(f.Path)
	if err != nil {
		t.Fatalf("GetFile: %v", err)
	}
	for i, ch := range got.Chunks {
		before := f.Chunks[i].Assigned
		if !slices.Contains(before, "cs2:7001") {
			continue
		}
		var added []string
		for _, addr := range ch.Assigned {
			if !slices.Contains(before, addr) {
				added = append(added, addr)
			}
		}
		if len(added) != 1 {
			t.Errorf("chunk %d: Assigned %v -> %v, want exactly one new address", i, before, ch.Assigned)
			continue
		}
		stored, err := c.stores[added[0]].Read(ch.ID)
		if err != nil {
			t.Errorf("chunk %d: not stored on new replica %s: %v", i, added[0], err)
			continue
		}
		if !bytes.Equal(stored, data[ch.ID]) {
			t.Errorf("chunk %d: %s holds different bytes than the original", i, added[0])
		}
	}
	if left := c.md.UnderReplicated(c.reg.Unavailable(), ReplicationFactor); len(left) != 0 {
		t.Errorf("UnderReplicated after repair = %+v, want nothing", left)
	}
}

func TestRunOnceRepairsLostReplicas(t *testing.T) {
	c := startRepairCluster(t, 4)
	f, data := c.visibleFile(t, "/a.bin", threeOfFour, "chunk zero", "chunk one", "chunk two")
	c.heartbeatAll("cs2:7001")

	if n := c.rep.RunOnce(rpcContext(t)); n != len(f.Chunks) {
		t.Errorf("RunOnce repaired %d chunks, want %d (every chunk had a copy on cs2)", n, len(f.Chunks))
	}
	c.checkRepaired(t, f, data)
}

func TestRunOnceNothingToDo(t *testing.T) {
	c := startRepairCluster(t, 4)
	c.visibleFile(t, "/a.bin", threeOfFour, "chunk zero", "chunk one")
	c.heartbeatAll() // nobody down

	if n := c.rep.RunOnce(rpcContext(t)); n != 0 {
		t.Errorf("RunOnce with no unavailable nodes = %d, want 0", n)
	}
	if d := c.dials.Load(); d != 0 {
		t.Errorf("RunOnce with no unavailable nodes dialed %d chunkservers, want 0", d)
	}
}

func TestRunRepairsAndStopsOnCancel(t *testing.T) {
	c := startRepairCluster(t, 4)
	f, data := c.visibleFile(t, "/a.bin", threeOfFour, "chunk zero", "chunk one")
	c.heartbeatAll("cs2:7001")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		c.rep.Run(ctx, 10*time.Millisecond)
		close(done)
	}()

	// Run must keep ticking until the repair has happened...
	deadline := time.Now().Add(5 * time.Second)
	for len(c.md.UnderReplicated(c.reg.Unavailable(), ReplicationFactor)) != 0 || c.dials.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Run did not repair the lost replicas within 5s")
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.checkRepaired(t, f, data)

	// ...and return promptly once its context is cancelled.
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
}

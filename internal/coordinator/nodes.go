package coordinator

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// Liveness timing. A node is judged by how long ago its last heartbeat was.
const (
	SuspectAfter     = 2 * time.Second        // silent this long: Suspect
	UnavailableAfter = 3 * time.Second        // silent this long: Unavailable
	SweepEvery       = 500 * time.Millisecond // how often RunSweeper re-checks nodes
)

// NodeState is the coordinator's current opinion of a chunkserver.
type NodeState int

const (
	NodeHealthy NodeState = iota
	NodeSuspect
	NodeUnavailable
)

// String returns the state's name, so logs read "suspect" rather than "1".
func (s NodeState) String() string {
	switch s {
	case NodeHealthy:
		return "healthy"
	case NodeSuspect:
		return "suspect"
	case NodeUnavailable:
		return "unavailable"
	default:
		return "unknown"
	}
}

// Node is what the coordinator knows about one chunkserver.
type Node struct {
	ID       string
	Addr     string
	LastSeen time.Time
	State    NodeState
}

// Registry tracks every chunkserver that has ever sent a heartbeat.
// All fields are protected by mu; never return internal pointers to callers.
type Registry struct {
	mu    sync.RWMutex
	nodes map[string]*Node // node ID -> node
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{nodes: make(map[string]*Node)}
}

// Len returns how many nodes have ever sent a heartbeat.
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.nodes)
}

// Heartbeat records that node id, reachable at addr, is alive at time now, and
// returns the node's resulting state.
func (r *Registry) Heartbeat(id, addr string, now time.Time) NodeState {
	r.mu.Lock()
	defer r.mu.Unlock()

	n, ok := r.nodes[id]
	if !ok {
		n = &Node{ID: id} // Register Node; Seeing it for the first time
		r.nodes[id] = n
	}

	n.Addr = addr
	n.LastSeen = now
	n.State = NodeHealthy // Hearing from it means it is alive
	return n.State
}

// Sweep re-evaluates every node's state as of time now and returns copies of
// the nodes whose state changed (nothing if none did).
func (r *Registry) Sweep(now time.Time) []Node {
	r.mu.Lock()
	defer r.mu.Unlock()

	var changed []Node
	for _, n := range r.nodes {
		silent := now.Sub(n.LastSeen) // When did we last hear from the particular node

		next := NodeHealthy
		switch {
		case silent >= UnavailableAfter:
			next = NodeUnavailable
		case silent >= SuspectAfter:
			next = NodeSuspect
		}

		if next != n.State {
			n.State = next
			changed = append(changed, *n)
		}
	}
	return changed
}

// Healthy returns the addresses of all Healthy nodes, sorted. Sorted output
// keeps placement deterministic, because Go map iteration order is random.
func (r *Registry) Healthy() []string {
	r.mu.RLock() // read only: shared Lock
	defer r.mu.RUnlock()

	var out []string
	for _, n := range r.nodes {
		if n.State == NodeHealthy {
			out = append(out, n.Addr)
		}
	}
	sort.Strings(out) // map order is random; placement needs a stable order
	return out
}

// RunSweeper calls Sweep every interval until ctx is cancelled. Run it in its
// own goroutine.
func (r *Registry) RunSweeper(ctx context.Context, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return // coordinator is shutting down
		case now := <-ticker.C:
			for _, n := range r.Sweep(now) {
				slog.Warn("node state changed", "node_id", n.ID, "addr", n.Addr, "state", n.State.String())
			}
		}
	}
}

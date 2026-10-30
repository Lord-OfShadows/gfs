package coordinator

import (
	"context"
	"slices"
	"testing"
	"time"
)

// t0 is a fixed fake time: these tests never sleep waiting for liveness timeouts.
var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// wantSweep runs Sweep and checks exactly one node changed, to the given state.
func wantSweep(t *testing.T, r *Registry, now time.Time, id string, want NodeState) {
	t.Helper()
	changed := r.Sweep(now)
	if len(changed) != 1 || changed[0].ID != id || changed[0].State != want {
		t.Fatalf("Sweep(+%v) = %v, want only %s changing to %v", now.Sub(t0), changed, id, want)
	}
}

func TestRegistryStateMachine(t *testing.T) {
	r := NewRegistry()

	if got := r.Heartbeat("cs1", "cs1:7001", t0); got != NodeHealthy {
		t.Fatalf("first Heartbeat = %v, want %v", got, NodeHealthy)
	}
	if r.Len() != 1 {
		t.Fatalf("Len = %d, want 1", r.Len())
	}

	wantSweep(t, r, t0.Add(2500*time.Millisecond), "cs1", NodeSuspect)
	wantSweep(t, r, t0.Add(3500*time.Millisecond), "cs1", NodeUnavailable)

	if changed := r.Sweep(t0.Add(4 * time.Second)); len(changed) != 0 {
		t.Fatalf("Sweep with no change = %v, want nothing", changed)
	}

	if got := r.Heartbeat("cs1", "cs1:7001", t0.Add(5*time.Second)); got != NodeHealthy {
		t.Fatalf("Heartbeat after Unavailable = %v, want %v", got, NodeHealthy)
	}
	if r.Len() != 1 {
		t.Fatalf("Len after repeat heartbeat = %d, want 1", r.Len())
	}
}

func TestRegistrySweepLeavesFreshNodeHealthy(t *testing.T) {
	r := NewRegistry()
	r.Heartbeat("cs1", "cs1:7001", t0)

	if changed := r.Sweep(t0.Add(time.Second)); len(changed) != 0 {
		t.Fatalf("Sweep(+1s) = %v, want nothing", changed)
	}
	if got, want := r.Healthy(), []string{"cs1:7001"}; !slices.Equal(got, want) {
		t.Fatalf("Healthy = %v, want %v", got, want)
	}
}

func TestRegistryHealthyIsSortedAndOnlyHealthy(t *testing.T) {
	r := NewRegistry()
	r.Heartbeat("cs3", "cs3:7001", t0) // registered out of order on purpose
	r.Heartbeat("cs1", "cs1:7001", t0)
	r.Heartbeat("cs4", "cs4:7001", t0)
	r.Heartbeat("cs2", "cs2:7001", t0)

	// At +2.5s: cs4 stayed silent (Suspect); the rest have just reported.
	later := t0.Add(2500 * time.Millisecond)
	r.Heartbeat("cs3", "cs3:7001", later)
	r.Heartbeat("cs1", "cs1:7001", later)
	r.Heartbeat("cs2", "cs2:7001", later)
	wantSweep(t, r, later, "cs4", NodeSuspect)

	want := []string{"cs1:7001", "cs2:7001", "cs3:7001"}
	if got := r.Healthy(); !slices.Equal(got, want) {
		t.Fatalf("Healthy = %v, want %v", got, want)
	}
}

func TestRunSweeperSweepsUntilCancelled(t *testing.T) {
	r := NewRegistry()
	// RunSweeper uses the real clock, so a heartbeat from an hour ago is long expired.
	r.Heartbeat("cs1", "cs1:7001", time.Now().Add(-time.Hour))
	if got := r.Healthy(); len(got) != 1 {
		t.Fatalf("before sweeping: Healthy = %v, want cs1:7001", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		r.RunSweeper(ctx, 5*time.Millisecond)
		close(done)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for len(r.Healthy()) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("RunSweeper never swept the expired node")
		}
		time.Sleep(5 * time.Millisecond)
	}

	select {
	case <-done:
		t.Fatal("RunSweeper returned before its context was cancelled")
	default:
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("RunSweeper did not stop after its context was cancelled")
	}
}

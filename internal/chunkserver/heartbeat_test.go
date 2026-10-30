package chunkserver

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/Lord-OfShadows/gfs/proto/gfspb"
)

const (
	testAdvertise = "cs1:7001"
	testWait      = 2 * time.Second // how long a test waits for something that should happen
)

// fakeCoordinator records Heartbeat calls. Embedding the interface satisfies
// the methods we do not need; calling one of those would panic.
type fakeCoordinator struct {
	gfspb.CoordinatorServiceClient
	calls       chan *gfspb.HeartbeatRequest
	failFirst   atomic.Int32 // this many calls return an error
	noDeadlines atomic.Int32 // calls made without a deadline
}

func newFakeCoordinator() *fakeCoordinator {
	return &fakeCoordinator{calls: make(chan *gfspb.HeartbeatRequest, 10000)}
}

func (f *fakeCoordinator) Heartbeat(ctx context.Context, req *gfspb.HeartbeatRequest, _ ...grpc.CallOption) (*gfspb.HeartbeatResponse, error) {
	if _, ok := ctx.Deadline(); !ok {
		f.noDeadlines.Add(1)
	}
	f.calls <- req
	if f.failFirst.Add(-1) >= 0 {
		return nil, errors.New("coordinator not up yet")
	}
	return &gfspb.HeartbeatResponse{State: gfspb.NodeState_NODE_STATE_HEALTHY}, nil
}

// waitCall returns the next heartbeat, or fails the test if none arrives.
func (f *fakeCoordinator) waitCall(t *testing.T) *gfspb.HeartbeatRequest {
	t.Helper()
	select {
	case req := <-f.calls:
		return req
	case <-time.After(testWait):
		t.Fatal("no heartbeat arrived")
		return nil
	}
}

// startHeartbeats runs RunHeartbeats against a fake coordinator. Cancel the
// returned context to stop it; done closes when RunHeartbeats returns.
func startHeartbeats(t *testing.T, f *fakeCoordinator, every time.Duration) (cancel context.CancelFunc, done <-chan struct{}) {
	t.Helper()
	store, err := NewStore(filepath.Join(t.TempDir(), "data"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	s := NewServer("cs1", store)
	s.SetCoordinator(f)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	finished := make(chan struct{})
	go func() {
		s.RunHeartbeats(ctx, testAdvertise, every)
		close(finished)
	}()
	return cancel, finished
}

func TestRunHeartbeatsSendsImmediately(t *testing.T) {
	f := newFakeCoordinator()
	startHeartbeats(t, f, time.Hour) // the first tick is far away, so only an immediate send can pass

	req := f.waitCall(t)
	if req.GetNodeId() != "cs1" || req.GetAddress() != testAdvertise {
		t.Errorf("heartbeat = {node_id %q, address %q}, want {cs1, %s}", req.GetNodeId(), req.GetAddress(), testAdvertise)
	}
}

func TestRunHeartbeatsRepeatsWithDeadline(t *testing.T) {
	f := newFakeCoordinator()
	startHeartbeats(t, f, 5*time.Millisecond)

	for range 3 {
		req := f.waitCall(t)
		if req.GetNodeId() != "cs1" || req.GetAddress() != testAdvertise {
			t.Errorf("heartbeat = {node_id %q, address %q}, want {cs1, %s}", req.GetNodeId(), req.GetAddress(), testAdvertise)
		}
	}
	if n := f.noDeadlines.Load(); n != 0 {
		t.Errorf("%d heartbeats had no deadline", n)
	}
}

func TestRunHeartbeatsSurvivesFailures(t *testing.T) {
	f := newFakeCoordinator()
	f.failFirst.Store(2)
	startHeartbeats(t, f, 5*time.Millisecond)

	for range 4 { // two failures, then it must keep going
		f.waitCall(t)
	}
}

func TestRunHeartbeatsStopsOnCancel(t *testing.T) {
	f := newFakeCoordinator()
	cancel, done := startHeartbeats(t, f, 5*time.Millisecond)

	f.waitCall(t)
	select {
	case <-done:
		t.Fatal("RunHeartbeats returned before its context was cancelled")
	default:
	}

	cancel()
	select {
	case <-done:
	case <-time.After(testWait):
		t.Fatal("RunHeartbeats did not stop after its context was cancelled")
	}

	// Nothing may be sent once it has returned.
	for len(f.calls) > 0 {
		<-f.calls
	}
	time.Sleep(25 * time.Millisecond)
	if n := len(f.calls); n != 0 {
		t.Errorf("%d heartbeats sent after RunHeartbeats returned", n)
	}
}

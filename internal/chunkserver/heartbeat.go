package chunkserver

import (
	"context"
	"log/slog"
	"time"

	"github.com/Lord-OfShadows/gfs/proto/gfspb"
)

const (
	HeartbeatEvery   = 1 * time.Second // exported: cmd/chunkserver passes it to RunHeartbeats
	heartbeatTimeout = 2 * time.Second // deadline for each Heartbeat call to the coordinator
)

func (s *Server) RunHeartbeats(ctx context.Context, advertiseAddr string, every time.Duration) {
	if s.coord == nil {
		return // no coordinator configured: nobody to tell
	}
	ticker := time.NewTicker(every)
	defer ticker.Stop()

	for {
		s.sendHeartbeat(ctx, advertiseAddr) // send one right away, then once per tick
		select {
		case <-ctx.Done():
			return // shutting down
		case <-ticker.C:
		}
	}
}

func (s *Server) sendHeartbeat(ctx context.Context, advertiseAddr string) {
	hctx, cancel := context.WithTimeout(ctx, heartbeatTimeout)
	defer cancel()

	count, used, _ := s.store.Stats() // best effort: stats are informational
	_, err := s.coord.Heartbeat(hctx, &gfspb.HeartbeatRequest{
		NodeId:  s.nodeID,
		Address: advertiseAddr,
		Stats: &gfspb.NodeStats{
			ChunkCount: count,
			UsedBytes:  used,
		},
	})
	if err != nil {
		slog.Warn("heartbeat failed", "coordinator_err", err)
	}
}

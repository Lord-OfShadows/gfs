// Command chunkserver runs a GFS storage node that holds chunk bytes.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/Lord-OfShadows/gfs/internal/chunkserver"
	"github.com/Lord-OfShadows/gfs/internal/grpclog"
	"github.com/Lord-OfShadows/gfs/proto/gfspb"
)

func main() {
	addr := flag.String("addr", ":7001", "address to listen on (host:port)")
	dataDir := flag.String("data-dir", "./data/cs1", "directory where chunk files are stored")
	nodeID := flag.String("node-id", "cs1", "unique ID of this chunkserver")
	coordAddr := flag.String("coordinator", "localhost:7000", "coordinator address to report chunks to (empty disables reporting)")
	advertise := flag.String("advertise", "", `address other nodes and clients use to reach this server (default "localhost" + the port from --addr)`)
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With("node_id", *nodeID)
	slog.SetDefault(logger)

	if err := run(*addr, *dataDir, *nodeID, *coordAddr, *advertise); err != nil {
		slog.Error("chunkserver exited", "err", err)
		os.Exit(1)
	}
}

func run(addr, dataDir, nodeID, coordAddr, advertise string) error {
	if advertise == "" {
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			return fmt.Errorf("parse --addr %q: %w", addr, err)
		}
		advertise = net.JoinHostPort("localhost", port)
	}

	store, err := chunkserver.NewStore(dataDir)
	if err != nil {
		return err
	}

	srv := chunkserver.NewServer(nodeID, store)
	defer srv.Close() // runs after Serve returns: closes cached peer connections
	if coordAddr != "" {
		// Lazy: no network traffic until the first ReportChunk, so the
		// coordinator does not have to be running yet.
		conn, err := grpc.NewClient(coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("create coordinator client for %s: %w", coordAddr, err)
		}
		defer conn.Close()
		srv.SetCoordinator(gfspb.NewCoordinatorServiceClient(conn))
	} else {
		slog.Warn("no coordinator configured; stored chunks will not be reported")
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(grpclog.UnaryServerInterceptor),
		grpc.ChainStreamInterceptor(grpclog.StreamServerInterceptor),
	)
	gfspb.RegisterChunkServerServiceServer(grpcServer, srv)

	// Stop cleanly on Ctrl+C (SIGINT) or SIGTERM (what `docker stop` sends).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		slog.Info("shutting down")
		grpcServer.GracefulStop()
	}()

	if coordAddr != "" {
		go srv.RunHeartbeats(ctx, advertise, chunkserver.HeartbeatEvery)
	}

	slog.Info("chunkserver started", "addr", lis.Addr().String(), "advertise", advertise, "data_dir", dataDir, "coordinator", coordAddr)
	return grpcServer.Serve(lis)
}

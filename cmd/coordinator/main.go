// Command coordinator runs the GFS metadata server.
package main

import (
	"context"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"google.golang.org/grpc"

	"github.com/Lord-OfShadows/gfs/internal/coordinator"
	"github.com/Lord-OfShadows/gfs/internal/grpclog"
	"github.com/Lord-OfShadows/gfs/proto/gfspb"
)

func main() {
	addr := flag.String("addr", ":7000", "address to listen on (host:port)")
	chunkservers := flag.String("chunkservers", "localhost:7001", "comma-separated chunkserver addresses")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	if err := run(*addr, splitList(*chunkservers)); err != nil {
		slog.Error("coordinator exited", "err", err)
		os.Exit(1)
	}
}

func run(addr string, chunkservers []string) error {
	if len(chunkservers) == 0 {
		slog.Warn("no chunkservers configured; only empty files can be created")
	}

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(grpclog.UnaryServerInterceptor),
		grpc.ChainStreamInterceptor(grpclog.StreamServerInterceptor),
	)
	gfspb.RegisterCoordinatorServiceServer(grpcServer, coordinator.NewServer(coordinator.NewMetadata(), chunkservers))

	// Stop cleanly on Ctrl+C (SIGINT) or SIGTERM (what `docker stop` sends).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		slog.Info("shutting down")
		grpcServer.GracefulStop()
	}()

	slog.Info("coordinator started", "addr", lis.Addr().String(), "chunkservers", chunkservers)
	return grpcServer.Serve(lis)
}

// splitList turns "a, b,,c" into ["a" "b" "c"].
func splitList(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// Package grpclog provides gRPC server interceptors that log one structured
// line per RPC. (Not to be confused with google.golang.org/grpc/grpclog,
// gRPC's own internal logger.)
package grpclog

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// UnaryServerInterceptor logs every unary (request/response) RPC.
func UnaryServerInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	start := time.Now()
	resp, err := handler(ctx, req) // run the actual RPC handler
	logRPC(ctx, info.FullMethod, start, err)
	return resp, err
}

// StreamServerInterceptor logs every streaming RPC once, when the stream ends.
func StreamServerInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	start := time.Now()
	err := handler(srv, ss) // run the actual RPC handler
	logRPC(ss.Context(), info.FullMethod, start, err)
	return err
}

// logRPC writes one line: INFO for success, WARN for any error.
func logRPC(ctx context.Context, method string, start time.Time, err error) {
	code := status.Code(err) // OK for nil; Unknown for errors that are not gRPC statuses
	attrs := []slog.Attr{
		slog.String("method", method),
		slog.Float64("duration_ms", float64(time.Since(start))/float64(time.Millisecond)),
		slog.String("code", code.String()),
	}
	level := slog.LevelInfo
	if code != codes.OK {
		level = slog.LevelWarn
		attrs = append(attrs, slog.String("error", status.Convert(err).Message()))
	}
	slog.LogAttrs(ctx, level, "rpc", attrs...)
}

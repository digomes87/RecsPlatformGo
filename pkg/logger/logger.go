// Package logger configures structured logging with slog, correlating every
// log line with the request id injected by the middleware package.
package logger

import (
	"context"
	"log/slog"
	"os"
)

type ctxKey struct{}

// New returns a slog.Logger suited for the given environment: JSON in
// production (machine-parseable for CloudWatch/Loki), text otherwise.
func New(service, environment string) *slog.Logger {
	opts := &slog.HandlerOptions{Level: slog.LevelInfo}
	var h slog.Handler = slog.NewTextHandler(os.Stdout, opts)
	if environment == "production" {
		h = slog.NewJSONHandler(os.Stdout, opts)
	}
	return slog.New(h).With("service", service)
}

// WithLogger stores the logger in the context.
func WithLogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, l)
}

// From extracts the logger from the context, falling back to the default.
func From(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok {
		return l
	}
	return slog.Default()
}

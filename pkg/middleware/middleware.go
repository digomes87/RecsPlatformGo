// Package middleware provides composable net/http middlewares: panic recovery,
// request id propagation, RED metrics, JWT authentication and token-bucket
// rate limiting. They wrap plain http.Handler so any router can use them.
package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/diegogo/recsplatform/pkg/errorsx"
	"github.com/diegogo/recsplatform/pkg/metrics"
)

type ctxKey string // namespaced key type avoids collisions

type routePatternKey struct{}

// RoutePatternHolder is a mutable cell shared down the request scope: inner
// handlers (which know the matched route) write, outer middleware reads.
type RoutePatternHolder struct{ Pattern string }

// WithRoutePatternHolder seeds the context with a holder the Metrics
// middleware can read after the handler completes.
func WithRoutePatternHolder(ctx context.Context) (context.Context, *RoutePatternHolder) {
	h := &RoutePatternHolder{Pattern: "unmatched"}
	return context.WithValue(ctx, routePatternKey{}, h), h
}

// SetRoutePattern records the matched route pattern on the holder, if the
// context carries one (it does whenever Metrics wrapped the request).
func SetRoutePattern(ctx context.Context, pattern string) {
	if h, ok := ctx.Value(routePatternKey{}).(*RoutePatternHolder); ok {
		h.Pattern = pattern
	}
}

// RequestID injects a correlation id into the context and the response
// headers so incidents can be traced across services and log platforms.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" {
			buf := make([]byte, 8)
			_, _ = rand.Read(buf)
			id = hex.EncodeToString(buf)
		}
		w.Header().Set("X-Request-ID", id)
		ctx := context.WithValue(r.Context(), ctxKey("request_id"), id)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RequestIDFrom extracts the correlation id set by RequestID.
func RequestIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(ctxKey("request_id")).(string); ok {
		return v
	}
	return ""
}

// Recover converts panics into 500 responses instead of killing the
// goroutine silently, logging the stack trace for postmortems.
func Recover(log *slog.Logger, service string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Error("panic recovered",
						"error", rec,
						"request_id", RequestIDFrom(r.Context()),
						"stack", string(debug.Stack()))
					writeError(w, errorsx.Internal("internal server error"))
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

// Metrics records RED metrics for every request. Route is a static pattern
// (e.g. "/v1/users/{id}/recommendations"), never the raw path, to keep
// metric cardinality bounded.
func Metrics(service string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			ctx, holder := WithRoutePatternHolder(r.Context())
			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r.WithContext(ctx))
			elapsed := time.Since(start).Seconds()
			metrics.HTTPDuration.WithLabelValues(service, holder.Pattern, r.Method, itoa(rec.status)).Observe(elapsed)
			metrics.HTTPRequests.WithLabelValues(service, holder.Pattern, r.Method, itoa(rec.status)).Inc()
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func itoa(n int) string {
	if n == 0 {
		return "200"
	}
	buf := make([]byte, 0, 3)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}
	return string(buf)
}

func writeError(w http.ResponseWriter, appErr *errorsx.AppError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(appErr.Status)
	_ = json.NewEncoder(w).Encode(appErr)
}

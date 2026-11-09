package api

import (
	"log/slog"
	"net/http"

	"github.com/diegogo/recsplatform/pkg/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Router builds the gateway's middleware chain and routes using Go 1.22+
// pattern routing. Probes and metrics stay unauthenticated; the /v1 API is
// guarded by rate limiting and JWT auth.
func Router(h *Handler, log *slog.Logger, jwtSecret string, rateLimit *middleware.RateLimiter, service string) http.Handler {
	apiMux := http.NewServeMux()
	apiMux.HandleFunc("GET /v1/users/{userID}/recommendations", withRoute("/v1/users/{userID}/recommendations", h.GetRecommendations))
	apiMux.HandleFunc("POST /v1/events", withRoute("/v1/events", h.PostEvents))

	rateKey := func(r *http.Request) string {
		if claims, ok := middleware.ClaimsFrom(r.Context()); ok {
			return claims.UserID
		}
		return r.RemoteAddr
	}

	var api http.Handler = apiMux
	api = middleware.Auth(jwtSecret)(api)
	api = rateLimit.Middleware(rateKey)(api)

	root := http.NewServeMux()
	root.HandleFunc("GET /healthz", withRoute("/healthz", h.Health))
	root.Handle("GET /metrics", promhttp.Handler())
	root.Handle("/v1/", api)

	var handler http.Handler = root
	handler = middleware.Metrics(service)(handler)
	handler = middleware.RequestID(handler)
	handler = middleware.Recover(log, service)(handler)
	return handler
}

// withRoute tags the request context with a static route pattern so the
// Metrics middleware can label requests with bounded cardinality.
func withRoute(pattern string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		middleware.SetRoutePattern(r.Context(), pattern)
		next.ServeHTTP(w, r)
	}
}

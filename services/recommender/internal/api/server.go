// Package api exposes the recommender's internal HTTP surface.
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/diegogo/recsplatform/pkg/domain"
	"github.com/diegogo/recsplatform/pkg/middleware"
	"github.com/diegogo/recsplatform/services/recommender/internal/engine"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server serves the internal recommendation endpoint.
type Server struct {
	engine *engine.Engine
	log    *slog.Logger
}

func New(e *engine.Engine, log *slog.Logger) *Server {
	return &Server{engine: e, log: log}
}

// Handler builds the router with the shared middleware chain.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /internal/v1/recommendations", withRoute("/internal/v1/recommendations", s.getRecommendations))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})
	mux.Handle("GET /metrics", promhttp.Handler())

	var h http.Handler = mux
	h = middleware.Metrics("recommender")(h)
	h = middleware.RequestID(h)
	h = middleware.Recover(s.log, "recommender")(h)
	return h
}

// withRoute tags the request context with a static route pattern so the
// Metrics middleware can label requests with bounded cardinality.
func withRoute(pattern string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		middleware.SetRoutePattern(r.Context(), pattern)
		next.ServeHTTP(w, r)
	}
}

// getRecommendations serves GET /internal/v1/recommendations?user_id=&limit=.
func (s *Server) getRecommendations(w http.ResponseWriter, r *http.Request) {
	userID := r.URL.Query().Get("user_id")
	if userID == "" {
		httpError(w, http.StatusBadRequest, "user_id query parameter is required")
		return
	}
	limit := 10
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := parseInt(v)
		if err != nil || n < 1 || n > 50 {
			httpError(w, http.StatusBadRequest, "limit must be between 1 and 50")
			return
		}
		limit = n
	}
	items, err := s.engine.Recommend(r.Context(), userID, limit)
	if err != nil {
		s.log.Error("recommend failed", "error", err, "user_id", userID)
		httpError(w, http.StatusInternalServerError, "failed to compute recommendations")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
}

func httpError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(domain.ErrorBody{Code: status, Message: msg})
}

func parseInt(s string) (int, error) { return strconv.Atoi(s) }

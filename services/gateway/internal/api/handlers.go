// Package api wires the gateway HTTP surface: public endpoints, health
// probes and Prometheus scraping.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/diegogo/recsplatform/pkg/domain"
	"github.com/diegogo/recsplatform/pkg/errorsx"
)

// Deps groups the handler dependencies, keeping wiring explicit.
type Deps struct {
	Recommender RecommenderService
	Cache       Cache
	Producer    EventProducer
	CacheTTLSec int
}

type RecommenderService interface {
	GetRecommendations(ctx context.Context, userID string, limit int) (*domain.Recommendation, error)
}

type Cache interface {
	Get(ctx context.Context, userID string) (*domain.Recommendation, bool, error)
	Set(ctx context.Context, userID string, rec *domain.Recommendation, ttl time.Duration) error
	DoDeduped(userID string, fn func() (*domain.Recommendation, error)) (*domain.Recommendation, error)
}

type EventProducer interface {
	Publish(ctx context.Context, evt domain.Event) error
}

// Handler holds the HTTP handlers for the gateway.
type Handler struct {
	deps Deps
}

func New(d Deps) *Handler { return &Handler{deps: d} }

// GetRecommendations serves GET /v1/users/{userID}/recommendations.
// Flow: auth -> rate limit -> cache read -> (singleflight) recommender call
// -> cache write. On recommender outage the breaker fails fast and we
// return whatever cache we still have, degraded but available.
func (h *Handler) GetRecommendations(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userID := r.PathValue("userID")
	if userID == "" {
		writeJSON(w, errorsx.InvalidArgument("user id is required"))
		return
	}
	limit := 10
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 50 {
			writeJSON(w, errorsx.InvalidArgument("limit must be between 1 and 50"))
			return
		}
		limit = n
	}

	if rec, hit, err := h.deps.Cache.Get(ctx, userID); err == nil && hit {
		writeJSON(w, rec)
		return
	}

	rec, err := h.deps.Cache.DoDeduped(userID, func() (*domain.Recommendation, error) {
		rec, err := h.deps.Recommender.GetRecommendations(ctx, userID, limit)
		if err != nil {
			// Degraded mode: fall back to a stale cache read if possible.
			if stale, hit, _ := h.deps.Cache.Get(ctx, userID); hit && stale != nil {
				return stale, nil
			}
			return nil, err
		}
		_ = h.deps.Cache.Set(ctx, userID, rec, time.Duration(h.deps.CacheTTLSec)*time.Second)
		return rec, nil
	})
	if err != nil {
		writeJSON(w, errorsx.UpstreamFailure("recommendations unavailable"))
		return
	}
	writeJSON(w, rec)
}

// PostEvents serves POST /v1/events: validate, enrich, publish to Kafka.
// Ingestion is asynchronous by design — the API stays latency-predictable
// under load while the ingestor batches writes into MongoDB.
func (h *Handler) PostEvents(w http.ResponseWriter, r *http.Request) {
	var evt domain.Event
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 64<<10)).Decode(&evt); err != nil {
		writeJSON(w, errorsx.InvalidArgument("invalid JSON body"))
		return
	}
	if evt.UserID == "" || evt.ItemID == "" || evt.EventID == "" {
		writeJSON(w, errorsx.InvalidArgument("event_id, user_id and item_id are required"))
		return
	}
	switch evt.EventType {
	case domain.EventView, domain.EventLike, domain.EventPurchase, domain.EventRating:
	default:
		writeJSON(w, errorsx.InvalidArgument("event_type must be view, like, purchase or rating"))
		return
	}
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now().UTC()
	}
	if err := h.deps.Producer.Publish(r.Context(), evt); err != nil {
		writeJSON(w, errorsx.UpstreamFailure("failed to accept event"))
		return
	}
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "accepted", "event_id": evt.EventID})
}

// Health is the liveness probe payload.
func (h *Handler) Health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if appErr, ok := v.(*errorsx.AppError); ok {
		w.WriteHeader(appErr.Status)
	}
	_ = json.NewEncoder(w).Encode(v)
}

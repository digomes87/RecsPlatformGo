package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/diegogo/recsplatform/pkg/domain"
)

// fakeCache is an in-memory Cache for handler tests.
type fakeCache struct {
	data map[string]*domain.Recommendation
	hit  bool
}

func (f *fakeCache) Get(_ context.Context, _ string) (*domain.Recommendation, bool, error) {
	if f.hit {
		return f.data["u"], true, nil
	}
	return nil, false, nil
}
func (f *fakeCache) Set(_ context.Context, userID string, rec *domain.Recommendation, _ time.Duration) error {
	f.data[userID] = rec
	return nil
}
func (f *fakeCache) DoDeduped(_ string, fn func() (*domain.Recommendation, error)) (*domain.Recommendation, error) {
	return fn()
}

type fakeRecommender struct {
	rec *domain.Recommendation
	err error
}

func (f *fakeRecommender) GetRecommendations(_ context.Context, _ string, _ int) (*domain.Recommendation, error) {
	return f.rec, f.err
}

type fakeProducer struct{ published []domain.Event }

func (f *fakeProducer) Publish(_ context.Context, evt domain.Event) error {
	f.published = append(f.published, evt)
	return nil
}

func TestGetRecommendations_CacheHitSkipsRecommender(t *testing.T) {
	cached := &domain.Recommendation{UserID: "u", Items: []domain.Item{{ItemID: "cached", Score: 1}}}
	rec := &fakeRecommender{rec: &domain.Recommendation{UserID: "u", Items: []domain.Item{{ItemID: "fresh", Score: 1}}}}
	h := New(Deps{Recommender: rec, Cache: &fakeCache{data: map[string]*domain.Recommendation{"u": cached}, hit: true}, CacheTTLSec: 60})

	rw := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/users/u/recommendations", nil)
	r.SetPathValue("userID", "u")
	h.GetRecommendations(rw, r)

	var body domain.Recommendation
	_ = json.NewDecoder(rw.Body).Decode(&body)
	if len(body.Items) != 1 || body.Items[0].ItemID != "cached" {
		t.Fatalf("expected cached item, got %+v", body)
	}
}

func TestGetRecommendations_UpstreamFailureReturns502(t *testing.T) {
	h := New(Deps{
		Recommender: &fakeRecommender{err: errors.New("breaker open")},
		Cache:       &fakeCache{data: map[string]*domain.Recommendation{}},
		CacheTTLSec: 60,
	})
	rw := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/v1/users/u/recommendations", nil)
	r.SetPathValue("userID", "u")
	h.GetRecommendations(rw, r)
	if rw.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 on upstream failure, got %d", rw.Code)
	}
}

func TestPostEvents_Validation(t *testing.T) {
	prod := &fakeProducer{}
	h := New(Deps{Producer: prod})

	invalid := []string{
		`{"event_id":"","user_id":"u","item_id":"i","event_type":"view"}`,
		`{"event_id":"e","user_id":"","item_id":"i","event_type":"view"}`,
		`{"event_id":"e","user_id":"u","item_id":"","event_type":"view"}`,
		`{"event_id":"e","user_id":"u","item_id":"i","event_type":"click"}`,
	}
	for _, body := range invalid {
		rw := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/v1/events", jsonBody(body))
		h.PostEvents(rw, r)
		if rw.Code != http.StatusBadRequest {
			t.Fatalf("body %s should 400, got %d", body, rw.Code)
		}
	}
	if len(prod.published) != 0 {
		t.Fatal("no event should be published for invalid payloads")
	}

	rw := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/v1/events", jsonBody(`{"event_id":"e1","user_id":"u","item_id":"i","event_type":"purchase"}`))
	h.PostEvents(rw, r)
	if rw.Code != http.StatusAccepted {
		t.Fatalf("valid event should 202, got %d", rw.Code)
	}
	if len(prod.published) != 1 {
		t.Fatalf("expected 1 published event, got %d", len(prod.published))
	}
	if prod.published[0].Timestamp.IsZero() {
		t.Fatal("timestamp should be enriched with server time")
	}
}

func jsonBody(s string) *bytes.Reader { return bytes.NewReader([]byte(s)) }

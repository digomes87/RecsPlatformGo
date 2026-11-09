// Package recommender is the gateway's HTTP client for the recommender
// service, hardened with a circuit breaker: when the dependency degrades we
// fail fast and serve cached/fallback data instead of piling up timeouts.
package recommender

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/diegogo/recsplatform/pkg/domain"
	"github.com/sony/gobreaker"
)

// Service is the port the gateway depends on.
type Service interface {
	GetRecommendations(ctx context.Context, userID string, limit int) (*domain.Recommendation, error)
}

// Client is the HTTP implementation talking to the recommender service.
type Client struct {
	baseURL string
	http    *http.Client
	breaker *gobreaker.CircuitBreaker
}

// NewClient wires a breaker tuned for a latency-sensitive read path: a small
// consecutive-failure window and a short half-open probe.
func NewClient(baseURL string) *Client {
	breaker := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "recommender",
		MaxRequests: 5,
		Timeout:     10 * time.Second,
		Interval:    60 * time.Second,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.ConsecutiveFailures >= 5
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			// Exported via Prometheus in main through StateGetter.
		},
	})
	return &Client{
		baseURL: baseURL,
		http: &http.Client{
			Timeout: 2 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		breaker: breaker,
	}
}

// Breaker exposes the breaker for metrics instrumentation.
func (c *Client) Breaker() *gobreaker.CircuitBreaker { return c.breaker }

type apiResponse struct {
	Items []domain.Item `json:"items"`
}

// GetRecommendations calls the internal recommender endpoint through the
// breaker. A half-open/open breaker returns ErrOpenState immediately.
func (c *Client) GetRecommendations(ctx context.Context, userID string, limit int) (*domain.Recommendation, error) {
	result, err := c.breaker.Execute(func() (any, error) {
		url := fmt.Sprintf("%s/internal/v1/recommendations?user_id=%s&limit=%d", c.baseURL, userID, limit)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			return &domain.Recommendation{UserID: userID, Items: []domain.Item{}}, nil
		}
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("recommender status %d", resp.StatusCode)
		}
		var body apiResponse
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			return nil, err
		}
		return &domain.Recommendation{UserID: userID, Items: body.Items}, nil
	})
	if err != nil {
		return nil, err
	}
	rec, _ := result.(*domain.Recommendation)
	return rec, nil
}

// PostEvents is unused by the gateway (events go via Kafka producer), kept
// out intentionally to keep the client surface minimal.
var _ = bytes.MinRead

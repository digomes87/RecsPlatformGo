// Package metrics exposes Prometheus instruments shared by all services:
// RED metrics (rate, errors, duration) plus domain-specific counters.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTPDuration records handler latency per service/route/status.
	HTTPDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "http_request_duration_seconds",
		Help:    "HTTP request latency in seconds.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	}, []string{"service", "route", "method", "status"})

	// HTTPRequests counts total requests for error-rate dashboards.
	HTTPRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "http_requests_total",
		Help: "Total HTTP requests.",
	}, []string{"service", "route", "method", "status"})

	// CircuitBreakerState tracks breaker transitions (1 = state active).
	CircuitBreakerState = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "circuit_breaker_state",
		Help: "Circuit breaker state per dependency (closed=0, half-open=1, open=2).",
	}, []string{"service", "dependency"})

	// CacheHits counts cache lookups per service.
	CacheHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_hits_total",
		Help: "Cache lookups by result.",
	}, []string{"service", "result"})
)

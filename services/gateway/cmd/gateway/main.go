// Command gateway is the public API of the recommendation platform. It
// serves user-facing endpoints, caches recommendations in Redis, and
// publishes interaction events to Kafka.
package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/diegogo/recsplatform/pkg/config"
	"github.com/diegogo/recsplatform/pkg/domain"
	"github.com/diegogo/recsplatform/pkg/kafka"
	"github.com/diegogo/recsplatform/pkg/logger"
	"github.com/diegogo/recsplatform/pkg/metrics"
	"github.com/diegogo/recsplatform/pkg/middleware"
	"github.com/diegogo/recsplatform/pkg/redisclient"
	"github.com/diegogo/recsplatform/services/gateway/internal/api"
	"github.com/diegogo/recsplatform/services/gateway/internal/cache"
	"github.com/diegogo/recsplatform/services/gateway/internal/producer"
	"github.com/diegogo/recsplatform/services/gateway/internal/recommender"
	"github.com/sony/gobreaker"
)

func main() {
	cfg := config.Load("gateway")
	log := logger.New(cfg.ServiceName, cfg.Environment)

	redisClient, err := redisclient.Connect(context.Background(), cfg.RedisAddr)
	if err != nil {
		// Redis down should not take the API down: we log and run cache-less.
		log.Warn("redis unavailable, running without cache", "error", err)
	}

	recClient := recommender.NewClient(getEnv("RECOMMENDER_URL", "http://localhost:8081"))
	exportBreakerState(recClient.Breaker())

	kafkaWriter := kafka.NewWriter(cfg.KafkaBrokers, cfg.KafkaTopic)
	prod := producer.New(kafkaWriter)

	cacheTTL := time.Duration(config.GetEnvInt("CACHE_TTL_SECONDS", 60)) * time.Second
	var recCache api.Cache = cache.NewRedisCache(redisClient, cacheTTL)
	if redisClient == nil {
		recCache = noopCache{}
	}

	handler := api.New(api.Deps{
		Recommender: recClient,
		Cache:       recCache,
		Producer:    prod,
		CacheTTLSec: int(cacheTTL.Seconds()),
	})

	rl := middleware.NewRateLimiter(
		float64(config.GetEnvInt("RATE_LIMIT_RPS", 100)),
		config.GetEnvInt("RATE_LIMIT_BURST", 200),
	)
	jwtSecret := getEnv("JWT_SECRET", "dev-secret-change-me")

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Router(handler, log, jwtSecret, rl, cfg.ServiceName),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	go func() {
		log.Info("gateway listening", "addr", cfg.HTTPAddr, "env", cfg.Environment)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	// Graceful shutdown: stop accepting, drain in-flight requests, flush Kafka.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Info("shutting down", "timeout", cfg.ShutdownTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Error("graceful shutdown failed", "error", err)
	}
	if err := prod.Close(); err != nil {
		log.Error("kafka flush failed", "error", err)
	}
	_ = redisClient.Close()
	log.Info("gateway stopped")
}

// exportBreakerState mirrors the breaker state into a Prometheus gauge so
// pages can alert on sustained open circuits.
func exportBreakerState(b *gobreaker.CircuitBreaker) {
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			state := map[gobreaker.State]float64{
				gobreaker.StateClosed: 0, gobreaker.StateHalfOpen: 1, gobreaker.StateOpen: 2,
			}[b.State()]
			metrics.CircuitBreakerState.WithLabelValues("gateway", "recommender").Set(state)
		}
	}()
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// noopCache is used when Redis is unavailable: every read misses and every
// write succeeds silently, so the request path still works (degraded).
type noopCache struct{}

func (noopCache) Get(_ context.Context, _ string) (*domain.Recommendation, bool, error) {
	return nil, false, nil
}
func (noopCache) Set(context.Context, string, *domain.Recommendation, time.Duration) error { return nil }
func (noopCache) DoDeduped(_ string, fn func() (*domain.Recommendation, error)) (*domain.Recommendation, error) {
	return fn()
}

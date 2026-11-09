// Package cache wraps Redis with singleflight stampede protection: when a
// user's recommendations expire, only one request recomputes them while
// concurrent requests wait on the same in-flight call.
package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/diegogo/recsplatform/pkg/domain"
	"github.com/diegogo/recsplatform/pkg/metrics"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// RecommendationCache is the port used by handlers.
type RecommendationCache interface {
	Get(ctx context.Context, userID string) (*domain.Recommendation, bool, error)
	Set(ctx context.Context, userID string, rec *domain.Recommendation, ttl time.Duration) error
}

// RedisCache implements RecommendationCache on top of Redis.
type RedisCache struct {
	client *redis.Client
	group  singleflight.Group
	ttl    time.Duration
}

func NewRedisCache(client *redis.Client, ttl time.Duration) *RedisCache {
	return &RedisCache{client: client, ttl: ttl}
}

func key(userID string) string { return "recs:" + userID }

// Get returns a cached recommendation, distinguishing hits from misses.
func (c *RedisCache) Get(ctx context.Context, userID string) (*domain.Recommendation, bool, error) {
	raw, err := c.client.Get(ctx, key(userID)).Bytes()
	if err == redis.Nil {
		metrics.CacheHits.WithLabelValues("gateway", "miss").Inc()
		return nil, false, nil
	}
	if err != nil {
		metrics.CacheHits.WithLabelValues("gateway", "error").Inc()
		return nil, false, fmt.Errorf("cache get: %w", err)
	}
	var rec domain.Recommendation
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, false, fmt.Errorf("cache unmarshal: %w", err)
	}
	metrics.CacheHits.WithLabelValues("gateway", "hit").Inc()
	return &rec, true, nil
}

// Set stores a recommendation with the configured TTL.
func (c *RedisCache) Set(ctx context.Context, userID string, rec *domain.Recommendation, _ time.Duration) error {
	rec.Cached = true
	rec.ExpireAt = time.Now().Add(c.ttl)
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, key(userID), raw, c.ttl).Err()
}

// Invalidate drops a user's cache, e.g. right after a strong event (purchase).
func (c *RedisCache) Invalidate(ctx context.Context, userID string) error {
	return c.client.Del(ctx, key(userID)).Err()
}

// DoDeduped runs fn at most once concurrently per key across this instance,
// preventing cache stampedes when a hot user's entry expires.
func (c *RedisCache) DoDeduped(userID string, fn func() (*domain.Recommendation, error)) (*domain.Recommendation, error) {
	v, err, _ := c.group.Do(key(userID), func() (any, error) { return fn() })
	if err != nil {
		return nil, err
	}
	rec, _ := v.(*domain.Recommendation)
	return rec, nil
}

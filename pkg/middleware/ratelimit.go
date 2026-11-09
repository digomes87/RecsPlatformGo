package middleware

import (
	"net/http"
	"sync"

	"github.com/diegogo/recsplatform/pkg/errorsx"
)

// tokenBucket is a minimal GCRA-free token bucket: refill is computed lazily
// on each take, so no background goroutine is needed.
type tokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	lastTake int64 // unix nanos
}

func (b *tokenBucket) take(now int64, ratePerSec float64, burst float64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	elapsed := float64(now-b.lastTake) / 1e9
	b.tokens = min(burst, b.tokens+elapsed*ratePerSec)
	b.lastTake = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// RateLimiter keeps one bucket per client key. Buckets are pruned lazily;
// for a multi-instance deployment the same interface can be backed by Redis.
type RateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*tokenBucket
	rate    float64
	burst   float64
	now     func() int64
}

// NewRateLimiter allows `rate` requests per second with a `burst` ceiling.
func NewRateLimiter(rate float64, burst int) *RateLimiter {
	return &RateLimiter{
		buckets: make(map[string]*tokenBucket),
		rate:    rate,
		burst:   float64(burst),
		now:     func() int64 { return timeNowUnixNano() },
	}
}

// Middleware enforces the limit per key extracted from the request (user id
// when authenticated, otherwise remote address).
func (rl *RateLimiter) Middleware(keyFunc func(r *http.Request) string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := keyFunc(r)
			rl.mu.Lock()
			b, ok := rl.buckets[key]
			if !ok {
				// Opportunistic pruning keeps the map bounded under abuse.
				if len(rl.buckets) > 100_000 {
					rl.buckets = make(map[string]*tokenBucket)
				}
				b = &tokenBucket{tokens: rl.burst, lastTake: rl.now()}
				rl.buckets[key] = b
			}
			rl.mu.Unlock()
			if !b.take(rl.now(), rl.rate, rl.burst) {
				w.Header().Set("Retry-After", "1")
				writeError(w, errorsx.TooManyRequests("rate limit exceeded"))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

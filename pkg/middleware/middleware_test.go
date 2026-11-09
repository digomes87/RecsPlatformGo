package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestRateLimiter_AllowsUnderBurstThenBlocks(t *testing.T) {
	rl := NewRateLimiter(1, 3) // 1 rps, burst 3
	handler := rl.Middleware(func(r *http.Request) string { return "key" })(okHandler)

	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d should pass, got %d", i, rec.Code)
		}
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("4th immediate request should be limited, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header on 429")
	}
}

func TestRateLimiter_RefillsOverTime(t *testing.T) {
	rl := NewRateLimiter(1, 1)
	rl.now = func() int64 { return 0 } // freeze time
	handler := rl.Middleware(func(r *http.Request) string { return "k" })(okHandler)
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("bucket should be empty, got %d", rec.Code)
	}
	// Advance 1s: one token refilled.
	rl.now = func() int64 { return int64(time.Second) }
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("bucket should have refilled, got %d", rec.Code)
	}
}

func TestRateLimiter_KeysAreIsolated(t *testing.T) {
	rl := NewRateLimiter(0.0001, 1)
	key := func(r *http.Request) string { return r.Header.Get("X-User") }
	handler := rl.Middleware(key)(okHandler)

	ok := func(user string) bool {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-User", user)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec.Code == http.StatusOK
	}
	if !ok("a") || ok("a") {
		t.Fatal("user a should hit the limit on the second request")
	}
	if !ok("b") {
		t.Fatal("user b should not be affected by user a's limit")
	}
}

func TestAuth_RejectsMissingAndInvalidTokens(t *testing.T) {
	handler := Auth("secret")(okHandler)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no token should 401, got %d", rec.Code)
	}

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer not-a-jwt")
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("garbage token should 401, got %d", rec.Code)
	}
}

func TestAuth_AcceptsValidTokenAndPropagatesClaims(t *testing.T) {
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"sub":    "user-42",
		"scopes": []any{"read"},
	})
	signed, err := tok.SignedString([]byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	var got Claims
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = ClaimsFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	handler := Auth("secret")(inner)

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("valid token should pass, got %d", rec.Code)
	}
	if got.UserID != "user-42" || len(got.Scopes) != 1 {
		t.Fatalf("claims not propagated: %+v", got)
	}
}

func TestAuth_RejectsWrongSecret(t *testing.T) {
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"sub": "u"})
	signed, _ := tok.SignedString([]byte("other-secret"))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+signed)
	rec := httptest.NewRecorder()
	Auth("secret")(okHandler).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("token signed with wrong secret must be rejected, got %d", rec.Code)
	}
}

func TestRequestID_GeneratesAndEchoes(t *testing.T) {
	var seen string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = RequestIDFrom(r.Context())
	})
	rec := httptest.NewRecorder()
	RequestID(inner).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if seen == "" || rec.Header().Get("X-Request-ID") == "" {
		t.Fatalf("request id should be generated and echoed, got %q / header %q", seen, rec.Header().Get("X-Request-ID"))
	}
}

// okHandler is a minimal always-200 handler for middleware tests.
var okHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
})

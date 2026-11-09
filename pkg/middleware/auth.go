package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/diegogo/recsplatform/pkg/errorsx"
	"github.com/golang-jwt/jwt/v5"
)

type userKey struct{}

// Claims carries the authenticated principal through the request scope.
type Claims struct {
	UserID string
	Scopes []string
}

// Auth validates a Bearer JWT (HS256) and stores the claims in the context.
// In production the verifier would fetch JWKS from an identity provider;
// a shared secret keeps the study case self-contained.
func Auth(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			header := r.Header.Get("Authorization")
			tokenStr, ok := strings.CutPrefix(header, "Bearer ")
			if !ok || tokenStr == "" {
				writeError(w, errorsx.Unauthorized("missing bearer token"))
				return
			}
			claims := jwt.MapClaims{}
			_, err := jwt.ParseWithClaims(tokenStr, claims, func(t *jwt.Token) (any, error) {
				return []byte(secret), nil
			}, jwt.WithValidMethods([]string{"HS256"}))
			if err != nil {
				writeError(w, errorsx.Unauthorized("invalid token"))
				return
			}
			sub, _ := claims["sub"].(string)
			if sub == "" {
				writeError(w, errorsx.Unauthorized("token missing subject"))
				return
			}
			var scopes []string
			if raw, ok := claims["scopes"].([]any); ok {
				for _, s := range raw {
					if str, ok := s.(string); ok {
						scopes = append(scopes, str)
					}
				}
			}
			ctx := context.WithValue(r.Context(), userKey{}, Claims{UserID: sub, Scopes: scopes})
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// ClaimsFrom extracts the authenticated claims set by Auth.
func ClaimsFrom(ctx context.Context) (Claims, bool) {
	c, ok := ctx.Value(userKey{}).(Claims)
	return c, ok
}

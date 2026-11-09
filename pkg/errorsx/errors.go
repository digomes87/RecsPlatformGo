// Package errorsx defines application errors mapped to HTTP status codes,
// keeping transport concerns out of the domain logic.
package errorsx

import "net/http"

// AppError is an error with an HTTP mapping.
type AppError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Status  int    `json:"-"`
}

func (e *AppError) Error() string { return e.Code + ": " + e.Message }

func New(code, message string, status int) *AppError {
	return &AppError{Code: code, Message: message, Status: status}
}

// Common constructors keep error shapes consistent across services.
func InvalidArgument(msg string) *AppError {
	return New("invalid_argument", msg, http.StatusBadRequest)
}

func Unauthorized(msg string) *AppError {
	return New("unauthorized", msg, http.StatusUnauthorized)
}

func NotFound(msg string) *AppError {
	return New("not_found", msg, http.StatusNotFound)
}

func TooManyRequests(msg string) *AppError {
	return New("rate_limited", msg, http.StatusTooManyRequests)
}

func UpstreamFailure(msg string) *AppError {
	return New("upstream_failure", msg, http.StatusBadGateway)
}

func Internal(msg string) *AppError {
	return New("internal", msg, http.StatusInternalServerError)
}

// Package domain holds the shared business types used across services:
// user interaction events (inbound) and recommendations (outbound).
package domain

import "time"

// EventType enumerates the interactions users can have with items.
type EventType string

const (
	EventView      EventType = "view"
	EventLike      EventType = "like"
	EventPurchase  EventType = "purchase"
	EventRating    EventType = "rating"
)

// Weight maps event types to the contribution weight used when computing
// user-item affinity. Purchases signal stronger intent than views.
func (e EventType) Weight() float64 {
	switch e {
	case EventPurchase:
		return 3.0
	case EventRating:
		return 2.0
	case EventLike:
		return 1.5
	case EventView:
		return 1.0
	default:
		return 0.5
	}
}

// Event is a single user interaction, ingested through the public API and
// published to Kafka for asynchronous processing.
type Event struct {
	EventID   string    `json:"event_id"`
	UserID    string    `json:"user_id"`
	ItemID    string    `json:"item_id"`
	EventType EventType `json:"event_type"`
	Value     float64   `json:"value,omitempty"` // e.g. rating stars
	Timestamp time.Time `json:"timestamp"`
}

// Item is a recommended item with its blended score.
type Item struct {
	ItemID string  `json:"item_id"`
	Score  float64 `json:"score"`
	Reason string  `json:"reason,omitempty"` // "similar to items you liked", "trending", ...
}

// ErrorBody is the internal error payload shape.
type ErrorBody struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// Recommendation is the response payload for the recommendations endpoint.
type Recommendation struct {
	UserID   string    `json:"user_id"`
	Items    []Item    `json:"items"`
	Cached   bool      `json:"cached"`
	ExpireAt time.Time `json:"expire_at"`
}

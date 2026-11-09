// Package producer publishes validated interaction events to Kafka. The
// key is the user id so all events of a user land on the same partition,
// preserving per-user ordering.
package producer

import (
	"context"
	"encoding/json"
	"time"

	"github.com/diegogo/recsplatform/pkg/domain"
	"github.com/segmentio/kafka-go"
)

// EventProducer is the port used by handlers.
type EventProducer interface {
	Publish(ctx context.Context, evt domain.Event) error
	Close() error
}

// KafkaProducer implements EventProducer with the segmentio writer.
type KafkaProducer struct {
	writer *kafka.Writer
}

func New(w *kafka.Writer) *KafkaProducer { return &KafkaProducer{writer: w} }

// Publish serializes the event to JSON and writes it keyed by user id.
func (p *KafkaProducer) Publish(ctx context.Context, evt domain.Event) error {
	if evt.Timestamp.IsZero() {
		evt.Timestamp = time.Now().UTC()
	}
	raw, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(evt.UserID),
		Value: raw,
	})
}

// Close flushes pending batches before shutdown.
func (p *KafkaProducer) Close() error { return p.writer.Close() }

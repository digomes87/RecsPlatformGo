// Package kafka normalizes reader/writer construction with the segmentio
// driver: explicit consumer groups, balanced partitions and timeouts.
package kafka

import (
	"github.com/segmentio/kafka-go"
)

// NewWriter returns a producer for the given topic with idempotent writes
// and all-replica acknowledgement, trading a little latency for durability.
func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		RequiredAcks: kafka.RequireAll,
		AllowAutoTopicCreation: true,
		BatchSize:    100,
		Async:        false,
	}
}

// NewReader returns a consumer-group reader. GroupID enables horizontal
// scale-out: partitions are rebalanced automatically across instances.
func NewReader(brokers []string, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
		// When a brand-new group has no committed offset, start from the
		// beginning so history is not silently skipped.
		StartOffset: kafka.FirstOffset,
		CommitInterval: 0, // synchronous commits: at-most-once duplicates are dedup'd downstream
	})
}

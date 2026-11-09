// Package consumer polls Kafka and flushes events to MongoDB in batches:
// batching amortizes round trips and keeps write throughput high while
// bounding both latency (flush interval) and memory (batch size).
package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/diegogo/recsplatform/pkg/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
)

var (
	eventsConsumed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ingestor_events_consumed_total",
		Help: "Events consumed from Kafka.",
	})
	eventsStored = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "ingestor_events_stored_total",
		Help: "Events successfully persisted to MongoDB.",
	})
	batchLatency = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "ingestor_batch_duration_seconds",
		Help:    "MongoDB batch flush duration.",
		Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
	})
	consumerLag = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag_seconds",
		Help: "Seconds the consumer group is behind the log end (drives HPA).",
	})
)

func init() {
	prometheus.MustRegister(eventsConsumed, eventsStored, batchLatency, consumerLag)
}

// Store is the persistence port implemented by store.Store.
type Store interface {
	Batch(ctx context.Context, events []domain.Event) (int, error)
}

// Consumer batches messages before persisting them.
type Consumer struct {
	reader    *kafka.Reader
	store     Store
	log       *slog.Logger
	batchSize int
	flushEvery time.Duration
}

// New configures the consumer; batchSize and flushEvery bound memory and
// end-to-end latency respectively.
func New(reader *kafka.Reader, store Store, log *slog.Logger, batchSize int, flushEvery time.Duration) *Consumer {
	if batchSize <= 0 {
		batchSize = 200
	}
	if flushEvery <= 0 {
		flushEvery = 2 * time.Second
	}
	return &Consumer{reader: reader, store: store, log: log, batchSize: batchSize, flushEvery: flushEvery}
}

// Run consumes until the context is cancelled, flushing the in-flight
// batch on shutdown so no accepted event is lost.
func (c *Consumer) Run(ctx context.Context) error {
	batch := make([]domain.Event, 0, c.batchSize)
	flushTicker := time.NewTicker(c.flushEvery)
	defer flushTicker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		start := time.Now()
		n, err := c.store.Batch(ctx, batch)
		if err != nil {
			// The consumer group offset is not committed on error, so the
			// batch is redelivered; the dedup set makes retries idempotent.
			c.log.Error("batch persist failed", "error", err, "batch_size", len(batch))
		} else {
			eventsStored.Add(float64(n))
			batchLatency.Observe(time.Since(start).Seconds())
		}
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return nil
		case <-flushTicker.C:
			flush()
			c.exportLag()
		default:
		}

		msg, err := c.readWithCtx(ctx)
		if err != nil {
			if ctx.Err() != nil {
				flush()
				return nil
			}
			c.log.Error("kafka read failed", "error", err)
			continue
		}
		var evt domain.Event
		if err := json.Unmarshal(msg.Value, &evt); err != nil {
			// Poison message: log and skip — never block the partition.
			c.log.Warn("skipping malformed event", "error", err)
			_ = c.reader.CommitMessages(ctx, msg)
			continue
		}
		batch = append(batch, evt)
		eventsConsumed.Inc()
		if len(batch) >= c.batchSize {
			flush()
		}
	}
}

// exportLag publishes the consumer group lag in seconds; the HPA scales on
// this signal, keeping ingestion latency bounded during traffic spikes.
func (c *Consumer) exportLag() {
	lag := c.reader.Lag()
	if lag < 0 {
		consumerLag.Set(0)
		return
	}
	// Approximation: lag messages / conservative throughput per second.
	throughputPerSec := float64(c.batchSize) * (1.0 / c.flushEvery.Seconds())
	if throughputPerSec <= 0 {
		consumerLag.Set(0)
		return
	}
	consumerLag.Set(float64(lag) / throughputPerSec)
}

// readWithCtx bridges the non-context FetchMessage API with cancellation.
func (c *Consumer) readWithCtx(ctx context.Context) (kafka.Message, error) {
	type result struct {
		msg kafka.Message
		err error
	}
	ch := make(chan result, 1)
	go func() {
		m, err := c.reader.FetchMessage(ctx)
		ch <- result{m, err}
	}()
	select {
	case r := <-ch:
		return r.msg, r.err
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	}
}

// Command ingestor consumes interaction events from Kafka, persists them
// to MongoDB in idempotent batches and periodically rebuilds the item
// similarity matrix used by the recommender.
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
	"github.com/diegogo/recsplatform/pkg/kafka"
	"github.com/diegogo/recsplatform/pkg/logger"
	recsmongo "github.com/diegogo/recsplatform/pkg/mongo"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/diegogo/recsplatform/services/ingestor/internal/consumer"
	"github.com/diegogo/recsplatform/services/ingestor/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func main() {
	cfg := config.Load("ingestor")
	log := logger.New(cfg.ServiceName, cfg.Environment)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, disconnect, err := recsmongo.Connect(ctx, cfg.MongoURI)
	if err != nil {
		log.Error("mongo connection failed", "error", err)
		os.Exit(1)
	}
	defer disconnect()
	db := client.Database(cfg.MongoDB)

	if err := recsmongo.EnsureIndexes(ctx, db, indexes()); err != nil {
		log.Error("index provisioning failed", "error", err)
		os.Exit(1)
	}

	st := store.New(db)
	reader := kafka.NewReader(cfg.KafkaBrokers, cfg.KafkaTopic, "ingestor-group")
	cons := consumer.New(reader, st, log,
		config.GetEnvInt("BATCH_SIZE", 200),
		time.Duration(config.GetEnvInt("FLUSH_INTERVAL_MS", 2000))*time.Millisecond)

	// Internal ops endpoint: probes and metrics.
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	ops := &http.Server{Addr: cfg.HTTPAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("ingestor ops endpoint listening", "addr", cfg.HTTPAddr)
		if err := ops.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("ops server error", "error", err)
		}
	}()

	// Similarity rebuild loop: every SIMILARITY_INTERVAL (default 5m), or
	// once at startup so a fresh environment has usable data quickly.
	rebuildEvery := time.Duration(config.GetEnvInt("SIMILARITY_INTERVAL_SECONDS", 300)) * time.Second
	go func() {
		window := time.Duration(config.GetEnvInt("SIMILARITY_WINDOW_HOURS", 48)) * time.Hour
		run := func() {
			start := time.Now()
			if err := st.RebuildSimilarity(ctx, window); err != nil && ctx.Err() == nil {
				log.Error("similarity rebuild failed", "error", err)
				return
			}
			log.Info("similarity matrix rebuilt", "window", window, "took", time.Since(start))
		}
		run()
		ticker := time.NewTicker(rebuildEvery)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				run()
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		if err := cons.Run(ctx); err != nil {
			log.Error("consumer stopped with error", "error", err)
			cancel()
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Info("shutting down ingestor")
	cancel()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	_ = ops.Shutdown(shutdownCtx)
	_ = reader.Close()
	log.Info("ingestor stopped")
}

func indexes() []recsmongo.Index {
	return []recsmongo.Index{
		{Collection: "processed_events", Name: "event_id_unique", Keys: bson.D{{Key: "event_id", Value: 1}}, Unique: true},
		{Collection: "processed_events", Name: "processed_ttl", Keys: bson.D{{Key: "processed_at", Value: 1}}, TTLSeconds: 86400},
		{Collection: "events", Name: "day_ttl", Keys: bson.D{{Key: "timestamp", Value: 1}}, TTLSeconds: 30 * 86400},
		{Collection: "user_interactions", Name: "user_recent", Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "at", Value: -1}}},
		{Collection: "trending", Name: "score_desc", Keys: bson.D{{Key: "score", Value: -1}}},
	}
}

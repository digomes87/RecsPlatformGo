// Command recommender is the internal recommendation engine. It reads
// precomputed similarity/trending data from MongoDB and blends signals to
// produce personalized recommendations.
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
	"github.com/diegogo/recsplatform/pkg/logger"
	recsmongo "github.com/diegogo/recsplatform/pkg/mongo"
	"github.com/diegogo/recsplatform/services/recommender/internal/api"
	"github.com/diegogo/recsplatform/services/recommender/internal/engine"
	"github.com/diegogo/recsplatform/services/recommender/internal/repository"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func main() {
	cfg := config.Load("recommender")
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

	repo := repository.New(db)
	eng := engine.New(repo, engine.DefaultWeights(),
		config.GetEnvInt("HISTORY_DEPTH", 20),
		config.GetEnvInt("NEIGHBOR_DEPTH", 10))

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.New(eng, log).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
	}

	go func() {
		log.Info("recommender listening", "addr", cfg.HTTPAddr, "env", cfg.Environment)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Info("shutting down recommender")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("graceful shutdown failed", "error", err)
	}
	log.Info("recommender stopped")
}

func indexes() []recsmongo.Index {
	return []recsmongo.Index{
		{Collection: "user_interactions", Name: "user_recent", Keys: bson.D{{Key: "user_id", Value: 1}, {Key: "at", Value: -1}}},
		{Collection: "item_similarity", Name: "neighbors", Keys: bson.D{{Key: "_id", Value: 1}}},
		{Collection: "trending", Name: "score_desc", Keys: bson.D{{Key: "score", Value: -1}}},
	}
}

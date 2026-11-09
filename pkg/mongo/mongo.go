// Package mongo wraps the official driver with connection pooling, timeout
// enforcement and index provisioning shared by every service.
package mongo

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Connect opens a pooled connection. The client is safe for concurrent use
// and managed by the caller via the returned disconnect function.
func Connect(ctx context.Context, uri string) (*mongo.Client, func(), error) {
	client, err := mongo.Connect(options.Client().
		ApplyURI(uri).
		SetMaxPoolSize(100).
		SetMinPoolSize(5).
		SetMaxConnIdleTime(5 * time.Minute).
		SetConnectTimeout(5 * time.Second).
		SetServerSelectionTimeout(3 * time.Second))
	if err != nil {
		return nil, nil, fmt.Errorf("mongo connect: %w", err)
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		return nil, nil, fmt.Errorf("mongo ping: %w", err)
	}
	disconnect := func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		_ = client.Disconnect(dctx)
	}
	return client, disconnect, nil
}

// Index describes an index to ensure at startup, keeping schema provisioning
// in code (no manual migration step in CI/CD).
type Index struct {
	Collection string
	Keys       any
	Unique     bool
	TTLSeconds int32
	Name       string
}

// EnsureIndexes creates the given indexes if missing. Idempotent, safe to
// call on every boot.
func EnsureIndexes(ctx context.Context, db *mongo.Database, idxs []Index) error {
	for _, idx := range idxs {
		opts := options.Index().SetName(idx.Name)
		if idx.Unique {
			opts.SetUnique(true)
		}
		if idx.TTLSeconds > 0 {
			opts.SetExpireAfterSeconds(idx.TTLSeconds)
		}
		_, err := db.Collection(idx.Collection).Indexes().CreateOne(ctx, mongo.IndexModel{
			Keys:    idx.Keys,
			Options: opts,
		})
		if err != nil {
			return fmt.Errorf("ensure index %s.%s: %w", idx.Collection, idx.Name, err)
		}
	}
	return nil
}

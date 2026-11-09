// Package repository provides the MongoDB data access used by the
// recommendation engine: item similarity vectors, trending counters and
// user interaction history.
package repository

import (
	"context"
	"time"

	"github.com/diegogo/recsplatform/pkg/domain"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// SimilarityItem is one neighbor of an item: another item and its cosine
// similarity score, computed by the ingestor's aggregation pipeline.
type SimilarityItem struct {
	ItemID         string  `bson:"item_id"`
	Similarity     float64 `bson:"similarity"`
}

// ItemSimilarity holds the precomputed neighbor list for one item.
type ItemSimilarity struct {
	ItemID     string           `bson:"_id"`
	Neighbors  []SimilarityItem `bson:"neighbors"`
	UpdatedAt  time.Time        `bson:"updated_at"`
}

// TrendingItem is an item with its decayed popularity score for a window.
type TrendingItem struct {
	ItemID string  `bson:"_id"`
	Score  float64 `bson:"score"`
}

// UserInteraction is the denormalized per-user interaction record written
// by the ingestor; the recommender reads it to build a user taste profile.
type UserInteraction struct {
	UserID string          `bson:"user_id"`
	ItemID string          `bson:"item_id"`
	Type   domain.EventType `bson:"type"`
	Weight float64         `bson:"weight"`
	At     time.Time        `bson:"at"`
}

// Repository is the storage port of the recommender.
type Repository interface {
	GetNeighbors(ctx context.Context, itemID string, limit int) ([]SimilarityItem, error)
	GetTrending(ctx context.Context, limit int) ([]TrendingItem, error)
	GetUserHistory(ctx context.Context, userID string, limit int) ([]UserInteraction, error)
}

// MongoRepository implements Repository against MongoDB.
type MongoRepository struct {
	sim *mongo.Collection
	trd *mongo.Collection
	his *mongo.Collection
}

// New wires the three collections used by the engine.
func New(db *mongo.Database) *MongoRepository {
	return &MongoRepository{
		sim: db.Collection("item_similarity"),
		trd: db.Collection("trending"),
		his: db.Collection("user_interactions"),
	}
}

// GetNeighbors returns the most similar items for a given item.
func (r *MongoRepository) GetNeighbors(ctx context.Context, itemID string, limit int) ([]SimilarityItem, error) {
	var doc ItemSimilarity
	err := r.sim.FindOne(ctx, bson.M{"_id": itemID}).Decode(&doc)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(doc.Neighbors) > limit {
		doc.Neighbors = doc.Neighbors[:limit]
	}
	return doc.Neighbors, nil
}

// GetTrending returns the top items by decayed score, newest first.
func (r *MongoRepository) GetTrending(ctx context.Context, limit int) ([]TrendingItem, error) {
	cursor, err := r.trd.Find(ctx, bson.M{}, options.Find().
		SetSort(bson.D{{Key: "score", Value: -1}}).
		SetLimit(int64(limit)))
	if err != nil {
		return nil, err
	}
	var out []TrendingItem
	return out, cursor.All(ctx, &out)
}

// GetUserHistory returns the user's most recent interactions.
func (r *MongoRepository) GetUserHistory(ctx context.Context, userID string, limit int) ([]UserInteraction, error) {
	cursor, err := r.his.Find(ctx, bson.M{"user_id": userID}, options.Find().
		SetSort(bson.D{{Key: "at", Value: -1}}).
		SetLimit(int64(limit)))
	if err != nil {
		return nil, err
	}
	var out []UserInteraction
	return out, cursor.All(ctx, &out)
}

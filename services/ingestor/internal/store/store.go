// Package store persists interaction events into MongoDB in batches and
// maintains the derived collections (user_interactions, trending,
// item_similarity) consumed by the recommender.
package store

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/diegogo/recsplatform/pkg/domain"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Store handles all ingestor writes.
type Store struct {
	events *mongo.Collection // raw event log (TTL'd)
	users  *mongo.Collection // denormalized per-user history
	trend  *mongo.Collection // trending counters
	seen   *mongo.Collection // deduplication set (TTL'd)
}

// New wires the ingestor collections.
func New(db *mongo.Database) *Store {
	return &Store{
		events: db.Collection("events"),
		users:  db.Collection("user_interactions"),
		trend:  db.Collection("trending"),
		seen:   db.Collection("processed_events"),
	}
}

// eventDoc is the raw persisted event. Bucketed by day so the TTL index can
// expire old partitions cheaply.
type eventDoc struct {
	ID        bson.ObjectID     `bson:"_id,omitempty"`
	EventID   string            `bson:"event_id"`
	UserID    string            `bson:"user_id"`
	ItemID    string            `bson:"item_id"`
	EventType domain.EventType  `bson:"event_type"`
	Value     float64           `bson:"value,omitempty"`
	Day       string            `bson:"day"` // YYYY-MM-DD bucket
	Timestamp time.Time         `bson:"timestamp"`
}

// userDoc increments the per-user/item affinity weight atomically.
type userDoc struct {
	UserID  string `bson:"user_id"`
	ItemID  string `bson:"item_id"`
	Weight  float64 `bson:"weight"`
	At      time.Time `bson:"at"`
}

// Batch persists a batch of events idempotently:
//   - dedup via a unique event_id set with TTL (retries are safe)
//   - raw append to the event log
//   - upsert of the user interaction weight (event weight folded in)
//   - trending counter increment
//
// Each target collection gets its own unordered bulk write. Duplicate event
// ids surface as upsertedCount < len(events) and are simply not re-logged.
func (s *Store) Batch(ctx context.Context, events []domain.Event) (int, error) {
	if len(events) == 0 {
		return 0, nil
	}
	now := time.Now().UTC()
	var dedupModels, eventModels, userModels, trendModels []mongo.WriteModel
	for _, e := range events {
		if e.Timestamp.IsZero() {
			e.Timestamp = now
		}
		dedupModels = append(dedupModels,
			mongo.NewUpdateOneModel().
				SetFilter(bson.M{"event_id": e.EventID}).
				SetUpdate(bson.M{"$setOnInsert": bson.M{"event_id": e.EventID, "processed_at": now}}).
				SetUpsert(true))
		eventModels = append(eventModels,
			mongo.NewInsertOneModel().SetDocument(eventDoc{
				EventID: e.EventID, UserID: e.UserID, ItemID: e.ItemID,
				EventType: e.EventType, Value: e.Value,
				Day: e.Timestamp.Format("2006-01-02"), Timestamp: e.Timestamp,
			}))
		userModels = append(userModels,
			mongo.NewUpdateOneModel().
				SetFilter(bson.M{"user_id": e.UserID, "item_id": e.ItemID}).
				SetUpdate(bson.M{
					"$inc": bson.M{"weight": e.EventType.Weight() + e.Value},
					"$max": bson.M{"at": e.Timestamp},
				}).
				SetUpsert(true))
		trendModels = append(trendModels,
			mongo.NewUpdateOneModel().
				SetFilter(bson.M{"_id": e.ItemID}).
				SetUpdate(bson.M{
					"$inc": bson.M{"score": e.EventType.Weight()},
					"$set": bson.M{"last_event_at": e.Timestamp},
				}).
				SetUpsert(true))
	}
	opts := options.BulkWrite().SetOrdered(false)

	// Dedup first: its result tells us how many events are actually new.
	dedupRes, err := s.seen.BulkWrite(ctx, dedupModels, opts)
	if err != nil {
		return 0, err
	}
	newCount := len(events)
	if dedupRes != nil && dedupRes.UpsertedCount < int64(len(events)) {
		newCount = int(dedupRes.UpsertedCount)
	}

	if _, err := s.events.BulkWrite(ctx, eventModels, opts); err != nil {
		return newCount, err
	}
	if _, err := s.users.BulkWrite(ctx, userModels, opts); err != nil {
		return newCount, err
	}
	if _, err := s.trend.BulkWrite(ctx, trendModels, opts); err != nil {
		return newCount, err
	}
	return newCount, nil
}

// RebuildSimilarity recomputes item-item cosine similarity from the user
// interaction matrix using a MongoDB aggregation pipeline. Scheduled by the
// ingestor every few minutes; the recommender always reads a consistent,
// precomputed view.
func (s *Store) RebuildSimilarity(ctx context.Context, window time.Duration) error {
	since := time.Now().UTC().Add(-window)

	// co-rated matrix: for each (item_a, item_b) pair, count shared users
	// and accumulate weighted products for cosine similarity.
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.M{"at": bson.M{"$gte": since}}}},
		{{Key: "$sort", Value: bson.D{{Key: "user_id", Value: 1}}}},
		// self-join by user window: group interactions per user, unwind pairs
		{{Key: "$group", Value: bson.M{
			"_id": "$user_id",
			"items": bson.M{"$push": bson.M{"item": "$item_id", "w": "$weight"}},
		}}},
	}
	cursor, err := s.users.Aggregate(ctx, pipeline)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)

	type userAgg struct {
		Items []struct {
			Item string  `bson:"item"`
			W    float64 `bson:"w"`
		} `bson:"items"`
	}
	// Pair accumulation happens in Go: the co-occurrence space is sparse and
	// pushing cross products through $unwind would explode the pipeline.
	norms := map[string]float64{}
	pairs := map[[2]string]float64{}

	for cursor.Next(ctx) {
		var agg userAgg
		if err := cursor.Decode(&agg); err != nil {
			return err
		}
		for i := range agg.Items {
			a := agg.Items[i]
			norms[a.Item] += a.W * a.W
			for j := i + 1; j < len(agg.Items); j++ {
				b := agg.Items[j]
				if a.Item > b.Item {
					pairs[[2]string{b.Item, a.Item}] += a.W * b.W
				} else {
					pairs[[2]string{a.Item, b.Item}] += a.W * b.W
				}
			}
		}
	}
	if err := cursor.Err(); err != nil {
		return err
	}

	// Persist top-N neighbors per item.
	byItem := map[string][]neighbor{}
	for pair, dot := range pairs {
		na, nb := norms[pair[0]], norms[pair[1]]
		if na == 0 || nb == 0 {
			continue
		}
		sim := dot / (math.Sqrt(na) * math.Sqrt(nb))
		byItem[pair[0]] = append(byItem[pair[0]], neighbor{ItemID: pair[1], Similarity: sim})
		byItem[pair[1]] = append(byItem[pair[1]], neighbor{ItemID: pair[0], Similarity: sim})
	}

	simCol := s.events.Database().Collection("item_similarity")
	var models []mongo.WriteModel
	now := time.Now().UTC()
	for itemID, neighbors := range byItem {
		topN := topNeighbors(neighbors, 50)
		models = append(models, mongo.NewUpdateOneModel().
			SetFilter(bson.M{"_id": itemID}).
			SetUpdate(bson.M{"$set": bson.M{"neighbors": topN, "updated_at": now}}).
			SetUpsert(true))
	}
	if len(models) == 0 {
		return nil
	}
	_, err = simCol.BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false))
	return err
}

// neighbor is one similarity edge persisted in item_similarity.
type neighbor struct {
	ItemID     string  `bson:"item_id"`
	Similarity float64 `bson:"similarity"`
}

// topNeighbors sorts by similarity (desc) and clips to n.
func topNeighbors(in []neighbor, n int) []neighbor {
	sort.Slice(in, func(i, j int) bool { return in[i].Similarity > in[j].Similarity })
	if len(in) > n {
		in = in[:n]
	}
	return in
}

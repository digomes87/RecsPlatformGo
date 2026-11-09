package engine

import (
	"context"
	"testing"

	"github.com/diegogo/recsplatform/pkg/domain"
	"github.com/diegogo/recsplatform/services/recommender/internal/repository"
)

// fakeRepo stubs the repository for algorithmic tests.
type fakeRepo struct {
	history   []repository.UserInteraction
	neighbors map[string][]repository.SimilarityItem
	trending  []repository.TrendingItem
}

func (f *fakeRepo) GetNeighbors(_ context.Context, itemID string, limit int) ([]repository.SimilarityItem, error) {
	ns := f.neighbors[itemID]
	if len(ns) > limit {
		ns = ns[:limit]
	}
	return ns, nil
}

func (f *fakeRepo) GetTrending(_ context.Context, limit int) ([]repository.TrendingItem, error) {
	if len(f.trending) > limit {
		return f.trending[:limit], nil
	}
	return f.trending, nil
}

func (f *fakeRepo) GetUserHistory(_ context.Context, userID string, limit int) ([]repository.UserInteraction, error) {
	h := f.history
	if len(h) > limit {
		h = h[:limit]
	}
	return h, nil
}

func TestRecommend_ExcludesSeenItems(t *testing.T) {
	repo := &fakeRepo{
		history: []repository.UserInteraction{
			{UserID: "u1", ItemID: "i1", Weight: 3, Type: domain.EventPurchase},
		},
		neighbors: map[string][]repository.SimilarityItem{
			"i1": {{ItemID: "i1", Similarity: 1.0}, {ItemID: "i2", Similarity: 0.8}},
		},
		trending: []repository.TrendingItem{{ItemID: "i1", Score: 10}},
	}
	eng := New(repo, DefaultWeights(), 20, 10)
	items, err := eng.Recommend(context.Background(), "u1", 10)
	if err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	for _, it := range items {
		if it.ItemID == "i1" {
			t.Fatalf("recommended an item the user already interacted with")
		}
	}
}

func TestRecommend_RanksByBlendedScore(t *testing.T) {
	repo := &fakeRepo{
		history: []repository.UserInteraction{
			{UserID: "u1", ItemID: "i1", Weight: 3, Type: domain.EventPurchase},
		},
		neighbors: map[string][]repository.SimilarityItem{
			"i1": {{ItemID: "weak", Similarity: 0.2}, {ItemID: "strong", Similarity: 0.9}},
		},
		trending: []repository.TrendingItem{{ItemID: "mid", Score: 5}},
	}
	eng := New(repo, DefaultWeights(), 20, 10)
	items, err := eng.Recommend(context.Background(), "u1", 10)
	if err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	if len(items) < 2 || items[0].ItemID != "strong" {
		t.Fatalf("expected 'strong' ranked first, got %+v", items)
	}
}

func TestRecommend_ColdStartFallsBackToPopular(t *testing.T) {
	repo := &fakeRepo{
		trending: []repository.TrendingItem{
			{ItemID: "t1", Score: 9},
			{ItemID: "t2", Score: 5},
			{ItemID: "t3", Score: 1},
		},
	}
	eng := New(repo, DefaultWeights(), 20, 10)
	items, err := eng.Recommend(context.Background(), "new-user", 2)
	if err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	if len(items) != 2 || items[0].ItemID != "t1" {
		t.Fatalf("expected trending fallback ['t1','t2'], got %+v", items)
	}
	if items[0].Reason != "popular right now" {
		t.Fatalf("expected cold-start reason, got %q", items[0].Reason)
	}
}

func TestRecommend_RespectsLimit(t *testing.T) {
	repo := &fakeRepo{
		trending: []repository.TrendingItem{
			{ItemID: "a", Score: 3}, {ItemID: "b", Score: 2}, {ItemID: "c", Score: 1},
		},
	}
	eng := New(repo, DefaultWeights(), 20, 10)
	items, err := eng.Recommend(context.Background(), "u", 2)
	if err != nil {
		t.Fatalf("Recommend: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected limit honored, got %d items", len(items))
	}
}

func BenchmarkRecommend(b *testing.B) {
	repo := &fakeRepo{
		history: makeHistory(20),
		neighbors: map[string][]repository.SimilarityItem{
			"i1": makeNeighbors(10), "i2": makeNeighbors(10),
		},
		trending: makeTrending(50),
	}
	eng := New(repo, DefaultWeights(), 20, 10)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := eng.Recommend(ctx, "u1", 10); err != nil {
			b.Fatal(err)
		}
	}
}

func makeHistory(n int) []repository.UserInteraction {
	out := make([]repository.UserInteraction, n)
	for i := range out {
		out[i] = repository.UserInteraction{UserID: "u1", ItemID: "i1", Weight: 2, Type: domain.EventLike}
	}
	return out
}

func makeNeighbors(n int) []repository.SimilarityItem {
	out := make([]repository.SimilarityItem, n)
	for i := range out {
		out[i] = repository.SimilarityItem{ItemID: "n", Similarity: 0.5}
	}
	return out
}

func makeTrending(n int) []repository.TrendingItem {
	out := make([]repository.TrendingItem, n)
	for i := range out {
		out[i] = repository.TrendingItem{ItemID: "t", Score: float64(n - i)}
	}
	return out
}

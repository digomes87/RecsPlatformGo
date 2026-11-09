// Package engine implements the recommendation strategies and their blend.
//
// Three signals are combined with configurable weights:
//   - collaborative filtering (item-based cosine similarity, precomputed
//     by the ingestor and stored in MongoDB)
//   - trending (time-decayed popularity per window)
//   - popular fallback (cold-start users)
package engine

import (
	"context"
	"sort"

	"github.com/diegogo/recsplatform/pkg/domain"
	"github.com/diegogo/recsplatform/services/recommender/internal/repository"
)

// Weights controls the score blend; exposed as config for experimentation.
type Weights struct {
	Collaborative float64 // e.g. 0.6
	Trending      float64 // e.g. 0.3
	Popular       float64 // e.g. 0.1
}

// DefaultWeights favors personalization, adding trending for discovery.
func DefaultWeights() Weights { return Weights{Collaborative: 0.6, Trending: 0.3, Popular: 0.1} }

// Engine produces blended recommendations for a user.
type Engine struct {
	repo    repository.Repository
	weights Weights
	historyDepth int // how many recent interactions build the taste profile
	neighborDepth int
}

func New(repo repository.Repository, w Weights, historyDepth, neighborDepth int) *Engine {
	if historyDepth <= 0 {
		historyDepth = 20
	}
	if neighborDepth <= 0 {
		neighborDepth = 10
	}
	return &Engine{repo: repo, weights: w, historyDepth: historyDepth, neighborDepth: neighborDepth}
}

// Recommend returns at most `limit` items the user has not interacted with.
func (e *Engine) Recommend(ctx context.Context, userID string, limit int) ([]domain.Item, error) {
	history, err := e.repo.GetUserHistory(ctx, userID, e.historyDepth)
	if err != nil {
		return nil, err
	}

	seen := make(map[string]bool, len(history))
	for _, h := range history {
		seen[h.ItemID] = true
	}

	scores := map[string]*scoreAccumulator{}

	// Cold start: with no history we skip personalization entirely and lean
	// on ranked popularity, keeping the response consistent for new users.
	if len(history) == 0 {
		trending, err := e.repo.GetTrending(ctx, limit)
		if err != nil {
			return nil, err
		}
		out := make([]domain.Item, 0, limit)
		for i, t := range trending {
			if len(out) >= limit {
				break
			}
			out = append(out, domain.Item{
				ItemID: t.ItemID,
				Score: e.weights.Popular * (1.0 - float64(i)*0.01),
				Reason: "popular right now",
			})
		}
		return out, nil
	}

	// 1) Collaborative filtering: for each recent interaction, pull the
	// item's precomputed neighbors and accumulate similarity weighted by
	// the interaction weight (a purchase counts more than a view).
	for _, h := range history {
		neighbors, err := e.repo.GetNeighbors(ctx, h.ItemID, e.neighborDepth)
		if err != nil {
			return nil, err
		}
		for _, n := range neighbors {
			if seen[n.ItemID] {
				continue
			}
			acc := ensure(scores, n.ItemID)
			acc.add(e.weights.Collaborative * n.Similarity * h.Weight, "similar to items you interacted with")
		}
	}

	// 2) Trending: time-decayed popularity adds serendipity and news.
	trending, err := e.repo.GetTrending(ctx, limit*2)
	if err != nil {
		return nil, err
	}
	maxTrend := 1.0
	if len(trending) > 0 {
		maxTrend = trending[0].Score
	}
	for _, t := range trending {
		if seen[t.ItemID] {
			continue
		}
		normalized := t.Score / maxTrend // normalize to (0, 1]
		acc := ensure(scores, t.ItemID)
		acc.add(e.weights.Trending*normalized, "trending now")
	}

	out := make([]domain.Item, 0, len(scores))
	for itemID, acc := range scores {
		out = append(out, domain.Item{ItemID: itemID, Score: acc.score, Reason: acc.reason})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

type scoreAccumulator struct {
	score  float64
	reason string
}

func (a *scoreAccumulator) add(score float64, reason string) {
	a.score += score
	// Keep the reason of the strongest contribution seen so far.
	if reason != "" && reason != a.reason {
		if a.reason == "" || score > 0 {
			a.reason = reason
		}
	}
}

func ensure(m map[string]*scoreAccumulator, key string) *scoreAccumulator {
	if acc, ok := m[key]; ok {
		return acc
	}
	acc := &scoreAccumulator{}
	m[key] = acc
	return acc
}

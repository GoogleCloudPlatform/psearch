package services

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"psearch/serving-go/internal/models"
)

// LTR feature order: 12 normalized features in [0,1].
var LTRFeatureNames = []string{
	"rrf_score", "vector_score", "text_score",
	"clicks", "orders", "discount_pct", "popularity_score", "margin_pct", "ctr", "conversion_rate", "recency_score",
	"sla_hours",
}

const ltrNumFeatures = 12

// LTRStump is one depth-1 regression tree: adds Left if x[Feature] <= Threshold else Right.
type LTRStump struct {
	Feature   int     `json:"feature"`
	Threshold float64 `json:"threshold"`
	Left      float64 `json:"left"`
	Right     float64 `json:"right"`
}

// LTRModel = bias + linear weights + boosted stumps over normalized features.
// Weights can be learned in BigQuery ML (bigquery_scripts/ltr_train.sql) and
// loaded via LTR_MODEL_JSON; DefaultLTRModel is a hand-tuned baseline.
type LTRModel struct {
	Version string     `json:"version"`
	Bias    float64    `json:"bias"`
	Weights []float64  `json:"weights"` // len 12, LTRFeatureNames order
	Stumps  []LTRStump `json:"stumps"`
}

// DefaultLTRModel is a conservative baseline: relevance dominates, business
// signals and fast delivery break ties.
func DefaultLTRModel() *LTRModel {
	return &LTRModel{
		Version: "baseline-2026-10",
		Weights: []float64{0.55, 0.15, 0.15, 0.02, 0.04, 0.01, 0.02, 0.01, 0.02, 0.02, 0.01, 0.05},
		Stumps: []LTRStump{
			{Feature: 2, Threshold: 0.0, Left: 0, Right: 0.05},   // any exact text match
			{Feature: 11, Threshold: 0.5, Left: -0.03, Right: 0}, // slow delivery penalty (sla feature: 1 = fastest)
			{Feature: 9, Threshold: 0.3, Left: 0, Right: 0.02},   // high conversion
		},
	}
}

// ParseLTRModel validates a JSON model.
func ParseLTRModel(raw string) (*LTRModel, error) {
	var m LTRModel
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return nil, err
	}
	if len(m.Weights) != ltrNumFeatures {
		return nil, fmt.Errorf("ltr model needs %d weights, got %d", ltrNumFeatures, len(m.Weights))
	}
	for _, s := range m.Stumps {
		if s.Feature < 0 || s.Feature >= ltrNumFeatures {
			return nil, fmt.Errorf("stump feature %d out of range", s.Feature)
		}
	}
	if m.Version == "" {
		m.Version = "custom"
	}
	return &m, nil
}

// Score evaluates the model on a feature vector.
func (m *LTRModel) Score(x []float64) float64 {
	s := m.Bias
	for i, w := range m.Weights {
		s += w * x[i]
	}
	for _, st := range m.Stumps {
		if x[st.Feature] <= st.Threshold {
			s += st.Left
		} else {
			s += st.Right
		}
	}
	return s
}

// signalFeatures mirrors the BoostSignalScore normalizations (each in [0,1]).
func signalFeatures(s models.BoostSignals) [8]float64 {
	c, o := float64(s.Clicks), float64(s.Orders)
	return [8]float64{
		c / (c + 100), o / (o + 20), clamp(s.DiscountPct, 0, 100) / 100, clamp(s.PopularityScore, 0, 1),
		clamp(s.MarginPct, 0, 100) / 100, clamp(s.CTR, 0, 1), clamp(s.ConversionRate, 0, 1), clamp(s.RecencyScore, 0, 1),
	}
}

// slaFeature maps delivery SLA to [0,1] (1 = next day, 0 = >= 7 days); 0.5 when unknown.
func slaFeature(p *models.DeliveryPromise) float64 {
	if p == nil || p.SLAHours <= 0 {
		return 0.5
	}
	return clamp(1-float64(p.SLAHours-24)/(24*6), 0, 1)
}

// LTRResult reports stage-2 metadata.
type LTRResult struct {
	LatencyUS    int64
	ModelVersion string
}

// Rerank re-orders stage-1 candidates in place with the LTR model (stage 2).
// rrf/vector/text are min-max normalized within the candidate set. Availability,
// merchandising pins and buries keep precedence over the model score.
func Rerank(m *LTRModel, results []models.SearchResult, signals map[string]models.BoostSignals, pins, buries []string) LTRResult {
	start := time.Now()
	n := len(results)
	if n == 0 {
		return LTRResult{ModelVersion: m.Version}
	}
	maxOf := func(key string) float64 {
		mx := 0.0
		for _, r := range results {
			mx = math.Max(mx, r.Score[key])
		}
		if mx == 0 {
			return 1
		}
		return mx
	}
	mRRF, mVec, mTxt := maxOf("hybrid"), maxOf("vector"), maxOf("text")
	x := make([]float64, ltrNumFeatures)
	for i := range results {
		r := &results[i]
		if r.Score == nil {
			r.Score = map[string]float64{}
		}
		x[0], x[1], x[2] = r.Score["hybrid"]/mRRF, r.Score["vector"]/mVec, r.Score["text"]/mTxt
		sig, ok := signals[r.ID]
		if !ok && r.Signals != nil {
			sig, ok = *r.Signals, true
		}
		f := signalFeatures(sig)
		copy(x[3:11], f[:])
		x[11] = slaFeature(r.DeliveryPromise)
		r.Score["ltr_score"] = m.Score(x)
		r.Score["stage1_rank"] = float64(i + 1)
	}
	sort.SliceStable(results, func(i, j int) bool {
		a, b := results[i], results[j]
		ua, ub := isUnavailable(a.Availability, a.AvailableQuantity), isUnavailable(b.Availability, b.AvailableQuantity)
		if ua != ub {
			return !ua
		}
		if pa, pb := pinRank(pins, a.ID), pinRank(pins, b.ID); pa != pb {
			return pa < pb
		}
		if ba, bb := containsID(buries, a.ID), containsID(buries, b.ID); ba != bb {
			return !ba
		}
		if a.Score["ltr_score"] != b.Score["ltr_score"] {
			return a.Score["ltr_score"] > b.Score["ltr_score"]
		}
		return a.Score["stage1_rank"] < b.Score["stage1_rank"]
	})
	for i := range results {
		results[i].Score["stage2_rank"] = float64(i + 1)
	}
	return LTRResult{LatencyUS: time.Since(start).Microseconds(), ModelVersion: m.Version}
}

// CandidateSignals loads the 8 boost signals for candidates (Spanner) or from
// seed data (memory mode).
func (s *SpannerService) CandidateSignals(ctx context.Context, ids []string) (map[string]models.BoostSignals, error) {
	out := map[string]models.BoostSignals{}
	if len(ids) == 0 {
		return out, nil
	}
	if s.client == nil {
		want := map[string]bool{}
		for _, id := range ids {
			want[id] = true
		}
		for _, p := range s.seedProducts {
			if want[p.ID] && p.Signals != nil {
				out[p.ID] = *p.Signals
			}
		}
		return out, nil
	}
	iter := s.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT product_id, IFNULL(clicks, 0), IFNULL(orders, 0), IFNULL(discount_pct, 0), IFNULL(popularity_score, 0),
				IFNULL(margin_pct, 0), IFNULL(ctr, 0), IFNULL(conversion_rate, 0), IFNULL(recency_score, 0)
			FROM products WHERE product_id IN UNNEST(@ids)`,
		Params: map[string]interface{}{"ids": ids},
	})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		var id string
		var b models.BoostSignals
		if err := row.Columns(&id, &b.Clicks, &b.Orders, &b.DiscountPct, &b.PopularityScore, &b.MarginPct, &b.CTR, &b.ConversionRate, &b.RecencyScore); err != nil {
			return nil, err
		}
		out[id] = b
	}
}

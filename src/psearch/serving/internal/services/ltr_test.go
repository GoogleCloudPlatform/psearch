package services

import (
	"context"
	"strings"
	"testing"

	"psearch/serving-go/internal/models"
)

func TestLTRModelParseAndScore(t *testing.T) {
	m := DefaultLTRModel()
	if len(m.Weights) != len(LTRFeatureNames) || len(LTRFeatureNames) != 12 {
		t.Fatal("model must have 12 features")
	}
	lo, hi := make([]float64, 12), make([]float64, 12)
	for i := range hi {
		hi[i] = 1
	}
	if m.Score(hi) <= m.Score(lo) {
		t.Fatal("all-max features must outscore all-zero features")
	}
	if _, err := ParseLTRModel(`{"weights":[1,2]}`); err == nil {
		t.Fatal("wrong weight count must be rejected")
	}
	if _, err := ParseLTRModel(`{"weights":[0,0,0,0,0,0,0,0,0,0,0,0],"stumps":[{"feature":12}]}`); err == nil {
		t.Fatal("out-of-range stump feature must be rejected")
	}
	pm, err := ParseLTRModel(`{"version":"bqml-1","bias":0.1,"weights":[1,0,0,0,0,0,0,0,0,0,0,0]}`)
	if err != nil || pm.Version != "bqml-1" {
		t.Fatalf("valid model: %+v %v", pm, err)
	}
}

func TestSLAFeature(t *testing.T) {
	if slaFeature(nil) != 0.5 || slaFeature(&models.DeliveryPromise{SLAHours: 24}) != 1 || slaFeature(&models.DeliveryPromise{SLAHours: 24 * 8}) != 0 {
		t.Fatal("sla feature mapping wrong")
	}
}

// ADR-006: stage 2 reorders the stage-1 candidates, reports ranks, and keeps
// availability and merchandising pins in precedence.
func TestRerankTwoStage(t *testing.T) {
	ctx := context.Background()
	svc := &SpannerService{config: testConfig("v1"), seedProducts: DefaultSeedProducts()}
	out, _ := svc.HybridSearchDetailed(ctx, "shoes", 100, 0, 0.5, models.SearchFilters{})
	var ids []string
	for _, r := range out.Results {
		ids = append(ids, r.ID)
	}
	signals, _ := svc.CandidateSignals(ctx, ids)
	// A model that only values orders must reorder by the orders signal.
	m := &LTRModel{Version: "orders-only", Weights: []float64{0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0}}
	res := Rerank(m, out.Results, signals, []string{"prod-014"}, nil)
	if res.ModelVersion != "orders-only" || res.LatencyUS < 0 {
		t.Fatalf("ltr result %+v", res)
	}
	if out.Results[0].ID != "prod-014" {
		t.Fatalf("pinned product must stay first after re-ranking: %v", resultIDs(out.Results))
	}
	seenOut := false
	prevOrders := int64(1 << 62)
	for i, r := range out.Results {
		if r.Score["stage2_rank"] != float64(i+1) || r.Score["stage1_rank"] == 0 {
			t.Fatalf("ranks not reported for %s: %v", r.ID, r.Score)
		}
		unavailable := isUnavailable(r.Availability, r.AvailableQuantity)
		if unavailable {
			seenOut = true
		} else if seenOut {
			t.Fatal("available product after unavailable one")
		}
		if i > 0 && !unavailable && r.ID != "prod-014" {
			o := signals[r.ID].Orders
			if o > prevOrders {
				t.Fatalf("orders-only model not respected at %s", r.ID)
			}
			prevOrders = o
		}
	}
}

func TestRerankLatencyBudget(t *testing.T) {
	results := make([]models.SearchResult, 100)
	for i := range results {
		results[i] = models.SearchResult{ID: strings.Repeat("p", 1) + string(rune('a'+i%26)) + string(rune('a'+i/26)),
			Score: map[string]float64{"hybrid": float64(100-i) / 1000, "vector": 0.01, "text": 0.01}, Signals: &models.BoostSignals{Clicks: int64(i)}}
	}
	res := Rerank(DefaultLTRModel(), results, nil, nil, nil)
	if res.LatencyUS > 2000 {
		t.Fatalf("stage-2 over 100 candidates took %dµs (> 2ms budget)", res.LatencyUS)
	}
}

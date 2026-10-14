package services

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"psearch/serving-go/internal/models"
)

func TestBoostSignalScoreBoundsAndMonotonic(t *testing.T) {
	zero := BoostSignalScore(models.BoostSignals{})
	max := BoostSignalScore(models.BoostSignals{Clicks: 1e9, Orders: 1e9, DiscountPct: 500, PopularityScore: 9, MarginPct: 500, CTR: 9, ConversionRate: 9, RecencyScore: 9})
	if zero != 0 || max > 1.0000001 || max < 0.99 {
		t.Fatalf("score bounds: zero=%v max=%v", zero, max)
	}
	lo := BoostSignalScore(models.BoostSignals{Orders: 5})
	hi := BoostSignalScore(models.BoostSignals{Orders: 50})
	if hi <= lo {
		t.Fatal("more orders must increase the score")
	}
	if !strings.Contains(boostSignalSQL, "p.recency_score") || strings.Count(boostSignalSQL, "0.1") < 6 {
		t.Fatal("SQL formula must cover all 8 signals")
	}
}

func newRuleSvc() *RuleService { return NewRuleService(NewMemoryRuleStore(), time.Minute) }

func TestRuleValidation(t *testing.T) {
	svc, ctx := newRuleSvc(), context.Background()
	bad := []Rule{
		{Name: "", BoostMultiplier: 2, TargetBrand: "nike"},
		{Name: "x", BoostMultiplier: 50, TargetBrand: "nike"},
		{Name: "x", BoostMultiplier: 2},               // boost without target
		{Name: "x", BoostMultiplier: 1},               // no effect
		{Name: "x", PinProductIDs: make([]string, 0)}, // no effect
	}
	for i, r := range bad {
		if _, err := svc.Save(ctx, r); err == nil {
			t.Errorf("case %d should be rejected: %+v", i, r)
		}
	}
	r, err := svc.Save(ctx, Rule{Name: "Nike week", QueryPattern: " Tênis ", TargetBrand: "Nike", BoostMultiplier: 1.5, Active: true})
	if err != nil || r.RuleID == "" || r.QueryPattern != "tenis" || r.TargetBrand != "nike" || r.TenantID != "default" {
		t.Fatalf("save normalization: %+v err=%v", r, err)
	}
}

func TestRuleMatchMergesActiveRules(t *testing.T) {
	svc, ctx := newRuleSvc(), context.Background()
	svc.Save(ctx, Rule{Name: "a", QueryPattern: "shoes", TargetBrand: "nike", BoostMultiplier: 2, Active: true})
	svc.Save(ctx, Rule{Name: "b", QueryPattern: "", PinProductIDs: []string{"prod-004"}, BuryProductIDs: []string{"prod-001"}, Active: true})
	svc.Save(ctx, Rule{Name: "c", QueryPattern: "shoes", TargetCategory: "footwear", BoostMultiplier: 3, Active: false})
	rs := svc.Match(ctx, "default", "Running Shoes")
	if len(rs.Applied) != 2 || len(rs.Boosts) != 1 || rs.Pins[0] != "prod-004" || rs.Buries[0] != "prod-001" {
		t.Fatalf("match = %+v", rs)
	}
	if m := rs.Multiplier("Nike", "Footwear"); m != 2 {
		t.Fatalf("multiplier = %v", m)
	}
	if got := svc.Match(ctx, "default", "laptop"); len(got.Boosts) != 0 || len(got.Pins) != 1 {
		t.Fatalf("non-matching query should only get the catch-all rule: %+v", got)
	}
	if got := svc.Match(ctx, "acme", "shoes"); len(got.Applied) != 0 {
		t.Fatal("rules must be tenant-scoped")
	}
}

// boost multiplier reorders, pins go first, buries go last (within availability).
func TestMemorySearchAppliesRules(t *testing.T) {
	ctx := context.Background()
	cfg := testConfig("v1")
	svc := &SpannerService{config: cfg, seedProducts: DefaultSeedProducts()}
	rules := newRuleSvc()
	svc.SetRules(rules)
	base, _ := svc.HybridSearchDetailed(ctx, "running shoes", 15, 0, 0.5, models.SearchFilters{})
	if base.Results[0].ID != "prod-001" {
		t.Fatalf("baseline top = %s", base.Results[0].ID)
	}
	rules.Save(ctx, Rule{Name: "pin pegasus", QueryPattern: "shoes", PinProductIDs: []string{"prod-003"}, BuryProductIDs: []string{"prod-001"}, Active: true})
	out, _ := svc.HybridSearchDetailed(ctx, "running shoes", 15, 0, 0.5, models.SearchFilters{})
	if out.Results[0].ID != "prod-003" {
		t.Fatalf("pinned prod-003 must be first: %v", resultIDs(out.Results))
	}
	lastAvailable := ""
	for _, r := range out.Results {
		if !isUnavailable(r.Availability, r.AvailableQuantity) {
			lastAvailable = r.ID
		}
	}
	if lastAvailable != "prod-001" {
		t.Fatalf("buried prod-001 must be last among available products: %v", resultIDs(out.Results))
	}
	if len(out.AppliedRules) != 1 {
		t.Fatalf("applied rules = %v", out.AppliedRules)
	}
	// Boost multiplier: final_score = rrf * mult + boost.
	rules.Save(ctx, Rule{Name: "boost apple", TargetBrand: "apple", BoostMultiplier: 5, Active: true})
	out, _ = svc.HybridSearchDetailed(ctx, "laptop", 15, 0, 0.5, models.SearchFilters{})
	for _, r := range out.Results {
		if r.ID == "prod-007" {
			if r.Score["rule_multiplier"] != 5 || math.Abs(r.Score["final_score"]-r.Score["hybrid"]*5-r.Score["boost"]) > 1e-12 {
				t.Fatalf("prod-007 scores = %v", r.Score)
			}
		}
	}
	if out.Results[0].ID != "prod-007" {
		t.Fatalf("5x apple boost should put prod-007 first for 'laptop': %v", resultIDs(out.Results))
	}
}

func TestBoostSignalsReorderNeighbours(t *testing.T) {
	cfg := testConfig("v1")
	cfg.BoostWeight = DefaultBoostWeight
	svc := &SpannerService{config: cfg, seedProducts: DefaultSeedProducts()}
	out, _ := svc.HybridSearchDetailed(context.Background(), "shoes", 15, 0, 0.5, models.SearchFilters{})
	for _, r := range out.Results {
		if r.Signals == nil || r.Score["boost"] <= 0 || r.Score["boost"] > DefaultBoostWeight {
			t.Fatalf("%s boost=%v must be in (0, %v]", r.ID, r.Score["boost"], DefaultBoostWeight)
		}
	}
}

func TestBuildHybridSQL_MerchandisingTerms(t *testing.T) {
	sql := buildHybridSQLWith(vectorColumnV2, false, true)
	for _, frag := range []string{"@boost_weight", "UNNEST(@rule_boosts)", "@pin_ids", "@bury_ids", "JOIN products AS p", "rrf_score * rule_mult + boost_score DESC", "boost_score, rule_mult"} {
		if !strings.Contains(sql, frag) {
			t.Errorf("SQL missing %q", frag)
		}
	}
}

func TestRuleMatchClampsUnvalidatedMultipliers(t *testing.T) {
	store := NewMemoryRuleStore()
	_ = store.Upsert(context.Background(), Rule{TenantID: "default", RuleID: "raw", Name: "raw", TargetBrand: "nike", BoostMultiplier: -3, Active: true})
	rs := NewRuleService(store, time.Minute).Match(context.Background(), "default", "shoes")
	if len(rs.Boosts) != 1 || rs.Boosts[0].Multiplier != 0.1 {
		t.Fatalf("raw multiplier -3 must be clamped to 0.1: %+v", rs.Boosts)
	}
}

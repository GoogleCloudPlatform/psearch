package services

import (
	"context"
	"testing"

	"psearch/serving-go/internal/models"
)

func TestSessionRecordValidation(t *testing.T) {
	s := NewSessionService(NewMemorySessionStore(), &SpannerService{config: testConfig("v1"), seedProducts: DefaultSeedProducts()}, 0)
	ctx := context.Background()
	for _, bad := range []SessionEvent{
		{SessionID: "short", ProductID: "prod-001", EventType: "click"},
		{SessionID: "sess-abcdef12", ProductID: "prod-001", EventType: "view"},
		{SessionID: "sess-abcdef12", ProductID: "", EventType: "click"},
	} {
		if _, err := s.Record(ctx, bad); err == nil {
			t.Errorf("Record(%+v) should fail", bad)
		}
	}
	e, err := s.Record(ctx, SessionEvent{SessionID: "sess-abcdef12", ProductID: "prod-001", EventType: "PURCHASE"})
	if err != nil || e.EventID == "" || e.EventType != "purchase" || e.TenantID != "default" {
		t.Fatalf("Record = %+v, %v", e, err)
	}
	if ev, _ := s.Recent(ctx, "default", "sess-abcdef12"); len(ev) != 1 {
		t.Fatal("event not stored")
	}
}

func TestCentroidWeightsAndDecay(t *testing.T) {
	vec := map[string][]float64{"a": {1, 0}, "b": {0, 1}}
	// newest first: purchase of b outweighs older click of a
	c := Centroid([]SessionEvent{{ProductID: "b", EventType: "purchase"}, {ProductID: "a", EventType: "click"}}, vec)
	if c == nil || c[1] <= c[0] {
		t.Fatalf("centroid should lean to the purchased product: %v", c)
	}
	if Centroid([]SessionEvent{{ProductID: "zzz", EventType: "click"}}, vec) != nil {
		t.Fatal("no vectors => nil centroid")
	}
}

// ADR-007 acceptance: two sessions with different histories get different rankings
// for the same broad query, with session_boost > 0 on aligned products.
func TestPersonalizeDiffersBySession(t *testing.T) {
	ctx := context.Background()
	svc := &SpannerService{config: testConfig("v1"), seedProducts: DefaultSeedProducts()}
	s := NewSessionService(NewMemorySessionStore(), svc, 0.01)
	run := func(history string) []models.SearchResult {
		out, _ := svc.HybridSearchDetailed(ctx, "shoes", 15, 0, 0.5, models.SearchFilters{})
		ok := s.Personalize(ctx, "default", []SessionEvent{{ProductID: history, EventType: "purchase"}}, out.Results, nil, nil, "", false)
		if !ok {
			t.Fatal("personalization should apply")
		}
		return out.Results
	}
	a := run("prod-003") // Nike trail runner
	b := run("prod-005") // laptop
	if resultIDs(a)[0] == resultIDs(b)[0] && resultIDs(a)[1] == resultIDs(b)[1] {
		t.Fatalf("rankings should differ by session: A=%v B=%v", resultIDs(a), resultIDs(b))
	}
	boosted := false
	for _, r := range a {
		boosted = boosted || r.Score["session_boost"] > 0
	}
	if !boosted {
		t.Fatal("expected session_boost > 0 for aligned products")
	}
	// availability still dominant
	seenOut := false
	for _, r := range b {
		if isUnavailable(r.Availability, r.AvailableQuantity) {
			seenOut = true
		} else if seenOut {
			t.Fatal("unavailable products must stay last after personalization")
		}
	}
}

package services

import (
	"context"
	"strings"
	"testing"

	"psearch/serving-go/internal/models"
)

func TestValidatePricing(t *testing.T) {
	for _, ok := range [][2]string{{"", ""}, {"b2b-corporate", "price_asc"}, {"vip-club", "relevance"}} {
		if err := ValidatePricing(ok[0], ok[1]); err != nil {
			t.Errorf("ValidatePricing(%v) = %v", ok, err)
		}
	}
	for _, bad := range [][2]string{{"B2B Corp", ""}, {"x'; DROP", ""}, {"", "cheapest"}} {
		if err := ValidatePricing(bad[0], bad[1]); err == nil {
			t.Errorf("ValidatePricing(%v) should fail", bad)
		}
	}
}

func TestDefaultPriceRows(t *testing.T) {
	rows := DefaultPriceRows(DefaultSeedProducts())
	if len(rows) != 3*15 {
		t.Fatalf("rows = %d, want 45", len(rows))
	}
	m := newMemoryPricing(rows)
	if got := m.effective("b2b-corporate", "prod-001", 189.90); got != 170.91 {
		t.Fatalf("b2b-corporate prod-001 = %v, want 170.91 (10%% off)", got)
	}
	if got := m.effective("", "prod-001", 189.90); got != 189.90 {
		t.Fatal("no table => list price")
	}
	if got := m.effective("unknown", "prod-001", 189.90); got != 189.90 {
		t.Fatal("unknown table => list price")
	}
}

func TestExclusiveAssortment(t *testing.T) {
	m := newMemoryPricing([]PriceRow{{ProductID: "p1", PriceTableID: "vip-club", Price: 1, Exclusive: true}})
	if m.visible("", "p1") || m.visible("b2b-gold", "p1") || !m.visible("vip-club", "p1") || !m.visible("", "p2") {
		t.Fatal("exclusive products are visible only to buyers on their table")
	}
}

// effective prices, price sort and price facets.
func TestMemoryContractPricing(t *testing.T) {
	ctx := context.Background()
	svc := &SpannerService{config: testConfig("v1"), seedProducts: DefaultSeedProducts()}
	out, _ := svc.HybridSearchDetailed(ctx, "laptop", 15, 0, 0.5, models.SearchFilters{PriceTable: "b2b-gold", SortBy: "price_asc"})
	prev := -1.0
	for _, r := range out.Results {
		if isUnavailable(r.Availability, r.AvailableQuantity) {
			continue
		}
		if r.PriceInfo.NumericPrice < prev {
			t.Fatalf("price_asc violated at %s: %v < %v", r.ID, r.PriceInfo.NumericPrice, prev)
		}
		prev = r.PriceInfo.NumericPrice
		if r.PriceInfo.PriceTableID != "b2b-gold" || r.PriceInfo.BasePrice <= r.PriceInfo.NumericPrice {
			t.Fatalf("%s must carry contract price below base: %+v", r.ID, r.PriceInfo)
		}
	}
	// prod-007 MacBook list 1399 -> b2b-gold Computers 18% off = 1147.18 (below the 1200 cap)
	capped, _ := svc.HybridSearchDetailed(ctx, "macbook", 15, 0, 0.5, models.SearchFilters{PriceTable: "b2b-gold", MaxPrice: 1200})
	found := false
	for _, r := range capped.Results {
		found = found || r.ID == "prod-007"
	}
	if !found {
		t.Fatal("max_price must apply to the effective contract price (prod-007 1147.18 <= 1200)")
	}
	f, _ := svc.Facets(ctx, []string{"prod-006"}, "b2b-gold", "default") // 1650 list -> 1353 contract
	if len(f.PriceRanges) != 1 || f.PriceRanges[0].Value != "500-1500" {
		t.Fatalf("price facet must use the contract price: %+v", f.PriceRanges)
	}
}

func TestPricingSQL(t *testing.T) {
	sql := buildHybridSQLWith(vectorColumnV2, false, true)
	for _, frag := range []string{"FROM product_prices AS pp", "@price_table", "is_exclusive", "effective_price", "IF(@sort_by = 'price_asc', effective_price, NULL) ASC"} {
		if !strings.Contains(sql, frag) {
			t.Errorf("SQL missing %q", frag)
		}
	}
	if !strings.HasPrefix(optimizerHint, "@{OPTIMIZER_VERSION=") {
		t.Error("hybrid query must carry the optimizer-version statement hint (SCORE needs v6+)")
	}
	if !strings.Contains(facetPriceSQL, "product_prices") {
		t.Error("price facet must use the effective price")
	}
}

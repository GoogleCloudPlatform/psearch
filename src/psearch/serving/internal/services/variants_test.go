package services

import (
	"cloud.google.com/go/spanner"
	"context"
	"strings"
	"testing"

	"psearch/serving-go/internal/models"
)

func TestDefaultVariants(t *testing.T) {
	vs := DefaultVariants(DefaultSeedProducts())
	per := map[string]int{}
	for _, v := range vs {
		per[v.ProductID]++
		if (v.ProductID == "prod-004" || v.ProductID == "prod-012") && v.Stock > 0 {
			t.Fatalf("unavailable product %s must have no variant stock", v.ProductID)
		}
	}
	if per["prod-001"] != 18 { // footwear: 3 colors x 6 sizes
		t.Fatalf("prod-001 variants = %d, want 18", per["prod-001"])
	}
	if per["prod-005"] != 2 || per["prod-008"] != 2 {
		t.Fatalf("tech/audio variants: %v", per)
	}
	if per["prod-011"] != 0 {
		t.Fatal("appliances have no variants")
	}
	std := false
	for _, v := range vs {
		std = std || v.SKUID == "prod-001-std"
	}
	if !std {
		t.Fatal("the existing -std SKU must be the first variant (keeps regional_inventory valid)")
	}
}

// one card per parent, child color/size filters via EXISTS
// semantics, aggregated variants on the parent.
func TestVariantCollapseAndFilters(t *testing.T) {
	ctx := context.Background()
	svc := &SpannerService{config: testConfig("v1"), seedProducts: DefaultSeedProducts()}
	out, _ := svc.HybridSearchDetailed(ctx, "shoes", 100, 0, 0.5, models.SearchFilters{})
	seen := map[string]bool{}
	for _, r := range out.Results {
		if seen[r.ID] {
			t.Fatalf("parent %s returned twice (variants must collapse)", r.ID)
		}
		seen[r.ID] = true
		if r.ID == "prod-001" && (r.VariantCount != 18 || len(r.AvailableColors) == 0 || len(r.AvailableSizes) == 0) {
			t.Fatalf("prod-001 aggregates: count=%d colors=%v sizes=%v", r.VariantCount, r.AvailableColors, r.AvailableSizes)
		}
	}
	blue, _ := svc.HybridSearchDetailed(ctx, "shoes", 100, 0, 0.5, models.SearchFilters{Color: "Azul", Size: "42"})
	if len(blue.Results) == 0 {
		t.Fatal("expected footwear with an in-stock blue size 42")
	}
	for _, r := range blue.Results {
		if r.Categories[0] != "Footwear" {
			t.Fatalf("color/size filter leaked non-footwear %s", r.ID)
		}
		inStock := false
		for _, v := range r.Variants {
			if v.Color != "azul" || v.Size != "42" {
				t.Fatalf("%s variants must be restricted to azul/42: %+v", r.ID, v)
			}
			inStock = inStock || v.Available
		}
		if !inStock {
			t.Fatalf("%s returned without an in-stock matching variant", r.ID)
		}
	}
	none, _ := svc.HybridSearchDetailed(ctx, "shoes", 100, 0, 0.5, models.SearchFilters{Color: "roxo"})
	if len(none.Results) != 0 {
		t.Fatal("no product has a purple variant")
	}
}

func TestVariantFacets(t *testing.T) {
	svc := &SpannerService{config: testConfig("v1"), seedProducts: DefaultSeedProducts()}
	f, err := svc.Facets(context.Background(), []string{"prod-001", "prod-002", "prod-005"}, "", "")
	if err != nil || len(f.Colors) == 0 || len(f.Sizes) == 0 {
		t.Fatalf("variant facets: %+v %v", f, err)
	}
	for _, b := range f.Colors {
		if b.Count > 3 {
			t.Fatalf("each parent counts once per color: %+v", b)
		}
	}
}

func TestVariantSQL(t *testing.T) {
	sql := buildHybridSQLWith(vectorColumnV2, false, true)
	for _, frag := range []string{"FROM skus AS vc", "FROM skus AS vs", "@color", "@size", "ARRAY(SELECT AS STRUCT s.sku_id AS sku_id", "effective_price, variants"} {
		if !strings.Contains(sql, frag) {
			t.Errorf("hybrid SQL missing %q", frag)
		}
	}
	if !strings.Contains(browseSQL(), "s.product_id = products.product_id") {
		t.Error("browse SQL must aggregate variants")
	}
}

// decode a real Spanner ARRAY<STRUCT> column into the
// type used by spannerHybridSearch/spannerBrowse (memory-mode tests never decode rows).
func TestVariantRowsDecodeFromSpannerArrayOfStruct(t *testing.T) {
	in := []*variantRow{{SKUID: "prod-001-std", Color: "azul", Size: "42", Stock: 3, Available: true}, nil}
	row, err := spanner.NewRow([]string{"variants"}, []interface{}{in})
	if err != nil {
		t.Fatalf("NewRow: %v", err)
	}
	var got []*variantRow
	if err := row.Columns(&got); err != nil {
		t.Fatalf("decode ARRAY<STRUCT> into []*variantRow: %v", err)
	}
	vs := toVariants(got)
	if len(vs) != 1 || vs[0].SKUID != "prod-001-std" || vs[0].Color != "azul" || !vs[0].Available {
		t.Fatalf("decoded variants = %+v", vs)
	}
	var wrong []variantRow
	if err := row.Columns(&wrong); err == nil {
		t.Log("note: this client version also decodes into []variantRow")
	}
}

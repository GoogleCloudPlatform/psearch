package services

import (
	"context"
	"strings"
	"testing"

	"psearch/serving-go/internal/models"
)

func TestNormalizeCEP(t *testing.T) {
	for in, want := range map[string]string{"01310-100": "01310", "01310100": "01310", " 20040.002 ": "20040", "": ""} {
		if got, err := NormalizeCEP(in); err != nil || got != want {
			t.Errorf("NormalizeCEP(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"0131", "01310-10a", "123456789", "abcde-fgh"} {
		if _, err := NormalizeCEP(bad); err == nil {
			t.Errorf("NormalizeCEP(%q) should fail", bad)
		}
	}
}

func TestPromiseLabel(t *testing.T) {
	if l := PromiseLabel(24, "cd-sp-cajamar"); l != "Chega amanhã - CD São Paulo" {
		t.Errorf("label = %q", l)
	}
	if l := PromiseLabel(96, "cd-pe-recife"); !strings.Contains(l, "4 dias") {
		t.Errorf("label = %q", l)
	}
}

func TestDefaultInventoryRules(t *testing.T) {
	inv := DefaultInventory(DefaultSeedProducts())
	if len(inv) == 0 {
		t.Fatal("empty inventory")
	}
	for _, r := range inv {
		if (r.ProductID == "prod-004" || r.ProductID == "prod-012") && r.Stock > 0 {
			t.Fatalf("unavailable product %s must have no stock", r.ProductID)
		}
		if r.SKUID != r.ProductID+"-std" || r.ZipStart > r.ZipEnd {
			t.Fatalf("bad row %+v", r)
		}
	}
}

// CEP filter + max SLA + fastest promise per product.
func TestMemoryDeliveryFilterAndPromises(t *testing.T) {
	ctx := context.Background()
	svc := &SpannerService{config: testConfig("v1"), seedProducts: DefaultSeedProducts()}
	all, _ := svc.HybridSearchDetailed(ctx, "shoes", 15, 0, 0.5, models.SearchFilters{})
	sp, _ := svc.HybridSearchDetailed(ctx, "shoes", 15, 0, 0.5, models.SearchFilters{Zip5: "01310"})
	fast, _ := svc.HybridSearchDetailed(ctx, "shoes", 15, 0, 0.5, models.SearchFilters{Zip5: "01310", MaxSLAHours: 24})
	if len(sp.Results) == 0 || len(sp.Results) >= len(all.Results) || len(fast.Results) > len(sp.Results) {
		t.Fatalf("filter sizes all=%d sp=%d sp24=%d", len(all.Results), len(sp.Results), len(fast.Results))
	}
	var ids []string
	for _, r := range fast.Results {
		ids = append(ids, r.ID)
		if isUnavailable(r.Availability, r.AvailableQuantity) {
			t.Fatalf("out-of-stock %s cannot be deliverable", r.ID)
		}
	}
	promises, err := svc.DeliveryPromises(ctx, ids, "01310", 24)
	if err != nil || len(promises) != len(ids) {
		t.Fatalf("promises = %d for %d ids (err %v)", len(promises), len(ids), err)
	}
	for id, p := range promises {
		if p.SLAHours > 24 || p.RegionCode != "SP" || p.Label == "" {
			t.Fatalf("%s promise %+v", id, p)
		}
	}
}

func TestDeliverySQL(t *testing.T) {
	sql := buildHybridSQLWith(vectorColumnV2, false, true)
	if strings.Count(sql, "FROM regional_inventory AS ri") != 2 || !strings.Contains(sql, "@zip5 BETWEEN ri.zip_prefix_start AND ri.zip_prefix_end") {
		t.Fatal("delivery EXISTS filter must be in both legs")
	}
	if !strings.Contains(deliveryPromiseSQL, "JOIN skus AS s") || !strings.Contains(deliveryPromiseSQL, "ORDER BY ri.product_id, ri.sla_hours") {
		t.Fatal("promise SQL malformed")
	}
}

func TestFreeShippingZeroesCost(t *testing.T) {
	p := toPromise(InventoryRow{FreeShipping: true, ShippingCost: 15, SLAHours: 24, CDID: "cd-sp-cajamar"})
	if p.ShippingCost != 0 || !p.FreeShipping {
		t.Fatalf("free shipping must report cost 0: %+v", p)
	}
}

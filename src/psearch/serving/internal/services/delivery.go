package services

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"psearch/serving-go/internal/models"
)

// InventoryRow mirrors regional_inventory joined with skus (migration 017, ADR-005).
type InventoryRow struct {
	ProductID, SKUID, SellerID, RegionCode, ZipStart, ZipEnd, CDID string
	Stock, SLAHours                                                int64
	ShippingCost                                                   float64
	FreeShipping                                                   bool
}

// Region is a CEP range served by a distribution center.
type Region struct {
	Code, ZipStart, ZipEnd, CDID, CDName string
	BaseSLA                              int64
}

// Regions are the demo CEP ranges (5-digit prefixes) and their CDs.
var Regions = []Region{
	{"SP", "01000", "19999", "cd-sp-cajamar", "CD São Paulo", 24},
	{"RJ", "20000", "28999", "cd-rj-duque", "CD Rio de Janeiro", 48},
	{"MG", "30000", "39999", "cd-mg-contagem", "CD Minas Gerais", 48},
	{"NE", "40000", "65999", "cd-pe-recife", "CD Nordeste", 96},
	{"SUL", "80000", "99999", "cd-pr-curitiba", "CD Sul", 72},
}

// NormalizeCEP returns the 5-digit prefix of a Brazilian CEP ("01310-100" -> "01310").
// Empty input returns "" (no delivery filter); malformed input returns an error.
func NormalizeCEP(cep string) (string, error) {
	digits := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		if r == '-' || r == '.' || r == ' ' {
			return -1
		}
		return 'x'
	}, strings.TrimSpace(cep))
	if digits == "" {
		return "", nil
	}
	if len(digits) != 8 || strings.ContainsRune(digits, 'x') {
		return "", fmt.Errorf("invalid cep %q: expected 8 digits (e.g. 01310-100)", cep)
	}
	return digits[:5], nil
}

// DefaultInventory builds deterministic demo inventory for the seed catalog
// (mirrored by migration 017b): one SKU per product; unavailable products have
// no stock anywhere; others are stocked in a product-dependent subset of regions.
func DefaultInventory(products []models.SearchResult) []InventoryRow {
	var rows []InventoryRow
	for i, p := range products {
		seller := "seller-psearch"
		if i%3 == 2 {
			seller = "seller-marketplace-" + strings.ToLower(firstOr(p.Brands, "partner"))
		}
		for j, r := range Regions {
			if (i+j)%4 == 3 { // not every product is stocked in every region
				continue
			}
			stock := int64(0)
			if !isUnavailable(p.Availability, p.AvailableQuantity) {
				stock = int64(5 + (i*7+j*3)%40)
			}
			sla := r.BaseSLA
			if seller != "seller-psearch" {
				sla += 24 // marketplace sellers ship from their own warehouse
			}
			rows = append(rows, InventoryRow{
				ProductID: p.ID, SKUID: p.ID + "-std", SellerID: seller, RegionCode: r.Code,
				ZipStart: r.ZipStart, ZipEnd: r.ZipEnd, CDID: r.CDID, Stock: stock, SLAHours: sla,
				ShippingCost: float64(10 + (i+j)%5*5), FreeShipping: p.PriceInfo.NumericPrice >= 199,
			})
		}
	}
	return rows
}

func firstOr(v []string, d string) string {
	if len(v) > 0 {
		return v[0]
	}
	return d
}

func cdName(cdID string) string {
	for _, r := range Regions {
		if r.CDID == cdID {
			return r.CDName
		}
	}
	return cdID
}

// PromiseLabel renders a human SLA label (pt-BR, as in the issue: "Chega amanhã - CD São Paulo").
func PromiseLabel(sla int64, cdID string) string {
	switch {
	case sla <= 24:
		return "Chega amanhã - " + cdName(cdID)
	case sla <= 48:
		return "Chega em 2 dias - " + cdName(cdID)
	default:
		return fmt.Sprintf("Chega em até %d dias - %s", (sla+23)/24, cdName(cdID))
	}
}

func toPromise(r InventoryRow) *models.DeliveryPromise {
	if r.FreeShipping {
		r.ShippingCost = 0 // the buyer pays nothing when shipping is free
	}
	return &models.DeliveryPromise{SLAHours: r.SLAHours, CDID: r.CDID, SellerID: r.SellerID, RegionCode: r.RegionCode, CEP: "",
		FreeShipping: r.FreeShipping, ShippingCost: r.ShippingCost, Label: PromiseLabel(r.SLAHours, r.CDID)}
}

// deliveryFilterSQL restricts both RRF legs to products deliverable to @zip5
// within @max_sla (0 = any SLA) via a co-located EXISTS on the interleaved
// regional_inventory table. Disabled when @zip5 = ”.
const deliveryFilterSQL = `(@zip5 = '' OR EXISTS (
						SELECT 1 FROM regional_inventory AS ri
						WHERE ri.product_id = products.product_id
							AND @zip5 BETWEEN ri.zip_prefix_start AND ri.zip_prefix_end
							AND ri.stock > 0
							AND (@max_sla = 0 OR ri.sla_hours <= @max_sla)))`

const deliveryPromiseSQL = `SELECT ri.product_id, ri.sku_id, IFNULL(s.seller_id, ''), ri.region_code, IFNULL(ri.cd_id, ''),
		ri.stock, IFNULL(ri.sla_hours, 0), IFNULL(ri.shipping_cost, 0), IFNULL(ri.free_shipping, FALSE)
	FROM regional_inventory AS ri
	JOIN skus AS s ON s.product_id = ri.product_id AND s.sku_id = ri.sku_id
	WHERE ri.product_id IN UNNEST(@ids)
		AND @zip5 BETWEEN ri.zip_prefix_start AND ri.zip_prefix_end
		AND ri.stock > 0
		AND (@max_sla = 0 OR ri.sla_hours <= @max_sla)
	ORDER BY ri.product_id, ri.sla_hours, ri.shipping_cost`

// DeliveryPromises returns the fastest in-stock promise per product for zip5.
func (s *SpannerService) DeliveryPromises(ctx context.Context, ids []string, zip5 string, maxSLA int64) (map[string]*models.DeliveryPromise, error) {
	out := map[string]*models.DeliveryPromise{}
	if zip5 == "" || len(ids) == 0 {
		return out, nil
	}
	var rows []InventoryRow
	if s.client == nil {
		want := map[string]bool{}
		for _, id := range ids {
			want[id] = true
		}
		for _, r := range DefaultInventory(s.seedProducts) {
			if want[r.ProductID] && zip5 >= r.ZipStart && zip5 <= r.ZipEnd && r.Stock > 0 && (maxSLA == 0 || r.SLAHours <= maxSLA) {
				rows = append(rows, r)
			}
		}
		sort.SliceStable(rows, func(i, j int) bool {
			if rows[i].ProductID != rows[j].ProductID {
				return rows[i].ProductID < rows[j].ProductID
			}
			if rows[i].SLAHours != rows[j].SLAHours {
				return rows[i].SLAHours < rows[j].SLAHours
			}
			return rows[i].ShippingCost < rows[j].ShippingCost
		})
	} else {
		iter := s.client.Single().Query(ctx, spanner.Statement{SQL: deliveryPromiseSQL,
			Params: map[string]interface{}{"ids": ids, "zip5": zip5, "max_sla": maxSLA}})
		defer iter.Stop()
		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				return nil, err
			}
			var r InventoryRow
			if err := row.Columns(&r.ProductID, &r.SKUID, &r.SellerID, &r.RegionCode, &r.CDID, &r.Stock, &r.SLAHours, &r.ShippingCost, &r.FreeShipping); err != nil {
				return nil, err
			}
			rows = append(rows, r)
		}
	}
	for _, r := range rows {
		if _, ok := out[r.ProductID]; !ok { // rows are ordered fastest first
			out[r.ProductID] = toPromise(r)
		}
	}
	return out, nil
}

// deliverableInMemory reports whether a seed product can be delivered to zip5 within maxSLA.
func (s *SpannerService) deliverableInMemory(productID, zip5 string, maxSLA int64) bool {
	if zip5 == "" {
		return true
	}
	s.invOnce.Do(func() { s.inventory = DefaultInventory(s.seedProducts) })
	for _, r := range s.inventory {
		if r.ProductID == productID && zip5 >= r.ZipStart && zip5 <= r.ZipEnd && r.Stock > 0 && (maxSLA == 0 || r.SLAHours <= maxSLA) {
			return true
		}
	}
	return false
}

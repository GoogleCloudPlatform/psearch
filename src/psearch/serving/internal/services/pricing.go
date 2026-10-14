package services

import (
	"fmt"
	"regexp"
	"strings"

	"psearch/serving-go/internal/models"
)

// B2B contract pricing (ADR-008, Issue #22). product_prices is interleaved in
// products (migration 019), so these correlated lookups are split-local.

// effectivePriceSQL resolves the buyer's price for products alias `alias`.
func effectivePriceSQL(alias string) string {
	return `COALESCE((SELECT pp.price FROM product_prices AS pp
					WHERE pp.product_id = ` + alias + `.product_id AND pp.price_table_id = @price_table
						AND pp.tenant_id = @tenant_id), ` + alias + `.price)`
}

// assortmentSQL hides products that have exclusive contract rows unless the buyer
// is on one of those price tables.
const assortmentSQL = `(NOT EXISTS (SELECT 1 FROM product_prices AS x
						WHERE x.product_id = products.product_id AND IFNULL(x.is_exclusive, FALSE))
					OR EXISTS (SELECT 1 FROM product_prices AS y
						WHERE y.product_id = products.product_id AND IFNULL(y.is_exclusive, FALSE)
							AND y.price_table_id = @price_table AND y.tenant_id = @tenant_id))`

var priceTableRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,63}$`)

// ValidatePricing checks price_table_id and sort_by request values.
func ValidatePricing(priceTable, sortBy string) error {
	if priceTable != "" && !priceTableRe.MatchString(priceTable) {
		return fmt.Errorf("invalid price_table_id %q (lowercase letters, digits, '-', max 64)", priceTable)
	}
	switch sortBy {
	case "", "relevance", "price_asc", "price_desc":
		return nil
	}
	return fmt.Errorf("invalid sort_by %q (relevance | price_asc | price_desc)", sortBy)
}

// PriceRow mirrors product_prices.
type PriceRow struct {
	ProductID, PriceTableID, TenantID, Currency string
	Price                                       float64
	MinQuantity                                 int64
	Exclusive                                   bool
}

// PriceTables are the demo contract tables (mirrored by migration 019b).
var PriceTables = []struct {
	ID       string
	Name     string
	Discount func(category string) float64
}{
	{"b2b-corporate", "B2B Corporate", func(string) float64 { return 0.10 }},
	{"b2b-gold", "B2B Gold", func(c string) float64 {
		if c == "Computers" || c == "Appliances" {
			return 0.18
		}
		return 0.08
	}},
	{"vip-club", "VIP Club", func(c string) float64 {
		if c == "Audio" || c == "TV & Video" {
			return 0.25
		}
		return 0.05
	}},
}

func round2(v float64) float64 { return float64(int64(v*100+0.5)) / 100 }

// DefaultPriceRows builds deterministic contract prices for the seed catalog.
func DefaultPriceRows(products []models.SearchResult) []PriceRow {
	var rows []PriceRow
	for _, t := range PriceTables {
		for _, p := range products {
			cat := firstOr(p.Categories, "")
			rows = append(rows, PriceRow{ProductID: p.ID, PriceTableID: t.ID, TenantID: "default", Currency: "USD",
				Price: round2(p.PriceInfo.NumericPrice * (1 - t.Discount(cat))), MinQuantity: 1})
		}
	}
	return rows
}

// memoryPricing indexes price rows for memory mode.
type memoryPricing struct {
	price     map[string]map[string]float64 // table -> product -> price
	exclusive map[string]map[string]bool    // product -> tables with exclusive rows
}

func newMemoryPricing(rows []PriceRow) *memoryPricing {
	m := &memoryPricing{price: map[string]map[string]float64{}, exclusive: map[string]map[string]bool{}}
	for _, r := range rows {
		if m.price[r.PriceTableID] == nil {
			m.price[r.PriceTableID] = map[string]float64{}
		}
		m.price[r.PriceTableID][r.ProductID] = r.Price
		if r.Exclusive {
			if m.exclusive[r.ProductID] == nil {
				m.exclusive[r.ProductID] = map[string]bool{}
			}
			m.exclusive[r.ProductID][r.PriceTableID] = true
		}
	}
	return m
}

func (m *memoryPricing) effective(table, productID string, base float64) float64 {
	if p, ok := m.price[table][productID]; ok && table != "" {
		return p
	}
	return base
}

func (m *memoryPricing) visible(table, productID string) bool {
	ex := m.exclusive[productID]
	return len(ex) == 0 || ex[table]
}

// applyEffectivePrice rewrites priceInfo with the buyer's effective price,
// keeping the list price as originalPrice/basePrice.
func applyEffectivePrice(r *models.SearchResult, effective float64, table string) {
	base := r.PriceInfo.NumericPrice
	if r.PriceInfo.BasePrice == 0 {
		r.PriceInfo.BasePrice = base
	}
	if table == "" || effective == r.PriceInfo.BasePrice {
		return
	}
	r.PriceInfo.NumericPrice = effective
	r.PriceInfo.Price = fmt.Sprintf("%.2f", effective)
	r.PriceInfo.OriginalPrice = fmt.Sprintf("%.2f", r.PriceInfo.BasePrice)
	r.PriceInfo.PriceTableID = table
}

func priceSortKey(sortBy string) string { return strings.TrimSpace(sortBy) }

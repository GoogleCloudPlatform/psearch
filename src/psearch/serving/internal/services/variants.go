package services

import (
	"context"
	"sort"
	"strings"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"psearch/serving-go/internal/models"
)

// Variant grouping / collapse (ADR-009, Issue #32): results stay one card per
// parent product_id; child SKUs (interleaved in products) carry color/size/stock.

// variantFilterSQL keeps a parent only if an in-stock child SKU matches the
// requested color / size (EXISTS on the interleaved skus table).
const variantFilterSQL = `(@color = '' OR EXISTS (SELECT 1 FROM skus AS vc
						WHERE vc.product_id = products.product_id AND LOWER(IFNULL(vc.color, '')) = @color
							AND IFNULL(vc.is_available, TRUE) AND IFNULL(vc.stock, 0) > 0))
					AND (@size = '' OR EXISTS (SELECT 1 FROM skus AS vs
						WHERE vs.product_id = products.product_id AND LOWER(IFNULL(vs.size, '')) = @size
							AND IFNULL(vs.is_available, TRUE) AND IFNULL(vs.stock, 0) > 0))`

// variantsArraySQL aggregates a parent's variants (restricted to the requested
// color/size) in the same statement; alias is the parent product alias.
func variantsArraySQL(alias string) string {
	return `ARRAY(SELECT AS STRUCT s.sku_id AS sku_id, IFNULL(s.color, '') AS color, IFNULL(s.size, '') AS size,
					IFNULL(s.stock, 0) AS stock, (IFNULL(s.is_available, TRUE) AND IFNULL(s.stock, 0) > 0) AS available
				FROM skus AS s
				WHERE s.product_id = ` + alias + `.product_id AND (s.color IS NOT NULL OR s.size IS NOT NULL)
					AND (@color = '' OR LOWER(IFNULL(s.color, '')) = @color)
					AND (@size = '' OR LOWER(IFNULL(s.size, '')) = @size)
				ORDER BY s.color, s.size, s.sku_id)`
}

// variantRow decodes ARRAY<STRUCT<...>> rows.
type variantRow struct {
	SKUID     string `spanner:"sku_id"`
	Color     string `spanner:"color"`
	Size      string `spanner:"size"`
	Stock     int64  `spanner:"stock"`
	Available bool   `spanner:"available"`
}

// toVariants converts decoded ARRAY<STRUCT> rows. The Spanner client only decodes
// ARRAY<STRUCT> into a slice of struct POINTERS (rollout finding on #32).
func toVariants(rows []*variantRow) []models.Variant {
	out := make([]models.Variant, 0, len(rows))
	for _, r := range rows {
		if r == nil {
			continue
		}
		out = append(out, models.Variant{SKUID: r.SKUID, Color: r.Color, Size: r.Size, Stock: r.Stock, Available: r.Available})
	}
	return out
}

// attachVariants sets variants and their aggregates on a result.
func attachVariants(r *models.SearchResult, vs []models.Variant) {
	if len(vs) == 0 {
		return
	}
	r.Variants = vs
	r.VariantCount = len(vs)
	colors, sizes := map[string]bool{}, map[string]bool{}
	for _, v := range vs {
		if !v.Available {
			continue
		}
		if v.Color != "" {
			colors[v.Color] = true
		}
		if v.Size != "" {
			sizes[v.Size] = true
		}
	}
	r.AvailableColors, r.AvailableSizes = sortedKeys(colors), sortedKeys(sizes)
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// VariantSeed is one seeded SKU variant (mirrored by migration 022b).
type VariantSeed struct {
	ProductID, SKUID, Color, Size string
	Stock                         int64
}

var (
	footwearColors = []string{"preto", "branco", "azul"}
	footwearSizes  = []string{"38", "39", "40", "41", "42", "43"}
	techColors     = []string{"prata", "grafite"}
	audioColors    = []string{"preto", "branco"}
)

// DefaultVariants builds deterministic variants: the existing "<id>-std" SKU
// becomes the first variant; footwear gets colors x sizes, computers/phones and
// audio get colors. Unavailable products have no stock in any variant.
func DefaultVariants(products []models.SearchResult) []VariantSeed {
	var out []VariantSeed
	for i, p := range products {
		cat := firstOr(p.Categories, "")
		var combos [][2]string
		switch cat {
		case "Footwear":
			for _, c := range footwearColors {
				for _, s := range footwearSizes {
					combos = append(combos, [2]string{c, s})
				}
			}
		case "Computers", "Smartphones":
			for _, c := range techColors {
				combos = append(combos, [2]string{c, ""})
			}
		case "Audio":
			for _, c := range audioColors {
				combos = append(combos, [2]string{c, ""})
			}
		default:
			continue
		}
		unavailable := isUnavailable(p.Availability, p.AvailableQuantity)
		for j, cs := range combos {
			sku := p.ID + "-std"
			if j > 0 {
				sku = p.ID + "-" + cs[0]
				if cs[1] != "" {
					sku += "-" + cs[1]
				}
			}
			stock := int64((i*7 + j*5) % 9) // some variants out of stock
			if unavailable {
				stock = 0
			}
			out = append(out, VariantSeed{ProductID: p.ID, SKUID: sku, Color: cs[0], Size: cs[1], Stock: stock})
		}
	}
	return out
}

// memoryVariants indexes seed variants by product (memory mode).
func (s *SpannerService) memoryVariants() map[string][]models.Variant {
	s.varOnce.Do(func() {
		s.variants = map[string][]models.Variant{}
		for _, v := range DefaultVariants(s.seedProducts) {
			s.variants[v.ProductID] = append(s.variants[v.ProductID], models.Variant{SKUID: v.SKUID, Color: v.Color, Size: v.Size, Stock: v.Stock, Available: v.Stock > 0})
		}
		for id := range s.variants {
			vs := s.variants[id]
			sort.Slice(vs, func(i, j int) bool {
				if vs[i].Color != vs[j].Color {
					return vs[i].Color < vs[j].Color
				}
				if vs[i].Size != vs[j].Size {
					return vs[i].Size < vs[j].Size
				}
				return vs[i].SKUID < vs[j].SKUID
			})
		}
	})
	return s.variants
}

// memoryVariantMatch applies the color/size filters to a seed product's variants and
// returns the matching variants (nil, false when the product must be excluded).
func (s *SpannerService) memoryVariantMatch(productID string, f models.SearchFilters) ([]models.Variant, bool) {
	all := s.memoryVariants()[productID]
	if f.Color == "" && f.Size == "" {
		return all, true
	}
	var out []models.Variant
	inStockMatch := false
	for _, v := range all {
		if (f.Color == "" || strings.EqualFold(v.Color, f.Color)) && (f.Size == "" || strings.EqualFold(v.Size, f.Size)) {
			out = append(out, v)
			inStockMatch = inStockMatch || v.Available
		}
	}
	return out, inStockMatch
}

// variantFacets counts in-stock colors and sizes across the candidate parents
// (each parent counted once per value).
func (s *SpannerService) variantFacets(ctx context.Context, ids []string) (colors, sizes []models.FacetBucket, err error) {
	cc, sc := map[string]int64{}, map[string]int64{}
	if s.client == nil {
		mv := s.memoryVariants()
		for _, id := range ids {
			seenC, seenS := map[string]bool{}, map[string]bool{}
			for _, v := range mv[id] {
				if !v.Available {
					continue
				}
				if v.Color != "" && !seenC[v.Color] {
					seenC[v.Color] = true
					cc[v.Color]++
				}
				if v.Size != "" && !seenS[v.Size] {
					seenS[v.Size] = true
					sc[v.Size]++
				}
			}
		}
		return buckets(cc), buckets(sc), nil
	}
	for _, q := range []struct {
		col string
		m   map[string]int64
	}{{"color", cc}, {"size", sc}} {
		iter := s.client.Single().Query(ctx, spanner.Statement{
			SQL: `SELECT LOWER(s.` + q.col + `) AS value, COUNT(DISTINCT s.product_id) AS count FROM skus AS s
				WHERE s.product_id IN UNNEST(@ids) AND s.` + q.col + ` IS NOT NULL AND s.` + q.col + ` != ''
					AND IFNULL(s.is_available, TRUE) AND IFNULL(s.stock, 0) > 0
				GROUP BY value`,
			Params: map[string]interface{}{"ids": ids},
		})
		for {
			row, ierr := iter.Next()
			if ierr == iterator.Done {
				break
			}
			if ierr != nil {
				iter.Stop()
				return nil, nil, ierr
			}
			var v string
			var c int64
			if row.Columns(&v, &c) == nil {
				q.m[v] = c
			}
		}
		iter.Stop()
	}
	return buckets(cc), buckets(sc), nil
}

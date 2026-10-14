package services

import (
	"context"
	"sort"

	"cloud.google.com/go/spanner"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
	"psearch/serving-go/internal/models"
)

// Facet SQL expressions (/ ). Price ranges follow .
const (
	facetCategorySQL     = `IFNULL(category, '')`
	facetBrandSQL        = `IFNULL(brand, '')`
	facetPriceSQL        = `CASE WHEN price IS NULL OR price < 100 THEN '0-100' WHEN price < 500 THEN '100-500' WHEN price < 1500 THEN '500-1500' ELSE '1500+' END`
	facetAvailabilitySQL = `IF(IFNULL(is_available, TRUE) AND IFNULL(stock, 1) > 0, 'IN_STOCK', 'OUT_OF_STOCK')`
)

// Facets aggregates categorical, price-range and availability counts over the
// candidate product IDs. In Spanner mode the four GROUP BY queries run
// concurrently (errgroup); in memory mode the same buckets are computed in Go.
func (s *SpannerService) Facets(ctx context.Context, ids []string) (*models.Facets, error) {
	f := &models.Facets{
		Categories: []models.FacetBucket{}, Brands: []models.FacetBucket{},
		PriceRanges: []models.FacetBucket{}, Availability: []models.FacetBucket{},
	}
	if len(ids) == 0 {
		return f, nil
	}
	if s.client == nil {
		return s.memoryFacets(ids), nil
	}
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() (err error) { f.Categories, err = s.queryFacet(gctx, facetCategorySQL, ids); return })
	g.Go(func() (err error) { f.Brands, err = s.queryFacet(gctx, facetBrandSQL, ids); return })
	g.Go(func() (err error) { f.PriceRanges, err = s.queryFacet(gctx, facetPriceSQL, ids); return })
	g.Go(func() (err error) { f.Availability, err = s.queryFacet(gctx, facetAvailabilitySQL, ids); return })
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return f, nil
}

// facetSQL renders a GROUP BY facet query; expr is always a package constant.
func facetSQL(expr string) string {
	return `SELECT ` + expr + ` AS value, COUNT(*) AS count
		FROM products
		WHERE product_id IN UNNEST(@ids)
		GROUP BY value
		ORDER BY count DESC, value`
}

func (s *SpannerService) queryFacet(ctx context.Context, expr string, ids []string) ([]models.FacetBucket, error) {
	iter := s.client.Single().Query(ctx, spanner.Statement{SQL: facetSQL(expr), Params: map[string]interface{}{"ids": ids}})
	defer iter.Stop()
	out := []models.FacetBucket{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		var value spanner.NullString
		var count int64
		if err := row.Columns(&value, &count); err != nil {
			return nil, err
		}
		if value.StringVal == "" {
			continue
		}
		out = append(out, models.FacetBucket{Value: value.StringVal, Count: count})
	}
}

func (s *SpannerService) memoryFacets(ids []string) *models.Facets {
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	cats, brands, prices, avail := map[string]int64{}, map[string]int64{}, map[string]int64{}, map[string]int64{}
	for _, p := range s.seedProducts {
		if !want[p.ID] {
			continue
		}
		if len(p.Categories) > 0 {
			cats[p.Categories[0]]++
		}
		if len(p.Brands) > 0 {
			brands[p.Brands[0]]++
		}
		prices[PriceBucket(p.PriceInfo.NumericPrice)]++
		if isUnavailable(p.Availability, p.AvailableQuantity) {
			avail["OUT_OF_STOCK"]++
		} else {
			avail["IN_STOCK"]++
		}
	}
	return &models.Facets{Categories: buckets(cats), Brands: buckets(brands), PriceRanges: buckets(prices), Availability: buckets(avail)}
}

// PriceBucket maps a price to its range.
func PriceBucket(price float64) string {
	switch {
	case price < 100:
		return "0-100"
	case price < 500:
		return "100-500"
	case price < 1500:
		return "500-1500"
	default:
		return "1500+"
	}
}

// buckets sorts like the SQL: count DESC, value ASC.
func buckets(m map[string]int64) []models.FacetBucket {
	out := make([]models.FacetBucket, 0, len(m))
	for v, c := range m {
		out = append(out, models.FacetBucket{Value: v, Count: c})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count != out[j].Count {
			return out[i].Count > out[j].Count
		}
		return out[i].Value < out[j].Value
	})
	return out
}

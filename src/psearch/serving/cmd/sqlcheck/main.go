// Command sqlcheck executes the production hybrid-search SQL against Cloud
// Spanner (read-only) for a set of queries/filters and exits non-zero on any
// SQL error. Run by @deployer before each rollout that changes search SQL.
package main

import (
	"context"
	"log"
	"os"

	"psearch/serving-go/internal/config"
	"psearch/serving-go/internal/models"
	"psearch/serving-go/internal/services"
)

func main() {
	ctx := context.Background()
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	var embedder services.QueryEmbedder
	if emb, err := services.NewEmbeddingService(ctx, cfg); err == nil {
		embedder = emb
	} else {
		log.Printf("WARN: embedding service unavailable: %v", err)
	}
	svc, err := services.NewSpannerService(ctx, cfg, embedder)
	if err != nil {
		log.Fatalf("spanner: %v", err)
	}
	defer svc.Close()

	cases := []struct {
		q       string
		alpha   float64
		filters models.SearchFilters
	}{
		{"running shoes", 0, models.SearchFilters{}},
		{"running shoes", 1, models.SearchFilters{}},
		{"shoes", 0.5, models.SearchFilters{}},
		{"shoes", 0.5, models.SearchFilters{OnlyInStock: true}},
		{"shoes", 0.5, models.SearchFilters{Channel: "store"}},
		{"tenis", 0.5, models.SearchFilters{Channel: "mobile", OnlyInStock: true}},
	}
	failed := false
	for _, c := range cases {
		n, mode, err := svc.CheckSpannerQuery(ctx, c.q, c.alpha, c.filters)
		if err != nil {
			failed = true
			log.Printf("FAIL q=%q alpha=%.1f filters=%+v: %v", c.q, c.alpha, c.filters, err)
			continue
		}
		log.Printf("OK   q=%q alpha=%.1f filters=%+v rows=%d mode=%s", c.q, c.alpha, c.filters, n, mode)
	}
	if failed {
		os.Exit(1)
	}
}

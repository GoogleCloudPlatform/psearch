// Command sqlcheck executes the production hybrid-search SQL against Cloud
// Spanner (read-only) for a set of queries/filters and exits non-zero on any
// SQL error. Run before each rollout that changes search SQL.
package main

import (
	"context"
	"log"
	"os"
	"time"

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
	syn := services.NewSynonymService(services.SpannerSynonymStore{Client: svc.Client()}, time.Minute)
	svc.SetSynonyms(syn)
	if rows, err := syn.List(ctx, "default", ""); err != nil {
		log.Fatalf("FAIL synonyms table: %v", err)
	} else {
		log.Printf("OK   search_synonyms rows=%d expand(geladeira,pt-BR)=%v", len(rows), syn.Expand(ctx, "default", "pt-BR", "geladeira").Terms)
	}

	// Merchandising: verify the rules table is readable, then exercise the SQL with
	// non-empty rule params using an in-memory rule (nothing is written to Spanner).
	if rules, err := (services.SpannerRuleStore{Client: svc.Client()}).List(ctx, "default"); err != nil {
		log.Fatalf("FAIL merchandising_rules table: %v", err)
	} else {
		log.Printf("OK   merchandising_rules rows=%d", len(rules))
	}
	memRules := services.NewRuleService(services.NewMemoryRuleStore(), time.Minute)
	if _, err := memRules.Save(ctx, services.Rule{Name: "sqlcheck", QueryPattern: "shoes", TargetBrand: "nike", BoostMultiplier: 1.5,
		PinProductIDs: []string{"prod-003"}, BuryProductIDs: []string{"prod-001"}, Active: true}); err != nil {
		log.Fatalf("rule: %v", err)
	}
	svc.SetRules(memRules)

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
		{"tenis", 0, models.SearchFilters{Locale: "pt-BR"}},
		{"notebook", 0.5, models.SearchFilters{Category: "Computers", Brand: "Acer", TenantID: "default"}},
		{"shoes", 0.5, models.SearchFilters{TenantID: "acme"}},
		{"geladeira", 0, models.SearchFilters{Locale: "pt-BR"}},
		{"notebook gamer", 0.5, models.SearchFilters{}},
		{"running shoes", 0.5, models.SearchFilters{}}, // rule params non-empty (pin/bury/boost)
		{"shoes", 0.5, models.SearchFilters{Zip5: "01310", MaxSLAHours: 24}},
		{"notebook", 0.5, models.SearchFilters{Zip5: "20040"}},
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
	// Delivery promises: interleaved regional_inventory JOIN skus.
	if p, err := svc.DeliveryPromises(ctx, []string{"prod-001", "prod-002", "prod-003", "prod-004"}, "01310", 0); err != nil {
		failed = true
		log.Printf("FAIL delivery promises: %v", err)
	} else {
		log.Printf("OK   delivery promises for 01310: %d products", len(p))
	}
	// Autocomplete: n-gram and 1-char prefix paths.
	ac := services.NewAutocompleteService(svc.Client(), nil, time.Second)
	for _, q := range []string{"gel", "tên", "n"} {
		if out, _, err := ac.Suggest(ctx, "default", "", q, 5); err != nil {
			failed = true
			log.Printf("FAIL autocomplete %q: %v", q, err)
		} else {
			log.Printf("OK   autocomplete %q -> %d suggestions", q, len(out))
		}
	}
	// LTR stage-2 inputs: boost signals for candidates.
	if sig, err := svc.CandidateSignals(ctx, []string{"prod-001", "prod-002", "prod-003"}); err != nil {
		failed = true
		log.Printf("FAIL ltr candidate signals: %v", err)
	} else {
		log.Printf("OK   ltr candidate signals: %d products", len(sig))
	}
	// Facet queries over the full catalog.
	if f, err := svc.Facets(ctx, []string{"prod-001", "prod-002", "prod-004", "prod-012"}); err != nil {
		failed = true
		log.Printf("FAIL facets: %v", err)
	} else {
		log.Printf("OK   facets categories=%v price_ranges=%v availability=%v", f.Categories, f.PriceRanges, f.Availability)
	}
	if failed {
		os.Exit(1)
	}
}

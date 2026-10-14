package services

import (
	"context"
	"strings"
	"testing"
	"time"

	"psearch/serving-go/internal/models"
)

func TestSynonymExpand(t *testing.T) {
	svc := NewSynonymService(NewMemorySynonymStore(), time.Minute)
	e := svc.Expand(context.Background(), "default", "pt-BR", "Tênis  de corrida")
	if e.Weight != DefaultSynonymWeight || !contains(e.Terms, "sneaker") || !contains(e.Terms, "sapato esportivo") {
		t.Fatalf("expansion = %+v", e)
	}
	if q := e.SpannerQuery(); !strings.Contains(q, `"sapato esportivo"`) || !strings.Contains(q, " OR ") {
		t.Fatalf("SpannerQuery = %s", q)
	}
	if got := svc.Expand(context.Background(), "acme", "pt-BR", "tenis"); len(got.Terms) != 0 {
		t.Fatal("other tenants must not inherit default synonyms")
	}
	var nilSvc *SynonymService
	if got := nilSvc.Expand(context.Background(), "default", "", "tenis"); len(got.Terms) != 0 {
		t.Fatal("nil service must not expand")
	}
}

func TestSynonymUpsertValidation(t *testing.T) {
	svc := NewSynonymService(NewMemorySynonymStore(), time.Minute)
	ctx := context.Background()
	if _, err := svc.Upsert(ctx, Synonym{Locale: "pt-BR", Term: "bota", Synonyms: []string{"coturno"}, Weight: 1.0}); err == nil {
		t.Fatal("weight >= 1 must be rejected (exact must always win)")
	}
	if _, err := svc.Upsert(ctx, Synonym{Locale: "", Term: "bota", Synonyms: []string{"coturno"}}); err == nil {
		t.Fatal("locale is required")
	}
	saved, err := svc.Upsert(ctx, Synonym{Locale: "pt-BR", Term: "Bóta", Synonyms: []string{"Coturno", "bota"}})
	if err != nil || saved.Term != "bota" || saved.Weight != DefaultSynonymWeight || len(saved.Synonyms) != 1 {
		t.Fatalf("upsert normalization: %+v err=%v", saved, err)
	}
	if e := svc.Expand(ctx, "default", "pt-BR", "bota"); !contains(e.Terms, "coturno") {
		t.Fatal("upsert must invalidate the cache and take effect immediately")
	}
}

// exact matches always outrank synonym-only matches in the text leg.
func TestMemoryExactBeatsSynonym(t *testing.T) {
	svc := &SpannerService{config: testConfig("v1"), seedProducts: DefaultSeedProducts()}
	svc.SetSynonyms(NewSynonymService(NewMemorySynonymStore(), time.Minute))
	out, err := svc.HybridSearchDetailed(context.Background(), "geladeira", 15, 0, 0, models.SearchFilters{})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Results) < 2 || out.Results[0].ID != "prod-011" {
		t.Fatalf("exact 'Geladeira' (prod-011) must rank first at alpha=0: %v", resultIDs(out.Results))
	}
	if !contains(out.ExpandedTerms, "refrigerator") {
		t.Fatalf("expanded terms = %v", out.ExpandedTerms)
	}
	// prod-012 'Refrigerator' matches only via synonym (and is OUT_OF_STOCK, so it is
	// ordered last by the availability rule); it must still carry a reduced text score
	// strictly below the exact match.
	var exact, syn float64
	for _, r := range out.Results {
		switch r.ID {
		case "prod-011":
			exact = r.Score["text"]
		case "prod-012":
			syn = r.Score["text"]
		}
	}
	if syn == 0 || syn >= exact {
		t.Fatalf("synonym text score %v must be > 0 and < exact %v", syn, exact)
	}
}

func TestBuildHybridSQL_SynonymLeg(t *testing.T) {
	sql := buildHybridSQLWith(vectorColumnV2, false, true)
	for _, frag := range []string{"SEARCH(title_tokens_folded, @syn_text)", "@syn_weight * (SCORE(", "exact_first"} {
		if !strings.Contains(sql, frag) {
			t.Errorf("SQL missing %q", frag)
		}
	}
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

func resultIDs(rs []models.SearchResult) []string {
	out := []string{}
	for _, r := range rs {
		out = append(out, r.ID)
	}
	return out
}

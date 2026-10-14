package services

import (
	"context"
	"strings"
	"testing"
	"time"
)

func newAC() *AutocompleteService {
	return NewAutocompleteService(nil, DefaultSeedProducts(), time.Minute)
}

func TestAutocompletePrefixFirstAndAccentInsensitive(t *testing.T) {
	ac := newAC()
	out, _, err := ac.Suggest(context.Background(), "default", "", "Tên", 8)
	if err != nil || len(out) == 0 || !strings.HasPrefix(FoldAccents(out[0].Text), "ten") {
		t.Fatalf("'Tên' should suggest a 'ten…' prefix match first: %+v (err %v)", out, err)
	}
	sub, _, _ := ac.Suggest(context.Background(), "default", "", "corrida", 8)
	if len(sub) == 0 || !strings.Contains(sub[0].Text, "corrida") {
		t.Fatalf("substring match expected: %+v", sub)
	}
}

func TestAutocompleteFiltersAndLimits(t *testing.T) {
	ac := newAC()
	ctx := context.Background()
	if out, _, _ := ac.Suggest(ctx, "default", "pt-BR", "re", 20); len(out) > 0 {
		for _, s := range out {
			if s.Locale != "pt-BR" {
				t.Fatalf("locale filter leaked %+v", s)
			}
		}
	}
	if out, _, _ := ac.Suggest(ctx, "acme", "", "ten", 8); len(out) != 0 {
		t.Fatal("other tenants have no suggestions")
	}
	if out, _, _ := ac.Suggest(ctx, "default", "", "e", 3); len(out) > 3 {
		t.Fatalf("limit not applied: %d", len(out))
	}
	if out, _, _ := ac.Suggest(ctx, "default", "", "   ", 8); len(out) != 0 {
		t.Fatal("blank query returns nothing")
	}
}

func TestAutocompleteCache(t *testing.T) {
	ac := newAC()
	ctx := context.Background()
	if _, cached, _ := ac.Suggest(ctx, "default", "", "note", 8); cached {
		t.Fatal("first call cannot be cached")
	}
	if _, cached, _ := ac.Suggest(ctx, "default", "", "NOTE ", 8); !cached {
		t.Fatal("normalized repeat must hit the cache")
	}
}

func TestAutocompleteSQL(t *testing.T) {
	if !strings.Contains(autocompleteSQL, "SEARCH_NGRAMS(suggestion_ngrams, @q)") || !strings.Contains(autocompleteSQL, "ctr_score DESC, search_count DESC") {
		t.Fatal("autocomplete SQL must use n-grams and rank by CTR then volume")
	}
	if strings.Contains(autocompleteSQL, "OR STARTS_WITH") {
		t.Fatal("SEARCH_NGRAMS cannot be OR-ed with STARTS_WITH (unsupported query shape)")
	}
	if strings.Contains(autocompletePrefixSQL, "SEARCH_NGRAMS") {
		t.Fatal("1-char prefix SQL must not use n-grams")
	}
}

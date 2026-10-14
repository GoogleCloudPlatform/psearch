package services

import (
	"context"
	"strings"
	"testing"
)

func newSpell() *SpellCheckService {
	s := NewSpellCheckService(nil)
	s.AddVocabulary(DefaultSeedProducts(), DefaultSuggestions(DefaultSeedProducts()))
	return s
}

func TestDamerau(t *testing.T) {
	for _, c := range []struct {
		a, b string
		d    int
	}{{"sansung", "samsung", 1}, {"notbook", "notebook", 1}, {"geladera", "geladeira", 1}, {"teh", "the", 1}, {"abc", "abc", 0}, {"kitten", "sitting", 3}} {
		if got := damerau(c.a, c.b); got != c.d {
			t.Errorf("damerau(%q,%q)=%d want %d", c.a, c.b, got, c.d)
		}
	}
}

func TestCorrect(t *testing.T) {
	s := newSpell()
	ctx := context.Background()
	for in, want := range map[string]string{"sansung": "samsung", "notbook": "notebook", "geladera": "geladeira", "sansung galaxy": "samsung galaxy"} {
		if got, changed := s.Correct(ctx, "default", in); !changed || got != want {
			t.Errorf("Correct(%q) = %q, %v; want %q", in, got, changed, want)
		}
	}
	if got, changed := s.Correct(ctx, "default", "notebook"); changed || got != "notebook" {
		t.Error("known words must not be changed")
	}
	if _, changed := s.Correct(ctx, "default", "xqzwv"); changed {
		t.Error("no candidate within distance => unchanged")
	}
	if _, changed := s.Correct(ctx, "default", "4070"); changed {
		t.Error("numeric tokens are never corrected")
	}
}

func TestRelaxedQuery(t *testing.T) {
	if got := RelaxedQuery(`samsung "galaxy" OR geladeira`); got != "samsung OR galaxy OR geladeira" {
		t.Errorf("RelaxedQuery = %q", got)
	}
	sql := buildHybridSQLWith(vectorColumnV2, false, true)
	if !strings.Contains(sql, "SEARCH(title_tokens_folded, @query_text)") {
		t.Fatal("relaxation reuses the bound @query_text param")
	}
}

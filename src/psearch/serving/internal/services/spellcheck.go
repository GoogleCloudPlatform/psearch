package services

import (
	"context"
	"sort"
	"strings"
	"sync"
	"unicode/utf8"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"psearch/serving-go/internal/models"
)

// SpellCheckService corrects query typos (ADR-008, Issue #21): candidates come from
// an in-memory catalog vocabulary plus Spanner SEARCH_NGRAMS over the autocomplete
// n-gram index; ranking is Damerau-Levenshtein distance, then popularity.
type SpellCheckService struct {
	client *spanner.Client // optional
	mu     sync.RWMutex
	vocab  map[string]int64 // folded token -> popularity
}

func NewSpellCheckService(client *spanner.Client) *SpellCheckService {
	return &SpellCheckService{client: client, vocab: map[string]int64{}}
}

// AddVocabulary indexes words from product titles/brands/categories (popularity
// from clicks when known) and suggestion texts.
func (s *SpellCheckService) AddVocabulary(products []models.SearchResult, suggestions []Suggestion) {
	s.mu.Lock()
	defer s.mu.Unlock()
	add := func(text string, pop int64) {
		for _, w := range strings.Fields(FoldAccents(strings.ToLower(text))) {
			w = strings.Trim(w, `.,;:!?()"'`)
			if utf8.RuneCountInString(w) >= 3 && !strings.ContainsAny(w, "0123456789") {
				s.vocab[w] += pop
			}
		}
	}
	for _, p := range products {
		pop := int64(10)
		if p.Signals != nil {
			pop = p.Signals.Clicks
		}
		add(p.Title, pop)
		add(strings.Join(p.Brands, " "), pop)
		add(strings.Join(p.Categories, " "), pop)
	}
	for _, sg := range suggestions {
		add(sg.Text, sg.SearchCount)
	}
}

// damerau computes the optimal-string-alignment edit distance.
func damerau(a, b string) int {
	ra, rb := []rune(a), []rune(b)
	d := make([][]int, len(ra)+1)
	for i := range d {
		d[i] = make([]int, len(rb)+1)
		d[i][0] = i
	}
	for j := range d[0] {
		d[0][j] = j
	}
	for i := 1; i <= len(ra); i++ {
		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			d[i][j] = min(d[i-1][j]+1, d[i][j-1]+1, d[i-1][j-1]+cost)
			if i > 1 && j > 1 && ra[i-1] == rb[j-2] && ra[i-2] == rb[j-1] {
				d[i][j] = min(d[i][j], d[i-2][j-2]+1)
			}
		}
	}
	return d[len(ra)][len(rb)]
}

func maxDistance(token string) int {
	if utf8.RuneCountInString(token) <= 4 {
		return 1
	}
	return 2
}

// ngramCandidates fetches words from the autocomplete n-gram index (Spanner).
// SEARCH_NGRAMS must be the only search predicate (see #9).
func (s *SpellCheckService) ngramCandidates(ctx context.Context, tenant, token string) map[string]int64 {
	out := map[string]int64{}
	if s.client == nil || utf8.RuneCountInString(token) < 3 {
		return out
	}
	iter := s.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT IFNULL(suggestion_folded, ''), IFNULL(search_count, 0) FROM autocomplete_suggestions
			WHERE tenant_id = @tenant AND SEARCH_NGRAMS(suggestion_ngrams, @tok)
			ORDER BY search_count DESC LIMIT 50`,
		Params: map[string]interface{}{"tenant": tenant, "tok": token},
	})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done || err != nil {
			return out
		}
		var text string
		var count int64
		if row.Columns(&text, &count) == nil {
			for _, w := range strings.Fields(text) {
				out[w] += count
			}
		}
	}
}

// Correct returns the corrected query and whether any token changed. Only tokens
// unknown to the vocabulary are corrected, and only with an unambiguous best
// candidate (lowest distance, ties broken by popularity, distance 2 needs len >= 6).
func (s *SpellCheckService) Correct(ctx context.Context, tenant, query string) (string, bool) {
	tokens := strings.Fields(FoldAccents(strings.ToLower(query)))
	changed := false
	for i, tok := range tokens {
		s.mu.RLock()
		_, known := s.vocab[tok]
		cands := make(map[string]int64, len(s.vocab))
		if !known {
			for w, p := range s.vocab {
				cands[w] = p
			}
		}
		s.mu.RUnlock()
		if known || utf8.RuneCountInString(tok) < 3 || strings.ContainsAny(tok, "0123456789") {
			continue
		}
		for w, p := range s.ngramCandidates(ctx, tenant, tok) {
			cands[w] += p
		}
		type c struct {
			w    string
			dist int
			pop  int64
		}
		var best []c
		limit := maxDistance(tok)
		for w, p := range cands {
			if dd := damerau(tok, w); dd > 0 && dd <= limit {
				best = append(best, c{w, dd, p})
			}
		}
		if len(best) == 0 {
			continue
		}
		sort.Slice(best, func(a, b int) bool {
			if best[a].dist != best[b].dist {
				return best[a].dist < best[b].dist
			}
			if best[a].pop != best[b].pop {
				return best[a].pop > best[b].pop
			}
			return best[a].w < best[b].w
		})
		top := best[0]
		if top.dist == 2 && utf8.RuneCountInString(tok) < 6 {
			continue
		}
		if len(best) > 1 && best[1].dist == top.dist && best[1].pop == top.pop {
			continue // ambiguous
		}
		tokens[i] = top.w
		changed = true
	}
	return strings.Join(tokens, " "), changed
}

// RelaxedQuery turns an implicit-AND query into Spanner SEARCH OR syntax.
func RelaxedQuery(query string) string {
	tokens := strings.Fields(query)
	clean := tokens[:0]
	for _, t := range tokens {
		t = strings.Trim(t, `"()`)
		if t != "" && !strings.EqualFold(t, "or") && !strings.EqualFold(t, "and") {
			clean = append(clean, t)
		}
	}
	return strings.Join(clean, " OR ")
}

// LexicalHits counts results with a non-zero text score.
func LexicalHits(results []models.SearchResult) int {
	n := 0
	for _, r := range results {
		if r.Score["text"] > 0 {
			n++
		}
	}
	return n
}

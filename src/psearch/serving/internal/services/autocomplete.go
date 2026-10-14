package services

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"psearch/serving-go/internal/models"
)

// Suggestion is one autocomplete entry (ADR-006, Issue #9).
type Suggestion struct {
	Text        string  `json:"text"`
	SearchCount int64   `json:"search_count"`
	CTRScore    float64 `json:"ctr_score"`
	Locale      string  `json:"locale,omitempty"`
}

// autocompleteSQL uses the accent-folded n-gram TOKENLIST (migration 018) for
// substring matching and prefers prefix matches, then CTR, then volume.
// SEARCH_NGRAMS must not be OR-ed with non-indexed predicates (Spanner rejects that
// query shape); with ngram_size_min=2 it already matches every >=2-char prefix.
const autocompleteSQL = `SELECT suggestion_text, IFNULL(search_count, 0), IFNULL(ctr_score, 0), locale
	FROM autocomplete_suggestions
	WHERE tenant_id = @tenant AND (@locale = '' OR locale = @locale)
		AND SEARCH_NGRAMS(suggestion_ngrams, @q)
	ORDER BY IF(STARTS_WITH(suggestion_folded, @q), 0, 1), ctr_score DESC, search_count DESC, suggestion_text
	LIMIT @limit`

// autocompletePrefixSQL serves 1-character prefixes (below the 2-gram minimum).
const autocompletePrefixSQL = `SELECT suggestion_text, IFNULL(search_count, 0), IFNULL(ctr_score, 0), locale
	FROM autocomplete_suggestions
	WHERE tenant_id = @tenant AND (@locale = '' OR locale = @locale) AND STARTS_WITH(suggestion_folded, @q)
	ORDER BY ctr_score DESC, search_count DESC, suggestion_text
	LIMIT @limit`

// AutocompleteService answers typeahead queries with a short TTL cache.
type AutocompleteService struct {
	client *spanner.Client // nil = memory mode
	memory []models.SearchResult
	ttl    time.Duration

	mu    sync.Mutex
	cache map[string]acEntry
}

type acEntry struct {
	out     []Suggestion
	expires time.Time
}

const acMaxCache = 5000

func NewAutocompleteService(client *spanner.Client, seed []models.SearchResult, ttl time.Duration) *AutocompleteService {
	return &AutocompleteService{client: client, memory: seed, ttl: ttl, cache: map[string]acEntry{}}
}

// Suggest returns up to limit suggestions for prefix q.
func (a *AutocompleteService) Suggest(ctx context.Context, tenantID, locale, q string, limit int) ([]Suggestion, bool, error) {
	fq := FoldAccents(NormalizeQuery(q))
	if fq == "" {
		return []Suggestion{}, false, nil
	}
	if tenantID == "" {
		tenantID = "default"
	}
	key := tenantID + "|" + locale + "|" + fq + "|" + strconv.Itoa(limit)
	a.mu.Lock()
	if e, ok := a.cache[key]; ok && time.Now().Before(e.expires) {
		a.mu.Unlock()
		return e.out, true, nil
	}
	a.mu.Unlock()

	var out []Suggestion
	var err error
	if a.client == nil {
		out = a.memorySuggest(tenantID, locale, fq, limit)
	} else {
		out, err = a.spannerSuggest(ctx, tenantID, locale, fq, limit)
		if err != nil {
			return nil, false, err
		}
	}
	a.mu.Lock()
	if len(a.cache) >= acMaxCache {
		a.cache = map[string]acEntry{} // simple bounded reset
	}
	a.cache[key] = acEntry{out: out, expires: time.Now().Add(a.ttl)}
	a.mu.Unlock()
	return out, false, nil
}

func (a *AutocompleteService) spannerSuggest(ctx context.Context, tenantID, locale, fq string, limit int) ([]Suggestion, error) {
	sql := autocompleteSQL
	if utf8.RuneCountInString(fq) < 2 {
		sql = autocompletePrefixSQL
	}
	iter := a.client.Single().Query(ctx, spanner.Statement{SQL: sql, Params: map[string]interface{}{
		"tenant": tenantID, "locale": locale, "q": fq, "limit": int64(limit)}})
	defer iter.Stop()
	out := []Suggestion{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		var s Suggestion
		if err := row.Columns(&s.Text, &s.SearchCount, &s.CTRScore, &s.Locale); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
}

// DefaultSuggestions derives catalog-based suggestions (titles, categories, brands
// and synonym terms) with deterministic popularity, mirrored by migration 018b.
// They bootstrap autocomplete until the BigQuery analytics export populates real
// query volume/CTR.
func DefaultSuggestions(products []models.SearchResult) []Suggestion {
	agg := map[string]*Suggestion{}
	add := func(text, locale string, count int64, ctr float64) {
		text = strings.ToLower(strings.TrimSpace(text))
		if text == "" {
			return
		}
		k := locale + "|" + text
		if s, ok := agg[k]; ok {
			s.SearchCount += count
			return
		}
		agg[k] = &Suggestion{Text: text, SearchCount: count, CTRScore: ctr, Locale: locale}
	}
	for _, p := range products {
		sig := p.Signals
		count, ctr := int64(100), 0.05
		if sig != nil {
			count, ctr = sig.Clicks, sig.CTR
		}
		add(p.Title, p.Locale, count, ctr)
		add(firstOr(p.Categories, ""), p.Locale, count/2, ctr/2)
		add(firstOr(p.Brands, ""), p.Locale, count/3, ctr/2)
	}
	for _, syn := range DefaultSynonyms() {
		add(syn.Term, syn.Locale, 300, 0.12)
	}
	out := make([]Suggestion, 0, len(agg))
	for _, s := range agg {
		out = append(out, *s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Locale+out[i].Text < out[j].Locale+out[j].Text })
	return out
}

func (a *AutocompleteService) memorySuggest(tenantID, locale, fq string, limit int) []Suggestion {
	if tenantID != "default" {
		return []Suggestion{}
	}
	type cand struct {
		s      Suggestion
		prefix bool
	}
	var cs []cand
	for _, s := range DefaultSuggestions(a.memory) {
		if locale != "" && s.Locale != locale {
			continue
		}
		folded := FoldAccents(s.Text)
		if strings.Contains(folded, fq) {
			cs = append(cs, cand{s, strings.HasPrefix(folded, fq)})
		}
	}
	sort.Slice(cs, func(i, j int) bool {
		if cs[i].prefix != cs[j].prefix {
			return cs[i].prefix
		}
		if cs[i].s.CTRScore != cs[j].s.CTRScore {
			return cs[i].s.CTRScore > cs[j].s.CTRScore
		}
		if cs[i].s.SearchCount != cs[j].s.SearchCount {
			return cs[i].s.SearchCount > cs[j].s.SearchCount
		}
		return cs[i].s.Text < cs[j].s.Text
	})
	out := []Suggestion{}
	for i := 0; i < len(cs) && i < limit; i++ {
		out = append(out, cs[i].s)
	}
	return out
}

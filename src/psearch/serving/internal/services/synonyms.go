package services

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

// DefaultSynonymWeight is the ADR-003 low-relevance multiplier for synonym matches.
const DefaultSynonymWeight = 0.3

// Synonym is one row of search_synonyms (migration 015).
type Synonym struct {
	TenantID string   `json:"tenant_id"`
	Locale   string   `json:"locale"`
	Term     string   `json:"term"`
	Synonyms []string `json:"synonyms"`
	Weight   float64  `json:"weight"`
}

// SynonymStore persists synonyms (Spanner in production, memory in tests/demo).
type SynonymStore interface {
	List(ctx context.Context, tenantID, locale string) ([]Synonym, error) // locale "" = all locales
	Upsert(ctx context.Context, s Synonym) error
}

// Expansion is the result of expanding a query with synonyms.
type Expansion struct {
	Terms  []string // synonym phrases added (folded, lowercased)
	Weight float64  // multiplier applied to synonym-only matches
}

// SpannerQuery renders the expansion as a Spanner SEARCH query ("a" OR "b c").
func (e Expansion) SpannerQuery() string {
	quoted := make([]string, 0, len(e.Terms))
	for _, t := range e.Terms {
		quoted = append(quoted, `"`+strings.ReplaceAll(t, `"`, ``)+`"`)
	}
	return strings.Join(quoted, " OR ")
}

// SynonymService expands queries with tenant/locale synonyms (ADR-003, Issue #7).
// Lookups are cached per tenant|locale for ttl; Upsert invalidates the cache.
type SynonymService struct {
	store SynonymStore
	ttl   time.Duration

	mu    sync.Mutex
	cache map[string]synCacheEntry
}

type synCacheEntry struct {
	rows    []Synonym
	expires time.Time
}

func NewSynonymService(store SynonymStore, ttl time.Duration) *SynonymService {
	return &SynonymService{store: store, ttl: ttl, cache: map[string]synCacheEntry{}}
}

func normalizeTerm(s string) string { return FoldAccents(NormalizeQuery(s)) }

// List returns synonyms for a tenant/locale (cached).
func (s *SynonymService) List(ctx context.Context, tenantID, locale string) ([]Synonym, error) {
	key := tenantID + "|" + locale
	s.mu.Lock()
	if e, ok := s.cache[key]; ok && time.Now().Before(e.expires) {
		s.mu.Unlock()
		return e.rows, nil
	}
	s.mu.Unlock()
	rows, err := s.store.List(ctx, tenantID, locale)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.cache[key] = synCacheEntry{rows: rows, expires: time.Now().Add(s.ttl)}
	s.mu.Unlock()
	return rows, nil
}

// Upsert validates and stores a synonym entry.
func (s *SynonymService) Upsert(ctx context.Context, syn Synonym) (Synonym, error) {
	syn.TenantID = strings.TrimSpace(syn.TenantID)
	if syn.TenantID == "" {
		syn.TenantID = "default"
	}
	syn.Locale = strings.TrimSpace(syn.Locale)
	syn.Term = normalizeTerm(syn.Term)
	if syn.Locale == "" || syn.Term == "" || len(syn.Term) > 128 {
		return syn, fmt.Errorf("locale and term (<=128 chars) are required")
	}
	clean := []string{}
	for _, v := range syn.Synonyms {
		if v = normalizeTerm(v); v != "" && v != syn.Term && len(v) <= 128 {
			clean = append(clean, v)
		}
	}
	if len(clean) == 0 || len(clean) > 50 {
		return syn, fmt.Errorf("1-50 non-empty synonyms are required")
	}
	syn.Synonyms = clean
	if syn.Weight <= 0 {
		syn.Weight = DefaultSynonymWeight
	}
	if syn.Weight >= 1 {
		return syn, fmt.Errorf("weight must be in (0, 1) so exact matches always outrank synonyms")
	}
	if err := s.store.Upsert(ctx, syn); err != nil {
		return syn, err
	}
	s.mu.Lock()
	s.cache = map[string]synCacheEntry{}
	s.mu.Unlock()
	return syn, nil
}

// Expand returns synonym phrases for the query's tokens and whole phrase.
// Errors degrade to "no expansion" (search must not fail because of synonyms).
func (s *SynonymService) Expand(ctx context.Context, tenantID, locale, query string) Expansion {
	q := normalizeTerm(query)
	if s == nil || q == "" {
		return Expansion{}
	}
	rows, err := s.List(ctx, tenantID, locale)
	if err != nil {
		return Expansion{}
	}
	keys := map[string]bool{q: true}
	for _, tok := range strings.Fields(q) {
		keys[tok] = true
	}
	seen := map[string]bool{}
	weight := 0.0
	var terms []string
	for _, r := range rows {
		if !keys[r.Term] {
			continue
		}
		if weight == 0 || r.Weight < weight {
			weight = r.Weight // most conservative weight wins
		}
		for _, v := range r.Synonyms {
			if !seen[v] && !keys[v] {
				seen[v] = true
				terms = append(terms, v)
			}
		}
	}
	sort.Strings(terms)
	if len(terms) == 0 {
		return Expansion{}
	}
	return Expansion{Terms: terms, Weight: weight}
}

// DefaultSynonyms seeds e-commerce synonyms (also used by migration 015b).
func DefaultSynonyms() []Synonym {
	return []Synonym{
		{"default", "pt-BR", "tenis", []string{"calcado", "sapato esportivo", "sneaker", "running shoes"}, DefaultSynonymWeight},
		{"default", "pt-BR", "notebook", []string{"laptop", "computador portatil"}, DefaultSynonymWeight},
		{"default", "pt-BR", "geladeira", []string{"refrigerador", "refrigerator", "fridge"}, DefaultSynonymWeight},
		{"default", "pt-BR", "fone", []string{"headphones", "fone de ouvido"}, DefaultSynonymWeight},
		{"default", "en-US", "sneakers", []string{"shoes", "running shoes", "tenis"}, DefaultSynonymWeight},
		{"default", "en-US", "laptop", []string{"notebook", "computer"}, DefaultSynonymWeight},
		{"default", "en-US", "fridge", []string{"refrigerator", "geladeira"}, DefaultSynonymWeight},
		{"default", "en-US", "tv", []string{"television", "smart tv"}, DefaultSynonymWeight},
	}
}

// MemorySynonymStore is an in-process store seeded with DefaultSynonyms.
type MemorySynonymStore struct {
	mu   sync.Mutex
	rows map[string]Synonym
}

func NewMemorySynonymStore() *MemorySynonymStore {
	m := &MemorySynonymStore{rows: map[string]Synonym{}}
	for _, s := range DefaultSynonyms() {
		m.rows[s.TenantID+"|"+s.Locale+"|"+s.Term] = s
	}
	return m
}

func (m *MemorySynonymStore) List(_ context.Context, tenantID, locale string) ([]Synonym, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []Synonym{}
	for _, s := range m.rows {
		if s.TenantID == tenantID && (locale == "" || s.Locale == locale) {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Locale+out[i].Term < out[j].Locale+out[j].Term })
	return out, nil
}

func (m *MemorySynonymStore) Upsert(_ context.Context, s Synonym) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rows[s.TenantID+"|"+s.Locale+"|"+s.Term] = s
	return nil
}

// SpannerSynonymStore reads/writes the search_synonyms table.
type SpannerSynonymStore struct{ Client *spanner.Client }

func (s SpannerSynonymStore) List(ctx context.Context, tenantID, locale string) ([]Synonym, error) {
	stmt := spanner.Statement{
		SQL: `SELECT tenant_id, locale, term, IFNULL(synonyms, []), IFNULL(weight, 0.3)
			FROM search_synonyms
			WHERE tenant_id = @tenant AND (@locale = '' OR locale = @locale)
			ORDER BY locale, term`,
		Params: map[string]interface{}{"tenant": tenantID, "locale": locale},
	}
	iter := s.Client.Single().Query(ctx, stmt)
	defer iter.Stop()
	out := []Synonym{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		var r Synonym
		if err := row.Columns(&r.TenantID, &r.Locale, &r.Term, &r.Synonyms, &r.Weight); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
}

func (s SpannerSynonymStore) Upsert(ctx context.Context, r Synonym) error {
	_, err := s.Client.Apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdate("search_synonyms",
		[]string{"tenant_id", "locale", "term", "synonyms", "weight", "updated_at"},
		[]interface{}{r.TenantID, r.Locale, r.Term, r.Synonyms, r.Weight, spanner.CommitTimestamp})})
	return err
}

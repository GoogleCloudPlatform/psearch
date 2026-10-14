package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/genai"
	"google.golang.org/grpc/codes"
	"psearch/serving-go/internal/config"
	"psearch/serving-go/internal/models"
)

// Intent is the structured decomposition of a natural-language query.
type Intent struct {
	CleanQuery string  `json:"clean_query"`
	MinPrice   float64 `json:"min_price,omitempty"`
	MaxPrice   float64 `json:"max_price,omitempty"`
	Brand      string  `json:"brand,omitempty"`
	Category   string  `json:"category,omitempty"`
	Color      string  `json:"color,omitempty"` // soft: kept in clean_query (catalog has no color column)
}

// HasFilters reports whether any structured filter was extracted.
func (i Intent) HasFilters() bool {
	return i.MinPrice > 0 || i.MaxPrice > 0 || i.Brand != "" || i.Category != "" || i.Color != ""
}

// IntentLLM is the optional generative fallback (Gemini Flash Lite).
type IntentLLM interface {
	Extract(ctx context.Context, query string) (Intent, error)
}

// IntentStore is the persistent intent cache (Spanner query_intent_cache, migration 020).
type IntentStore interface {
	Get(ctx context.Context, key string) (Intent, bool, error)
	Put(ctx context.Context, key, query string, in Intent) error
}

var (
	num          = `(\d{1,3}(?:[.,]\d{3})+(?:[.,]\d{1,2})?|\d{1,7}(?:[.,]\d{1,2})?)`
	cur          = `(?:r\$|\$|us\$|usd|brl|mxn)?\s*`
	curSuffix    = `(?:\s*(?:reais|real|dolares|dollars|dolar|dollar|bucks|pesos|brl|usd))?`
	reBetween    = regexp.MustCompile(`\b(?:entre|between|de)\s+` + cur + num + curSuffix + `\s+(?:e|and|a|y|to|-)\s+` + cur + num + curSuffix)
	reMax        = regexp.MustCompile(`\b(?:ate|abaixo de|menos de|por menos de|no maximo|maximo|under|below|less than|up to|max|hasta|por debajo de|menor que)\s+` + cur + num + curSuffix)
	reMin        = regexp.MustCompile(`\b(?:acima de|a partir de|mais de|no minimo|minimo|over|above|more than|from|at least|min|desde|mas de|mayor que)\s+` + cur + num + curSuffix)
	reSpaces     = regexp.MustCompile(`\s+`)
	colorLexicon = map[string]string{
		"azul": "blue", "blue": "blue", "preto": "black", "preta": "black", "negro": "black", "negra": "black", "black": "black",
		"branco": "white", "branca": "white", "blanco": "white", "blanca": "white", "white": "white",
		"vermelho": "red", "vermelha": "red", "rojo": "red", "roja": "red", "red": "red",
		"verde": "green", "green": "green", "cinza": "gray", "gris": "gray", "gray": "gray", "grey": "gray",
		"rosa": "pink", "pink": "pink", "amarelo": "yellow", "amarela": "yellow", "amarillo": "yellow", "yellow": "yellow",
		"roxo": "purple", "roxa": "purple", "morado": "purple", "purple": "purple",
		"marrom": "brown", "marron": "brown", "brown": "brown", "laranja": "orange", "naranja": "orange", "orange": "orange",
	}
	dangling = map[string]bool{"de": true, "da": true, "do": true, "para": true, "com": true, "por": true, "for": true, "with": true, "the": true, "a": true, "e": true, "and": true, "y": true, "con": true, "el": true, "la": true}
)

func parseNum(s string) float64 {
	// The last separator followed by 1-2 digits is the decimal mark; every other
	// separator is a thousands separator ("1.500,00" -> 1500, "1,299.90" -> 1299.9,
	// "1.500" -> 1500, "19,90" -> 19.9).
	last := strings.LastIndexAny(s, ".,")
	if last >= 0 && len(s)-last-1 <= 2 {
		intPart := strings.NewReplacer(".", "", ",", "").Replace(s[:last])
		s = intPart + "." + s[last+1:]
	} else {
		s = strings.NewReplacer(".", "", ",", "").Replace(s)
	}
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

// QueryUnderstandingService decomposes queries: deterministic parser first,
// Gemini fallback only for conversational queries the parser can't decompose.
type QueryUnderstandingService struct {
	vocabMu    sync.RWMutex
	brands     map[string]string // folded -> canonical
	categories map[string]string
	llm        IntentLLM   // optional
	store      IntentStore // optional
	mu         sync.Mutex
	cache      map[string]Intent
}

func NewQueryUnderstandingService(products []models.SearchResult, llm IntentLLM, store IntentStore) *QueryUnderstandingService {
	q := &QueryUnderstandingService{llm: llm, store: store, cache: map[string]Intent{}}
	q.SetVocabulary(products)
	return q
}

// SetVocabulary (re)loads catalog brands/categories used for entity extraction.
func (q *QueryUnderstandingService) SetVocabulary(products []models.SearchResult) {
	brands, cats := map[string]string{}, map[string]string{}
	for _, p := range products {
		for _, b := range p.Brands {
			if b != "" {
				brands[FoldAccents(strings.ToLower(b))] = b
			}
		}
		for _, c := range p.Categories {
			if c != "" {
				cats[FoldAccents(strings.ToLower(c))] = c
			}
		}
	}
	q.vocabMu.Lock()
	q.brands, q.categories = brands, cats
	q.vocabMu.Unlock()
}

// Parse runs the deterministic parser.
func (q *QueryUnderstandingService) Parse(query string) Intent {
	s := " " + FoldAccents(strings.ToLower(query)) + " "
	var in Intent
	if m := reBetween.FindStringSubmatch(s); m != nil {
		a, b := parseNum(m[1]), parseNum(m[2])
		if a > b {
			a, b = b, a
		}
		in.MinPrice, in.MaxPrice = a, b
		s = strings.Replace(s, m[0], " ", 1)
	}
	if m := reMax.FindStringSubmatch(s); m != nil {
		in.MaxPrice = parseNum(m[1])
		s = strings.Replace(s, m[0], " ", 1)
	}
	if m := reMin.FindStringSubmatch(s); m != nil {
		in.MinPrice = parseNum(m[1])
		s = strings.Replace(s, m[0], " ", 1)
	}
	q.vocabMu.RLock()
	tokens := strings.Fields(s)
	kept := tokens[:0]
	for _, t := range tokens {
		switch {
		case in.Brand == "" && q.brands[t] != "":
			in.Brand = q.brands[t]
		case in.Category == "" && q.categories[t] != "":
			in.Category = q.categories[t]
			kept = append(kept, t) // category words still carry meaning for text/vector legs
		default:
			if c, ok := colorLexicon[t]; ok && in.Color == "" {
				in.Color = c
			}
			kept = append(kept, t)
		}
	}
	q.vocabMu.RUnlock()
	for len(kept) > 0 && dangling[kept[len(kept)-1]] {
		kept = kept[:len(kept)-1]
	}
	in.CleanQuery = strings.TrimSpace(reSpaces.ReplaceAllString(strings.Join(kept, " "), " "))
	if in.CleanQuery == "" {
		in.CleanQuery = strings.TrimSpace(query)
	}
	if in.MinPrice > 0 && in.MaxPrice > 0 && in.MinPrice > in.MaxPrice {
		in.MinPrice, in.MaxPrice = in.MaxPrice, in.MinPrice
	}
	return in
}

func intentKey(tenant, query string) string {
	sum := sha256.Sum256([]byte(tenant + "\x00" + FoldAccents(NormalizeQuery(query))))
	return hex.EncodeToString(sum[:])
}

// Understand returns the intent and its source: "parser", "cache", "gemini" or "none".
func (q *QueryUnderstandingService) Understand(ctx context.Context, tenant, query string) (Intent, string) {
	in := q.Parse(query)
	if in.HasFilters() || q.llm == nil || len(strings.Fields(query)) < 4 {
		if in.HasFilters() {
			return in, "parser"
		}
		return in, "none"
	}
	key := intentKey(tenant, query)
	q.mu.Lock()
	if c, ok := q.cache[key]; ok {
		q.mu.Unlock()
		return c, "cache"
	}
	q.mu.Unlock()
	if q.store != nil {
		sctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		c, ok, err := q.store.Get(sctx, key)
		cancel()
		if err == nil && ok {
			q.remember(key, c)
			return c, "cache"
		}
	}
	lctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	g, err := q.llm.Extract(lctx, query)
	if err != nil {
		log.Printf("WARN: gemini intent fallback failed (using parser result): %v", err)
		return in, "none"
	}
	g = sanitizeIntent(g, query)
	q.remember(key, g)
	if q.store != nil {
		go func() {
			wctx, c2 := context.WithTimeout(context.Background(), 5*time.Second)
			defer c2()
			if err := q.store.Put(wctx, key, query, g); err != nil {
				log.Printf("WARN: query_intent_cache write failed: %v", err)
			}
		}()
	}
	return g, "gemini"
}

func (q *QueryUnderstandingService) remember(key string, in Intent) {
	q.mu.Lock()
	if len(q.cache) > 5000 {
		q.cache = map[string]Intent{}
	}
	q.cache[key] = in
	q.mu.Unlock()
}

// sanitizeIntent bounds LLM output (never trust it blindly).
func sanitizeIntent(in Intent, original string) Intent {
	in.CleanQuery = strings.TrimSpace(in.CleanQuery)
	if in.CleanQuery == "" || len(in.CleanQuery) > 200 {
		in.CleanQuery = strings.TrimSpace(original)
	}
	if in.MinPrice < 0 || in.MinPrice > 1e7 {
		in.MinPrice = 0
	}
	if in.MaxPrice < 0 || in.MaxPrice > 1e7 {
		in.MaxPrice = 0
	}
	trunc := func(s string) string {
		if len(s) > 64 {
			return s[:64]
		}
		return s
	}
	in.Brand, in.Category, in.Color = trunc(in.Brand), trunc(in.Category), trunc(in.Color)
	return in
}

// ApplyIntent merges extracted filters into the request filters. Explicit request
// values always win over extracted ones.
func ApplyIntent(f models.SearchFilters, in Intent) models.SearchFilters {
	if f.MinPrice <= 0 {
		f.MinPrice = in.MinPrice
	}
	if f.MaxPrice <= 0 {
		f.MaxPrice = in.MaxPrice
	}
	if f.Brand == "" {
		f.Brand = in.Brand
	}
	if f.Category == "" {
		f.Category = in.Category
	}
	return f
}

// --- Gemini Flash Lite fallback -------------------------------------------

type geminiIntentLLM struct {
	client *genai.Client
	model  string
}

// NewGeminiIntentLLM builds the Vertex AI-backed fallback (GENAI_LOCATION, GEMINI_TEXT_MODEL / GEMINI_LITE_MODEL).
func NewGeminiIntentLLM(ctx context.Context, cfg *config.Config, model string) (IntentLLM, error) {
	if model == "" && cfg != nil {
		model = cfg.IntentLLMModel
	}
	if model == "" {
		model = "gemini-3.8-flash"
	}
	model = config.NormalizeModelID(model)
	client, err := genai.NewClient(ctx, &genai.ClientConfig{Backend: genai.BackendVertexAI, Project: cfg.ProjectID, Location: cfg.GenAILocation})
	if err != nil {
		return nil, err
	}
	return &geminiIntentLLM{client: client, model: model}, nil
}

func (g *geminiIntentLLM) Extract(ctx context.Context, query string) (Intent, error) {
	schema := &genai.Schema{Type: genai.TypeObject, Properties: map[string]*genai.Schema{
		"clean_query": {Type: genai.TypeString}, "min_price": {Type: genai.TypeNumber}, "max_price": {Type: genai.TypeNumber},
		"brand": {Type: genai.TypeString}, "category": {Type: genai.TypeString}, "color": {Type: genai.TypeString},
	}, Required: []string{"clean_query"}}
	prompt := "Extract e-commerce search filters from the shopper query. Return clean_query as the product being searched " +
		"(no prices/brands), prices as numbers (0 if absent), and empty strings for absent fields. Query: " + query
	res, err := g.client.Models.GenerateContent(ctx, g.model, genai.Text(prompt), &genai.GenerateContentConfig{
		ResponseMIMEType: "application/json", ResponseSchema: schema, Temperature: genai.Ptr[float32](0),
	})
	if err != nil {
		return Intent{}, err
	}
	var in Intent
	if err := json.Unmarshal([]byte(res.Text()), &in); err != nil {
		return Intent{}, fmt.Errorf("gemini returned non-JSON intent: %w", err)
	}
	return in, nil
}

// --- Spanner intent cache --------------------------------------------------

// SpannerIntentStore uses query_intent_cache (migration 020).
type SpannerIntentStore struct{ Client *spanner.Client }

func (s SpannerIntentStore) Get(ctx context.Context, key string) (Intent, bool, error) {
	row, err := s.Client.Single().ReadRow(ctx, "query_intent_cache", spanner.Key{key}, []string{"intent_json"})
	if spanner.ErrCode(err) == codes.NotFound {
		return Intent{}, false, nil
	}
	if err != nil {
		return Intent{}, false, err
	}
	var raw spanner.NullJSON
	if err := row.Columns(&raw); err != nil || !raw.Valid {
		return Intent{}, false, err
	}
	b, _ := json.Marshal(raw.Value)
	var in Intent
	if err := json.Unmarshal(b, &in); err != nil {
		return Intent{}, false, err
	}
	return in, true, nil
}

func (s SpannerIntentStore) Put(ctx context.Context, key, query string, in Intent) error {
	if len(query) > 512 {
		query = query[:512]
	}
	_, err := s.Client.Apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdate("query_intent_cache",
		[]string{"query_hash", "query_text", "intent_json", "source", "created_at"},
		[]interface{}{key, query, spanner.NullJSON{Value: in, Valid: true}, "gemini", spanner.CommitTimestamp})})
	return err
}

// CatalogVocabulary returns lightweight products (title, brand, category, clicks)
// for the query parser and spell checker; memory mode uses the seed.
func (s *SpannerService) CatalogVocabulary(ctx context.Context) []models.SearchResult {
	if s.client == nil {
		return s.seedProducts
	}
	iter := s.client.Single().Query(ctx, spanner.Statement{SQL: `SELECT IFNULL(title, ''), IFNULL(brand, ''), IFNULL(category, ''), IFNULL(clicks, 10) FROM products LIMIT 20000`})
	defer iter.Stop()
	var out []models.SearchResult
	for {
		row, err := iter.Next()
		if err != nil {
			if len(out) == 0 {
				return s.seedProducts // iterator.Done with no rows, or error
			}
			return out
		}
		var t, b, c string
		var clicks int64
		if row.Columns(&t, &b, &c, &clicks) == nil {
			out = append(out, models.SearchResult{Title: t, Brands: []string{b}, Categories: []string{c}, Signals: &models.BoostSignals{Clicks: clicks}})
		}
	}
}

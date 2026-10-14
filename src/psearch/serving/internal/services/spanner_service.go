package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"psearch/serving-go/internal/config"
	"psearch/serving-go/internal/models"
)

type SpannerService struct {
	client       *spanner.Client
	config       *config.Config
	embeddings   QueryEmbedder
	seedProducts []models.SearchResult
	backendMode  string
	synonyms     *SynonymService // optional query expansion
	rules        *RuleService    // optional merchandising rules
	inventory    []InventoryRow  // memory-mode regional inventory, lazily built
	invOnce      sync.Once
	pricing      *memoryPricing // memory-mode contract prices
	priceOnce    sync.Once
}

func (s *SpannerService) memPricing() *memoryPricing {
	s.priceOnce.Do(func() { s.pricing = newMemoryPricing(DefaultPriceRows(s.seedProducts)) })
	return s.pricing
}

// SetRules installs the merchandising rule service.
func (s *SpannerService) SetRules(r *RuleService) { s.rules = r }

func nonNilBoosts(b []ruleBoost) []ruleBoost {
	if b == nil {
		return []ruleBoost{}
	}
	return b
}

func nonNilStrings(v []string) []string {
	if v == nil {
		return []string{}
	}
	return v
}

// SetSynonyms installs the synonym expansion service.
func (s *SpannerService) SetSynonyms(syn *SynonymService) { s.synonyms = syn }

func NewSpannerService(ctx context.Context, cfg *config.Config, embeddings QueryEmbedder) (*SpannerService, error) {
	svc := &SpannerService{
		config:       cfg,
		embeddings:   embeddings,
		seedProducts: DefaultSeedProducts(),
		backendMode:  "memory",
	}

	if cfg.UseMemoryStore || cfg.ProjectID == "" || cfg.SpannerInstanceID == "" || cfg.SpannerDatabaseID == "" {
		return svc, nil
	}

	databaseName := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		cfg.ProjectID, cfg.SpannerInstanceID, cfg.SpannerDatabaseID)
	client, err := spanner.NewClient(ctx, databaseName)
	if err != nil {
		log.Printf("WARN: Spanner client creation failed (%v), using memory fallback", err)
		return svc, nil
	}
	svc.client = client
	svc.backendMode = "spanner"

	go func() {
		seedCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		// Retry once: on a cold, CPU-throttled instance the first session can take
		// longer than the budget. The check only seeds an EMPTY products table, so a
		// failure here never affects serving.
		err := svc.SeedIfEmpty(seedCtx)
		if err != nil && seedCtx.Err() != nil {
			retryCtx, cancel2 := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel2()
			err = svc.SeedIfEmpty(retryCtx)
		}
		if err != nil {
			log.Printf("INFO: Spanner seed check skipped (non-fatal): %v", err)
		}
	}()

	return svc, nil
}

func (s *SpannerService) Close() {
	if s.client != nil {
		s.client.Close()
	}
}

// SetQueryEmbedder replaces the query embedder (used to install the embedding cache,
// which needs the Spanner client created by NewSpannerService).
func (s *SpannerService) SetQueryEmbedder(e QueryEmbedder) { s.embeddings = e }

// Client exposes the Spanner client (nil in memory mode).
func (s *SpannerService) Client() *spanner.Client { return s.client }

func (s *SpannerService) BackendMode() string {
	return s.backendMode
}

func (s *SpannerService) ProductCount() int {
	if s.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		stmt := spanner.Statement{SQL: `SELECT COUNT(*) FROM products`}
		iter := s.client.Single().Query(ctx, stmt)
		defer iter.Stop()
		if row, err := iter.Next(); err == nil {
			var cnt int64
			if err := row.Columns(&cnt); err == nil && cnt > 0 {
				return int(cnt)
			}
		}
	}
	return len(s.seedProducts)
}

func (s *SpannerService) SeedIfEmpty(ctx context.Context) error {
	if s.client == nil {
		return nil
	}
	stmt := spanner.Statement{SQL: `SELECT COUNT(*) FROM products`}
	iter := s.client.Single().Query(ctx, stmt)
	row, err := iter.Next()
	iter.Stop()
	if err != nil {
		return err
	}
	var count int64
	if err := row.Columns(&count); err != nil {
		return err
	}
	if count > 0 {
		return nil
	}

	log.Printf("Seeding %d baseline products into Cloud Spanner (%s)...", len(s.seedProducts), s.config.SpannerDatabaseID)
	var muts []*spanner.Mutation
	for _, p := range s.seedProducts {
		rawJSON, _ := json.Marshal(p)
		emb := DeterministicEmbedding(p.Title+" "+p.Description+" "+strings.Join(p.Categories, " ")+" "+strings.Join(p.Brands, " "), 768)
		cat := ""
		if len(p.Categories) > 0 {
			cat = p.Categories[0]
		}
		brand := ""
		if len(p.Brands) > 0 {
			brand = p.Brands[0]
		}
		sig := models.BoostSignals{}
		if p.Signals != nil {
			sig = *p.Signals
		}
		stock := int64(0)
		if p.AvailableQuantity != nil {
			stock = int64(*p.AvailableQuantity)
		}
		muts = append(muts, spanner.InsertOrUpdate("products",
			[]string{"product_id", "product_data", "title", "description", "category", "brand", "price", "is_available", "stock", "is_visible", "channels", "tenant_id", "locale", "clicks", "orders", "discount_pct", "popularity_score", "margin_pct", "ctr", "conversion_rate", "recency_score", "embedding", "timestamp"},
			[]interface{}{
				p.ID,
				spanner.NullJSON{Value: json.RawMessage(rawJSON), Valid: true},
				p.Title,
				p.Description,
				cat,
				brand,
				p.PriceInfo.NumericPrice,
				p.Availability == "IN_STOCK",
				stock,
				true,
				p.Channels,
				p.TenantID,
				p.Locale,
				sig.Clicks, sig.Orders, sig.DiscountPct, sig.PopularityScore, sig.MarginPct, sig.CTR, sig.ConversionRate, sig.RecencyScore,
				emb,
				spanner.CommitTimestamp,
			},
		))
	}
	_, err = s.client.Apply(ctx, muts)
	return err
}

// Retrieval modes reported per search response so operators can verify which
// path actually served a query .
const (
	RetrievalHybridV1    = "hybrid_v1"             // Spanner RRF, legacy 768-d deterministic vectors
	RetrievalHybridV2    = "hybrid_v2"             // Spanner RRF, gemini-embedding-2 1024-d vectors
	RetrievalLexicalOnly = "lexical_only_degraded" // v2 active but query embedding unavailable
	RetrievalMemory      = "memory_fallback"       // in-process demo engine (no Spanner / Spanner error)
)

// SearchOutcome carries results plus the retrieval path that produced them.
type SearchOutcome struct {
	Results       []models.SearchResult
	RetrievalMode string
	CacheHit      bool     // query embedding served from the embedding cache (v2 only)
	ExpandedTerms []string // synonym phrases added to the text leg
	AppliedRules  []string // merchandising rule_ids applied
	Pins, Buries  []string // merchandising pins/buries (kept by the LTR re-ranker)
}

// HybridSearch executes hybrid vector + full-text RRF search against Cloud Spanner (with deterministic fallback).
func (s *SpannerService) HybridSearch(ctx context.Context, query string, limit int, minScore float64, alpha float64, filters models.SearchFilters) ([]models.SearchResult, error) {
	out, err := s.HybridSearchDetailed(ctx, query, limit, minScore, alpha, filters)
	return out.Results, err
}

// HybridSearchDetailed is HybridSearch plus the retrieval mode that served the query.
func (s *SpannerService) HybridSearchDetailed(ctx context.Context, query string, limit int, minScore float64, alpha float64, filters models.SearchFilters) (SearchOutcome, error) {
	if limit <= 0 {
		limit = s.config.DefaultLimit
	}
	if alpha < 0 {
		alpha = 0
	}
	if alpha > 1 {
		alpha = 1
	}

	nf := normalizeFilters(filters)
	expansion := s.synonyms.Expand(ctx, nf.TenantID, nf.Locale, query)
	ruleSet := s.rules.Match(ctx, nf.TenantID, query)
	if s.client != nil && strings.TrimSpace(query) != "" {
		plan := s.planVectorQuery(ctx, query)
		plan.expansion = expansion
		plan.rules = ruleSet
		res, err := s.spannerHybridSearch(ctx, query, limit, minScore, alpha, plan, filters)
		switch {
		case err != nil:
			log.Printf("WARN: Spanner HybridSearch fallback triggered: %v", err)
		case plan.lexicalOnly:
			// Degraded v2: return Spanner's lexical results as-is (even if empty).
			// Falling back to the in-memory engine here would mask the outage with
			// deterministic 768-d vector scores.
			return SearchOutcome{Results: res, RetrievalMode: RetrievalLexicalOnly, ExpandedTerms: expansion.Terms, AppliedRules: ruleSet.Applied, Pins: ruleSet.Pins, Buries: ruleSet.Buries}, nil
		default:
			// A successful Spanner query is authoritative even when it returns no rows
			// (e.g. tenant/category filters exclude everything). Falling back to the
			// in-memory seed engine here could leak products Spanner filtered out.
			mode := RetrievalHybridV1
			if plan.column == vectorColumnV2 {
				mode = RetrievalHybridV2
			}
			return SearchOutcome{Results: res, RetrievalMode: mode, CacheHit: plan.cacheHit, ExpandedTerms: expansion.Terms, AppliedRules: ruleSet.Applied, Pins: ruleSet.Pins, Buries: ruleSet.Buries}, nil
		}
	}

	return SearchOutcome{Results: s.memoryHybridSearchExpanded(query, limit, minScore, alpha, filters, expansion, ruleSet), RetrievalMode: RetrievalMemory, ExpandedTerms: expansion.Terms, AppliedRules: ruleSet.Applied, Pins: ruleSet.Pins, Buries: ruleSet.Buries}, nil
}

// vectorPlan describes how the vector leg of the hybrid query is executed.
type vectorPlan struct {
	column      string      // vector column to search ("embedding" for v1, "embedding_v2" for v2)
	embedding   interface{} // []float64 (v1 deterministic) or []float32 (v2 Gemini)
	lexicalOnly bool        // true when no usable query embedding exists (degraded mode)
	cacheHit    bool        // query embedding came from the embedding cache
	expansion   Expansion   // synonym expansion for the text leg
	rules       RuleSet     // merchandising rules matching the query
}

// metaEmbedder is implemented by EmbeddingCache.
type metaEmbedder interface {
	EmbedQueryWithMeta(ctx context.Context, text string) ([]float32, bool, error)
}

const (
	vectorColumnV1 = "embedding"
	vectorColumnV2 = "embedding_v2"
)

// planVectorQuery selects the vector column / query embedding for the active
// embedding version. v1 keeps the legacy 768-d deterministic
// embedding; v2 uses gemini-embedding-2 via the Gen AI SDK. Any embedding failure
// degrades to lexical-only retrieval instead of failing the request.
func (s *SpannerService) planVectorQuery(ctx context.Context, query string) vectorPlan {
	if !s.config.UseGeminiEmbeddings() {
		return vectorPlan{column: vectorColumnV1, embedding: DeterministicEmbedding(query, config.LegacyEmbeddingDimension)}
	}
	if s.embeddings == nil {
		log.Printf("WARN: ACTIVE_EMBEDDING_VERSION=v2 but embedding service is unavailable; lexical-only search")
		return vectorPlan{column: vectorColumnV2, lexicalOnly: true}
	}
	var emb []float32
	var hit bool
	var err error
	if me, ok := s.embeddings.(metaEmbedder); ok {
		emb, hit, err = me.EmbedQueryWithMeta(ctx, query)
	} else {
		emb, err = s.embeddings.EmbedQuery(ctx, query)
	}
	if err != nil {
		log.Printf("WARN: query embedding failed (%v); lexical-only search", err)
		return vectorPlan{column: vectorColumnV2, lexicalOnly: true}
	}
	return vectorPlan{column: vectorColumnV2, embedding: emb, cacheHit: hit}
}

// catalogFilterSQL is applied inside BOTH retrieval legs (/ ):
// hidden products are never returned; channel and only-available filters are
// optional (empty channel / false flag disables them). NULL columns default to
// visible, all channels, available.
var catalogFilterSQL = `COALESCE(is_visible, TRUE)
					AND (@channel = '' OR @channel IN UNNEST(IFNULL(channels, ['web', 'mobile', 'store'])))
					AND (NOT @only_available OR (IFNULL(is_available, TRUE) AND IFNULL(stock, 1) > 0))
					AND IFNULL(tenant_id, 'default') = @tenant_id
					AND (@locale = '' OR IFNULL(locale, '') = @locale)
					AND (@category = '' OR LOWER(IFNULL(category, '')) = @category)
					AND (@brand = '' OR LOWER(IFNULL(brand, '')) = @brand)
					AND (@min_price <= 0 OR IFNULL(` + effectivePriceSQL("products") + `, 0) >= @min_price)
					AND (@max_price <= 0 OR IFNULL(` + effectivePriceSQL("products") + `, 0) <= @max_price)
					AND ` + assortmentSQL + `
					AND ` + deliveryFilterSQL

// optimizerHint pins the hybrid query to the latest Spanner query optimizer.
const optimizerHint = "@{OPTIMIZER_VERSION=latest_version} "

// buildHybridSQL renders the RRF hybrid query. column is always one of the
// package constants (never user input). Unavailable products (is_available =
// FALSE or stock <= 0) are always ordered after available ones.
func buildHybridSQL(column string, lexicalOnly bool) string {
	return buildHybridSQLWith(column, lexicalOnly, false)
}

// buildHybridSQLWith optionally searches the accent-folded tokenlists
// (title_tokens_folded / description_tokens_folded, migration 014b).
func buildHybridSQLWith(column string, lexicalOnly, folded bool) string {
	titleTok, descTok := "title_tokens", "description_tokens"
	if folded {
		titleTok, descTok = "title_tokens_folded", "description_tokens_folded"
	}
	fts := `
		fts AS (
			SELECT offset + 1 AS rank, product_id, title, product_data, is_available, stock
			FROM UNNEST(ARRAY(
				SELECT AS STRUCT product_id, title, product_data, is_available, stock
				FROM products
				WHERE (SEARCH(` + titleTok + `, @query_text) OR SEARCH(` + descTok + `, @query_text)
					OR SEARCH(` + titleTok + `, @syn_text) OR SEARCH(` + descTok + `, @syn_text))
					AND ` + catalogFilterSQL + `
				ORDER BY
					/* exact_first: exact matches always outrank synonym-only matches */
					IF(SCORE(` + titleTok + `, @query_text) + SCORE(` + descTok + `, @query_text) > 0, 0, 1),
					(SCORE(` + titleTok + `, @query_text) + SCORE(` + descTok + `, @query_text))
						+ @syn_weight * (SCORE(` + titleTok + `, @syn_text) + SCORE(` + descTok + `, @syn_text)) DESC
				LIMIT @limit
			)) WITH OFFSET AS offset
		)`
	ann := `
		ann AS (
			SELECT offset + 1 AS rank, product_id, title, product_data, is_available, stock
			FROM UNNEST(ARRAY(
				SELECT AS STRUCT product_id, title, product_data, is_available, stock
				FROM products
				WHERE ` + column + ` IS NOT NULL
					AND ` + catalogFilterSQL + `
				ORDER BY COSINE_DISTANCE(` + column + `, @query_embedding)
				LIMIT @limit
			)) WITH OFFSET AS offset
		),`
	union := `
			SELECT 'vector' AS source, @alpha AS weight, rank, product_id, title, product_data, is_available, stock FROM ann
			UNION ALL
			SELECT 'text' AS source, (1.0 - @alpha) AS weight, rank, product_id, title, product_data, is_available, stock FROM fts`
	if lexicalOnly {
		ann = ""
		union = `
			SELECT 'text' AS source, 1.0 AS weight, rank, product_id, title, product_data, is_available, stock FROM fts`
	}
	// The aggregate query is wrapped so the unavailable-last ORDER BY works on
	// plain columns (ordering by ANY_VALUE(...) directly nests aggregates, which
	// Spanner rejects: "Aggregations of aggregations are not allowed").
	return `
		WITH` + ann + fts + `
		SELECT rrf_score, vector_score, text_score, product_id, title, product_data, is_available, stock, boost_score, rule_mult, effective_price
		FROM (
			/* final = rrf_score + boost_signal_score + merchandising_rule_delta,
			   rule_delta = (rule_mult - 1) * rrf_score; pins first, buries last (within availability) */
			SELECT a.*,
				@boost_weight * ` + boostSignalSQL + ` AS boost_score,
				IFNULL((SELECT EXP(SUM(LN(r.multiplier))) FROM UNNEST(@rule_boosts) AS r
					WHERE (r.brand = '' OR LOWER(IFNULL(p.brand, '')) = r.brand)
						AND (r.category = '' OR LOWER(IFNULL(p.category, '')) = r.category)), 1.0) AS rule_mult,
				IFNULL((SELECT MIN(o) FROM UNNEST(@pin_ids) AS pid WITH OFFSET AS o WHERE pid = a.product_id), 1000) AS pin_rank,
				IF(a.product_id IN UNNEST(@bury_ids), 1, 0) AS buried,
				` + effectivePriceSQL("p") + ` AS effective_price
			FROM (
			SELECT
				SUM(weight / (60.0 + rank)) AS rrf_score,
				MAX(IF(source = 'vector', 1.0 / (60.0 + rank), 0.0)) AS vector_score,
				MAX(IF(source = 'text', 1.0 / (60.0 + rank), 0.0)) AS text_score,
				product_id,
				ANY_VALUE(title) AS title,
				ANY_VALUE(product_data) AS product_data,
				ANY_VALUE(is_available) AS is_available,
				ANY_VALUE(stock) AS stock
			FROM (` + union + `
			)
			GROUP BY product_id
			) AS a
			JOIN products AS p ON p.product_id = a.product_id
		)
		ORDER BY IF(IFNULL(is_available, TRUE) AND IFNULL(stock, 1) > 0, 0, 1) /* unavailable_rank */,
			pin_rank, buried,
			IF(@sort_by = 'price_asc', effective_price, NULL) ASC, IF(@sort_by = 'price_desc', effective_price, NULL) DESC,
			rrf_score * rule_mult + boost_score DESC /* final_score */,
			vector_score DESC, text_score DESC, product_id
		LIMIT @limit
	`
}

// CheckSpannerQuery runs the real Spanner hybrid query (no memory fallback) and
// returns its error, so operators can validate SQL against psearch-db before a
// rollout (cmd/sqlcheck). Read-only.
func (s *SpannerService) CheckSpannerQuery(ctx context.Context, query string, alpha float64, filters models.SearchFilters) (int, string, error) {
	if s.client == nil {
		return 0, "", fmt.Errorf("no Spanner client (memory mode)")
	}
	plan := s.planVectorQuery(ctx, query)
	res, err := s.spannerHybridSearch(ctx, query, s.config.DefaultLimit, 0, alpha, plan, filters)
	mode := RetrievalHybridV1
	if plan.lexicalOnly {
		mode = RetrievalLexicalOnly
	} else if plan.column == vectorColumnV2 {
		mode = RetrievalHybridV2
	}
	return len(res), mode, err
}

// isUnavailable implements the availability rule shared by Spanner and memory modes.
func isUnavailable(availability string, quantity *int) bool {
	return availability == "OUT_OF_STOCK" || (quantity != nil && *quantity <= 0)
}

// matchesChannel reports whether a product sold on channels matches the requested channel
// (empty request or empty product channel list = all channels).
func matchesChannel(requested string, channels []string) bool {
	if requested == "" || len(channels) == 0 {
		return true
	}
	for _, c := range channels {
		if c == requested {
			return true
		}
	}
	return false
}

func (s *SpannerService) spannerHybridSearch(ctx context.Context, query string, limit int, minScore float64, alpha float64, plan vectorPlan, filters models.SearchFilters) ([]models.SearchResult, error) {
	// The correlated product_prices lookups change the plan shape; SCORE() in
	// the fts leg then requires query optimizer version >= 6, which is newer than the
	// database default. A statement hint is the least invasive fix (verified via cmd/sqlcheck).
	sql := optimizerHint + buildHybridSQLWith(plan.column, plan.lexicalOnly, s.config.TextFolding)
	queryText := query
	if s.config.TextFolding {
		queryText = FoldAccents(query)
	}
	if filters.RelaxText {
		queryText = RelaxedQuery(queryText) // implicit AND -> OR
	}
	f := normalizeFilters(filters)
	// Synonym leg: when there is no expansion, reuse the exact query with weight 0
	// (avoids an empty SEARCH query and leaves scores unchanged).
	synText, synWeight := queryText, 0.0
	if len(plan.expansion.Terms) > 0 {
		synText, synWeight = plan.expansion.SpannerQuery(), plan.expansion.Weight
	}
	params := map[string]interface{}{
		"query_text":     queryText,
		"syn_text":       synText,
		"syn_weight":     synWeight,
		"boost_weight":   s.config.BoostWeight,
		"rule_boosts":    nonNilBoosts(plan.rules.Boosts),
		"pin_ids":        nonNilStrings(plan.rules.Pins),
		"bury_ids":       nonNilStrings(plan.rules.Buries),
		"tenant_id":      f.TenantID,
		"locale":         f.Locale,
		"category":       f.Category,
		"brand":          f.Brand,
		"zip5":           f.Zip5,
		"min_price":      f.MinPrice,
		"price_table":    f.PriceTable,
		"sort_by":        priceSortKey(f.SortBy),
		"max_price":      f.MaxPrice,
		"max_sla":        f.MaxSLAHours,
		"alpha":          alpha,
		"limit":          int64(limit),
		"channel":        f.Channel,
		"only_available": f.OnlyInStock,
	}
	if !plan.lexicalOnly {
		params["query_embedding"] = plan.embedding
	}

	stmt := spanner.Statement{SQL: sql, Params: params}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var results []models.SearchResult
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		var rrfScore, vecScore, txtScore float64
		var productID, title string
		var productDataJSON spanner.NullJSON
		var isAvailable spanner.NullBool
		var stock spanner.NullInt64
		var boostScore, ruleMult float64
		var effPrice spanner.NullFloat64
		if err := row.Columns(&rrfScore, &vecScore, &txtScore, &productID, &title, &productDataJSON, &isAvailable, &stock, &boostScore, &ruleMult, &effPrice); err != nil {
			return nil, err
		}
		if rrfScore < minScore {
			continue
		}
		var item models.SearchResult
		if productDataJSON.Valid {
			raw, _ := json.Marshal(productDataJSON.Value)
			_ = json.Unmarshal(raw, &item)
		}
		if item.ID == "" {
			item.ID = productID
		}
		if item.Title == "" {
			item.Title = title
		}
		if stock.Valid {
			quantity := int(stock.Int64)
			item.AvailableQuantity = &quantity
		}
		if isAvailable.Valid || stock.Valid {
			if (isAvailable.Valid && !isAvailable.Bool) || (stock.Valid && stock.Int64 <= 0) {
				item.Availability = "OUT_OF_STOCK"
			} else {
				item.Availability = "IN_STOCK"
			}
		}
		if effPrice.Valid {
			applyEffectivePrice(&item, effPrice.Float64, f.PriceTable)
		}
		effAlpha := alpha
		if plan.lexicalOnly {
			effAlpha = 0
		}
		item.Score = map[string]float64{
			"hybrid":          rrfScore,
			"vector":          vecScore,
			"text":            txtScore,
			"alpha":           effAlpha,
			"boost":           boostScore,
			"rule_multiplier": ruleMult,
			"final_score":     rrfScore*ruleMult + boostScore,
		}
		results = append(results, item)
	}
	return results, nil
}

func (s *SpannerService) memoryHybridSearch(query string, limit int, minScore float64, alpha float64, filters models.SearchFilters) []models.SearchResult {
	nf := normalizeFilters(filters)
	return s.memoryHybridSearchExpanded(query, limit, minScore, alpha, filters,
		s.synonyms.Expand(context.Background(), nf.TenantID, nf.Locale, query), s.rules.Match(context.Background(), nf.TenantID, query))
}

func (s *SpannerService) memoryHybridSearchExpanded(query string, limit int, minScore float64, alpha float64, filters models.SearchFilters, expansion Expansion, rules RuleSet) []models.SearchResult {
	filters = normalizeFilters(filters)
	qLower := FoldAccents(strings.ToLower(strings.TrimSpace(query)))
	qTokens := strings.Fields(qLower)
	qVec := DeterministicEmbedding(query, 768)

	type scored struct {
		prod     models.SearchResult
		vecSim   float64
		txtRaw   float64
		exact    bool
		vecRank  int
		txtRank  int
		rrfScore float64
		vecRRF   float64
		txtRRF   float64
	}

	items := make([]scored, len(s.seedProducts))
	for i, p := range s.seedProducts {
		docText := FoldAccents(strings.ToLower(p.Title + " " + p.Description + " " + strings.Join(p.Brands, " ") + " " + strings.Join(p.Categories, " ")))
		pVec := DeterministicEmbedding(docText, 768)
		vecSim := CosineSimilarity(qVec, pVec)

		var txtRaw float64
		if qLower == "" {
			txtRaw = 1.0
		} else {
			for _, tok := range qTokens {
				if strings.Contains(FoldAccents(strings.ToLower(p.Title)), tok) {
					txtRaw += 2.0
				}
				if strings.Contains(docText, tok) {
					txtRaw += 1.0
				}
			}
		}
		exact := txtRaw > 0
		titleFolded := FoldAccents(strings.ToLower(p.Title))
		for _, term := range expansion.Terms {
			if strings.Contains(titleFolded, term) {
				txtRaw += 2.0 * expansion.Weight
			}
			if strings.Contains(docText, term) {
				txtRaw += 1.0 * expansion.Weight
			}
		}
		items[i] = scored{prod: p, vecSim: vecSim, txtRaw: txtRaw, exact: exact}
	}

	// Assign vector ranks
	vecOrder := make([]int, len(items))
	for i := range vecOrder {
		vecOrder[i] = i
	}
	sort.Slice(vecOrder, func(i, j int) bool {
		return items[vecOrder[i]].vecSim > items[vecOrder[j]].vecSim
	})
	for rankIdx, itemIdx := range vecOrder {
		items[itemIdx].vecRank = rankIdx + 1
	}

	// Assign text ranks
	txtOrder := make([]int, len(items))
	for i := range txtOrder {
		txtOrder[i] = i
	}
	sort.Slice(txtOrder, func(i, j int) bool {
		if items[txtOrder[i]].exact != items[txtOrder[j]].exact {
			return items[txtOrder[i]].exact // exact matches outrank synonym-only
		}
		if items[txtOrder[i]].txtRaw == items[txtOrder[j]].txtRaw {
			return items[txtOrder[i]].prod.ID < items[txtOrder[j]].prod.ID
		}
		return items[txtOrder[i]].txtRaw > items[txtOrder[j]].txtRaw
	})
	for rankIdx, itemIdx := range txtOrder {
		items[itemIdx].txtRank = rankIdx + 1
	}

	var out []models.SearchResult
	for i := range items {
		if filters.OnlyInStock && isUnavailable(items[i].prod.Availability, items[i].prod.AvailableQuantity) {
			continue
		}
		eff := s.memPricing().effective(filters.PriceTable, items[i].prod.ID, items[i].prod.PriceInfo.NumericPrice)
		if !s.memPricing().visible(filters.PriceTable, items[i].prod.ID) ||
			(filters.MinPrice > 0 && eff < filters.MinPrice) || (filters.MaxPrice > 0 && eff > filters.MaxPrice) {
			continue
		}
		if !matchesChannel(filters.Channel, items[i].prod.Channels) || !matchesCatalog(filters, items[i].prod) ||
			!s.deliverableInMemory(items[i].prod.ID, filters.Zip5, filters.MaxSLAHours) {
			continue
		}
		items[i].vecRRF = 1.0 / float64(60+items[i].vecRank)
		if items[i].txtRaw > 0 || qLower == "" {
			items[i].txtRRF = 1.0 / float64(60+items[i].txtRank)
		}
		items[i].rrfScore = alpha*items[i].vecRRF + (1.0-alpha)*items[i].txtRRF
		if items[i].rrfScore < minScore {
			continue
		}
		res := items[i].prod
		boost := 0.0
		if res.Signals != nil {
			boost = s.config.BoostWeight * BoostSignalScore(*res.Signals)
		}
		brand, category := "", ""
		if len(res.Brands) > 0 {
			brand = res.Brands[0]
		}
		if len(res.Categories) > 0 {
			category = res.Categories[0]
		}
		applyEffectivePrice(&res, s.memPricing().effective(filters.PriceTable, res.ID, res.PriceInfo.NumericPrice), filters.PriceTable)
		mult := rules.Multiplier(brand, category)
		res.Score = map[string]float64{
			"hybrid":          items[i].rrfScore,
			"vector":          items[i].vecRRF,
			"text":            items[i].txtRRF,
			"cosine_sim":      items[i].vecSim,
			"alpha":           alpha,
			"boost":           boost,
			"rule_multiplier": mult,
			"final_score":     items[i].rrfScore*mult + boost,
		}
		out = append(out, res)
	}

	sort.SliceStable(out, func(i, j int) bool {
		ui := isUnavailable(out[i].Availability, out[i].AvailableQuantity)
		uj := isUnavailable(out[j].Availability, out[j].AvailableQuantity)
		if ui != uj {
			return !ui // available products first
		}
		if pi, pj := pinRank(rules.Pins, out[i].ID), pinRank(rules.Pins, out[j].ID); pi != pj {
			return pi < pj // pinned products first, in rule order
		}
		if bi, bj := containsID(rules.Buries, out[i].ID), containsID(rules.Buries, out[j].ID); bi != bj {
			return !bi // buried products last (within availability group)
		}
		if filters.SortBy == "price_asc" || filters.SortBy == "price_desc" {
			if pi, pj := out[i].PriceInfo.NumericPrice, out[j].PriceInfo.NumericPrice; pi != pj {
				return (pi < pj) == (filters.SortBy == "price_asc")
			}
		}
		if out[i].Score["final_score"] != out[j].Score["final_score"] {
			return out[i].Score["final_score"] > out[j].Score["final_score"]
		}
		if out[i].Score["vector"] != out[j].Score["vector"] {
			return out[i].Score["vector"] > out[j].Score["vector"]
		}
		if out[i].Score["text"] != out[j].Score["text"] {
			return out[i].Score["text"] > out[j].Score["text"]
		}
		return out[i].ID < out[j].ID // deterministic tie-break
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// normalizeFilters lowercases/trims filter values and applies defaults.
func normalizeFilters(f models.SearchFilters) models.SearchFilters {
	f.Channel = strings.ToLower(strings.TrimSpace(f.Channel))
	f.Category = strings.ToLower(strings.TrimSpace(f.Category))
	f.Brand = strings.ToLower(strings.TrimSpace(f.Brand))
	f.TenantID = strings.TrimSpace(f.TenantID)
	if f.TenantID == "" {
		f.TenantID = "default"
	}
	f.Locale = strings.TrimSpace(f.Locale)
	return f
}

// matchesCatalog applies tenant / locale / category / brand filters in memory mode.
func matchesCatalog(f models.SearchFilters, p models.SearchResult) bool {
	tenant := p.TenantID
	if tenant == "" {
		tenant = "default"
	}
	if tenant != f.TenantID {
		return false
	}
	if f.Locale != "" && p.Locale != f.Locale {
		return false
	}
	if f.Category != "" && !containsFold(p.Categories, f.Category) {
		return false
	}
	if f.Brand != "" && !containsFold(p.Brands, f.Brand) {
		return false
	}
	return true // price filters use the effective (contract) price, applied by the caller
}

func containsFold(values []string, want string) bool {
	for _, v := range values {
		if strings.EqualFold(v, want) {
			return true
		}
	}
	return false
}

func pinRank(pins []string, id string) int {
	for i, p := range pins {
		if p == id {
			return i
		}
	}
	return 1000
}

func containsID(ids []string, id string) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

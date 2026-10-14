package services

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
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
}

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
		if err := svc.SeedIfEmpty(seedCtx); err != nil {
			log.Printf("WARN: Spanner seed check: %v", err)
		}
	}()

	return svc, nil
}

func (s *SpannerService) Close() {
	if s.client != nil {
		s.client.Close()
	}
}

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
		stock := int64(0)
		if p.AvailableQuantity != nil {
			stock = int64(*p.AvailableQuantity)
		}
		muts = append(muts, spanner.InsertOrUpdate("products",
			[]string{"product_id", "product_data", "title", "description", "category", "brand", "price", "is_available", "stock", "embedding", "timestamp"},
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
}

// HybridSearch executes hybrid vector + full-text RRF search against Cloud Spanner (with deterministic fallback).
func (s *SpannerService) HybridSearch(ctx context.Context, query string, limit int, minScore float64, alpha float64) ([]models.SearchResult, error) {
	out, err := s.HybridSearchDetailed(ctx, query, limit, minScore, alpha)
	return out.Results, err
}

// HybridSearchDetailed is HybridSearch plus the retrieval mode that served the query.
func (s *SpannerService) HybridSearchDetailed(ctx context.Context, query string, limit int, minScore float64, alpha float64) (SearchOutcome, error) {
	if limit <= 0 {
		limit = s.config.DefaultLimit
	}
	if alpha < 0 {
		alpha = 0
	}
	if alpha > 1 {
		alpha = 1
	}

	if s.client != nil && strings.TrimSpace(query) != "" {
		plan := s.planVectorQuery(ctx, query)
		res, err := s.spannerHybridSearch(ctx, query, limit, minScore, alpha, plan)
		switch {
		case err != nil:
			log.Printf("WARN: Spanner HybridSearch fallback triggered: %v", err)
		case plan.lexicalOnly:
			// Degraded v2: return Spanner's lexical results as-is (even if empty).
			// Falling back to the in-memory engine here would mask the outage with
			// deterministic 768-d vector scores.
			return SearchOutcome{Results: res, RetrievalMode: RetrievalLexicalOnly}, nil
		case len(res) > 0:
			mode := RetrievalHybridV1
			if plan.column == vectorColumnV2 {
				mode = RetrievalHybridV2
			}
			return SearchOutcome{Results: res, RetrievalMode: mode}, nil
		}
	}

	return SearchOutcome{Results: s.memoryHybridSearch(query, limit, minScore, alpha), RetrievalMode: RetrievalMemory}, nil
}

// vectorPlan describes how the vector leg of the hybrid query is executed.
type vectorPlan struct {
	column      string      // vector column to search ("embedding" for v1, "embedding_v2" for v2)
	embedding   interface{} // []float64 (v1 deterministic) or []float32 (v2 Gemini)
	lexicalOnly bool        // true when no usable query embedding exists (degraded mode)
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
	emb, err := s.embeddings.EmbedQuery(ctx, query)
	if err != nil {
		log.Printf("WARN: query embedding failed (%v); lexical-only search", err)
		return vectorPlan{column: vectorColumnV2, lexicalOnly: true}
	}
	return vectorPlan{column: vectorColumnV2, embedding: emb}
}

// buildHybridSQL renders the RRF hybrid query. column is always one of the
// package constants (never user input).
func buildHybridSQL(column string, lexicalOnly bool) string {
	fts := `
		fts AS (
			SELECT offset + 1 AS rank, product_id, title, product_data
			FROM UNNEST(ARRAY(
				SELECT AS STRUCT product_id, title, product_data
				FROM products
				WHERE SEARCH(title_tokens, @query_text) OR SEARCH(description_tokens, @query_text)
				ORDER BY (SCORE(title_tokens, @query_text) + SCORE(description_tokens, @query_text)) DESC
				LIMIT @limit
			)) WITH OFFSET AS offset
		)`
	ann := `
		ann AS (
			SELECT offset + 1 AS rank, product_id, title, product_data
			FROM UNNEST(ARRAY(
				SELECT AS STRUCT product_id, title, product_data
				FROM products
				WHERE ` + column + ` IS NOT NULL
				ORDER BY COSINE_DISTANCE(` + column + `, @query_embedding)
				LIMIT @limit
			)) WITH OFFSET AS offset
		),`
	union := `
			SELECT 'vector' AS source, @alpha AS weight, rank, product_id, title, product_data FROM ann
			UNION ALL
			SELECT 'text' AS source, (1.0 - @alpha) AS weight, rank, product_id, title, product_data FROM fts`
	if lexicalOnly {
		ann = ""
		union = `
			SELECT 'text' AS source, 1.0 AS weight, rank, product_id, title, product_data FROM fts`
	}
	return `
		WITH` + ann + fts + `
		SELECT
			SUM(weight / (60.0 + rank)) AS rrf_score,
			MAX(IF(source = 'vector', 1.0 / (60.0 + rank), 0.0)) AS vector_score,
			MAX(IF(source = 'text', 1.0 / (60.0 + rank), 0.0)) AS text_score,
			product_id,
			ANY_VALUE(title) AS title,
			ANY_VALUE(product_data) AS product_data
		FROM (` + union + `
		)
		GROUP BY product_id
		ORDER BY rrf_score DESC
		LIMIT @limit
	`
}

func (s *SpannerService) spannerHybridSearch(ctx context.Context, query string, limit int, minScore float64, alpha float64, plan vectorPlan) ([]models.SearchResult, error) {
	sql := buildHybridSQL(plan.column, plan.lexicalOnly)
	params := map[string]interface{}{
		"query_text": query,
		"alpha":      alpha,
		"limit":      int64(limit),
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
		if err := row.Columns(&rrfScore, &vecScore, &txtScore, &productID, &title, &productDataJSON); err != nil {
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
		effAlpha := alpha
		if plan.lexicalOnly {
			effAlpha = 0
		}
		item.Score = map[string]float64{
			"hybrid": rrfScore,
			"vector": vecScore,
			"text":   txtScore,
			"alpha":  effAlpha,
		}
		results = append(results, item)
	}
	return results, nil
}

func (s *SpannerService) memoryHybridSearch(query string, limit int, minScore float64, alpha float64) []models.SearchResult {
	qLower := strings.ToLower(strings.TrimSpace(query))
	qTokens := strings.Fields(qLower)
	qVec := DeterministicEmbedding(query, 768)

	type scored struct {
		prod     models.SearchResult
		vecSim   float64
		txtRaw   float64
		vecRank  int
		txtRank  int
		rrfScore float64
		vecRRF   float64
		txtRRF   float64
	}

	items := make([]scored, len(s.seedProducts))
	for i, p := range s.seedProducts {
		docText := strings.ToLower(p.Title + " " + p.Description + " " + strings.Join(p.Brands, " ") + " " + strings.Join(p.Categories, " "))
		pVec := DeterministicEmbedding(docText, 768)
		vecSim := CosineSimilarity(qVec, pVec)

		var txtRaw float64
		if qLower == "" {
			txtRaw = 1.0
		} else {
			for _, tok := range qTokens {
				if strings.Contains(strings.ToLower(p.Title), tok) {
					txtRaw += 2.0
				}
				if strings.Contains(docText, tok) {
					txtRaw += 1.0
				}
			}
		}
		items[i] = scored{prod: p, vecSim: vecSim, txtRaw: txtRaw}
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
		items[i].vecRRF = 1.0 / float64(60+items[i].vecRank)
		if items[i].txtRaw > 0 || qLower == "" {
			items[i].txtRRF = 1.0 / float64(60+items[i].txtRank)
		}
		items[i].rrfScore = alpha*items[i].vecRRF + (1.0-alpha)*items[i].txtRRF
		if items[i].rrfScore < minScore {
			continue
		}
		res := items[i].prod
		res.Score = map[string]float64{
			"hybrid":     items[i].rrfScore,
			"vector":     items[i].vecRRF,
			"text":       items[i].txtRRF,
			"cosine_sim": items[i].vecSim,
			"alpha":      alpha,
		}
		out = append(out, res)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Score["hybrid"] > out[j].Score["hybrid"]
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

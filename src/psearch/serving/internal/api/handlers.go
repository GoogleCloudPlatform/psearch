package api

import (
	"context"
	"crypto/subtle"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"psearch/serving-go/internal/config"
	"psearch/serving-go/internal/models"
	"psearch/serving-go/internal/services"
)

type Controller struct {
	config       *config.Config
	spannerSvc   *services.SpannerService
	embeddingSvc *services.EmbeddingService
	cache        *services.EmbeddingCache // nil when the embedding service is unavailable
	synonyms     *services.SynonymService
	rules        *services.RuleService
	autocomplete *services.AutocompleteService
	ltrModel     *services.LTRModel
	intent       *services.QueryUnderstandingService
	spell        *services.SpellCheckService
	session      *services.SessionService
}

func NewController(cfg *config.Config) (*Controller, error) {
	ctx := context.Background()
	var embedder services.QueryEmbedder
	embeddingSvc, err := services.NewEmbeddingService(ctx, cfg)
	if err != nil {
		log.Printf("WARN: embedding service unavailable (%v); vector retrieval disabled, lexical search only when ACTIVE_EMBEDDING_VERSION=v2", err)
		embeddingSvc = nil
	} else {
		embedder = embeddingSvc
		log.Printf("embedding: model=%s location=%s active_version=%s", embeddingSvc.Model(), cfg.GenAILocation, cfg.ActiveVersion())
	}
	spannerSvc, err := services.NewSpannerService(ctx, cfg, embedder)
	if err != nil {
		return nil, err
	}
	var cache *services.EmbeddingCache
	if embeddingSvc != nil {
		var store services.CacheStore
		if client := spannerSvc.Client(); client != nil {
			store = services.SpannerCacheStore{Client: client}
		}
		cache = services.NewEmbeddingCache(embeddingSvc, store, embeddingSvc.Model(), cfg.GeminiEmbeddingDimension, 1000)
		spannerSvc.SetQueryEmbedder(cache)
	}
	var synStore services.SynonymStore = services.NewMemorySynonymStore()
	if client := spannerSvc.Client(); client != nil {
		synStore = services.SpannerSynonymStore{Client: client}
	}
	synonyms := services.NewSynonymService(synStore, time.Minute)
	spannerSvc.SetSynonyms(synonyms)
	var ruleStore services.RuleStore = services.NewMemoryRuleStore()
	if client := spannerSvc.Client(); client != nil {
		ruleStore = services.SpannerRuleStore{Client: client}
	}
	rules := services.NewRuleService(ruleStore, 30*time.Second)
	spannerSvc.SetRules(rules)
	var intentLLM services.IntentLLM
	var intentStore services.IntentStore
	if cfg.IntentLLMFallback {
		if llm, err := services.NewGeminiIntentLLM(ctx, cfg, cfg.IntentLLMModel); err != nil {
			log.Printf("WARN: intent LLM fallback disabled: %v", err)
		} else {
			intentLLM = llm
		}
		if client := spannerSvc.Client(); client != nil {
			intentStore = services.SpannerIntentStore{Client: client}
		}
	}
	vocabCtx, vocabCancel := context.WithTimeout(ctx, 5*time.Second)
	vocab := spannerSvc.CatalogVocabulary(vocabCtx)
	vocabCancel()
	intent := services.NewQueryUnderstandingService(vocab, intentLLM, intentStore)
	spell := services.NewSpellCheckService(spannerSvc.Client())
	spell.AddVocabulary(vocab, services.DefaultSuggestions(services.DefaultSeedProducts()))
	for _, syn := range services.DefaultSynonyms() {
		spell.AddVocabulary([]models.SearchResult{{Title: syn.Term + " " + strings.Join(syn.Synonyms, " ")}}, nil)
	}
	var sessionStore services.SessionStore = services.NewMemorySessionStore()
	if client := spannerSvc.Client(); client != nil {
		sessionStore = services.SpannerSessionStore{Client: client}
	}
	session := services.NewSessionService(sessionStore, spannerSvc, cfg.SessionBoostWeight)
	ltrModel := services.DefaultLTRModel()
	if cfg.LTRModelJSON != "" {
		if m, err := services.ParseLTRModel(cfg.LTRModelJSON); err != nil {
			log.Printf("WARN: invalid LTR_MODEL_JSON (%v); using baseline model", err)
		} else {
			ltrModel = m
		}
	}
	autocomplete := services.NewAutocompleteService(spannerSvc.Client(), services.DefaultSeedProducts(), 30*time.Second)
	return &Controller{
		config:       cfg,
		spannerSvc:   spannerSvc,
		embeddingSvc: embeddingSvc,
		cache:        cache,
		synonyms:     synonyms,
		rules:        rules,
		autocomplete: autocomplete,
		ltrModel:     ltrModel,
		intent:       intent,
		spell:        spell,
		session:      session,
	}, nil
}

func (c *Controller) HealthCheck(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{
		"status":       "healthy",
		"service":      "psearch-serving",
		"backend_mode": c.spannerSvc.BackendMode(),
	})
}

func (c *Controller) Info(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{
		"service":                  "psearch-serving",
		"repo":                     "GoogleCloudPlatform/psearch",
		"project_id":               c.config.ProjectID,
		"spanner_instance":         c.config.SpannerInstanceID,
		"spanner_database":         c.config.SpannerDatabaseID,
		"backend_mode":             c.spannerSvc.BackendMode(),
		"product_count":            c.spannerSvc.ProductCount(),
		"active_embedding_version": c.config.ActiveVersion(),
		"embedding_dimension":      c.config.ActiveDimension(),
		"vector_dimensions":        c.config.ActiveDimension(),
		"embedding_model":          c.config.EmbeddingModelName,
		"text_model":               c.config.IntentLLMModel,
		"pro_model":                c.config.ProLLMModel,
		"genai_location":           c.config.GenAILocation,
		"embedding_cache":          c.cacheStats(),
		"default_alpha":            c.config.DefaultAlpha,
	})
}

func (c *Controller) Search(ctx *gin.Context) {
	start := time.Now()
	var req models.SearchRequest

	if ctx.Request.Method == http.MethodGet {
		req.Query = ctx.Query("q")
		if req.Query == "" {
			req.Query = ctx.Query("query")
		}
		if v := ctx.Query("alpha"); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				req.Alpha = &f
			}
		}
		if v := ctx.Query("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				req.Limit = &n
			}
		}
		req.Category = ctx.Query("category")
		req.Brand = ctx.Query("brand")
		req.Locale = ctx.Query("locale")
		req.TenantID = ctx.Query("tenant_id")
		req.LTR = ctx.Query("ltr") == "true" || ctx.Query("ltr") == "1"
		if v := ctx.Query("parse_intent"); v != "" {
			b := v == "true" || v == "1"
			req.ParseIntent = &b
		}
		req.PriceTable = ctx.Query("price_table_id")
		req.SortBy = ctx.Query("sort_by")
		req.SessionID = ctx.Query("session_id")
		if v := ctx.Query("session_product_ids"); v != "" {
			req.SessionProductIDs = strings.Split(v, ",")
		}
		if v, err := strconv.ParseFloat(ctx.Query("min_price"), 64); err == nil {
			req.MinPrice = v
		}
		if v, err := strconv.ParseFloat(ctx.Query("max_price"), 64); err == nil {
			req.MaxPrice = v
		}
		if v := ctx.Query("max_sla_hours"); v != "" {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				req.MaxSLAHours = n
			}
		}
		req.Channel = ctx.Query("channel")
		req.CEP = ctx.Query("cep")
		if v := ctx.Query("only_in_stock"); v == "true" || v == "1" {
			req.OnlyInStock = true
		}
	} else {
		if err := ctx.ShouldBindJSON(&req); err != nil {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}

	limit := c.config.DefaultLimit
	if req.Limit != nil && *req.Limit > 0 {
		limit = *req.Limit
	}
	minScore := c.config.MinScoreValue
	if req.MinScore != nil {
		minScore = *req.MinScore
	}
	alpha := c.config.DefaultAlpha
	if req.Alpha != nil {
		alpha = *req.Alpha
	}
	if alpha < 0 {
		alpha = 0
	} else if alpha > 1 {
		alpha = 1
	}

	zip5, cepErr := services.NormalizeCEP(req.CEP)
	if cepErr != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": cepErr.Error()})
		return
	}
	if req.MaxSLAHours < 0 || req.MaxSLAHours > 24*60 {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "max_sla_hours must be between 0 and 1440"})
		return
	}
	if req.MinPrice < 0 || req.MaxPrice < 0 || (req.MaxPrice > 0 && req.MinPrice > req.MaxPrice) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "min_price/max_price must be >= 0 and min_price <= max_price"})
		return
	}
	req.PriceTable, req.SortBy = strings.TrimSpace(req.PriceTable), strings.TrimSpace(req.SortBy)
	if err := services.ValidatePricing(req.PriceTable, req.SortBy); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	filters := models.SearchFilters{
		PriceTable:  req.PriceTable,
		SortBy:      req.SortBy,
		MinPrice:    req.MinPrice,
		MaxPrice:    req.MaxPrice,
		Zip5:        zip5,
		MaxSLAHours: req.MaxSLAHours,
		OnlyInStock: req.OnlyInStock,
		Channel:     strings.ToLower(strings.TrimSpace(req.Channel)),
		Category:    req.Category,
		Brand:       req.Brand,
		TenantID:    req.TenantID,
		Locale:      req.Locale,
	}
	// decompose natural-language queries into a clean query
	// plus structured filters; explicit request filters win.
	var parsed *models.ParsedQuery
	searchQuery := req.Query
	// Enabled by default; parse_intent=false opts out. When nothing is extracted the
	// query is left untouched and parsed_query is omitted (unless explicitly requested).
	explicitIntent := req.ParseIntent != nil && *req.ParseIntent
	if (req.ParseIntent == nil || *req.ParseIntent) && strings.TrimSpace(req.Query) != "" {
		in, source := c.intent.Understand(ctx.Request.Context(), filters.TenantID, req.Query)
		if !in.HasFilters() && !explicitIntent {
			goto intentDone
		}
		filters = services.ApplyIntent(filters, in)
		searchQuery = in.CleanQuery
		extracted := map[string]interface{}{}
		if in.MinPrice > 0 {
			extracted["min_price"] = in.MinPrice
		}
		if in.MaxPrice > 0 {
			extracted["max_price"] = in.MaxPrice
		}
		if in.Brand != "" {
			extracted["brand"] = in.Brand
		}
		if in.Category != "" {
			extracted["category"] = in.Category
		}
		if in.Color != "" {
			extracted["color"] = in.Color
		}
		parsed = &models.ParsedQuery{OriginalQuery: req.Query, CleanQuery: in.CleanQuery, ExtractedFilters: extracted, IntentSource: source}
	}
intentDone:
	// Facets are aggregated over a wider candidate set (top facetCandidates) than
	// the page returned to the client.
	const facetCandidates = 100
	candidateLimit := limit
	if candidateLimit < facetCandidates {
		candidateLimit = facetCandidates
	}
	outcome, err := c.spannerSvc.HybridSearchDetailed(ctx.Request.Context(), searchQuery, candidateLimit, minScore, alpha, filters)
	// zero-results recovery when the lexical leg found nothing.
	var didYouMean string
	var autoCorrected, relaxed bool
	if err == nil && strings.TrimSpace(searchQuery) != "" && services.LexicalHits(outcome.Results) == 0 {
		if corrected, changed := c.spell.Correct(ctx.Request.Context(), filters.TenantID, searchQuery); changed {
			didYouMean = corrected
			// The corrected token may be an entity that is only indexed as a structured
			// column (e.g. brand "samsung"), not in title/description tokens. Re-parse
			// the corrected query so such entities become filters.
			altQuery, altFilters := corrected, filters
			entity := false
			if in := c.intent.Parse(corrected); in.Brand != "" || in.Category != "" {
				altQuery, altFilters, entity = in.CleanQuery, services.ApplyIntent(filters, in), true
			}
			if alt, aerr := c.spannerSvc.HybridSearchDetailed(ctx.Request.Context(), altQuery, candidateLimit, minScore, alpha, altFilters); aerr == nil &&
				(services.LexicalHits(alt.Results) > 0 || (entity && len(alt.Results) > 0)) {
				outcome, searchQuery, filters, autoCorrected = alt, altQuery, altFilters, true
			}
		}
		if !autoCorrected && len(strings.Fields(searchQuery)) > 1 {
			rf := filters
			rf.RelaxText = true
			if alt, aerr := c.spannerSvc.HybridSearchDetailed(ctx.Request.Context(), searchQuery, candidateLimit, minScore, alpha, rf); aerr == nil && services.LexicalHits(alt.Results) > 0 {
				outcome, relaxed = alt, true
			}
		}
	}
	results := outcome.Results
	if results == nil {
		results = []models.SearchResult{} // always serialize as [] (never null)
	}
	if err != nil {
		ctx.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if outcome.RetrievalMode == services.RetrievalLexicalOnly {
		alpha = 0 // vector leg was not executed
	}
	ids := make([]string, 0, len(results))
	for _, r := range results {
		ids = append(ids, r.ID)
	}
	facetStart := time.Now()
	facets, ferr := c.spannerSvc.Facets(ctx.Request.Context(), ids, filters.PriceTable, filters.TenantID)
	facetsMs := float64(time.Since(facetStart).Microseconds()) / 1000.0
	if ferr != nil {
		log.Printf("WARN: facets unavailable: %v", ferr)
		facets, facetsMs = nil, 0
	}
	candidates := len(results)
	// Delivery promises are attached to the whole candidate set when LTR needs the
	// sla_hours feature; otherwise only to the returned page.
	if !req.LTR && len(results) > limit {
		results = results[:limit]
	}
	if zip5 != "" && len(results) > 0 {
		pageIDs := make([]string, 0, len(results))
		for _, r := range results {
			pageIDs = append(pageIDs, r.ID)
		}
		if promises, err := c.spannerSvc.DeliveryPromises(ctx.Request.Context(), pageIDs, zip5, req.MaxSLAHours); err != nil {
			log.Printf("WARN: delivery promises unavailable: %v", err)
		} else {
			for i := range results {
				if p, ok := promises[results[i].ID]; ok {
					p.CEP = req.CEP
					results[i].DeliveryPromise = p
				}
			}
		}
	}
	var ltrRes services.LTRResult
	priceSorted := req.SortBy == "price_asc" || req.SortBy == "price_desc"
	if req.LTR && len(results) > 0 && !priceSorted { // an explicit price sort wins over LTR
		// in-memory re-ranking of the stage-1 top-100 candidates.
		signals, err := c.spannerSvc.CandidateSignals(ctx.Request.Context(), ids)
		if err != nil {
			log.Printf("WARN: ltr signals unavailable (re-ranking on relevance features only): %v", err)
		}
		ltrRes = services.Rerank(c.ltrModel, results, signals, outcome.Pins, outcome.Buries)
	}
	// session-aware re-ranking by the session centroid.
	personalized := false
	if req.SessionID != "" || len(req.SessionProductIDs) > 0 {
		var events []services.SessionEvent
		if req.SessionID != "" && services.ValidSessionID(req.SessionID) {
			if ev, err := c.session.Recent(ctx.Request.Context(), filters.TenantID, req.SessionID); err != nil {
				log.Printf("WARN: session events unavailable: %v", err)
			} else {
				events = ev
			}
		}
		for i, id := range req.SessionProductIDs {
			if i >= 20 {
				break
			}
			if id = strings.TrimSpace(id); id != "" {
				events = append(events, services.SessionEvent{ProductID: id, EventType: "click"})
			}
		}
		personalized = c.session.Personalize(ctx.Request.Context(), filters.TenantID, events, results, outcome.Pins, outcome.Buries, req.SortBy, ltrRes.ModelVersion != "")
	}
	if len(results) > limit {
		results = results[:limit]
	}
	tenant := strings.TrimSpace(req.TenantID)
	if tenant == "" {
		tenant = "default"
	}

	ctx.JSON(http.StatusOK, models.SearchResponse{
		Query:            req.Query,
		EffectiveAlpha:   alpha,
		Results:          results,
		TotalFound:       len(results),
		Candidates:       candidates,
		Facets:           facets,
		FacetsLatencyMs:  facetsMs,
		TenantID:         tenant,
		Locale:           strings.TrimSpace(req.Locale),
		LatencyMs:        float64(time.Since(start).Microseconds()) / 1000.0,
		BackendMode:      c.spannerSvc.BackendMode(),
		EmbeddingVersion: c.config.ActiveVersion(),
		CacheHit:         outcome.CacheHit,
		ExpandedTerms:    outcome.ExpandedTerms,
		AppliedRules:     outcome.AppliedRules,
		LTR:              req.LTR && ltrRes.ModelVersion != "",
		LTRLatencyUS:     ltrRes.LatencyUS,
		LTRModel:         ltrRes.ModelVersion,
		ParsedQuery:      parsed,
		DidYouMean:       didYouMean,
		AutoCorrected:    autoCorrected,
		QueryRelaxed:     relaxed,
		PriceTableID:     req.PriceTable,
		SortBy:           req.SortBy,
		SessionID:        req.SessionID,
		Personalized:     personalized,
		RetrievalMode:    outcome.RetrievalMode,
	})
}

func (c *Controller) cacheStats() interface{} {
	if c.cache == nil {
		return gin.H{"enabled": false}
	}
	st := c.cache.Stats()
	return gin.H{"enabled": true, "memory_hits": st.MemoryHits, "spanner_hits": st.SpannerHits,
		"misses": st.Misses, "entries": st.Entries, "max_entries": st.MaxEntries}
}

// GetEmbeddingVersion: GET /api/config/embedding-version.
func (c *Controller) GetEmbeddingVersion(ctx *gin.Context) {
	ctx.JSON(http.StatusOK, gin.H{
		"active_embedding_version": c.config.ActiveVersion(),
		"vector_dimensions":        c.config.ActiveDimension(),
		"available":                []string{"v1", "v2"},
		"scope":                    "instance",
	})
}

// SetEmbeddingVersion: POST /api/config/embedding-version {"version":"v1"|"v2"}.
// Requires X-Admin-Token == ADMIN_TOKEN; disabled (403) when ADMIN_TOKEN is unset.
// The switch is per Cloud Run instance; set ACTIVE_EMBEDDING_VERSION for a fleet-wide change.
func (c *Controller) SetEmbeddingVersion(ctx *gin.Context) {
	if !c.authorizeAdmin(ctx) {
		return
	}
	var body struct {
		Version string `json:"version"`
	}
	if err := ctx.ShouldBindJSON(&body); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := c.config.SetActiveVersion(body.Version); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	log.Printf("config: active embedding version switched to %s (this instance)", body.Version)
	c.GetEmbeddingVersion(ctx)
}

// authorizeAdmin enforces X-Admin-Token == ADMIN_TOKEN (constant-time); writes 403 otherwise.
// Admin writes are disabled entirely when ADMIN_TOKEN is unset.
func (c *Controller) authorizeAdmin(ctx *gin.Context) bool {
	token := ctx.GetHeader("X-Admin-Token")
	if c.config.AdminToken == "" || subtle.ConstantTimeCompare([]byte(token), []byte(c.config.AdminToken)) != 1 {
		ctx.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
		return false
	}
	return true
}

// ListSynonyms: GET /api/synonyms?tenant_id=default&locale=pt-BR.
func (c *Controller) ListSynonyms(ctx *gin.Context) {
	tenant := strings.TrimSpace(ctx.Query("tenant_id"))
	if tenant == "" {
		tenant = "default"
	}
	rows, err := c.synonyms.List(ctx.Request.Context(), tenant, strings.TrimSpace(ctx.Query("locale")))
	if err != nil {
		log.Printf("WARN: list synonyms: %v", err)
		ctx.JSON(http.StatusServiceUnavailable, gin.H{"error": "synonyms unavailable"})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"tenant_id": tenant, "synonyms": rows, "count": len(rows)})
}

// UpsertSynonym: POST /api/synonyms {tenant_id, locale, term, synonyms[], weight} (admin only).
func (c *Controller) UpsertSynonym(ctx *gin.Context) {
	if !c.authorizeAdmin(ctx) {
		return
	}
	var body services.Synonym
	if err := ctx.ShouldBindJSON(&body); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	saved, err := c.synonyms.Upsert(ctx.Request.Context(), body)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ctx.JSON(http.StatusOK, saved)
}

func tenantParam(ctx *gin.Context) string {
	if t := strings.TrimSpace(ctx.Query("tenant_id")); t != "" {
		return t
	}
	return "default"
}

// ListRules: GET /api/rules?tenant_id=default. Public read.
func (c *Controller) ListRules(ctx *gin.Context) {
	tenant := tenantParam(ctx)
	rules, err := c.rules.List(ctx.Request.Context(), tenant)
	if err != nil {
		log.Printf("WARN: list rules: %v", err)
		ctx.JSON(http.StatusServiceUnavailable, gin.H{"error": "rules unavailable"})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"tenant_id": tenant, "rules": rules, "count": len(rules)})
}

// CreateRule: POST /api/rules (admin). PUT /api/rules/:id (admin) replaces a rule.
func (c *Controller) CreateRule(ctx *gin.Context) { c.saveRule(ctx, "") }

func (c *Controller) UpdateRule(ctx *gin.Context) { c.saveRule(ctx, ctx.Param("id")) }

func (c *Controller) saveRule(ctx *gin.Context, id string) {
	if !c.authorizeAdmin(ctx) {
		return
	}
	var r services.Rule
	if err := ctx.ShouldBindJSON(&r); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if id != "" {
		r.RuleID = id
	} else {
		r.RuleID = "" // server-assigned on create
	}
	if r.TenantID == "" {
		r.TenantID = tenantParam(ctx)
	}
	saved, err := c.rules.Save(ctx.Request.Context(), r)
	if err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	status := http.StatusOK
	if id == "" {
		status = http.StatusCreated
	}
	ctx.JSON(status, saved)
}

// DeleteRule: DELETE /api/rules/:id?tenant_id=default (admin).
func (c *Controller) DeleteRule(ctx *gin.Context) {
	if !c.authorizeAdmin(ctx) {
		return
	}
	if err := c.rules.Delete(ctx.Request.Context(), tenantParam(ctx), ctx.Param("id")); err != nil {
		log.Printf("WARN: delete rule: %v", err)
		ctx.JSON(http.StatusServiceUnavailable, gin.H{"error": "delete failed"})
		return
	}
	ctx.Status(http.StatusNoContent)
}

// Autocomplete: GET /api/autocomplete?q=<prefix>&locale=&tenant_id=&limit=.
func (c *Controller) Autocomplete(ctx *gin.Context) {
	start := time.Now()
	q := ctx.Query("q")
	if len(q) > 100 {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "q too long (max 100)"})
		return
	}
	limit := 8
	if v, err := strconv.Atoi(ctx.Query("limit")); err == nil && v > 0 {
		limit = v
	}
	if limit > 20 {
		limit = 20
	}
	sugg, cached, err := c.autocomplete.Suggest(ctx.Request.Context(), tenantParam(ctx), strings.TrimSpace(ctx.Query("locale")), q, limit)
	if err != nil {
		log.Printf("WARN: autocomplete: %v", err)
		sugg = []services.Suggestion{} // degrade to no suggestions, never 5xx on typeahead
	}
	ctx.JSON(http.StatusOK, gin.H{
		"query":       q,
		"suggestions": sugg,
		"cached":      cached,
		"latency_ms":  float64(time.Since(start).Microseconds()) / 1000.0,
	})
}

// RecordSessionEvent: POST /api/session/events {session_id, product_id, event_type, tenant_id?}.
func (c *Controller) RecordSessionEvent(ctx *gin.Context) {
	var e services.SessionEvent
	if err := ctx.ShouldBindJSON(&e); err != nil {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if e.TenantID == "" {
		e.TenantID = tenantParam(ctx)
	}
	saved, err := c.session.Record(ctx.Request.Context(), e)
	if err != nil {
		if strings.Contains(err.Error(), "must") || strings.Contains(err.Error(), "required") {
			ctx.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		} else {
			log.Printf("WARN: record session event: %v", err)
			ctx.JSON(http.StatusServiceUnavailable, gin.H{"error": "event not recorded"})
		}
		return
	}
	ctx.JSON(http.StatusCreated, saved)
}

// ListSessionEvents: GET /api/session/events?session_id=... (newest first, last 20).
func (c *Controller) ListSessionEvents(ctx *gin.Context) {
	sid := ctx.Query("session_id")
	if !services.ValidSessionID(sid) {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "invalid session_id"})
		return
	}
	events, err := c.session.Recent(ctx.Request.Context(), tenantParam(ctx), sid)
	if err != nil {
		ctx.JSON(http.StatusServiceUnavailable, gin.H{"error": "session unavailable"})
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"session_id": sid, "events": events, "count": len(events)})
}

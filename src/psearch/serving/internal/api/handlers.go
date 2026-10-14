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
	return &Controller{
		config:       cfg,
		spannerSvc:   spannerSvc,
		embeddingSvc: embeddingSvc,
		cache:        cache,
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
		"sandbox":                  c.config.ProjectID,
		"spanner_instance":         c.config.SpannerInstanceID,
		"spanner_database":         c.config.SpannerDatabaseID,
		"backend_mode":             c.spannerSvc.BackendMode(),
		"product_count":            c.spannerSvc.ProductCount(),
		"active_embedding_version": c.config.ActiveVersion(),
		"embedding_dimension":      c.config.ActiveDimension(),
		"vector_dimensions":        c.config.ActiveDimension(),
		"embedding_cache":          c.cacheStats(),
		"default_alpha":            c.config.DefaultAlpha,
		"orchestrator":             "lead",
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

	outcome, err := c.spannerSvc.HybridSearchDetailed(ctx.Request.Context(), req.Query, limit, minScore, alpha, models.SearchFilters{OnlyInStock: req.OnlyInStock, Channel: strings.ToLower(strings.TrimSpace(req.Channel))})
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

	ctx.JSON(http.StatusOK, models.SearchResponse{
		Query:            req.Query,
		EffectiveAlpha:   alpha,
		Results:          results,
		TotalFound:       len(results),
		LatencyMs:        float64(time.Since(start).Microseconds()) / 1000.0,
		BackendMode:      c.spannerSvc.BackendMode(),
		EmbeddingVersion: c.config.ActiveVersion(),
		CacheHit:         outcome.CacheHit,
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

// GetEmbeddingVersion: GET /api/config/embedding-version (ADR-001 Blue/Green).
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
	token := ctx.GetHeader("X-Admin-Token")
	if c.config.AdminToken == "" || subtle.ConstantTimeCompare([]byte(token), []byte(c.config.AdminToken)) != 1 {
		ctx.JSON(http.StatusForbidden, gin.H{"error": "forbidden"})
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

package api

import (
	"context"
	"log"
	"net/http"
	"strconv"
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
		log.Printf("embedding: model=%s location=%s active_version=%s", embeddingSvc.Model(), cfg.GenAILocation, cfg.ActiveEmbeddingVersion)
	}
	spannerSvc, err := services.NewSpannerService(ctx, cfg, embedder)
	if err != nil {
		return nil, err
	}
	return &Controller{
		config:       cfg,
		spannerSvc:   spannerSvc,
		embeddingSvc: embeddingSvc,
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
		"active_embedding_version": c.config.ActiveEmbeddingVersion,
		"embedding_dimension":      c.config.EmbeddingDimension,
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

	outcome, err := c.spannerSvc.HybridSearchDetailed(ctx.Request.Context(), req.Query, limit, minScore, alpha)
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
		EmbeddingVersion: c.config.ActiveEmbeddingVersion,
		RetrievalMode:    outcome.RetrievalMode,
	})
}

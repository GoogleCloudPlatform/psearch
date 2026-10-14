package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"psearch/serving-go/internal/config"
	"psearch/serving-go/internal/models"
)

func newTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	cfg := &config.Config{
		Port:                   8080,
		Environment:            "test",
		ProjectID:              "psearch-project",
		Region:                 "us-central1",
		SpannerInstanceID:      "psearch-instance",
		SpannerDatabaseID:      "psearch-db",
		EmbeddingDimension:     768,
		ActiveEmbeddingVersion: "v1",
		DefaultAlpha:           0.5,
		DefaultLimit:           10,
		UseMemoryStore:         true,
	}
	SetupRouter(r, cfg)
	return r
}

func TestHealthAndInfoEndpoints(t *testing.T) {
	r := newTestRouter()

	for _, path := range []string{"/health", "/api/health", "/api/info"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", path, w.Code)
		}
	}
}

func TestHybridSearchAlphaParameterization(t *testing.T) {
	r := newTestRouter()

	req := httptest.NewRequest(http.MethodGet, "/api/search?q=running+shoes&alpha=0.8&limit=5", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /api/search status = %d, body = %s", w.Code, w.Body.String())
	}

	var resp models.SearchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal SearchResponse: %v", err)
	}
	if resp.EffectiveAlpha != 0.8 {
		t.Errorf("effective_alpha = %v, want 0.8", resp.EffectiveAlpha)
	}
	if len(resp.Results) == 0 {
		t.Fatalf("expected non-empty search results")
	}
}

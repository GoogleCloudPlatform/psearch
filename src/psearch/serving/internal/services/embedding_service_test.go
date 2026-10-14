package services

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/genai"
	"psearch/serving-go/internal/config"
	"psearch/serving-go/internal/models"
)

func testConfig(version string) *config.Config {
	return &config.Config{
		ProjectID:                "test-project",
		GenAILocation:            "global",
		EmbeddingModelName:       "gemini-embedding-2",
		GeminiEmbeddingDimension: 1024,
		ActiveEmbeddingVersion:   version,
	}
}

func fakeEmbed(dim int, err error, captured *genai.EmbedContentConfig, model *string) embedContentFunc {
	return func(_ context.Context, m string, _ []*genai.Content, cfg *genai.EmbedContentConfig) (*genai.EmbedContentResponse, error) {
		if captured != nil && cfg != nil {
			*captured = *cfg
		}
		if model != nil {
			*model = m
		}
		if err != nil {
			return nil, err
		}
		return &genai.EmbedContentResponse{Embeddings: []*genai.ContentEmbedding{{Values: make([]float32, dim)}}}, nil
	}
}

func TestEmbedQuery_RequestsConfiguredModelDimensionAndTaskType(t *testing.T) {
	var got genai.EmbedContentConfig
	var model string
	svc := newEmbeddingServiceWith(testConfig("v2"), fakeEmbed(1024, nil, &got, &model))

	vec, err := svc.EmbedQuery(context.Background(), "tênis de corrida")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(vec) != 1024 {
		t.Fatalf("want 1024 dims, got %d", len(vec))
	}
	if model != "gemini-embedding-2" {
		t.Errorf("want model gemini-embedding-2, got %q", model)
	}
	if got.TaskType != "RETRIEVAL_QUERY" {
		t.Errorf("want task type RETRIEVAL_QUERY, got %q", got.TaskType)
	}
	if got.OutputDimensionality == nil || *got.OutputDimensionality != 1024 {
		t.Errorf("want OutputDimensionality=1024, got %v", got.OutputDimensionality)
	}
}

func TestEmbedQuery_RejectsDimensionMismatchAndErrors(t *testing.T) {
	svc := newEmbeddingServiceWith(testConfig("v2"), fakeEmbed(3072, nil, nil, nil))
	if _, err := svc.EmbedQuery(context.Background(), "q"); err == nil {
		t.Fatal("expected dimension mismatch error")
	}
	svc = newEmbeddingServiceWith(testConfig("v2"), fakeEmbed(0, errors.New("403 PERMISSION_DENIED"), nil, nil))
	if _, err := svc.EmbedQuery(context.Background(), "q"); err == nil {
		t.Fatal("expected upstream error to propagate")
	}
}

type stubEmbedder struct {
	vec []float32
	err error
}

func (s stubEmbedder) EmbedQuery(context.Context, string) ([]float32, error) { return s.vec, s.err }

func TestPlanVectorQuery_V1KeepsLegacyDeterministic768(t *testing.T) {
	svc := &SpannerService{config: testConfig("v1"), embeddings: stubEmbedder{err: errors.New("must not be called")}}
	plan := svc.planVectorQuery(context.Background(), "tenis")
	if plan.lexicalOnly || plan.column != vectorColumnV1 {
		t.Fatalf("v1 plan wrong: %+v", plan)
	}
	if v, ok := plan.embedding.([]float64); !ok || len(v) != config.LegacyEmbeddingDimension {
		t.Fatalf("v1 must use 768-d deterministic embedding, got %T", plan.embedding)
	}
}

func TestPlanVectorQuery_V2UsesGemini(t *testing.T) {
	svc := &SpannerService{config: testConfig("v2"), embeddings: stubEmbedder{vec: make([]float32, 1024)}}
	plan := svc.planVectorQuery(context.Background(), "tenis")
	if plan.lexicalOnly || plan.column != vectorColumnV2 {
		t.Fatalf("v2 plan wrong: %+v", plan)
	}
	if v, ok := plan.embedding.([]float32); !ok || len(v) != 1024 {
		t.Fatalf("v2 must use 1024-d Gemini embedding, got %T", plan.embedding)
	}
}

func TestPlanVectorQuery_V2DegradesToLexicalOnly(t *testing.T) {
	for name, emb := range map[string]QueryEmbedder{
		"nil service":   nil,
		"embed failure": stubEmbedder{err: errors.New("403 PERMISSION_DENIED")},
	} {
		svc := &SpannerService{config: testConfig("v2"), embeddings: emb}
		if plan := svc.planVectorQuery(context.Background(), "tenis"); !plan.lexicalOnly {
			t.Errorf("%s: expected lexical-only fallback, got %+v", name, plan)
		}
	}
}

func TestBuildHybridSQL(t *testing.T) {
	v2 := buildHybridSQL(vectorColumnV2, false)
	if !strings.Contains(v2, "COSINE_DISTANCE(embedding_v2, @query_embedding)") {
		t.Error("v2 SQL must search embedding_v2")
	}
	lex := buildHybridSQL(vectorColumnV2, true)
	if strings.Contains(lex, "@query_embedding") || strings.Contains(lex, "ann AS") {
		t.Error("lexical-only SQL must not reference the vector leg")
	}
	if !strings.Contains(lex, "fts AS") {
		t.Error("lexical-only SQL must keep the full-text leg")
	}
}

func TestConfigActiveDimension(t *testing.T) {
	if d := testConfig("v1").ActiveDimension(); d != 768 {
		t.Errorf("v1 active dimension want 768, got %d", d)
	}
	if d := testConfig("v2").ActiveDimension(); d != 1024 {
		t.Errorf("v2 active dimension want 1024, got %d", d)
	}
}

func TestHybridSearchDetailed_ReportsMemoryFallbackMode(t *testing.T) {
	svc := &SpannerService{config: testConfig("v2"), seedProducts: DefaultSeedProducts()}
	out, err := svc.HybridSearchDetailed(context.Background(), "tenis", 5, 0, 0.5, models.SearchFilters{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.RetrievalMode != RetrievalMemory {
		t.Fatalf("want retrieval mode %q without Spanner, got %q", RetrievalMemory, out.RetrievalMode)
	}
	if len(out.Results) == 0 {
		t.Fatal("memory fallback should return seed results")
	}
}

// catalog filters must be applied inside BOTH legs, and the row
// projection must stay at the 8 columns scanned by spannerHybridSearch.
func TestBuildHybridSQL_CatalogFiltersAndUnavailableLast(t *testing.T) {
	for _, lexical := range []bool{false, true} {
		sql := buildHybridSQL(vectorColumnV2, lexical)
		wantLegs := 2
		if lexical {
			wantLegs = 1
		}
		if n := strings.Count(sql, "COALESCE(is_visible, TRUE)"); n != wantLegs {
			t.Errorf("lexical=%v: visibility filter appears %d times, want %d", lexical, n, wantLegs)
		}
		for _, frag := range []string{"@channel", "@only_available", "unavailable_rank", "ANY_VALUE(is_available) AS is_available", "ANY_VALUE(stock) AS stock"} {
			if !strings.Contains(sql, frag) {
				t.Errorf("lexical=%v: SQL missing %q", lexical, frag)
			}
		}
	}
}

func TestMemorySearchFilters(t *testing.T) {
	svc := &SpannerService{config: testConfig("v1"), seedProducts: DefaultSeedProducts()}
	out, _ := svc.HybridSearchDetailed(context.Background(), "refrigerator", 15, 0, 0.5, models.SearchFilters{Channel: "store", OnlyInStock: true})
	for _, r := range out.Results {
		if isUnavailable(r.Availability, r.AvailableQuantity) || !matchesChannel("store", r.Channels) {
			t.Fatalf("filtered result %s violates filters (avail=%s channels=%v)", r.ID, r.Availability, r.Channels)
		}
	}
}

func emptyFilters() models.SearchFilters { return models.SearchFilters{} }

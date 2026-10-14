package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/joho/godotenv"
)

type Config struct {
	Port                     int
	Environment              string
	ProjectID                string
	Region                   string
	SpannerInstanceID        string
	SpannerDatabaseID        string
	GenAILocation            string // Vertex AI location for Gen AI SDK calls (Gemini 3.x / gemini-embedding-2 => "global")
	EmbeddingModelName       string // Gemini embedding model used for the v2 vector column
	GeminiEmbeddingDimension int    // output_dimensionality requested from the embedding model (ADR-001: 1024)
	EmbeddingDimension       int    // dimension of the ACTIVE vector column (v1=768 legacy, v2=GeminiEmbeddingDimension)
	ActiveEmbeddingVersion   string // initial version from env; use ActiveVersion() at runtime
	LTRModelJSON             string // optional learned LTR model (BigQuery ML export); empty = baseline
	IntentLLMFallback        bool   // ADR-007: Gemini Flash Lite fallback for unparsed conversational queries
	IntentLLMModel           string // Issue #23: text/intent model ("gemini-3.8-flash")
	ProLLMModel              string // Issue #23: pro reasoning model ("gemini-4-argon")
	SessionBoostWeight       float64 // ADR-007 (#20): weight of the session centroid similarity boost
	BoostWeight              float64 // scale of the 8-signal boost added to RRF (ADR-004); 0 disables
	TextFolding              bool    // use accent-folded tokenlists (migration 014b) so "tenis" matches "Tênis" (Issue #4)
	AdminToken               string  // guards POST /api/config/embedding-version (empty = disabled)
	activeVersion            atomic.Pointer[string]
	DefaultAlpha             float64
	DefaultLimit             int
	MinScoreValue            float64
	UseMemoryStore           bool
	StaticDir                string
}

// NormalizeModelID normalizes model aliases (e.g. gemini-4-pro / gemini-4.0-pro -> gemini-4-argon).
func NormalizeModelID(model string) string {
	switch strings.ToLower(strings.TrimSpace(model)) {
	case "gemini-4-pro", "gemini-4.0-pro":
		return "gemini-4-argon"
	default:
		return model
	}
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{
		Port:                     8080,
		Environment:              getEnv("ENVIRONMENT", "production"),
		ProjectID:                getEnv("PROJECT_ID", getEnv("GCP_SANDBOX_PROJECT", getEnv("CLOUDSDK_CORE_PROJECT", "psearch-sandbox"))),
		Region:                   getEnv("REGION", "us-central1"),
		SpannerInstanceID:        getEnv("SPANNER_INSTANCE_ID", "psearch-instance"),
		SpannerDatabaseID:        getEnv("SPANNER_DATABASE_ID", "psearch-db"),
		GenAILocation:            getEnv("GENAI_LOCATION", "global"),
		EmbeddingModelName:       getEnv("EMBEDDING_MODEL_NAME", getEnv("GEMINI_MODEL_NAME", "gemini-embedding-2")),
		GeminiEmbeddingDimension: 1024,
		ActiveEmbeddingVersion:   getEnv("ACTIVE_EMBEDDING_VERSION", "v1"),
		AdminToken:               os.Getenv("ADMIN_TOKEN"),
		TextFolding:              getEnv("TEXT_FOLDING", "false") == "true",
		BoostWeight:              0.002,
		SessionBoostWeight:       0.003,
		IntentLLMFallback:        getEnv("INTENT_LLM_FALLBACK", "false") == "true",
		IntentLLMModel:           NormalizeModelID(getEnv("GEMINI_TEXT_MODEL", getEnv("GEMINI_LITE_MODEL", "gemini-3.8-flash"))),
		ProLLMModel:              NormalizeModelID(getEnv("GEMINI_PRO_MODEL", "gemini-4-argon")),
		LTRModelJSON:             os.Getenv("LTR_MODEL_JSON"),
		DefaultAlpha:             0.5,
		DefaultLimit:             20,
		MinScoreValue:            0.0,
		UseMemoryStore:           getEnv("USE_MEMORY_STORE", "false") == "true",
		StaticDir:                getEnv("STATIC_DIR", "/app/static"),
	}

	if port, err := strconv.Atoi(getEnv("PORT", "8080")); err == nil {
		cfg.Port = port
	}
	if dim, err := strconv.Atoi(getEnv("EMBEDDING_DIMENSION", "1024")); err == nil && dim > 0 {
		cfg.GeminiEmbeddingDimension = dim
	}
	cfg.EmbeddingDimension = cfg.ActiveDimension()
	if w, err := strconv.ParseFloat(getEnv("SESSION_BOOST_WEIGHT", "0.003"), 64); err == nil && w >= 0 && w <= 0.05 {
		cfg.SessionBoostWeight = w
	}
	if w, err := strconv.ParseFloat(getEnv("BOOST_WEIGHT", "0.002"), 64); err == nil && w >= 0 && w <= 0.05 {
		cfg.BoostWeight = w
	}
	if alpha, err := strconv.ParseFloat(getEnv("DEFAULT_HYBRID_ALPHA", "0.5"), 64); err == nil {
		cfg.DefaultAlpha = alpha
	}
	if limit, err := strconv.Atoi(getEnv("DEFAULT_LIMIT", "20")); err == nil {
		cfg.DefaultLimit = limit
	}
	if minScore, err := strconv.ParseFloat(getEnv("MIN_SCORE_VALUE", "0.0"), 64); err == nil {
		cfg.MinScoreValue = minScore
	}

	return cfg, nil
}

// LegacyEmbeddingDimension is the dimension of the v1 `embedding` column
// (deterministic hash embeddings, ARRAY<FLOAT32>(vector_length=>768)).
const LegacyEmbeddingDimension = 768

// UseGeminiEmbeddings reports whether the v2 (Gemini, embedding_v2) vector column is active.
func (c *Config) UseGeminiEmbeddings() bool { return c.ActiveVersion() == "v2" }

// ActiveVersion returns the embedding version currently in effect. It can be
// switched at runtime (per instance) via SetActiveVersion (ADR-001 Blue/Green).
func (c *Config) ActiveVersion() string {
	if p := c.activeVersion.Load(); p != nil {
		return *p
	}
	return c.ActiveEmbeddingVersion
}

// SetActiveVersion switches the active embedding version ("v1" or "v2") for this instance.
func (c *Config) SetActiveVersion(v string) error {
	if v != "v1" && v != "v2" {
		return fmt.Errorf("invalid embedding version %q (want v1 or v2)", v)
	}
	c.activeVersion.Store(&v)
	return nil
}

// ActiveDimension returns the dimension of the currently active vector column.
func (c *Config) ActiveDimension() int {
	if c.UseGeminiEmbeddings() {
		return c.GeminiEmbeddingDimension
	}
	return LegacyEmbeddingDimension
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

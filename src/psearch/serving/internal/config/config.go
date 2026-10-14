package config

import (
	"os"
	"strconv"

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
	ActiveEmbeddingVersion   string
	DefaultAlpha             float64
	DefaultLimit             int
	MinScoreValue            float64
	UseMemoryStore           bool
	StaticDir                string
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{
		Port:                     8080,
		Environment:              getEnv("ENVIRONMENT", "production"),
		ProjectID:                getEnv("PROJECT_ID", "riojucu-sandbox"),
		Region:                   getEnv("REGION", "us-central1"),
		SpannerInstanceID:        getEnv("SPANNER_INSTANCE_ID", "psearch-instance"),
		SpannerDatabaseID:        getEnv("SPANNER_DATABASE_ID", "psearch-db"),
		GenAILocation:            getEnv("GENAI_LOCATION", "global"),
		EmbeddingModelName:       getEnv("EMBEDDING_MODEL_NAME", getEnv("GEMINI_MODEL_NAME", "gemini-embedding-2")),
		GeminiEmbeddingDimension: 1024,
		ActiveEmbeddingVersion:   getEnv("ACTIVE_EMBEDDING_VERSION", "v1"),
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
func (c *Config) UseGeminiEmbeddings() bool { return c.ActiveEmbeddingVersion == "v2" }

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

package config

import (
	"fmt"
	"log"
	"net/url"
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
	GenAILocation            string   // Vertex AI location for Gen AI SDK calls (Gemini 3.x / gemini-embedding-2 => "global")
	EmbeddingModelName       string   // Gemini embedding model used for the v2 vector column
	GeminiEmbeddingDimension int      // output_dimensionality requested from the embedding model (1024)
	EmbeddingDimension       int      // dimension of the ACTIVE vector column (v1=768 legacy, v2=GeminiEmbeddingDimension)
	ActiveEmbeddingVersion   string   // initial version from env; use ActiveVersion() at runtime
	LTRModelJSON             string   // optional learned LTR model (BigQuery ML export); empty = baseline
	IntentLLMFallback        bool     // Gemini Flash Lite fallback for unparsed conversational queries
	IntentLLMModel           string   // text/intent model ("gemini-3.8-flash")
	ProLLMModel              string   // pro reasoning model ("gemini-4-pro")
	SessionBoostWeight       float64  // weight of the session centroid similarity boost
	BoostWeight              float64  // scale of the 8-signal boost added to RRF; 0 disables
	TextFolding              bool     // use accent-folded tokenlists (migration 014b) so "tenis" matches "Tênis"
	RequireAPIAuth           bool     // enforces API authentication on non-health API routes
	APIToken                 string   // optional X-API-Key token for read/query endpoints
	AdminToken               string   // guards admin write endpoints via X-Admin-Token (empty = disabled)
	AuthAudience             string   // expected OIDC audience for Bearer token validation
	AllowedPrincipals        []string // allowlisted OIDC email principals (lowercase)
	AllowedDomains           []string // allowlisted Google Workspace hd/email domains (lowercase)
	AllowedOrigins           []string // explicit non-wildcard CORS origins
	activeVersion            atomic.Pointer[string]
	DefaultAlpha             float64
	DefaultLimit             int
	MinScoreValue            float64
	UseMemoryStore           bool
	StaticDir                string
}

// NormalizeModelID normalizes model aliases (e.g. gemini-4.0-pro -> gemini-4-pro).
func NormalizeModelID(model string) string {
	switch strings.ToLower(strings.TrimSpace(model)) {
	case "gemini-4-pro", "gemini-4.0-pro":
		return "gemini-4-pro"
	default:
		return model
	}
}

func Load() (*Config, error) {
	_ = godotenv.Load()

	cfg := &Config{
		Port:                     8080,
		Environment:              getEnv("ENVIRONMENT", "production"),
		ProjectID:                getEnv("PROJECT_ID", getEnv("GOOGLE_CLOUD_PROJECT", getEnv("CLOUDSDK_CORE_PROJECT", "psearch-project"))),
		Region:                   getEnv("REGION", "us-central1"),
		SpannerInstanceID:        getEnv("SPANNER_INSTANCE_ID", "psearch-instance"),
		SpannerDatabaseID:        getEnv("SPANNER_DATABASE_ID", "psearch-db"),
		GenAILocation:            getEnv("GENAI_LOCATION", "global"),
		EmbeddingModelName:       getEnv("EMBEDDING_MODEL_NAME", getEnv("GEMINI_MODEL_NAME", "gemini-embedding-2")),
		GeminiEmbeddingDimension: 1024,
		ActiveEmbeddingVersion:   getEnv("ACTIVE_EMBEDDING_VERSION", "v1"),
		APIToken:                 strings.TrimSpace(os.Getenv("API_TOKEN")),
		AdminToken:               strings.TrimSpace(os.Getenv("ADMIN_TOKEN")),
		AuthAudience:             strings.TrimSpace(os.Getenv("SERVING_AUTH_AUDIENCE")),
		AllowedPrincipals:        parseCSVLower(os.Getenv("SERVING_ALLOWED_PRINCIPALS")),
		AllowedDomains:           parseDomainCSV(os.Getenv("SERVING_ALLOWED_DOMAINS")),
		AllowedOrigins:           SanitizeAllowedOrigins(strings.Split(os.Getenv("ALLOWED_ORIGINS"), ",")),
		TextFolding:              getEnv("TEXT_FOLDING", "false") == "true",
		BoostWeight:              0.002,
		SessionBoostWeight:       0.003,
		IntentLLMFallback:        getEnv("INTENT_LLM_FALLBACK", "false") == "true",
		IntentLLMModel:           NormalizeModelID(getEnv("GEMINI_TEXT_MODEL", getEnv("GEMINI_LITE_MODEL", "gemini-3.8-flash"))),
		ProLLMModel:              NormalizeModelID(getEnv("GEMINI_PRO_MODEL", "gemini-4-pro")),
		LTRModelJSON:             os.Getenv("LTR_MODEL_JSON"),
		DefaultAlpha:             0.5,
		DefaultLimit:             20,
		MinScoreValue:            0.0,
		UseMemoryStore:           getEnv("USE_MEMORY_STORE", "false") == "true",
		StaticDir:                getEnv("STATIC_DIR", "/app/static"),
	}

	if raw := strings.TrimSpace(strings.ToLower(os.Getenv("REQUIRE_API_AUTH"))); raw != "" {
		cfg.RequireAPIAuth = raw == "true" || raw == "1"
	} else {
		cfg.RequireAPIAuth = (strings.EqualFold(cfg.Environment, "production") && !cfg.UseMemoryStore) || cfg.APIToken != "" || cfg.AuthAudience != ""
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
// switched at runtime (per instance) via SetActiveVersion.
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

func parseCSVLower(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		if v := strings.ToLower(strings.TrimSpace(part)); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func parseDomainCSV(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		v := strings.TrimSpace(part)
		v = strings.TrimPrefix(v, "@")
		v = strings.ToLower(strings.TrimSpace(v))
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

// SanitizeAllowedOrigins parses, validates, and normalizes explicit HTTP(S) origins
// (lowercasing scheme/host, stripping optional trailing "/", deduplicating, and
// skipping empty, wildcard, or malformed entries) so credentialed CORS never panics
// or reflects invalid origins.
func SanitizeAllowedOrigins(rawOrigins []string) []string {
	var out []string
	seen := make(map[string]struct{})
	for _, part := range rawOrigins {
		v := strings.TrimSpace(part)
		if v == "" {
			continue
		}
		if strings.Contains(v, "*") {
			log.Printf("WARN: ignoring wildcard %q in ALLOWED_ORIGINS; specify explicit origins instead", v)
			continue
		}
		if strings.ContainsAny(v, "?# \t\r\n") {
			log.Printf("WARN: ignoring invalid origin %q in ALLOWED_ORIGINS", v)
			continue
		}
		u, err := url.Parse(v)
		if err != nil {
			log.Printf("WARN: ignoring invalid origin %q in ALLOWED_ORIGINS: %v", v, err)
			continue
		}
		scheme := strings.ToLower(u.Scheme)
		if (scheme != "http" && scheme != "https") ||
			u.Opaque != "" ||
			u.Host == "" ||
			u.Hostname() == "" ||
			u.User != nil ||
			u.RawQuery != "" ||
			u.ForceQuery ||
			u.Fragment != "" ||
			(u.Path != "" && u.Path != "/") ||
			(u.RawPath != "" && u.RawPath != "/") {
			log.Printf("WARN: ignoring invalid origin %q in ALLOWED_ORIGINS; expected bare http(s)://host[:port]", v)
			continue
		}
		normalized := scheme + "://" + strings.ToLower(u.Host)
		if _, exists := seen[normalized]; !exists {
			seen[normalized] = struct{}{}
			out = append(out, normalized)
		}
	}
	return out
}

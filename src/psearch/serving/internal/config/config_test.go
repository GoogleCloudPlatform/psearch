package config

import (
	"os"
	"reflect"
	"testing"
)

func TestNormalizeModelID(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"gemini-4-pro", "gemini-4-pro"},
		{"gemini-4.0-pro", "gemini-4-pro"},
		{"GEMINI-4-PRO", "gemini-4-pro"},
		{"gemini-3.8-flash", "gemini-3.8-flash"},
		{"custom-model", "custom-model"},
		{"", ""},
	}

	for _, tt := range tests {
		got := NormalizeModelID(tt.input)
		if got != tt.want {
			t.Errorf("NormalizeModelID(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestConfigModelDefaults(t *testing.T) {
	// Clear relevant env vars
	os.Unsetenv("GEMINI_TEXT_MODEL")
	os.Unsetenv("GEMINI_LITE_MODEL")
	os.Unsetenv("GEMINI_PRO_MODEL")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.IntentLLMModel != "gemini-3.8-flash" {
		t.Errorf("default IntentLLMModel = %q, want gemini-3.8-flash", cfg.IntentLLMModel)
	}
	if cfg.ProLLMModel != "gemini-4-pro" {
		t.Errorf("default ProLLMModel = %q, want gemini-4-pro", cfg.ProLLMModel)
	}
	if cfg.GenAILocation != "global" {
		t.Errorf("default GenAILocation = %q, want global", cfg.GenAILocation)
	}
}

func clearAuthEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"ENVIRONMENT",
		"USE_MEMORY_STORE",
		"REQUIRE_API_AUTH",
		"API_TOKEN",
		"ADMIN_TOKEN",
		"SERVING_AUTH_AUDIENCE",
		"SERVING_ALLOWED_PRINCIPALS",
		"SERVING_ALLOWED_DOMAINS",
		"ALLOWED_ORIGINS",
	} {
		t.Setenv(k, "")
	}
}

func TestLoadDefaultsRequireAPIAuthInProduction(t *testing.T) {
	t.Run("production default enables RequireAPIAuth and filters wildcard origins", func(t *testing.T) {
		clearAuthEnv(t)
		t.Setenv("ALLOWED_ORIGINS", "*, https://ui.example.com , ")
		t.Setenv("SERVING_ALLOWED_PRINCIPALS", " Alice@Example.COM , svc@proj.iam.gserviceaccount.com ")
		t.Setenv("SERVING_ALLOWED_DOMAINS", " @Corp.Example.COM , sub.example.org , @ ")
		t.Setenv("ADMIN_TOKEN", "  admin-secret  ")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cfg.Environment != "production" {
			t.Fatalf("Environment = %q, want production", cfg.Environment)
		}
		if !cfg.RequireAPIAuth {
			t.Fatal("RequireAPIAuth = false, want true in default production mode")
		}
		wantOrigins := []string{"https://ui.example.com"}
		if !reflect.DeepEqual(cfg.AllowedOrigins, wantOrigins) {
			t.Fatalf("AllowedOrigins = %#v, want %#v", cfg.AllowedOrigins, wantOrigins)
		}
		wantPrincipals := []string{"alice@example.com", "svc@proj.iam.gserviceaccount.com"}
		if !reflect.DeepEqual(cfg.AllowedPrincipals, wantPrincipals) {
			t.Fatalf("AllowedPrincipals = %#v, want %#v", cfg.AllowedPrincipals, wantPrincipals)
		}
		wantDomains := []string{"corp.example.com", "sub.example.org"}
		if !reflect.DeepEqual(cfg.AllowedDomains, wantDomains) {
			t.Fatalf("AllowedDomains = %#v, want %#v", cfg.AllowedDomains, wantDomains)
		}
		if cfg.AdminToken != "admin-secret" {
			t.Fatalf("AdminToken = %q, want trimmed admin-secret", cfg.AdminToken)
		}
	})

	t.Run("wildcard-only ALLOWED_ORIGINS produces empty slice", func(t *testing.T) {
		clearAuthEnv(t)
		t.Setenv("ALLOWED_ORIGINS", "*")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if len(cfg.AllowedOrigins) != 0 {
			t.Fatalf("AllowedOrigins = %#v, want empty", cfg.AllowedOrigins)
		}
	})

	t.Run("memory store without explicit auth defaults RequireAPIAuth to false", func(t *testing.T) {
		clearAuthEnv(t)
		t.Setenv("USE_MEMORY_STORE", "true")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cfg.RequireAPIAuth {
			t.Fatal("RequireAPIAuth = true, want false when USE_MEMORY_STORE=true and auth unset")
		}
	})

	t.Run("API_TOKEN or SERVING_AUTH_AUDIENCE enables RequireAPIAuth even with memory store", func(t *testing.T) {
		clearAuthEnv(t)
		t.Setenv("USE_MEMORY_STORE", "true")
		t.Setenv("API_TOKEN", "  my-api-token  ")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if !cfg.RequireAPIAuth || cfg.APIToken != "my-api-token" {
			t.Fatalf("RequireAPIAuth=%v APIToken=%q, want true / my-api-token", cfg.RequireAPIAuth, cfg.APIToken)
		}

		clearAuthEnv(t)
		t.Setenv("USE_MEMORY_STORE", "true")
		t.Setenv("SERVING_AUTH_AUDIENCE", " https://serving.run.app ")

		cfg2, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if !cfg2.RequireAPIAuth || cfg2.AuthAudience != "https://serving.run.app" {
			t.Fatalf("RequireAPIAuth=%v AuthAudience=%q, want true / https://serving.run.app", cfg2.RequireAPIAuth, cfg2.AuthAudience)
		}
	})

	t.Run("explicit REQUIRE_API_AUTH overrides default", func(t *testing.T) {
		clearAuthEnv(t)
		t.Setenv("ENVIRONMENT", "production")
		t.Setenv("REQUIRE_API_AUTH", "false")

		cfg, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if cfg.RequireAPIAuth {
			t.Fatal("RequireAPIAuth = true, want false when REQUIRE_API_AUTH=false")
		}

		clearAuthEnv(t)
		t.Setenv("USE_MEMORY_STORE", "true")
		t.Setenv("REQUIRE_API_AUTH", "1")

		cfg2, err := Load()
		if err != nil {
			t.Fatalf("Load() error = %v", err)
		}
		if !cfg2.RequireAPIAuth {
			t.Fatal("RequireAPIAuth = false, want true when REQUIRE_API_AUTH=1")
		}
	})
}

package config

import (
	"os"
	"testing"
)

func TestNormalizeModelID(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"gemini-4-pro", "gemini-4-argon"},
		{"gemini-4.0-pro", "gemini-4-argon"},
		{"GEMINI-4-PRO", "gemini-4-argon"},
		{"gemini-4-argon", "gemini-4-argon"},
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
	if cfg.ProLLMModel != "gemini-4-argon" {
		t.Errorf("default ProLLMModel = %q, want gemini-4-argon", cfg.ProLLMModel)
	}
	if cfg.GenAILocation != "global" {
		t.Errorf("default GenAILocation = %q, want global", cfg.GenAILocation)
	}
}

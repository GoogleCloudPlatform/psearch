package services

import (
	"context"
	"errors"
	"testing"
)

func newQU(llm IntentLLM) *QueryUnderstandingService {
	return NewQueryUnderstandingService(DefaultSeedProducts(), llm, nil)
}

func TestParsePricesBrandsColors(t *testing.T) {
	qu := newQU(nil)
	cases := []struct {
		q            string
		clean        string
		min, max     float64
		brand, color string
	}{
		{"tenis de corrida ate 200 reais", "tenis de corrida", 0, 200, "", ""},
		{"Tênis de corrida masculino azul até R$ 300", "tenis de corrida masculino azul", 0, 300, "", "blue"},
		{"running shoes adidas under 200", "running shoes", 0, 200, "Adidas", ""},
		{"notebook acima de 1000", "notebook", 1000, 0, "", ""},
		{"laptop between 500 and 1.500,00", "laptop", 500, 1500, "", ""},
		{"geladeira entre 1500 e 900", "geladeira", 900, 1500, "", ""},
		{"zapatillas negras hasta 150 pesos", "zapatillas negras", 0, 150, "", ""},
	}
	for _, c := range cases {
		in := qu.Parse(c.q)
		if in.CleanQuery != c.clean || in.MinPrice != c.min || in.MaxPrice != c.max || in.Brand != c.brand || (c.color != "" && in.Color != c.color) {
			t.Errorf("Parse(%q) = %+v; want clean=%q min=%v max=%v brand=%q color=%q", c.q, in, c.clean, c.min, c.max, c.brand, c.color)
		}
	}
}

type fakeLLM struct {
	calls int
	in    Intent
	err   error
}

func (f *fakeLLM) Extract(context.Context, string) (Intent, error) { f.calls++; return f.in, f.err }

func TestUnderstandParserFirstThenLLMWithCache(t *testing.T) {
	ctx := context.Background()
	llm := &fakeLLM{in: Intent{CleanQuery: "presente para corredor", MaxPrice: 250}}
	qu := newQU(llm)
	if _, src := qu.Understand(ctx, "default", "running shoes under 200"); src != "parser" || llm.calls != 0 {
		t.Fatalf("parser should handle priced queries without the LLM (src=%s calls=%d)", src, llm.calls)
	}
	if _, src := qu.Understand(ctx, "default", "tenis"); src != "none" || llm.calls != 0 {
		t.Fatal("short queries never call the LLM")
	}
	in, src := qu.Understand(ctx, "default", "um presente bacana para quem gosta de correr")
	if src != "gemini" || in.MaxPrice != 250 || llm.calls != 1 {
		t.Fatalf("conversational query should use the LLM: %+v %s", in, src)
	}
	if _, src := qu.Understand(ctx, "default", "um presente bacana para quem gosta de correr"); src != "cache" || llm.calls != 1 {
		t.Fatal("repeat must be served from cache")
	}
	bad := newQU(&fakeLLM{err: errors.New("timeout")})
	if in, src := bad.Understand(ctx, "default", "algo legal para dar de presente hoje"); src != "none" || in.CleanQuery == "" {
		t.Fatal("LLM failure degrades to the parser result")
	}
}

func TestSanitizeIntent(t *testing.T) {
	in := sanitizeIntent(Intent{CleanQuery: "", MaxPrice: -5, MinPrice: 1e9}, "orig")
	if in.CleanQuery != "orig" || in.MaxPrice != 0 || in.MinPrice != 0 {
		t.Fatalf("sanitize = %+v", in)
	}
}

func TestParseNum(t *testing.T) {
	for in, want := range map[string]float64{"1.500,00": 1500, "1,299.90": 1299.9, "1.500": 1500, "19,90": 19.9, "300": 300, "1500": 1500} {
		if got := parseNum(in); got != want {
			t.Errorf("parseNum(%q) = %v, want %v", in, got, want)
		}
	}
}

// recovery relies on re-parsing corrected queries: brand-only entities (indexed
// as a column, not in title tokens) must be extracted as filters.
func TestParseBrandOnlyQuery(t *testing.T) {
	in := newQU(nil).Parse("samsung")
	if in.Brand != "Samsung" || in.CleanQuery == "" {
		t.Fatalf("Parse(samsung) = %+v", in)
	}
}

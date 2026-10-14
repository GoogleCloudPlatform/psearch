package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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
		ProjectID:              "psearch-sandbox",
		Region:                 "us-central1",
		SpannerInstanceID:      "psearch-instance",
		SpannerDatabaseID:      "psearch-db",
		EmbeddingModelName:     "gemini-embedding-2",
		EmbeddingDimension:     768,
		ActiveEmbeddingVersion: "v1",
		IntentLLMModel:         "gemini-3.8-flash",
		ProLLMModel:            "gemini-4-argon",
		GenAILocation:          "global",
		DefaultAlpha:           0.5,
		DefaultLimit:           10,
		UseMemoryStore:         true,
	}
	SetupRouter(r, cfg)
	return r
}

func TestHealthAndInfoEndpoints(t *testing.T) {
	r := newTestRouter()

	for _, path := range []string{"/health", "/api/health"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", path, w.Code)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/api/info", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /api/info status = %d, want 200", w.Code)
	}
	var info map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &info); err != nil {
		t.Fatalf("unmarshal /api/info: %v", err)
	}
	if info["embedding_model"] != "gemini-embedding-2" {
		t.Errorf("embedding_model = %v, want gemini-embedding-2", info["embedding_model"])
	}
	if info["text_model"] != "gemini-3.8-flash" {
		t.Errorf("text_model = %v, want gemini-3.8-flash", info["text_model"])
	}
	if info["pro_model"] != "gemini-4-argon" {
		t.Errorf("pro_model = %v, want gemini-4-argon", info["pro_model"])
	}
	if info["genai_location"] != "global" {
		t.Errorf("genai_location = %v, want global", info["genai_location"])
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

func doSearch(t *testing.T, r *gin.Engine, rawQuery string) models.SearchResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/api/search?"+rawQuery, nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("GET /api/search?%s status = %d, body = %s", rawQuery, w.Code, w.Body.String())
	}
	var resp models.SearchResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal SearchResponse: %v", err)
	}
	return resp
}

func ids(results []models.SearchResult) string {
	out := make([]string, 0, len(results))
	for _, r := range results {
		out = append(out, r.ID)
	}
	return strings.Join(out, ",")
}

// Issue #1: alpha=0 (pure text) and alpha=1 (pure vector) must produce different
// rankings; score.alpha/final_score echo the effective weight; out-of-range alpha is clamped.
func TestAlphaExtremesChangeRankingAndAreClamped(t *testing.T) {
	r := newTestRouter()
	text := doSearch(t, r, "q=running+shoes&alpha=0&limit=15")
	vec := doSearch(t, r, "q=running+shoes&alpha=1&limit=15")
	// final_score must reflect the weights: pure-text scores ignore the vector leg and vice versa.
	textScore := map[string]float64{}
	for _, res := range text.Results {
		textScore[res.ID] = res.Score["final_score"]
		if res.Score["final_score"] != res.Score["text"] {
			t.Fatalf("alpha=0: %s final_score %v != text %v", res.ID, res.Score["final_score"], res.Score["text"])
		}
	}
	differs := false
	for _, res := range vec.Results {
		if res.Score["final_score"] != res.Score["vector"] {
			t.Fatalf("alpha=1: %s final_score %v != vector %v", res.ID, res.Score["final_score"], res.Score["vector"])
		}
		differs = differs || textScore[res.ID] != res.Score["final_score"]
	}
	if !differs {
		t.Fatal("alpha=0 and alpha=1 produced identical final_scores")
	}
	for _, res := range text.Results {
		if res.Score["alpha"] != 0 {
			t.Fatalf("alpha=0: score.alpha = %v", res.Score["alpha"])
		}
		if _, ok := res.Score["final_score"]; !ok {
			t.Fatalf("result %s missing score.final_score", res.ID)
		}
	}
	if text.EffectiveAlpha != 0 || vec.EffectiveAlpha != 1 {
		t.Fatalf("effective_alpha = %v / %v, want 0 / 1", text.EffectiveAlpha, vec.EffectiveAlpha)
	}
	if got := doSearch(t, r, "q=running+shoes&alpha=7").EffectiveAlpha; got != 1 {
		t.Errorf("alpha=7 effective_alpha = %v, want clamped 1", got)
	}
	if got := doSearch(t, r, "q=running+shoes&alpha=-3").EffectiveAlpha; got != 0 {
		t.Errorf("alpha=-3 effective_alpha = %v, want clamped 0", got)
	}
}

func isOut(r models.SearchResult) bool {
	return r.Availability == "OUT_OF_STOCK" || (r.AvailableQuantity != nil && *r.AvailableQuantity <= 0)
}

// Issue #3: unavailable products are always ordered last; only_in_stock removes them.
func TestUnavailableLastAndOnlyInStock(t *testing.T) {
	r := newTestRouter()
	all := doSearch(t, r, "q=shoes&limit=15")
	seenOut := false
	for _, res := range all.Results {
		if isOut(res) {
			seenOut = true
		} else if seenOut {
			t.Fatalf("available product %s ranked after an unavailable one: %s", res.ID, ids(all.Results))
		}
	}
	if !seenOut {
		t.Fatalf("seed catalog should yield at least one OUT_OF_STOCK result: %s", ids(all.Results))
	}
	inStock := doSearch(t, r, "q=shoes&limit=15&only_in_stock=true")
	for _, res := range inStock.Results {
		if isOut(res) {
			t.Fatalf("only_in_stock=true returned unavailable product %s", res.ID)
		}
	}
	if len(inStock.Results) == 0 || len(inStock.Results) >= len(all.Results) {
		t.Fatalf("only_in_stock should drop unavailable items: %d vs %d", len(inStock.Results), len(all.Results))
	}
}

// Issue #3: channel filter via GET and POST (top-level JSON fields).
func TestChannelFilterGetAndPost(t *testing.T) {
	r := newTestRouter()
	store := doSearch(t, r, "q=shoes&limit=15&channel=store")
	if len(store.Results) == 0 {
		t.Fatal("expected store-channel results")
	}
	for _, res := range store.Results {
		found := false
		for _, c := range res.Channels {
			found = found || c == "store"
		}
		if !found {
			t.Fatalf("channel=store returned %s with channels %v", res.ID, res.Channels)
		}
	}
	body, _ := json.Marshal(map[string]interface{}{"query": "shoes", "limit": 15, "channel": "store", "only_in_stock": true})
	req := httptest.NewRequest(http.MethodPost, "/api/search", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	var resp models.SearchResponse
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &resp) != nil {
		t.Fatalf("POST /api/search status=%d body=%s", w.Code, w.Body.String())
	}
	for _, res := range resp.Results {
		if isOut(res) {
			t.Fatalf("POST only_in_stock returned unavailable %s", res.ID)
		}
	}
	if ids(resp.Results) == "" {
		t.Fatal("POST with channel=store&only_in_stock should return results")
	}
}

func newTokenRouter(token string) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	cfg := &config.Config{ProjectID: "psearch-project", ActiveEmbeddingVersion: "v1", GeminiEmbeddingDimension: 1024,
		DefaultAlpha: 0.5, DefaultLimit: 10, UseMemoryStore: true, AdminToken: token}
	SetupRouter(r, cfg)
	return r
}

func postVersion(r *gin.Engine, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/config/embedding-version", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("X-Admin-Token", token)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

// Issue #2: runtime Blue/Green toggle is admin-protected and validated.
func TestEmbeddingVersionEndpoint(t *testing.T) {
	if w := postVersion(newTokenRouter(""), "anything", `{"version":"v2"}`); w.Code != http.StatusForbidden {
		t.Fatalf("POST with ADMIN_TOKEN unset: status %d, want 403", w.Code)
	}
	r := newTokenRouter("s3cret")
	if w := postVersion(r, "", `{"version":"v2"}`); w.Code != http.StatusForbidden {
		t.Fatalf("POST without token: status %d, want 403", w.Code)
	}
	if w := postVersion(r, "wrong", `{"version":"v2"}`); w.Code != http.StatusForbidden {
		t.Fatalf("POST wrong token: status %d, want 403", w.Code)
	}
	if w := postVersion(r, "s3cret", `{"version":"v9"}`); w.Code != http.StatusBadRequest {
		t.Fatalf("POST invalid version: status %d, want 400", w.Code)
	}
	if w := postVersion(r, "s3cret", `{"version":"v2"}`); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"active_embedding_version":"v2"`) {
		t.Fatalf("POST valid: status %d body %s", w.Code, w.Body.String())
	}
	req := httptest.NewRequest(http.MethodGet, "/api/info", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	var info map[string]interface{}
	_ = json.Unmarshal(w.Body.Bytes(), &info)
	if info["active_embedding_version"] != "v2" || info["vector_dimensions"] != float64(1024) {
		t.Fatalf("info after switch = %v / %v", info["active_embedding_version"], info["vector_dimensions"])
	}
	if _, ok := info["embedding_cache"]; !ok {
		t.Fatal("/api/info must include embedding_cache stats")
	}
}

// Issue #5: facets are returned, computed over the candidate set, while results are truncated to limit.
func TestSearchReturnsFacetsAndTruncates(t *testing.T) {
	r := newTestRouter()
	resp := doSearch(t, r, "q=shoes&limit=3")
	if len(resp.Results) != 3 || resp.TotalFound != 3 {
		t.Fatalf("results = %d (total_found %d), want 3", len(resp.Results), resp.TotalFound)
	}
	if resp.Facets == nil || len(resp.Facets.Categories) == 0 || len(resp.Facets.Brands) == 0 || len(resp.Facets.PriceRanges) == 0 || len(resp.Facets.Availability) == 0 {
		t.Fatalf("expected 4 non-empty facet groups, got %+v", resp.Facets)
	}
	var n int64
	for _, b := range resp.Facets.Availability {
		n += b.Count
	}
	if n != int64(resp.Candidates) || resp.Candidates <= 3 {
		t.Fatalf("facet counts (%d) must cover the candidate set (%d), wider than the page", n, resp.Candidates)
	}
}

// Issues #4/#5: tenant, locale, category and brand filters; accent-insensitive matching.
func TestCatalogFiltersLocaleTenantCategoryBrand(t *testing.T) {
	r := newTestRouter()
	pt := doSearch(t, r, "q=notebook&limit=15&locale=pt-BR")
	for _, res := range pt.Results {
		if res.Locale != "pt-BR" {
			t.Fatalf("locale=pt-BR returned %s (%s)", res.ID, res.Locale)
		}
	}
	if len(pt.Results) == 0 || pt.Locale != "pt-BR" || pt.TenantID != "default" {
		t.Fatalf("pt-BR search: %d results, locale=%q tenant=%q", len(pt.Results), pt.Locale, pt.TenantID)
	}
	if other := doSearch(t, r, "q=shoes&tenant_id=acme"); len(other.Results) != 0 {
		t.Fatalf("unknown tenant must not see default catalog, got %d results", len(other.Results))
	}
	cat := doSearch(t, r, "q=shoes&limit=15&category=footwear")
	for _, res := range cat.Results {
		if len(res.Categories) == 0 || !strings.EqualFold(res.Categories[0], "Footwear") {
			t.Fatalf("category=footwear returned %s %v", res.ID, res.Categories)
		}
	}
	brand := doSearch(t, r, "q=shoes&limit=15&brand=Nike")
	if len(brand.Results) != 1 || brand.Results[0].ID != "prod-003" {
		t.Fatalf("brand=Nike: %s", ids(brand.Results))
	}
	folded := doSearch(t, r, "q=tenis&limit=15&alpha=0")
	if len(folded.Results) == 0 || folded.Results[0].ID != "prod-002" {
		t.Fatalf("accent-insensitive 'tenis' should rank 'Tênis...' (prod-002) first at alpha=0: %s", ids(folded.Results))
	}
}

// Issue #7: synonyms API (GET public, POST admin-only) and expanded_terms in search.
func TestSynonymsEndpoints(t *testing.T) {
	r := newTokenRouter("s3cret")
	req := httptest.NewRequest(http.MethodGet, "/api/synonyms?locale=pt-BR", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"term":"tenis"`) {
		t.Fatalf("GET /api/synonyms: %d %s", w.Code, w.Body.String())
	}
	post := func(token, body string) int {
		req := httptest.NewRequest(http.MethodPost, "/api/synonyms", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("X-Admin-Token", token)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w.Code
	}
	if code := post("", `{"locale":"pt-BR","term":"bota","synonyms":["coturno"]}`); code != http.StatusForbidden {
		t.Fatalf("POST without token = %d, want 403", code)
	}
	if code := post("s3cret", `{"locale":"pt-BR","term":"bota","synonyms":["coturno"],"weight":2}`); code != http.StatusBadRequest {
		t.Fatalf("POST weight>=1 = %d, want 400", code)
	}
	if code := post("s3cret", `{"locale":"en-US","term":"kicks","synonyms":["sneaker","shoes"]}`); code != http.StatusOK {
		t.Fatalf("POST valid = %d", code)
	}
	resp := doSearch(t, r, "q=kicks&locale=en-US")
	if len(resp.ExpandedTerms) == 0 || len(resp.Results) == 0 {
		t.Fatalf("search 'kicks' should expand via new synonym: terms=%v results=%d", resp.ExpandedTerms, len(resp.Results))
	}
}

// Issue #6: /api/rules CRUD (GET public; writes admin-only) and applied_rules in search.
func TestRulesCRUD(t *testing.T) {
	r := newTokenRouter("s3cret")
	do := func(method, path, token, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("X-Admin-Token", token)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}
	rule := `{"name":"pin pegasus","query_pattern":"shoes","pin_product_ids":["prod-003"],"active":true}`
	if w := do(http.MethodPost, "/api/rules", "", rule); w.Code != http.StatusForbidden {
		t.Fatalf("POST without token = %d", w.Code)
	}
	w := do(http.MethodPost, "/api/rules", "s3cret", rule)
	if w.Code != http.StatusCreated {
		t.Fatalf("POST = %d %s", w.Code, w.Body.String())
	}
	var created struct {
		RuleID string `json:"rule_id"`
	}
	_ = json.Unmarshal(w.Body.Bytes(), &created)
	if created.RuleID == "" {
		t.Fatal("server must assign rule_id")
	}
	if w := do(http.MethodGet, "/api/rules", "", ""); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), created.RuleID) {
		t.Fatalf("GET = %d %s", w.Code, w.Body.String())
	}
	resp := doSearch(t, r, "q=running+shoes&limit=5")
	if len(resp.Results) == 0 || resp.Results[0].ID != "prod-003" || len(resp.AppliedRules) != 1 {
		t.Fatalf("pin rule not applied: %s rules=%v", ids(resp.Results), resp.AppliedRules)
	}
	if w := do(http.MethodPut, "/api/rules/"+created.RuleID, "s3cret", `{"name":"off","query_pattern":"shoes","pin_product_ids":["prod-003"],"active":false}`); w.Code != http.StatusOK {
		t.Fatalf("PUT = %d %s", w.Code, w.Body.String())
	}
	if resp := doSearch(t, r, "q=running+shoes&limit=5"); len(resp.AppliedRules) != 0 {
		t.Fatal("inactive rule must not apply")
	}
	if w := do(http.MethodPost, "/api/rules", "s3cret", `{"name":"bad","boost_multiplier":2}`); w.Code != http.StatusBadRequest {
		t.Fatalf("invalid rule = %d", w.Code)
	}
	if w := do(http.MethodDelete, "/api/rules/"+created.RuleID, "s3cret", ""); w.Code != http.StatusNoContent {
		t.Fatalf("DELETE = %d", w.Code)
	}
	if w := do(http.MethodGet, "/api/rules", "", ""); strings.Contains(w.Body.String(), created.RuleID) {
		t.Fatal("rule still listed after delete")
	}
}

// Issue #8: CEP + max_sla_hours filter, delivery promise on each result, invalid CEP -> 400.
func TestDeliveryPromiseSearch(t *testing.T) {
	r := newTestRouter()
	resp := doSearch(t, r, "q=shoes&limit=15&cep=01310-100&max_sla_hours=24")
	if len(resp.Results) == 0 {
		t.Fatal("expected deliverable results for CEP 01310-100")
	}
	for _, res := range resp.Results {
		if res.DeliveryPromise == nil || res.DeliveryPromise.SLAHours > 24 || res.DeliveryPromise.CEP != "01310-100" {
			t.Fatalf("%s promise = %+v", res.ID, res.DeliveryPromise)
		}
	}
	req := httptest.NewRequest(http.MethodGet, "/api/search?q=shoes&cep=123", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid CEP = %d, want 400", w.Code)
	}
	if noCEP := doSearch(t, r, "q=shoes&limit=3"); noCEP.Results[0].DeliveryPromise != nil {
		t.Fatal("no CEP => no delivery promise")
	}
}

// Issue #9: /api/autocomplete endpoint.
func TestAutocompleteEndpoint(t *testing.T) {
	r := newTestRouter()
	req := httptest.NewRequest(http.MethodGet, "/api/autocomplete?q=geladei&locale=pt-BR", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	var resp struct {
		Suggestions []struct {
			Text string `json:"text"`
		} `json:"suggestions"`
		LatencyMs float64 `json:"latency_ms"`
	}
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &resp) != nil || len(resp.Suggestions) == 0 || !strings.HasPrefix(resp.Suggestions[0].Text, "geladeira") {
		t.Fatalf("autocomplete: %d %s", w.Code, w.Body.String())
	}
	req = httptest.NewRequest(http.MethodGet, "/api/autocomplete?q="+strings.Repeat("x", 101), nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("long q = %d, want 400", w.Code)
	}
}

// Issue #10: ?ltr=true returns stage ranks, ltr_score and latency; limit still applies.
func TestLTRSearch(t *testing.T) {
	r := newTestRouter()
	resp := doSearch(t, r, "q=shoes&limit=5&ltr=true")
	if !resp.LTR || resp.LTRModel == "" || len(resp.Results) != 5 {
		t.Fatalf("ltr=%v model=%q results=%d", resp.LTR, resp.LTRModel, len(resp.Results))
	}
	for i, res := range resp.Results {
		if res.Score["stage2_rank"] != float64(i+1) || res.Score["stage1_rank"] == 0 {
			t.Fatalf("%s ranks %v", res.ID, res.Score)
		}
		if _, ok := res.Score["ltr_score"]; !ok {
			t.Fatalf("%s missing ltr_score", res.ID)
		}
	}
	if plain := doSearch(t, r, "q=shoes&limit=5"); plain.LTR || plain.Results[0].Score["ltr_score"] != 0 {
		t.Fatal("ltr must be opt-in")
	}
}

// Issue #19: parse_intent extracts price/brand filters and enforces them.
func TestParseIntentSearch(t *testing.T) {
	r := newTestRouter()
	resp := doSearch(t, r, "q=tenis+de+corrida+ate+200+reais&parse_intent=true&limit=15")
	if resp.ParsedQuery == nil || resp.ParsedQuery.CleanQuery != "tenis de corrida" || resp.ParsedQuery.ExtractedFilters["max_price"] != float64(200) || resp.ParsedQuery.IntentSource != "parser" {
		t.Fatalf("parsed_query = %+v", resp.ParsedQuery)
	}
	if len(resp.Results) == 0 {
		t.Fatal("expected results under 200")
	}
	for _, res := range resp.Results {
		if res.PriceInfo.NumericPrice > 200 {
			t.Fatalf("%s price %v exceeds max_price 200", res.ID, res.PriceInfo.NumericPrice)
		}
	}
	brand := doSearch(t, r, "q=running+shoes+adidas+under+200&parse_intent=true")
	if brand.ParsedQuery.ExtractedFilters["brand"] != "Adidas" || len(brand.Results) != 1 || brand.Results[0].ID != "prod-001" {
		t.Fatalf("brand intent: %+v %s", brand.ParsedQuery, ids(brand.Results))
	}
	explicit := doSearch(t, r, "q=notebook+ate+100&parse_intent=true&max_price=2000")
	if explicit.ParsedQuery.ExtractedFilters["max_price"] != float64(100) || len(explicit.Results) == 0 {
		t.Fatal("explicit max_price must override the extracted one")
	}
	if off := doSearch(t, r, "q=tenis+ate+200&parse_intent=false"); off.ParsedQuery != nil {
		t.Fatal("parse_intent=false must disable intent parsing")
	}
	auto := doSearch(t, r, "q=tenis+de+corrida+ate+200+reais&limit=15")
	if auto.ParsedQuery == nil || auto.ParsedQuery.ExtractedFilters["max_price"] != float64(200) {
		t.Fatalf("intent parsing must be on by default (ADR-007): %+v", auto.ParsedQuery)
	}
	if plain := doSearch(t, r, "q=geladeira"); plain.ParsedQuery != nil {
		t.Fatal("queries without extractable filters keep the response unchanged")
	}
	req := httptest.NewRequest(http.MethodGet, "/api/search?q=x&min_price=500&max_price=100", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("min>max = %d, want 400", w.Code)
	}
}

// Issue #21: typo recovery with did_you_mean / auto_corrected.
func TestZeroResultsRecovery(t *testing.T) {
	r := newTestRouter()
	resp := doSearch(t, r, "q=sansung&limit=5&alpha=0")
	if resp.DidYouMean != "samsung" || !resp.AutoCorrected || len(resp.Results) == 0 || resp.Results[0].ID != "prod-010" {
		t.Fatalf("did_you_mean=%q auto=%v results=%s", resp.DidYouMean, resp.AutoCorrected, ids(resp.Results))
	}
	gel := doSearch(t, r, "q=geladera&limit=5&alpha=0")
	if gel.DidYouMean != "geladeira" || !gel.AutoCorrected || gel.Results[0].ID != "prod-011" {
		t.Fatalf("geladera: %q %v %s", gel.DidYouMean, gel.AutoCorrected, ids(gel.Results))
	}
	if ok := doSearch(t, r, "q=samsung&limit=5"); ok.DidYouMean != "" || ok.AutoCorrected || ok.QueryRelaxed {
		t.Fatal("queries with lexical hits are untouched")
	}
}

// Issue #21 regression: a correction that resolves to a brand-only
// entity must be accepted even when the corrected word has no title/description hit.
func TestSpellCorrectionToBrandEntity(t *testing.T) {
	r := newTestRouter()
	resp := doSearch(t, r, "q=sansung&limit=5&alpha=0&parse_intent=false")
	if resp.DidYouMean != "samsung" || !resp.AutoCorrected {
		t.Fatalf("did_you_mean=%q auto=%v", resp.DidYouMean, resp.AutoCorrected)
	}
	for _, res := range resp.Results {
		if len(res.Brands) == 0 || res.Brands[0] != "Samsung" {
			t.Fatalf("brand entity filter not applied after correction: %s", ids(resp.Results))
		}
	}
}

// Issue #22: contract price table, price sort, validation.
func TestContractPricingSearch(t *testing.T) {
	r := newTestRouter()
	resp := doSearch(t, r, "q=laptop&price_table_id=b2b-corporate&sort_by=price_asc&limit=15")
	if resp.PriceTableID != "b2b-corporate" || resp.SortBy != "price_asc" || len(resp.Results) == 0 {
		t.Fatalf("pricing response: table=%q sort=%q n=%d", resp.PriceTableID, resp.SortBy, len(resp.Results))
	}
	r0 := resp.Results[0]
	if r0.PriceInfo.BasePrice == 0 || r0.PriceInfo.NumericPrice >= r0.PriceInfo.BasePrice {
		t.Fatalf("contract price must be below base: %+v", r0.PriceInfo)
	}
	if retail := doSearch(t, r, "q=laptop&limit=3"); retail.Results[0].PriceInfo.PriceTableID != "" {
		t.Fatal("no price_table_id => list prices")
	}
	for _, bad := range []string{"q=x&sort_by=cheap", "q=x&price_table_id=Bad%20Table"} {
		req := httptest.NewRequest(http.MethodGet, "/api/search?"+bad, nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s = %d, want 400", bad, w.Code)
		}
	}
}

// Issue #20: session events endpoint + personalized search.
func TestSessionPersonalizationEndpoints(t *testing.T) {
	r := newTestRouter()
	post := func(body string) int {
		req := httptest.NewRequest(http.MethodPost, "/api/session/events", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w.Code
	}
	if code := post(`{"session_id":"bad","product_id":"prod-001","event_type":"click"}`); code != http.StatusBadRequest {
		t.Fatalf("invalid session = %d", code)
	}
	for _, id := range []string{"prod-005", "prod-007"} {
		if code := post(`{"session_id":"sess-laptop-01","product_id":"` + id + `","event_type":"purchase"}`); code != http.StatusCreated {
			t.Fatalf("record = %d", code)
		}
	}
	plain := doSearch(t, r, "q=shoes&limit=15")
	pers := doSearch(t, r, "q=shoes&limit=15&session_id=sess-laptop-01")
	if !pers.Personalized || pers.SessionID != "sess-laptop-01" || plain.Personalized {
		t.Fatalf("personalized=%v session=%q plain=%v", pers.Personalized, pers.SessionID, plain.Personalized)
	}
	if ids(plain.Results) == ids(pers.Results) {
		t.Fatal("session history should change the ranking")
	}
	if alt := doSearch(t, r, "q=shoes&limit=15&session_product_ids=prod-003"); !alt.Personalized {
		t.Fatal("session_product_ids should personalize without stored events")
	}
	req := httptest.NewRequest(http.MethodGet, "/api/session/events?session_id=sess-laptop-01", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"count":2`) {
		t.Fatalf("list events: %d %s", w.Code, w.Body.String())
	}
}

// Issue #32: variant filters and aggregates through the API.
func TestVariantGroupingAPI(t *testing.T) {
	r := newTestRouter()
	resp := doSearch(t, r, "q=running+shoes&limit=15&color=azul&size=42")
	if len(resp.Results) == 0 {
		t.Fatal("expected results for azul/42")
	}
	for _, res := range resp.Results {
		if res.VariantCount == 0 || len(res.AvailableColors) != 1 || res.AvailableColors[0] != "azul" {
			t.Fatalf("%s variants=%d colors=%v", res.ID, res.VariantCount, res.AvailableColors)
		}
	}
	all := doSearch(t, r, "q=shoes&limit=15")
	if all.Facets == nil || len(all.Facets.Colors) == 0 || len(all.Facets.Sizes) == 0 {
		t.Fatal("facets must include colors and sizes")
	}
	req := httptest.NewRequest(http.MethodGet, "/api/search?q=x&size="+strings.Repeat("9", 40), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("oversized size = %d, want 400", w.Code)
	}
}

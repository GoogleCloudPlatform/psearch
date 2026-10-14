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

// alpha=0 (pure text) and alpha=1 (pure vector) must produce different
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

// unavailable products are always ordered last; only_in_stock removes them.
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

// channel filter via GET and POST (top-level JSON fields).
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

// runtime Blue/Green toggle is admin-protected and validated.
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

// facets are returned, computed over the candidate set, while results are truncated to limit.
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

// tenant, locale, category and brand filters; accent-insensitive matching.
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

// synonyms API (GET public, POST admin-only) and expanded_terms in search.
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

// /api/rules CRUD (GET public; writes admin-only) and applied_rules in search.
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

// CEP + max_sla_hours filter, delivery promise on each result, invalid CEP -> 400.
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

// /api/autocomplete endpoint.
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

// ?ltr=true returns stage ranks, ltr_score and latency; limit still applies.
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

package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"google.golang.org/api/idtoken"
	"psearch/serving-go/internal/config"
)

func newAuthTestRouter(cfg *config.Config, verifier TokenVerifier) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	if cfg.ProjectID == "" {
		cfg.ProjectID = "psearch-project"
	}
	if cfg.ActiveEmbeddingVersion == "" {
		cfg.ActiveEmbeddingVersion = "v1"
	}
	if cfg.GeminiEmbeddingDimension == 0 {
		cfg.GeminiEmbeddingDimension = 1024
	}
	if cfg.DefaultAlpha == 0 {
		cfg.DefaultAlpha = 0.5
	}
	if cfg.DefaultLimit == 0 {
		cfg.DefaultLimit = 10
	}
	SetupRouterWithVerifier(r, cfg, verifier)
	return r
}

func TestAuthorizeAPIFailsClosedInProductionWhenUnconfigured(t *testing.T) {
	r := newAuthTestRouter(&config.Config{
		Environment:    "production",
		RequireAPIAuth: true,
		UseMemoryStore: true,
	}, nil)

	protected := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/api/search?q=shoes", ""},
		{http.MethodPost, "/api/search", `{"query":"shoes"}`},
		{http.MethodGet, "/search?q=shoes", ""},
		{http.MethodPost, "/search", `{"query":"shoes"}`},
		{http.MethodGet, "/api/info", ""},
		{http.MethodGet, "/api/config/embedding-version", ""},
		{http.MethodGet, "/api/autocomplete?q=sh", ""},
		{http.MethodPost, "/api/session/events", `{"session_id":"sess-12345678","product_id":"prod-001","event_type":"click"}`},
		{http.MethodGet, "/api/session/events?session_id=sess-12345678", ""},
		{http.MethodGet, "/api/rules", ""},
		{http.MethodGet, "/api/synonyms", ""},
	}

	for _, tc := range protected {
		req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
		if tc.body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusForbidden {
			t.Fatalf("%s %s status = %d, want 403 (body=%s)", tc.method, tc.path, w.Code, w.Body.String())
		}
		if !strings.Contains(w.Body.String(), "API authentication is not configured") {
			t.Fatalf("%s %s body = %s, want unconfigured error", tc.method, tc.path, w.Body.String())
		}
	}

	// Even with a Bearer header, unconfigured production server must fail closed with 403.
	reqWithBearer := httptest.NewRequest(http.MethodGet, "/api/search?q=shoes", nil)
	reqWithBearer.Header.Set("Authorization", "Bearer anything")
	wBearer := httptest.NewRecorder()
	r.ServeHTTP(wBearer, reqWithBearer)
	if wBearer.Code != http.StatusForbidden {
		t.Fatalf("GET /api/search with Bearer when unconfigured = %d, want 403", wBearer.Code)
	}

	for _, pubPath := range []string{"/health", "/healthz", "/api/health", "/"} {
		req := httptest.NewRequest(http.MethodGet, pubPath, nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("public GET %s status = %d, want 200", pubPath, w.Code)
		}
	}
}

func TestAuthorizeAPIAcceptsAPITokenButBlocksAdminWrites(t *testing.T) {
	r := newAuthTestRouter(&config.Config{
		Environment:    "production",
		RequireAPIAuth: true,
		UseMemoryStore: true,
		APIToken:       "read-key",
		AdminToken:     "admin-key",
	}, nil)

	doReq := func(method, path string, headers map[string]string, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}

	// Missing credentials -> 401 with WWW-Authenticate: Bearer.
	wMissing := doReq(http.MethodGet, "/api/search?q=shoes", nil, "")
	if wMissing.Code != http.StatusUnauthorized || wMissing.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Fatalf("missing creds: status=%d www-auth=%q body=%s", wMissing.Code, wMissing.Header().Get("WWW-Authenticate"), wMissing.Body.String())
	}

	// Invalid X-API-Key must NOT fall through even if valid X-Admin-Token is present.
	wBadAPIKey := doReq(http.MethodGet, "/api/search?q=shoes", map[string]string{
		"X-API-Key":     "wrong-key",
		"X-Admin-Token": "admin-key",
	}, "")
	if wBadAPIKey.Code != http.StatusForbidden {
		t.Fatalf("wrong X-API-Key status = %d, want 403", wBadAPIKey.Code)
	}

	// Invalid X-Admin-Token -> 403.
	wBadAdmin := doReq(http.MethodGet, "/api/search?q=shoes", map[string]string{
		"X-Admin-Token": "wrong-admin",
	}, "")
	if wBadAdmin.Code != http.StatusForbidden {
		t.Fatalf("wrong X-Admin-Token status = %d, want 403", wBadAdmin.Code)
	}

	// Bearer token when OIDC is not configured -> 403.
	wUnconfigOIDC := doReq(http.MethodGet, "/api/search?q=shoes", map[string]string{
		"Authorization": "Bearer some-token",
	}, "")
	if wUnconfigOIDC.Code != http.StatusForbidden || !strings.Contains(wUnconfigOIDC.Body.String(), "OIDC authentication is not configured") {
		t.Fatalf("Bearer without OIDC config: status=%d body=%s", wUnconfigOIDC.Code, wUnconfigOIDC.Body.String())
	}

	readHeaders := map[string]string{"X-API-Key": "read-key"}
	for _, path := range []string{"/api/search?q=shoes", "/api/rules", "/api/synonyms", "/api/info"} {
		if w := doReq(http.MethodGet, path, readHeaders, ""); w.Code != http.StatusOK {
			t.Fatalf("X-API-Key GET %s status = %d, want 200 (body=%s)", path, w.Code, w.Body.String())
		}
	}

	// Read-only X-API-Key must be blocked (403) on all admin-write endpoints.
	adminWrites := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/api/rules", `{"name":"pin","query_pattern":"shoes","pin_product_ids":["prod-001"],"active":true}`},
		{http.MethodPut, "/api/rules/rule-1", `{"name":"pin","query_pattern":"shoes","pin_product_ids":["prod-001"],"active":true}`},
		{http.MethodDelete, "/api/rules/rule-1", ""},
		{http.MethodPost, "/api/synonyms", `{"locale":"en-US","term":"kicks","synonyms":["shoes"]}`},
		{http.MethodPost, "/api/config/embedding-version", `{"version":"v2"}`},
	}
	for _, aw := range adminWrites {
		if w := doReq(aw.method, aw.path, readHeaders, aw.body); w.Code != http.StatusForbidden {
			t.Fatalf("X-API-Key %s %s status = %d, want 403", aw.method, aw.path, w.Code)
		}
	}

	// X-Admin-Token succeeds on both read and admin-write routes.
	adminHeaders := map[string]string{"X-Admin-Token": "admin-key"}
	if w := doReq(http.MethodGet, "/api/search?q=shoes", adminHeaders, ""); w.Code != http.StatusOK {
		t.Fatalf("X-Admin-Token GET /api/search status = %d, want 200", w.Code)
	}
	if w := doReq(http.MethodGet, "/api/rules", adminHeaders, ""); w.Code != http.StatusOK {
		t.Fatalf("X-Admin-Token GET /api/rules status = %d, want 200", w.Code)
	}
	if w := doReq(http.MethodPost, "/api/rules", adminHeaders, `{"name":"pin","query_pattern":"shoes","pin_product_ids":["prod-001"],"active":true}`); w.Code != http.StatusCreated {
		t.Fatalf("X-Admin-Token POST /api/rules status = %d, want 201 (body=%s)", w.Code, w.Body.String())
	}
	if w := doReq(http.MethodPost, "/api/synonyms", adminHeaders, `{"locale":"en-US","term":"kicks","synonyms":["shoes"]}`); w.Code != http.StatusOK {
		t.Fatalf("X-Admin-Token POST /api/synonyms status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}
	if w := doReq(http.MethodPost, "/api/config/embedding-version", adminHeaders, `{"version":"v2"}`); w.Code != http.StatusOK {
		t.Fatalf("X-Admin-Token POST /api/config/embedding-version status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}
}

func TestAuthorizeAPIOIDCEnforcesVerifiedEmailAndDomainHD(t *testing.T) {
	const wantAudience = "https://serving.example.run.app"
	tokens := map[string]*TokenClaims{
		"sa-ok": {
			Issuer:        "https://accounts.google.com",
			Email:         "SVC-UI@proj.iam.gserviceaccount.com",
			EmailVerified: true,
		},
		"domain-ok": {
			Issuer:        "accounts.google.com",
			Email:         "Alice@Corp.Example.com",
			EmailVerified: true,
			HostedDomain:  "corp.example.com",
		},
		"unverified-principal": {
			Issuer:        "https://accounts.google.com",
			Email:         "svc-ui@proj.iam.gserviceaccount.com",
			EmailVerified: false,
		},
		"empty-email": {
			Issuer:        "https://accounts.google.com",
			Email:         "   ",
			EmailVerified: true,
		},
		"bad-issuer": {
			Issuer:        "https://evil-idp.example.com",
			Email:         "svc-ui@proj.iam.gserviceaccount.com",
			EmailVerified: true,
		},
		"missing-hd": {
			Issuer:        "https://accounts.google.com",
			Email:         "alice@corp.example.com",
			EmailVerified: true,
			HostedDomain:  "",
		},
		"mismatched-hd": {
			Issuer:        "https://accounts.google.com",
			Email:         "alice@other.example.com",
			EmailVerified: true,
			HostedDomain:  "corp.example.com",
		},
		"unlisted": {
			Issuer:        "https://accounts.google.com",
			Email:         "bob@external.example.org",
			EmailVerified: true,
			HostedDomain:  "external.example.org",
		},
	}

	stubVerifier := func(_ context.Context, token, audience string) (*TokenClaims, error) {
		if audience != wantAudience {
			return nil, errors.New("unexpected audience: " + audience)
		}
		claims, ok := tokens[token]
		if !ok {
			return nil, errors.New("invalid token")
		}
		return claims, nil
	}

	r := newAuthTestRouter(&config.Config{
		Environment:       "production",
		RequireAPIAuth:    true,
		UseMemoryStore:    true,
		AuthAudience:      wantAudience,
		AllowedPrincipals: []string{"svc-ui@proj.iam.gserviceaccount.com"},
		AllowedDomains:    []string{"corp.example.com"},
		AdminToken:        "admin-key",
	}, stubVerifier)

	callSearch := func(authHeader string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, "/api/search?q=shoes", nil)
		if authHeader != "" {
			req.Header.Set("Authorization", authHeader)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}

	// Valid allowlisted principal (case-insensitive bearer scheme) -> 200.
	if w := callSearch("bearer sa-ok"); w.Code != http.StatusOK {
		t.Fatalf("allowlisted SA status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}

	// Valid allowlisted domain with matching hd -> 200.
	if w := callSearch("Bearer domain-ok"); w.Code != http.StatusOK {
		t.Fatalf("allowlisted domain status = %d, want 200 (body=%s)", w.Code, w.Body.String())
	}

	// OIDC caller without X-Admin-Token cannot perform admin writes -> 403.
	reqAdmin := httptest.NewRequest(http.MethodPost, "/api/synonyms", strings.NewReader(`{"locale":"en-US","term":"kicks","synonyms":["shoes"]}`))
	reqAdmin.Header.Set("Content-Type", "application/json")
	reqAdmin.Header.Set("Authorization", "Bearer domain-ok")
	wAdmin := httptest.NewRecorder()
	r.ServeHTTP(wAdmin, reqAdmin)
	if wAdmin.Code != http.StatusForbidden {
		t.Fatalf("OIDC caller POST /api/synonyms without admin token = %d, want 403", wAdmin.Code)
	}

	// 401 cases (with WWW-Authenticate: Bearer).
	for _, hdr := range []string{"", "Basic dXNlcjpwYXNz", "Bearer", "Bearer   ", "Bearer a b", "Bearer unknown-jwt", "Bearer bad-issuer"} {
		w := callSearch(hdr)
		if w.Code != http.StatusUnauthorized || w.Header().Get("WWW-Authenticate") != "Bearer" {
			t.Fatalf("Authorization=%q: status=%d www-auth=%q, want 401 + Bearer", hdr, w.Code, w.Header().Get("WWW-Authenticate"))
		}
	}

	// 403 cases (authenticated JWT structure, but unverified email, missing/mismatched hd, or unlisted caller).
	for _, tok := range []string{"unverified-principal", "empty-email", "missing-hd", "mismatched-hd", "unlisted"} {
		w := callSearch("Bearer " + tok)
		if w.Code != http.StatusForbidden {
			t.Fatalf("token=%q: status=%d, want 403 (body=%s)", tok, w.Code, w.Body.String())
		}
	}
}

func TestCORSBlocksWildcardOriginReflection(t *testing.T) {
	assertNoCORS := func(t *testing.T, r *gin.Engine, origin string) {
		t.Helper()
		for _, method := range []string{http.MethodGet, http.MethodOptions} {
			req := httptest.NewRequest(method, "/health", nil)
			req.Header.Set("Origin", origin)
			if method == http.MethodOptions {
				req.Header.Set("Access-Control-Request-Method", "GET")
			}
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
				t.Fatalf("%s with Origin=%s got Access-Control-Allow-Origin=%q, want empty", method, origin, got)
			}
			if got := w.Header().Get("Access-Control-Allow-Credentials"); got != "" {
				t.Fatalf("%s with Origin=%s got Access-Control-Allow-Credentials=%q, want empty", method, origin, got)
			}
		}
	}

	t.Run("nil AllowedOrigins emits no CORS headers", func(t *testing.T) {
		r := newAuthTestRouter(&config.Config{
			Environment:    "test",
			UseMemoryStore: true,
			AllowedOrigins: nil,
		}, nil)
		assertNoCORS(t, r, "https://evil.example")
	})

	t.Run("wildcard AllowedOrigins is ignored and emits no CORS headers", func(t *testing.T) {
		r := newAuthTestRouter(&config.Config{
			Environment:    "test",
			UseMemoryStore: true,
			AllowedOrigins: []string{"*"},
		}, nil)
		assertNoCORS(t, r, "https://evil.example")
	})

	t.Run("explicit AllowedOrigins allows only configured origin", func(t *testing.T) {
		r := newAuthTestRouter(&config.Config{
			Environment:    "production",
			RequireAPIAuth: true,
			UseMemoryStore: true,
			APIToken:       "read-key",
			AllowedOrigins: []string{"https://ui.example.com"},
		}, nil)

		assertNoCORS(t, r, "https://evil.example")

		// Preflight from allowed origin succeeds without credentials and advertises custom auth headers.
		pre := httptest.NewRequest(http.MethodOptions, "/api/search", nil)
		pre.Header.Set("Origin", "https://ui.example.com")
		pre.Header.Set("Access-Control-Request-Method", "GET")
		pre.Header.Set("Access-Control-Request-Headers", "Authorization, X-API-Key, X-Admin-Token")
		wPre := httptest.NewRecorder()
		r.ServeHTTP(wPre, pre)
		if got := wPre.Header().Get("Access-Control-Allow-Origin"); got != "https://ui.example.com" {
			t.Fatalf("preflight Allow-Origin = %q, want https://ui.example.com (status=%d)", got, wPre.Code)
		}
		if got := wPre.Header().Get("Access-Control-Allow-Credentials"); got != "true" {
			t.Fatalf("preflight Allow-Credentials = %q, want true", got)
		}
		allowHeaders := strings.ToLower(wPre.Header().Get("Access-Control-Allow-Headers"))
		for _, wantHdr := range []string{"authorization", "x-api-key", "x-admin-token"} {
			if !strings.Contains(allowHeaders, wantHdr) {
				t.Fatalf("preflight Allow-Headers = %q, missing %q", allowHeaders, wantHdr)
			}
		}

		// Normal request from allowed origin gets exact origin header.
		req := httptest.NewRequest(http.MethodGet, "/api/search?q=shoes", nil)
		req.Header.Set("Origin", "https://ui.example.com")
		req.Header.Set("X-API-Key", "read-key")
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("GET /api/search status = %d, want 200", w.Code)
		}
		if got := w.Header().Get("Access-Control-Allow-Origin"); got != "https://ui.example.com" {
			t.Fatalf("Allow-Origin = %q, want https://ui.example.com", got)
		}
	})
}

func TestStorefrontIncludesOptionalAuthHeaders(t *testing.T) {
	for _, frag := range []string{
		"function getStorefrontAuthHeaders()",
		"psearchAuthToken",
		"psearch_api_token",
		"psearchApiToken",
	} {
		if !strings.Contains(defaultStorefrontHTML, frag) {
			t.Errorf("defaultStorefrontHTML missing %q", frag)
		}
	}
}

func TestClaimsFromPayloadRequiresStrictBoolEmailVerified(t *testing.T) {
	strPayload := &idtoken.Payload{
		Issuer: "https://accounts.google.com",
		Claims: map[string]interface{}{
			"email":          "alice@corp.example.com",
			"email_verified": "true",
			"hd":             "corp.example.com",
		},
	}
	if got := claimsFromPayload(strPayload); got.EmailVerified {
		t.Fatal("claimsFromPayload treated string \"true\" as EmailVerified=true")
	}

	boolPayload := &idtoken.Payload{
		Issuer: "https://accounts.google.com",
		Claims: map[string]interface{}{
			"email":          "alice@corp.example.com",
			"email_verified": true,
			"hd":             "corp.example.com",
		},
	}
	got := claimsFromPayload(boolPayload)
	if !got.EmailVerified || got.Email != "alice@corp.example.com" || got.HostedDomain != "corp.example.com" || got.Issuer != "https://accounts.google.com" {
		t.Fatalf("unexpected claimsFromPayload result: %+v", got)
	}
}

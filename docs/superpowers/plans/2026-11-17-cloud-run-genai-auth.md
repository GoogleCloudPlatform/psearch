# Multi-Service Cloud Run & GenAI Security Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate unauthenticated Cloud Run exposure, shared `ingestion-service-account` over-privilege, and unauthenticated Vertex AI / BigQuery / Spanner API endpoints across `gen_ai`, `search_api` (`psearch-serving`), `ui`, and Terraform IaC.

**Architecture:** Remove `allUsers` from every remaining Cloud Run Terraform module (`gen_ai`, `search_api`, `ui`), enforce `INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER` by default across all four Cloud Run services, and isolate each runtime service onto a dedicated least-privilege service account (`gen-ai-sa`, `search-api-sa`, `psearch-ui-sa`, `ingestion-source-sa`) while trimming `ingestion-service-account`. Add fail-closed authentication (`X-Admin-Token` / `X-API-Key` + allowlisted Google OIDC `Bearer` tokens) and non-wildcard CORS to `src/psearch/gen_ai` (FastAPI) and `src/psearch/serving` (Go/Gin), and propagate auth headers in `src/application/ui`.

**Tech Stack:** Terraform (`hashicorp/google` >= 5.10), Python 3.11+ (FastAPI, Pydantic v2, `google-auth`, `pytest`, `httpx`, `python-hcl2`), Go 1.26 (Gin, `google.golang.org/api/idtoken`), React / Axios.

**Spec:** `docs/superpowers/specs/2026-11-17-cloud-run-genai-auth-design.md`

## Global Constraints

- Zero `"allUsers"` or `"allAuthenticatedUsers"` bindings or defaults may remain in `src/iac/modules/{gen_ai,ingestion_source,search_api,ui}` or `src/iac/{main.tf,variables.tf}`; `invoker_members` defaults to `[]` and `ingress` defaults to `"INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"` on all four Cloud Run services.
- Every operational endpoint in `src/psearch/gen_ai/main.py` (`/conversational-search`, `/enrichment`, `/marketing`, `/generate-enhanced-image`, `/generate-sql`, `/sql/task-status/{task_id}`, `/sql/tasks-summary`, `/sql/simple-fix`, `/sql/validate`, `/sql/fix`, `/sql/analyze`) must enforce `require_auth`; only `GET /` and `GET /health` remain public, and `/docs`, `/redoc`, `/openapi.json` are disabled.
- `gen_ai` authentication fails closed (`403`) when neither `GEN_AI_ADMIN_TOKEN`/`ADMIN_TOKEN` nor (`GEN_AI_AUTH_AUDIENCE` + `GEN_AI_ALLOWED_PRINCIPALS` or `GEN_AI_ALLOWED_DOMAINS`) is configured.
- `psearch-serving` (`src/psearch/serving`) enables `RequireAPIAuth` by default when `ENVIRONMENT == "production"` and `!USE_MEMORY_STORE` (or when `API_TOKEN` / `SERVING_AUTH_AUDIENCE` is configured), gating all non-health API routes while keeping `authorizeAdmin` (`X-Admin-Token == ADMIN_TOKEN`) strictly required for admin writes.
- Wildcard `"*"` origins must never be enabled alongside `allow_credentials=True` / `AllowCredentials: true` in either `gen_ai` or `psearch-serving`.

## Review Focus

1. Importing `psearch.gen_ai.main` or `gen_ai.main` when `PROJECT_ID` and GCP ADC credentials are absent must succeed without instantiating `genai.Client(vertexai=True)` or `bigquery.Client` at module import time.
2. On `psearch-serving`, a caller presenting a valid search `X-API-Key` (`API_TOKEN`) must be allowed on `GET /api/search` (`200`) yet **rejected (`403`)** on admin write routes (`POST /api/rules`, `POST /api/synonyms`, `POST /api/config/embedding-version`) unless a valid `X-Admin-Token` (`ADMIN_TOKEN`) is provided.
3. On both `gen_ai` (`auth.py`) and `psearch-serving` (`auth.go`), a validly signed Google OIDC `Bearer` token matching the configured audience must still be rejected (`403`) if `email_verified` is not boolean `true`, or if domain allowlisting is used without a matching Google Workspace `hd` claim (`hd == email_domain`).
4. `GET /sql/task-status/{task_id}` in `src/psearch/gen_ai/main.py` must reject non-UUIDv4 `task_id` values with HTTP `400` before querying `task_manager`, and oversized SQL inputs (`> MAX_GENERATED_SQL_CHARS = 200_000`) on `/sql/simple-fix`, `/sql/validate`, `/sql/fix`, and `/sql/analyze` must return `422`.
5. `gen_ai_service_account` (`gen-ai-sa`) in Terraform must receive `roles/bigquery.dataViewer` on `psearch_raw` and `psearch` (`depends_on = [module.ingestion_spanner]`) plus project-level `roles/bigquery.jobUser` + `roles/aiplatform.user` so `/generate-sql` sample reads (`tabledata.list`) and dry-run validations succeed in GCP without granting BigQuery write/truncate privileges.

---

### Task 1: Lock Down All Cloud Run Modules (`gen_ai`, `search_api`, `ui`, `ingestion_source`) & Isolate Runtime Service Accounts

**Files:**
- Modify: `src/iac/modules/iam/main.tf`
- Modify: `src/iac/modules/iam/outputs.tf`
- Modify: `src/iac/modules/spanner/outputs.tf` (if needed for database name) and `src/iac/modules/spanner/main.tf`
- Modify: `src/iac/modules/gen_ai/variables.tf`
- Modify: `src/iac/modules/gen_ai/main.tf`
- Modify: `src/iac/modules/search_api/variables.tf`
- Modify: `src/iac/modules/search_api/main.tf`
- Modify: `src/iac/modules/ui/variables.tf`
- Modify: `src/iac/modules/ui/main.tf`
- Modify: `src/iac/modules/ingestion_source/variables.tf`
- Modify: `src/iac/modules/ingestion_source/main.tf`
- Modify: `src/iac/variables.tf`
- Modify: `src/iac/main.tf`
- Modify: `src/psearch/ingestion_source/tests/test_iac_security.py`

**Interfaces:**
- Consumes: existing `ingestion-source-sa` and `psearch_raw` bucket resources in `src/iac/`
- Produces:
  - IAM outputs: `gen_ai_service_account_email`, `search_api_service_account_email`, `ui_service_account_email`, `ingestion_source_service_account_email`
  - Cloud Run module variables (`gen_ai`, `search_api`, `ui`, `ingestion_source`): `invoker_members` (`default = []`, rejecting `allUsers`/`allAuthenticatedUsers`) and `ingress` (`default = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"`)
  - Runtime auth/CORS env blocks on `gen_ai` (`GEN_AI_AUTH_AUDIENCE`, `GEN_AI_ALLOWED_PRINCIPALS`, `GEN_AI_ALLOWED_DOMAINS`, `ALLOWED_ORIGINS`) and `search_api` (`REQUIRE_API_AUTH`, `SERVING_AUTH_AUDIENCE`, `SERVING_ALLOWED_PRINCIPALS`, `SERVING_ALLOWED_DOMAINS`, `ALLOWED_ORIGINS`)

- [ ] **Step 1: Extend `test_iac_security.py` with failing whole-repo IaC security tests**

Add tests in `src/psearch/ingestion_source/tests/test_iac_security.py` asserting:
- `test_no_cloud_run_module_allows_all_users_and_all_enforce_ingress`: across all 4 modules (`gen_ai`, `ingestion_source`, `search_api`, `ui`), `"allUsers"` and `"allAuthenticatedUsers"` never appear in resource/default definitions, no `google_cloud_run_v2_service_iam_policy` or `google_cloud_run_v2_service_iam_binding` exists, `google_cloud_run_v2_service_iam_member.invokers` uses `for_each = toset(var.invoker_members)` with `default = []` and public-principal validation, and every `google_cloud_run_v2_service` sets `ingress = "${var.ingress}"` defaulting to `"INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"`.
- `test_dedicated_least_privilege_service_accounts_for_every_runtime_service` (Review Focus #5):
  - `gen-ai-sa`, `search-api-sa`, `psearch-ui-sa`, and `ingestion-source-sa` exist in `modules/iam/main.tf` and are wired to `module.gen_ai`, `module.search_api`, `module.ui`, and `module.ingestion_source` in `src/iac/main.tf`.
  - `gen-ai-sa` has project roles `{"roles/aiplatform.user", "roles/bigquery.jobUser", "roles/logging.logWriter", "roles/serviceusage.serviceUsageConsumer"}` + dataset-scoped `roles/bigquery.dataViewer` on `psearch_raw` and `psearch` in `src/iac/main.tf`.
  - `search-api-sa` has project roles `{"roles/aiplatform.user", "roles/logging.logWriter", "roles/serviceusage.serviceUsageConsumer"}` + database-scoped `google_spanner_database_iam_member` (`roles/spanner.databaseUser`).
  - `psearch-ui-sa` has only `{"roles/logging.logWriter"}`.
  - `ingestion_service_account` (`grant_sa_function_roles`) no longer holds `roles/storage.objectAdmin`, `roles/spanner.databaseAdmin`, `roles/datastore.owner`, `roles/cloudfunctions.developer`, `roles/run.developer`, or `roles/cloudbuild.builds.builder`.

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src uv run --with pytest --with python-hcl2 pytest -W default -q src/psearch/ingestion_source/tests/test_iac_security.py`
Expected: FAIL (`gen_ai` defaults to `["allUsers"]`, `search_api` and `ui` still have `noauth` policy with `allUsers`, and shared `ingestion_service_account` is still wired)

- [ ] **Step 3: Implement the Terraform changes across `iam`, `spanner`, `gen_ai`, `search_api`, `ui`, `ingestion_source`, and root `iac`**

Update the `.tf` files listed above with dedicated SAs, scoped dataset/database IAM bindings, `invoker_members = []` + public-principal validation, `ingress = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"`, and root pass-through variables.

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src uv run --with pytest --with python-hcl2 pytest -W default -q src/psearch/ingestion_source/tests/test_iac_security.py`
Expected: PASS (`0 warnings`)

- [ ] **Step 5: Commit**

```bash
git add src/iac/ src/psearch/ingestion_source/tests/test_iac_security.py
git commit -m "fix(iac): remove allUsers across all Cloud Run services and isolate runtime SAs"
```

---

### Task 2: `gen_ai` FastAPI Fail-Closed Authentication, DI Lazy-Init, CORS & Route Hardening

**Files:**
- Create: `src/psearch/gen_ai/__init__.py`
- Create: `src/psearch/gen_ai/auth.py`
- Modify: `src/psearch/gen_ai/requirements.txt`
- Modify: `src/psearch/gen_ai/main.py`
- Create: `src/psearch/gen_ai/tests/__init__.py`
- Create: `src/psearch/gen_ai/tests/test_gen_ai_auth_and_api.py`

**Interfaces:**
- Consumes: `src/psearch/gen_ai/services/*` and `src/psearch/gen_ai/tasks/task_manager.py`
- Produces:
  - `src/psearch/gen_ai/auth.py`: `AuthPrincipal`, `verify_bearer_token(token: str, audience: str) -> Dict[str, Any]`, `require_auth(...) -> AuthPrincipal` (reading `GEN_AI_ADMIN_TOKEN` / `ADMIN_TOKEN`, `GEN_AI_AUTH_AUDIENCE`, `GEN_AI_ALLOWED_PRINCIPALS`, `GEN_AI_ALLOWED_DOMAINS`)
  - `src/psearch/gen_ai/main.py`: `create_app() -> FastAPI`, `app = create_app()`, lazy/DI service factories (`get_conversational_search_service`, `get_enrichment_service`, `get_marketing_service`, `get_image_generation_service`, `get_sql_fix_service`, `get_transformation_pipeline`), `protected_router` gating all 11 operational endpoints, public `GET /` and `GET /health`, OpenAPI/docs disabled.

- [ ] **Step 1: Write the failing `gen_ai` authentication & API security tests**

Create `src/psearch/gen_ai/tests/test_gen_ai_auth_and_api.py` testing:
- `test_import_gen_ai_main_succeeds_without_gcp_credentials_or_project_id` (Review Focus #1).
- `test_health_routes_are_public_and_openapi_docs_are_disabled`: `GET /` and `GET /health` return `200` unauthenticated; `/openapi.json`, `/docs`, `/docs/oauth2-redirect`, `/redoc` return `404`.
- `test_all_eleven_operational_routes_enforce_fail_closed_auth`: parametrize across all 11 routes (`POST /conversational-search`, `POST /enrichment`, `POST /marketing`, `POST /generate-enhanced-image`, `POST /generate-sql`, `GET /sql/task-status/<uuid>`, `GET /sql/tasks-summary`, `POST /sql/simple-fix`, `POST /sql/validate`, `POST /sql/fix`, `POST /sql/analyze`):
  - Unconfigured env → `403`.
  - `GEN_AI_ADMIN_TOKEN="secret"` with missing header → `401`; wrong `X-Admin-Token` → `403`; valid `X-Admin-Token` → `200`.
- `test_oidc_bearer_enforces_audience_verified_email_and_principal_or_hd_domain` (Review Focus #3): valid allowlisted email or matching `hd == email_domain` succeeds (`200`); `email_verified=False`, wrong issuer, missing `hd` on domain match, or unlisted Gmail user returns `401`/`403`.
- `test_task_status_validates_uuidv4_and_sql_endpoints_bound_length_and_sanitize_errors` (Review Focus #4): non-UUIDv4 `task_id` returns `400`; `sql_script` > `200_000` chars returns `422`; downstream internal `RuntimeError("secret://internal-url")` returns sanitized `500` without leaking `str(e)`; `ALLOWED_ORIGINS="*"` is ignored.

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src uv run --with pytest --with httpx --with fastapi --with pydantic --with google-auth --with requests --with google-cloud-storage --with google-cloud-bigquery --with google-genai --with pillow pytest -W default -q src/psearch/gen_ai/tests/test_gen_ai_auth_and_api.py`
Expected: FAIL (`ModuleNotFoundError: No module named 'psearch.gen_ai.auth'` / import-time `genai.Client` failure / unauthenticated `200`s)

- [ ] **Step 3: Implement `gen_ai/auth.py`, `requirements.txt`, and `gen_ai/main.py` hardening**

Implement `auth.py`, add `google-auth>=2.27.0` to `src/psearch/gen_ai/requirements.txt`, and refactor `src/psearch/gen_ai/main.py` with `create_app()`, DI service factories, `protected_router` across all 11 operational endpoints, UUIDv4 `task_id` validation, `max_length=MAX_GENERATED_SQL_CHARS` bounds on SQL inputs, non-wildcard `ALLOWED_ORIGINS` CORS, `docs_url=None, redoc_url=None, openapi_url=None`, and sanitized `500` responses (preserving dual import compatibility under both `PYTHONPATH=src` and `PYTHONPATH=src/psearch`).

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src uv run --with pytest --with httpx --with fastapi --with pydantic --with google-auth --with requests --with google-cloud-storage --with google-cloud-bigquery --with google-genai --with pillow pytest -W default -q src/psearch/gen_ai/tests/ src/psearch/gen_ai/services/sql/common/test_input_validation.py && PYTHONPATH=src/psearch uv run --with pytest --with httpx --with fastapi --with pydantic --with google-auth --with requests --with google-cloud-storage --with google-cloud-bigquery --with google-genai --with pillow pytest -W default -q src/psearch/gen_ai/tests/`
Expected: PASS (`0 warnings`)

- [ ] **Step 5: Commit**

```bash
git add src/psearch/gen_ai/__init__.py src/psearch/gen_ai/auth.py src/psearch/gen_ai/requirements.txt src/psearch/gen_ai/main.py src/psearch/gen_ai/tests/
git commit -m "fix(gen_ai): enforce fail-closed route authentication, DI lazy-init, and error sanitization"
```

---

### Task 3: `psearch-serving` (Go/Gin) Production Fail-Closed API Auth & CORS Hardening

**Files:**
- Modify: `src/psearch/serving/internal/config/config.go`
- Modify: `src/psearch/serving/internal/config/config_test.go`
- Create: `src/psearch/serving/internal/api/auth.go`
- Create: `src/psearch/serving/internal/api/auth_test.go`
- Modify: `src/psearch/serving/internal/api/routes.go`
- Modify: `DEPLOYMENT.md`
- Modify: `DESIGN.md`

**Interfaces:**
- Consumes: `google.golang.org/api/idtoken`, `crypto/subtle`, `github.com/gin-gonic/gin`
- Produces:
  - `Config` fields in `config.go`: `RequireAPIAuth bool`, `APIToken string`, `AuthAudience string`, `AllowedPrincipals []string`, `AllowedDomains []string`, `AllowedOrigins []string`
  - `TokenClaims` struct & `TokenVerifier func(ctx context.Context, token, audience string) (*TokenClaims, error)` + `AuthorizeAPI(cfg *config.Config, verifier TokenVerifier) gin.HandlerFunc` in `internal/api/auth.go`
  - Updated `SetupRouter` / `SetupRouterWithVerifier` in `internal/api/routes.go`

- [ ] **Step 1: Write failing Go tests in `config_test.go` and `auth_test.go`**

Write tests verifying:
- `TestLoadDefaultsRequireAPIAuthInProduction`: `config.Load()` with default `ENVIRONMENT=production` (`USE_MEMORY_STORE` unset) sets `RequireAPIAuth == true` and filters `"*"` out of `ALLOWED_ORIGINS`; with `USE_MEMORY_STORE=true` and `REQUIRE_API_AUTH` unset, `RequireAPIAuth == false`.
- `TestAuthorizeAPIFailsClosedInProductionWhenUnconfigured`: `Config{Environment: "production", RequireAPIAuth: true, UseMemoryStore: true}` rejects unauthenticated `GET /api/search`, `GET /api/info`, `GET /api/autocomplete`, `POST /api/session/events`, `GET /api/rules`, `GET /api/synonyms` with `403`, while `GET /health`, `GET /healthz`, and `GET /api/health` return `200`.
- `TestAuthorizeAPIAcceptsAPITokenButBlocksAdminWrites` (Review Focus #2): with `APIToken: "read-key"` and `AdminToken: "admin-key"`, `X-API-Key: read-key` succeeds (`200`) on `GET /api/search` and `GET /api/rules`, is rejected (`403`) on `POST /api/rules`, `POST /api/synonyms`, and `POST /api/config/embedding-version`, whereas `X-Admin-Token: admin-key` succeeds on both read and admin-write routes; missing credentials return `401` with `WWW-Authenticate: Bearer`.
- `TestAuthorizeAPIOIDCEnforcesVerifiedEmailAndDomainHD` (Review Focus #3): stubbing `TokenVerifier`, allowlisted email or matching `hd == email_domain` returns `200`, while `EmailVerified: false`, bad issuer, missing `hd`, or unlisted caller returns `401`/`403`.
- `TestCORSBlocksWildcardOriginReflection`: with `AllowedOrigins: nil` or `ALLOWED_ORIGINS="*"`, `Origin: https://evil.example` receives no `Access-Control-Allow-Origin` / `Access-Control-Allow-Credentials` header; explicit `AllowedOrigins: []string{"https://ui.example.com"}` allows only that origin.

- [ ] **Step 2: Run Go tests to verify they fail**

Run: `cd src/psearch/serving && go test ./internal/config ./internal/api`
Expected: FAIL (missing `Config` fields / `AuthorizeAPI` undefined)

- [ ] **Step 3: Implement `config.go`, `auth.go`, `routes.go`, and update `DEPLOYMENT.md` / `DESIGN.md`**

- Update `config.go` to parse `REQUIRE_API_AUTH`, `API_TOKEN`, `SERVING_AUTH_AUDIENCE`, `SERVING_ALLOWED_PRINCIPALS`, `SERVING_ALLOWED_DOMAINS`, and `ALLOWED_ORIGINS` (ignoring `"*"`).
- Implement `internal/api/auth.go` using `crypto/subtle.ConstantTimeCompare` and `google.golang.org/api/idtoken.Validate`.
- Update `internal/api/routes.go` to configure CORS only for explicit non-wildcard `cfg.AllowedOrigins`, attach `AuthorizeAPI` to all non-health API routes while keeping `/health`, `/healthz`, `/api/health`, and `/` public, and attach optional `sessionStorage`/`localStorage` auth headers in `defaultStorefrontHTML`'s `fetch` calls.
- Document the new auth/CORS env vars in `DEPLOYMENT.md` and `DESIGN.md`.

- [ ] **Step 4: Run full Go vet & race test suite to verify it passes**

Run: `cd src/psearch/serving && go vet ./... && go test -race ./...`
Expected: PASS (all new and existing `api`, `config`, and `services` tests pass, including `TestStorefrontInlineJSParses`)

- [ ] **Step 5: Commit**

```bash
git add src/psearch/serving/ DEPLOYMENT.md DESIGN.md
git commit -m "fix(serving): enforce production API authentication and non-wildcard CORS"
```

---

### Task 4: React UI Auth Header Propagation (`genAiService` & Serving Clients)

**Files:**
- Create: `src/application/ui/src/services/apiAuth.js`
- Modify: `src/application/ui/src/services/genAiService.js`
- Modify: `src/application/ui/src/services/sessionService.js`
- Modify: `src/application/ui/src/services/ruleService.js`
- Modify: `src/application/ui/src/App.js`
- Modify: `src/application/ui/src/components/SearchInput.js`
- Create: `src/psearch/gen_ai/tests/test_ui_auth_headers.py`

**Interfaces:**
- Consumes: browser `localStorage` / `sessionStorage` and `process.env`
- Produces:
  - `getGenAiAuthHeaders()` (`Authorization: Bearer <genAiAuthToken || psearchAuthToken>`, `X-Admin-Token: <genAiAdminToken || REACT_APP_GEN_AI_ADMIN_TOKEN>`)
  - `getServingAuthHeaders()` (`Authorization: Bearer <psearchAuthToken>`, `X-API-Key: <psearchApiToken || REACT_APP_API_TOKEN>`)

- [ ] **Step 1: Write failing Node/pytest verification tests for UI auth header propagation**

Create `src/psearch/gen_ai/tests/test_ui_auth_headers.py` running `node` in a subprocess to verify:
- `apiAuth.js` exports `getGenAiAuthHeaders` and `getServingAuthHeaders` safely when `localStorage` throws or `process` is undefined.
- Every `axios` call to `GEN_AI_URL` in `genAiService.js` includes `...getGenAiAuthHeaders()`.
- `sessionService.js`, `ruleService.js`, `App.js`, and `SearchInput.js` include `getServingAuthHeaders()` on `API_URL` requests while `ruleService.js` preserves explicit `X-Admin-Token` precedence on admin rule mutations.

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src uv run --with pytest pytest -W default -q src/psearch/gen_ai/tests/test_ui_auth_headers.py`
Expected: FAIL (`apiAuth.js` does not exist / `genAiService.js` sends no auth headers)

- [ ] **Step 3: Implement `apiAuth.js` and wire headers across UI service & component callers**

Create `src/application/ui/src/services/apiAuth.js` and update `genAiService.js`, `sessionService.js`, `ruleService.js`, `App.js`, and `SearchInput.js`.

- [ ] **Step 4: Run full cross-repo verification suite**

Run:
```bash
PYTHONPATH=src uv run --with pytest --with httpx --with python-hcl2 --with fastapi --with pydantic --with python-multipart --with google-auth --with requests --with google-cloud-storage --with google-cloud-bigquery --with pandas --with google-genai --with pillow pytest -W default -q src/psearch/ingestion_source/tests/ src/psearch/gen_ai/tests/ src/psearch/gen_ai/services/sql/common/test_input_validation.py && (cd src/psearch/serving && go vet ./... && go test -race ./...)
```
Expected: All Python, Node/UI, HCL/Terraform, and Go suites PASS with `0 warnings`.

- [ ] **Step 5: Commit**

```bash
git add src/application/ui/src/ src/psearch/gen_ai/tests/test_ui_auth_headers.py
git commit -m "feat(ui): propagate auth headers across GenAI and Search API clients"
```

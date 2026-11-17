# Multi-Service Cloud Run IAM, Ingress, and Application-Layer Authentication Hardening Design

## Intent & Problem Statement

Across the PSearch repository (`commit 29da444355ed68904063e482825919e926fffa12`), Terraform Cloud Run modules (`gen_ai`, `ingestion_source`, `search_api`, and `ui`) exposed `roles/run.invoker` to `allUsers` with unrestricted ingress while sharing the over-privileged `ingestion-service-account` (`roles/bigquery.user`, `roles/storage.objectAdmin`, `roles/spanner.databaseAdmin`, `roles/datastore.owner`, `roles/cloudfunctions.developer`, `roles/run.developer`). At the application layer, `src/psearch/gen_ai/main.py` implemented zero authentication on any route (`/conversational-search`, `/enrichment`, `/marketing`, `/generate-enhanced-image`, `/generate-sql`, `/sql/task-status/{task_id}`, `/sql/tasks-summary`, `/sql/simple-fix`, `/sql/validate`, `/sql/fix`, `/sql/analyze`), and `src/psearch/serving/internal/api/routes.go` gated only admin writes (`X-Admin-Token`), leaving Vertex AI embedding/LLM and Spanner endpoints unauthenticated.

Building on the `ingestion_source` remediation (`fix/ingestion-source-auth`), this design closes the remaining S1 `CWE-306` / `TP_CALLER_SET_TOO_BROAD` attack surface across `gen_ai`, `search_api` (`psearch-serving`), `ui`, and `iam`.

## Goals

1. **Lock down Cloud Run IAM and ingress across every Terraform service module (`gen_ai`, `ingestion_source`, `search_api`, `ui`)**:
   - Default `invoker_members = []` on all four Cloud Run modules and forbid `"allUsers"` and `"allAuthenticatedUsers"` via Terraform variable validations.
   - Default `ingress = "INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER"` on all four `google_cloud_run_v2_service` resources (validated against Cloud Run v2 ingress modes).
2. **Isolate every runtime service onto its own least-privilege Service Account (`src/iac/modules/iam/`)**:
   - `gen-ai-sa`: project-level `roles/aiplatform.user`, `roles/bigquery.jobUser`, `roles/logging.logWriter`, `roles/serviceusage.serviceUsageConsumer`, plus dataset-level `roles/bigquery.dataViewer` on `psearch_raw` and `psearch` (read-only sample/dry-run access; zero BigQuery data write, GCS, Spanner, or Datastore privileges).
   - `search-api-sa`: project-level `roles/aiplatform.user`, `roles/logging.logWriter`, `roles/serviceusage.serviceUsageConsumer`, plus database-level `roles/spanner.databaseUser` on `products-db`.
   - `psearch-ui-sa`: project-level `roles/logging.logWriter` only.
   - Trim `ingestion-service-account` (used only by `spanner` and `ingestion_spanner` scheduled BQ → Spanner Reverse ETL) down to `roles/bigquery.user`, `roles/spanner.databaseUser`, `roles/aiplatform.user`, `roles/logging.logWriter`, and `roles/serviceusage.serviceUsageConsumer`, dropping `storage.objectAdmin`, `spanner.databaseAdmin`, `datastore.owner`, `cloudfunctions.developer`, `run.developer`, and `cloudbuild.*`.
3. **Enforce fail-closed FastAPI authentication and boundary hardening in `src/psearch/gen_ai/`**:
   - Require `require_auth` (`GEN_AI_ADMIN_TOKEN` / `ADMIN_TOKEN` via `X-Admin-Token` or Google OIDC `Bearer` token with `GEN_AI_AUTH_AUDIENCE` + `GEN_AI_ALLOWED_PRINCIPALS` / `GEN_AI_ALLOWED_DOMAINS`) across all 11 operational endpoints on `protected_router`; fail closed (`403`) when unconfigured.
   - Keep only `GET /` and `GET /health` public, disable `/docs`, `/redoc`, and `/openapi.json`, restrict CORS via explicit non-wildcard `ALLOWED_ORIGINS`, convert import-time GCP client singletons to FastAPI `Depends(...)` factories, validate `task_id` as UUIDv4, bound free-form SQL payload lengths, and sanitize `500` error responses.
4. **Enforce production fail-closed API authentication and CORS hardening in `psearch-serving` (`src/psearch/serving/`)**:
   - Gate all non-health API routes (`/api/info`, `/api/config/embedding-version`, `/api/synonyms`, `/api/autocomplete`, `/api/session/events`, `/api/rules`, `/search`, `/api/search`) behind `AuthorizeAPI` (`X-API-Key == API_TOKEN`, `X-Admin-Token == ADMIN_TOKEN`, or allowlisted Google OIDC `Bearer` token via `google.golang.org/api/idtoken` against `SERVING_AUTH_AUDIENCE` + `SERVING_ALLOWED_PRINCIPALS` / `SERVING_ALLOWED_DOMAINS`).
   - Enable `RequireAPIAuth` by default whenever `ENVIRONMENT == "production"` and `!USE_MEMORY_STORE` (or when `API_TOKEN` / `SERVING_AUTH_AUDIENCE` is set), failing closed (`403`) if unconfigured in production while preserving existing offline memory-store tests (`UseMemoryStore: true`).
   - Preserve separate `authorizeAdmin` (`X-Admin-Token == ADMIN_TOKEN`) on admin write endpoints so read-only `X-API-Key` callers cannot mutate rules, synonyms, or embedding versions.
   - Replace wildcard credentialed CORS (`AllowOrigins: ["*"]`, `AllowCredentials: true`) with explicit `ALLOWED_ORIGINS` (filtering `"*"`).
5. **Propagate optional auth headers in the React UI and embedded storefront (`src/application/ui/`, `routes.go`)**:
   - Attach configured `Authorization: Bearer` / `X-Admin-Token` / `X-API-Key` headers in `genAiService.js`, `sessionService.js`, `ruleService.js`, `App.js`, `SearchInput.js`, and the embedded storefront script in `routes.go`.

## Non-Goals & Anti-Goals

- **Non-goal**: Replacing the in-memory `task_manager` store in `gen_ai` with external database persistence.
- **Anti-goal**: Leaving `"allUsers"` or `"allAuthenticatedUsers"` as a default or valid `invoker_members` entry on any Cloud Run module in `src/iac/modules/`.
- **Anti-goal**: Allowing a search-only `X-API-Key` (`API_TOKEN`) to bypass `X-Admin-Token` (`ADMIN_TOKEN`) on `psearch-serving` admin write routes.

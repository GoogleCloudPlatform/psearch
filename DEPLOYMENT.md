# PSearch — Deployment Guide

Real project IDs, service-account emails and service URLs are intentionally replaced with placeholders in this document:

| Placeholder | Meaning |
|---|---|
| `<PROJECT_ID>` / `<PROJECT_NUMBER>` | Google Cloud project |
| `<SERVICE_URL>` | `psearch-serving` Cloud Run URL |
| `<RUNTIME_SA>` | dedicated runtime service account `psearch-serving@<PROJECT_ID>.iam.gserviceaccount.com` (roles: `roles/aiplatform.user` on the project, `roles/spanner.databaseUser` on `psearch-db`) |

## 1. Environment

| Item | Value |
|---|---|
| Region | `us-central1` (Cloud Run); Vertex AI calls use location `global` |
| Cloud Spanner | instance `psearch-instance` (Enterprise, 100 PU), database `psearch-db` |
| BigQuery | dataset `psearch_dataset` (no tables yet; query logging is a follow-up) |
| Container | root `Dockerfile` (multi-stage `golang:1.26-alpine` → `alpine:3.20`, binary `/app/psearch-server`, port 8080) |

## 2. Runtime configuration (live revision)

| Variable | Value | Notes |
|---|---|---|
| `PROJECT_ID` / `GOOGLE_CLOUD_PROJECT` / `CLOUDSDK_CORE_PROJECT` | `<PROJECT_ID>` | `ProjectID` falls back through these |
| `SPANNER_INSTANCE_ID` / `SPANNER_DATABASE_ID` | `psearch-instance` / `psearch-db` | |
| `ACTIVE_EMBEDDING_VERSION` | `v2` | 1024-d `embedding_v2` |
| `GENAI_LOCATION` | `global` | |
| `EMBEDDING_MODEL_NAME` / `EMBEDDING_DIMENSION` | `gemini-embedding-2` / `1024` | |
| `TEXT_FOLDING` | `true` | accent-folded tokenlists (migration 014b) |
| `SPANNER_DISABLE_BUILTIN_METRICS` | `true` | silences client-metrics export (runtime SA has no monitoring role) |
| `GEMINI_TEXT_MODEL` / `GEMINI_PRO_MODEL` | defaults `gemini-3.8-flash` / `gemini-4-pro` | reported by `/api/info` as `text_model` / `pro_model` |
| `INTENT_LLM_FALLBACK` | unset (`false`) | parser-only query understanding |
| `ADMIN_TOKEN` | unset | admin writes (rules, synonyms, embedding-version switch) return 403 |
| `BOOST_WEIGHT`, `SESSION_BOOST_WEIGHT`, `LTR_MODEL_JSON` | defaults (`0.002`, `0.003`, baseline model) | |

Service account: `<RUNTIME_SA>`. Full variable reference: [DESIGN.md §16](DESIGN.md#16-configuration-environment).

## 3. Spanner schema migrations

The base schema (`products` with 768-d `embedding`, `title_tokens`, `description_tokens`, search index `products_text_idx`) predates the numbered migrations and is defined in `src/iac/modules/spanner`. Numbered migrations live in `src/iac/migrations/` and are applied online with `gcloud spanner databases ddl update psearch-db --instance=psearch-instance --ddl-file=<file>` (DDL) or `gcloud spanner databases execute-sql` (DML, comment lines stripped).

| Migration | Kind | Issue | Change |
|---|---|---|---|
| `011_add_embedding_v2_gemini_1024.sql` | DDL | #77 / #79 | `products.embedding_v2 ARRAY<FLOAT32>(vector_length=>1024)` (backfilled by `cmd/backfill-embeddings`) |
| `012_availability_visibility_channels.sql` | DDL | #80 | `is_visible`, `channels` |
| `012b_backfill_visibility_channels.dml.sql` | DML | #80 | visibility + channels for the seed catalog |
| `013_embedding_cache.sql` | DDL | #79 | `embedding_cache` |
| `014_tenant_locale.sql` | DDL | #81 | `tenant_id`, `locale` |
| `014b_accent_folded_tokens.sql` | DDL | #81 | `title_tokens_folded`, `description_tokens_folded` + `ALTER SEARCH INDEX products_text_idx` (applied in two steps) |
| `014c_backfill_tenant_locale.dml.sql` | DDL | #81 | tenant/locale from `product_data` |
| `015_search_synonyms.sql` / `015b_seed_synonyms.dml.sql` | DDL / DML | #84 | `search_synonyms` + 8 seed rows |
| `016_merchandising.sql` / `016b_backfill_boost_signals.dml.sql` | DDL / DML | #83 | 8 signal columns + `merchandising_rules`; signal backfill |
| `017_delivery_interleaved.sql` | DDL | #85 | `skus` (interleaved in `products`), `regional_inventory` (interleaved in `skus`) |
| `017b_seed_skus.dml.sql` / `017c_seed_regional_inventory.dml.sql` | DML | #85 | 15 SKUs, 57 inventory rows (5 CD regions) |
| `018_autocomplete.sql` / `018b_seed_autocomplete.dml.sql` | DDL / DML | #86 | `autocomplete_suggestions` + `autocomplete_ngram_idx`; 48 bootstrap suggestions |
| `019_b2b_product_prices.sql` / `019b_seed_product_prices.dml.sql` | DDL / DML | #91 | `product_prices` (interleaved in `products`); 45 contract prices |
| `020_query_intent_cache.sql` | DDL | #88 | `query_intent_cache` |
| `021_user_session_events.sql` | DDL | #89 | `user_session_events` |
| `022_sku_variants.sql` / `022b_seed_sku_variants_std.dml.sql` / `022c_seed_sku_variants.dml.sql` | DDL / DML | #93 | `skus` variant columns (`color`, `size`, `stock`, `is_available`) + variant seed rows |

## 4. Release procedure

1. Code reviewed in an isolated worktree — never in the shared checkout.
2. Stage a clean snapshot: `git archive <sha> | tar -x -C /tmp/deploy-<sha>`.
3. Apply additive DDL/DML first (split multi-statement DDL so failures are attributable).
4. `go run ./cmd/sqlcheck` from the archive with the production env — every case must pass.
5. `gcloud run deploy psearch-serving --source /tmp/deploy-<sha> --region us-central1 --service-account <RUNTIME_SA> …`, then `gcloud run services update-traffic psearch-serving --to-latest` (`deploy` does not accept `--to-latest`; after any `--to-revisions` rollback the next rollout must restore `--to-latest`).
6. Verify live endpoints and logs; on failure `update-traffic --to-revisions=<previous>=100`.

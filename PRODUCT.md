# PSearch — Product Requirements Document

**Status:** All functional requirements delivered across Waves 1–6.
**Related:** [DESIGN.md](DESIGN.md) · [DEPLOYMENT.md](DEPLOYMENT.md)

---

## 1. Product vision

PSearch is a multi-tenant e-commerce search service that combines lexical and semantic retrieval in Cloud Spanner, enriches queries with Gemini models on Vertex AI, and exposes merchandising, personalization, delivery and B2B pricing controls through a single `/api/search` endpoint. Two front ends ship with it: a React application (`src/application/ui`) and a zero-dependency storefront embedded in the serving binary (`GET /`).

**Primary users**

| Persona | Needs |
|---|---|
| Shopper (B2C) | Relevant results for typo-ridden or conversational queries in pt-BR/en-US/es-MX, accurate stock and delivery promises, personalized ranking |
| B2B buyer | Contract prices, exclusive assortments, price-sorted results |
| Merchandiser | Pin/bury/boost rules, synonyms, transparent scoring |
| Search engineer | Tunable hybrid weight (`alpha`), LTR, observability fields (`retrieval_mode`, latencies, stage ranks) |

**Model platform (Issue #23):** text/flash `gemini-3.8-flash`, pro `gemini-4-argon` (aliases such as `gemini-4-pro` are normalized), embeddings `gemini-embedding-2` at 1024 dimensions, all on Vertex AI location `global` via the Google Gen AI SDK (`google.golang.org/genai`, `google-genai`).

---

## 2. Functional requirements

Each requirement lists the user-facing behavior, the API surface, acceptance evidence, and UI coverage (React = `src/application/ui/src`, Storefront = `defaultStorefrontHTML` in `src/psearch/serving/internal/api/routes.go`).

### 2.1 Hybrid search with parametrized `alpha` (Issue #1 · Wave 1)
- Reciprocal Rank Fusion of a vector leg and a full-text leg, weighted by `alpha` (`0` = text only, `1` = vector only, default `0.5` via `DEFAULT_HYBRID_ALPHA`). Out-of-range values are clamped.
- API: `alpha` (GET/POST); response `effective_alpha`, per result `score.{hybrid, vector, text, final_score, alpha}`.
- Acceptance: `q=running shoes` at `alpha=0` vs `alpha=1` produces different rankings; `alpha=1.7` → `effective_alpha=1`.
- UI: React alpha slider (`SearchInput.js`) and `effective_alpha` chip (`App.js`); storefront `#alpha` slider and per-card scores.

### 2.2 Vector search, 1024-d Blue/Green and embedding cache (Issues #2, #11 · Wave 1)
- `gemini-embedding-2` (1024-d, `RETRIEVAL_QUERY`) against `embedding_v2`; the legacy 768-d `embedding` column remains as `v1`. `ACTIVE_EMBEDDING_VERSION` selects the column; `GET /api/config/embedding-version` reports it and an admin-token `POST` switches it per instance.
- Two-tier embedding cache (in-process LRU of 1000 entries + Spanner `embedding_cache`), with `cache_hit` in responses and cache statistics in `/api/info`.
- Failures degrade to `retrieval_mode=lexical_only_degraded`, never a 5xx.
- UI: `retrieval_mode` chip (React); storefront backend/retrieval badges.

### 2.3 Availability, visibility and channels (Issue #3 · Wave 1)
- Hidden products (`is_visible=false`) are never returned; `channel` (`web|mobile|store`) and `only_in_stock` filter in both retrieval legs; unavailable items (`is_available=false` or `stock<=0`) are always ordered last; real `availability` / `availableQuantity` in results.
- UI: channel selector and "Only in stock" switch, `IN_STOCK (N unid.)` / `OUT_OF_STOCK` badges in both UIs.

### 2.4 Multi-language, multi-tenant catalog (Issue #4 · Wave 2)
- `tenant_id` (default `default`) and optional `locale` (`pt-BR`, `en-US`, `es-MX`; empty = all) filters; unknown tenants see nothing. Accent-insensitive search ("tenis" matches "Tênis") through accent-folded tokenlists (`TEXT_FOLDING=true`).
- Successful empty Spanner results are authoritative (no fallback to demo data), preventing tenant leakage.
- UI: tenant input and locale selector in both UIs, per-card locale badge.

### 2.5 Server-side facets (Issue #5 · Wave 2)
- Four concurrent Spanner `GROUP BY` aggregations (Go `errgroup`) over the top-100 hybrid candidates: `categories`, `brands`, `price_ranges` (`0-100`, `100-500`, `500-1500`, `1500+`), `availability`; `facets_latency_ms` and `candidates` in the response. Facet failures never fail a search. `category` / `brand` drill-down filters are applied server-side.
- UI: React `Filters.js` with server counts (client-side fallback); storefront facet sidebar.

### 2.6 Relevance & merchandising rules + 8 boost signals (Issue #6 · Wave 3)
- Signals on `products`: `clicks`, `orders`, `discount_pct`, `popularity_score`, `margin_pct`, `ctr`, `conversion_rate`, `recency_score`, normalized and scaled by `BOOST_WEIGHT` (default `0.002`).
- Rules (`merchandising_rules`): `query_pattern`, `target_brand`, `target_category`, `boost_multiplier` (0.1–10), `pin_product_ids` (≤20), `bury_product_ids` (≤100), `active`, tenant-scoped. Ranking: `final = rrf + boost + (rule_multiplier − 1)·rrf`; pins first, buries last, availability still dominant.
- API: `GET /api/rules` (public), `POST /api/rules`, `PUT/DELETE /api/rules/:id` (require `X-Admin-Token`); response `applied_rules`, `score.boost`, `score.rule_multiplier`.
- UI: React `RuleManager.js` / `ruleService.js` at `/manage` (admin token kept in `sessionStorage` only); applied-rules chip.

### 2.7 Low-relevance synonyms (Issue #7 · Wave 3)
- `search_synonyms` per tenant/locale (default weight `0.3`, must be in (0,1)); exact matches always rank above synonym-only matches in the text leg.
- API: `GET /api/synonyms` (public), `POST /api/synonyms` (admin token); response `expanded_terms`.
- UI: "Also matching: …" line in both UIs.

### 2.8 Delivery promises by CEP (Issue #8 · Wave 3)
- `products` → `skus` → `regional_inventory` (interleaved tables). `cep` (8 digits) restricts results to products deliverable to that CEP prefix within `max_sla_hours` (0–1440) and attaches the fastest in-stock `deliveryPromise` (`sla_hours`, `cd_id`, `seller_id`, `region_code`, `free_shipping`, `shipping_cost`, `label` such as "Chega amanhã - CD São Paulo"). Invalid CEP → 400.
- UI: "Calcular Entrega" CEP input, "Full 24h" switch, SLA and "Frete grátis" badges in both UIs.

### 2.9 Autocomplete (Issue #9 · Wave 4)
- `autocomplete_suggestions` with accent-folded n-grams (`TOKENIZE_NGRAMS` 2–4) and search index; prefix matches first, then `ctr_score`, then `search_count`; 30-second cache.
- API: `GET /api/autocomplete?q=&locale=&tenant_id=&limit=` (limit ≤ 20, `q` ≤ 100 chars).
- Data: catalog-derived bootstrap suggestions; a BigQuery export (`src/iac/bigquery_scripts/autocomplete_export_to_spanner.sql`) aggregates `search_query_logs` once query logging exists.
- UI: debounced typeahead with keyboard navigation (React); `<datalist>` (storefront).

### 2.10 Two-stage Learning to Rank (Issue #10 · Wave 4)
- Stage 1: Spanner hybrid top-100. Stage 2: in-memory Go model (bias + linear weights + boosted stumps) over 12 features; opt-in `ltr=true`. Learned weights can be loaded through `LTR_MODEL_JSON` (BigQuery ML script `src/iac/bigquery_scripts/ltr_train.sql`); baseline model `baseline-2026-10`.
- Response: `ltr`, `ltr_model`, `ltr_latency_us`, per result `score.{ltr_score, stage1_rank, stage2_rank}`.
- UI: "Re-rank (LTR)" toggle and rank-change badges in both UIs.

### 2.11 Conversational query understanding (Issue #19 · Wave 5)
- Parser-first decomposition (pt-BR/en/es price expressions incl. ranges and `1.500,00` formats, catalog brands and categories, colors) into `clean_query` + extracted filters (`min_price`, `max_price`, `brand`, `category`, `color`). On by default; `parse_intent=false` opts out; explicit request filters override extracted ones.
- Optional Gemini fallback (`gemini-3.8-flash`, structured JSON) for unparsed conversational queries, behind `INTENT_LLM_FALLBACK` (default off), with a two-tier cache (LRU + `query_intent_cache`).
- Response: `parsed_query {original_query, clean_query, extracted_filters, intent_source}`.
- UI: "Smart query" toggle and removable filter chips in both UIs.

### 2.12 Session-aware personalization (Issue #20 · Wave 5)
- `POST /api/session/events` (`click` | `purchase`, opaque `session_id` of 8–128 chars) and `GET /api/session/events`. Search accepts `session_id` or `session_product_ids`; results get `score.session_boost` from the session centroid; response `personalized`, `session_id`. No session → unchanged ranking.
- UI: React `SessionPanel.js` (active session, reset, demo personas Corredor / Tech / Audiófilo, recent events), click/purchase tracking, "Personalizado" badges; storefront session field and click recording.

### 2.13 Zero-results recovery & spell check (Issue #21 · Wave 5)
- Only when the lexical leg finds nothing: high-confidence spelling correction (`did_you_mean`, `auto_corrected=true`; e.g. "sansung" → "samsung", "geladera" → "geladeira"), including corrections that resolve to a brand/category entity; otherwise multi-term AND → OR relaxation (`query_relaxed=true`).
- UI: "Mostrando resultados para …", "Você quis dizer: …?" and relaxed-query banners (React; storefront shows corrected and relaxed banners).

### 2.14 B2B contract pricing & assortment (Issue #22 · Wave 5)
- `product_prices` interleaved under `products` with tables `b2b-corporate`, `b2b-gold`, `vip-club`. `price_table_id` resolves the buyer's effective price (contract or list) for results, `min_price`/`max_price`, price facets and `sort_by` (`relevance`, `price_asc`, `price_desc`); exclusive rows hide products from other buyers. `priceInfo.numericPrice` = effective price, `priceInfo.basePrice` = list price.
- UI: price-table and sort selectors; contract price with struck-through base price and "Contrato: …" label in both UIs.

---

## 3. Non-functional requirements

| Area | Requirement | How it is met |
|---|---|---|
| Correctness | Every Spanner query validated before deploy | `go run ./cmd/sqlcheck` |
| Resilience | Model, cache, rules, synonyms, facets and delivery failures never fail a search | Each path degrades to a safe default and logs a warning |
| Security | Bound SQL parameters only; admin writes gated | `X-Admin-Token` compared in constant time; writes disabled when `ADMIN_TOKEN` is unset |
| Privacy | No PII stored for personalization | `session_id` is an opaque client token; events hold product IDs only |
| Latency | Interactive search; LTR stage 2 < 2 ms for 100 candidates | Tested; observed ~20–50 µs stage 2, 6–47 ms facets |
| Testability | Every feature works without Spanner | Deterministic in-memory engine; `go test -race ./...` |

## 4. Out of scope / follow-ups

- Query/click logging to BigQuery (needed for analytics-driven autocomplete and trained LTR weights).
- `ADMIN_TOKEN` provisioning via Secret Manager to enable rule/synonym writes in production.
- NDCG@10 display on the evaluation dashboard; region facet; ScaNN vector index on `embedding_v2`; tenant-aware primary key; per-locale tokenizer language tags.

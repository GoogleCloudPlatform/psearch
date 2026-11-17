# `ingestion_source` Authentication, Least-Privilege IAM, and Endpoint Hardening Design

## Intent & Problem Statement

`ingestion-source-service` (`src/iac/modules/ingestion_source/main.tf` and `src/psearch/ingestion_source/main.py`) is currently deployed as a publicly invokable Cloud Run service (`allUsers` granted `roles/run.invoker`), running as the shared over-privileged `ingestion-service-account` (`roles/bigquery.user`, `roles/storage.objectAdmin`, `roles/spanner.databaseAdmin`, `roles/datastore.owner`, `roles/cloudfunctions.developer`, `roles/run.developer`), with zero application-level authentication on any FastAPI route.

As reported in VulnHunt S1 finding (`CWE-306` / `TP_CALLER_SET_TOO_BROAD`), any unauthenticated caller can:
1. Create arbitrary BigQuery datasets and tables (`POST /datasets`, `POST /tables`, `POST /ensure-dataset`).
2. Overwrite or truncate existing BigQuery tables via `WRITE_TRUNCATE` load jobs (`POST /create_and_load`, `POST /load`).
3. Upload unbounded CSV/JSON payloads into `gs://{project_id}_psearch_raw/` while reading full files into memory (`POST /upload`, `storage_service.py`, `schema_detection_service.py`).
4. Enumerate all GCS buckets in the GCP project (`GET /buckets`, `storage_service.py`).

## Goals

1. **Eliminate anonymous Cloud Run invocation by default**: Remove hardcoded `allUsers` from `ingestion_source` Terraform IAM (`invoker_members` defaults to `[]` and rejects `allUsers` / `allAuthenticatedUsers`).
2. **Isolate runtime IAM privileges**: Run `ingestion-source-service` under a dedicated service account (`ingestion-source-sa`) with project-level `roles/bigquery.user` + `roles/logging.logWriter` and bucket-level `roles/storage.objectAdmin` scoped strictly to `${project_id}_psearch_raw`.
3. **Enforce fail-closed FastAPI authentication**: Require authentication on every operational route in `src/psearch/ingestion_source/main.py` (leaving only `GET /` unauthenticated for Cloud Run health probes), supporting both constant-time `X-Admin-Token` (`INGESTION_ADMIN_TOKEN` / `ADMIN_TOKEN`) and Google OIDC Bearer tokens (`INGESTION_AUTH_AUDIENCE` + allowlisted `INGESTION_ALLOWED_PRINCIPALS` / `INGESTION_ALLOWED_DOMAINS`). Fail closed (`403`) when no auth configuration is present.
4. **Harden storage, BigQuery, and input boundaries**:
   - Enforce streaming upload byte caps (`MAX_UPLOAD_SIZE_BYTES`, default 50 MiB, HTTP `413`), guaranteed temporary file cleanup, and bounded CSV schema sampling without reading full files into memory.
   - Scope `GET /buckets` (`StorageService.list_buckets`) strictly to the ingestion bucket `{project_id}_psearch_raw` (`lookup_bucket`), never calling project-wide `client.list_buckets()`.
   - Default `LoadJobRequest.write_disposition` to `"WRITE_EMPTY"`, validate `write_disposition` against `{"WRITE_EMPTY", "WRITE_APPEND", "WRITE_TRUNCATE"}`, and block `"WRITE_TRUNCATE"` (`403`) unless `ALLOW_WRITE_TRUNCATE=true`.
   - Strictly validate `dataset_id`, `table_id`, and `file_id` (UUIDv4), reject cross-project dataset references, support optional `INGESTION_ALLOWED_DATASETS`, restrict wildcard CORS, and prevent `except Exception` blocks from turning HTTP `4xx` responses into `500`s.
5. **Preserve React UI compatibility**: Update `sourceIngestionService.js` and `SourceIngestion.js` to attach configured auth headers and default loads to `"WRITE_EMPTY"`.

## Non-Goals & Anti-Goals

- **Non-goal**: Modifying `search_api` or `gen_ai` Terraform modules or unrelated Spanner ingestion pipelines (`ingestion_spanner`).
- **Anti-goal**: Leaving `allUsers` or "any Google account" (`verify_oauth2_token` without principal/domain allowlisting) as an accepted default caller set.
- **Anti-goal**: Breaking mock-mode behavior in `src/application/ui` or changing response JSON shapes for authorized callers.

## Design Decisions (Builder Calls)

1. **Dual Auth (`X-Admin-Token` + Google OIDC Bearer)**: Matches `psearch-serving`'s existing `ADMIN_TOKEN` / `X-Admin-Token` pattern while also supporting IAM/IAP/Google Sign-In OIDC Bearer tokens with strict audience + principal/domain allowlisting.
2. **Fail-Closed Default (`403` when unconfigured)**: Mirrors `psearch-serving` (`handlers.go:authorizeAdmin`) so an unconfigured deployment never silently exposes privileged BigQuery/GCS routes.
3. **`WRITE_EMPTY` Default + `ALLOW_WRITE_TRUNCATE` Opt-In**: Satisfies the UI's upload-and-create workflow safely while preventing accidental or malicious table truncation unless explicitly enabled.
4. **Single-Bucket `lookup_bucket` for `GET /buckets`**: Preserves the `List[Dict[str, Any]]` API contract while eliminating project-wide bucket enumeration and removing the need for `storage.buckets.list` project IAM permission.

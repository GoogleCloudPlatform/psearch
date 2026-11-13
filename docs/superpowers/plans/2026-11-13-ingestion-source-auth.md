# Ingestion Source Security Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate unauthenticated access and privilege-boundary abuse in `ingestion_source` across Terraform IAM, FastAPI route authentication, bounded GCS uploads/bucket lookup, BigQuery load/identifier guards, and the React ingestion client.

**Architecture:** Harden infrastructure first by removing `allUsers` from `ingestion-source-service` Cloud Run IAM (`invoker_members = []` with validation against public principals) and binding a dedicated least-privilege `ingestion-source-sa` scoped to `${project_id}_psearch_raw`. Enforce fail-closed application authentication (`X-Admin-Token` or allowlisted Google OIDC `Bearer` token) via a FastAPI router dependency on all operational routes, backed by strict input validators, streaming upload size caps (`413`), single-bucket metadata lookup, and `WRITE_EMPTY` default load disposition.

**Tech Stack:** Python 3.11+ (FastAPI, Pydantic v2, `google-auth`, `google-cloud-storage`, `google-cloud-bigquery`, `pytest`, `httpx`, `python-hcl2`), Terraform (`hashicorp/google` >= 5.10), React / Axios (`src/application/ui`).

**Spec:** `docs/superpowers/specs/2026-11-13-ingestion-source-auth-design.md`

## Global Constraints

- Never grant `roles/run.invoker` to `"allUsers"` or `"allAuthenticatedUsers"` on `ingestion-source-service`; `invoker_members` must default to `[]`.
- Every operational endpoint in `src/psearch/ingestion_source/main.py` (`/upload`, `/datasets`, `/tables`, `/create_and_load`, `/load`, `/dry-run-query`, `/jobs/{job_id}`, `/jobs`, `/ensure-dataset`, `/buckets`) must enforce `require_auth`; only `GET /` remains public for health probes.
- Authentication fails closed (`403`) when neither admin token (`INGESTION_ADMIN_TOKEN` / `ADMIN_TOKEN`) nor OIDC audience + allowlist (`INGESTION_AUTH_AUDIENCE` + `INGESTION_ALLOWED_PRINCIPALS` or `INGESTION_ALLOWED_DOMAINS`) is configured.
- File uploads default to `MAX_UPLOAD_SIZE_BYTES = 52_428_800` (50 MiB), enforce limits during streaming before GCS upload, clean up temp files in `finally`, and return HTTP `413` when exceeded.
- `LoadJobRequest.write_disposition` defaults to `"WRITE_EMPTY"` and rejects `"WRITE_TRUNCATE"` with HTTP `403` unless `ALLOW_WRITE_TRUNCATE=true`.
- `StorageService.list_buckets()` must never call `client.list_buckets()`; it inspects only `{project_id}_psearch_raw` via `client.lookup_bucket()`.
- Run Python tests with `uv run --with pytest --with httpx --with python-hcl2 --with fastapi --with pydantic --with python-multipart --with google-auth --with requests --with google-cloud-bigquery --with google-cloud-storage --with pandas pytest -q`.

## Review Focus

1. Missing or dotless `file.filename` on `POST /upload` (e.g. `filename="README"` or `filename=""`) must return HTTP `400` cleanly instead of raising `AttributeError` or misclassifying `"readme"` as an extension.
2. Chunked uploads omitting or spoofing a small `Content-Length` while streaming bytes past `MAX_UPLOAD_SIZE_BYTES` must abort mid-stream, unlink the temporary file, and return HTTP `413` without calling `blob.upload_from_filename`.
3. Cross-project dataset IDs passed to `POST /ensure-dataset` or `POST /datasets` (e.g. `"other-project.raw_ds"`) must be rejected (`400`/`422`), whereas `"{PROJECT_ID}.raw_ds"` on `/ensure-dataset` normalizes to `"raw_ds"`.
4. Validly signed Google OIDC Bearer tokens matching `INGESTION_AUTH_AUDIENCE` must still be rejected (`403`) when `email_verified` is `False` or the caller's email/domain is outside `INGESTION_ALLOWED_PRINCIPALS` and `INGESTION_ALLOWED_DOMAINS`.
5. Path-traversal `file_id` values (e.g. `"../../secret"` or non-UUID strings) on `POST /create_and_load` and `POST /load` must return `400` without touching GCS and without being swallowed into `500` by `except Exception`.

---

### Task 1: Terraform Cloud Run Invoker Lockdown & Least-Privilege Service Account

**Files:**
- Modify: `src/iac/modules/iam/main.tf`
- Modify: `src/iac/modules/iam/outputs.tf`
- Modify: `src/iac/modules/ingestion_source/variables.tf`
- Modify: `src/iac/modules/ingestion_source/main.tf`
- Modify: `src/iac/variables.tf`
- Modify: `src/iac/main.tf`
- Create: `src/psearch/ingestion_source/tests/__init__.py`
- Create: `src/psearch/ingestion_source/tests/test_iac_security.py`

**Interfaces:**
- Consumes: none
- Produces:
  - Terraform output `module.iam.ingestion_source_service_account_email`
  - Terraform module `ingestion_source` variables: `invoker_members` (`list(string)`, default `[]`), `ingestion_auth_audience` (`string`, default `""`), `ingestion_allowed_principals` (`string`, default `""`), `allowed_origins` (`string`, default `""`), `max_upload_size_bytes` (`number`, default `52428800`)

- [ ] **Step 1: Write the failing HCL security regression tests**

Create `src/psearch/ingestion_source/tests/__init__.py` (empty) and `src/psearch/ingestion_source/tests/test_iac_security.py`:

```python
from pathlib import Path
import hcl2

REPO_ROOT = Path(__file__).resolve().parents[4]
IAC_ROOT = REPO_ROOT / "src" / "iac"


def _load_tf(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return hcl2.load(fh)


def test_ingestion_source_does_not_grant_all_users_invoker():
    raw = (IAC_ROOT / "modules" / "ingestion_source" / "main.tf").read_text(encoding="utf-8")
    parsed = _load_tf(IAC_ROOT / "modules" / "ingestion_source" / "main.tf")
    assert '"allUsers"' not in raw
    assert '"allAuthenticatedUsers"' not in raw
    policies = [
        r for r in parsed.get("resource", [])
        if "google_cloud_run_v2_service_iam_policy" in r
    ]
    assert policies == []
    members = [
        r["google_cloud_run_v2_service_iam_member"]
        for r in parsed.get("resource", [])
        if "google_cloud_run_v2_service_iam_member" in r
    ]
    assert len(members) == 1
    invoker = members[0]["invokers"]
    assert invoker["role"] == "roles/run.invoker"


def test_ingestion_source_invoker_members_defaults_empty_and_rejects_public():
    parsed = _load_tf(IAC_ROOT / "modules" / "ingestion_source" / "variables.tf")
    vars_by_name = {k: v for item in parsed.get("variable", []) for k, v in item.items()}
    assert "invoker_members" in vars_by_name
    assert vars_by_name["invoker_members"]["default"] == []
    validations = vars_by_name["invoker_members"].get("validation", [])
    assert len(validations) >= 1
    cond = validations[0]["condition"]
    assert "allUsers" in cond and "allAuthenticatedUsers" in cond


def test_dedicated_ingestion_source_sa_and_scoped_bucket_iam():
    iam_tf = _load_tf(IAC_ROOT / "modules" / "iam" / "main.tf")
    sas = {
        k: v
        for r in iam_tf.get("resource", [])
        if "google_service_account" in r
        for k, v in r["google_service_account"].items()
    }
    assert "ingestion_source_service_account" in sas
    assert sas["ingestion_source_service_account"]["account_id"] == "ingestion-source-sa"

    bucket_iam = [
        r["google_storage_bucket_iam_member"]
        for r in iam_tf.get("resource", [])
        if "google_storage_bucket_iam_member" in r
    ]
    assert any("ingestion_source_raw_bucket_admin" in m for m in bucket_iam)

    root_tf = (IAC_ROOT / "main.tf").read_text(encoding="utf-8")
    assert "module.iam.ingestion_source_service_account_email" in root_tf
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --with pytest --with python-hcl2 pytest -q src/psearch/ingestion_source/tests/test_iac_security.py`
Expected: FAIL (`AssertionError: assert '"allUsers"' not in raw`)

- [ ] **Step 3: Update Terraform modules (`iam`, `ingestion_source`, and root `iac`)**

1. In `src/iac/modules/iam/main.tf`:
   - Add `resource "google_service_account" "ingestion_source_service_account"` (`account_id = "ingestion-source-sa"`).
   - Add `resource "google_storage_bucket" "psearch_raw"` (`name = "${var.project_id}_psearch_raw"`, `location = "US"`, `uniform_bucket_level_access = true`).
   - Add `resource "google_storage_bucket_iam_member" "ingestion_source_raw_bucket_admin"` granting `roles/storage.objectAdmin` on `google_storage_bucket.psearch_raw.name` to `ingestion_source_service_account`.
   - Add `resource "google_project_iam_member" "grant_ingestion_source_sa_roles"` granting `for_each = toset(["roles/bigquery.user", "roles/logging.logWriter", "roles/serviceusage.serviceUsageConsumer"])` to `ingestion_source_service_account`.
2. In `src/iac/modules/iam/outputs.tf`: export `ingestion_source_service_account_email`.
3. In `src/iac/modules/ingestion_source/variables.tf`: add `invoker_members` (`list(string)`, `default = []`, `validation` ensuring `!contains(var.invoker_members, "allUsers") && !contains(var.invoker_members, "allAuthenticatedUsers")`), plus `ingestion_auth_audience`, `ingestion_allowed_principals`, `allowed_origins`, and `max_upload_size_bytes` (`default = 52428800`).
4. In `src/iac/modules/ingestion_source/main.tf`:
   - Inject `INGESTION_AUTH_AUDIENCE`, `INGESTION_ALLOWED_PRINCIPALS`, `ALLOWED_ORIGINS`, and `MAX_UPLOAD_SIZE_BYTES` env blocks into `google_cloud_run_v2_service.ingestion_source_service`.
   - Replace `data.google_iam_policy.noauth` and `google_cloud_run_v2_service_iam_policy.noauth` with `resource "google_cloud_run_v2_service_iam_member" "invokers"` (`for_each = toset(var.invoker_members)`, `role = "roles/run.invoker"`, `member = each.value`).
5. In `src/iac/variables.tf` & `src/iac/main.tf`: add `ingestion_source_invoker_members` (`default = []`, same public-principal validation) and wire `service_account_email = module.iam.ingestion_source_service_account_email` and `invoker_members = var.ingestion_source_invoker_members` in `module "ingestion_source"`.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run --with pytest --with python-hcl2 pytest -q src/psearch/ingestion_source/tests/test_iac_security.py`
Expected: PASS (`3 passed`)

- [ ] **Step 5: Commit**

```bash
git add src/iac/ src/psearch/ingestion_source/tests/__init__.py src/psearch/ingestion_source/tests/test_iac_security.py
git commit -m "fix(iac): remove allUsers invoker and scope ingestion_source service account"
```

---

### Task 2: Fail-Closed Authentication & Input Validation Modules

**Files:**
- Create: `src/psearch/ingestion_source/__init__.py`
- Create: `src/psearch/ingestion_source/auth.py`
- Create: `src/psearch/ingestion_source/validation.py`
- Modify: `src/psearch/ingestion_source/requirements.txt`
- Create: `src/psearch/ingestion_source/tests/test_auth_and_validation.py`

**Interfaces:**
- Consumes: none
- Produces:
  - `src/psearch/ingestion_source/auth.py`:
    - `AuthPrincipal(BaseModel)`: `method: str`, `subject: str`
    - `verify_bearer_token(token: str, audience: str) -> Dict[str, Any]` (thin wrapper around `google.oauth2.id_token.verify_oauth2_token` for clean dependency injection/testing)
    - `require_auth(x_admin_token: Optional[str] = Header(default=None, alias="X-Admin-Token"), authorization: Optional[str] = Header(default=None, alias="Authorization")) -> AuthPrincipal`
  - `src/psearch/ingestion_source/validation.py`:
    - `DEFAULT_MAX_UPLOAD_SIZE_BYTES = 52_428_800`
    - `UploadTooLargeError(ValueError)`
    - `get_max_upload_size_bytes() -> int`
    - `validate_upload_filename(filename: Optional[str]) -> str` (returns `"csv"` or `"json"`, raises `ValueError` otherwise)
    - `validate_file_id(file_id: str) -> str` (validates UUIDv4, returns lowercase canonical UUID string, raises `ValueError` otherwise)
    - `validate_dataset_id(dataset_id: str, project_id: Optional[str] = None, allow_project_prefix: bool = False) -> str` (validates `^[A-Za-z0-9_]{1,1024}$`, rejects cross-project prefix with `ValueError`, checks `INGESTION_ALLOWED_DATASETS` raising `PermissionError` if disallowed)
    - `validate_table_id(table_id: str) -> str` (validates `^[A-Za-z0-9_]{1,1024}$`)
    - `validate_write_disposition(disposition: str) -> str` (allows `WRITE_EMPTY`, `WRITE_APPEND`, `WRITE_TRUNCATE`; raises `PermissionError` on `WRITE_TRUNCATE` unless `ALLOW_WRITE_TRUNCATE=true`; raises `ValueError` on unknown values)

- [ ] **Step 1: Write the failing unit tests for auth and validation**

Create `src/psearch/ingestion_source/tests/test_auth_and_validation.py` testing:
- `test_require_auth_fails_closed_when_unconfigured`: with no env vars set, `require_auth()` raises `HTTPException(status_code=403)`.
- `test_require_auth_missing_credentials_returns_401`: with `INGESTION_ADMIN_TOKEN="secret"`, `require_auth(None, None)` raises `HTTPException(status_code=401)`.
- `test_require_auth_accepts_matching_admin_token_and_rejects_wrong_token`: valid `X-Admin-Token` returns `AuthPrincipal(method="admin_token", subject="admin_token")`; wrong token raises `403`.
- `test_require_auth_oidc_accepts_allowlisted_principal`: monkeypatching `verify_bearer_token` to return `{"iss": "https://accounts.google.com", "email": "ops@example.com", "email_verified": True}` with `INGESTION_AUTH_AUDIENCE="https://svc.run.app"` and `INGESTION_ALLOWED_PRINCIPALS="ops@example.com"` returns `AuthPrincipal(method="oidc", subject="ops@example.com")`.
- `test_require_auth_oidc_rejects_unverified_or_unlisted_principal` (Review Focus #4): `email_verified=False` or `"stranger@gmail.com"` raises `HTTPException(status_code=403)`; Audience without any allowlist configured raises `403`.
- `test_validate_upload_filename_rejects_missing_or_dotless` (Review Focus #1): `None`, `""`, `"README"`, `"archive.tar.gz"` raise `ValueError`; `"catalog.CSV"` and `"items.json"` return `"csv"` and `"json"`.
- `test_validate_file_id_rejects_traversal` (Review Focus #5): `"../../etc/passwd"` and `"not-a-uuid"` raise `ValueError`; valid UUIDv4 passes.
- `test_validate_dataset_id_rejects_cross_project_and_honors_allowlist` (Review Focus #3): `"other-proj.ds"` raises `ValueError`; `"my-proj.raw_ds"` with `project_id="my-proj", allow_project_prefix=True` returns `"raw_ds"`; `INGESTION_ALLOWED_DATASETS="raw_ds"` rejects `"prod_ds"` with `PermissionError`.
- `test_validate_write_disposition_blocks_truncate_by_default`: `"WRITE_EMPTY"` and `"WRITE_APPEND"` pass; `"WRITE_TRUNCATE"` raises `PermissionError` when `ALLOW_WRITE_TRUNCATE` is unset and passes when `ALLOW_WRITE_TRUNCATE="true"`; `"__class__"` raises `ValueError`.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run --with pytest --with fastapi --with pydantic --with google-auth --with requests pytest -q src/psearch/ingestion_source/tests/test_auth_and_validation.py`
Expected: FAIL (`ModuleNotFoundError: No module named 'psearch.ingestion_source.auth'` or `ingestion_source.auth`)

- [ ] **Step 3: Implement `auth.py`, `validation.py`, `__init__.py`, and pin `requests` in `requirements.txt`**

- Create `src/psearch/ingestion_source/__init__.py`.
- Implement `src/psearch/ingestion_source/auth.py` using `hmac.compare_digest` and `google.oauth2.id_token.verify_oauth2_token` + `google.auth.transport.requests.Request`.
- Implement `src/psearch/ingestion_source/validation.py` with the exact signatures and rules in **Interfaces**.
- Add `requests>=2.31.0` under Google Cloud dependencies in `src/psearch/ingestion_source/requirements.txt`.

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src uv run --with pytest --with fastapi --with pydantic --with google-auth --with requests pytest -q src/psearch/ingestion_source/tests/test_auth_and_validation.py`
Expected: PASS (`9 passed`)

- [ ] **Step 5: Commit**

```bash
git add src/psearch/ingestion_source/__init__.py src/psearch/ingestion_source/auth.py src/psearch/ingestion_source/validation.py src/psearch/ingestion_source/requirements.txt src/psearch/ingestion_source/tests/test_auth_and_validation.py
git commit -m "feat(ingestion_source): add fail-closed auth and input validation"
```

---

### Task 3: Harden Storage, Schema Detection, Dataset, and BigQuery Services

**Files:**
- Create: `src/psearch/ingestion_source/services/__init__.py`
- Modify: `src/psearch/ingestion_source/services/storage_service.py`
- Modify: `src/psearch/ingestion_source/services/schema_detection_service.py`
- Modify: `src/psearch/ingestion_source/services/bigquery_service.py`
- Modify: `src/psearch/ingestion_source/services/dataset_service.py`
- Create: `src/psearch/ingestion_source/tests/test_services_hardening.py`

**Interfaces:**
- Consumes: `UploadTooLargeError`, `get_max_upload_size_bytes`, `validate_file_id`, `validate_dataset_id`, `validate_table_id`, `validate_write_disposition` from `validation.py`
- Produces:
  - `StorageService(project_id: str, client: Optional[storage.Client] = None, max_upload_size_bytes: Optional[int] = None)`
  - `StorageService.upload_file(file: UploadFile, file_id: str) -> str` (streams with chunk cap, raises `UploadTooLargeError` mid-stream, always unlinks temp file in `finally`)
  - `StorageService.list_buckets() -> List[Dict[str, Any]]` (calls `self.client.lookup_bucket(self.bucket_name)` only — never `self.client.list_buckets()`)
  - `SchemaDetectionService(max_upload_size_bytes: Optional[int] = None)` (bounded CSV sample `MAX_SCHEMA_SAMPLE_BYTES = 5 * 1024 * 1024` without full-file `await file.read()`, and bounded JSON read raising `UploadTooLargeError` above `max_upload_size_bytes`)
  - `BigQueryService.load_table_from_uri(..., write_disposition: str = "WRITE_EMPTY", ...)` using an explicit map `{"WRITE_EMPTY": bigquery.WriteDisposition.WRITE_EMPTY, "WRITE_APPEND": bigquery.WriteDisposition.WRITE_APPEND, "WRITE_TRUNCATE": bigquery.WriteDisposition.WRITE_TRUNCATE}` instead of `getattr`
  - `DatasetService.ensure_dataset_exists` rejecting cross-project `dataset_id` via `validate_dataset_id(..., project_id=self.project_id, allow_project_prefix=True)`

- [ ] **Step 1: Write the failing service hardening tests**

Create `src/psearch/ingestion_source/tests/test_services_hardening.py` testing:
- `test_storage_upload_aborts_mid_stream_and_cleans_temp_file_when_exceeding_limit` (Review Focus #2): upload a 2 KiB CSV and a 2 KiB JSON with `max_upload_size_bytes=1024`; assert `UploadTooLargeError` is raised, `blob.upload_from_filename` is never called, and no temp files remain leaked.
- `test_storage_list_buckets_scopes_to_single_ingestion_bucket`: attach a mock `storage.Client` whose `list_buckets` raises `AssertionError("must not list all project buckets")` and `lookup_bucket("proj_psearch_raw")` returns a mock bucket; assert `list_buckets()` returns `[{"name": "proj_psearch_raw", ..., "is_default": True}]`.
- `test_storage_get_file_uri_rejects_non_uuid_and_never_lists_blobs`: calling `get_file_uri("../../secret", "csv")` raises `ValueError` without calling `bucket.blob`.
- `test_schema_detection_csv_samples_without_reading_entire_stream`: pass a custom `UploadFile` stream whose total size is `10 * 1024 * 1024` bytes (10 MiB > `MAX_SCHEMA_SAMPLE_BYTES = 5 MiB`); assert `detect_schema(file, "csv")` succeeds and reads at most `MAX_SCHEMA_SAMPLE_BYTES + 4096` bytes rather than the whole 10 MiB.
- `test_dataset_service_rejects_cross_project_dataset` (Review Focus #3): `DatasetService("proj-a", client=mock_bq).ensure_dataset_exists("proj-b.secret")` raises `ValueError`.
- `test_bigquery_service_defaults_to_write_empty_and_validates_disposition`: verify `load_table_from_uri` maps `"WRITE_EMPTY"` via explicit dictionary and rejects invalid dispositions before starting a load job.

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src uv run --with pytest --with fastapi --with pydantic --with python-multipart --with google-cloud-storage --with google-cloud-bigquery --with pandas pytest -q src/psearch/ingestion_source/tests/test_services_hardening.py`
Expected: FAIL (`TypeError` / `AssertionError`)

- [ ] **Step 3: Implement service hardening in `storage_service.py`, `schema_detection_service.py`, `bigquery_service.py`, and `dataset_service.py`**

- Add `src/psearch/ingestion_source/services/__init__.py`.
- Update `StorageService`: accept optional `client` and `max_upload_size_bytes`, read both JSON and CSV in bounded 1 MiB chunks raising `UploadTooLargeError` as soon as `total_bytes > limit`, wrap temp-file lifecycle in `try ... finally` (`os.unlink`), validate `file_id` via `validate_file_id` in `upload_file` and `get_file_uri`, remove blob-listing fallback in `get_file_uri`, and replace `self.client.list_buckets()` with `self.client.lookup_bucket(self.bucket_name)` in `list_buckets()`.
- Update `SchemaDetectionService`: bound `_detect_csv_schema` to read at most `MAX_SCHEMA_SAMPLE_BYTES = 5 * 1024 * 1024` bytes (cutting at the last newline when truncated so `pd.read_csv(..., nrows=1000)` never sees a half-line or reads unbounded payloads), and read JSON in chunks up to `max_upload_size_bytes` raising `UploadTooLargeError` if exceeded.
- Update `BigQueryService` and `DatasetService` to accept optional `client`, validate identifiers, default `write_disposition` to `"WRITE_EMPTY"`, and map `WriteDisposition` via an explicit dict.

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src uv run --with pytest --with fastapi --with pydantic --with python-multipart --with google-cloud-storage --with google-cloud-bigquery --with pandas pytest -q src/psearch/ingestion_source/tests/test_services_hardening.py`
Expected: PASS (`6 passed`)

- [ ] **Step 5: Commit**

```bash
git add src/psearch/ingestion_source/services/ src/psearch/ingestion_source/tests/test_services_hardening.py
git commit -m "fix(ingestion_source): bound upload size, scope bucket lookup, and harden BQ services"
```

---

### Task 4: Wire FastAPI Route Auth, Validation, Error Handling & UI Client Updates

**Files:**
- Modify: `src/psearch/ingestion_source/main.py`
- Create: `src/psearch/ingestion_source/tests/test_main_api.py`
- Modify: `src/application/ui/src/services/sourceIngestionService.js`
- Modify: `src/application/ui/src/components/SourceIngestion.js`

**Interfaces:**
- Consumes: `require_auth` from `auth.py`; `UploadTooLargeError`, `get_max_upload_size_bytes`, `validate_upload_filename`, `validate_file_id`, `validate_dataset_id`, `validate_table_id`, `validate_write_disposition` from `validation.py`; hardened services from Task 3.
- Produces:
  - Protected `APIRouter(dependencies=[Depends(require_auth)])` registered on `app` in `src/psearch/ingestion_source/main.py` covering all 10 operational routes; public `GET /` health check.
  - Updated `sourceIngestionService.js` (`getAuthHeaders()` attaching `X-Admin-Token` / `Authorization: Bearer`) and `SourceIngestion.js` (`write_disposition: "WRITE_EMPTY"`).

- [ ] **Step 1: Write the failing FastAPI endpoint security & regression tests**

Create `src/psearch/ingestion_source/tests/test_main_api.py` using `fastapi.testclient.TestClient` testing:
- `test_health_root_is_public`: `GET /` returns `200` with no auth headers even when `INGESTION_ADMIN_TOKEN` is unset.
- `test_all_operational_routes_reject_unauthenticated_callers`: parametrize across all 10 routes (`POST /upload`, `POST /datasets`, `POST /tables`, `POST /create_and_load`, `POST /load`, `POST /dry-run-query`, `GET /jobs/j1`, `GET /jobs`, `POST /ensure-dataset`, `GET /buckets`):
  - When unconfigured → returns `403` (fail-closed).
  - When `INGESTION_ADMIN_TOKEN="topsecret"` and no header is sent → returns `401`.
  - When wrong `X-Admin-Token` is sent → returns `403`.
- `test_upload_enforces_size_limit_and_validates_filename` (Review Focus #1 & #2): with valid `X-Admin-Token`, `POST /upload` with `"README"` returns `400`; payload exceeding `MAX_UPLOAD_SIZE_BYTES` returns `413`; valid CSV returns `200`.
- `test_load_and_create_and_load_block_write_truncate_and_invalid_file_id` (Review Focus #5):
  - Default `LoadJobRequest` uses `"WRITE_EMPTY"` and succeeds (`200`) for valid UUID `file_id`.
  - Explicit `write_disposition="WRITE_TRUNCATE"` returns `403` when `ALLOW_WRITE_TRUNCATE` is unset, and `200` when `ALLOW_WRITE_TRUNCATE="true"`.
  - `file_id="../../etc/passwd"` returns `400` (not `500`!).
- `test_buckets_endpoint_returns_only_default_bucket_when_authenticated`: `GET /buckets` with valid `X-Admin-Token` returns only `proj_psearch_raw`.
- `test_ensure_dataset_and_datasets_reject_cross_project_and_disallowed_datasets` (Review Focus #3): `"evil-proj.ds"` returns `400`/`422`; dataset not in `INGESTION_ALLOWED_DATASETS` returns `403`.

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src uv run --with pytest --with httpx --with fastapi --with pydantic --with python-multipart --with google-auth --with requests --with google-cloud-storage --with google-cloud-bigquery --with pandas pytest -q src/psearch/ingestion_source/tests/test_main_api.py`
Expected: FAIL (unauthenticated requests currently return `200` / `422` / `500` instead of `403` / `401`)

- [ ] **Step 3: Update `main.py`, `sourceIngestionService.js`, and `SourceIngestion.js`**

1. In `src/psearch/ingestion_source/main.py`:
   - Configure CORS from `ALLOWED_ORIGINS` env var (`origins = [o.strip() for o in os.environ.get("ALLOWED_ORIGINS", "").split(",") if o.strip()]`, `allow_credentials=bool(origins)`, `allow_methods=["GET", "POST", "OPTIONS"]`, `allow_headers=["Authorization", "Content-Type", "X-Admin-Token"]`).
   - Change `LoadJobRequest.write_disposition` default to `"WRITE_EMPTY"` and add Pydantic `@field_validator`s on `DatasetRequest`, `TableRequest`, and `LoadJobRequest` calling `validate_dataset_id` and `validate_table_id` (converting `PermissionError` in route handlers to `HTTPException(403)`).
   - Move all 10 operational routes onto `protected_router = APIRouter(dependencies=[Depends(require_auth)])` and call `app.include_router(protected_router)`. Keep `@app.get("/")` on `app`.
   - In `/upload`: check `Content-Length` header if present against `get_max_upload_size_bytes()` → `413`, validate filename with `validate_upload_filename` → `400`, translate `UploadTooLargeError` → `413`.
   - In `/create_and_load` and `/load`: validate `file_id` via `validate_file_id` (`ValueError` → `400`) and `write_disposition` via `validate_write_disposition` (`PermissionError` → `403`, `ValueError` → `400`).
   - Add `except HTTPException: raise` before `except Exception:` in every route and return sanitized 500 details (no raw internal exception strings).
2. In `src/application/ui/src/services/sourceIngestionService.js`:
   - Add `getAuthHeaders()` reading `localStorage.getItem('ingestionAuthToken')` (`Authorization: Bearer <token>`) and `localStorage.getItem('ingestionAdminToken') || process.env.REACT_APP_INGESTION_ADMIN_TOKEN` (`X-Admin-Token`), and pass `headers: { ...getAuthHeaders(), ... }` on every `axios` call to `INGESTION_SOURCE_URL`.
3. In `src/application/ui/src/components/SourceIngestion.js`:
   - Update `handleLoadData` (`line 397`) from `write_disposition: "WRITE_TRUNCATE"` to `write_disposition: "WRITE_EMPTY"`.

- [ ] **Step 4: Run full Python, Go, and React UI verification**

Run:
```bash
PYTHONPATH=src uv run --with pytest --with httpx --with python-hcl2 --with fastapi --with pydantic --with python-multipart --with google-auth --with requests --with google-cloud-storage --with google-cloud-bigquery --with pandas --with google-genai pytest -q src/psearch/ingestion_source/tests/ src/psearch/gen_ai/services/sql/common/test_input_validation.py && (cd src/psearch/serving && go test ./...)
```
Expected: All Python tests (`ingestion_source` + `gen_ai`) and Go `serving` tests PASS. (Also build UI if `node_modules` is present or verify syntax cleanly.)

- [ ] **Step 5: Commit**

```bash
git add src/psearch/ingestion_source/main.py src/psearch/ingestion_source/tests/test_main_api.py src/application/ui/src/services/sourceIngestionService.js src/application/ui/src/components/SourceIngestion.js
git commit -m "fix(ingestion_source): enforce route authentication, safe defaults, and UI headers"
```

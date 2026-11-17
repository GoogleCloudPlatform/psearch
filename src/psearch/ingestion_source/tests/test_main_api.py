#
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""FastAPI endpoint security, validation, CORS, and error-handling tests for ingestion_source."""

from datetime import datetime, timezone
import errno
from typing import Any, Dict, Tuple
from unittest.mock import MagicMock
import uuid
import warnings

# Starlette 0.4x/1.0 emits StarletteDeprecationWarning (subclassing Warning, not
# DeprecationWarning) on import when `httpx` rather than `httpx2` is installed.
with warnings.catch_warnings():
    warnings.filterwarnings(
        "ignore",
        message=r"Using `httpx` with `starlette\.testclient` is deprecated.*",
        category=Warning,
    )
    from fastapi.testclient import TestClient
from google.api_core.exceptions import BadRequest, NotFound
from google.cloud import bigquery
import pytest

try:
    from psearch.ingestion_source import auth, main as ingestion_main
    from psearch.ingestion_source.services.bigquery_service import BigQueryService
    from psearch.ingestion_source.services.dataset_service import DatasetService
    from psearch.ingestion_source.services.schema_detection_service import (
        SchemaDetectionService,
    )
    from psearch.ingestion_source.services.storage_service import StorageService
except ModuleNotFoundError as exc:
    if exc.name not in {
        "psearch",
        "psearch.ingestion_source",
        "psearch.ingestion_source.services",
    }:
        raise
    from ingestion_source import auth, main as ingestion_main
    from ingestion_source.services.bigquery_service import BigQueryService
    from ingestion_source.services.dataset_service import DatasetService
    from ingestion_source.services.schema_detection_service import (
        SchemaDetectionService,
    )
    from ingestion_source.services.storage_service import StorageService


_AUTH_ENV_VARS = (
    "INGESTION_ADMIN_TOKEN",
    "ADMIN_TOKEN",
    "INGESTION_AUTH_AUDIENCE",
    "INGESTION_ALLOWED_PRINCIPALS",
    "INGESTION_ALLOWED_DOMAINS",
)


def _clear_auth_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in _AUTH_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def _build_test_app_and_mocks(
    monkeypatch: pytest.MonkeyPatch,
    project_id: str = "proj",
) -> Tuple[Any, MagicMock, MagicMock]:
    """Create a FastAPI app wired with real services backed by mocked GCP SDK clients."""
    monkeypatch.setenv("PROJECT_ID", project_id)
    ingestion_main.jobs.clear()

    mock_gcs_client = MagicMock()
    mock_gcs_client.list_buckets.side_effect = AssertionError(
        "must not enumerate all project buckets"
    )
    mock_bucket = MagicMock()
    mock_bucket.name = f"{project_id}_psearch_raw"
    mock_bucket.location = "US-CENTRAL1"
    mock_bucket.time_created = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    mock_bucket.storage_class = "STANDARD"
    mock_bucket.list_blobs.side_effect = AssertionError("must not list bucket blobs")

    mock_blob = MagicMock()
    mock_blob.exists.return_value = True
    mock_bucket.blob.return_value = mock_blob
    mock_gcs_client.lookup_bucket.return_value = mock_bucket

    mock_bq_client = MagicMock()
    mock_bq_client.get_dataset.return_value = MagicMock(location="US")
    mock_bq_client.get_table.return_value = MagicMock(num_rows=5)
    mock_bq_client.load_table_from_uri.return_value = MagicMock(
        job_id="bq-job-1",
        errors=None,
        total_bytes_processed=256,
    )
    mock_bq_client.query.return_value = MagicMock(total_bytes_processed=1024)
    monkeypatch.setattr(
        ingestion_main.bigquery, "Client", lambda project=None: mock_bq_client
    )

    app = (
        ingestion_main.create_app()
        if hasattr(ingestion_main, "create_app")
        else ingestion_main.app
    )
    app.dependency_overrides[ingestion_main.get_storage_service] = (
        lambda: StorageService(project_id, client=mock_gcs_client)
    )
    app.dependency_overrides[ingestion_main.get_schema_detection_service] = (
        lambda: SchemaDetectionService()
    )
    app.dependency_overrides[ingestion_main.get_bigquery_service] = (
        lambda: BigQueryService(project_id, client=mock_bq_client)
    )
    app.dependency_overrides[ingestion_main.get_dataset_service] = (
        lambda: DatasetService(project_id, client=mock_bq_client)
    )

    return app, mock_gcs_client, mock_bq_client


def test_health_root_is_public(monkeypatch: pytest.MonkeyPatch):
    _clear_auth_env(monkeypatch)
    app, _, _ = _build_test_app_and_mocks(monkeypatch)
    try:
        with TestClient(app) as client:
            response = client.get("/")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"
    finally:
        app.dependency_overrides.clear()


@pytest.mark.parametrize(
    "doc_path",
    ["/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"],
)
def test_openapi_and_docs_endpoints_are_not_publicly_served(
    monkeypatch: pytest.MonkeyPatch,
    doc_path: str,
):
    _clear_auth_env(monkeypatch)
    app, _, _ = _build_test_app_and_mocks(monkeypatch)
    try:
        with TestClient(app) as client:
            unauth_resp = client.get(doc_path)
            assert unauth_resp.status_code == 404

            monkeypatch.setenv("INGESTION_ADMIN_TOKEN", "topsecret")
            no_header_resp = client.get(doc_path)
            assert no_header_resp.status_code == 404
    finally:
        app.dependency_overrides.clear()


def test_table_request_uses_schema_alias_without_shadowing_base_model(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("INGESTION_ALLOWED_DATASETS", raising=False)
    assert "schema_fields" in ingestion_main.TableRequest.model_fields
    assert "schema" not in ingestion_main.TableRequest.model_fields

    from_alias = ingestion_main.TableRequest.model_validate(
        {
            "dataset_id": "raw_ds",
            "table_id": "items",
            "schema": [{"name": "id", "type": "STRING"}],
        }
    )
    assert len(from_alias.schema_fields) == 1
    assert from_alias.schema_fields[0].name == "id"

    from_field_name = ingestion_main.TableRequest(
        dataset_id="raw_ds",
        table_id="items",
        schema_fields=[ingestion_main.SchemaField(name="sku", type="STRING")],
    )
    assert len(from_field_name.schema_fields) == 1
    assert from_field_name.schema_fields[0].name == "sku"


_OPERATIONAL_ROUTES = [
    (
        "POST",
        "/upload",
        {"files": {"file": ("items.csv", b"id,name\n1,Widget\n", "text/csv")}},
    ),
    ("POST", "/datasets", {"json": {"dataset_id": "raw_ds"}}),
    (
        "POST",
        "/tables",
        {
            "json": {
                "dataset_id": "raw_ds",
                "table_id": "items",
                "schema": [{"name": "id", "type": "STRING"}],
            }
        },
    ),
    (
        "POST",
        f"/create_and_load?file_id={uuid.uuid4()}&file_type=csv",
        {"json": {"dataset_id": "raw_ds", "table_id": "items"}},
    ),
    (
        "POST",
        f"/load?file_id={uuid.uuid4()}&file_type=csv",
        {"json": {"dataset_id": "raw_ds", "table_id": "items"}},
    ),
    ("POST", "/dry-run-query", {"json": {"sql_script": "SELECT 1"}}),
    ("GET", "/jobs/j1", {}),
    ("GET", "/jobs", {}),
    ("POST", "/ensure-dataset", {"json": {"dataset_id": "raw_ds"}}),
    ("GET", "/buckets", {}),
]


@pytest.mark.parametrize("method,path,req_kwargs", _OPERATIONAL_ROUTES)
def test_all_operational_routes_reject_unauthenticated_callers(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
    req_kwargs: Dict[str, Any],
):
    app, _, _ = _build_test_app_and_mocks(monkeypatch)
    try:
        with TestClient(app) as client:
            # 1. Fail-closed (403) when auth is completely unconfigured
            _clear_auth_env(monkeypatch)
            unconfigured_resp = client.request(method, path, **req_kwargs)
            assert unconfigured_resp.status_code == 403

            # 2. Missing credentials -> 401 when admin token is configured
            monkeypatch.setenv("INGESTION_ADMIN_TOKEN", "topsecret")
            missing_resp = client.request(method, path, **req_kwargs)
            assert missing_resp.status_code == 401

            # 3. Wrong X-Admin-Token -> 403
            wrong_resp = client.request(
                method,
                path,
                headers={"X-Admin-Token": "wrong-token"},
                **req_kwargs,
            )
            assert wrong_resp.status_code == 403
    finally:
        app.dependency_overrides.clear()


def test_upload_enforces_size_limit_and_validates_filename(
    monkeypatch: pytest.MonkeyPatch,
):
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("INGESTION_ADMIN_TOKEN", "topsecret")
    monkeypatch.setenv("MAX_UPLOAD_SIZE_BYTES", "512")
    auth_headers = {"X-Admin-Token": "topsecret"}

    app, mock_gcs_client, _ = _build_test_app_and_mocks(monkeypatch)
    mock_blob = mock_gcs_client.lookup_bucket.return_value.blob.return_value
    try:
        with TestClient(app) as client:
            # 1. Dotless filename "README" -> 400 without touching GCS
            dotless_resp = client.post(
                "/upload",
                headers=auth_headers,
                files={"file": ("README", b"id,name\n1,a\n", "text/plain")},
            )
            assert dotless_resp.status_code == 400
            mock_blob.upload_from_filename.assert_not_called()

            # 2. Unsupported extension -> 400
            parquet_resp = client.post(
                "/upload",
                headers=auth_headers,
                files={"file": ("catalog.parquet", b"PAR1", "application/octet-stream")},
            )
            assert parquet_resp.status_code == 400
            mock_blob.upload_from_filename.assert_not_called()

            # 3. Malformed Content-Length header -> 400 (never 500)
            bad_cl_resp = client.post(
                "/upload",
                headers={**auth_headers, "Content-Length": "not-an-integer"},
                files={"file": ("tiny.csv", b"id\n1\n", "text/csv")},
            )
            assert bad_cl_resp.status_code == 400
            mock_blob.upload_from_filename.assert_not_called()

            # 4. Payload exceeding MAX_UPLOAD_SIZE_BYTES via Content-Length pre-check -> 413
            oversized_csv = b"id,name\n" + (b"1,widget_item_row\n" * 40)
            assert len(oversized_csv) > 512
            too_large_resp = client.post(
                "/upload",
                headers=auth_headers,
                files={"file": ("oversized.csv", oversized_csv, "text/csv")},
            )
            assert too_large_resp.status_code == 413
            mock_blob.upload_from_filename.assert_not_called()

            # 5. Payload exceeding MAX_UPLOAD_SIZE_BYTES via bounded stream check
            #    even when Content-Length header is spoofed smaller -> 413
            spoofed_cl_resp = client.post(
                "/upload",
                headers={**auth_headers, "Content-Length": "100"},
                files={"file": ("oversized.csv", oversized_csv, "text/csv")},
            )
            assert spoofed_cl_resp.status_code == 413
            mock_blob.upload_from_filename.assert_not_called()

            # 6. Valid CSV within limit -> 200
            valid_csv = b"id,name\n1,Widget\n2,Gadget\n"
            assert len(valid_csv) <= 512
            ok_resp = client.post(
                "/upload",
                headers=auth_headers,
                files={"file": ("items.csv", valid_csv, "text/csv")},
            )
            assert ok_resp.status_code == 200
            body = ok_resp.json()
            assert uuid.UUID(body["file_id"]).version == 4
            assert body["file_type"] == "csv"
            assert body["gcs_uri"] == f"gs://proj_psearch_raw/csv/{body['file_id']}.csv"
            assert body["row_count_estimate"] == 2
    finally:
        app.dependency_overrides.clear()


@pytest.mark.parametrize("endpoint", ["/load", "/create_and_load"])
def test_load_and_create_and_load_block_write_truncate_and_invalid_file_id(
    monkeypatch: pytest.MonkeyPatch,
    endpoint: str,
):
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("INGESTION_ADMIN_TOKEN", "topsecret")
    monkeypatch.delenv("ALLOW_WRITE_TRUNCATE", raising=False)
    monkeypatch.delenv("INGESTION_ALLOWED_DATASETS", raising=False)
    auth_headers = {"X-Admin-Token": "topsecret"}

    app, _, mock_bq_client = _build_test_app_and_mocks(monkeypatch)
    valid_file_id = str(uuid.uuid4())
    try:
        with TestClient(app) as client:
            # 1. Path traversal file_id -> 400 (never 500, no job created)
            bad_id_resp = client.post(
                endpoint,
                params={"file_id": "../../etc/passwd", "file_type": "csv"},
                headers=auth_headers,
                json={"dataset_id": "raw_ds", "table_id": "items"},
            )
            assert bad_id_resp.status_code == 400
            assert ingestion_main.jobs == {}
            mock_bq_client.load_table_from_uri.assert_not_called()

            # 2. Explicit WRITE_TRUNCATE when ALLOW_WRITE_TRUNCATE is unset -> 403
            truncate_blocked = client.post(
                endpoint,
                params={"file_id": valid_file_id, "file_type": "csv"},
                headers=auth_headers,
                json={
                    "dataset_id": "raw_ds",
                    "table_id": "items",
                    "write_disposition": "WRITE_TRUNCATE",
                },
            )
            assert truncate_blocked.status_code == 403
            assert ingestion_main.jobs == {}
            mock_bq_client.load_table_from_uri.assert_not_called()

            # 2b. Invalid source_format / write_disposition -> 400/422 (no job created)
            bad_format = client.post(
                endpoint,
                params={"file_id": valid_file_id, "file_type": "csv"},
                headers=auth_headers,
                json={
                    "dataset_id": "raw_ds",
                    "table_id": "items",
                    "source_format": "PARQUET",
                },
            )
            assert bad_format.status_code in {400, 422}
            assert ingestion_main.jobs == {}

            bad_disp = client.post(
                endpoint,
                params={"file_id": valid_file_id, "file_type": "csv"},
                headers=auth_headers,
                json={
                    "dataset_id": "raw_ds",
                    "table_id": "items",
                    "write_disposition": "WRITE_DROP",
                },
            )
            assert bad_disp.status_code in {400, 422}
            assert ingestion_main.jobs == {}
            mock_bq_client.load_table_from_uri.assert_not_called()

            # 3. Default LoadJobRequest uses WRITE_EMPTY and succeeds (200)
            default_ok = client.post(
                endpoint,
                params={"file_id": valid_file_id, "file_type": "csv"},
                headers=auth_headers,
                json={"dataset_id": "raw_ds", "table_id": "items"},
            )
            assert default_ok.status_code == 200
            mock_bq_client.load_table_from_uri.assert_called_once()
            _, load_kwargs = mock_bq_client.load_table_from_uri.call_args
            assert (
                load_kwargs["job_config"].write_disposition
                == bigquery.WriteDisposition.WRITE_EMPTY
            )

            # 4. Explicit WRITE_TRUNCATE succeeds (200) when ALLOW_WRITE_TRUNCATE="true"
            mock_bq_client.load_table_from_uri.reset_mock()
            monkeypatch.setenv("ALLOW_WRITE_TRUNCATE", "true")
            truncate_allowed = client.post(
                endpoint,
                params={"file_id": valid_file_id, "file_type": "csv"},
                headers=auth_headers,
                json={
                    "dataset_id": "raw_ds",
                    "table_id": "items",
                    "write_disposition": "WRITE_TRUNCATE",
                },
            )
            assert truncate_allowed.status_code == 200
            mock_bq_client.load_table_from_uri.assert_called_once()
            _, trunc_kwargs = mock_bq_client.load_table_from_uri.call_args
            assert (
                trunc_kwargs["job_config"].write_disposition
                == bigquery.WriteDisposition.WRITE_TRUNCATE
            )
    finally:
        app.dependency_overrides.clear()


def test_buckets_endpoint_returns_only_default_bucket_when_authenticated(
    monkeypatch: pytest.MonkeyPatch,
):
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("INGESTION_ADMIN_TOKEN", "topsecret")
    app, mock_gcs_client, _ = _build_test_app_and_mocks(monkeypatch, project_id="proj")
    try:
        with TestClient(app) as client:
            resp = client.get("/buckets", headers={"X-Admin-Token": "topsecret"})
        assert resp.status_code == 200
        buckets = resp.json()
        assert len(buckets) == 1
        assert buckets[0]["name"] == "proj_psearch_raw"
        assert buckets[0]["is_default"] is True
        mock_gcs_client.list_buckets.assert_not_called()
    finally:
        app.dependency_overrides.clear()


def test_ensure_dataset_and_datasets_reject_cross_project_and_disallowed_datasets(
    monkeypatch: pytest.MonkeyPatch,
):
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("INGESTION_ADMIN_TOKEN", "topsecret")
    monkeypatch.setenv("INGESTION_ALLOWED_DATASETS", "raw_ds")
    auth_headers = {"X-Admin-Token": "topsecret"}

    app, _, mock_bq_client = _build_test_app_and_mocks(monkeypatch, project_id="proj")
    try:
        with TestClient(app) as client:
            # Cross-project dataset -> 400 or 422 on both /ensure-dataset and /datasets
            ensure_cross = client.post(
                "/ensure-dataset",
                headers=auth_headers,
                json={"dataset_id": "evil-proj.ds"},
            )
            assert ensure_cross.status_code in {400, 422}

            datasets_cross = client.post(
                "/datasets",
                headers=auth_headers,
                json={"dataset_id": "evil-proj.ds"},
            )
            assert datasets_cross.status_code in {400, 422}

            # Disallowed dataset -> 403 on /ensure-dataset, /datasets, and /tables
            ensure_disallowed = client.post(
                "/ensure-dataset",
                headers=auth_headers,
                json={"dataset_id": "secret_ds"},
            )
            assert ensure_disallowed.status_code == 403

            ensure_prefixed_disallowed = client.post(
                "/ensure-dataset",
                headers=auth_headers,
                json={"dataset_id": "proj.secret_ds"},
            )
            assert ensure_prefixed_disallowed.status_code == 403

            datasets_disallowed = client.post(
                "/datasets",
                headers=auth_headers,
                json={"dataset_id": "secret_ds"},
            )
            assert datasets_disallowed.status_code == 403

            tables_disallowed = client.post(
                "/tables",
                headers=auth_headers,
                json={
                    "dataset_id": "secret_ds",
                    "table_id": "items",
                    "schema": [{"name": "id", "type": "STRING"}],
                },
            )
            assert tables_disallowed.status_code == 403
            mock_bq_client.get_dataset.assert_not_called()

            # Same-project prefix on /ensure-dataset normalizes to "raw_ds" and succeeds
            ensure_prefixed_ok = client.post(
                "/ensure-dataset",
                headers=auth_headers,
                json={"dataset_id": "proj.raw_ds"},
            )
            assert ensure_prefixed_ok.status_code == 200
            assert ensure_prefixed_ok.json()["dataset_id"] == "raw_ds"

            # Colon-qualified same-project dataset (from /dry-run-query NotFound) succeeds on /ensure-dataset
            ensure_colon_ok = client.post(
                "/ensure-dataset",
                headers=auth_headers,
                json={"dataset_id": "proj:raw_ds"},
            )
            assert ensure_colon_ok.status_code == 200
            assert ensure_colon_ok.json()["dataset_id"] == "raw_ds"

            # Colon-qualified disallowed dataset returns 403; cross-project returns 400/422
            ensure_colon_disallowed = client.post(
                "/ensure-dataset",
                headers=auth_headers,
                json={"dataset_id": "proj:secret_ds"},
            )
            assert ensure_colon_disallowed.status_code == 403

            ensure_colon_cross = client.post(
                "/ensure-dataset",
                headers=auth_headers,
                json={"dataset_id": "evil-proj:raw_ds"},
            )
            assert ensure_colon_cross.status_code in {400, 422}

            # /datasets continues to reject qualified dataset IDs
            datasets_colon_rejected = client.post(
                "/datasets",
                headers=auth_headers,
                json={"dataset_id": "proj:raw_ds"},
            )
            assert datasets_colon_rejected.status_code in {400, 422}

            # Simple allowed dataset on /datasets succeeds
            datasets_ok = client.post(
                "/datasets",
                headers=auth_headers,
                json={"dataset_id": "raw_ds"},
            )
            assert datasets_ok.status_code == 200
            assert datasets_ok.json()["dataset_id"] == "raw_ds"
    finally:
        app.dependency_overrides.clear()


def test_cors_rejects_wildcard_star_origin_and_logs_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    # 1. ALLOWED_ORIGINS="*" must NOT allow wildcard reflection with credentials
    monkeypatch.setenv("ALLOWED_ORIGINS", "*")
    with caplog.at_level("WARNING"):
        star_app = ingestion_main.create_app()
    assert any("*" in rec.message for rec in caplog.records)

    with TestClient(star_app) as client:
        star_preflight = client.options(
            "/buckets",
            headers={
                "Origin": "https://evil.example.com",
                "Access-Control-Request-Method": "GET",
            },
        )
        assert star_preflight.headers.get("access-control-allow-origin") is None
        assert star_preflight.headers.get("access-control-allow-credentials") is None

    # 2. ALLOWED_ORIGINS="*, https://ui.example.com" drops "*" while keeping explicit origin
    monkeypatch.setenv("ALLOWED_ORIGINS", "*, https://ui.example.com")
    mixed_app = ingestion_main.create_app()
    with TestClient(mixed_app) as client:
        evil_preflight = client.options(
            "/buckets",
            headers={
                "Origin": "https://evil.example.com",
                "Access-Control-Request-Method": "GET",
            },
        )
        assert evil_preflight.headers.get("access-control-allow-origin") is None

        ui_preflight = client.options(
            "/buckets",
            headers={
                "Origin": "https://ui.example.com",
                "Access-Control-Request-Method": "GET",
            },
        )
        assert (
            ui_preflight.headers.get("access-control-allow-origin")
            == "https://ui.example.com"
        )
        assert ui_preflight.headers.get("access-control-allow-credentials") == "true"


def test_upload_guard_enforces_auth_before_multipart_parse_and_caps_raw_stream(
    monkeypatch: pytest.MonkeyPatch,
):
    import asyncio
    import json as _json

    app, mock_gcs_client, _ = _build_test_app_and_mocks(monkeypatch)
    mock_blob = mock_gcs_client.lookup_bucket.return_value.blob.return_value
    malformed_headers = {"Content-Type": "multipart/form-data; boundary=xyz"}

    try:
        with TestClient(app) as client:
            # 1a. Unconfigured auth + malformed multipart -> 403 (never 400/422 from form parser)
            _clear_auth_env(monkeypatch)
            unconf_resp = client.post(
                "/upload",
                content=b"not-valid-multipart",
                headers=malformed_headers,
            )
            assert unconf_resp.status_code == 403

            # 1b. Missing credentials + malformed multipart -> 401 with WWW-Authenticate
            monkeypatch.setenv("INGESTION_ADMIN_TOKEN", "topsecret")
            missing_resp = client.post(
                "/upload",
                content=b"not-valid-multipart",
                headers=malformed_headers,
            )
            assert missing_resp.status_code == 401
            assert missing_resp.headers.get("www-authenticate") == "Bearer"

            # 1c. Wrong X-Admin-Token + malformed multipart -> 403
            wrong_resp = client.post(
                "/upload",
                content=b"not-valid-multipart",
                headers={**malformed_headers, "X-Admin-Token": "wrong"},
            )
            assert wrong_resp.status_code == 403

            # 2. Small MAX_UPLOAD_SIZE_BYTES=64 allows 16-byte CSV inside ~210-byte multipart
            #    envelope (200) while still rejecting 100-byte CSV (413)
            monkeypatch.setenv("MAX_UPLOAD_SIZE_BYTES", "64")
            auth_headers = {"X-Admin-Token": "topsecret"}

            tiny_ok = client.post(
                "/upload",
                headers=auth_headers,
                files={"file": ("tiny.csv", b"id,name\n1,Alice\n", "text/csv")},
            )
            assert tiny_ok.status_code == 200
            mock_blob.upload_from_filename.reset_mock()

            hundred_byte_csv = b"id,name\n" + (b"1,Alice_Row\n" * 9)
            assert len(hundred_byte_csv) > 64
            over_file_cap = client.post(
                "/upload",
                headers=auth_headers,
                files={"file": ("over.csv", hundred_byte_csv, "text/csv")},
            )
            assert over_file_cap.status_code == 413
            mock_blob.upload_from_filename.assert_not_called()

            # 3. Authenticated stream > max_bytes + 65536 aborts at ASGI layer with 413
            #    BEFORE upload_file dependencies or handler ever execute
            app.dependency_overrides[ingestion_main.get_storage_service] = (
                lambda: (_ for _ in ()).throw(
                    AssertionError("storage dependency must not run")
                )
            )
            app.dependency_overrides[ingestion_main.get_schema_detection_service] = (
                lambda: (_ for _ in ()).throw(
                    AssertionError("schema dependency must not run")
                )
            )

            huge_multipart = (
                b"--xyz\r\n"
                b'Content-Disposition: form-data; name="file"; filename="huge.csv"\r\n'
                b"Content-Type: text/csv\r\n\r\n"
                + (b"1,2\n" * 20000)
                + b"\r\n--xyz--\r\n"
            )
            assert len(huge_multipart) > 64 + 65536

            # 3a. Spoofed small Content-Length via TestClient -> 413 before dependencies run
            spoofed_huge_resp = client.post(
                "/upload",
                content=huge_multipart,
                headers={
                    **auth_headers,
                    "Content-Type": "multipart/form-data; boundary=xyz",
                    "Content-Length": "32",
                },
            )
            assert spoofed_huge_resp.status_code == 413
            assert (
                spoofed_huge_resp.json()["detail"]
                == "Uploaded file exceeds maximum allowed size"
            )

        # 3b. Direct ASGI chunked stream stops pulling chunks as soon as raw bytes > 64 + 65536
        chunks_pulled = 0
        total_chunks = 50  # 50 * 8192 = 400 KiB

        async def _run_raw_asgi_stream():
            nonlocal chunks_pulled
            sent_messages = []

            async def receive():
                nonlocal chunks_pulled
                if chunks_pulled == 0:
                    chunks_pulled += 1
                    return {
                        "type": "http.request",
                        "body": (
                            b"--xyz\r\n"
                            b'Content-Disposition: form-data; name="file"; filename="huge.csv"\r\n'
                            b"Content-Type: text/csv\r\n\r\n"
                            + (b"a" * 8192)
                        ),
                        "more_body": True,
                    }
                if chunks_pulled < total_chunks:
                    chunks_pulled += 1
                    return {
                        "type": "http.request",
                        "body": b"a" * 8192,
                        "more_body": chunks_pulled < total_chunks,
                    }
                return {"type": "http.request", "body": b"", "more_body": False}

            async def send(message):
                sent_messages.append(message)

            scope = {
                "type": "http",
                "asgi": {"version": "3.0"},
                "http_version": "1.1",
                "method": "POST",
                "scheme": "http",
                "path": "/upload",
                "raw_path": b"/upload",
                "query_string": b"",
                "headers": [
                    (b"x-admin-token", b"topsecret"),
                    (b"content-type", b"multipart/form-data; boundary=xyz"),
                ],
            }
            await app(scope, receive, send)
            return sent_messages

        messages = asyncio.run(_run_raw_asgi_stream())
        start_msg = next(m for m in messages if m["type"] == "http.response.start")
        body_bytes = b"".join(
            m.get("body", b"") for m in messages if m["type"] == "http.response.body"
        )
        assert start_msg["status"] == 413
        assert (
            _json.loads(body_bytes)["detail"]
            == "Uploaded file exceeds maximum allowed size"
        )
        assert chunks_pulled <= 10, (
            f"Expected ASGI stream guard to abort after ~9 chunks, pulled {chunks_pulled}"
        )
    finally:
        app.dependency_overrides.clear()


def test_upload_validates_schema_before_gcs_upload_and_rejects_non_object_json(
    monkeypatch: pytest.MonkeyPatch,
):
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("INGESTION_ADMIN_TOKEN", "topsecret")
    monkeypatch.delenv("MAX_UPLOAD_SIZE_BYTES", raising=False)
    auth_headers = {"X-Admin-Token": "topsecret"}

    app, mock_gcs_client, _ = _build_test_app_and_mocks(monkeypatch)
    mock_blob = mock_gcs_client.lookup_bucket.return_value.blob.return_value

    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            for filename, payload, mime in (
                ("broken.json", b"{not valid json}", "application/json"),
                ("broken.csv", b"\xff\xfe\x00\x00", "text/csv"),
                ("scalar_num.json", b"42", "application/json"),
                ("scalar_str.json", b'"hello"', "application/json"),
                ("scalar_null.json", b"null", "application/json"),
                ("prim_array.json", b"[1, 2, 3]", "application/json"),
                ("mixed_array.json", b'[{"id": 1}, "bad"]', "application/json"),
            ):
                resp = client.post(
                    "/upload",
                    headers=auth_headers,
                    files={"file": (filename, payload, mime)},
                )
                assert resp.status_code == 400, (
                    f"Expected 400 for {filename}, got {resp.status_code}: {resp.text}"
                )
                assert resp.json()["detail"] == "Malformed CSV or JSON file content."
                mock_blob.upload_from_filename.assert_not_called()
    finally:
        app.dependency_overrides.clear()


def test_cors_origins_restricted_and_oidc_bearer_accepted(
    monkeypatch: pytest.MonkeyPatch,
):
    assert hasattr(ingestion_main, "create_app")
    assert not hasattr(ingestion_main, "get_sql_fix_service")

    # 1. Default (unset ALLOWED_ORIGINS) denies cross-origin wildcard
    monkeypatch.delenv("ALLOWED_ORIGINS", raising=False)
    default_app = ingestion_main.create_app()
    with TestClient(default_app) as client:
        preflight = client.options(
            "/buckets",
            headers={
                "Origin": "https://evil.example.com",
                "Access-Control-Request-Method": "GET",
            },
        )
        assert preflight.headers.get("access-control-allow-origin") is None

    # 2. Explicit ALLOWED_ORIGINS permits listed origin + X-Admin-Token / Authorization
    monkeypatch.setenv(
        "ALLOWED_ORIGINS", "https://ui.example.com, https://admin.example.com"
    )
    configured_app, _, _ = _build_test_app_and_mocks(monkeypatch)
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("INGESTION_AUTH_AUDIENCE", "https://ingestion.run.app")
    monkeypatch.setenv("INGESTION_ALLOWED_PRINCIPALS", "ops@example.com")
    monkeypatch.setattr(
        auth,
        "verify_bearer_token",
        lambda token, audience: {
            "iss": "https://accounts.google.com",
            "email": "ops@example.com",
            "email_verified": True,
        },
    )
    try:
        with TestClient(configured_app) as client:
            allowed_preflight = client.options(
                "/buckets",
                headers={
                    "Origin": "https://ui.example.com",
                    "Access-Control-Request-Method": "GET",
                    "Access-Control-Request-Headers": "Authorization, X-Admin-Token",
                },
            )
            assert (
                allowed_preflight.headers.get("access-control-allow-origin")
                == "https://ui.example.com"
            )
            assert (
                allowed_preflight.headers.get("access-control-allow-credentials")
                == "true"
            )

            oidc_resp = client.get(
                "/buckets",
                headers={"Authorization": "Bearer valid-oidc-jwt"},
            )
            assert oidc_resp.status_code == 200
    finally:
        configured_app.dependency_overrides.clear()


def test_dry_run_not_found_and_error_sanitization(monkeypatch: pytest.MonkeyPatch):
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("INGESTION_ADMIN_TOKEN", "topsecret")
    auth_headers = {"X-Admin-Token": "topsecret"}

    app, _, _ = _build_test_app_and_mocks(monkeypatch, project_id="proj")

    # 1. google.api_core.exceptions.NotFound is handled cleanly without AttributeError
    mock_query_client = MagicMock()
    mock_query_client.query.side_effect = NotFound(
        "Dataset proj.missing_ds not found in location US"
    )
    monkeypatch.setattr(
        ingestion_main.bigquery, "Client", lambda project=None: mock_query_client
    )

    try:
        with TestClient(app) as client:
            nf_resp = client.post(
                "/dry-run-query",
                headers=auth_headers,
                json={"sql_script": "SELECT * FROM `proj.missing_ds.tbl`"},
            )
            assert nf_resp.status_code == 200
            nf_body = nf_resp.json()
            assert nf_body["valid"] is False
            assert nf_body["details"]["missing_dataset"] == "proj.missing_ds"

            # 1b. Colon-qualified 'was not found' NotFound + unmatched NotFound fallback
            mock_query_client.query.side_effect = NotFound(
                "GET https://bigquery.googleapis.com/bigquery/v2/projects/secret-proj/datasets/missing_ds: "
                "Not found: Dataset proj:missing_ds was not found in location US"
            )
            nf_colon_resp = client.post(
                "/dry-run-query",
                headers=auth_headers,
                json={"sql_script": "SELECT * FROM `proj.missing_ds.tbl`"},
            )
            assert nf_colon_resp.status_code == 200
            nf_colon_body = nf_colon_resp.json()
            assert nf_colon_body["valid"] is False
            assert nf_colon_body["details"]["missing_dataset"] == "proj:missing_ds"
            assert "https://" not in nf_colon_body["error"]
            assert "secret-proj" not in nf_colon_body["error"]

            mock_query_client.query.side_effect = NotFound(
                "Not found: Table proj:raw_ds.missing_tbl was not found in location US"
            )
            nf_tbl_resp = client.post(
                "/dry-run-query",
                headers=auth_headers,
                json={"sql_script": "SELECT * FROM `proj.raw_ds.missing_tbl`"},
            )
            assert nf_tbl_resp.status_code == 200
            assert (
                nf_tbl_resp.json()["details"]["missing_table"]
                == "proj:raw_ds.missing_tbl"
            )

            mock_query_client.query.side_effect = NotFound(
                "GET https://bigquery.googleapis.com/bigquery/v2/projects/secret-proj/jobs/j99: "
                "Resource was not found"
            )
            nf_unmatched_resp = client.post(
                "/dry-run-query",
                headers=auth_headers,
                json={"sql_script": "SELECT 1"},
            )
            assert nf_unmatched_resp.status_code == 200
            assert (
                nf_unmatched_resp.json()["error"]
                == "Referenced BigQuery dataset or table was not found."
            )
            assert "secret-proj" not in nf_unmatched_resp.json()["error"]
            assert "https://" not in nf_unmatched_resp.json()["error"]

            # 2. BadRequest surfaces syntax feedback cleanly while scrubbing raw REST URLs
            mock_query_client.query.side_effect = BadRequest(
                "POST https://bigquery.googleapis.com/bigquery/v2/projects/secret-proj/jobs?prettyPrint=false: "
                "Invalid field reference 'foo_col'"
            )
            br_resp = client.post(
                "/dry-run-query",
                headers=auth_headers,
                json={"sql_script": "SELECT foo_col FROM tbl"},
            )
            assert br_resp.status_code == 200
            assert br_resp.json()["valid"] is False
            assert br_resp.json()["details"]["missing_field"] == "foo_col"
            assert "https://" not in br_resp.json()["error"]
            assert "secret-proj" not in br_resp.json()["error"]
            assert not br_resp.json()["error"].startswith("400 ")

            # 3. Unexpected internal exception in /dry-run-query does not leak raw secret details or Python class names
            mock_query_client.query.side_effect = RuntimeError(
                "secret-sa-key-path=/var/secrets/key.json"
            )
            err_resp = client.post(
                "/dry-run-query",
                headers=auth_headers,
                json={"sql_script": "SELECT 1"},
            )
            assert err_resp.status_code == 200
            assert err_resp.json()["valid"] is False
            assert "secret-sa-key-path" not in (err_resp.json().get("error") or "")
            assert err_resp.json()["details"] == {"error_type": "internal_error"}

            mock_query_client.query.side_effect = RuntimeError(
                "504 Deadline Exceeded on internal-host.corp"
            )
            timeout_resp = client.post(
                "/dry-run-query",
                headers=auth_headers,
                json={"sql_script": "SELECT 1"},
            )
            assert timeout_resp.json()["details"] == {"error_type": "timeout"}
            assert "internal-host.corp" not in timeout_resp.json()["error"]

            # 4. Unexpected internal exception on 500 routes does not leak raw exception text
            broken_storage = MagicMock()
            broken_storage.list_buckets.side_effect = RuntimeError(
                "internal-cred-leak-98765"
            )
            app.dependency_overrides[ingestion_main.get_storage_service] = (
                lambda: broken_storage
            )
            buckets_500 = client.get("/buckets", headers=auth_headers)
            assert buckets_500.status_code == 500
            assert "internal-cred-leak-98765" not in buckets_500.json()["detail"]
    finally:
        app.dependency_overrides.clear()


def test_os_permission_errors_config_value_errors_and_malformed_uploads_are_sanitized(
    monkeypatch: pytest.MonkeyPatch,
):
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("INGESTION_ADMIN_TOKEN", "topsecret")
    monkeypatch.delenv("INGESTION_ALLOWED_DATASETS", raising=False)
    auth_headers = {"X-Admin-Token": "topsecret"}

    app, _, _ = _build_test_app_and_mocks(monkeypatch, project_id="proj")
    try:
        with TestClient(app, raise_server_exceptions=False) as client:
            # 1. Malformed JSON/UTF-8 content in /upload returns fixed safe 400 detail
            bad_json_resp = client.post(
                "/upload",
                headers=auth_headers,
                files={
                    "file": (
                        "broken.json",
                        b"{not valid json: /internal/path}",
                        "application/json",
                    )
                },
            )
            assert bad_json_resp.status_code == 400
            assert (
                bad_json_resp.json()["detail"]
                == "Malformed CSV or JSON file content."
            )

            bad_utf8_resp = client.post(
                "/upload",
                headers=auth_headers,
                files={"file": ("broken.csv", b"\xff\xfe\x00\x00", "text/csv")},
            )
            assert bad_utf8_resp.status_code == 400
            assert (
                bad_utf8_resp.json()["detail"]
                == "Malformed CSV or JSON file content."
            )

            # 2. Invalid server MAX_UPLOAD_SIZE_BYTES env var returns sanitized 500 (not 400)
            app.dependency_overrides[ingestion_main.get_storage_service] = (
                lambda: MagicMock()
            )
            app.dependency_overrides[ingestion_main.get_schema_detection_service] = (
                lambda: MagicMock()
            )
            monkeypatch.setenv("MAX_UPLOAD_SIZE_BYTES", "broken-server-env")
            bad_env_resp = client.post(
                "/upload",
                headers=auth_headers,
                files={"file": ("ok.csv", b"id\n1\n", "text/csv")},
            )
            assert bad_env_resp.status_code == 500
            assert "broken-server-env" not in bad_env_resp.json()["detail"]
            monkeypatch.delenv("MAX_UPLOAD_SIZE_BYTES", raising=False)

            # 3. OS PermissionError (errno=EACCES) inside services returns sanitized 500 (never 403!)
            os_perm_err = PermissionError(
                errno.EACCES, "Permission denied", "/var/secrets/sa-key.json"
            )
            broken_bq = MagicMock()
            broken_bq.create_dataset.side_effect = os_perm_err
            app.dependency_overrides[ingestion_main.get_bigquery_service] = (
                lambda: broken_bq
            )
            ds_os_perm = client.post(
                "/datasets",
                headers=auth_headers,
                json={"dataset_id": "raw_ds"},
            )
            assert ds_os_perm.status_code == 500
            assert "/var/secrets/sa-key.json" not in ds_os_perm.json()["detail"]

            # 4. Downstream internal ValueError inside services returns sanitized 500 (never 400!)
            broken_bq.create_dataset.side_effect = ValueError(
                "internal-sdk-parse-secret-xyz"
            )
            ds_val_err = client.post(
                "/datasets",
                headers=auth_headers,
                json={"dataset_id": "raw_ds"},
            )
            assert ds_val_err.status_code == 500
            assert "internal-sdk-parse-secret-xyz" not in ds_val_err.json()["detail"]

            # 5. Global _permission_error_handler also treats OS PermissionError as sanitized 500
            @app.get("/_test_os_perm")
            async def _raise_os_perm():
                raise os_perm_err

            global_os_perm = client.get("/_test_os_perm")
            assert global_os_perm.status_code == 500
            assert "/var/secrets/sa-key.json" not in global_os_perm.json()["detail"]
    finally:
        app.dependency_overrides.clear()

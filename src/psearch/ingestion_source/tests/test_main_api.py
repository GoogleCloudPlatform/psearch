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
from typing import Any, Dict, Tuple
from unittest.mock import MagicMock
import uuid

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

            # 2. BadRequest still surfaces syntax feedback cleanly
            mock_query_client.query.side_effect = BadRequest(
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

            # 3. Unexpected internal exception in /dry-run-query does not leak raw secret details
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

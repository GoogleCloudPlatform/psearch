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

"""Authentication, lazy DI initialization, CORS, validation, and sanitization tests for gen_ai."""

import base64
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple
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

from fastapi import HTTPException
from google.api_core.exceptions import BadRequest, Forbidden, NotFound
from google.auth.exceptions import GoogleAuthError
from google.genai.types import FinishReason
import pytest
import requests

try:
    from psearch.gen_ai import auth, main as gen_ai_main
    from psearch.gen_ai.services import (
        enrichment_service as enrichment_service_mod,
        image_fetcher,
        imagen_service as imagen_service_mod,
        marketing_service as marketing_service_mod,
    )
    from psearch.gen_ai.services.conversational_search_service import (
        ConversationalSearchResponse,
    )
    from psearch.gen_ai.services.enrichment_service import EnrichmentService
    from psearch.gen_ai.services.imagen_service import ImageGenerationService
    from psearch.gen_ai.services.marketing_service import MarketingService
    from psearch.gen_ai.services.sql.common.input_validation import (
        MAX_GENERATED_SQL_CHARS,
    )
    from psearch.gen_ai.services.sql.pipeline.transformation_pipeline import (
        TransformationPipeline,
    )
    from psearch.gen_ai.services.sql.validation.sql_validator import SQLValidator
    from psearch.gen_ai.tasks import task_manager
except ModuleNotFoundError as exc:
    if exc.name not in {
        "psearch",
        "psearch.gen_ai",
        "psearch.gen_ai.services",
        "psearch.gen_ai.tasks",
    }:
        raise
    from gen_ai import auth, main as gen_ai_main
    from gen_ai.services import (
        enrichment_service as enrichment_service_mod,
        image_fetcher,
        imagen_service as imagen_service_mod,
        marketing_service as marketing_service_mod,
    )
    from gen_ai.services.conversational_search_service import (
        ConversationalSearchResponse,
    )
    from gen_ai.services.enrichment_service import EnrichmentService
    from gen_ai.services.imagen_service import ImageGenerationService
    from gen_ai.services.marketing_service import MarketingService
    from gen_ai.services.sql.common.input_validation import (
        MAX_GENERATED_SQL_CHARS,
    )
    from gen_ai.services.sql.pipeline.transformation_pipeline import (
        TransformationPipeline,
    )
    from gen_ai.services.sql.validation.sql_validator import SQLValidator
    from gen_ai.tasks import task_manager


_REPO_ROOT = Path(__file__).resolve().parents[4]
_SRC_DIR = _REPO_ROOT / "src"
_PSEARCH_DIR = _SRC_DIR / "psearch"

_AUTH_ENV_VARS = (
    "GEN_AI_ADMIN_TOKEN",
    "ADMIN_TOKEN",
    "GEN_AI_AUTH_AUDIENCE",
    "GEN_AI_ALLOWED_PRINCIPALS",
    "GEN_AI_ALLOWED_DOMAINS",
)

_EXISTING_TASK_ID = "550e8400-e29b-41d4-a716-446655440000"


def _clear_auth_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in _AUTH_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def _build_test_app_and_mocks(
    monkeypatch: pytest.MonkeyPatch,
) -> Tuple[Any, Dict[str, MagicMock]]:
    """Build a FastAPI app with mocked GenAI / BigQuery downstream services."""
    monkeypatch.setenv("PROJECT_ID", "test-project")
    task_manager.task_status.clear()
    task_manager.init_task(
        _EXISTING_TASK_ID,
        task_type="sql_generation",
        input_details={"source_table_name": "psearch-dev-ze.raw_data.catalog"},
    )

    conv_mock = MagicMock()
    conv_mock.process_query.return_value = ConversationalSearchResponse(
        answer="Filtered results",
        suggested_products=[{"id": "prod-1", "reason": "Matches query"}],
        follow_up_questions=["Any preferred color?"],
    )

    enrichment_mock = MagicMock()
    enrichment_mock.process_enrichment.return_value = {
        "product_id": "prod-1",
        "enriched_fields": {"description": "Enriched description"},
    }

    marketing_mock = MagicMock()
    marketing_mock.generate_content.return_value = {
        "product_id": "prod-1",
        "content": "Compelling marketing copy",
    }

    image_mock = MagicMock()
    image_mock.generate_image.return_value = {
        "product_id": "prod-1",
        "generated_image_base64": "YWJj",
    }

    sql_fix_mock = MagicMock()
    sql_fix_mock.fixer.simple_fix_sql.return_value = ("SELECT 1;", None)
    sql_fix_mock.validate_sql.return_value = {
        "valid": True,
        "message": "SQL syntax validated successfully",
        "details": {"estimated_bytes_processed": 128},
    }
    sql_fix_mock.generate_sql_fix.return_value = {
        "success": True,
        "suggested_sql": "SELECT 1;",
        "analysis": {"diff_text": "--- a\n+++ b\n"},
        "error": None,
    }
    sql_fix_mock.diff_analyzer.analyze_sql_differences.return_value = {
        "diff": "--- a\n+++ b\n",
        "changes": ["Fixed query"],
        "removed_lines_count": 1,
        "added_lines_count": 1,
    }

    pipeline_mock = MagicMock()
    pipeline_mock.execute_pipeline.return_value = None

    app = gen_ai_main.create_app()
    app.dependency_overrides[gen_ai_main.get_conversational_search_service] = (
        lambda: conv_mock
    )
    app.dependency_overrides[gen_ai_main.get_enrichment_service] = (
        lambda: enrichment_mock
    )
    app.dependency_overrides[gen_ai_main.get_marketing_service] = (
        lambda: marketing_mock
    )
    app.dependency_overrides[gen_ai_main.get_image_generation_service] = (
        lambda: image_mock
    )
    app.dependency_overrides[gen_ai_main.get_sql_fix_service] = lambda: sql_fix_mock
    app.dependency_overrides[gen_ai_main.get_transformation_pipeline] = (
        lambda: pipeline_mock
    )

    mocks = {
        "conversational": conv_mock,
        "enrichment": enrichment_mock,
        "marketing": marketing_mock,
        "image": image_mock,
        "sql_fix": sql_fix_mock,
        "pipeline": pipeline_mock,
    }
    return app, mocks


_ELEVEN_OPERATIONAL_ROUTES = [
    (
        "POST",
        "/conversational-search",
        {"json": {"query": "running shoes"}},
    ),
    (
        "POST",
        "/enrichment",
        {"json": {"product_id": "p1", "product_data": {"title": "Shoe"}}},
    ),
    (
        "POST",
        "/marketing",
        {
            "json": {
                "product_id": "p1",
                "product_data": {"title": "Shoe"},
                "content_type": "product_description",
            }
        },
    ),
    (
        "POST",
        "/generate-enhanced-image",
        {
            "json": {
                "product_id": "p1",
                "product_data": {"title": "Shoe"},
                "image_base64": "YWJj",
                "background_prompt": "studio lighting",
            }
        },
    ),
    (
        "POST",
        "/generate-sql",
        {
            "json": {
                "source_table": "psearch-dev-ze.raw_data.product_catalog",
                "destination_table": "psearch-dev-ze.processed_data.products",
                "source_schema_fields": ["id", "title"],
                "destination_schema": {
                    "fields": [{"name": "id", "type": "STRING", "mode": "REQUIRED"}]
                },
            }
        },
    ),
    (
        "GET",
        f"/sql/task-status/{_EXISTING_TASK_ID}",
        {},
    ),
    (
        "GET",
        "/sql/tasks-summary",
        {},
    ),
    (
        "POST",
        "/sql/simple-fix",
        {
            "json": {
                "sql_script": "SELECT * FORM tbl",
                "error_message": "Syntax error",
            }
        },
    ),
    (
        "POST",
        "/sql/validate",
        {"json": {"sql_script": "SELECT 1", "timeout_seconds": 10}},
    ),
    (
        "POST",
        "/sql/fix",
        {
            "json": {
                "original_sql": "SELECT bad FROM tbl",
                "current_sql": "SELECT bad FROM tbl",
                "error_message": "Unrecognized name: bad",
                "attempt_number": 1,
            }
        },
    ),
    (
        "POST",
        "/sql/analyze",
        {
            "json": {
                "original_sql": "SELECT bad FROM tbl",
                "fixed_sql": "SELECT NULL AS bad FROM tbl",
            }
        },
    ),
]


def test_import_gen_ai_main_succeeds_without_gcp_credentials_or_project_id(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    """Importing gen_ai.main under either PYTHONPATH root must not touch GCP SDK clients."""
    script = (
        "import sys\n"
        "from google import genai\n"
        "from google.cloud import bigquery\n"
        "def _forbid(*a, **kw):\n"
        "    raise AssertionError('GCP client instantiated at import time')\n"
        "genai.Client = _forbid\n"
        "bigquery.Client = _forbid\n"
        "import importlib\n"
        "mod = importlib.import_module(sys.argv[1])\n"
        "assert hasattr(mod, 'app')\n"
        "assert hasattr(mod, 'create_app')\n"
    )

    for pythonpath, module_name in (
        (str(_SRC_DIR), "psearch.gen_ai.main"),
        (str(_PSEARCH_DIR), "gen_ai.main"),
    ):
        clean_env = {
            key: val
            for key, val in os.environ.items()
            if key
            not in {
                "PROJECT_ID",
                "GOOGLE_CLOUD_PROJECT",
                "GCLOUD_PROJECT",
                "GOOGLE_APPLICATION_CREDENTIALS",
            }
        }
        clean_env["HOME"] = str(tmp_path)
        clean_env["PYTHONPATH"] = pythonpath

        completed = subprocess.run(
            [sys.executable, "-W", "error::DeprecationWarning", "-c", script, module_name],
            env=clean_env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, (
            f"Importing {module_name} with PYTHONPATH={pythonpath} failed:\n"
            f"stdout={completed.stdout}\nstderr={completed.stderr}"
        )

    # In-process check: missing PROJECT_ID raises sanitized 500 when lazy factories are invoked
    gen_ai_main.clear_service_cache()
    monkeypatch.delenv("PROJECT_ID", raising=False)
    factories = (
        gen_ai_main.get_conversational_search_service,
        gen_ai_main.get_enrichment_service,
        gen_ai_main.get_marketing_service,
        gen_ai_main.get_image_generation_service,
        gen_ai_main.get_sql_fix_service,
        gen_ai_main.get_transformation_pipeline,
    )
    for factory in factories:
        with pytest.raises(HTTPException) as exc_info:
            factory()
        assert exc_info.value.status_code == 500
        assert exc_info.value.detail == "Internal server configuration error"

    # When PROJECT_ID is configured, lazy factories initialize once and cache per (project, location)
    monkeypatch.setenv("PROJECT_ID", "lazy-proj")
    monkeypatch.setenv("GENAI_LOCATION", "global")
    monkeypatch.setattr("google.genai.Client", lambda **kwargs: MagicMock())
    monkeypatch.setattr("google.cloud.bigquery.Client", lambda **kwargs: MagicMock())
    try:
        for factory in factories:
            first = factory()
            second = factory()
            assert first is second
    finally:
        gen_ai_main.clear_service_cache()


def test_health_routes_are_public_and_openapi_docs_are_disabled(
    monkeypatch: pytest.MonkeyPatch,
):
    _clear_auth_env(monkeypatch)
    app, _ = _build_test_app_and_mocks(monkeypatch)
    try:
        with TestClient(app) as client:
            root_resp = client.get("/")
            assert root_resp.status_code == 200
            assert root_resp.json() == {"message": "Gen AI Service is running"}

            health_resp = client.get("/health")
            assert health_resp.status_code == 200
            assert health_resp.json() == {"status": "healthy"}

            for doc_path in ("/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"):
                unconf_doc = client.get(doc_path)
                assert unconf_doc.status_code == 404

            monkeypatch.setenv("GEN_AI_ADMIN_TOKEN", "secret")
            for doc_path in ("/openapi.json", "/docs", "/docs/oauth2-redirect", "/redoc"):
                assert client.get(doc_path).status_code == 404
                assert (
                    client.get(
                        doc_path, headers={"X-Admin-Token": "secret"}
                    ).status_code
                    == 404
                )
    finally:
        app.dependency_overrides.clear()


@pytest.mark.parametrize("method,path,req_kwargs", _ELEVEN_OPERATIONAL_ROUTES)
def test_all_eleven_operational_routes_enforce_fail_closed_auth(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
    req_kwargs: Dict[str, Any],
):
    assert len(_ELEVEN_OPERATIONAL_ROUTES) == 11
    app, _ = _build_test_app_and_mocks(monkeypatch)
    try:
        with TestClient(app) as client:
            # 1. Unconfigured env -> fail-closed 403
            _clear_auth_env(monkeypatch)
            unconf_resp = client.request(method, path, **req_kwargs)
            assert unconf_resp.status_code == 403
            assert unconf_resp.json()["detail"] == "Gen AI authentication is not configured"

            # 2. GEN_AI_ADMIN_TOKEN configured + missing header -> 401 with Bearer challenge
            monkeypatch.setenv("GEN_AI_ADMIN_TOKEN", "secret")
            missing_resp = client.request(method, path, **req_kwargs)
            assert missing_resp.status_code == 401
            assert missing_resp.headers.get("www-authenticate") == "Bearer"

            # 3. Wrong X-Admin-Token -> 403 (even if Authorization Bearer is also present)
            wrong_resp = client.request(
                method,
                path,
                headers={
                    "X-Admin-Token": "wrong",
                    "Authorization": "Bearer some-token",
                },
                **req_kwargs,
            )
            assert wrong_resp.status_code == 403

            # 4. Valid X-Admin-Token -> 200
            ok_resp = client.request(
                method,
                path,
                headers={"X-Admin-Token": "secret"},
                **req_kwargs,
            )
            assert ok_resp.status_code == 200

            # 5. Fallback ADMIN_TOKEN works when GEN_AI_ADMIN_TOKEN is unset
            monkeypatch.delenv("GEN_AI_ADMIN_TOKEN", raising=False)
            monkeypatch.setenv("ADMIN_TOKEN", "fallback-secret")
            fallback_ok = client.request(
                method,
                path,
                headers={"X-Admin-Token": "fallback-secret"},
                **req_kwargs,
            )
            assert fallback_ok.status_code == 200
    finally:
        app.dependency_overrides.clear()


def test_oidc_bearer_enforces_audience_verified_email_and_principal_or_hd_domain(
    monkeypatch: pytest.MonkeyPatch,
):
    # Verify direct Python invocation of require_auth() normalizes FastAPI Header defaults
    _clear_auth_env(monkeypatch)
    with pytest.raises(HTTPException) as unconf_direct:
        auth.require_auth()
    assert unconf_direct.value.status_code == 403
    assert unconf_direct.value.detail == "Gen AI authentication is not configured"

    monkeypatch.setenv("GEN_AI_ADMIN_TOKEN", "secret")
    with pytest.raises(HTTPException) as missing_direct:
        auth.require_auth()
    assert missing_direct.value.status_code == 401
    assert missing_direct.value.headers == {"WWW-Authenticate": "Bearer"}

    # Verify verify_bearer_token reuses a module-level google_requests.Request session
    seen_requests = []

    def fake_verify_oauth2(token: str, request_obj: Any, audience: str = None):
        seen_requests.append((token, request_obj, audience))
        return {"sub": "123"}

    monkeypatch.setattr(auth.id_token, "verify_oauth2_token", fake_verify_oauth2)
    auth.verify_bearer_token("tok-1", "https://gen-ai.run.app")
    auth.verify_bearer_token("tok-2", "https://gen-ai.run.app")
    assert len(seen_requests) == 2
    assert seen_requests[0][1] is seen_requests[1][1]
    assert seen_requests[0][2] == "https://gen-ai.run.app"

    # End-to-end HTTP OIDC checks
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("GEN_AI_AUTH_AUDIENCE", "https://gen-ai.run.app")
    monkeypatch.setenv(
        "GEN_AI_ALLOWED_PRINCIPALS",
        "ops@example.com, ui-sa@proj.iam.gserviceaccount.com",
    )
    monkeypatch.setenv("GEN_AI_ALLOWED_DOMAINS", "@corp.example.com")

    app, _ = _build_test_app_and_mocks(monkeypatch)
    try:
        with TestClient(app) as client:
            # 1. Valid allowlisted principal -> 200
            captured: Dict[str, str] = {}

            def verify_allowlisted(token: str, audience: str) -> Dict[str, Any]:
                captured["token"] = token
                captured["audience"] = audience
                return {
                    "iss": "https://accounts.google.com",
                    "email": "OPS@Example.com",
                    "email_verified": True,
                }

            monkeypatch.setattr(auth, "verify_bearer_token", verify_allowlisted)
            ok_principal = client.get(
                "/sql/tasks-summary",
                headers={"Authorization": "Bearer allowlisted-jwt"},
            )
            assert ok_principal.status_code == 200
            assert captured == {
                "token": "allowlisted-jwt",
                "audience": "https://gen-ai.run.app",
            }

            # 2. Valid Google Workspace domain with matching hd == email_domain -> 200
            monkeypatch.setattr(
                auth,
                "verify_bearer_token",
                lambda token, audience: {
                    "iss": "accounts.google.com",
                    "email": "Alice@Corp.Example.com",
                    "email_verified": True,
                    "hd": "corp.example.com",
                },
            )
            ok_domain = client.get(
                "/sql/tasks-summary",
                headers={"Authorization": "Bearer domain-jwt"},
            )
            assert ok_domain.status_code == 200

            # 3. email_verified=False or string "true" -> 403
            for bad_verified in (False, None, "true", 1):
                monkeypatch.setattr(
                    auth,
                    "verify_bearer_token",
                    lambda token, audience, v=bad_verified: {
                        "iss": "https://accounts.google.com",
                        "email": "ops@example.com",
                        "email_verified": v,
                    },
                )
                unverified_resp = client.get(
                    "/sql/tasks-summary",
                    headers={"Authorization": "Bearer unverified-jwt"},
                )
                assert unverified_resp.status_code == 403

            # 4. Wrong issuer -> 401 with WWW-Authenticate: Bearer
            monkeypatch.setattr(
                auth,
                "verify_bearer_token",
                lambda token, audience: {
                    "iss": "https://evil.example.com",
                    "email": "ops@example.com",
                    "email_verified": True,
                },
            )
            bad_iss_resp = client.get(
                "/sql/tasks-summary",
                headers={"Authorization": "Bearer bad-iss-jwt"},
            )
            assert bad_iss_resp.status_code == 401
            assert bad_iss_resp.headers.get("www-authenticate") == "Bearer"

            # 5. Invalid/expired signature (ValueError / GoogleAuthError) -> 401
            for err in (ValueError("Expired token"), GoogleAuthError("Cert fetch error")):
                def _raise_err(token: str, audience: str, exc=err):
                    raise exc

                monkeypatch.setattr(auth, "verify_bearer_token", _raise_err)
                bad_sig_resp = client.get(
                    "/sql/tasks-summary",
                    headers={"Authorization": "Bearer expired-jwt"},
                )
                assert bad_sig_resp.status_code == 401
                assert bad_sig_resp.headers.get("www-authenticate") == "Bearer"

            # 6. Domain match WITHOUT matching hd claim -> 403
            for bad_hd in (None, "", "other.example.com"):
                monkeypatch.setattr(
                    auth,
                    "verify_bearer_token",
                    lambda token, audience, hd_val=bad_hd: {
                        "iss": "https://accounts.google.com",
                        "email": "bob@corp.example.com",
                        "email_verified": True,
                        "hd": hd_val,
                    },
                )
                missing_hd_resp = client.get(
                    "/sql/tasks-summary",
                    headers={"Authorization": "Bearer no-hd-jwt"},
                )
                assert missing_hd_resp.status_code == 403

            # 7. Unlisted Gmail user -> 403
            monkeypatch.setattr(
                auth,
                "verify_bearer_token",
                lambda token, audience: {
                    "iss": "https://accounts.google.com",
                    "email": "stranger@gmail.com",
                    "email_verified": True,
                },
            )
            unlisted_resp = client.get(
                "/sql/tasks-summary",
                headers={"Authorization": "Bearer gmail-jwt"},
            )
            assert unlisted_resp.status_code == 403

            # 8. Audience configured WITHOUT principals/domains fails closed with 403
            monkeypatch.delenv("GEN_AI_ALLOWED_PRINCIPALS", raising=False)
            monkeypatch.delenv("GEN_AI_ALLOWED_DOMAINS", raising=False)
            no_allowlist_resp = client.get(
                "/sql/tasks-summary",
                headers={"Authorization": "Bearer any-jwt"},
            )
            assert no_allowlist_resp.status_code == 403
    finally:
        app.dependency_overrides.clear()


def test_task_status_validates_uuidv4_and_sql_endpoints_bound_length_and_sanitize_errors(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("GEN_AI_ADMIN_TOKEN", "secret")
    auth_headers = {"X-Admin-Token": "secret"}

    app, mocks = _build_test_app_and_mocks(monkeypatch)
    try:
        with TestClient(app) as client:
            # 1. Non-UUIDv4 task_id rejected with 400 BEFORE task_manager.get_task_status runs
            spy_get_status = MagicMock(wraps=task_manager.get_task_status)
            monkeypatch.setattr(task_manager, "get_task_status", spy_get_status)

            invalid_task_ids = [
                "not-a-uuid",
                str(uuid.uuid1()),
                "00000000-0000-0000-0000-000000000000",
                _EXISTING_TASK_ID.replace("-", ""),
                f"{{{_EXISTING_TASK_ID}}}",
            ]
            for bad_id in invalid_task_ids:
                bad_resp = client.get(
                    f"/sql/task-status/{bad_id}",
                    headers=auth_headers,
                )
                assert bad_resp.status_code == 400
                assert bad_resp.json()["detail"] == "Invalid task_id format"
            spy_get_status.assert_not_called()

            # Valid unknown UUIDv4 reaches task_manager and returns 404
            unknown_uuid = str(uuid.uuid4())
            not_found_resp = client.get(
                f"/sql/task-status/{unknown_uuid}",
                headers=auth_headers,
            )
            assert not_found_resp.status_code == 404
            spy_get_status.assert_called_once_with(unknown_uuid)

            # 2. Free-form SQL & error inputs exceeding MAX_GENERATED_SQL_CHARS (200_000) -> 422
            oversized = "S" * (MAX_GENERATED_SQL_CHARS + 1)

            assert (
                client.post(
                    "/sql/validate",
                    headers=auth_headers,
                    json={"sql_script": oversized},
                ).status_code
                == 422
            )
            assert (
                client.post(
                    "/sql/simple-fix",
                    headers=auth_headers,
                    json={"sql_script": oversized, "error_message": "err"},
                ).status_code
                == 422
            )
            assert (
                client.post(
                    "/sql/simple-fix",
                    headers=auth_headers,
                    json={"sql_script": "SELECT 1", "error_message": oversized},
                ).status_code
                == 422
            )
            assert (
                client.post(
                    "/sql/fix",
                    headers=auth_headers,
                    json={
                        "original_sql": oversized,
                        "current_sql": "SELECT 1",
                        "error_message": "err",
                    },
                ).status_code
                == 422
            )
            assert (
                client.post(
                    "/sql/fix",
                    headers=auth_headers,
                    json={
                        "original_sql": "SELECT 1",
                        "current_sql": oversized,
                        "error_message": "err",
                    },
                ).status_code
                == 422
            )
            assert (
                client.post(
                    "/sql/fix",
                    headers=auth_headers,
                    json={
                        "original_sql": "SELECT 1",
                        "current_sql": "SELECT 1",
                        "error_message": oversized,
                    },
                ).status_code
                == 422
            )
            assert (
                client.post(
                    "/sql/analyze",
                    headers=auth_headers,
                    json={"original_sql": oversized, "fixed_sql": "SELECT 1"},
                ).status_code
                == 422
            )
            assert (
                client.post(
                    "/sql/analyze",
                    headers=auth_headers,
                    json={"original_sql": "SELECT 1", "fixed_sql": oversized},
                ).status_code
                == 422
            )

            # 3. Internal exceptions return sanitized 500 without leaking str(e)
            secret_marker = "secret://internal-url/db-password"
            mocks["conversational"].process_query.side_effect = RuntimeError(secret_marker)
            mocks["enrichment"].process_enrichment.side_effect = RuntimeError(secret_marker)
            mocks["marketing"].generate_content.side_effect = RuntimeError(secret_marker)
            mocks["image"].generate_image.side_effect = RuntimeError(secret_marker)
            mocks["sql_fix"].fixer.simple_fix_sql.side_effect = RuntimeError(secret_marker)
            mocks["sql_fix"].validate_sql.side_effect = RuntimeError(secret_marker)
            mocks["sql_fix"].generate_sql_fix.side_effect = RuntimeError(secret_marker)
            mocks["sql_fix"].diff_analyzer.analyze_sql_differences.side_effect = (
                RuntimeError(secret_marker)
            )
            monkeypatch.setattr(
                task_manager,
                "init_task",
                MagicMock(side_effect=RuntimeError(secret_marker)),
            )
            monkeypatch.setattr(
                task_manager,
                "get_task_status",
                MagicMock(side_effect=RuntimeError(secret_marker)),
            )
            monkeypatch.setattr(
                task_manager,
                "get_all_tasks_summary",
                MagicMock(side_effect=RuntimeError(secret_marker)),
            )

            for method, path, req_kwargs in _ELEVEN_OPERATIONAL_ROUTES:
                err_resp = client.request(
                    method,
                    path,
                    headers=auth_headers,
                    **req_kwargs,
                )
                assert err_resp.status_code == 500, f"Expected 500 on {method} {path}"
                assert secret_marker not in err_resp.text

        # 4. Wildcard ALLOWED_ORIGINS="*" is ignored and logged; explicit origins work
        monkeypatch.setenv("ALLOWED_ORIGINS", "*")
        with caplog.at_level("WARNING"):
            star_app = gen_ai_main.create_app()
        assert any("*" in rec.message for rec in caplog.records)

        with TestClient(star_app) as star_client:
            star_preflight = star_client.options(
                "/conversational-search",
                headers={
                    "Origin": "https://evil.example.com",
                    "Access-Control-Request-Method": "POST",
                },
            )
            assert star_preflight.headers.get("access-control-allow-origin") is None
            assert star_preflight.headers.get("access-control-allow-credentials") is None

        monkeypatch.setenv("ALLOWED_ORIGINS", "*, https://ui.example.com")
        explicit_app = gen_ai_main.create_app()
        with TestClient(explicit_app) as explicit_client:
            evil_preflight = explicit_client.options(
                "/conversational-search",
                headers={
                    "Origin": "https://evil.example.com",
                    "Access-Control-Request-Method": "POST",
                },
            )
            assert evil_preflight.headers.get("access-control-allow-origin") is None

            allowed_preflight = explicit_client.options(
                "/conversational-search",
                headers={
                    "Origin": "https://ui.example.com",
                    "Access-Control-Request-Method": "POST",
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
    finally:
        app.dependency_overrides.clear()


def test_downstream_sdk_error_dicts_are_sanitized_without_breaking_expected_feedback(
    monkeypatch: pytest.MonkeyPatch,
):
    """Real downstream services catch SDK Exceptions into dicts; main.py must scrub str(e)."""
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("GEN_AI_ADMIN_TOKEN", "secret")
    monkeypatch.setenv("PROJECT_ID", "real-svc-proj")
    monkeypatch.setenv("GENAI_LOCATION", "global")
    auth_headers = {"X-Admin-Token": "secret"}

    secret_marker = (
        "https://aiplatform.googleapis.com/v1/projects/secret-proj?key=supersecret "
        "vpc-sc-perimeter-token-999"
    )

    mock_genai_client = MagicMock()
    mock_genai_client.models.generate_content.side_effect = RuntimeError(secret_marker)

    mock_bq_client = MagicMock()
    mock_bq_client.query.side_effect = RuntimeError(secret_marker)

    monkeypatch.setattr("google.genai.Client", lambda **kwargs: mock_genai_client)
    monkeypatch.setattr("google.cloud.bigquery.Client", lambda **kwargs: mock_bq_client)
    gen_ai_main.clear_service_cache()

    app = gen_ai_main.create_app()
    try:
        with TestClient(app) as client:
            # 1. POST /enrichment: downstream SDK exception must not echo str(e)
            enrichment_resp = client.post(
                "/enrichment",
                headers=auth_headers,
                json={"product_id": "p1", "product_data": {"title": "Shoe"}},
            )
            assert "secret-proj" not in enrichment_resp.text
            assert "vpc-sc-perimeter-token-999" not in enrichment_resp.text
            assert "https://aiplatform.googleapis.com" not in enrichment_resp.text

            # 2. POST /marketing: must NOT return 200 with fake content starting with
            #    "Error generating marketing content: <str(e)>"
            marketing_resp = client.post(
                "/marketing",
                headers=auth_headers,
                json={
                    "product_id": "p1",
                    "product_data": {"title": "Shoe"},
                    "content_type": "product_description",
                },
            )
            assert marketing_resp.status_code == 500
            assert (
                marketing_resp.json()["detail"]
                == "Internal server error while generating marketing content"
            )
            assert "secret-proj" not in marketing_resp.text
            assert "vpc-sc-perimeter-token-999" not in marketing_resp.text

            # 3a. POST /generate-enhanced-image: invalid 1-char base64 preserves safe client error
            bad_b64_resp = client.post(
                "/generate-enhanced-image",
                headers=auth_headers,
                json={
                    "product_id": "p1",
                    "product_data": {"title": "Shoe"},
                    "image_base64": "A",
                    "background_prompt": "studio",
                },
            )
            assert bad_b64_resp.status_code == 200
            assert bad_b64_resp.json()["error"] == "Invalid base64 image format"

            # 3b. POST /generate-enhanced-image: Gemini SDK error scrubs str(e)
            img_err_resp = client.post(
                "/generate-enhanced-image",
                headers=auth_headers,
                json={
                    "product_id": "p1",
                    "product_data": {"title": "Shoe"},
                    "image_base64": "YWJj",
                    "background_prompt": "studio",
                },
            )
            assert "secret-proj" not in img_err_resp.text
            assert "vpc-sc-perimeter-token-999" not in img_err_resp.text
            assert img_err_resp.json().get("error") == "Error generating enhanced image"

            # 4a. POST /sql/simple-fix: GenAIClient SDK exception scrubs "Failed to generate content: <str(e)>"
            simple_fix_err = client.post(
                "/sql/simple-fix",
                headers=auth_headers,
                json={"sql_script": "SELECT 1", "error_message": "Syntax error"},
            )
            assert simple_fix_err.status_code == 200
            assert simple_fix_err.json()["suggested_fixed_sql"] is None
            assert "secret-proj" not in simple_fix_err.text
            assert "vpc-sc-perimeter-token-999" not in simple_fix_err.text
            assert "Failed to generate content:" not in (
                simple_fix_err.json().get("error") or ""
            )

            # 5a. POST /sql/fix: GenAIClient SDK exception scrubs wrapped str(e)
            fix_err = client.post(
                "/sql/fix",
                headers=auth_headers,
                json={
                    "original_sql": "SELECT bad FROM tbl",
                    "current_sql": "SELECT bad FROM tbl",
                    "error_message": "Unrecognized name: bad",
                },
            )
            assert fix_err.status_code == 200
            assert fix_err.json()["success"] is False
            assert "secret-proj" not in fix_err.text
            assert "vpc-sc-perimeter-token-999" not in fix_err.text
            assert "Failed to generate content:" not in (
                fix_err.json().get("error") or ""
            )

            # 6. POST /sql/analyze: GenAIClient SDK exception scrubs str(e) while keeping diff_text
            analyze_err = client.post(
                "/sql/analyze",
                headers=auth_headers,
                json={
                    "original_sql": "SELECT bad FROM tbl",
                    "fixed_sql": "SELECT NULL AS bad FROM tbl",
                },
            )
            assert analyze_err.status_code == 200
            assert "diff_text" in analyze_err.json()
            assert "secret-proj" not in analyze_err.text
            assert "vpc-sc-perimeter-token-999" not in analyze_err.text
            assert "Failed to generate content:" not in (
                analyze_err.json().get("error") or ""
            )

            # 4b & 5b. Model-level MAX_TOKENS truncation feedback is preserved on /sql/simple-fix and /sql/fix
            truncated_candidate = MagicMock()
            truncated_candidate.finish_reason = FinishReason.MAX_TOKENS
            truncated_candidate.finish_message = None
            truncated_response = MagicMock()
            truncated_response.candidates = [truncated_candidate]
            mock_genai_client.models.generate_content.side_effect = None
            mock_genai_client.models.generate_content.return_value = truncated_response

            simple_trunc = client.post(
                "/sql/simple-fix",
                headers=auth_headers,
                json={"sql_script": "SELECT 1", "error_message": "Syntax error"},
            )
            assert simple_trunc.status_code == 200
            assert (
                simple_trunc.json()["error"]
                == "Output truncated due to MAX_TOKENS limit."
            )

            fix_trunc = client.post(
                "/sql/fix",
                headers=auth_headers,
                json={
                    "original_sql": "SELECT bad FROM tbl",
                    "current_sql": "SELECT bad FROM tbl",
                    "error_message": "Unrecognized name: bad",
                },
            )
            assert fix_trunc.status_code == 200
            assert "Output truncated due to MAX_TOKENS limit." in (
                fix_trunc.json()["error"] or ""
            )

            # 7a. POST /sql/validate: non-BadRequest exception in SQLValidator returns sanitized 500
            val_unexpected = client.post(
                "/sql/validate",
                headers=auth_headers,
                json={"sql_script": "SELECT 1"},
            )
            assert val_unexpected.status_code == 500
            assert (
                val_unexpected.json()["detail"]
                == "Internal server error while validating SQL"
            )
            assert "secret-proj" not in val_unexpected.text
            assert "vpc-sc-perimeter-token-999" not in val_unexpected.text
            assert "RuntimeError" not in val_unexpected.text

            # 7b. POST /sql/validate: BadRequest SQL feedback is preserved with REST URLs scrubbed
            mock_bq_client.query.side_effect = BadRequest(
                "POST https://bigquery.googleapis.com/bigquery/v2/projects/secret-proj/jobs?prettyPrint=false: "
                "Unrecognized name: colorFamily [at 1:15]"
            )
            val_bad_req = client.post(
                "/sql/validate",
                headers=auth_headers,
                json={"sql_script": "SELECT colorFamily FROM tbl"},
            )
            assert val_bad_req.status_code == 200
            val_body = val_bad_req.json()
            assert val_body["valid"] is False
            assert "Unrecognized name: colorFamily [at 1:15]" in val_body["error_message"]
            assert val_body["details"]["unrecognized_name"] == "colorFamily"
            assert "https://bigquery.googleapis.com" not in val_bad_req.text
            assert "secret-proj" not in val_bad_req.text
    finally:
        gen_ai_main.clear_service_cache()


def test_thread_local_auth_request_and_locked_service_cache_concurrency(
    monkeypatch: pytest.MonkeyPatch,
):
    """Verify per-thread google_requests.Request isolation and locked singleton initialization."""
    # 1. Per-thread Request transport isolation across 4 concurrent threads
    barrier = threading.Barrier(4)
    seen_lock = threading.Lock()
    per_thread_requests: Dict[int, Tuple[Any, Any]] = {}

    def fake_verify_oauth2(token: str, request_obj: Any, audience: str = None):
        return {"request_obj": request_obj, "token": token, "audience": audience}

    monkeypatch.setattr(auth.id_token, "verify_oauth2_token", fake_verify_oauth2)

    def _worker_verify(idx: int) -> None:
        barrier.wait(timeout=5)
        first = auth.verify_bearer_token(f"t-{idx}-1", "https://gen-ai.run.app")[
            "request_obj"
        ]
        second = auth.verify_bearer_token(f"t-{idx}-2", "https://gen-ai.run.app")[
            "request_obj"
        ]
        with seen_lock:
            per_thread_requests[idx] = (first, second)

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(_worker_verify, i) for i in range(4)]
        for fut in futures:
            fut.result(timeout=5)

    assert len(per_thread_requests) == 4
    for first, second in per_thread_requests.values():
        assert first is second
    distinct_ids = {id(pair[0]) for pair in per_thread_requests.values()}
    assert len(distinct_ids) == 4, (
        "Expected each worker thread to hold its own google_requests.Request instance"
    )

    # 2. Double-checked locking around _service_cache under 8-thread cold-start contention
    assert hasattr(gen_ai_main, "_service_cache_lock")
    assert hasattr(gen_ai_main, "clear_service_cache")

    monkeypatch.setenv("PROJECT_ID", "concurrent-proj")
    monkeypatch.setenv("GENAI_LOCATION", "global")
    gen_ai_main.clear_service_cache()

    init_count = 0
    init_count_lock = threading.Lock()
    start_barrier = threading.Barrier(8)

    class _SlowService:
        def __init__(self, project_id: str, location: str):
            nonlocal init_count
            time.sleep(0.05)
            with init_count_lock:
                init_count += 1
            self.project_id = project_id
            self.location = location

    monkeypatch.setattr(gen_ai_main, "ConversationalSearchService", _SlowService)

    def _worker_get_service() -> Any:
        start_barrier.wait(timeout=5)
        return gen_ai_main.get_conversational_search_service()

    try:
        with ThreadPoolExecutor(max_workers=8) as pool:
            instances = [
                fut.result(timeout=5)
                for fut in [pool.submit(_worker_get_service) for _ in range(8)]
            ]
        assert init_count == 1
        assert len({id(inst) for inst in instances}) == 1
    finally:
        gen_ai_main.clear_service_cache()


def test_ctas_dry_run_falls_back_to_inner_select_under_readonly_gen_ai_sa(
    monkeypatch: pytest.MonkeyPatch,
):
    """CTAS dry run under read-only gen-ai-sa falls back to inner SELECT/WITH safely."""
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("GEN_AI_ADMIN_TOKEN", "secret")
    monkeypatch.setenv("PROJECT_ID", "readonly-proj")
    monkeypatch.setenv("GENAI_LOCATION", "global")
    auth_headers = {"X-Admin-Token": "secret"}

    mock_genai_client = MagicMock()
    mock_bq_client = MagicMock()
    monkeypatch.setattr("google.genai.Client", lambda **kwargs: mock_genai_client)
    monkeypatch.setattr("google.cloud.bigquery.Client", lambda **kwargs: mock_bq_client)
    gen_ai_main.clear_service_cache()

    ok_job = MagicMock(
        total_bytes_processed=2048,
        job_id="dry-run-inner-1",
        location="US",
    )

    try:
        validator = SQLValidator(project_id="readonly-proj")

        # 1a. Outer CTAS raises Forbidden (bigquery.tables.create) -> inner SELECT dry run succeeds
        ctas_with_options = (
            "-- header comment with ; and AS SELECT 999\n"
            "CREATE OR REPLACE TABLE `readonly-proj.psearch.products` "
            "PARTITION BY DATE(CAST(created_at AS TIMESTAMP)) "
            "CLUSTER BY id "
            'OPTIONS(description="AS SELECT 1; DROP TABLE x") '
            "AS SELECT id, 'a;b' AS tag FROM `readonly-proj.psearch_raw.catalog`;"
        )
        seen_queries = []

        def _ctas_forbidden_then_ok(sql: str, job_config: Any = None, timeout: Any = None):
            seen_queries.append(sql)
            if sql.lstrip().startswith(("--", "CREATE")):
                raise Forbidden(
                    "POST https://bigquery.googleapis.com/bigquery/v2/projects/readonly-proj/jobs: "
                    "Access Denied: Dataset readonly-proj:psearch: User does not have "
                    "bigquery.tables.create permission in dataset readonly-proj:psearch."
                )
            return ok_job

        mock_bq_client.query.side_effect = _ctas_forbidden_then_ok
        res_forbidden = validator.validate_sql_dry_run(ctas_with_options)
        assert res_forbidden["valid"] is True
        assert res_forbidden["details"]["estimated_bytes_processed"] == 2048
        assert len(seen_queries) == 2
        assert (
            seen_queries[1]
            == "SELECT id, 'a;b' AS tag FROM `readonly-proj.psearch_raw.catalog`"
        )

        # 1b. Outer CTAS with destination dataset NotFound + CTE WITH body -> inner WITH succeeds
        ctas_temp_with = (
            "CREATE TEMP TABLE IF NOT EXISTS tmp_items AS "
            "WITH base AS (SELECT id FROM `readonly-proj.psearch_raw.catalog`) "
            "SELECT id FROM base ;  -- trailing comment\n"
        )
        seen_queries.clear()

        def _ctas_not_found_then_ok(sql: str, job_config: Any = None, timeout: Any = None):
            seen_queries.append(sql)
            if sql.lstrip().upper().startswith("CREATE"):
                raise NotFound(
                    "GET https://bigquery.googleapis.com/bigquery/v2/projects/readonly-proj/datasets/dest_ds: "
                    "Not found: Dataset readonly-proj:dest_ds was not found in location US"
                )
            return ok_job

        mock_bq_client.query.side_effect = _ctas_not_found_then_ok
        res_not_found = validator.validate_sql_dry_run(ctas_temp_with)
        assert res_not_found["valid"] is True
        assert len(seen_queries) == 2
        assert seen_queries[1] == (
            "WITH base AS (SELECT id FROM `readonly-proj.psearch_raw.catalog`) "
            "SELECT id FROM base"
        )

        # 2. Outer CTAS raises Forbidden, inner SELECT raises BadRequest("Invalid field reference 'foo'")
        #    -> returns valid: False with details["missing_field"] == "foo" and REST URLs scrubbed
        def _ctas_forbidden_then_bad_col(sql: str, job_config: Any = None, timeout: Any = None):
            if sql.lstrip().upper().startswith("CREATE"):
                raise Forbidden("Access Denied: bigquery.tables.create")
            raise BadRequest(
                "POST https://bigquery.googleapis.com/bigquery/v2/projects/secret-proj/jobs: "
                "Invalid field reference 'foo' [at 1:8]"
            )

        mock_bq_client.query.side_effect = _ctas_forbidden_then_bad_col
        res_bad_col = validator.validate_sql_dry_run(
            "CREATE OR REPLACE TABLE `readonly-proj.psearch.products` AS "
            "SELECT foo FROM `readonly-proj.psearch_raw.catalog`"
        )
        assert res_bad_col["valid"] is False
        assert res_bad_col["details"]["missing_field"] == "foo"
        assert "https://bigquery.googleapis.com" not in (res_bad_col.get("error_message") or "")

        # 3. Multi-statement SQL or non-CTAS SQL is NEVER extracted; OPTIONS(description="AS SELECT 1") IS extracted cleanly
        for rejected_sql in (
            "CREATE OR REPLACE TABLE t AS SELECT 1; DROP TABLE x",
            "CREATE OR REPLACE TABLE t AS SELECT 1; DROP TABLE x;",
            "CREATE TABLE t (id INT64); SELECT 1 AS id",
            "CREATE OR REPLACE TABLE t AS DROP TABLE x",
            "SELECT * FROM `readonly-proj.psearch_raw.missing_tbl`",
        ):
            seen_queries.clear()
            mock_bq_client.query.side_effect = lambda sql, **kw: (
                seen_queries.append(sql),
                (_ for _ in ()).throw(
                    Forbidden(
                        "POST https://bigquery.googleapis.com/bigquery/v2/projects/secret-proj/jobs: "
                        "Access Denied: Table secret-proj:raw.tbl"
                    )
                ),
            )[1]
            res_rejected = validator.validate_sql_dry_run(rejected_sql)
            assert res_rejected["valid"] is False
            assert len(seen_queries) == 1, f"Must not retry inner query for: {rejected_sql}"
            assert res_rejected["details"] == {"reason": "access_denied"}
            assert "error_type" not in res_rejected["details"]
            assert "https://bigquery.googleapis.com" not in res_rejected["error"]
            assert "https://bigquery.googleapis.com" not in res_rejected["error_message"]

        seen_queries.clear()

        def _options_as_check(sql: str, **kw: Any) -> Any:
            seen_queries.append(sql)
            if len(seen_queries) == 1:
                raise Forbidden("Access Denied: bigquery.tables.create")
            return ok_job

        mock_bq_client.query.side_effect = _options_as_check
        res_opts = validator.validate_sql_dry_run(
            'CREATE OR REPLACE TABLE t OPTIONS(description="AS SELECT 1") AS SELECT id FROM src'
        )
        assert res_opts["valid"] is True
        assert seen_queries == [
            'CREATE OR REPLACE TABLE t OPTIONS(description="AS SELECT 1") AS SELECT id FROM src',
            "SELECT id FROM src",
        ]

        # 4. POST /sql/validate HTTP integration:
        #    - CTAS Forbidden -> 200 valid: True
        #    - Inner SELECT NotFound / Forbidden on source table -> 200 valid: False (NOT 500!) with URLs scrubbed
        app = gen_ai_main.create_app()
        with TestClient(app) as client:
            mock_bq_client.query.side_effect = _ctas_forbidden_then_ok
            http_ctas_ok = client.post(
                "/sql/validate",
                headers=auth_headers,
                json={"sql_script": ctas_with_options},
            )
            assert http_ctas_ok.status_code == 200
            assert http_ctas_ok.json()["valid"] is True

            def _ctas_inner_source_not_found(sql: str, **kw: Any) -> Any:
                if sql.lstrip().upper().startswith("CREATE"):
                    raise Forbidden("Access Denied: bigquery.tables.create")
                raise NotFound(
                    "GET https://bigquery.googleapis.com/bigquery/v2/projects/secret-proj/tables/src: "
                    "Not found: Table readonly-proj:psearch_raw.missing_src was not found in location US"
                )

            mock_bq_client.query.side_effect = _ctas_inner_source_not_found
            http_src_nf = client.post(
                "/sql/validate",
                headers=auth_headers,
                json={
                    "sql_script": (
                        "CREATE OR REPLACE TABLE `readonly-proj.psearch.products` AS "
                        "SELECT id FROM `readonly-proj.psearch_raw.missing_src`"
                    )
                },
            )
            assert http_src_nf.status_code == 200
            nf_body = http_src_nf.json()
            assert nf_body["valid"] is False
            assert nf_body["details"] == {"reason": "not_found"}
            assert "https://bigquery.googleapis.com" not in http_src_nf.text
            assert "secret-proj" not in http_src_nf.text
            assert "missing_src" in nf_body["error"]
            assert nf_body["error"] == nf_body["error_message"]

            def _ctas_inner_source_forbidden(sql: str, **kw: Any) -> Any:
                if sql.lstrip().upper().startswith("CREATE"):
                    raise Forbidden("Access Denied: bigquery.tables.create")
                raise Forbidden(
                    "POST https://bigquery.googleapis.com/bigquery/v2/projects/secret-proj/jobs: "
                    "Access Denied: Table readonly-proj:restricted.src: User does not have permission."
                )

            mock_bq_client.query.side_effect = _ctas_inner_source_forbidden
            http_src_forb = client.post(
                "/sql/validate",
                headers=auth_headers,
                json={
                    "sql_script": (
                        "CREATE OR REPLACE TABLE `readonly-proj.psearch.products` AS "
                        "SELECT id FROM `readonly-proj.restricted.src`"
                    )
                },
            )
            assert http_src_forb.status_code == 200
            forb_body = http_src_forb.json()
            assert forb_body["valid"] is False
            assert forb_body["details"] == {"reason": "access_denied"}
            assert "https://bigquery.googleapis.com" not in http_src_forb.text
            assert "secret-proj" not in http_src_forb.text

        # 5. TransformationPipeline end-to-end: CTAS raising Forbidden on destination table
        #    succeeds on attempt 1 without invoking SQLFixer!
        pipeline = TransformationPipeline(project_id="readonly-proj", location="global")
        generated_ctas = (
            "CREATE OR REPLACE TABLE `readonly-proj.psearch.products` AS "
            "SELECT id, title AS name FROM `readonly-proj.psearch_raw.catalog`"
        )
        pipeline.initial_generator.generate = MagicMock(
            return_value=(generated_ctas, None)
        )
        pipeline.field_analyzer.identify_defaulted_fields = MagicMock(return_value=[])
        pipeline.sql_fixer.fix_sql = MagicMock()
        mock_bq_client.query.side_effect = _ctas_forbidden_then_ok

        pipeline_task_id = str(uuid.uuid4())
        task_manager.init_task(pipeline_task_id, task_type="sql_generation")
        pipeline.execute_pipeline(
            task_id=pipeline_task_id,
            source_table_name="readonly-proj.psearch_raw.catalog",
            destination_table_name="readonly-proj.psearch.products",
            source_schema_fields=["id", "title"],
            destination_schema={
                "fields": [
                    {"name": "id", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "name", "type": "STRING", "mode": "REQUIRED"},
                ]
            },
            source_data_sample_json='[{"id": "1", "title": "Runner"}]',
        )
        final_status = task_manager.get_task_status(pipeline_task_id)
        assert final_status is not None
        assert final_status["status"] == "completed"
        assert final_status["result"] == generated_ctas
        pipeline.sql_fixer.fix_sql.assert_not_called()
    finally:
        gen_ai_main.clear_service_cache()


def test_allowed_origins_normalizes_case_and_trailing_slash_and_skips_malformed(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    """ALLOWED_ORIGINS normalizes scheme/host case & trailing '/' and rejects malformed origins."""
    monkeypatch.setenv(
        "ALLOWED_ORIGINS",
        "HTTPS://Shop.Example.com/, https://ui.example.com/, https://shop.example.com, "
        "*, https://*.example.com, ftp://bad.example.com, https://bad.example.com/subpath, "
        "https://user:pw@bad.example.com, https://bad.example.com?q=1, https://bad.example.com#frag",
    )
    with caplog.at_level("WARNING"):
        cors_app = gen_ai_main.create_app()
    assert len(caplog.records) >= 5

    with TestClient(cors_app) as client:
        for allowed_origin in ("https://shop.example.com", "https://ui.example.com"):
            resp = client.options(
                "/conversational-search",
                headers={
                    "Origin": allowed_origin,
                    "Access-Control-Request-Method": "POST",
                },
            )
            assert resp.headers.get("access-control-allow-origin") == allowed_origin
            assert resp.headers.get("access-control-allow-credentials") == "true"

        for blocked_origin in ("https://bad.example.com", "https://evil.example.com"):
            bad_resp = client.options(
                "/conversational-search",
                headers={
                    "Origin": blocked_origin,
                    "Access-Control-Request-Method": "POST",
                },
            )
            assert bad_resp.status_code == 400
            assert bad_resp.headers.get("access-control-allow-origin") is None

    # All-invalid ALLOWED_ORIGINS produces empty allowlist and disables credentials
    monkeypatch.setenv("ALLOWED_ORIGINS", "*, ftp://bad.example.com, https://bad.example.com/path")
    empty_cors_app = gen_ai_main.create_app()
    with TestClient(empty_cors_app) as empty_client:
        none_resp = empty_client.options(
            "/conversational-search",
            headers={
                "Origin": "https://bad.example.com",
                "Access-Control-Request-Method": "POST",
            },
        )
        assert none_resp.headers.get("access-control-allow-origin") is None
        assert none_resp.headers.get("access-control-allow-credentials") is None


_SAMPLE_PNG_BYTES = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
_SAMPLE_WEBP_BYTES = b"RIFF\x1a\x00\x00\x00WEBPVP8 \x0e\x00\x00\x00\x30\x01\x00\x9d\x01\x2a"


class _FakePinnedStreamResponse:
    """Minimal context-manager HTTP response double for pinned-session integration tests."""

    def __init__(
        self,
        status_code: int,
        headers: Dict[str, str],
        body: bytes,
    ) -> None:
        self.status_code = status_code
        self.headers = headers
        self._body = body

    def iter_content(self, chunk_size: int = 65536) -> Iterable[bytes]:
        for offset in range(0, len(self._body), chunk_size):
            yield self._body[offset : offset + chunk_size]

    def close(self) -> None:
        return None

    def __enter__(self) -> "_FakePinnedStreamResponse":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        return False


class _FakePinnedSession:
    """Minimal session double recording pinned HTTPS fetches without touching real sockets."""

    def __init__(
        self,
        hostname: str,
        validated_ips: List[str],
        route_table: Dict[str, Tuple[int, Dict[str, str], bytes]],
        recorded_calls: List[Tuple[str, Tuple[str, ...], str]],
    ) -> None:
        self._hostname = hostname
        self._validated_ips = tuple(validated_ips)
        self._route_table = route_table
        self._recorded_calls = recorded_calls

    def get(
        self,
        url: str,
        *,
        stream: bool = False,
        allow_redirects: bool = True,
        timeout: Any = None,
    ) -> _FakePinnedStreamResponse:
        assert stream is True
        assert allow_redirects is False
        assert timeout is not None
        self._recorded_calls.append((self._hostname, self._validated_ips, url))
        status_code, headers, body = self._route_table[url]
        return _FakePinnedStreamResponse(
            status_code=status_code,
            headers=headers,
            body=body,
        )

    def close(self) -> None:
        return None


def test_enrichment_and_marketing_services_block_ssrf_and_attach_validated_raster_images(
    monkeypatch: pytest.MonkeyPatch,
):
    """EnrichmentService and MarketingService block SSRF/non-image URLs and attach validated PNG/WebP parts."""
    for var in (
        "GEN_AI_MAX_IMAGE_BYTES",
        "GEN_AI_IMAGE_FETCH_TIMEOUT_SECONDS",
        "GEN_AI_MAX_IMAGE_REDIRECTS",
        "GEN_AI_ALLOWED_IMAGE_HOSTS",
    ):
        monkeypatch.delenv(var, raising=False)

    def _forbid_unvalidated_requests_get(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("Unvalidated requests.get must never be called")

    monkeypatch.setattr(requests, "get", _forbid_unvalidated_requests_get)
    assert not hasattr(enrichment_service_mod, "requests")
    assert not hasattr(marketing_service_mod, "requests")

    dns_map: Dict[str, str] = {
        "evil.example.com": "10.0.0.5",
        "json.example.com": "93.184.216.34",
        "spoof.example.com": "93.184.216.34",
        "cdn.example.com": "93.184.216.34",
        "storage.googleapis.com": "142.250.80.16",
    }

    def _fake_getaddrinfo(host: str, port: int, type: int = 0) -> List[Any]:
        ip = dns_map.get(host)
        if ip is None:
            raise socket.gaierror(f"Unexpected DNS lookup for {host}")
        return [(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, port))]

    ssrf_json_secret = b'{"secret": "metadata-token-12345"}'
    route_table: Dict[str, Tuple[int, Dict[str, str], bytes]] = {
        "https://json.example.com/secret.jpg": (
            200,
            {"Content-Type": "application/json"},
            ssrf_json_secret,
        ),
        "https://spoof.example.com/secret.png": (
            200,
            {"Content-Type": "image/png"},
            ssrf_json_secret,
        ),
        "https://cdn.example.com/catalog/item-42": (
            200,
            {"Content-Type": "image/webp"},
            _SAMPLE_WEBP_BYTES,
        ),
        "https://storage.googleapis.com/psearch-catalog-bucket/products/shoe%2B1.png": (
            200,
            {"Content-Type": "image/png"},
            _SAMPLE_PNG_BYTES,
        ),
    }
    recorded_fetches: List[Tuple[str, Tuple[str, ...], str]] = []

    monkeypatch.setattr(image_fetcher.socket, "getaddrinfo", _fake_getaddrinfo)
    monkeypatch.setattr(
        image_fetcher,
        "_create_pinned_https_session",
        lambda hostname, validated_ips: _FakePinnedSession(
            hostname,
            validated_ips,
            route_table,
            recorded_fetches,
        ),
    )

    enrichment_captured_contents: List[Any] = []
    marketing_captured_contents: List[Any] = []

    enrichment_genai_client = MagicMock()
    enrichment_genai_resp = MagicMock()
    enrichment_genai_resp.text = json.dumps({"description": "Enriched copy"})

    def _capture_enrichment_call(*, model: str, contents: Any, config: Any) -> Any:
        enrichment_captured_contents.append(contents)
        return enrichment_genai_resp

    enrichment_genai_client.models.generate_content.side_effect = _capture_enrichment_call

    marketing_genai_client = MagicMock()
    marketing_genai_resp = MagicMock()
    marketing_genai_resp.text = json.dumps({"content": "Marketing copy"})

    def _capture_marketing_call(*, model: str, contents: Any, config: Any) -> Any:
        marketing_captured_contents.append(contents)
        return marketing_genai_resp

    marketing_genai_client.models.generate_content.side_effect = _capture_marketing_call

    clients = iter([enrichment_genai_client, marketing_genai_client])
    monkeypatch.setattr("google.genai.Client", lambda **kwargs: next(clients))

    enrichment_svc = EnrichmentService("proj-ssrf", "global")
    marketing_svc = MarketingService("proj-ssrf", "global")

    blocked_urls = [
        "http://169.254.169.254/computeMetadata/v1/",
        "https://metadata.google.internal/computeMetadata/v1/",
        "https://evil.example.com/secret.jpg",
        "https://json.example.com/secret.jpg",
        "https://spoof.example.com/secret.png",
        "https://[::1/a.png",
        "https://example.com]/a.png",
        "https://[not_ipv6]/a.png",
    ]
    for bad_url in blocked_urls:
        enrichment_captured_contents.clear()
        marketing_captured_contents.clear()

        assert enrichment_svc._fetch_product_image(bad_url) is None
        assert enrichment_svc._get_image_bytes_from_url(bad_url) is None
        assert marketing_svc._fetch_product_image(bad_url) is None
        assert marketing_svc._get_image_bytes_from_url(bad_url) is None

        enrich_res = enrichment_svc.process_enrichment(
            product_id="p-ssrf",
            product_data={"title": "Sneaker", "images": [{"uri": bad_url}]},
            fields_to_enrich=["description"],
        )
        assert enrich_res["enriched_fields"]["description"] == "Enriched copy"
        assert len(enrichment_captured_contents) == 1
        enrich_parts = enrichment_captured_contents[0][0].parts
        assert len(enrich_parts) == 1, f"Unexpected image part attached for {bad_url}"
        assert "metadata-token-12345" not in (enrich_parts[0].text or "")

        mkt_res = marketing_svc.generate_content(
            product_id="p-ssrf",
            product_data={"title": "Sneaker", "images": [{"uri": bad_url}]},
            content_type="product_description",
        )
        assert mkt_res["content"] == "Marketing copy"
        assert len(marketing_captured_contents) == 1
        mkt_parts = marketing_captured_contents[0][0].parts
        assert len(mkt_parts) == 1, f"Unexpected image part attached for {bad_url}"
        assert "metadata-token-12345" not in (mkt_parts[0].text or "")

    # Malformed bracketed IPv6 URL is also safely guarded inside _build_prompt without crashing
    enrichment_captured_contents.clear()
    marketing_captured_contents.clear()
    assert (
        enrichment_svc.process_enrichment(
            product_id="p-ipv6-bad",
            product_data={
                "title": "Sneaker",
                "images": [{"uri": "https://[::1/secret.png"}],
            },
            fields_to_enrich=["description"],
        )["enriched_fields"]["description"]
        == "Enriched copy"
    )
    assert len(enrichment_captured_contents[0][0].parts) == 1
    assert (
        marketing_svc.generate_content(
            product_id="p-ipv6-bad",
            product_data={
                "title": "Sneaker",
                "images": [{"uri": "https://[::1/secret.png"}],
            },
            content_type="product_description",
        )["content"]
        == "Marketing copy"
    )
    assert len(marketing_captured_contents[0][0].parts) == 1

    # Verify private IP host (evil.example.com -> 10.0.0.5) and metadata hosts never opened a session
    fetched_hosts = {host for host, _, _ in recorded_fetches}
    assert "evil.example.com" not in fetched_hosts
    assert "metadata.google.internal" not in fetched_hosts
    assert "169.254.169.254" not in fetched_hosts

    # Valid public HTTPS WebP (no .webp extension in URL) and gs:// PNG attach canonical MIME parts
    valid_cases = [
        ("https://cdn.example.com/catalog/item-42", _SAMPLE_WEBP_BYTES, "image/webp"),
        (
            "gs://psearch-catalog-bucket/products/shoe+1.png",
            _SAMPLE_PNG_BYTES,
            "image/png",
        ),
    ]
    for good_url, expected_bytes, expected_mime in valid_cases:
        assert enrichment_svc._fetch_product_image(good_url) == (
            expected_bytes,
            expected_mime,
        )
        assert enrichment_svc._get_image_bytes_from_url(good_url) == expected_bytes
        assert marketing_svc._fetch_product_image(good_url) == (
            expected_bytes,
            expected_mime,
        )
        assert marketing_svc._get_image_bytes_from_url(good_url) == expected_bytes

        enrichment_captured_contents.clear()
        marketing_captured_contents.clear()

        enrichment_svc.process_enrichment(
            product_id="p-ok",
            product_data={"title": "Sneaker", "images": [{"uri": good_url}]},
            fields_to_enrich=["description"],
        )
        enrich_parts = enrichment_captured_contents[0][0].parts
        assert len(enrich_parts) == 2
        assert enrich_parts[1].inline_data.data == expected_bytes
        assert enrich_parts[1].inline_data.mime_type == expected_mime

        marketing_svc.generate_content(
            product_id="p-ok",
            product_data={"title": "Sneaker", "images": [{"uri": good_url}]},
            content_type="product_description",
        )
        mkt_parts = marketing_captured_contents[0][0].parts
        assert len(mkt_parts) == 2
        assert mkt_parts[1].inline_data.data == expected_bytes
        assert mkt_parts[1].inline_data.mime_type == expected_mime


def test_enrichment_and_marketing_routes_handle_malformed_images_cleanly(
    monkeypatch: pytest.MonkeyPatch,
):
    """POST /enrichment and POST /marketing return HTTP 200 on malformed product_data['images'] shapes."""
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("GEN_AI_ADMIN_TOKEN", "secret")
    monkeypatch.setenv("PROJECT_ID", "malformed-images-proj")
    monkeypatch.setenv("GENAI_LOCATION", "global")
    auth_headers = {"X-Admin-Token": "secret"}

    captured_calls: List[Any] = []
    mock_genai_client = MagicMock()

    def _fake_generate_content(*, model: str, contents: Any, config: Any) -> Any:
        captured_calls.append(contents)
        resp = MagicMock()
        resp.text = json.dumps({"description": "Clean enrichment", "content": "Clean copy"})
        return resp

    mock_genai_client.models.generate_content.side_effect = _fake_generate_content
    monkeypatch.setattr("google.genai.Client", lambda **kwargs: mock_genai_client)
    gen_ai_main.clear_service_cache()

    malformed_images_values: List[Any] = [
        123,
        True,
        "https://cdn.example.com/shoe.png",
        {"uri": "https://cdn.example.com/shoe.png"},
        [None],
        ["https://cdn.example.com/shoe.png"],
        [{}],
        [{"uri": 123}],
        [{"uri": ""}],
    ]

    app = gen_ai_main.create_app()
    try:
        with TestClient(app) as client:
            for malformed_images in malformed_images_values:
                captured_calls.clear()
                enrich_resp = client.post(
                    "/enrichment",
                    headers=auth_headers,
                    json={
                        "product_id": "p-malformed",
                        "product_data": {
                            "title": "Running Shoe",
                            "images": malformed_images,
                        },
                        "fields_to_enrich": ["description"],
                    },
                )
                assert enrich_resp.status_code == 200, (
                    f"/enrichment failed on images={malformed_images!r}: {enrich_resp.text}"
                )
                assert enrich_resp.json() == {
                    "product_id": "p-malformed",
                    "enriched_fields": {"description": "Clean enrichment"},
                }
                assert len(captured_calls) == 1
                assert len(captured_calls[0][0].parts) == 1

                captured_calls.clear()
                mkt_resp = client.post(
                    "/marketing",
                    headers=auth_headers,
                    json={
                        "product_id": "p-malformed",
                        "product_data": {
                            "title": "Running Shoe",
                            "images": malformed_images,
                        },
                        "content_type": "product_description",
                    },
                )
                assert mkt_resp.status_code == 200, (
                    f"/marketing failed on images={malformed_images!r}: {mkt_resp.text}"
                )
                assert mkt_resp.json() == {
                    "product_id": "p-malformed",
                    "content": "Clean copy",
                }
                assert len(captured_calls) == 1
                assert len(captured_calls[0][0].parts) == 1
    finally:
        gen_ai_main.clear_service_cache()


def test_generate_enhanced_image_enforces_413_size_limits_and_preserves_base64_contracts(
    monkeypatch: pytest.MonkeyPatch,
):
    """POST /generate-enhanced-image enforces 413 pre-decode bounds while preserving 'A' and 'YWJj' contracts."""
    assert "Image exceeds maximum allowed size" in gen_ai_main._SAFE_IMAGE_ERRORS

    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("GEN_AI_ADMIN_TOKEN", "secret")
    monkeypatch.setenv("PROJECT_ID", "imagen-bounds-proj")
    monkeypatch.setenv("GENAI_LOCATION", "global")
    auth_headers = {"X-Admin-Token": "secret"}

    captured_imagen_contents: List[Any] = []
    mock_genai_client = MagicMock()

    def _fake_imagen_generate(*, model: str, contents: Any, config: Any) -> Any:
        captured_imagen_contents.append(contents)
        part = MagicMock()
        part.inline_data.mime_type = "image/png"
        part.inline_data.data = _SAMPLE_PNG_BYTES
        cand = MagicMock()
        cand.content.parts = [part]
        resp = MagicMock()
        resp.candidates = [cand]
        return resp

    mock_genai_client.models.generate_content.side_effect = _fake_imagen_generate
    monkeypatch.setattr("google.genai.Client", lambda **kwargs: mock_genai_client)
    gen_ai_main.clear_service_cache()

    app = gen_ai_main.create_app()
    try:
        with TestClient(app) as client:
            # 1. Oversized raw and data:image/png;base64,... payloads rejected at route pre-check with 413
            #    WITHOUT allocating decoded bytes or calling ImageGenerationService.generate_image.
            monkeypatch.setenv("GEN_AI_MAX_IMAGE_BYTES", "32")
            oversized_raw_b64 = base64.b64encode(b"X" * 64).decode("ascii")
            oversized_data_uri = f"data:image/png;base64,{oversized_raw_b64}"

            spy_b64decode = MagicMock(wraps=image_fetcher.base64.b64decode)
            monkeypatch.setattr(image_fetcher.base64, "b64decode", spy_b64decode)

            for payload in (oversized_raw_b64, oversized_data_uri):
                resp_413 = client.post(
                    "/generate-enhanced-image",
                    headers=auth_headers,
                    json={
                        "product_id": "p1",
                        "product_data": {"title": "Shoe"},
                        "image_base64": payload,
                        "background_prompt": "studio",
                    },
                )
                assert resp_413.status_code == 413
                assert resp_413.json() == {
                    "detail": "Image exceeds maximum allowed size"
                }

            spy_b64decode.assert_not_called()
            assert captured_imagen_contents == []

            # 2. Quantum-boundary payloads where len(stripped) == max_base64_chars_for_bytes(4) == 8
            #    but decoded bytes == 5 > 4 also return 413, while 4 decoded bytes succeed with 200.
            monkeypatch.setenv("GEN_AI_MAX_IMAGE_BYTES", "4")
            five_byte_b64 = base64.b64encode(b"12345").decode("ascii")
            assert len(five_byte_b64) == 8
            for boundary_payload in (
                five_byte_b64,
                f"data:image/png;base64,{five_byte_b64}",
            ):
                boundary_413 = client.post(
                    "/generate-enhanced-image",
                    headers=auth_headers,
                    json={
                        "product_id": "p1",
                        "product_data": {"title": "Shoe"},
                        "image_base64": boundary_payload,
                        "background_prompt": "studio",
                    },
                )
                assert boundary_413.status_code == 413
                assert boundary_413.json() == {
                    "detail": "Image exceeds maximum allowed size"
                }
            assert captured_imagen_contents == []

            four_byte_b64 = base64.b64encode(b"1234").decode("ascii")
            exact_ok = client.post(
                "/generate-enhanced-image",
                headers=auth_headers,
                json={
                    "product_id": "p1",
                    "product_data": {"title": "Shoe"},
                    "image_base64": four_byte_b64,
                    "background_prompt": "studio",
                },
            )
            assert exact_ok.status_code == 200
            assert "generated_image_base64" in exact_ok.json()

            # 3. Existing base64 contracts: "A" -> HTTP 200 Invalid base64 image format;
            #    "YWJj" -> HTTP 200 with fallback mime_type="image/png";
            #    valid WebP data URI -> HTTP 200 with detected mime_type="image/webp".
            monkeypatch.setenv("GEN_AI_MAX_IMAGE_BYTES", "128")
            bad_short = client.post(
                "/generate-enhanced-image",
                headers=auth_headers,
                json={
                    "product_id": "p1",
                    "product_data": {"title": "Shoe"},
                    "image_base64": "A",
                    "background_prompt": "studio",
                },
            )
            assert bad_short.status_code == 200
            assert bad_short.json() == {
                "product_id": "p1",
                "error": "Invalid base64 image format",
            }

            captured_imagen_contents.clear()
            synth_ok = client.post(
                "/generate-enhanced-image",
                headers=auth_headers,
                json={
                    "product_id": "p1",
                    "product_data": {"title": "Shoe"},
                    "image_base64": "YWJj",
                    "background_prompt": "studio",
                },
            )
            assert synth_ok.status_code == 200
            assert len(captured_imagen_contents) == 1
            assert captured_imagen_contents[0][0].inline_data.data == b"abc"
            assert captured_imagen_contents[0][0].inline_data.mime_type == "image/png"

            captured_imagen_contents.clear()
            webp_data_uri = "data:image/webp;base64," + base64.b64encode(
                _SAMPLE_WEBP_BYTES
            ).decode("ascii")
            webp_ok = client.post(
                "/generate-enhanced-image",
                headers=auth_headers,
                json={
                    "product_id": "p1",
                    "product_data": {"title": "Shoe"},
                    "image_base64": webp_data_uri,
                    "background_prompt": "studio",
                },
            )
            assert webp_ok.status_code == 200
            assert len(captured_imagen_contents) == 1
            assert (
                captured_imagen_contents[0][0].inline_data.data == _SAMPLE_WEBP_BYTES
            )
            assert (
                captured_imagen_contents[0][0].inline_data.mime_type == "image/webp"
            )

            # 4. Direct ImageGenerationService.generate_image returns safe error dict on ImageTooLargeError,
            #    and route maps service-returned "Image exceeds maximum allowed size" to HTTP 413.
            monkeypatch.setenv("GEN_AI_MAX_IMAGE_BYTES", "8")
            direct_svc = ImageGenerationService("imagen-bounds-proj", "global")
            direct_too_large = direct_svc.generate_image(
                product_id="p-direct",
                product_data={"title": "Shoe"},
                image_base64=base64.b64encode(b"0123456789ABCDEF").decode("ascii"),
                background_prompt="studio",
            )
            assert direct_too_large == {
                "product_id": "p-direct",
                "error": "Image exceeds maximum allowed size",
            }

            stub_svc = MagicMock()
            stub_svc.generate_image.return_value = {
                "product_id": "p1",
                "error": "Image exceeds maximum allowed size",
            }
            app.dependency_overrides[gen_ai_main.get_image_generation_service] = (
                lambda: stub_svc
            )
            try:
                svc_413 = client.post(
                    "/generate-enhanced-image",
                    headers=auth_headers,
                    json={
                        "product_id": "p1",
                        "product_data": {"title": "Shoe"},
                        "image_base64": "YWJj",
                        "background_prompt": "studio",
                    },
                )
                assert svc_413.status_code == 413
                assert svc_413.json() == {
                    "detail": "Image exceeds maximum allowed size"
                }
            finally:
                app.dependency_overrides.clear()

            # 5. Misconfigured GEN_AI_MAX_IMAGE_BYTES maps to HTTP 500 "Internal server configuration error"
            for bad_cfg in ("0", "-10", "not-an-int"):
                monkeypatch.setenv("GEN_AI_MAX_IMAGE_BYTES", bad_cfg)
                cfg_500 = client.post(
                    "/generate-enhanced-image",
                    headers=auth_headers,
                    json={
                        "product_id": "p1",
                        "product_data": {"title": "Shoe"},
                        "image_base64": "YWJj",
                        "background_prompt": "studio",
                    },
                )
                assert cfg_500.status_code == 500
                assert cfg_500.json() == {
                    "detail": "Internal server configuration error"
                }
    finally:
        gen_ai_main.clear_service_cache()


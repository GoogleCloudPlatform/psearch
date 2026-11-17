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

import os
from pathlib import Path
import subprocess
import sys
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

from fastapi import HTTPException
from google.auth.exceptions import GoogleAuthError
import pytest

try:
    from psearch.gen_ai import auth, main as gen_ai_main
    from psearch.gen_ai.services.conversational_search_service import (
        ConversationalSearchResponse,
    )
    from psearch.gen_ai.services.sql.common.input_validation import (
        MAX_GENERATED_SQL_CHARS,
    )
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
    from gen_ai.services.conversational_search_service import (
        ConversationalSearchResponse,
    )
    from gen_ai.services.sql.common.input_validation import (
        MAX_GENERATED_SQL_CHARS,
    )
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
    gen_ai_main._service_cache.clear()
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
        gen_ai_main._service_cache.clear()


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

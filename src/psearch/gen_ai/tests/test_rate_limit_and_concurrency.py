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

"""Per-principal rate-limit, /generate-sql concurrency cap, and task_manager bounds tests."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from typing import Any, Dict, List, Tuple
from unittest.mock import MagicMock
import uuid
import warnings

with warnings.catch_warnings():
    warnings.filterwarnings(
        "ignore",
        message=r"Using `httpx` with `starlette\.testclient` is deprecated.*",
        category=Warning,
    )
    from fastapi.testclient import TestClient

from fastapi import HTTPException
import pytest

try:
    from psearch.gen_ai import auth, main as gen_ai_main
    import psearch.gen_ai.rate_limit as rate_limit
    from psearch.gen_ai.services.conversational_search_service import (
        ConversationalSearchResponse,
    )
    from psearch.gen_ai.services.sql.common.schema_utils import SchemaLoader
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
    import gen_ai.rate_limit as rate_limit
    from gen_ai.services.conversational_search_service import (
        ConversationalSearchResponse,
    )
    from gen_ai.services.sql.common.schema_utils import SchemaLoader
    from gen_ai.tasks import task_manager


_REPO_ROOT = Path(__file__).resolve().parents[4]
_SRC_DIR = _REPO_ROOT / "src"
_PSEARCH_DIR = _SRC_DIR / "psearch"

_AUTH_AND_LIMIT_ENV_VARS = (
    "GEN_AI_ADMIN_TOKEN",
    "ADMIN_TOKEN",
    "GEN_AI_AUTH_AUDIENCE",
    "GEN_AI_ALLOWED_PRINCIPALS",
    "GEN_AI_ALLOWED_DOMAINS",
    "GEN_AI_RATE_LIMIT_PER_MINUTE",
    "GEN_AI_SQL_TASK_RATE_LIMIT_PER_MINUTE",
    "GEN_AI_STATUS_RATE_LIMIT_PER_MINUTE",
    "GEN_AI_MAX_CONCURRENT_SQL_TASKS",
)

_GENERATE_SQL_PAYLOAD = {
    "source_table": "psearch-dev-ze.raw_data.product_catalog",
    "destination_table": "psearch-dev-ze.processed_data.products",
    "source_schema_fields": ["id", "title"],
    "destination_schema": {
        "fields": [{"name": "id", "type": "STRING", "mode": "REQUIRED"}]
    },
}


def _reset_env_and_state(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in _AUTH_AND_LIMIT_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("PROJECT_ID", "test-project")
    rate_limit.reset_limiters()
    task_manager.clear_tasks()


def _build_app_with_mocks(
    monkeypatch: pytest.MonkeyPatch,
) -> Tuple[Any, Dict[str, MagicMock]]:
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


def test_poc_25_anonymous_and_authenticated_burst_generate_sql_blocks_wallet_drain(
    monkeypatch: pytest.MonkeyPatch,
):
    """25-request PoC regression: anonymous burst -> 0 work; authenticated burst -> capped at 3."""
    _reset_env_and_state(monkeypatch)

    mock_genai_client = MagicMock()
    mock_bq_client = MagicMock()
    monkeypatch.setattr("google.genai.Client", lambda **kwargs: mock_genai_client)
    monkeypatch.setattr("google.cloud.bigquery.Client", lambda **kwargs: mock_bq_client)

    app, mocks = _build_app_with_mocks(monkeypatch)
    try:
        with TestClient(app) as client:
            # 1a. Unconfigured auth: 25 anonymous POST /generate-sql -> 25x 403, 0 tasks/calls
            anon_unconf_statuses = [
                client.post("/generate-sql", json=_GENERATE_SQL_PAYLOAD).status_code
                for _ in range(25)
            ]
            assert anon_unconf_statuses == [403] * 25
            assert mocks["pipeline"].execute_pipeline.call_count == 0
            assert task_manager.get_all_tasks_summary() == []
            assert mock_genai_client.models.generate_content.call_count == 0
            assert mock_bq_client.query.call_count == 0
            assert rate_limit.sql_task_concurrency_limiter.active_count == 0

            # 1b. Configured auth: 25 anonymous POST /generate-sql -> 25x 401, 0 tasks/calls
            monkeypatch.setenv("GEN_AI_ADMIN_TOKEN", "top-secret-admin")
            anon_conf_statuses = [
                client.post("/generate-sql", json=_GENERATE_SQL_PAYLOAD).status_code
                for _ in range(25)
            ]
            assert anon_conf_statuses == [401] * 25
            assert mocks["pipeline"].execute_pipeline.call_count == 0
            assert task_manager.get_all_tasks_summary() == []
            assert mock_genai_client.models.generate_content.call_count == 0
            assert mock_bq_client.query.call_count == 0
            assert rate_limit.sql_task_concurrency_limiter.active_count == 0

            # 2. Authenticated 25-request concurrent burst with max_concurrent=3
            monkeypatch.setenv("GEN_AI_MAX_CONCURRENT_SQL_TASKS", "3")
            monkeypatch.setenv("GEN_AI_SQL_TASK_RATE_LIMIT_PER_MINUTE", "100")
            monkeypatch.setenv("GEN_AI_RATE_LIMIT_PER_MINUTE", "100")

            all_three_started = threading.Barrier(4)
            release_pipelines = threading.Event()
            pipeline_factory_calls = 0
            factory_lock = threading.Lock()

            def _blocking_pipeline(**kwargs: Any) -> None:
                all_three_started.wait(timeout=5.0)
                assert release_pipelines.wait(timeout=5.0)

            mocks["pipeline"].execute_pipeline.side_effect = _blocking_pipeline

            def _counting_pipeline_factory() -> MagicMock:
                nonlocal pipeline_factory_calls
                with factory_lock:
                    pipeline_factory_calls += 1
                return mocks["pipeline"]

            app.dependency_overrides[gen_ai_main.get_transformation_pipeline] = (
                _counting_pipeline_factory
            )

            auth_headers = {"X-Admin-Token": "top-secret-admin"}

            def _fire_generate_sql() -> Tuple[int, Dict[str, str], Any]:
                resp = client.post(
                    "/generate-sql",
                    headers=auth_headers,
                    json=_GENERATE_SQL_PAYLOAD,
                )
                return resp.status_code, dict(resp.headers), resp.json()

            with ThreadPoolExecutor(max_workers=25) as pool:
                # Start 3 requests first or all 25 concurrently; fire all 25 at once!
                futures = [pool.submit(_fire_generate_sql) for _ in range(25)]

                # Wait until 3 background tasks are simultaneously inside execute_pipeline
                all_three_started.wait(timeout=5.0)
                assert rate_limit.sql_task_concurrency_limiter.active_count == 3

                # Wait until all 22 excess requests have completed with 429 while 3 remain held
                deadline = time.monotonic() + 5.0
                while sum(1 for f in futures if f.done()) < 22:
                    assert time.monotonic() < deadline, "Timed out waiting for 22 rejected requests"
                    time.sleep(0.01)

                assert sum(1 for f in futures if f.done()) == 22
                assert rate_limit.sql_task_concurrency_limiter.active_count == 3

                # Release the 3 held pipelines and collect all 25 responses
                release_pipelines.set()
                results = [f.result(timeout=5.0) for f in futures]

            accepted = [r for r in results if r[0] == 200]
            rejected = [r for r in results if r[0] == 429]
            assert len(accepted) == 3
            assert len(rejected) == 22
            for _, headers, body in rejected:
                assert headers.get("retry-after") == "5"
                assert (
                    body["detail"]
                    == "Maximum concurrent SQL generation tasks reached; please retry shortly"
                )

            # Rejected requests must fail BEFORE get_transformation_pipeline() or init_task()
            assert pipeline_factory_calls == 3
            assert mocks["pipeline"].execute_pipeline.call_count == 3
            assert len(task_manager.get_all_tasks_summary()) == 3
            assert rate_limit.sql_task_concurrency_limiter.active_count == 0

            # Once released, subsequent requests succeed cleanly
            mocks["pipeline"].execute_pipeline.side_effect = None
            post_release_resp = client.post(
                "/generate-sql",
                headers=auth_headers,
                json=_GENERATE_SQL_PAYLOAD,
            )
            assert post_release_resp.status_code == 200
            assert rate_limit.sql_task_concurrency_limiter.active_count == 0
    finally:
        app.dependency_overrides.clear()


def test_per_principal_rate_limits_isolate_principals_and_spare_status_polling(
    monkeypatch: pytest.MonkeyPatch,
):
    """Per-principal sliding window isolates Alice vs Bob and separates status polling from paid inference."""
    _reset_env_and_state(monkeypatch)
    monkeypatch.setenv("GEN_AI_AUTH_AUDIENCE", "https://gen-ai.run.app")
    monkeypatch.setenv(
        "GEN_AI_ALLOWED_PRINCIPALS",
        "alice@example.com,bob@example.com,carol@example.com",
    )
    monkeypatch.setenv("GEN_AI_RATE_LIMIT_PER_MINUTE", "3")
    monkeypatch.setenv("GEN_AI_SQL_TASK_RATE_LIMIT_PER_MINUTE", "2")
    monkeypatch.setenv("GEN_AI_STATUS_RATE_LIMIT_PER_MINUTE", "4")

    def _fake_verify(token: str, audience: str) -> Dict[str, Any]:
        email_map = {
            "alice-jwt": "alice@example.com",
            "bob-jwt": "bob@example.com",
            "carol-jwt": "carol@example.com",
        }
        return {
            "iss": "https://accounts.google.com",
            "email": email_map[token],
            "email_verified": True,
        }

    monkeypatch.setattr(auth, "verify_bearer_token", _fake_verify)

    existing_task_id = str(uuid.uuid4())
    app, mocks = _build_app_with_mocks(monkeypatch)
    task_manager.init_task(existing_task_id, task_type="sql_generation")

    alice_headers = {"Authorization": "Bearer alice-jwt"}
    bob_headers = {"Authorization": "Bearer bob-jwt"}
    carol_headers = {"Authorization": "Bearer carol-jwt"}

    try:
        with TestClient(app) as client:
            # 1. Alice uses all 3 paid_inference requests across paid endpoints
            assert (
                client.post(
                    "/conversational-search",
                    headers=alice_headers,
                    json={"query": "shoes"},
                ).status_code
                == 200
            )
            assert (
                client.post(
                    "/enrichment",
                    headers=alice_headers,
                    json={"product_id": "p1", "product_data": {"title": "Shoe"}},
                ).status_code
                == 200
            )
            assert (
                client.post(
                    "/sql/validate",
                    headers=alice_headers,
                    json={"sql_script": "SELECT 1"},
                ).status_code
                == 200
            )

            # 4th paid_inference call by Alice is blocked on ALL paid inference routes
            paid_routes_and_payloads = [
                ("/conversational-search", {"query": "shoes"}),
                ("/enrichment", {"product_id": "p1", "product_data": {"title": "Shoe"}}),
                (
                    "/marketing",
                    {
                        "product_id": "p1",
                        "product_data": {"title": "Shoe"},
                        "content_type": "ad",
                    },
                ),
                (
                    "/generate-enhanced-image",
                    {
                        "product_id": "p1",
                        "product_data": {"title": "Shoe"},
                        "image_base64": "YWJj",
                        "background_prompt": "studio",
                    },
                ),
                ("/generate-sql", _GENERATE_SQL_PAYLOAD),
                ("/sql/simple-fix", {"sql_script": "SELECT 1", "error_message": "err"}),
                ("/sql/validate", {"sql_script": "SELECT 1"}),
                (
                    "/sql/fix",
                    {
                        "original_sql": "SELECT 1",
                        "current_sql": "SELECT 1",
                        "error_message": "err",
                    },
                ),
                (
                    "/sql/analyze",
                    {"original_sql": "SELECT 1", "fixed_sql": "SELECT 2"},
                ),
            ]
            conv_calls_before = mocks["conversational"].process_query.call_count
            for path, payload in paid_routes_and_payloads:
                limited_resp = client.post(path, headers=alice_headers, json=payload)
                assert limited_resp.status_code == 429, f"Expected 429 on {path}"
                assert (
                    limited_resp.json()["detail"]
                    == "Rate limit exceeded; please retry later"
                )
                retry_after = int(limited_resp.headers["retry-after"])
                assert 1 <= retry_after <= 60
            assert mocks["conversational"].process_query.call_count == conv_calls_before

            # 2. Bob is completely isolated from Alice's bucket and gets 200
            bob_resp = client.post(
                "/conversational-search",
                headers=bob_headers,
                json={"query": "jackets"},
            )
            assert bob_resp.status_code == 200

            # 3. Alice's status polling uses separate status_poll scope (limit=4) and still works!
            assert (
                client.get(
                    f"/sql/task-status/{existing_task_id}", headers=alice_headers
                ).status_code
                == 200
            )
            assert (
                client.get(
                    f"/sql/task-status/{uuid.uuid4()}", headers=alice_headers
                ).status_code
                == 404
            )
            assert (
                client.get("/sql/tasks-summary", headers=alice_headers).status_code
                == 200
            )
            assert (
                client.get("/sql/tasks-summary", headers=alice_headers).status_code
                == 200
            )
            # 5th status poll exceeds GEN_AI_STATUS_RATE_LIMIT_PER_MINUTE=4 -> 429
            poll_429 = client.get("/sql/tasks-summary", headers=alice_headers)
            assert poll_429.status_code == 429
            assert int(poll_429.headers["retry-after"]) >= 1

            # 4. Carol tests dedicated sql_task rate limit (limit=2) vs paid_inference (increase to 10)
            monkeypatch.setenv("GEN_AI_RATE_LIMIT_PER_MINUTE", "10")
            assert (
                client.post(
                    "/generate-sql", headers=carol_headers, json=_GENERATE_SQL_PAYLOAD
                ).status_code
                == 200
            )
            assert (
                client.post(
                    "/generate-sql", headers=carol_headers, json=_GENERATE_SQL_PAYLOAD
                ).status_code
                == 200
            )
            sql_rate_429 = client.post(
                "/generate-sql", headers=carol_headers, json=_GENERATE_SQL_PAYLOAD
            )
            assert sql_rate_429.status_code == 429
            assert (
                sql_rate_429.json()["detail"]
                == "Rate limit exceeded; please retry later"
            )
            # Carol can still call /conversational-search because paid_inference is only at 3/10
            assert (
                client.post(
                    "/conversational-search",
                    headers=carol_headers,
                    json={"query": "boots"},
                ).status_code
                == 200
            )
    finally:
        app.dependency_overrides.clear()


def test_sliding_window_rate_limiter_clock_expiry_and_bounded_key_eviction():
    """Verify deterministic Retry-After calculation, window pruning, and MAX_TRACKED_KEYS eviction."""
    assert rate_limit.WINDOW_SECONDS == 60.0
    assert rate_limit.MAX_TRACKED_KEYS == 10_000

    now = [100.0]
    limiter = rate_limit.SlidingWindowRateLimiter(
        window_seconds=60.0,
        max_tracked_keys=3,
        clock=lambda: now[0],
    )
    alice = auth.AuthPrincipal(method="oidc", subject="alice@example.com")

    limiter.check("paid_inference", alice, limit=2)
    now[0] = 110.0
    limiter.check("paid_inference", alice, limit=2)

    # At t=125.2, oldest timestamp is 100.0 -> 60.0 - 25.2 = 34.8 -> ceil = 35
    now[0] = 125.2
    with pytest.raises(HTTPException) as exc_info:
        limiter.check("paid_inference", alice, limit=2)
    assert exc_info.value.status_code == 429
    assert exc_info.value.headers == {"Retry-After": "35"}

    # At t=159.9, oldest timestamp 100.0 is not yet <= 99.9 -> Retry-After = 1
    now[0] = 159.9
    with pytest.raises(HTTPException) as exc_almost:
        limiter.check("paid_inference", alice, limit=2)
    assert exc_almost.value.headers == {"Retry-After": "1"}

    # At t=160.0, timestamp 100.0 <= 160.0 - 60.0 is pruned -> succeeds!
    now[0] = 160.0
    limiter.check("paid_inference", alice, limit=2)

    # Bounded key eviction: max_tracked_keys=3
    limiter.reset()
    now[0] = 200.0
    p1 = auth.AuthPrincipal(method="oidc", subject="p1@example.com")
    p2 = auth.AuthPrincipal(method="oidc", subject="p2@example.com")
    p3 = auth.AuthPrincipal(method="oidc", subject="p3@example.com")
    p4 = auth.AuthPrincipal(method="oidc", subject="p4@example.com")

    limiter.check("paid_inference", p1, limit=1)
    now[0] = 201.0
    limiter.check("paid_inference", p2, limit=1)
    now[0] = 202.0
    limiter.check("paid_inference", p3, limit=1)
    assert limiter.tracked_keys_count == 3

    # Touch p1 (even on a 429 check) so p2 becomes least-recently-used
    now[0] = 203.0
    with pytest.raises(HTTPException):
        limiter.check("paid_inference", p1, limit=1)

    # Adding p4 when all 3 are active must evict oldest-accessed (p2) and stay <= 3
    now[0] = 204.0
    limiter.check("paid_inference", p4, limit=1)
    assert limiter.tracked_keys_count == 3

    # p1 is still rate-limited, whereas evicted p2 gets a fresh bucket
    with pytest.raises(HTTPException):
        limiter.check("paid_inference", p1, limit=1)
    limiter.check("paid_inference", p2, limit=1)
    assert limiter.tracked_keys_count == 3

    # Expired bucket sweep: advance past p4's window, insert p3 -> sweeps expired without evicting fresh
    now[0] = 265.0
    limiter.check("paid_inference", p3, limit=1)
    assert limiter.tracked_keys_count <= 3


def test_invalid_rate_limit_and_concurrency_env_vars_fail_closed_with_sanitized_500(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
):
    """Non-numeric or non-positive rate-limit / concurrency env vars return sanitized 500."""
    _reset_env_and_state(monkeypatch)
    monkeypatch.setenv("GEN_AI_ADMIN_TOKEN", "secret")
    auth_headers = {"X-Admin-Token": "secret"}

    app, _ = _build_app_with_mocks(monkeypatch)
    env_cases = [
        ("GEN_AI_RATE_LIMIT_PER_MINUTE", "POST", "/conversational-search", {"query": "x"}),
        ("GEN_AI_SQL_TASK_RATE_LIMIT_PER_MINUTE", "POST", "/generate-sql", _GENERATE_SQL_PAYLOAD),
        ("GEN_AI_STATUS_RATE_LIMIT_PER_MINUTE", "GET", "/sql/tasks-summary", None),
        ("GEN_AI_MAX_CONCURRENT_SQL_TASKS", "POST", "/generate-sql", _GENERATE_SQL_PAYLOAD),
    ]
    bad_values = ["0", "-5", "not-a-number", "3.14"]

    try:
        with TestClient(app) as client:
            for env_var, method, path, payload in env_cases:
                for bad_val in bad_values:
                    for v, _, _, _ in env_cases:
                        monkeypatch.delenv(v, raising=False)
                    rate_limit.reset_limiters()
                    monkeypatch.setenv(env_var, bad_val)
                    caplog.clear()
                    with caplog.at_level("ERROR"):
                        if payload is not None:
                            resp = client.request(
                                method, path, headers=auth_headers, json=payload
                            )
                        else:
                            resp = client.request(method, path, headers=auth_headers)
                    assert resp.status_code == 500, f"{env_var}={bad_val} did not return 500"
                    assert resp.json() == {"detail": "Internal server configuration error"}
                    assert "ValueError" not in resp.text
                    assert any(env_var in rec.message for rec in caplog.records)
                    assert rate_limit.sql_task_concurrency_limiter.active_count == 0
    finally:
        app.dependency_overrides.clear()


def test_generate_sql_releases_concurrency_slot_if_pipeline_or_setup_raises(
    monkeypatch: pytest.MonkeyPatch,
):
    """Concurrency slot is always released if execute_pipeline or pre-task setup raises."""
    _reset_env_and_state(monkeypatch)
    monkeypatch.setenv("GEN_AI_ADMIN_TOKEN", "secret")
    monkeypatch.setenv("GEN_AI_MAX_CONCURRENT_SQL_TASKS", "1")
    auth_headers = {"X-Admin-Token": "secret"}

    app, mocks = _build_app_with_mocks(monkeypatch)
    try:
        # 1. Background execute_pipeline raises RuntimeError -> slot still released in finally
        mocks["pipeline"].execute_pipeline.side_effect = RuntimeError("pipeline exploded")
        with TestClient(app, raise_server_exceptions=False) as client:
            bg_fail_resp = client.post(
                "/generate-sql",
                headers=auth_headers,
                json=_GENERATE_SQL_PAYLOAD,
            )
            assert bg_fail_resp.status_code == 200
            assert rate_limit.sql_task_concurrency_limiter.active_count == 0

            # Slot is free so next request succeeds when pipeline recovers
            mocks["pipeline"].execute_pipeline.side_effect = None
            ok_resp = client.post(
                "/generate-sql",
                headers=auth_headers,
                json=_GENERATE_SQL_PAYLOAD,
            )
            assert ok_resp.status_code == 200
            assert rate_limit.sql_task_concurrency_limiter.active_count == 0

        # 2a. SchemaLoader.get_destination_schema raises before scheduling background task
        payload_without_schema = {
            "source_table": "psearch-dev-ze.raw_data.product_catalog",
            "destination_table": "psearch-dev-ze.processed_data.products",
            "source_schema_fields": ["id", "title"],
        }
        monkeypatch.setattr(
            SchemaLoader,
            "get_destination_schema",
            classmethod(lambda cls: (_ for _ in ()).throw(RuntimeError("schema load boom"))),
        )
        with TestClient(app) as client:
            schema_err_resp = client.post(
                "/generate-sql",
                headers=auth_headers,
                json=payload_without_schema,
            )
            assert schema_err_resp.status_code == 500
            assert rate_limit.sql_task_concurrency_limiter.active_count == 0

            # 2b. SchemaLoader returns None -> 400 HTTPException, slot still released
            monkeypatch.setattr(
                SchemaLoader,
                "get_destination_schema",
                classmethod(lambda cls: None),
            )
            empty_schema_resp = client.post(
                "/generate-sql",
                headers=auth_headers,
                json=payload_without_schema,
            )
            assert empty_schema_resp.status_code == 400
            assert rate_limit.sql_task_concurrency_limiter.active_count == 0

            # 2c. Subsequent valid request with inline destination_schema succeeds with max_concurrent=1
            recovered = client.post(
                "/generate-sql",
                headers=auth_headers,
                json=_GENERATE_SQL_PAYLOAD,
            )
            assert recovered.status_code == 200
            assert rate_limit.sql_task_concurrency_limiter.active_count == 0
    finally:
        app.dependency_overrides.clear()

    # 3. Direct unit check of run_guarded for both sync and async callables
    limiter = rate_limit.BackgroundTaskConcurrencyLimiter()
    limiter.acquire_or_reject(max_concurrent=1)
    assert limiter.active_count == 1

    async def _async_boom() -> None:
        assert limiter.active_count == 1
        raise RuntimeError("async failure")

    with pytest.raises(RuntimeError, match="async failure"):
        asyncio.run(limiter.run_guarded(_async_boom))
    assert limiter.active_count == 0

    # Extra release() never underflows below 0
    limiter.release()
    assert limiter.active_count == 0


def test_task_manager_is_thread_safe_and_evicts_oldest_completed_tasks_at_cap(
    monkeypatch: pytest.MonkeyPatch,
):
    """task_manager bounds stored tasks to MAX_STORED_TASKS, evicting oldest terminal entries first."""
    assert task_manager.MAX_STORED_TASKS == 1000
    task_manager.clear_tasks()

    monkeypatch.setattr(task_manager, "MAX_STORED_TASKS", 5)

    # Create 2 active tasks and 3 terminal tasks (total = 5 = cap)
    task_manager.init_task("active-1", "sql_generation", {"idx": 1})
    task_manager.init_task("done-1", "sql_generation", {"idx": 2})
    task_manager.update_task_status("done-1", status="completed", result="SELECT 1")

    task_manager.init_task("active-2", "sql_generation", {"idx": 3})
    task_manager.update_task_status("active-2", status="processing")

    task_manager.init_task("failed-1", "sql_generation", {"idx": 4})
    task_manager.update_task_status("failed-1", status="failed", error="bq err")

    task_manager.init_task("done-2", "sql_generation", {"idx": 5})
    task_manager.update_task_status("done-2", status="completed", result="SELECT 2")

    assert len(task_manager.task_status) == 5

    # Inserting 3 more tasks must evict done-1, failed-1, done-2 in FIFO order while keeping active-1 & active-2
    task_manager.init_task("new-1", "sql_generation")
    assert len(task_manager.task_status) == 5
    assert task_manager.get_task_status("done-1") is None
    assert task_manager.get_task_status("active-1") is not None
    assert task_manager.get_task_status("active-2") is not None

    task_manager.init_task("new-2", "sql_generation")
    assert len(task_manager.task_status) == 5
    assert task_manager.get_task_status("failed-1") is None
    assert task_manager.get_task_status("active-1") is not None
    assert task_manager.get_task_status("active-2") is not None

    task_manager.init_task("new-3", "sql_generation")
    assert len(task_manager.task_status) == 5
    assert task_manager.get_task_status("done-2") is None
    assert task_manager.get_task_status("active-1")["status"] == "received"
    assert task_manager.get_task_status("active-2")["status"] == "processing"

    # Returned copy from get_task_status must not allow mutating internal logs or input_details
    snap = task_manager.get_task_status("active-1")
    assert snap is not None
    snap["logs"].append({"timestamp": "fake", "message": "mutated"})
    snap["input_details"]["idx"] = 999
    fresh = task_manager.get_task_status("active-1")
    assert fresh["input_details"]["idx"] == 1
    assert all(entry["message"] != "mutated" for entry in fresh["logs"])

    # Multi-threaded stress test across 8 worker threads
    monkeypatch.setattr(task_manager, "MAX_STORED_TASKS", 50)
    task_manager.clear_tasks()

    def _worker(worker_id: int) -> None:
        for i in range(25):
            tid = f"w{worker_id}-t{i}"
            task_manager.init_task(tid, "sql_generation", {"worker": worker_id})
            task_manager.add_task_log(tid, f"step {i}")
            task_manager.update_task_status(tid, status="processing")
            _ = task_manager.get_task_status(tid)
            _ = task_manager.get_all_tasks_summary()
            task_manager.update_task_status(tid, status="completed", result="SELECT 1")

    with ThreadPoolExecutor(max_workers=8) as pool:
        futures = [pool.submit(_worker, w) for w in range(8)]
        for fut in futures:
            fut.result(timeout=10.0)

    assert len(task_manager.task_status) == 50
    assert len(task_manager.get_all_tasks_summary()) == 50
    task_manager.clear_tasks()


def test_rate_limit_module_imports_cleanly_under_both_pythonpath_roots(tmp_path: Path):
    """rate_limit.py must use relative imports so both PYTHONPATH=src and PYTHONPATH=src/psearch work."""
    script = (
        "import importlib, sys\n"
        "mod = importlib.import_module(sys.argv[1])\n"
        "assert mod.DEFAULT_RATE_LIMIT_PER_MINUTE == 60\n"
        "assert mod.DEFAULT_SQL_TASK_RATE_LIMIT_PER_MINUTE == 10\n"
        "assert mod.DEFAULT_STATUS_RATE_LIMIT_PER_MINUTE == 300\n"
        "assert mod.DEFAULT_MAX_CONCURRENT_SQL_TASKS == 3\n"
    )
    for pythonpath, module_name in (
        (str(_SRC_DIR), "psearch.gen_ai.rate_limit"),
        (str(_PSEARCH_DIR), "gen_ai.rate_limit"),
    ):
        env = dict(os.environ)
        env["HOME"] = str(tmp_path)
        env["PYTHONPATH"] = pythonpath
        completed = subprocess.run(
            [sys.executable, "-W", "default", "-c", script, module_name],
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, (
            f"Importing {module_name} with PYTHONPATH={pythonpath} failed:\n"
            f"stdout={completed.stdout}\nstderr={completed.stderr}"
        )

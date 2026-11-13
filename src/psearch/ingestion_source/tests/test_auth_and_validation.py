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

"""Unit tests for ingestion_source fail-closed authentication and input validation."""

import uuid

from fastapi import HTTPException
import pytest

try:
    from psearch.ingestion_source import auth, validation
except ModuleNotFoundError as exc:
    if exc.name not in {"psearch", "psearch.ingestion_source"}:
        raise
    from ingestion_source import auth, validation


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


def test_require_auth_fails_closed_when_unconfigured(monkeypatch: pytest.MonkeyPatch):
    _clear_auth_env(monkeypatch)

    with pytest.raises(HTTPException) as exc_info:
        auth.require_auth()
    assert exc_info.value.status_code == 403
    assert "not configured" in exc_info.value.detail.lower()

    # Whitespace-only tokens must also be treated as unconfigured.
    monkeypatch.setenv("INGESTION_ADMIN_TOKEN", "   ")
    monkeypatch.setenv("ADMIN_TOKEN", "\t\n")
    with pytest.raises(HTTPException) as ws_exc:
        auth.require_auth("   ", "Bearer some-token")
    assert ws_exc.value.status_code == 403


def test_require_auth_missing_credentials_returns_401(monkeypatch: pytest.MonkeyPatch):
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("INGESTION_ADMIN_TOKEN", "secret")

    with pytest.raises(HTTPException) as exc_info:
        auth.require_auth(None, None)
    assert exc_info.value.status_code == 401
    assert exc_info.value.headers == {"WWW-Authenticate": "Bearer"}

    # Direct no-arg Python invocation (FastAPI Header default objects) behaves identically.
    with pytest.raises(HTTPException) as direct_exc:
        auth.require_auth()
    assert direct_exc.value.status_code == 401

    # Non-Bearer Authorization schemes without X-Admin-Token return 401.
    for bad_auth in ("Basic dXNlcjpwYXNz", "Bearer", "Bearer   "):
        with pytest.raises(HTTPException) as scheme_exc:
            auth.require_auth(None, bad_auth)
        assert scheme_exc.value.status_code == 401
        assert scheme_exc.value.headers == {"WWW-Authenticate": "Bearer"}


def test_require_auth_accepts_matching_admin_token_and_rejects_wrong_token(
    monkeypatch: pytest.MonkeyPatch,
):
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("INGESTION_ADMIN_TOKEN", "secret")

    principal = auth.require_auth("secret", None)
    assert principal == auth.AuthPrincipal(method="admin_token", subject="admin_token")

    with pytest.raises(HTTPException) as wrong_exc:
        auth.require_auth("wrong-token", None)
    assert wrong_exc.value.status_code == 403

    # A wrong X-Admin-Token must never be silently ignored even if Authorization is present.
    with pytest.raises(HTTPException) as both_exc:
        auth.require_auth("wrong-token", "Bearer valid-looking-token")
    assert both_exc.value.status_code == 403

    # Fallback ADMIN_TOKEN is honored when INGESTION_ADMIN_TOKEN is unset.
    monkeypatch.delenv("INGESTION_ADMIN_TOKEN")
    monkeypatch.setenv("ADMIN_TOKEN", "fallback-secret")
    assert auth.require_auth("fallback-secret", None) == auth.AuthPrincipal(
        method="admin_token", subject="admin_token"
    )
    with pytest.raises(HTTPException) as fallback_wrong:
        auth.require_auth("secret", None)
    assert fallback_wrong.value.status_code == 403


def test_require_auth_oidc_accepts_allowlisted_principal(
    monkeypatch: pytest.MonkeyPatch,
):
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("INGESTION_AUTH_AUDIENCE", "https://svc.run.app")
    monkeypatch.setenv(
        "INGESTION_ALLOWED_PRINCIPALS",
        "ops@example.com, deploy-sa@proj.iam.gserviceaccount.com",
    )

    captured = {}

    def fake_verify(token: str, audience: str):
        captured["token"] = token
        captured["audience"] = audience
        return {
            "iss": "https://accounts.google.com",
            "email": "OPS@example.com",
            "email_verified": True,
        }

    monkeypatch.setattr(auth, "verify_bearer_token", fake_verify)

    principal = auth.require_auth(None, "Bearer good-jwt-token")
    assert principal == auth.AuthPrincipal(method="oidc", subject="ops@example.com")
    assert captured == {"token": "good-jwt-token", "audience": "https://svc.run.app"}

    # Domain allowlist with matching Google Workspace 'hd' claim also succeeds.
    monkeypatch.delenv("INGESTION_ALLOWED_PRINCIPALS")
    monkeypatch.setenv("INGESTION_ALLOWED_DOMAINS", "corp.example.com")
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
    domain_principal = auth.require_auth(None, "Bearer domain-jwt")
    assert domain_principal == auth.AuthPrincipal(
        method="oidc", subject="alice@corp.example.com"
    )


def test_require_auth_oidc_rejects_unverified_or_unlisted_principal(
    monkeypatch: pytest.MonkeyPatch,
):
    _clear_auth_env(monkeypatch)
    monkeypatch.setenv("INGESTION_AUTH_AUDIENCE", "https://svc.run.app")
    monkeypatch.setenv("INGESTION_ALLOWED_PRINCIPALS", "ops@example.com")
    monkeypatch.setenv("INGESTION_ALLOWED_DOMAINS", "corp.example.com")

    # Unverified email -> 403
    monkeypatch.setattr(
        auth,
        "verify_bearer_token",
        lambda token, audience: {
            "iss": "https://accounts.google.com",
            "email": "ops@example.com",
            "email_verified": False,
        },
    )
    with pytest.raises(HTTPException) as unverified_exc:
        auth.require_auth(None, "Bearer tok")
    assert unverified_exc.value.status_code == 403

    # Unlisted principal -> 403
    monkeypatch.setattr(
        auth,
        "verify_bearer_token",
        lambda token, audience: {
            "iss": "https://accounts.google.com",
            "email": "stranger@gmail.com",
            "email_verified": True,
        },
    )
    with pytest.raises(HTTPException) as unlisted_exc:
        auth.require_auth(None, "Bearer tok")
    assert unlisted_exc.value.status_code == 403

    # Matching email domain without matching 'hd' claim -> 403
    monkeypatch.setattr(
        auth,
        "verify_bearer_token",
        lambda token, audience: {
            "iss": "https://accounts.google.com",
            "email": "user@corp.example.com",
            "email_verified": True,
            "hd": None,
        },
    )
    with pytest.raises(HTTPException) as missing_hd_exc:
        auth.require_auth(None, "Bearer tok")
    assert missing_hd_exc.value.status_code == 403

    # Untrusted issuer -> 401
    monkeypatch.setattr(
        auth,
        "verify_bearer_token",
        lambda token, audience: {
            "iss": "https://evil.example.com",
            "email": "ops@example.com",
            "email_verified": True,
        },
    )
    with pytest.raises(HTTPException) as bad_iss_exc:
        auth.require_auth(None, "Bearer tok")
    assert bad_iss_exc.value.status_code == 401

    # Invalid signature / expired token raising ValueError -> 401
    def raise_invalid(token: str, audience: str):
        raise ValueError("Token expired")

    monkeypatch.setattr(auth, "verify_bearer_token", raise_invalid)
    with pytest.raises(HTTPException) as invalid_tok_exc:
        auth.require_auth(None, "Bearer expired-tok")
    assert invalid_tok_exc.value.status_code == 401

    # Audience configured WITHOUT any allowlist must fail closed with 403
    monkeypatch.delenv("INGESTION_ALLOWED_PRINCIPALS")
    monkeypatch.delenv("INGESTION_ALLOWED_DOMAINS")
    monkeypatch.setattr(
        auth,
        "verify_bearer_token",
        lambda token, audience: {
            "iss": "https://accounts.google.com",
            "email": "ops@example.com",
            "email_verified": True,
        },
    )
    with pytest.raises(HTTPException) as no_allowlist_exc:
        auth.require_auth(None, "Bearer tok")
    assert no_allowlist_exc.value.status_code == 403


def test_validate_upload_filename_rejects_missing_or_dotless(
    monkeypatch: pytest.MonkeyPatch,
):
    for bad_name in (
        None,
        "",
        "   ",
        "README",
        "archive.tar.gz",
        ".csv",
        "nested/.json",
        "catalog.parquet",
        "dir/csv/",
    ):
        with pytest.raises(ValueError):
            validation.validate_upload_filename(bad_name)

    assert validation.validate_upload_filename("catalog.CSV") == "csv"
    assert validation.validate_upload_filename("items.json") == "json"
    assert validation.validate_upload_filename("sub/dir/export.v2.JSON") == "json"

    assert validation.DEFAULT_MAX_UPLOAD_SIZE_BYTES == 52_428_800
    assert issubclass(validation.UploadTooLargeError, ValueError)
    monkeypatch.delenv("MAX_UPLOAD_SIZE_BYTES", raising=False)
    assert validation.get_max_upload_size_bytes() == 52_428_800
    monkeypatch.setenv("MAX_UPLOAD_SIZE_BYTES", "1048576")
    assert validation.get_max_upload_size_bytes() == 1_048_576


def test_validate_file_id_rejects_traversal():
    for bad_id in (
        "../../etc/passwd",
        "not-a-uuid",
        "",
        "   ",
        str(uuid.uuid1()),
        "550e8400e29b41d4a716446655440000",
        "{550e8400-e29b-41d4-a716-446655440000}",
    ):
        with pytest.raises(ValueError):
            validation.validate_file_id(bad_id)

    valid_v4 = str(uuid.uuid4())
    assert validation.validate_file_id(valid_v4) == valid_v4
    assert validation.validate_file_id(valid_v4.upper()) == valid_v4


def test_validate_dataset_id_rejects_cross_project_and_honors_allowlist(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("INGESTION_ALLOWED_DATASETS", raising=False)

    # Cross-project dataset references must raise ValueError
    with pytest.raises(ValueError):
        validation.validate_dataset_id("other-proj.ds")
    with pytest.raises(ValueError):
        validation.validate_dataset_id(
            "other-proj.ds", project_id="my-proj", allow_project_prefix=True
        )
    # Project prefix disallowed when allow_project_prefix=False
    with pytest.raises(ValueError):
        validation.validate_dataset_id(
            "my-proj.raw_ds", project_id="my-proj", allow_project_prefix=False
        )

    # Matching project prefix normalizes to simple dataset ID when allowed
    assert (
        validation.validate_dataset_id(
            "my-proj.raw_ds", project_id="my-proj", allow_project_prefix=True
        )
        == "raw_ds"
    )
    assert validation.validate_dataset_id("raw_ds_01") == "raw_ds_01"

    for invalid_ds in ("", "raw-ds", "raw ds", "raw_ds\n", "a.b.c"):
        with pytest.raises(ValueError):
            validation.validate_dataset_id(
                invalid_ds, project_id="my-proj", allow_project_prefix=True
            )

    # Table identifier validation
    assert validation.validate_table_id("products_2026") == "products_2026"
    for invalid_tbl in ("", "ds.tbl", "bad-table", "tbl\n", "../tbl"):
        with pytest.raises(ValueError):
            validation.validate_table_id(invalid_tbl)

    # INGESTION_ALLOWED_DATASETS allowlist enforcement
    monkeypatch.setenv("INGESTION_ALLOWED_DATASETS", "raw_ds, staging_ds")
    assert validation.validate_dataset_id("raw_ds") == "raw_ds"
    with pytest.raises(PermissionError):
        validation.validate_dataset_id("prod_ds")
    with pytest.raises(PermissionError):
        validation.validate_dataset_id(
            "my-proj.prod_ds", project_id="my-proj", allow_project_prefix=True
        )


def test_validate_write_disposition_blocks_truncate_by_default(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.delenv("ALLOW_WRITE_TRUNCATE", raising=False)

    assert validation.validate_write_disposition("WRITE_EMPTY") == "WRITE_EMPTY"
    assert validation.validate_write_disposition("WRITE_APPEND") == "WRITE_APPEND"

    with pytest.raises(PermissionError):
        validation.validate_write_disposition("WRITE_TRUNCATE")

    monkeypatch.setenv("ALLOW_WRITE_TRUNCATE", "false")
    with pytest.raises(PermissionError):
        validation.validate_write_disposition("WRITE_TRUNCATE")

    monkeypatch.setenv("ALLOW_WRITE_TRUNCATE", "true")
    assert validation.validate_write_disposition("WRITE_TRUNCATE") == "WRITE_TRUNCATE"

    for bad_disp in ("__class__", "WRITE_DROP", "", "write_empty"):
        with pytest.raises(ValueError):
            validation.validate_write_disposition(bad_disp)

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

"""Fail-closed authentication helpers for the PSearch Gen AI API."""

import hmac
import os
import threading
from typing import Any, Dict, Optional

from fastapi import Header, HTTPException
from google.auth.exceptions import GoogleAuthError
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token
from pydantic import BaseModel

_GOOGLE_OIDC_ISSUERS = ("https://accounts.google.com", "accounts.google.com")
_BEARER_CHALLENGE_HEADERS = {"WWW-Authenticate": "Bearer"}
_auth_request_local = threading.local()


def _get_google_auth_request() -> google_requests.Request:
    """Return a lazily initialized per-thread google.auth Request transport."""
    request = getattr(_auth_request_local, "request", None)
    if request is None:
        request = google_requests.Request()
        _auth_request_local.request = request
    return request


class AuthPrincipal(BaseModel):
    """Authenticated caller identity returned by ``require_auth``."""

    method: str
    subject: str


def verify_bearer_token(token: str, audience: str) -> Dict[str, Any]:
    """Verify a Google OIDC Bearer token against Google's public certs and audience."""
    return id_token.verify_oauth2_token(
        token,
        _get_google_auth_request(),
        audience=audience,
    )


def require_auth(
    x_admin_token: Optional[str] = Header(default=None, alias="X-Admin-Token"),
    authorization: Optional[str] = Header(default=None, alias="Authorization"),
) -> AuthPrincipal:
    """Authenticate requests via constant-time X-Admin-Token or allowlisted Google OIDC.

    Fails closed (HTTP 403) when neither admin-token authentication nor OIDC
    audience + principal/domain allowlisting is configured on the server.
    """
    provided_admin_token = x_admin_token if isinstance(x_admin_token, str) else None
    provided_authorization = authorization if isinstance(authorization, str) else None

    expected_admin_token = (
        os.environ.get("GEN_AI_ADMIN_TOKEN", "").strip()
        or os.environ.get("ADMIN_TOKEN", "").strip()
    )
    admin_token_configured = bool(expected_admin_token)

    audience = os.environ.get("GEN_AI_AUTH_AUDIENCE", "").strip()
    allowed_principals = {
        entry.strip().lower()
        for entry in os.environ.get("GEN_AI_ALLOWED_PRINCIPALS", "").split(",")
        if entry.strip()
    }
    allowed_domains = {
        entry.strip().lower().lstrip("@")
        for entry in os.environ.get("GEN_AI_ALLOWED_DOMAINS", "").split(",")
        if entry.strip().lstrip("@")
    }
    oidc_configured = bool(audience) and bool(allowed_principals or allowed_domains)

    if not admin_token_configured and not oidc_configured:
        raise HTTPException(
            status_code=403,
            detail="Gen AI authentication is not configured",
        )

    if provided_admin_token is not None:
        if admin_token_configured and hmac.compare_digest(
            provided_admin_token.encode("utf-8"),
            expected_admin_token.encode("utf-8"),
        ):
            return AuthPrincipal(method="admin_token", subject="admin_token")
        raise HTTPException(
            status_code=403,
            detail="Invalid admin token",
        )

    if not provided_authorization:
        raise HTTPException(
            status_code=401,
            detail="Missing authentication credentials",
            headers=_BEARER_CHALLENGE_HEADERS,
        )

    scheme, sep, raw_token = provided_authorization.strip().partition(" ")
    token = raw_token.strip()
    if scheme.lower() != "bearer" or not sep or not token:
        raise HTTPException(
            status_code=401,
            detail="Invalid authorization header; expected Bearer token",
            headers=_BEARER_CHALLENGE_HEADERS,
        )

    if not oidc_configured:
        raise HTTPException(
            status_code=403,
            detail="OIDC authentication is not configured",
        )

    try:
        claims = verify_bearer_token(token, audience)
    except (ValueError, GoogleAuthError) as exc:
        raise HTTPException(
            status_code=401,
            detail="Invalid bearer token",
            headers=_BEARER_CHALLENGE_HEADERS,
        ) from exc

    if not isinstance(claims, dict):
        raise HTTPException(
            status_code=401,
            detail="Invalid bearer token",
            headers=_BEARER_CHALLENGE_HEADERS,
        )

    if claims.get("iss") not in _GOOGLE_OIDC_ISSUERS:
        raise HTTPException(
            status_code=401,
            detail="Invalid token issuer",
            headers=_BEARER_CHALLENGE_HEADERS,
        )

    email = claims.get("email")
    if (
        claims.get("email_verified") is not True
        or not isinstance(email, str)
        or not email.strip()
    ):
        raise HTTPException(
            status_code=403,
            detail="Token email claim is missing or unverified",
        )

    email_lower = email.strip().lower()
    email_domain = email_lower.rsplit("@", 1)[1] if "@" in email_lower else ""
    hd_claim = claims.get("hd")
    hd_lower = hd_claim.strip().lower() if isinstance(hd_claim, str) else ""

    principal_allowed = email_lower in allowed_principals
    domain_allowed = (
        bool(email_domain)
        and email_domain in allowed_domains
        and hd_lower == email_domain
    )
    if not (principal_allowed or domain_allowed):
        raise HTTPException(
            status_code=403,
            detail="Caller principal is not authorized",
        )

    return AuthPrincipal(method="oidc", subject=email_lower)

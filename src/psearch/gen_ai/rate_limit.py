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

"""Per-principal sliding-window rate limiting and background task concurrency caps."""

from collections import OrderedDict, deque
import inspect
import logging
import math
import os
import threading
import time
from typing import Any, Callable, Deque, Optional, Tuple, Union

from fastapi import Depends, HTTPException

from .auth import AuthPrincipal, require_auth

logger = logging.getLogger(__name__)

DEFAULT_RATE_LIMIT_PER_MINUTE = 60
DEFAULT_SQL_TASK_RATE_LIMIT_PER_MINUTE = 10
DEFAULT_STATUS_RATE_LIMIT_PER_MINUTE = 300
DEFAULT_MAX_CONCURRENT_SQL_TASKS = 3

WINDOW_SECONDS = 60.0
MAX_TRACKED_KEYS = 10_000


def _get_positive_int_env(env_name: str, default: int) -> int:
    """Read a positive integer configuration from ``os.environ`` or raise HTTP 500."""
    raw = os.environ.get(env_name)
    if raw is None or not raw.strip():
        return default
    try:
        parsed = int(raw.strip())
    except ValueError as exc:
        logger.error("Invalid non-integer value configured for %s", env_name)
        raise HTTPException(
            status_code=500,
            detail="Internal server configuration error",
        ) from exc
    if parsed <= 0:
        logger.error(
            "Invalid non-positive value configured for %s: %d", env_name, parsed
        )
        raise HTTPException(
            status_code=500,
            detail="Internal server configuration error",
        )
    return parsed


class SlidingWindowRateLimiter:
    """Thread-safe in-memory sliding-window rate limiter with bounded key tracking."""

    def __init__(
        self,
        window_seconds: float = WINDOW_SECONDS,
        max_tracked_keys: int = MAX_TRACKED_KEYS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._window_seconds = window_seconds
        self._max_tracked_keys = max_tracked_keys
        self._clock = clock
        self._lock = threading.Lock()
        self._buckets: "OrderedDict[Tuple[str, str], Deque[float]]" = OrderedDict()

    @property
    def tracked_keys_count(self) -> int:
        with self._lock:
            return len(self._buckets)

    def reset(self) -> None:
        with self._lock:
            self._buckets.clear()

    def check(
        self,
        scope: str,
        principal: Union[AuthPrincipal, str],
        limit: int,
    ) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            logger.error("Invalid rate-limit threshold for scope %s: %r", scope, limit)
            raise HTTPException(
                status_code=500,
                detail="Internal server configuration error",
            )

        principal_key = (
            f"{principal.method}:{principal.subject}"
            if isinstance(principal, AuthPrincipal)
            else str(principal)
        )
        key = (scope, principal_key)

        with self._lock:
            now = self._clock()
            cutoff = now - self._window_seconds

            bucket = self._buckets.get(key)
            if bucket is not None:
                while bucket and bucket[0] <= cutoff:
                    bucket.popleft()
                self._buckets.move_to_end(key)
            else:
                if len(self._buckets) >= self._max_tracked_keys:
                    expired_keys = []
                    for existing_key, existing_bucket in self._buckets.items():
                        while existing_bucket and existing_bucket[0] <= cutoff:
                            existing_bucket.popleft()
                        if not existing_bucket:
                            expired_keys.append(existing_key)
                    for expired_key in expired_keys:
                        del self._buckets[expired_key]

                    while (
                        len(self._buckets) >= self._max_tracked_keys and self._buckets
                    ):
                        self._buckets.popitem(last=False)

                bucket = deque()
                self._buckets[key] = bucket

            if len(bucket) >= limit:
                retry_after = max(
                    1, math.ceil(self._window_seconds - (now - bucket[0]))
                )
                raise HTTPException(
                    status_code=429,
                    detail="Rate limit exceeded; please retry later",
                    headers={"Retry-After": str(retry_after)},
                )

            bucket.append(now)


class BackgroundTaskConcurrencyLimiter:
    """Thread-safe active-slot ceiling for long-running background tasks."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._active_count = 0

    @property
    def active_count(self) -> int:
        with self._lock:
            return self._active_count

    def acquire_or_reject(self, max_concurrent: Optional[int] = None) -> None:
        if max_concurrent is None:
            limit = _get_positive_int_env(
                "GEN_AI_MAX_CONCURRENT_SQL_TASKS",
                DEFAULT_MAX_CONCURRENT_SQL_TASKS,
            )
        elif (
            isinstance(max_concurrent, bool)
            or not isinstance(max_concurrent, int)
            or max_concurrent <= 0
        ):
            logger.error("Invalid max_concurrent threshold: %r", max_concurrent)
            raise HTTPException(
                status_code=500,
                detail="Internal server configuration error",
            )
        else:
            limit = max_concurrent

        with self._lock:
            if self._active_count >= limit:
                raise HTTPException(
                    status_code=429,
                    detail=(
                        "Maximum concurrent SQL generation tasks reached; "
                        "please retry shortly"
                    ),
                    headers={"Retry-After": "5"},
                )
            self._active_count += 1

    def release(self) -> None:
        with self._lock:
            self._active_count = max(0, self._active_count - 1)

    def reset(self) -> None:
        with self._lock:
            self._active_count = 0

    async def run_guarded_async(
        self, fn: Callable[..., Any], *args: Any, **kwargs: Any
    ) -> Any:
        try:
            return await fn(*args, **kwargs)
        finally:
            self.release()

    def run_guarded(self, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        if inspect.iscoroutinefunction(fn):
            return self.run_guarded_async(fn, *args, **kwargs)
        try:
            return fn(*args, **kwargs)
        finally:
            self.release()


rate_limiter = SlidingWindowRateLimiter()
sql_task_concurrency_limiter = BackgroundTaskConcurrencyLimiter()


def reset_limiters() -> None:
    """Clear all in-memory rate-limit buckets and active concurrency counters."""
    rate_limiter.reset()
    sql_task_concurrency_limiter.reset()


def enforce_paid_inference_rate_limit(
    principal: AuthPrincipal = Depends(require_auth),
) -> AuthPrincipal:
    limit = _get_positive_int_env(
        "GEN_AI_RATE_LIMIT_PER_MINUTE",
        DEFAULT_RATE_LIMIT_PER_MINUTE,
    )
    rate_limiter.check("paid_inference", principal, limit)
    return principal


def enforce_sql_task_rate_limit(
    principal: AuthPrincipal = Depends(require_auth),
) -> AuthPrincipal:
    limit = _get_positive_int_env(
        "GEN_AI_SQL_TASK_RATE_LIMIT_PER_MINUTE",
        DEFAULT_SQL_TASK_RATE_LIMIT_PER_MINUTE,
    )
    rate_limiter.check("sql_task", principal, limit)
    return principal


def enforce_status_poll_rate_limit(
    principal: AuthPrincipal = Depends(require_auth),
) -> AuthPrincipal:
    limit = _get_positive_int_env(
        "GEN_AI_STATUS_RATE_LIMIT_PER_MINUTE",
        DEFAULT_STATUS_RATE_LIMIT_PER_MINUTE,
    )
    rate_limiter.check("status_poll", principal, limit)
    return principal

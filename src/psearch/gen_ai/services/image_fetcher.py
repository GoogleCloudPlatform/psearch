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

"""SSRF-safe, pinned-IP, and size-bounded product image fetching and base64 decoding."""

import base64
import binascii
import ipaddress
import logging
import math
import os
import re
import socket
from typing import Any, Callable, Iterable, List, Optional, Sequence, Tuple
import urllib.parse

import requests
import requests.adapters
import urllib3.connection
import urllib3.connectionpool
import urllib3.exceptions
import urllib3.util.connection

logger = logging.getLogger(__name__)

DEFAULT_MAX_IMAGE_BYTES: int = 10 * 1024 * 1024
DEFAULT_IMAGE_FETCH_TIMEOUT_SECONDS: float = 10.0
DEFAULT_MAX_IMAGE_REDIRECTS: int = 0
MAX_ALLOWED_IMAGE_REDIRECTS: int = 3
MAX_CONNECT_TIMEOUT_SECONDS: float = 5.0
MAX_IMAGE_URL_CHARS: int = 2048
STREAM_CHUNK_BYTES: int = 65536

ALLOWED_IMAGE_CONTENT_TYPES: frozenset[str] = frozenset(
    {
        "image/jpeg",
        "image/jpg",
        "image/png",
        "image/gif",
        "image/webp",
    }
)

_GCS_STORAGE_HOST = "storage.googleapis.com"
_GCS_BUCKET_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$")
_DNS_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_DATA_IMAGE_BASE64_PREFIX_RE = re.compile(
    r"^data:image/(?!svg)[a-zA-Z0-9.+-]+;base64,",
    flags=re.IGNORECASE,
)

_BLOCKED_EXACT_HOSTNAMES: frozenset[str] = frozenset(
    {
        "localhost",
        "localhost.localdomain",
        "ip6-localhost",
        "ip6-loopback",
        "metadata",
        "metadata.google.internal",
        "google.internal",
        "internal",
        "local",
    }
)

_BLOCKED_HOSTNAME_SUFFIXES: Tuple[str, ...] = (
    ".localhost",
    ".google.internal",
    ".internal",
    ".local",
)

_DISALLOWED_IPV4_NETWORKS: Tuple[ipaddress.IPv4Network, ...] = (
    ipaddress.IPv4Network("0.0.0.0/8"),
    ipaddress.IPv4Network("10.0.0.0/8"),
    ipaddress.IPv4Network("100.64.0.0/10"),
    ipaddress.IPv4Network("127.0.0.0/8"),
    ipaddress.IPv4Network("169.254.0.0/16"),
    ipaddress.IPv4Network("172.16.0.0/12"),
    ipaddress.IPv4Network("192.0.0.0/24"),
    ipaddress.IPv4Network("192.0.2.0/24"),
    ipaddress.IPv4Network("192.168.0.0/16"),
    ipaddress.IPv4Network("198.18.0.0/15"),
    ipaddress.IPv4Network("198.51.100.0/24"),
    ipaddress.IPv4Network("203.0.113.0/24"),
    ipaddress.IPv4Network("224.0.0.0/4"),
    ipaddress.IPv4Network("240.0.0.0/4"),
)

_DISALLOWED_IPV6_NETWORKS: Tuple[ipaddress.IPv6Network, ...] = (
    ipaddress.IPv6Network("::/96"),
    ipaddress.IPv6Network("::ffff:0:0/96"),
    ipaddress.IPv6Network("::ffff:0:0:0/96"),
    ipaddress.IPv6Network("64:ff9b::/96"),
    ipaddress.IPv6Network("64:ff9b:1::/48"),
    ipaddress.IPv6Network("100::/64"),
    ipaddress.IPv6Network("2001::/32"),
    ipaddress.IPv6Network("2001:2::/48"),
    ipaddress.IPv6Network("2001:db8::/32"),
    ipaddress.IPv6Network("2002::/16"),
    ipaddress.IPv6Network("fc00::/7"),
    ipaddress.IPv6Network("fe80::/10"),
    ipaddress.IPv6Network("ff00::/8"),
)


class ImageValidationError(ValueError):
    """Raised when an image URL, host, IP, header, or payload fails validation."""


class ImageTooLargeError(ImageValidationError):
    """Raised when an encoded or decoded image payload exceeds configured byte bounds."""


def _validate_positive_int(name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _validate_positive_float(name: str, value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{name} must be a positive number")
    parsed = float(value)
    if not math.isfinite(parsed) or parsed <= 0.0:
        raise ValueError(f"{name} must be a finite positive number")
    return parsed


def _validate_max_redirects(name: str, value: Any) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or value < 0
        or value > MAX_ALLOWED_IMAGE_REDIRECTS
    ):
        raise ValueError(
            f"{name} must be an integer between 0 and {MAX_ALLOWED_IMAGE_REDIRECTS}"
        )
    return value


def get_max_image_bytes() -> int:
    """Return validated ``GEN_AI_MAX_IMAGE_BYTES`` (default 10 MiB) or raise ``ValueError``."""
    raw = os.environ.get("GEN_AI_MAX_IMAGE_BYTES")
    if raw is None or not raw.strip():
        return DEFAULT_MAX_IMAGE_BYTES
    try:
        parsed = int(raw.strip())
    except ValueError as exc:
        raise ValueError("GEN_AI_MAX_IMAGE_BYTES must be a positive integer") from exc
    return _validate_positive_int("GEN_AI_MAX_IMAGE_BYTES", parsed)


def _get_image_fetch_timeout_seconds() -> float:
    raw = os.environ.get("GEN_AI_IMAGE_FETCH_TIMEOUT_SECONDS")
    if raw is None or not raw.strip():
        return DEFAULT_IMAGE_FETCH_TIMEOUT_SECONDS
    try:
        parsed = float(raw.strip())
    except ValueError as exc:
        raise ValueError(
            "GEN_AI_IMAGE_FETCH_TIMEOUT_SECONDS must be a positive float"
        ) from exc
    return _validate_positive_float("GEN_AI_IMAGE_FETCH_TIMEOUT_SECONDS", parsed)


def _get_max_image_redirects() -> int:
    raw = os.environ.get("GEN_AI_MAX_IMAGE_REDIRECTS")
    if raw is None or not raw.strip():
        return DEFAULT_MAX_IMAGE_REDIRECTS
    try:
        parsed = int(raw.strip())
    except ValueError as exc:
        raise ValueError(
            f"GEN_AI_MAX_IMAGE_REDIRECTS must be an integer between 0 and {MAX_ALLOWED_IMAGE_REDIRECTS}"
        ) from exc
    return _validate_max_redirects("GEN_AI_MAX_IMAGE_REDIRECTS", parsed)


def _normalize_host_entry(raw_host: str) -> str:
    return raw_host.strip().lower().rstrip(".")


def _parse_and_validate_allowlist_entry(raw_entry: Any, source_label: str) -> str:
    if not isinstance(raw_entry, str):
        raise ValueError(f"{source_label} entries must be strings")
    trimmed = raw_entry.strip()
    if not trimmed or not trimmed.isascii() or _has_forbidden_url_chars(trimmed):
        raise ValueError(f"Invalid hostname entry in {source_label}: {raw_entry!r}")
    if trimmed.startswith("[") and trimmed.endswith("]"):
        trimmed = trimmed[1:-1]
    if (
        not trimmed
        or "%" in trimmed
        or "/" in trimmed
        or "?" in trimmed
        or "#" in trimmed
        or "@" in trimmed
        or trimmed.startswith(".")
        or trimmed.endswith("..")
    ):
        raise ValueError(f"Invalid hostname entry in {source_label}: {raw_entry!r}")

    cleaned = trimmed.lower()
    if cleaned.endswith("."):
        cleaned = cleaned[:-1]
    if not cleaned or ".." in cleaned:
        raise ValueError(f"Invalid hostname entry in {source_label}: {raw_entry!r}")

    try:
        parsed_ip = ipaddress.ip_address(cleaned)
    except ValueError:
        parsed_ip = None

    if parsed_ip is not None:
        return str(parsed_ip)

    if len(cleaned) > 253:
        raise ValueError(f"Hostname entry in {source_label} exceeds 253 characters")
    labels = cleaned.split(".")
    if any(not _DNS_LABEL_RE.fullmatch(label) for label in labels) or labels[-1].isdigit():
        raise ValueError(f"Invalid hostname entry in {source_label}: {raw_entry!r}")
    return cleaned


def _normalize_explicit_allowed_hosts(hosts: Iterable[str]) -> set[str]:
    if isinstance(hosts, (str, bytes)) or not isinstance(hosts, Iterable):
        raise ValueError("allowed_hosts must be a collection of hostname strings")
    normalized: set[str] = set()
    for item in hosts:
        normalized.add(_parse_and_validate_allowlist_entry(item, "allowed_hosts"))
    return normalized


def _get_allowed_image_hosts() -> Optional[set[str]]:
    raw = os.environ.get("GEN_AI_ALLOWED_IMAGE_HOSTS")
    if raw is None or not raw.strip():
        return None
    normalized: set[str] = set()
    for part in raw.split(","):
        if not part.strip():
            continue
        normalized.add(
            _parse_and_validate_allowlist_entry(part, "GEN_AI_ALLOWED_IMAGE_HOSTS")
        )
    if not normalized:
        raise ValueError(
            "GEN_AI_ALLOWED_IMAGE_HOSTS must contain at least one valid hostname when set"
        )
    return normalized


def max_base64_chars_for_bytes(max_bytes: int) -> int:
    """Return the maximum padded base64 character length for ``max_bytes`` raw bytes."""
    validated = _validate_positive_int("max_bytes", max_bytes)
    return ((validated + 2) // 3) * 4


def is_disallowed_ip(ip_text: str) -> bool:
    """Return ``True`` if ``ip_text`` is invalid, non-global, or in any blocked range."""
    if not isinstance(ip_text, str):
        return True
    cleaned = ip_text.strip()
    if not cleaned or "%" in cleaned:
        return True
    try:
        ip_obj = ipaddress.ip_address(cleaned)
    except ValueError:
        return True

    if (
        not ip_obj.is_global
        or ip_obj.is_private
        or ip_obj.is_loopback
        or ip_obj.is_link_local
        or ip_obj.is_multicast
        or ip_obj.is_reserved
        or ip_obj.is_unspecified
    ):
        return True

    if isinstance(ip_obj, ipaddress.IPv4Address):
        return any(ip_obj in net for net in _DISALLOWED_IPV4_NETWORKS)

    if (
        ip_obj.ipv4_mapped is not None
        or ip_obj.sixtofour is not None
        or ip_obj.teredo is not None
    ):
        return True

    return any(ip_obj in net for net in _DISALLOWED_IPV6_NETWORKS)


def detect_raster_mime_type(data: bytes) -> Optional[str]:
    """Inspect magic bytes for PNG, JPEG, GIF87a/GIF89a, or RIFF WEBP; else ``None``."""
    if not isinstance(data, (bytes, bytearray)) or len(data) < 3:
        return None
    if data.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if data.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
        return "image/webp"
    return None


def _has_forbidden_url_chars(value: str) -> bool:
    for ch in value:
        code = ord(ch)
        if ch.isspace() or code < 0x20 or 0x7F <= code <= 0x9F or ch == "\\":
            return True
    return False


def _validate_normalized_hostname(raw_hostname: str) -> Tuple[str, bool]:
    """Validate a raw hostname or IP literal and return ``(normalized_host, is_ipv6_literal)``."""
    if not raw_hostname or not raw_hostname.isascii():
        raise ImageValidationError("Image URL hostname must be non-empty ASCII")
    if "%" in raw_hostname or raw_hostname.startswith(".") or raw_hostname.endswith(".."):
        raise ImageValidationError("Invalid image URL hostname syntax")

    normalized = raw_hostname.lower()
    if normalized.endswith("."):
        normalized = normalized[:-1]
    if not normalized or ".." in normalized:
        raise ImageValidationError("Invalid image URL hostname syntax")

    if normalized in _BLOCKED_EXACT_HOSTNAMES or normalized.endswith(
        _BLOCKED_HOSTNAME_SUFFIXES
    ):
        raise ImageValidationError(f"Blocked internal image hostname: {normalized}")

    try:
        parsed_ip = ipaddress.ip_address(normalized)
    except ValueError:
        parsed_ip = None

    if parsed_ip is not None:
        if is_disallowed_ip(normalized):
            raise ImageValidationError(
                f"Disallowed IP literal in image URL: {normalized}"
            )
        return str(parsed_ip), isinstance(parsed_ip, ipaddress.IPv6Address)

    if len(normalized) > 253:
        raise ImageValidationError("Image URL hostname exceeds 253 characters")

    labels = normalized.split(".")
    if any(not _DNS_LABEL_RE.fullmatch(label) for label in labels):
        raise ImageValidationError(f"Invalid DNS hostname in image URL: {normalized}")

    # Reject numeric TLDs (blocks shorthand/decimal/octal IPv4 notations like 127.1 or 2130706433).
    if labels[-1].isdigit():
        raise ImageValidationError(
            f"Numeric TLD or non-canonical IP literal is not allowed: {normalized}"
        )

    return normalized, False


def _validate_gcs_bucket_name(bucket: str) -> None:
    if not _GCS_BUCKET_RE.fullmatch(bucket) or ".." in bucket:
        raise ImageValidationError(f"Invalid GCS bucket name in gs:// URL: {bucket!r}")
    if "." not in bucket:
        if len(bucket) > 63:
            raise ImageValidationError("Undotted GCS bucket name exceeds 63 characters")
    else:
        if len(bucket) > 222 or any(len(part) > 63 for part in bucket.split(".")):
            raise ImageValidationError("Dotted GCS bucket name exceeds GCS limits")


def validate_and_normalize_image_url(
    image_url: str,
    allowed_hosts: Optional[set[str]] = None,
) -> Tuple[str, str]:
    """Validate an ``https://`` or ``gs://`` URL and return ``(normalized_https_url, hostname)``."""
    effective_allowed_hosts: Optional[set[str]] = (
        _get_allowed_image_hosts()
        if allowed_hosts is None
        else _normalize_explicit_allowed_hosts(allowed_hosts)
    )

    if not isinstance(image_url, str) or not image_url:
        raise ImageValidationError("Image URL must be a non-empty string")
    if len(image_url) > MAX_IMAGE_URL_CHARS:
        raise ImageValidationError(
            f"Image URL exceeds maximum length of {MAX_IMAGE_URL_CHARS} characters"
        )
    if _has_forbidden_url_chars(image_url):
        raise ImageValidationError(
            "Image URL must not contain whitespace, backslashes, or control characters"
        )
    if "#" in image_url:
        raise ImageValidationError("URL fragments are not allowed in image URLs")

    lower_prefix = image_url[:8].lower()
    if lower_prefix.startswith("gs://"):
        remainder = image_url[5:]
        if "/" not in remainder:
            raise ImageValidationError("gs:// image URL must include an object path")
        bucket, raw_object = remainder.split("/", 1)
        _validate_gcs_bucket_name(bucket)

        raw_segments = raw_object.split("/")
        encoded_segments: List[str] = []
        for seg in raw_segments:
            if seg == "":
                continue
            unquoted = urllib.parse.unquote(seg)
            if seg in {".", ".."} or unquoted in {".", ".."} or "/" in unquoted or "\\" in unquoted:
                raise ImageValidationError(
                    "Path traversal segments are not allowed in gs:// image URLs"
                )
            encoded_segments.append(urllib.parse.quote(seg, safe=""))

        if not encoded_segments:
            raise ImageValidationError("gs:// image URL object path must not be empty")

        normalized_host = _GCS_STORAGE_HOST
        normalized_url = (
            f"https://{normalized_host}/{bucket}/{'/'.join(encoded_segments)}"
        )
    elif lower_prefix == "https://":
        parsed = urllib.parse.urlsplit(image_url)
        if parsed.scheme.lower() != "https" or not parsed.netloc:
            raise ImageValidationError("Invalid HTTPS image URL")
        if parsed.fragment:
            raise ImageValidationError("URL fragments are not allowed in image URLs")
        if (
            parsed.username is not None
            or parsed.password is not None
            or "@" in parsed.netloc
        ):
            raise ImageValidationError("Userinfo is not allowed in image URLs")
        if parsed.netloc.endswith(":"):
            raise ImageValidationError("Empty port is not allowed in image URLs")
        try:
            port = parsed.port
        except ValueError as exc:
            raise ImageValidationError("Invalid port in image URL") from exc
        if port is not None and port != 443:
            raise ImageValidationError("Only port 443 is allowed for HTTPS image URLs")

        raw_hostname = parsed.hostname
        if not raw_hostname:
            raise ImageValidationError("Missing hostname in image URL")
        normalized_host, is_ipv6 = _validate_normalized_hostname(raw_hostname)
        netloc_host = f"[{normalized_host}]" if is_ipv6 else normalized_host
        normalized_path = parsed.path or "/"
        normalized_url = urllib.parse.urlunsplit(
            ("https", netloc_host, normalized_path, parsed.query, "")
        )
    else:
        raise ImageValidationError("Only https:// and gs:// image URLs are allowed")

    if (
        effective_allowed_hosts is not None
        and normalized_host not in effective_allowed_hosts
    ):
        raise ImageValidationError(
            f"Host '{normalized_host}' is not in GEN_AI_ALLOWED_IMAGE_HOSTS"
        )

    return normalized_url, normalized_host


def resolve_and_validate_host_ips(
    hostname: str,
    port: int = 443,
    resolver: Optional[Callable[..., Any]] = None,
) -> List[str]:
    """Resolve ``hostname`` once and return deduplicated global IPs, rejecting if any record is non-global."""
    normalized_host, _ = _validate_normalized_hostname(hostname)
    if port != 443:
        raise ImageValidationError("Only port 443 is allowed for HTTPS image fetches")

    lookup_fn = resolver if resolver is not None else socket.getaddrinfo
    try:
        addrinfo_list = lookup_fn(normalized_host, port, type=socket.SOCK_STREAM)
    except (socket.gaierror, OSError) as exc:
        raise ImageValidationError(
            f"DNS resolution failed for host '{normalized_host}'"
        ) from exc

    if not addrinfo_list:
        raise ImageValidationError(
            f"DNS resolution returned no addresses for host '{normalized_host}'"
        )

    validated_ips: List[str] = []
    seen: set[str] = set()
    for record in addrinfo_list:
        try:
            sockaddr = record[4]
            raw_ip = sockaddr[0]
        except (IndexError, TypeError) as exc:
            raise ImageValidationError(
                f"Malformed addrinfo record while resolving '{normalized_host}'"
            ) from exc

        if is_disallowed_ip(raw_ip):
            raise ImageValidationError(
                f"Host '{normalized_host}' resolved to disallowed IP address '{raw_ip}'"
            )

        canonical_ip = str(ipaddress.ip_address(raw_ip.strip()))
        if canonical_ip not in seen:
            seen.add(canonical_ip)
            validated_ips.append(canonical_ip)

    if not validated_ips:
        raise ImageValidationError(
            f"No valid global IP addresses resolved for host '{normalized_host}'"
        )
    return validated_ips


class _PinnedIPHTTPSConnection(urllib3.connection.HTTPSConnection):
    """HTTPSConnection that connects TCP to pre-validated IPs while keeping TLS SNI/Host as ``original_hostname``."""

    pinned_ips: Tuple[str, ...] = ()
    original_hostname: str = ""

    def __init__(
        self,
        *args: Any,
        pinned_ips: Optional[Sequence[str]] = None,
        original_hostname: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        resolved_ips = tuple(
            pinned_ips if pinned_ips is not None else self.pinned_ips
        )
        if not resolved_ips:
            raise ValueError("pinned_ips must not be empty")
        super().__init__(*args, **kwargs)
        resolved_host = (
            original_hostname
            if original_hostname is not None
            else (self.original_hostname or self.host)
        )
        self._pinned_ips: Tuple[str, ...] = resolved_ips
        self._original_hostname: str = resolved_host
        self.host = resolved_host
        self.server_hostname = resolved_host

    def _new_conn(self) -> socket.socket:
        last_timeout: Optional[BaseException] = None
        last_os_error: Optional[OSError] = None

        for pinned_ip in self._pinned_ips:
            if is_disallowed_ip(pinned_ip):
                raise urllib3.exceptions.NewConnectionError(
                    self, f"Refusing TCP connection to disallowed IP: {pinned_ip}"
                )
            try:
                return urllib3.util.connection.create_connection(
                    (pinned_ip, self.port),
                    timeout=self.timeout,
                    source_address=self.source_address,
                    socket_options=self.socket_options,
                )
            except socket.timeout as exc:
                last_timeout = exc
            except OSError as exc:
                last_os_error = exc

        if last_timeout is not None:
            raise urllib3.exceptions.ConnectTimeoutError(
                self,
                f"Connection to {self.host} timed out. (connect timeout={self.timeout})",
            ) from last_timeout
        raise urllib3.exceptions.NewConnectionError(
            self,
            f"Failed to establish a new connection to {self.host}: {last_os_error}",
        ) from last_os_error


class _PinnedIPHTTPSConnectionPool(urllib3.connectionpool.HTTPSConnectionPool):
    """HTTPSConnectionPool bound to a `_PinnedIPHTTPSConnection` subclass."""

    ConnectionCls = _PinnedIPHTTPSConnection


class _PinnedIPHTTPSAdapter(requests.adapters.HTTPAdapter):
    """HTTPAdapter pinning all HTTPS connections for a single hostname to validated IPs."""

    def __init__(
        self,
        hostname: str,
        pinned_ips: Sequence[str],
        *args: Any,
        **kwargs: Any,
    ) -> None:
        normalized_host = _normalize_host_entry(hostname)
        if not normalized_host:
            raise ValueError("hostname must not be empty")
        ips_tuple = tuple(pinned_ips)
        if not ips_tuple:
            raise ValueError("pinned_ips must not be empty")
        for ip_str in ips_tuple:
            if is_disallowed_ip(ip_str):
                raise ImageValidationError(
                    f"Cannot pin disallowed IP address: {ip_str}"
                )
        self._hostname = normalized_host
        self._pinned_ips = ips_tuple
        kwargs.setdefault("max_retries", 0)
        super().__init__(*args, **kwargs)

    def init_poolmanager(
        self,
        connections: int,
        maxsize: int,
        block: bool = False,
        **pool_kwargs: Any,
    ) -> None:
        super().init_poolmanager(connections, maxsize, block=block, **pool_kwargs)
        bound_hostname = self._hostname
        bound_ips = self._pinned_ips

        class _BoundPinnedHTTPSConnection(_PinnedIPHTTPSConnection):
            pinned_ips = bound_ips
            original_hostname = bound_hostname

        class _BoundPinnedHTTPSConnectionPool(_PinnedIPHTTPSConnectionPool):
            ConnectionCls = _BoundPinnedHTTPSConnection

        # Assign a fresh dict so urllib3's module-level pool_classes_by_scheme is never mutated.
        self.poolmanager.pool_classes_by_scheme = {
            "https": _BoundPinnedHTTPSConnectionPool
        }

    def send(
        self,
        request: requests.PreparedRequest,
        stream: bool = False,
        timeout: Any = None,
        verify: Any = True,
        cert: Any = None,
        proxies: Any = None,
    ) -> requests.Response:
        if verify is False:
            raise requests.exceptions.SSLError(
                "TLS certificate verification cannot be disabled for pinned image fetches"
            )
        parsed = urllib.parse.urlsplit(request.url or "")
        req_host = _normalize_host_entry(parsed.hostname or "")
        if (
            parsed.scheme.lower() != "https"
            or req_host != self._hostname
            or parsed.port not in (None, 443)
        ):
            raise requests.exceptions.InvalidURL(
                f"Request URL does not match pinned HTTPS target '{self._hostname}'"
            )
        return super().send(
            request,
            stream=stream,
            timeout=timeout,
            verify=verify,
            cert=cert,
            proxies={},
        )


def _create_pinned_https_session(
    hostname: str, validated_ips: List[str]
) -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.max_redirects = 0
    session.adapters.clear()
    session.mount(
        "https://",
        _PinnedIPHTTPSAdapter(
            hostname=hostname,
            pinned_ips=validated_ips,
            pool_connections=1,
            pool_maxsize=1,
        ),
    )
    return session


def _redact_url_for_log(raw_url: Any) -> str:
    if not isinstance(raw_url, str):
        return "<non-string-url>"
    sanitized = "".join(
        ch if 0x20 <= ord(ch) < 0x7F else "?" for ch in raw_url[:256]
    )
    try:
        parsed = urllib.parse.urlsplit(sanitized)
    except ValueError:
        return sanitized[:128]
    if not parsed.scheme or not parsed.netloc:
        return sanitized[:128]
    host_part = parsed.hostname or "<invalid-host>"
    if ":" in host_part and not host_part.startswith("["):
        host_part = f"[{host_part}]"
    query_suffix = "?<redacted>" if parsed.query else ""
    return f"{parsed.scheme.lower()}://{host_part}{parsed.path[:128]}{query_suffix}"


def _read_bounded_raster_response(
    response: requests.Response,
    max_bytes: int,
) -> Tuple[bytes, str]:
    raw_content_type = response.headers.get("Content-Type", "")
    media_type = raw_content_type.split(";", 1)[0].strip().lower()
    if media_type not in ALLOWED_IMAGE_CONTENT_TYPES:
        raise ImageValidationError(
            f"Disallowed image Content-Type: {media_type or '<missing>'}"
        )
    canonical_header_mime = (
        "image/jpeg" if media_type == "image/jpg" else media_type
    )

    raw_content_length = response.headers.get("Content-Length")
    if raw_content_length is not None and raw_content_length.strip() != "":
        try:
            declared_length = int(raw_content_length.strip())
        except ValueError as exc:
            raise ImageValidationError("Invalid Content-Length header") from exc
        if declared_length < 0:
            raise ImageValidationError("Negative Content-Length header")
        if declared_length == 0:
            raise ImageValidationError("Empty image response")
        if declared_length > max_bytes:
            raise ImageTooLargeError("Image exceeds maximum allowed size")

    chunks: List[bytes] = []
    total_bytes = 0
    for chunk in response.iter_content(chunk_size=STREAM_CHUNK_BYTES):
        if not chunk:
            continue
        total_bytes += len(chunk)
        if total_bytes > max_bytes:
            raise ImageTooLargeError("Image exceeds maximum allowed size")
        chunks.append(chunk)

    if total_bytes == 0:
        raise ImageValidationError("Empty image response")

    image_bytes = b"".join(chunks)
    detected_mime = detect_raster_mime_type(image_bytes)
    if detected_mime is None or detected_mime != canonical_header_mime:
        raise ImageValidationError(
            "Image payload magic bytes do not match declared raster Content-Type"
        )
    return image_bytes, detected_mime


def fetch_validated_product_image(
    image_url: str,
    *,
    max_bytes: Optional[int] = None,
    timeout_seconds: Optional[float] = None,
    max_redirects: Optional[int] = None,
    allowed_hosts: Optional[set[str]] = None,
    resolver: Optional[Callable[..., Any]] = None,
    session_factory: Optional[Callable[[str, List[str]], requests.Session]] = None,
) -> Optional[Tuple[bytes, str]]:
    """Fetch a validated product image with pinned-IP TLS, streaming size bounds, and magic-byte checks.

    Configuration env vars are validated up front so misconfiguration ``ValueError``
    exceptions fail fast rather than being swallowed as untrusted URL errors.
    """
    resolved_max_bytes = (
        get_max_image_bytes()
        if max_bytes is None
        else _validate_positive_int("max_bytes", max_bytes)
    )
    resolved_timeout = (
        _get_image_fetch_timeout_seconds()
        if timeout_seconds is None
        else _validate_positive_float("timeout_seconds", timeout_seconds)
    )
    resolved_max_redirects = (
        _get_max_image_redirects()
        if max_redirects is None
        else _validate_max_redirects("max_redirects", max_redirects)
    )
    effective_allowed_hosts: Optional[set[str]] = (
        _get_allowed_image_hosts()
        if allowed_hosts is None
        else _normalize_explicit_allowed_hosts(allowed_hosts)
    )

    timeout_tuple = (
        min(MAX_CONNECT_TIMEOUT_SECONDS, resolved_timeout),
        resolved_timeout,
    )
    build_session = (
        session_factory
        if session_factory is not None
        else _create_pinned_https_session
    )

    try:
        current_url = image_url
        for hop in range(resolved_max_redirects + 1):
            normalized_url, normalized_host = validate_and_normalize_image_url(
                current_url,
                allowed_hosts=effective_allowed_hosts,
            )
            validated_ips = resolve_and_validate_host_ips(
                normalized_host,
                port=443,
                resolver=resolver,
            )

            session = build_session(normalized_host, validated_ips)
            try:
                with session.get(
                    normalized_url,
                    stream=True,
                    allow_redirects=False,
                    timeout=timeout_tuple,
                ) as response:
                    status_code = response.status_code
                    if 300 <= status_code < 400:
                        if hop >= resolved_max_redirects:
                            raise ImageValidationError(
                                f"HTTP {status_code} redirect disallowed (max_redirects={resolved_max_redirects})"
                            )
                        location = response.headers.get("Location")
                        if (
                            not location
                            or _has_forbidden_url_chars(location)
                            or "#" in location
                            or location.lower().startswith("gs:")
                        ):
                            raise ImageValidationError(
                                "Invalid or disallowed redirect Location header"
                            )
                        current_url = urllib.parse.urljoin(normalized_url, location)
                        continue

                    if status_code != 200:
                        raise ImageValidationError(
                            f"Unexpected HTTP status {status_code} fetching product image"
                        )

                    return _read_bounded_raster_response(
                        response,
                        max_bytes=resolved_max_bytes,
                    )
            finally:
                close_fn = getattr(session, "close", None)
                if callable(close_fn):
                    close_fn()

        raise ImageValidationError("Exceeded maximum allowed image redirects")
    except (
        ImageValidationError,
        requests.RequestException,
        urllib3.exceptions.HTTPError,
        OSError,
    ) as exc:
        logger.warning(
            "Rejected or failed product image fetch (%s): %s",
            _redact_url_for_log(image_url),
            exc,
        )
        return None


def decode_bounded_base64_image(
    image_base64: str,
    *,
    max_bytes: Optional[int] = None,
) -> Tuple[bytes, str]:
    """Strictly validate and decode bounded base64 image data, returning ``(bytes, mime_type)``."""
    resolved_max_bytes = (
        get_max_image_bytes()
        if max_bytes is None
        else _validate_positive_int("max_bytes", max_bytes)
    )
    if not isinstance(image_base64, str):
        raise ImageValidationError("Invalid base64 image format")

    stripped = image_base64.strip()
    if not stripped:
        raise ImageValidationError("Invalid base64 image format")

    if stripped[:5].lower() == "data:":
        prefix_match = _DATA_IMAGE_BASE64_PREFIX_RE.match(stripped)
        if prefix_match is None:
            raise ImageValidationError("Invalid base64 image format")
        stripped = stripped[prefix_match.end() :]
        if not stripped:
            raise ImageValidationError("Invalid base64 image format")

    max_encoded_chars = max_base64_chars_for_bytes(resolved_max_bytes)
    if len(stripped) > max_encoded_chars:
        raise ImageTooLargeError("Image exceeds maximum allowed size")

    if (
        not stripped.isascii()
        or len(stripped) < 4
        or len(stripped) % 4 != 0
        or "=" in stripped[:-2]
        or (stripped[-2] == "=" and stripped[-1] != "=")
    ):
        raise ImageValidationError("Invalid base64 image format")

    try:
        decoded = base64.b64decode(stripped, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ImageValidationError("Invalid base64 image format") from exc

    if not decoded:
        raise ImageValidationError("Invalid base64 image format")

    if len(decoded) > resolved_max_bytes:
        raise ImageTooLargeError("Image exceeds maximum allowed size")

    mime_type = detect_raster_mime_type(decoded) or "image/png"
    return decoded, mime_type


__all__ = [
    "ALLOWED_IMAGE_CONTENT_TYPES",
    "DEFAULT_MAX_IMAGE_BYTES",
    "ImageTooLargeError",
    "ImageValidationError",
    "MAX_IMAGE_URL_CHARS",
    "STREAM_CHUNK_BYTES",
    "decode_bounded_base64_image",
    "detect_raster_mime_type",
    "fetch_validated_product_image",
    "get_max_image_bytes",
    "is_disallowed_ip",
    "max_base64_chars_for_bytes",
    "resolve_and_validate_host_ips",
    "validate_and_normalize_image_url",
]

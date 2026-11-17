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

"""Unit tests for SSRF-safe, pinned-IP, size-bounded image_fetcher helpers."""

import base64
import os
from pathlib import Path
import socket
import ssl
import subprocess
import sys
import threading
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
import urllib3.connection
import urllib3.util.connection

import pytest
import requests

try:
    import psearch.gen_ai.services.image_fetcher as image_fetcher
    from psearch.gen_ai.services.image_fetcher import (
        ALLOWED_IMAGE_CONTENT_TYPES,
        DEFAULT_MAX_IMAGE_BYTES,
        ImageTooLargeError,
        ImageValidationError,
        MAX_IMAGE_URL_CHARS,
        STREAM_CHUNK_BYTES,
        decode_bounded_base64_image,
        detect_raster_mime_type,
        fetch_validated_product_image,
        get_max_image_bytes,
        is_disallowed_ip,
        max_base64_chars_for_bytes,
        resolve_and_validate_host_ips,
        validate_and_normalize_image_url,
    )
except ModuleNotFoundError as exc:
    if exc.name not in {
        "psearch",
        "psearch.gen_ai",
        "psearch.gen_ai.services",
    }:
        raise
    import gen_ai.services.image_fetcher as image_fetcher
    from gen_ai.services.image_fetcher import (
        ALLOWED_IMAGE_CONTENT_TYPES,
        DEFAULT_MAX_IMAGE_BYTES,
        ImageTooLargeError,
        ImageValidationError,
        MAX_IMAGE_URL_CHARS,
        STREAM_CHUNK_BYTES,
        decode_bounded_base64_image,
        detect_raster_mime_type,
        fetch_validated_product_image,
        get_max_image_bytes,
        is_disallowed_ip,
        max_base64_chars_for_bytes,
        resolve_and_validate_host_ips,
        validate_and_normalize_image_url,
    )


_REPO_ROOT = Path(__file__).resolve().parents[4]
_SRC_DIR = _REPO_ROOT / "src"
_PSEARCH_DIR = _SRC_DIR / "psearch"

SAMPLE_PNG = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01"
SAMPLE_JPEG = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x00\x00\x01\xff\xd9"
SAMPLE_GIF87A = b"GIF87a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff,"
SAMPLE_GIF89A = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\x00\x00\x00\xff\xff\xff;"
SAMPLE_WEBP = b"RIFF\x1a\x00\x00\x00WEBPVP8 \x0e\x00\x00\x00\x30\x01\x00\x9d\x01\x2a"

_IMAGE_ENV_VARS = (
    "GEN_AI_MAX_IMAGE_BYTES",
    "GEN_AI_IMAGE_FETCH_TIMEOUT_SECONDS",
    "GEN_AI_MAX_IMAGE_REDIRECTS",
    "GEN_AI_ALLOWED_IMAGE_HOSTS",
)


@pytest.fixture(autouse=True)
def _clean_image_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in _IMAGE_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def _addrinfo_v4(ip: str, port: int = 443) -> Tuple[int, int, int, str, Tuple[str, int]]:
    return (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, "", (ip, port))


def _addrinfo_v6(
    ip: str, port: int = 443
) -> Tuple[int, int, int, str, Tuple[str, int, int, int]]:
    return (
        socket.AF_INET6,
        socket.SOCK_STREAM,
        socket.IPPROTO_TCP,
        "",
        (ip, port, 0, 0),
    )


class _FakeStreamResponse:
    """Minimal context-manager HTTP response double for unit testing streaming/redirects."""

    def __init__(
        self,
        *,
        status_code: int = 200,
        headers: Optional[Dict[str, str]] = None,
        chunks: Optional[Iterable[bytes]] = None,
    ) -> None:
        self.status_code = status_code
        self.headers = requests.structures.CaseInsensitiveDict(headers or {})
        self._chunks = list(chunks or [])
        self.closed = False
        self.iter_content_called = False
        self.chunks_yielded = 0

    def iter_content(self, chunk_size: int = 1) -> Iterable[bytes]:
        self.iter_content_called = True
        assert chunk_size == STREAM_CHUNK_BYTES
        for chunk in self._chunks:
            self.chunks_yielded += 1
            yield chunk

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> "_FakeStreamResponse":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()


class _FakeSession:
    """Fake requests.Session recording .get() arguments and closing state."""

    def __init__(self, handler: Callable[..., _FakeStreamResponse]) -> None:
        self._handler = handler
        self.calls: List[Dict[str, Any]] = []
        self.closed = False

    def get(self, url: str, **kwargs: Any) -> _FakeStreamResponse:
        self.calls.append({"url": url, **kwargs})
        return self._handler(url, **kwargs)

    def close(self) -> None:
        self.closed = True


def test_public_constants_and_exception_hierarchy() -> None:
    assert DEFAULT_MAX_IMAGE_BYTES == 10 * 1024 * 1024
    assert MAX_IMAGE_URL_CHARS == 2048
    assert STREAM_CHUNK_BYTES == 65536
    assert isinstance(ALLOWED_IMAGE_CONTENT_TYPES, frozenset)
    assert ALLOWED_IMAGE_CONTENT_TYPES == frozenset(
        {"image/jpeg", "image/jpg", "image/png", "image/gif", "image/webp"}
    )
    assert issubclass(ImageValidationError, ValueError)
    assert issubclass(ImageTooLargeError, ImageValidationError)

    assert max_base64_chars_for_bytes(1) == 4
    assert max_base64_chars_for_bytes(2) == 4
    assert max_base64_chars_for_bytes(3) == 4
    assert max_base64_chars_for_bytes(4) == 8
    assert max_base64_chars_for_bytes(DEFAULT_MAX_IMAGE_BYTES) == 13_981_016

    for bad in (0, -1, True):
        with pytest.raises(ValueError):
            max_base64_chars_for_bytes(bad)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("raw_url", "expected_url", "expected_host"),
    [
        (
            "https://cdn.example.com/images/shoe.png",
            "https://cdn.example.com/images/shoe.png",
            "cdn.example.com",
        ),
        (
            "HTTPS://CDN.Example.COM:443/path/to/item.jpg?size=large",
            "https://cdn.example.com/path/to/item.jpg?size=large",
            "cdn.example.com",
        ),
        (
            "https://cdn.example.com./img.webp",
            "https://cdn.example.com/img.webp",
            "cdn.example.com",
        ),
        (
            "gs://my-product-bucket/catalog/2026/shoe.png",
            "https://storage.googleapis.com/my-product-bucket/catalog/2026/shoe.png",
            "storage.googleapis.com",
        ),
        (
            "gs://my.dotted-bucket_1/path/a?.png",
            "https://storage.googleapis.com/my.dotted-bucket_1/path/a%3F.png",
            "storage.googleapis.com",
        ),
        (
            "gs://my-bucket//nested///item&v=1.png",
            "https://storage.googleapis.com/my-bucket/nested/item%26v%3D1.png",
            "storage.googleapis.com",
        ),
    ],
)
def test_validate_and_normalize_image_url_accepts_valid_https_and_gs(
    raw_url: str, expected_url: str, expected_host: str
) -> None:
    normalized_url, normalized_host = validate_and_normalize_image_url(raw_url)
    assert normalized_url == expected_url
    assert normalized_host == expected_host


@pytest.mark.parametrize(
    "bad_url",
    [
        "",
        "   ",
        "http://cdn.example.com/a.png",
        "file:///etc/passwd",
        "ftp://cdn.example.com/a.png",
        "gopher://cdn.example.com:70/1",
        "data:image/png;base64,YWJj",
        "cdn.example.com/a.png",
        "//cdn.example.com/a.png",
        "https:cdn.example.com/a.png",
        "https://user:pass@example.com/a.png",
        "https://user@example.com/a.png",
        "https://@example.com/a.png",
        "https://example.com/a.png#x",
        "https://example.com/a.png#",
        "gs://my-bucket/a.png#frag",
        "https://example.com:8443/a.png",
        "https://example.com:80/a.png",
        "https://example.com:abc/a.png",
        "https://example.com/" + ("a" * 2048),
        " https://example.com/a.png",
        "https://example.com/a.png ",
        "https://example.com/a b.png",
        "https://example.com/a.png\r\nHost: evil",
        "https://example.com/a\x00.png",
        "https://example.com/a\x7f.png",
        "https://example.com/a\x80.png",
        "https://example.com/a.png?q=\x9b1",
        "gs://my-bucket/a\x80.png",
        "gs://my-bucket/path/\x9fitem.png",
        "https://evil.example.com\\@good.example.com/a.png",
        # gs:// path traversal & bucket validation failures
        "gs://my-bucket/../secret.png",
        "gs://my-bucket/a/../secret.png",
        "gs://my-bucket/./secret.png",
        "gs://my-bucket/%2e%2e/secret.png",
        "gs://my-bucket",
        "gs://my-bucket/",
        "gs://my-bucket///",
        "gs://MyUpperBucket/a.png",
        "gs://ab/a.png",
        "gs://my..bucket/a.png",
        "gs://" + ("a" * 64) + "/a.png",
        "gs://" + ".".join(["a" * 60] * 4) + "/a.png",
        # Blocked internal / metadata hostnames
        "https://localhost/a.png",
        "https://LOCALHOST./a.png",
        "https://sub.localhost/a.png",
        "https://metadata.google.internal/computeMetadata/v1/",
        "https://metadata.google.internal./computeMetadata/v1/",
        "https://sub.google.internal/a.png",
        "https://metadata/computeMetadata/v1/",
        "https://foo.internal/a.png",
        "https://bar.local/a.png",
        # Direct private/metadata/loopback/numeric-trick IP literals
        "https://169.254.169.254/latest/meta-data/",
        "https://127.0.0.1/a.png",
        "https://[::1]/a.png",
        "https://[::ffff:169.254.169.254]/a.png",
        "https://[fd00:ec2::254]/latest/meta-data/",
        "https://127.1/a.png",
        "https://2130706433/a.png",
        "https://0177.0.0.1/a.png",
    ],
)
def test_validate_and_normalize_image_url_rejects_disallowed_urls(bad_url: str) -> None:
    with pytest.raises(ImageValidationError):
        validate_and_normalize_image_url(bad_url)


def test_allowed_image_hosts_env_and_explicit_filtering(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "GEN_AI_ALLOWED_IMAGE_HOSTS",
        " CDN.Example.COM. , storage.googleapis.com ",
    )

    url, host = validate_and_normalize_image_url("https://cdn.example.com/item.png")
    assert url == "https://cdn.example.com/item.png"
    assert host == "cdn.example.com"

    gs_url, gs_host = validate_and_normalize_image_url("gs://my-bucket/item.png")
    assert gs_url == "https://storage.googleapis.com/my-bucket/item.png"
    assert gs_host == "storage.googleapis.com"

    with pytest.raises(ImageValidationError):
        validate_and_normalize_image_url("https://untrusted-cdn.example.org/item.png")

    # Even if misconfigured in GEN_AI_ALLOWED_IMAGE_HOSTS, internal hosts remain blocked.
    monkeypatch.setenv(
        "GEN_AI_ALLOWED_IMAGE_HOSTS",
        "localhost,metadata.google.internal",
    )
    with pytest.raises(ImageValidationError):
        validate_and_normalize_image_url("https://localhost/item.png")
    with pytest.raises(ImageValidationError):
        validate_and_normalize_image_url(
            "https://metadata.google.internal/computeMetadata/v1/"
        )

    # Explicit non-empty allowed_hosts normalizes entries and overrides env allowlist.
    monkeypatch.setenv("GEN_AI_ALLOWED_IMAGE_HOSTS", "env-only.example.com")
    exp_url, exp_host = validate_and_normalize_image_url(
        "https://Explicit.Example.ORG./item.png",
        allowed_hosts={" Explicit.Example.ORG. "},
    )
    assert exp_url == "https://explicit.example.org/item.png"
    assert exp_host == "explicit.example.org"

    with pytest.raises(ImageValidationError):
        validate_and_normalize_image_url(
            "https://env-only.example.com/item.png",
            allowed_hosts={"explicit.example.org"},
        )

    assert fetch_validated_product_image(
        "https://explicit.example.org/item.png",
        allowed_hosts={"Explicit.Example.ORG."},
        resolver=lambda h, p, type=0: [_addrinfo_v4("93.184.216.34", p)],
        session_factory=lambda h, ips: _FakeSession(
            lambda u, **kw: _FakeStreamResponse(
                status_code=200,
                headers={"Content-Type": "image/png"},
                chunks=[SAMPLE_PNG],
            )
        ),
    ) == (SAMPLE_PNG, "image/png")

    # Explicit empty set() MUST fail closed (reject all hosts), even when env var is set or unset.
    with pytest.raises(ImageValidationError):
        validate_and_normalize_image_url(
            "https://env-only.example.com/item.png",
            allowed_hosts=set(),
        )
    assert (
        fetch_validated_product_image(
            "https://env-only.example.com/item.png",
            allowed_hosts=set(),
            resolver=lambda h, p, type=0: [_addrinfo_v4("93.184.216.34", p)],
            session_factory=lambda h, ips: _FakeSession(
                lambda u, **kw: _FakeStreamResponse(
                    status_code=200,
                    headers={"Content-Type": "image/png"},
                    chunks=[SAMPLE_PNG],
                )
            ),
        )
        is None
    )

    monkeypatch.delenv("GEN_AI_ALLOWED_IMAGE_HOSTS", raising=False)
    with pytest.raises(ImageValidationError):
        validate_and_normalize_image_url(
            "https://cdn.example.com/item.png",
            allowed_hosts=set(),
        )
    assert (
        fetch_validated_product_image(
            "https://cdn.example.com/item.png",
            allowed_hosts=set(),
            resolver=lambda h, p, type=0: [_addrinfo_v4("93.184.216.34", p)],
            session_factory=lambda h, ips: _FakeSession(
                lambda u, **kw: _FakeStreamResponse(
                    status_code=200,
                    headers={"Content-Type": "image/png"},
                    chunks=[SAMPLE_PNG],
                )
            ),
        )
        is None
    )

    # Delimiter-only or malformed GEN_AI_ALLOWED_IMAGE_HOSTS values must fail fast with ValueError.
    for bad_hosts_env in (
        ",",
        " , , ",
        " . ",
        "cdn.example.com, . ",
        "https://cdn.example.com",
        "cdn.example.com:443",
        "bad..example.com",
        "bad host.example.com",
    ):
        monkeypatch.setenv("GEN_AI_ALLOWED_IMAGE_HOSTS", bad_hosts_env)
        with pytest.raises(ValueError) as exc_url:
            validate_and_normalize_image_url("https://cdn.example.com/item.png")
        assert not isinstance(exc_url.value, ImageValidationError)

        with pytest.raises(ValueError) as exc_fetch:
            fetch_validated_product_image("https://cdn.example.com/item.png")
        assert not isinstance(exc_fetch.value, ImageValidationError)

    monkeypatch.delenv("GEN_AI_ALLOWED_IMAGE_HOSTS", raising=False)

    # Malformed entries in explicit allowed_hosts also raise ValueError up front.
    for bad_explicit in ({" . "}, {"https://cdn.example.com"}, {""}):
        with pytest.raises(ValueError) as exc_exp:
            fetch_validated_product_image(
                "https://cdn.example.com/item.png",
                allowed_hosts=bad_explicit,
            )
        assert not isinstance(exc_exp.value, ImageValidationError)


@pytest.mark.parametrize(
    "blocked_ip",
    [
        "127.0.0.1",
        "127.0.0.53",
        "::1",
        "10.0.0.1",
        "172.16.0.1",
        "172.31.255.254",
        "192.168.1.1",
        "169.254.169.254",
        "fe80::1",
        "100.64.0.1",
        "100.100.100.200",
        "fd00:ec2::254",
        "fc00::1234",
        "::ffff:169.254.169.254",
        "::ffff:127.0.0.1",
        "::ffff:8.8.8.8",
        "64:ff9b::a9fe:a9fe",
        "64:ff9b::0808:0808",
        "64:ff9b:1::a9fe:a9fe",
        "2002:7f00:0001::1",
        "2002:0808:0808::1",
        "2001:0000:4136:e378:8000:63bf:3fff:fdd2",
        "::127.0.0.1",
        "::ffff:0:127.0.0.1",
        "0.0.0.0",
        "::",
        "224.0.0.1",
        "ff02::1",
        "240.0.0.1",
        "255.255.255.255",
        "198.18.0.1",
        "192.0.2.1",
        "198.51.100.1",
        "203.0.113.1",
        "2001:db8::1",
        "fe80::1%lo0",
        "not-an-ip",
        "",
    ],
)
def test_is_disallowed_ip_blocks_non_global_and_transition_addresses(
    blocked_ip: str,
) -> None:
    assert is_disallowed_ip(blocked_ip) is True


@pytest.mark.parametrize(
    "public_ip",
    [
        "8.8.8.8",
        "1.1.1.1",
        "93.184.216.34",
        "2606:4700:4700::1111",
        "2607:f8b0:4005:801::201b",
    ],
)
def test_is_disallowed_ip_allows_global_unicast(public_ip: str) -> None:
    assert is_disallowed_ip(public_ip) is False


def test_resolve_and_validate_host_ips_deduplicates_and_rejects_mixed_or_empty() -> None:
    ips = resolve_and_validate_host_ips(
        "cdn.example.com",
        resolver=lambda host, port, type=0: [
            _addrinfo_v4("93.184.216.34", port),
            _addrinfo_v4("93.184.216.34", port),
            _addrinfo_v6("2606:4700:4700::1111", port),
        ],
    )
    assert ips == ["93.184.216.34", "2606:4700:4700::1111"]

    # Empty DNS result must fail closed
    with pytest.raises(ImageValidationError):
        resolve_and_validate_host_ips(
            "cdn.example.com",
            resolver=lambda host, port, type=0: [],
        )

    # DNS error must wrap into ImageValidationError
    def _failing_resolver(host: str, port: int, type: int = 0) -> Any:
        raise socket.gaierror(socket.EAI_NONAME, "Name or service not known")

    with pytest.raises(ImageValidationError):
        resolve_and_validate_host_ips("cdn.example.com", resolver=_failing_resolver)

    # Mixed public + private multi-record response MUST reject entire host
    for poisoned_record in (
        _addrinfo_v4("10.0.0.8"),
        _addrinfo_v4("169.254.169.254"),
        _addrinfo_v6("::ffff:169.254.169.254"),
        _addrinfo_v6("64:ff9b::a9fe:a9fe"),
    ):
        with pytest.raises(ImageValidationError):
            resolve_and_validate_host_ips(
                "dual.example.com",
                resolver=lambda host, port, type=0, rec=poisoned_record: [
                    _addrinfo_v4("93.184.216.34", port),
                    rec,
                ],
            )


def test_pinned_ip_https_adapter_pins_tcp_socket_and_preserves_tls_sni(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dns_calls: List[Tuple[str, int, int]] = []

    def _single_shot_getaddrinfo(
        host: str,
        port: Any,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> List[Any]:
        dns_calls.append((host, int(port), type))
        if len(dns_calls) > 1:
            raise AssertionError("Secondary DNS lookup attempted (TOCTOU vulnerability)")
        return [_addrinfo_v4("93.184.216.34", int(port))]

    monkeypatch.setattr(socket, "getaddrinfo", _single_shot_getaddrinfo)

    connect_calls: List[Tuple[Tuple[str, int], Any]] = []
    tls_calls: List[Dict[str, Any]] = []
    captured_http_requests: List[bytes] = []
    created_client_sockets: List[socket.socket] = []

    def _fake_create_connection(
        address: Tuple[str, int],
        timeout: Any = None,
        source_address: Any = None,
        socket_options: Any = None,
    ) -> socket.socket:
        connect_calls.append((address, timeout))
        client_sock, server_sock = socket.socketpair()
        created_client_sockets.append(client_sock)

        def _serve_once() -> None:
            try:
                req_data = server_sock.recv(4096)
                captured_http_requests.append(req_data)
                http_resp = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: image/png\r\n"
                    + f"Content-Length: {len(SAMPLE_PNG)}\r\n".encode("ascii")
                    + b"Connection: close\r\n\r\n"
                    + SAMPLE_PNG
                )
                server_sock.sendall(http_resp)
            finally:
                server_sock.close()

        worker = threading.Thread(target=_serve_once, daemon=True)
        worker.start()
        return client_sock

    def _fake_ssl_wrap_and_match(
        sock: socket.socket, **kwargs: Any
    ) -> Tuple[socket.socket, bool]:
        tls_calls.append(kwargs)
        return sock, True

    monkeypatch.setattr(
        urllib3.util.connection, "create_connection", _fake_create_connection
    )
    monkeypatch.setattr(
        urllib3.connection,
        "_ssl_wrap_socket_and_match_hostname",
        _fake_ssl_wrap_and_match,
    )

    result = fetch_validated_product_image("https://cdn.example.com/products/shoe.png")
    assert result == (SAMPLE_PNG, "image/png")

    # 1. DNS resolved strictly once
    assert dns_calls == [("cdn.example.com", 443, socket.SOCK_STREAM)]
    # 2. TCP destination was pinned to the validated IP on port 443 (never hostname)
    assert len(connect_calls) == 1
    assert connect_calls[0][0] == ("93.184.216.34", 443)
    # 3. TLS SNI & hostname verification used original hostname "cdn.example.com"
    assert len(tls_calls) == 1
    assert tls_calls[0]["server_hostname"] == "cdn.example.com"
    assert tls_calls[0]["cert_reqs"] in ("CERT_REQUIRED", ssl.CERT_REQUIRED)
    # 4. HTTP Host header preserved original hostname
    assert len(captured_http_requests) == 1
    assert b"GET /products/shoe.png HTTP/1.1\r\n" in captured_http_requests[0]
    assert b"\r\nhost: cdn.example.com\r\n" in captured_http_requests[0].lower()
    # 5. Underlying socket closed cleanly on exit
    assert created_client_sockets[0].fileno() == -1


def test_pinned_ip_https_adapter_fails_closed_on_tls_cert_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda host, port, *a, **kw: [_addrinfo_v4("93.184.216.34", int(port))],
    )

    client_sockets: List[socket.socket] = []

    def _fake_create_connection(
        address: Tuple[str, int],
        timeout: Any = None,
        source_address: Any = None,
        socket_options: Any = None,
    ) -> socket.socket:
        c_sock, s_sock = socket.socketpair()
        s_sock.close()
        client_sockets.append(c_sock)
        return c_sock

    def _failing_ssl_wrap(
        sock: socket.socket, **kwargs: Any
    ) -> Tuple[socket.socket, bool]:
        sock.close()
        raise ssl.SSLCertVerificationError(
            1, "hostname 'cdn.example.com' doesn't match 'attacker.example'"
        )

    monkeypatch.setattr(
        urllib3.util.connection, "create_connection", _fake_create_connection
    )
    monkeypatch.setattr(
        urllib3.connection,
        "_ssl_wrap_socket_and_match_hostname",
        _failing_ssl_wrap,
    )

    assert fetch_validated_product_image("https://cdn.example.com/a.png") is None
    assert client_sockets[0].fileno() == -1


def test_redirects_disabled_by_default_and_revalidated_when_opted_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sessions_created: List[Tuple[str, List[str], _FakeSession]] = []

    # 1. Default (GEN_AI_MAX_IMAGE_REDIRECTS=0): 302 is rejected without following Location
    def _factory_default(host: str, ips: List[str]) -> Any:
        sess = _FakeSession(
            lambda url, **kw: _FakeStreamResponse(
                status_code=302,
                headers={"Location": "https://cdn2.example.com/final.png"},
            )
        )
        sessions_created.append((host, ips, sess))
        return sess

    res_default = fetch_validated_product_image(
        "https://cdn1.example.com/start.png",
        resolver=lambda host, port, type=0: [_addrinfo_v4("93.184.216.34", port)],
        session_factory=_factory_default,
    )
    assert res_default is None
    assert len(sessions_created) == 1
    assert sessions_created[0][2].calls[0]["allow_redirects"] is False
    assert sessions_created[0][2].closed is True

    # 2. Opt-in GEN_AI_MAX_IMAGE_REDIRECTS=2: blocks redirect to http://169.254.169.254/
    monkeypatch.setenv("GEN_AI_MAX_IMAGE_REDIRECTS", "2")
    ssrf_hops: List[str] = []

    def _factory_ssrf_redirect(host: str, ips: List[str]) -> Any:
        ssrf_hops.append(host)
        return _FakeSession(
            lambda url, **kw: _FakeStreamResponse(
                status_code=302,
                headers={"Location": "http://169.254.169.254/latest/meta-data/"},
            )
        )

    assert (
        fetch_validated_product_image(
            "https://cdn1.example.com/start.png",
            resolver=lambda host, port, type=0: [_addrinfo_v4("93.184.216.34", port)],
            session_factory=_factory_ssrf_redirect,
        )
        is None
    )
    assert ssrf_hops == ["cdn1.example.com"]

    # 3. Opt-in GEN_AI_MAX_IMAGE_REDIRECTS=2: blocks redirect to public URL whose DNS resolves to private IP
    def _resolver_rebind(host: str, port: int, type: int = 0) -> List[Any]:
        if host == "cdn1.example.com":
            return [_addrinfo_v4("93.184.216.34", port)]
        return [_addrinfo_v4("169.254.169.254", port)]

    rebind_sessions: List[str] = []

    def _factory_rebind(host: str, ips: List[str]) -> Any:
        rebind_sessions.append(host)
        return _FakeSession(
            lambda url, **kw: _FakeStreamResponse(
                status_code=302,
                headers={"Location": "https://rebind.example.org/img.png"},
            )
        )

    assert (
        fetch_validated_product_image(
            "https://cdn1.example.com/start.png",
            resolver=_resolver_rebind,
            session_factory=_factory_rebind,
        )
        is None
    )
    assert rebind_sessions == ["cdn1.example.com"]

    # 4. Opt-in GEN_AI_MAX_IMAGE_REDIRECTS=2: allows redirect to validated public HTTPS host
    hop_records: List[Tuple[str, List[str]]] = []

    def _resolver_two_public(host: str, port: int, type: int = 0) -> List[Any]:
        if host == "cdn1.example.com":
            return [_addrinfo_v4("93.184.216.34", port)]
        if host == "cdn2.example.com":
            return [_addrinfo_v4("1.1.1.1", port)]
        raise AssertionError(f"Unexpected host {host}")

    def _factory_valid_redirect(host: str, ips: List[str]) -> Any:
        hop_records.append((host, list(ips)))
        if host == "cdn1.example.com":
            return _FakeSession(
                lambda url, **kw: _FakeStreamResponse(
                    status_code=302,
                    headers={"Location": "https://cdn2.example.com/real.jpg"},
                )
            )
        return _FakeSession(
            lambda url, **kw: _FakeStreamResponse(
                status_code=200,
                headers={
                    "Content-Type": "image/jpeg",
                    "Content-Length": str(len(SAMPLE_JPEG)),
                },
                chunks=[SAMPLE_JPEG],
            )
        )

    ok_redirect = fetch_validated_product_image(
        "https://cdn1.example.com/start.png",
        resolver=_resolver_two_public,
        session_factory=_factory_valid_redirect,
    )
    assert ok_redirect == (SAMPLE_JPEG, "image/jpeg")
    assert hop_records == [
        ("cdn1.example.com", ["93.184.216.34"]),
        ("cdn2.example.com", ["1.1.1.1"]),
    ]


def test_streaming_byte_cap_enforced_via_content_length_and_chunked_body() -> None:
    # 1. Oversized Content-Length aborts BEFORE iter_content reads body
    oversized_cl_resp = _FakeStreamResponse(
        status_code=200,
        headers={"Content-Type": "image/png", "Content-Length": "1025"},
        chunks=[SAMPLE_PNG + b"x" * 2000],
    )
    assert (
        fetch_validated_product_image(
            "https://cdn.example.com/huge.png",
            max_bytes=1024,
            resolver=lambda h, p, type=0: [_addrinfo_v4("93.184.216.34", p)],
            session_factory=lambda h, ips: _FakeSession(
                lambda url, **kw: oversized_cl_resp
            ),
        )
        is None
    )
    assert oversized_cl_resp.iter_content_called is False
    assert oversized_cl_resp.closed is True

    # 2. Chunked stream without Content-Length aborts as soon as cumulative bytes > max_bytes
    chunked_resp = _FakeStreamResponse(
        status_code=200,
        headers={"Content-Type": "image/png"},
        chunks=[SAMPLE_PNG, b"x" * 600, b"y" * 600, b"z" * 600],
    )
    assert (
        fetch_validated_product_image(
            "https://cdn.example.com/stream.png",
            max_bytes=len(SAMPLE_PNG) + 1000,
            resolver=lambda h, p, type=0: [_addrinfo_v4("93.184.216.34", p)],
            session_factory=lambda h, ips: _FakeSession(lambda url, **kw: chunked_resp),
        )
        is None
    )
    assert chunked_resp.iter_content_called is True
    # Third chunk pushed total past cap; fourth chunk was never read!
    assert chunked_resp.chunks_yielded == 3
    assert chunked_resp.closed is True

    # 3. Exact boundary (len == max_bytes) succeeds
    exact_payload = SAMPLE_PNG + b"\x00" * 32
    exact_resp = _FakeStreamResponse(
        status_code=200,
        headers={
            "Content-Type": "image/png",
            "Content-Length": str(len(exact_payload)),
        },
        chunks=[exact_payload[:10], exact_payload[10:]],
    )
    assert fetch_validated_product_image(
        "https://cdn.example.com/exact.png",
        max_bytes=len(exact_payload),
        resolver=lambda h, p, type=0: [_addrinfo_v4("93.184.216.34", p)],
        session_factory=lambda h, ips: _FakeSession(lambda url, **kw: exact_resp),
    ) == (exact_payload, "image/png")
    assert exact_resp.closed is True


@pytest.mark.parametrize(
    ("content_type", "payload", "expected_mime"),
    [
        ("image/png", SAMPLE_PNG, "image/png"),
        ("IMAGE/PNG; charset=binary", SAMPLE_PNG, "image/png"),
        ("image/jpeg", SAMPLE_JPEG, "image/jpeg"),
        ("image/jpg", SAMPLE_JPEG, "image/jpeg"),
        ("image/gif", SAMPLE_GIF87A, "image/gif"),
        ("image/gif", SAMPLE_GIF89A, "image/gif"),
        ("image/webp", SAMPLE_WEBP, "image/webp"),
    ],
)
def test_content_type_and_magic_bytes_accept_valid_raster_formats(
    content_type: str, payload: bytes, expected_mime: str
) -> None:
    assert detect_raster_mime_type(payload) == expected_mime
    result = fetch_validated_product_image(
        "https://cdn.example.com/product.img",
        resolver=lambda h, p, type=0: [_addrinfo_v4("93.184.216.34", p)],
        session_factory=lambda h, ips: _FakeSession(
            lambda url, **kw: _FakeStreamResponse(
                status_code=200,
                headers={"Content-Type": content_type},
                chunks=[payload],
            )
        ),
    )
    assert result == (payload, expected_mime)


@pytest.mark.parametrize(
    ("content_type", "payload"),
    [
        ("image/jpeg", b'{"access_token":"secret-metadata-token"}'),
        ("image/png", b'<svg xmlns="http://www.w3.org/2000/svg"><script>1</script></svg>'),
        ("image/gif", b"<!DOCTYPE html><html><body>error</body></html>"),
        ("image/webp", b"RIFF\x10\x00\x00\x00WAVEfmt "),
        ("text/html", SAMPLE_PNG),
        ("application/json", SAMPLE_JPEG),
        ("image/svg+xml", b'<svg xmlns="http://www.w3.org/2000/svg"></svg>'),
        ("", SAMPLE_PNG),
        ("image/png", SAMPLE_JPEG),
        ("image/png", b""),
    ],
)
def test_content_type_and_magic_bytes_reject_spoofed_or_non_raster_payloads(
    content_type: str, payload: bytes
) -> None:
    result = fetch_validated_product_image(
        "https://cdn.example.com/spoofed.img",
        resolver=lambda h, p, type=0: [_addrinfo_v4("93.184.216.34", p)],
        session_factory=lambda h, ips: _FakeSession(
            lambda url, **kw: _FakeStreamResponse(
                status_code=200,
                headers={"Content-Type": content_type} if content_type else {},
                chunks=[payload] if payload else [],
            )
        ),
    )
    assert result is None


def test_env_var_validation_propagates_value_error_fast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert get_max_image_bytes() == DEFAULT_MAX_IMAGE_BYTES
    monkeypatch.setenv("GEN_AI_MAX_IMAGE_BYTES", "4096")
    assert get_max_image_bytes() == 4096

    for bad_bytes in ("0", "-10", "not-an-int", "3.14"):
        monkeypatch.setenv("GEN_AI_MAX_IMAGE_BYTES", bad_bytes)
        with pytest.raises(ValueError):
            get_max_image_bytes()
        with pytest.raises(ValueError):
            fetch_validated_product_image("https://cdn.example.com/a.png")
        with pytest.raises(ValueError):
            decode_bounded_base64_image("YWJj")
    monkeypatch.delenv("GEN_AI_MAX_IMAGE_BYTES")

    for bad_timeout in ("0", "-1.5", "abc", "nan", "inf"):
        monkeypatch.setenv("GEN_AI_IMAGE_FETCH_TIMEOUT_SECONDS", bad_timeout)
        with pytest.raises(ValueError):
            fetch_validated_product_image("https://cdn.example.com/a.png")
    monkeypatch.delenv("GEN_AI_IMAGE_FETCH_TIMEOUT_SECONDS")

    # Verify connect/read timeout tuple calculation: (min(5.0, timeout), timeout)
    monkeypatch.setenv("GEN_AI_IMAGE_FETCH_TIMEOUT_SECONDS", "12.0")
    recorded_sess = _FakeSession(
        lambda url, **kw: _FakeStreamResponse(
            status_code=200,
            headers={"Content-Type": "image/png"},
            chunks=[SAMPLE_PNG],
        )
    )
    assert fetch_validated_product_image(
        "https://cdn.example.com/a.png",
        resolver=lambda h, p, type=0: [_addrinfo_v4("93.184.216.34", p)],
        session_factory=lambda h, ips: recorded_sess,
    ) == (SAMPLE_PNG, "image/png")
    assert recorded_sess.calls[0]["timeout"] == (5.0, 12.0)

    monkeypatch.setenv("GEN_AI_IMAGE_FETCH_TIMEOUT_SECONDS", "2.5")
    short_sess = _FakeSession(
        lambda url, **kw: _FakeStreamResponse(
            status_code=200,
            headers={"Content-Type": "image/png"},
            chunks=[SAMPLE_PNG],
        )
    )
    assert fetch_validated_product_image(
        "https://cdn.example.com/a.png",
        resolver=lambda h, p, type=0: [_addrinfo_v4("93.184.216.34", p)],
        session_factory=lambda h, ips: short_sess,
    ) == (SAMPLE_PNG, "image/png")
    assert short_sess.calls[0]["timeout"] == (2.5, 2.5)
    monkeypatch.delenv("GEN_AI_IMAGE_FETCH_TIMEOUT_SECONDS")

    for bad_redirects in ("-1", "4", "99", "abc"):
        monkeypatch.setenv("GEN_AI_MAX_IMAGE_REDIRECTS", bad_redirects)
        with pytest.raises(ValueError):
            fetch_validated_product_image("https://cdn.example.com/a.png")


def test_decode_bounded_base64_image_validation_mime_detection_and_size_caps() -> None:
    # Existing short test fixture payload defaults to image/png when no magic bytes match
    assert decode_bounded_base64_image("YWJj") == (b"abc", "image/png")
    assert decode_bounded_base64_image("  \nYWJj\t ") == (b"abc", "image/png")

    # Real raster magic bytes are detected accurately
    jpeg_b64 = base64.b64encode(SAMPLE_JPEG).decode("ascii")
    assert decode_bounded_base64_image(jpeg_b64) == (SAMPLE_JPEG, "image/jpeg")

    webp_b64 = base64.b64encode(SAMPLE_WEBP).decode("ascii")
    assert decode_bounded_base64_image(webp_b64) == (SAMPLE_WEBP, "image/webp")

    # Optional data:image/<subtype>;base64, prefix is stripped cleanly
    png_data_uri = "data:image/png;base64," + base64.b64encode(SAMPLE_PNG).decode(
        "ascii"
    )
    assert decode_bounded_base64_image(png_data_uri) == (SAMPLE_PNG, "image/png")

    # Malformed base64 / bad padding / non-alphabet / empty payloads raise ImageValidationError
    for invalid_b64 in (
        "A",
        "",
        "   ",
        "====",
        "YW=",
        "YWJj!",
        "YWJj====",
        "data:image/png;base64,",
        "data:text/html;base64,YWJj",
        "data:image/svg+xml;base64,PHN2Zz4=",
    ):
        with pytest.raises(ImageValidationError, match="Invalid base64 image format"):
            decode_bounded_base64_image(invalid_b64)

    # Pre-decode encoded length check raises ImageTooLargeError
    oversized_b64 = base64.b64encode(b"x" * 100).decode("ascii")
    with pytest.raises(ImageTooLargeError):
        decode_bounded_base64_image(oversized_b64, max_bytes=16)

    # Post-decode exact byte cap check (max_bytes=4 -> max_b64_chars=8; 5 bytes -> 8 b64 chars)
    five_bytes_b64 = base64.b64encode(b"12345").decode("ascii")
    assert len(five_bytes_b64) == max_base64_chars_for_bytes(4)
    with pytest.raises(ImageTooLargeError):
        decode_bounded_base64_image(five_bytes_b64, max_bytes=4)

    four_bytes_b64 = base64.b64encode(b"1234").decode("ascii")
    assert decode_bounded_base64_image(four_bytes_b64, max_bytes=4) == (
        b"1234",
        "image/png",
    )


def test_image_fetcher_imports_cleanly_under_both_pythonpath_roots(
    tmp_path: Path,
) -> None:
    script = (
        "import importlib, sys\n"
        "mod = importlib.import_module(sys.argv[1])\n"
        "assert mod.DEFAULT_MAX_IMAGE_BYTES == 10 * 1024 * 1024\n"
        "assert mod.MAX_IMAGE_URL_CHARS == 2048\n"
        "assert mod.STREAM_CHUNK_BYTES == 65536\n"
    )
    for pythonpath, module_name in (
        (str(_SRC_DIR), "psearch.gen_ai.services.image_fetcher"),
        (str(_PSEARCH_DIR), "gen_ai.services.image_fetcher"),
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
        assert completed.stderr == ""

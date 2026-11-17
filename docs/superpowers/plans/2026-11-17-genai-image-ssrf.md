# GenAI Product Image SSRF & Unbounded Download Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate `CWE-918` SSRF, TOCTOU DNS rebinding, non-image response exfiltration, and unbounded image downloads/base64 payloads across `gen_ai` product enrichment, marketing, and image generation (closing GitHub Issue `#70`).

**Architecture:** Extract a dedicated zero-dependency utility `src/psearch/gen_ai/services/image_fetcher.py` that validates `https://` and `gs://` URLs, resolves DNS once per hop while rejecting all non-global/metadata/embedded-IPv4 addresses, connects via a custom `urllib3`/`requests` `_PinnedIPHTTPSAdapter` (keeping TLS SNI and hostname verification intact), blocks redirects by default (`allow_redirects=False`), streams bounded bytes (`10 MiB` default), and verifies both raster `Content-Type` and magic bytes. Wire `EnrichmentService`, `MarketingService`, `ImageGenerationService`, `src/psearch/gen_ai/main.py`, and Terraform `src/iac/modules/gen_ai/` to these shared guards.

**Tech Stack:** Python 3.11+, FastAPI, Pydantic v2, `requests` / `urllib3`, `ipaddress`, `pytest`, Terraform HCL (`python-hcl2` structural tests).

**Spec:** `docs/superpowers/specs/2026-11-17-genai-image-ssrf-design.md`

## Global Constraints

- Dual Python import-root compatibility MUST be preserved: all modules and tests under `src/psearch/gen_ai/` MUST pass under both `PYTHONPATH=src` and `PYTHONPATH=src/psearch`.
- Zero new third-party runtime dependencies in `src/psearch/gen_ai/requirements.txt` (use stdlib `ipaddress`, `socket`, `ssl`, `urllib.parse`, `base64` + already-installed `requests`/`urllib3`).
- Outbound image URLs MUST allow only `https://` and `gs://` (`<= 2048` chars, no whitespace/control chars, no userinfo, no fragments, port `None` or `443`). `gs://<bucket>/<object>` MUST validate bucket syntax and percent-encode object segments (`quote(seg, safe="")`, rejecting `.` and `..`) into `https://storage.googleapis.com/<bucket>/<encoded_path>`.
- DNS/IP validation MUST reject empty `getaddrinfo` results and reject the host if ANY resolved address (including IPv4-mapped IPv6 `::ffff:0:0/96`, 6to4 `2002::/16`, Teredo `2001::/32`, NAT64 `64:ff9b::/96` & `64:ff9b:1::/48`, CGNAT `100.64.0.0/10`, cloud metadata `100.100.100.200` / `fd00:ec2::254`, loopback, private, link-local, multicast, reserved, unspecified, benchmarking, or documentation ranges) is non-global, and TCP sockets MUST connect directly to the pre-validated IP tuple via `_PinnedIPHTTPSAdapter` with TLS SNI `server_hostname = hostname`.
- Redirects MUST always set `allow_redirects=False` on `requests`, defaulting `GEN_AI_MAX_IMAGE_REDIRECTS = 0` (capped at `3` when configured, re-validating every hop).
- Downloads MUST stream (`stream=True`, `chunk_size=65536`, `GEN_AI_MAX_IMAGE_BYTES` default `10485760`), require allowlisted raster `Content-Type` (`image/jpeg`, `image/jpg`, `image/png`, `image/gif`, `image/webp`) AND matching raster magic bytes (`JPEG`, `PNG`, `GIF87a`/`GIF89a`, `RIFF....WEBP`).
- Existing `POST /generate-enhanced-image` contracts MUST remain intact: `"image_base64": "A"` returns HTTP `200` with `{"product_id": ..., "error": "Invalid base64 image format"}`, `"image_base64": "YWJj"` reaches `ImageGenerationService` (falling back to `"image/png"` MIME when synthetic bytes have no magic header), and payloads exceeding `GEN_AI_MAX_IMAGE_BYTES` return HTTP `413` at the route boundary (or `"Image exceeds maximum allowed size"` if `ImageGenerationService.generate_image` is called directly).
- Never emit local machine home-directory paths or AI co-author trailers (`Co-Authored-By`, etc.) in any file or commit.

## Review Focus

1. IPv4-mapped IPv6 (`::ffff:169.254.169.254`, `::ffff:127.0.0.1`) and NAT64 (`64:ff9b::a9fe:a9fe`, `64:ff9b:1::a00:1`) DNS answers unwrap embedded IPv4 addresses and are rejected as disallowed IPs (pinned in Task 1).
2. Multi-record DNS responses mixing a public IP with a private/metadata IP (`["93.184.216.34", "169.254.169.254"]`) fail closed and reject the entire host without opening a socket (pinned in Task 1).
3. Spoofed `Content-Type: image/jpeg` serving JSON/HTML/SVG bytes (`b'{"secret":"token"}'` or `b'<svg>...</svg>'`) is rejected by magic-byte verification and returns `None` (pinned in Task 1).
4. Chunked responses omitting `Content-Length` that exceed `max_bytes` mid-stream abort immediately on the offending chunk, close the response, and return `None` (pinned in Task 1).
5. Malformed `product_data["images"]` shapes (`123`, `"https://..."`, `[None]`, `["https://..."]`, `[{}]`) in `POST /enrichment` and `POST /marketing` never raise unhandled `TypeError`/`AttributeError`, and oversized `image_base64` (including `data:image/png;base64,...` prefixes) returns HTTP `413` while `"A"` still returns `"Invalid base64 image format"` (pinned in Task 2).

---

### Task 1: Shared SSRF-Safe & Bounded Image Fetcher (`image_fetcher.py`)

**Files:**
- Create: `src/psearch/gen_ai/services/image_fetcher.py`
- Create: `src/psearch/gen_ai/tests/test_image_fetcher.py`

**Interfaces:**
- Consumes: Python stdlib (`base64`, `ipaddress`, `logging`, `os`, `re`, `socket`, `typing`, `urllib.parse`), `requests`, `urllib3`
- Produces (in `src/psearch/gen_ai/services/image_fetcher.py`):
  - `DEFAULT_MAX_IMAGE_BYTES: int = 10 * 1024 * 1024`
  - `MAX_IMAGE_URL_CHARS: int = 2048`
  - `STREAM_CHUNK_BYTES: int = 65536`
  - `ALLOWED_IMAGE_CONTENT_TYPES: frozenset[str]`
  - `class ImageValidationError(ValueError)`
  - `class ImageTooLargeError(ImageValidationError)`
  - `def get_max_image_bytes() -> int`
  - `def max_base64_chars_for_bytes(max_bytes: int) -> int`
  - `def is_disallowed_ip(ip_text: str) -> bool`
  - `def detect_raster_mime_type(data: bytes) -> Optional[str]`
  - `def validate_and_normalize_image_url(image_url: str, allowed_hosts: Optional[set[str]] = None) -> Tuple[str, str]` — returns `(normalized_https_url, normalized_hostname)` or raises `ImageValidationError`
  - `def resolve_and_validate_host_ips(hostname: str, port: int = 443, resolver: Optional[Callable[..., Any]] = None) -> List[str]` — returns deduplicated list of validated global IP strings or raises `ImageValidationError`
  - `def fetch_validated_product_image(image_url: str, *, max_bytes: Optional[int] = None, timeout_seconds: Optional[float] = None, max_redirects: Optional[int] = None, allowed_hosts: Optional[set[str]] = None, resolver: Optional[Callable[..., Any]] = None, session_factory: Optional[Callable[[str, List[str]], requests.Session]] = None) -> Optional[Tuple[bytes, str]]` — resolves configuration env vars up front (allowing invalid env-var `ValueError` to propagate), then catches `(ImageValidationError, requests.RequestException, OSError)` on untrusted `image_url` inputs, logs a warning, and returns `None` (or `(image_bytes, canonical_mime_type)` on success).
  - `def decode_bounded_base64_image(image_base64: str, *, max_bytes: Optional[int] = None) -> Tuple[bytes, str]` — strips surrounding whitespace and optional `data:image/<subtype>;base64,` prefix, checks encoded/decoded length against `max_bytes` (raising `ImageTooLargeError` before/after decode if exceeded), strictly validates base64 alphabet/padding (raising `ImageValidationError("Invalid base64 image format")` on malformed base64 or empty payload), and returns `(decoded_bytes, detected_mime_or_png_default)`.

- [ ] **Step 1: Write failing unit tests in `src/psearch/gen_ai/tests/test_image_fetcher.py`**

Cover all URL schemes (`https://` & `gs://` accepted; `http://`, `file://`, `ftp://`, `data:`, schemeless, userinfo `https://user:pass@example.com/a.png`, fragments `#x`, non-443 port `https://example.com:8443/a.png`, `gs://` traversal `gs://my-bucket/../secret.png` & query injection `gs://my-bucket/path/a?.png` percent-encoded safely), blocked hostnames (`localhost`, `sub.localhost`, `metadata.google.internal`, `metadata`, `foo.internal`, `bar.local`), `GEN_AI_ALLOWED_IMAGE_HOSTS` filtering, `is_disallowed_ip` (loopback, RFC1918, `169.254.169.254`, `fe80::1`, `100.64.0.1`, `100.100.100.200`, `fd00:ec2::254`, `::ffff:169.254.169.254`, `64:ff9b::a9fe:a9fe`, `2002:7f00:0001::1`), multi-record DNS mixed public+private rejection, `_PinnedIPHTTPSAdapter` socket-level IP pinning + TLS SNI `server_hostname` verification (mocking `urllib3.util.connection.create_connection` to prove the TCP destination is `(validated_ip, 443)` and `getaddrinfo` is called only once), `allow_redirects=False` default `302` rejection + opt-in `GEN_AI_MAX_IMAGE_REDIRECTS=2` blocking redirect to `http://169.254.169.254/` while allowing redirect to validated public HTTPS host, streaming byte cap via both `Content-Length` and chunked `iter_content` without `Content-Length`, `Content-Type` + magic-byte enforcement (PNG/JPEG/GIF/WebP pass; spoofed `image/jpeg` with JSON/SVG bytes fails), and `decode_bounded_base64_image` (`"A"` -> `ImageValidationError`, `"YWJj"` -> `(b"abc", "image/png")`, real JPEG base64 -> `"image/jpeg"`, `data:image/png;base64,...` prefix, oversized -> `ImageTooLargeError`).

- [ ] **Step 2: Run `pytest` to verify the new test file fails (RED)**

Run: `PYTHONPATH=src uv run --with-requirements src/psearch/gen_ai/requirements.txt --with pytest pytest -W default -q src/psearch/gen_ai/tests/test_image_fetcher.py`
Expected: FAIL with `ModuleNotFoundError: No module named 'psearch.gen_ai.services.image_fetcher'`.

- [ ] **Step 3: Implement `src/psearch/gen_ai/services/image_fetcher.py`**

Implement the exact signatures in **Interfaces** above. For `_PinnedIPHTTPSAdapter`, subclass `urllib3.connection.HTTPSConnection` and `urllib3.connectionpool.HTTPSConnectionPool` / `requests.adapters.HTTPAdapter` so `_new_conn` connects `urllib3.util.connection.create_connection((pinned_ip, self.port), timeout=self.timeout, socket_options=self.socket_options)` while keeping `self.host = original_hostname` so TLS `ssl_wrap_socket(..., server_hostname=self.host)` and `assert_hostname` validate the certificate against `original_hostname` with zero second DNS lookup. Ensure `with session.get(..., stream=True, allow_redirects=False, timeout=...) as response:` always closes sockets cleanly.

- [ ] **Step 4: Run `pytest` under both `PYTHONPATH=src` and `PYTHONPATH=src/psearch` (GREEN)**

Run:
`PYTHONPATH=src uv run --with-requirements src/psearch/gen_ai/requirements.txt --with pytest pytest -W default -q src/psearch/gen_ai/tests/test_image_fetcher.py`
`PYTHONPATH=src/psearch uv run --with-requirements src/psearch/gen_ai/requirements.txt --with pytest pytest -W default -q src/psearch/gen_ai/tests/test_image_fetcher.py`
Expected: PASS (`0` warnings).

- [ ] **Step 5: Commit Task 1**

```bash
git add src/psearch/gen_ai/services/image_fetcher.py src/psearch/gen_ai/tests/test_image_fetcher.py
git commit -m "feat(gen_ai): add SSRF-safe pinned-IP and size-bounded product image fetcher"
```

---

### Task 2: Wire GenAI Services, FastAPI Image Limits & Terraform Variables

**Files:**
- Modify: `src/psearch/gen_ai/services/enrichment_service.py`
- Modify: `src/psearch/gen_ai/services/marketing_service.py`
- Modify: `src/psearch/gen_ai/services/imagen_service.py`
- Modify: `src/psearch/gen_ai/main.py`
- Modify: `src/iac/modules/gen_ai/variables.tf`
- Modify: `src/iac/modules/gen_ai/main.tf`
- Modify: `src/iac/variables.tf`
- Modify: `src/iac/main.tf`
- Modify: `DEPLOYMENT.md`
- Modify: `DESIGN.md`
- Modify: `src/psearch/gen_ai/tests/test_gen_ai_auth_and_api.py`
- Modify: `src/psearch/ingestion_source/tests/test_iac_security.py`

**Interfaces:**
- Consumes (from Task 1 `image_fetcher.py`):
  - `ImageTooLargeError`, `ImageValidationError`, `decode_bounded_base64_image`, `fetch_validated_product_image`, `get_max_image_bytes`, `max_base64_chars_for_bytes`
- Produces:
  - `EnrichmentService._fetch_product_image(self, image_url: str) -> Optional[Tuple[bytes, str]]` + backward-compatible `_get_image_bytes_from_url(self, image_url: str) -> Optional[bytes]` delegating to `fetch_validated_product_image(image_url)`; `_build_prompt` uses canonical returned `mime_type` and guards non-list `images` / non-dict `images[0]` / non-str `uri`.
  - `MarketingService._fetch_product_image(self, image_url: str) -> Optional[Tuple[bytes, str]]` + backward-compatible `_get_image_bytes_from_url(self, image_url: str) -> Optional[bytes]` delegating to `fetch_validated_product_image(image_url)`; `_build_prompt` uses canonical returned `mime_type` and guards non-list `images` / non-dict `images[0]` / non-str `uri`.
  - `ImageGenerationService.generate_image(...)` uses `decode_bounded_base64_image(image_base64)` returning `{"product_id": product_id, "error": "Image exceeds maximum allowed size"}` on `ImageTooLargeError`, `{"product_id": product_id, "error": "Invalid base64 image format"}` on `ImageValidationError`, and passes the returned `mime_type` to `types.Part.from_bytes(data=image_bytes, mime_type=mime_type)`.
  - `src/psearch/gen_ai/main.py`:
    - Adds `"Image exceeds maximum allowed size"` to `_SAFE_IMAGE_ERRORS`.
    - Guards `isinstance(request.product_data.get("images"), list)` before `len(...)` in `enrichment` and `marketing`.
    - Checks `image_base64` size in `generate_enhanced_image` against `get_max_image_bytes()` / `max_base64_chars_for_bytes(...)` (and if `result.get("error") == "Image exceeds maximum allowed size"`, raises `HTTPException(status_code=413, detail="Image exceeds maximum allowed size")`; pre-checks raw `len(stripped_payload) > max_base64_chars_for_bytes(max_bytes)` before calling the service to raise `HTTPException(status_code=413, detail="Image exceeds maximum allowed size")` without allocating decoded memory!).
  - Terraform `src/iac/modules/gen_ai/{variables.tf,main.tf}` and root `src/iac/{variables.tf,main.tf}`:
    - Module vars `allowed_image_hosts` (`string`, default `""`) and `max_image_bytes` (`number`, default `10485760`, validation `var.max_image_bytes > 0` and integer `floor(var.max_image_bytes) == var.max_image_bytes`), wired to Cloud Run env vars `GEN_AI_ALLOWED_IMAGE_HOSTS` and `GEN_AI_MAX_IMAGE_BYTES`, with root pass-through vars `gen_ai_allowed_image_hosts` and `gen_ai_max_image_bytes`.

- [ ] **Step 1: Write failing integration & IaC tests in `test_gen_ai_auth_and_api.py` and `test_iac_security.py`**

Add tests verifying:
1. `EnrichmentService.process_enrichment` and `MarketingService.generate_content` block SSRF URLs (`http://169.254.169.254/computeMetadata/v1/`, `https://metadata.google.internal/`, `https://evil.example.com/secret.jpg` resolving to `10.0.0.5` or returning `application/json`) without making unvalidated `requests.get` calls and without attaching non-image parts to Gemini `contents`, while valid public `https://` / `gs://` PNG/WebP images attach `Part.from_bytes` with the detected `mime_type` (`image/webp`, `image/png`).
2. `POST /enrichment` and `POST /marketing` handle malformed `product_data["images"]` (`123`, `"https://..."`, `[None]`, `["https://..."]`, `[{}]`) cleanly with HTTP `200` (no unhandled `TypeError`/`AttributeError`).
3. `POST /generate-enhanced-image` rejects oversized `image_base64` (including `data:image/png;base64,...` prefix) with HTTP `413` (`"Image exceeds maximum allowed size"`), while `"A"` still returns HTTP `200` `{"error": "Invalid base64 image format"}` and `"YWJj"` still exercises `ImageGenerationService`.
4. `test_iac_security.py` asserts `GEN_AI_ALLOWED_IMAGE_HOSTS` and `GEN_AI_MAX_IMAGE_BYTES` (`default = 10485760`, positive integer validation) in `src/iac/modules/gen_ai/` and root `src/iac/` pass-through wiring.

- [ ] **Step 2: Run targeted `pytest` to verify the new tests fail (RED)**

Run: `PYTHONPATH=src uv run --with-requirements src/psearch/gen_ai/requirements.txt --with-requirements src/psearch/ingestion_source/requirements.txt --with pytest --with httpx --with python-hcl2 pytest -W default -q src/psearch/gen_ai/tests/test_gen_ai_auth_and_api.py src/psearch/ingestion_source/tests/test_iac_security.py`
Expected: FAIL on the newly added SSRF, malformed `images`, `413` `image_base64`, and HCL env assertions.

- [ ] **Step 3: Implement service wiring, route guards, Terraform variables, and docs**

Update `enrichment_service.py`, `marketing_service.py`, `imagen_service.py`, `main.py`, `src/iac/modules/gen_ai/{variables.tf,main.tf}`, `src/iac/{variables.tf,main.tf}`, `DEPLOYMENT.md`, and `DESIGN.md` per **Interfaces** above. Remove direct `requests.get` usage from `enrichment_service.py` and `marketing_service.py`.

- [ ] **Step 4: Run full Python, HCL, JS, and Go verification suites (GREEN)**

Run:
`PYTHONPATH=src uv run --with-requirements src/psearch/gen_ai/requirements.txt --with-requirements src/psearch/ingestion_source/requirements.txt --with pytest --with httpx --with python-hcl2 pytest -W default -q src/psearch/ingestion_source/tests/ src/psearch/gen_ai/tests/ src/psearch/gen_ai/services/sql/common/test_input_validation.py`
`PYTHONPATH=src/psearch uv run --with-requirements src/psearch/gen_ai/requirements.txt --with-requirements src/psearch/ingestion_source/requirements.txt --with pytest --with httpx --with python-hcl2 pytest -W default -q src/psearch/ingestion_source/tests/ src/psearch/gen_ai/tests/`
`(cd src/psearch/serving && go vet ./... && go test -count=1 -race ./...)`
Expected: All suites PASS with `0` warnings.

- [ ] **Step 5: Commit Task 2**

```bash
git add src/psearch/gen_ai/services/enrichment_service.py src/psearch/gen_ai/services/marketing_service.py src/psearch/gen_ai/services/imagen_service.py src/psearch/gen_ai/main.py src/iac/modules/gen_ai/variables.tf src/iac/modules/gen_ai/main.tf src/iac/variables.tf src/iac/main.tf DEPLOYMENT.md DESIGN.md src/psearch/gen_ai/tests/test_gen_ai_auth_and_api.py src/psearch/ingestion_source/tests/test_iac_security.py
git commit -m "fix(gen_ai,iac): prevent SSRF and cap image payloads across enrichment, marketing, and imagen"
```

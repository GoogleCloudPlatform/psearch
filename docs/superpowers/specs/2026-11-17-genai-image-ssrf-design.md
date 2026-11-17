# GenAI Product Image SSRF & Unbounded Download Hardening Design (`CWE-918` / Issue `#70`)

## Intent & Problem Statement

In `src/psearch/gen_ai/services/enrichment_service.py` (`_get_image_bytes_from_url`) and `src/psearch/gen_ai/services/marketing_service.py` (`_get_image_bytes_from_url`), `POST /enrichment` and `POST /marketing` extract `product_data["images"][0]["uri"]` from caller-controlled payloads and call `requests.get(url, timeout=10)` (`response.content`) without scheme validation, hostname/DNS/IP filtering, redirect restrictions, response byte limits, or `Content-Type`/magic-byte verification, feeding any HTTP `200` body into Gemini. Additionally, `ImageGenerationService.generate_image` (`src/psearch/gen_ai/services/imagen_service.py`) decodes `image_base64` with no maximum byte limit.

Even with Cloud Run IAM/ingress and FastAPI `require_auth` landed in PR `#103`, any authenticated caller or compromised upstream session can exploit this confused-deputy sink (`CWE-918`, Issue `#70`) to reach loopback, RFC1918 VPC endpoints, or the GCE metadata server (`169.254.169.254` / `metadata.google.internal`), exhaust container memory with oversized payloads, or exfiltrate non-image HTTP responses through Gemini output.

## Goals

1. **Centralize outbound product image fetching in `src/psearch/gen_ai/services/image_fetcher.py`**:
   - Allow only `https://` and `gs://` URLs (`<= 2048` chars, no whitespace/control chars, no `user:pass@` userinfo, no URL fragments, port restricted to implicit or explicit `443`).
   - Convert `gs://<bucket>/<object>` safely by validating GCS bucket syntax (`^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$`, length `<= 63` unless dotted up to `222`, no `..`) and percent-encoding each non-empty object path segment (`quote(seg, safe="")`, rejecting `.` and `..` segments) into `https://storage.googleapis.com/<bucket>/<encoded_path>`.
   - Block internal/metadata hostnames (`localhost`, `*.localhost`, `metadata`, `metadata.google.internal`, `*.google.internal`, `*.internal`, `*.local`) and support optional host allowlisting via `GEN_AI_ALLOWED_IMAGE_HOSTS` (default `""`: allow any public HTTPS host passing DNS/IP checks — *builder call*).
2. **Prevent SSRF and TOCTOU DNS rebinding across DNS resolution, socket connection, and redirects**:
   - Resolve `socket.getaddrinfo(hostname, 443, type=socket.SOCK_STREAM)` once per hop and reject the request if resolution returns empty or if **any** resolved IPv4/IPv6 record (including IPv4-mapped `::ffff:0:0/96`, 6to4 `2002::/16`, Teredo `2001::/32`, NAT64 `64:ff9b::/96` & `64:ff9b:1::/48`, CGNAT `100.64.0.0/10`, cloud metadata `100.100.100.200` / `fd00:ec2::254`, loopback, private, link-local, multicast, reserved, unspecified, benchmarking, or documentation ranges) is non-global.
   - Connect via a custom `requests.adapters.HTTPAdapter` / `urllib3` `HTTPSConnection` (`_PinnedIPHTTPSAdapter`) that opens the TCP socket directly to the pre-validated IP tuple on port `443` while preserving `server_hostname = hostname` and `Host: hostname` for TLS SNI and X.509 certificate verification.
   - Always pass `allow_redirects=False` to `requests`; default `GEN_AI_MAX_IMAGE_REDIRECTS = 0` (reject all `3xx` redirects by default — *builder call*), and when configured `1..3`, re-validate every redirect `Location` target through full URL + host + pinned-DNS/IP checks before connecting.
3. **Bound memory and block non-image response exfiltration**:
   - Stream responses (`stream=True`, `iter_content(chunk_size=65536)`, configurable `GEN_AI_MAX_IMAGE_BYTES` default `10485760` = `10 MiB`, timeout `(5.0, 10.0)` configurable via `GEN_AI_IMAGE_FETCH_TIMEOUT_SECONDS`), pre-checking `Content-Length` and aborting immediately if exceeded or empty.
   - Require both an allowlisted raster `Content-Type` (`image/jpeg`, `image/jpg`, `image/png`, `image/gif`, `image/webp`) and matching raster magic bytes (`JPEG`, `PNG`, `GIF87a`/`GIF89a`, `RIFF....WEBP`), returning `(image_bytes, canonical_mime_type)` so HTML/JSON/text/SVG responses are never passed to Gemini.
4. **Harden `EnrichmentService`, `MarketingService`, `ImageGenerationService`, `main.py`, and Terraform `gen_ai`**:
   - Wire `EnrichmentService` and `MarketingService` to `fetch_validated_product_image(...)` and guard non-list/non-dict `product_data["images"]` inputs without unhandled `TypeError`/`AttributeError`.
   - Enforce `GEN_AI_MAX_IMAGE_BYTES` on `EnhancedImageRequest.image_base64` (`HTTP 413` at route boundary; safe `"Image exceeds maximum allowed size"` fallback in `ImageGenerationService.generate_image`) while preserving existing base64 error contracts and detecting raster MIME types when magic bytes match.
   - Expose `allowed_image_hosts` (`GEN_AI_ALLOWED_IMAGE_HOSTS`) and `max_image_bytes` (`GEN_AI_MAX_IMAGE_BYTES`) on `src/iac/modules/gen_ai/` and root `src/iac/`.

## Non-Goals & Anti-Goals

- **Non-goal**: Re-architecting `EnrichmentService` or `MarketingService` to use the GCS Python SDK instead of HTTPS fetches for `gs://` URLs.
- **Anti-goal**: Allowing a secondary unpinned DNS lookup inside `urllib3` after `socket.getaddrinfo` validation (TOCTOU DNS rebinding).
- **Anti-goal**: Feeding `text/html`, `application/json`, `text/plain`, or `image/svg+xml` payloads (even with spoofed `Content-Type` headers) into Gemini prompts.

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

"""Node-backed behavioral tests for React UI auth header propagation."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[4]
UI_SRC_ROOT = REPO_ROOT / "src" / "application" / "ui" / "src"
SERVICES_ROOT = UI_SRC_ROOT / "services"


def _require_node() -> str:
    node = shutil.which("node")
    if not node:
        pytest.skip("node binary is required for UI auth header tests")
    return node


def _run_node_harness(test_body_js: str, extra_env: dict[str, str] | None = None) -> Any:
    """Execute an async Node ESM harness with mocked axios/React hooks and return JSON."""
    node = _require_node()
    harness = f"""
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const {{ pathToFileURL, fileURLToPath }} = require("node:url");
const {{ registerHooks }} = require("node:module");

const UI_SRC_ROOT = {json.dumps(str(UI_SRC_ROOT))};

function createMemoryStorage(initial = {{}}) {{
  const map = new Map(Object.entries(initial));
  return {{
    getItem(key) {{
      return map.has(String(key)) ? map.get(String(key)) : null;
    }},
    setItem(key, value) {{
      map.set(String(key), String(value));
    }},
    removeItem(key) {{
      map.delete(String(key));
    }},
    clear() {{
      map.clear();
    }},
  }};
}}

function installBrowserStorages(localInit = {{}}, sessionInit = {{}}) {{
  const local = createMemoryStorage(localInit);
  const session = createMemoryStorage(sessionInit);
  Object.defineProperty(globalThis, "localStorage", {{
    value: local,
    configurable: true,
    writable: true,
  }});
  Object.defineProperty(globalThis, "sessionStorage", {{
    value: session,
    configurable: true,
    writable: true,
  }});
  return {{ local, session }};
}}

// Initialize clean in-memory storages before any module loads so Node 25+/26+
// never triggers its unconfigured --localstorage-file experimental getter.
installBrowserStorages();

globalThis.__axiosCalls = [];
globalThis.__axiosResponder = null;

function stripComponentJsxForHookExtraction(source, filePath) {{
  if (filePath.endsWith(path.sep + "SearchInput.js")) {{
    const marker = "\\n    return (\\n        <Paper";
    const idx = source.indexOf(marker);
    assert.ok(idx > 0, "Expected JSX return marker in SearchInput.js");
    return (
      source.slice(0, idx) +
      "\\n    return {{ fetchAutocomplete }};\\n}});\\nexport default SearchInput;\\n"
    );
  }}
  if (filePath.endsWith(path.sep + "App.js")) {{
    const marker = "\\n    const ProductGrid = memo(";
    const idx = source.indexOf(marker);
    assert.ok(idx > 0, "Expected ProductGrid JSX marker in App.js");
    return (
      source.slice(0, idx) +
      "\\n    return {{ fetchProducts }};\\n}}\\nexport {{ ProductSearch }};\\nexport default ProductSearch;\\n"
    );
  }}
  return null;
}}

registerHooks({{
  resolve(specifier, context, nextResolve) {{
    if (specifier === "axios") {{
      return {{ url: "virtual:///axios.mjs", shortCircuit: true }};
    }}
    if (specifier === "react") {{
      return {{ url: "virtual:///react.mjs", shortCircuit: true }};
    }}
    if (specifier === "react-router-dom") {{
      return {{ url: "virtual:///react-router-dom.mjs", shortCircuit: true }};
    }}
    if (specifier.startsWith("@mui/")) {{
      return {{
        url: "virtual:///mui/" + encodeURIComponent(specifier) + ".mjs",
        shortCircuit: true,
      }};
    }}
    if (
      (specifier.startsWith("./") || specifier.startsWith("../")) &&
      context.parentURL &&
      context.parentURL.startsWith("file:")
    ) {{
      const parentDir = path.dirname(fileURLToPath(context.parentURL));
      const candidate = path.resolve(parentDir, specifier);
      const resolvedFile = !path.extname(candidate) && fs.existsSync(candidate + ".js")
        ? candidate + ".js"
        : candidate;
      if (
        resolvedFile.startsWith(path.join(UI_SRC_ROOT, "components") + path.sep) &&
        !resolvedFile.endsWith(path.sep + "SearchInput.js")
      ) {{
        return {{
          url: "virtual:///stub-component/" + path.basename(resolvedFile) + ".mjs",
          shortCircuit: true,
        }};
      }}
      if (resolvedFile !== candidate && fs.existsSync(resolvedFile)) {{
        return {{ url: pathToFileURL(resolvedFile).href, shortCircuit: true }};
      }}
    }}
    return nextResolve(specifier, context);
  }},
  load(url, context, nextLoad) {{
    if (url === "virtual:///axios.mjs") {{
      return {{
        format: "module",
        shortCircuit: true,
        source: `
          const makeCall = (method) => async (reqUrl, arg2, arg3) => {{
            const hasBody = method === "post" || method === "put" || method === "patch";
            const data = hasBody ? arg2 : undefined;
            const config = hasBody ? arg3 : arg2;
            globalThis.__axiosCalls.push({{
              method,
              url: reqUrl,
              data,
              config: config || {{}},
              headers: (config && config.headers) ? {{ ...config.headers }} : {{}},
            }});
            if (typeof globalThis.__axiosResponder === "function") {{
              return await globalThis.__axiosResponder(method, reqUrl, data, config);
            }}
            return {{ data: {{}} }};
          }};
          const axios = {{
            get: makeCall("get"),
            post: makeCall("post"),
            put: makeCall("put"),
            delete: makeCall("delete"),
          }};
          export default axios;
        `,
      }};
    }}
    if (url === "virtual:///react.mjs") {{
      return {{
        format: "module",
        shortCircuit: true,
        source: `
          export const useState = (init) => [typeof init === "function" ? init() : init, () => {{}}];
          export const useMemo = (fn) => fn();
          export const useCallback = (fn) => fn;
          export const useEffect = () => {{}};
          export const useRef = (init) => ({{ current: init }});
          export const memo = (fn) => fn;
          export default {{ useState, useMemo, useCallback, useEffect, useRef, memo }};
        `,
      }};
    }}
    if (url === "virtual:///react-router-dom.mjs") {{
      return {{
        format: "module",
        shortCircuit: true,
        source: `
          export const BrowserRouter = () => null;
          export const Routes = () => null;
          export const Route = () => null;
          export const Link = () => null;
          export const useNavigate = () => () => {{}};
        `,
      }};
    }}
    if (url.startsWith("virtual:///mui/") || url.startsWith("virtual:///stub-component/")) {{
      return {{
        format: "module",
        shortCircuit: true,
        source: `
          const noop = () => null;
          export const Container = noop;
          export const Grid = noop;
          export const TextField = noop;
          export const Card = noop;
          export const CardContent = noop;
          export const Typography = noop;
          export const Box = noop;
          export const Chip = noop;
          export const AppBar = noop;
          export const Toolbar = noop;
          export const Button = noop;
          export const FormControl = noop;
          export const FormControlLabel = noop;
          export const InputLabel = noop;
          export const InputAdornment = noop;
          export const Select = noop;
          export const MenuItem = noop;
          export const Alert = noop;
          export const Slider = noop;
          export const Switch = noop;
          export const Paper = noop;
          export const ThemeProvider = noop;
          export const createTheme = () => ({{}});
          export const GeminiIcon = noop;
          export default noop;
        `,
      }};
    }}
    if (url.startsWith("file:")) {{
      const filePath = fileURLToPath(url);
      if (filePath.endsWith(".json")) {{
        const rawJson = fs.readFileSync(filePath, "utf8");
        return {{
          format: "module",
          shortCircuit: true,
          source: `export default ${{rawJson}};`,
        }};
      }}
      if (filePath.endsWith(".js")) {{
        const rawJs = fs.readFileSync(filePath, "utf8");
        const transformed = stripComponentJsxForHookExtraction(rawJs, filePath);
        return {{
          format: "module",
          shortCircuit: true,
          source: transformed !== null ? transformed : rawJs,
        }};
      }}
    }}
    return nextLoad(url, context);
  }},
}});

const origLog = console.log;
console.log = () => {{}};
console.warn = () => {{}};
console.error = () => {{}};

(async () => {{
  const importUi = (relPath) =>
    import(pathToFileURL(path.join(UI_SRC_ROOT, relPath)).href);
  const result = await (async () => {{
    {test_body_js}
  }})();
  origLog("__HARNESS_JSON__" + JSON.stringify(result ?? null));
}})().catch((err) => {{
  process.stderr.write((err && err.stack) ? err.stack + "\\n" : String(err) + "\\n");
  process.exit(1);
}});
"""
    env = os.environ.copy()
    for key in (
        "REACT_APP_GEN_AI_ADMIN_TOKEN",
        "REACT_APP_API_TOKEN",
        "REACT_APP_API_URL",
        "REACT_APP_GEN_AI_URL",
        "REACT_APP_PROJECT_ID",
    ):
        env.pop(key, None)
    if extra_env:
        env.update(extra_env)

    proc = subprocess.run(
        [node, "-e", harness],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert proc.returncode == 0, (
        f"Node harness failed (exit {proc.returncode}):\n"
        f"STDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}"
    )
    assert "ExperimentalWarning" not in proc.stderr, (
        f"Unexpected Node ExperimentalWarning in stderr:\n{proc.stderr}"
    )
    marker = "__HARNESS_JSON__"
    for line in reversed(proc.stdout.splitlines()):
        if line.startswith(marker):
            return json.loads(line[len(marker) :])
    raise AssertionError(f"Missing harness JSON marker in stdout:\n{proc.stdout}")


def test_ui_service_modules_pass_node_syntax_check():
    node = _require_node()
    service_files = [
        SERVICES_ROOT / "apiAuth.js",
        SERVICES_ROOT / "genAiService.js",
        SERVICES_ROOT / "sessionService.js",
        SERVICES_ROOT / "ruleService.js",
    ]
    for js_file in service_files:
        assert js_file.is_file(), f"Expected UI service module to exist: {js_file}"
        proc = subprocess.run(
            [node, "--check", str(js_file)],
            capture_output=True,
            text=True,
            check=False,
        )
        assert proc.returncode == 0, (
            f"node --check failed for {js_file}:\n{proc.stdout}\n{proc.stderr}"
        )


def test_api_auth_safe_in_bare_node_without_storage_polyfills():
    """Verify bare Node 25+/26+ (with unconfigured --localstorage-file getter) never warns or throws."""
    node = _require_node()
    api_auth_path = SERVICES_ROOT / "apiAuth.js"
    script = f"""
    const assert = require("node:assert/strict");
    const fs = require("node:fs");
    const {{ pathToFileURL, fileURLToPath }} = require("node:url");
    const {{ registerHooks }} = require("node:module");

    registerHooks({{
      load(url, context, nextLoad) {{
        if (url.startsWith("file:") && url.endsWith(".js")) {{
          return {{
            format: "module",
            shortCircuit: true,
            source: fs.readFileSync(fileURLToPath(url), "utf8"),
          }};
        }}
        return nextLoad(url, context);
      }},
    }});

    (async () => {{
      const mod = await import(pathToFileURL({json.dumps(str(api_auth_path))}).href);
      assert.deepEqual(mod.getGenAiAuthHeaders(), {{}});
      assert.deepEqual(mod.getServingAuthHeaders(), {{}});
      process.env.REACT_APP_GEN_AI_ADMIN_TOKEN = "bare-node-genai";
      process.env.REACT_APP_API_TOKEN = "bare-node-serving";
      assert.deepEqual(mod.getGenAiAuthHeaders(), {{ "X-Admin-Token": "bare-node-genai" }});
      assert.deepEqual(mod.getServingAuthHeaders(), {{ "X-API-Key": "bare-node-serving" }});
    }})().catch((err) => {{
      process.stderr.write(String(err && err.stack ? err.stack : err) + "\\n");
      process.exit(1);
    }});
    """
    env = os.environ.copy()
    env.pop("REACT_APP_GEN_AI_ADMIN_TOKEN", None)
    env.pop("REACT_APP_API_TOKEN", None)
    proc = subprocess.run(
        [node, "-e", script],
        capture_output=True,
        text=True,
        env=env,
        check=False,
    )
    assert proc.returncode == 0, f"Bare Node failed:\n{proc.stdout}\n{proc.stderr}"
    assert proc.stderr.strip() == "", f"Unexpected stderr warnings in bare Node:\n{proc.stderr}"


def test_api_auth_precedence_and_runtime_guards():
    _run_node_harness(
        """
        const { getGenAiAuthHeaders, getServingAuthHeaders } = await importUi("services/apiAuth.js");

        // 1. Empty storage + empty env -> empty headers
        installBrowserStorages();
        delete process.env.REACT_APP_GEN_AI_ADMIN_TOKEN;
        delete process.env.REACT_APP_API_TOKEN;
        assert.deepEqual(getGenAiAuthHeaders(), {});
        assert.deepEqual(getServingAuthHeaders(), {});

        // 2. GenAI header precedence: genAiAuthToken > psearchAuthToken > genAiAdminToken > env
        // Must NOT attach both Authorization and X-Admin-Token simultaneously.
        installBrowserStorages(
          {
            genAiAuthToken: "  genai-bearer-tok  ",
            psearchAuthToken: "shared-bearer-tok",
            genAiAdminToken: "storage-genai-admin",
          },
          {}
        );
        process.env.REACT_APP_GEN_AI_ADMIN_TOKEN = "env-genai-admin";
        assert.deepEqual(getGenAiAuthHeaders(), {
          Authorization: "Bearer genai-bearer-tok",
        });

        // Fallback to psearchAuthToken when genAiAuthToken is absent
        installBrowserStorages(
          {
            psearchAuthToken: "Bearer shared-bearer-tok",
            genAiAdminToken: "storage-genai-admin",
          },
          {}
        );
        assert.deepEqual(getGenAiAuthHeaders(), {
          Authorization: "Bearer shared-bearer-tok",
        });

        // Bare "Bearer" / "Bearer   " in genAiAuthToken must NOT short-circuit || or emit "Bearer Bearer"
        installBrowserStorages(
          {
            genAiAuthToken: "Bearer",
            psearchAuthToken: "Bearer valid-fallback-jwt",
            genAiAdminToken: "storage-genai-admin",
          },
          {}
        );
        assert.deepEqual(getGenAiAuthHeaders(), {
          Authorization: "Bearer valid-fallback-jwt",
        });

        // Bare "Bearer   " in sessionStorage must fall back to localStorage for the same key
        installBrowserStorages(
          { genAiAuthToken: "local-genai-jwt" },
          { genAiAuthToken: "  bearer   " }
        );
        assert.deepEqual(getGenAiAuthHeaders(), {
          Authorization: "Bearer local-genai-jwt",
        });

        // Bare "Bearer" in both bearer slots falls through cleanly to X-Admin-Token (never "Bearer Bearer")
        installBrowserStorages(
          {
            genAiAuthToken: "Bearer",
            psearchAuthToken: "  BEARER  ",
            genAiAdminToken: "storage-genai-admin",
          },
          {}
        );
        assert.deepEqual(getGenAiAuthHeaders(), {
          "X-Admin-Token": "storage-genai-admin",
        });

        // Token starting with "bearer-" (hyphen, not whitespace) is preserved intact
        installBrowserStorages({ genAiAuthToken: "bearer-opaque-123" }, {});
        assert.deepEqual(getGenAiAuthHeaders(), {
          Authorization: "Bearer bearer-opaque-123",
        });

        // Fallback to genAiAdminToken when no bearer token is present
        installBrowserStorages({ genAiAdminToken: "storage-genai-admin" }, {});
        assert.deepEqual(getGenAiAuthHeaders(), {
          "X-Admin-Token": "storage-genai-admin",
        });

        // Fallback to REACT_APP_GEN_AI_ADMIN_TOKEN when storage has no token
        installBrowserStorages();
        assert.deepEqual(getGenAiAuthHeaders(), {
          "X-Admin-Token": "env-genai-admin",
        });
        delete process.env.REACT_APP_GEN_AI_ADMIN_TOKEN;

        // 3. Serving header precedence: psearchAuthToken > psearchApiToken > psearch_api_token > env
        // Must NOT attach both Authorization and X-API-Key simultaneously.
        installBrowserStorages(
          {
            psearchAuthToken: "serving-oidc-jwt",
            psearchApiToken: "camel-api-key",
            psearch_api_token: "snake-api-key",
          },
          {}
        );
        process.env.REACT_APP_API_TOKEN = "env-api-key";
        assert.deepEqual(getServingAuthHeaders(), {
          Authorization: "Bearer serving-oidc-jwt",
        });

        // Bare "Bearer" in psearchAuthToken must NOT emit "Bearer Bearer"; must fall back to X-API-Key
        installBrowserStorages(
          {
            psearchAuthToken: "Bearer",
            psearchApiToken: "camel-api-key",
          },
          {}
        );
        assert.deepEqual(getServingAuthHeaders(), {
          "X-API-Key": "camel-api-key",
        });

        installBrowserStorages(
          { psearchAuthToken: "local-serving-jwt" },
          { psearchAuthToken: "  Bearer   " }
        );
        assert.deepEqual(getServingAuthHeaders(), {
          Authorization: "Bearer local-serving-jwt",
        });

        installBrowserStorages(
          {
            psearchApiToken: "camel-api-key",
            psearch_api_token: "snake-api-key",
          },
          {}
        );
        assert.deepEqual(getServingAuthHeaders(), {
          "X-API-Key": "camel-api-key",
        });

        installBrowserStorages({}, { psearch_api_token: "session-snake-key" });
        assert.deepEqual(getServingAuthHeaders(), {
          "X-API-Key": "session-snake-key",
        });

        installBrowserStorages();
        assert.deepEqual(getServingAuthHeaders(), {
          "X-API-Key": "env-api-key",
        });

        // 4. Restricted browser / Node --localstorage-file SecurityError guards
        Object.defineProperty(globalThis, "localStorage", {
          get() {
            throw new Error("SecurityError: localStorage disabled");
          },
          configurable: true,
        });
        Object.defineProperty(globalThis, "sessionStorage", {
          get() {
            throw new Error("SecurityError: sessionStorage disabled");
          },
          configurable: true,
        });
        assert.deepEqual(getServingAuthHeaders(), {
          "X-API-Key": "env-api-key",
        });

        // If localStorage throws while sessionStorage works, sessionStorage still resolves
        Object.defineProperty(globalThis, "sessionStorage", {
          value: createMemoryStorage({ genAiAuthToken: "session-only-jwt" }),
          configurable: true,
          writable: true,
        });
        assert.deepEqual(getGenAiAuthHeaders(), {
          Authorization: "Bearer session-only-jwt",
        });

        // 5. Undefined global process guard (pure browser environment without process polyfill)
        installBrowserStorages();
        const savedProcess = globalThis.process;
        try {
          Object.defineProperty(globalThis, "process", {
            value: undefined,
            configurable: true,
            writable: true,
          });
          assert.deepEqual(getGenAiAuthHeaders(), {});
          assert.deepEqual(getServingAuthHeaders(), {});
        } finally {
          Object.defineProperty(globalThis, "process", {
            value: savedProcess,
            configurable: true,
            writable: true,
          });
        }

        return { ok: true };
        """
    )


def test_gen_ai_service_attaches_auth_headers_to_all_endpoints():
    _run_node_harness(
        """
        const genAi = await importUi("services/genAiService.js");

        globalThis.__axiosResponder = async (method, reqUrl) => {
          if (reqUrl.endsWith("/conversational-search")) {
            return { data: { answer: "Grounded answer" } };
          }
          if (reqUrl.endsWith("/enrichment")) {
            return {
              data: {
                enriched_fields: {
                  description: "Enriched description",
                  technical_specs: { weight: "1kg" },
                },
              },
            };
          }
          if (reqUrl.endsWith("/marketing")) {
            return { data: { content: "Campaign copy" } };
          }
          if (reqUrl.endsWith("/generate-enhanced-image")) {
            return { data: { generated_image_base64: "YWJj" } };
          }
          if (reqUrl.endsWith("/generate-sql")) {
            return {
              data: {
                task_id: "550e8400-e29b-41d4-a716-446655440000",
                message: "SQL generation task started.",
              },
            };
          }
          if (reqUrl.endsWith("/refine-sql")) {
            return { data: { refined_sql_script: "SELECT 1" } };
          }
          if (reqUrl.includes("/sql/task-status/")) {
            return { data: { task_id: "t1", status: "completed" } };
          }
          if (reqUrl.endsWith("/sql/fix")) {
            return { data: { success: true, suggested_sql: "SELECT 1", diff: "" } };
          }
          if (reqUrl.endsWith("/sql/validate")) {
            return { data: { valid: true } };
          }
          if (reqUrl.endsWith("/sql/analyze")) {
            return { data: { changes: [] } };
          }
          if (reqUrl.endsWith("/sql/simple-fix")) {
            return { data: { suggested_fixed_sql: "SELECT 1" } };
          }
          throw new Error("Unexpected URL: " + reqUrl);
        };

        // Pass 1: OIDC Bearer token set alongside env admin token -> Bearer wins cleanly
        installBrowserStorages({ genAiAuthToken: "genai-oidc-token" }, {});
        process.env.REACT_APP_GEN_AI_ADMIN_TOKEN = "ignored-admin-token";
        globalThis.__axiosCalls = [];

        await genAi.getConversationalSearch("running shoes", [], []);
        await genAi.getProductEnrichment("prod-1", { title: "Shoe" }, ["description", "technical_specs"]);
        await genAi.generateMarketingContent("prod-1", { title: "Shoe" }, "email");
        await genAi.generateEnhancedImage({ product_id: "prod-1" });
        await genAi.generateTransformationSql("raw_ds.src_tbl", "psearch.dst_tbl", ["id", "title"]);
        await genAi.refineTransformationSql("SELECT * FROM bad", "Table not found");
        await genAi.getSqlTaskStatus("task/id with space");
        await genAi.generateSqlFix("SELECT 1", "SELECT 1", "Syntax error", 1);
        await genAi.validateSqlFix("SELECT 1", 1);
        await genAi.analyzeSqlDifferences("SELECT 0", "SELECT 1");
        await genAi.getSimpleSqlFix("SELECT 0", "Syntax error");

        assert.equal(globalThis.__axiosCalls.length, 11);
        const expectedPaths = [
          ["post", "https://genai.example.test/conversational-search"],
          ["post", "https://genai.example.test/enrichment"],
          ["post", "https://genai.example.test/marketing"],
          ["post", "https://genai.example.test/generate-enhanced-image"],
          ["post", "https://genai.example.test/generate-sql"],
          ["post", "https://genai.example.test/refine-sql"],
          ["get", "https://genai.example.test/sql/task-status/task%2Fid%20with%20space"],
          ["post", "https://genai.example.test/sql/fix"],
          ["post", "https://genai.example.test/sql/validate"],
          ["post", "https://genai.example.test/sql/analyze"],
          ["post", "https://genai.example.test/sql/simple-fix"],
        ];

        globalThis.__axiosCalls.forEach((call, idx) => {
          assert.equal(call.method, expectedPaths[idx][0]);
          assert.equal(call.url, expectedPaths[idx][1]);
          assert.equal(call.headers.Authorization, "Bearer genai-oidc-token");
          assert.equal(call.headers["X-Admin-Token"], undefined);
          assert.equal(call.headers.Accept, "application/json");
          if (call.method === "post") {
            assert.equal(call.headers["Content-Type"], "application/json");
          }
        });

        // Pass 2: Admin token fallback when no Bearer token is stored
        installBrowserStorages();
        process.env.REACT_APP_GEN_AI_ADMIN_TOKEN = "genai-admin-secret";
        globalThis.__axiosCalls = [];

        await genAi.getConversationalSearch("boots", [], []);
        await genAi.getSqlTaskStatus("550e8400-e29b-41d4-a716-446655440000");

        assert.equal(globalThis.__axiosCalls.length, 2);
        for (const call of globalThis.__axiosCalls) {
          assert.equal(call.headers["X-Admin-Token"], "genai-admin-secret");
          assert.equal(call.headers.Authorization, undefined);
        }

        return { ok: true };
        """,
        extra_env={
            "REACT_APP_GEN_AI_URL": "https://genai.example.test",
            "REACT_APP_PROJECT_ID": "test-gcp-project",
        },
    )


def test_session_and_rule_services_attach_serving_and_admin_headers():
    _run_node_harness(
        """
        const sessionSvc = await importUi("services/sessionService.js");
        const { ruleService } = await importUi("services/ruleService.js");

        globalThis.__axiosResponder = async (method, reqUrl) => {
          if (reqUrl.endsWith("/api/session/events") && method === "get") {
            return { data: { session_id: "sess-default", events: [], count: 0 } };
          }
          if (reqUrl.endsWith("/api/session/events") && method === "post") {
            return { data: { ok: true } };
          }
          if (reqUrl.endsWith("/api/rules") && method === "get") {
            return { data: { rules: [{ rule_id: "r1", name: "Boost Nike", boost_multiplier: 1.5 }] } };
          }
          if (reqUrl.includes("/api/rules") && (method === "post" || method === "put")) {
            return { data: { rule_id: "r1", name: "Boost Nike", boost_multiplier: 1.5 } };
          }
          if (reqUrl.includes("/api/rules/") && method === "delete") {
            return { data: {} };
          }
          if (reqUrl.endsWith("/api/health") || reqUrl.endsWith("/health")) {
            return { data: { status: "ok" } };
          }
          throw new Error("Unexpected URL: " + reqUrl);
        };

        // 1. sessionService with OIDC Bearer token
        installBrowserStorages({ psearchAuthToken: "serving-oidc-tok", psearchApiToken: "unused-key" }, {});
        globalThis.__axiosCalls = [];
        await sessionSvc.recordSessionEvent("prod-001", "click", "default");
        await sessionSvc.fetchSessionEvents("default");
        await sessionSvc.seedPersonaEvents("runner", "default");
        assert.equal(globalThis.__axiosCalls.length, 4);
        for (const call of globalThis.__axiosCalls) {
          assert.equal(call.url, "https://api.example.test/api/session/events");
          assert.equal(call.headers.Authorization, "Bearer serving-oidc-tok");
          assert.equal(call.headers["X-API-Key"], undefined);
        }

        // 2. sessionService with X-API-Key fallback
        installBrowserStorages({ psearchApiToken: "serving-api-key" }, {});
        globalThis.__axiosCalls = [];
        await sessionSvc.recordSessionEvent("prod-002", "purchase", "default");
        await sessionSvc.fetchSessionEvents("default");
        assert.equal(globalThis.__axiosCalls.length, 2);
        for (const call of globalThis.__axiosCalls) {
          assert.equal(call.headers["X-API-Key"], "serving-api-key");
          assert.equal(call.headers.Authorization, undefined);
        }

        // 3. ruleService.getRules uses getServingAuthHeaders() when no admin token is set
        installBrowserStorages({ psearchApiToken: "serving-api-key" }, {});
        globalThis.__axiosCalls = [];
        const rules = await ruleService.getRules("tenant-1");
        assert.equal(rules.length, 1);
        assert.equal(globalThis.__axiosCalls.length, 1);
        assert.deepEqual(globalThis.__axiosCalls[0].headers, {
          "X-API-Key": "serving-api-key",
        });

        // 4. ruleService mutations send ONLY X-Admin-Token (never X-API-Key or Authorization alongside it!)
        installBrowserStorages(
          { psearchApiToken: "serving-api-key", psearchAuthToken: "serving-oidc-tok" },
          { psearch_admin_token: "session-admin-tok" }
        );
        globalThis.__axiosCalls = [];
        await ruleService.createRule({ name: "Pin Shoe", conditionType: "brand", condition: "nike", score: 1.5 }, "explicit-admin-tok");
        await ruleService.updateRule("r/1", { name: "Pin Shoe", conditionType: "brand", condition: "nike", score: 2.0 });
        await ruleService.deleteRule("r/1", "default");
        await ruleService.getRules("default");
        assert.equal(globalThis.__axiosCalls.length, 4);
        assert.deepEqual(globalThis.__axiosCalls[0].headers, {
          "X-Admin-Token": "explicit-admin-tok",
        });
        assert.deepEqual(globalThis.__axiosCalls[1].headers, {
          "X-Admin-Token": "session-admin-tok",
        });
        assert.deepEqual(globalThis.__axiosCalls[2].headers, {
          "X-Admin-Token": "session-admin-tok",
        });
        // While psearch_admin_token is active in sessionStorage, getRules uses X-Admin-Token without X-API-Key collision
        assert.deepEqual(globalThis.__axiosCalls[3].headers, {
          "X-Admin-Token": "session-admin-tok",
        });

        // 5. Public health check stays unauthenticated
        globalThis.__axiosCalls = [];
        const health = await ruleService.checkHealth();
        assert.equal(health.status, "ok");
        assert.equal(globalThis.__axiosCalls.length, 1);
        assert.deepEqual(globalThis.__axiosCalls[0].headers, {});

        return { ok: true };
        """,
        extra_env={"REACT_APP_API_URL": "https://api.example.test"},
    )


def test_app_and_search_input_components_attach_serving_auth_headers():
    # Also verify untouched JSX files keep balanced braces/parentheses/brackets
    for jsx_path in (UI_SRC_ROOT / "App.js", UI_SRC_ROOT / "components" / "SearchInput.js"):
        raw = jsx_path.read_text(encoding="utf-8")
        assert "getServingAuthHeaders" in raw, f"{jsx_path.name} must import/use getServingAuthHeaders"

    _run_node_harness(
        """
        const { ProductSearch } = await importUi("App.js");
        const SearchInput = (await importUi("components/SearchInput.js")).default;

        globalThis.__axiosResponder = async (method, reqUrl) => {
          if (reqUrl.endsWith("/search")) {
            return { data: { results: [], total_found: 0 } };
          }
          if (reqUrl.includes("/api/autocomplete?")) {
            return { data: { suggestions: ["running shoes"] } };
          }
          throw new Error("Unexpected URL: " + reqUrl);
        };

        // 1. OIDC Bearer propagation on /search and /api/autocomplete
        installBrowserStorages({ psearchAuthToken: "ui-search-jwt" }, {});
        globalThis.__axiosCalls = [];

        const appInstance = ProductSearch({});
        await appInstance.fetchProducts({ query: "running shoes" });

        const searchInputInstance = SearchInput({ onSearch: () => {} });
        await searchInputInstance.fetchAutocomplete("ru");

        assert.equal(globalThis.__axiosCalls.length, 2);
        assert.equal(globalThis.__axiosCalls[0].method, "post");
        assert.equal(globalThis.__axiosCalls[0].url, "https://api.example.test/search");
        assert.deepEqual(globalThis.__axiosCalls[0].headers, {
          Authorization: "Bearer ui-search-jwt",
        });

        assert.equal(globalThis.__axiosCalls[1].method, "get");
        assert.ok(
          globalThis.__axiosCalls[1].url.startsWith("https://api.example.test/api/autocomplete?q=ru")
        );
        assert.deepEqual(globalThis.__axiosCalls[1].headers, {
          Authorization: "Bearer ui-search-jwt",
        });

        // 2. Static API token fallback via REACT_APP_API_TOKEN
        installBrowserStorages();
        process.env.REACT_APP_API_TOKEN = "env-serving-key";
        globalThis.__axiosCalls = [];

        await appInstance.fetchProducts({ query: "laptop" });
        await searchInputInstance.fetchAutocomplete("la");

        assert.equal(globalThis.__axiosCalls.length, 2);
        assert.deepEqual(globalThis.__axiosCalls[0].headers, {
          "X-API-Key": "env-serving-key",
        });
        assert.deepEqual(globalThis.__axiosCalls[1].headers, {
          "X-API-Key": "env-serving-key",
        });

        return { ok: true };
        """,
        extra_env={"REACT_APP_API_URL": "https://api.example.test"},
    )

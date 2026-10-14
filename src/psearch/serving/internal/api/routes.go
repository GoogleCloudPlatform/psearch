package api

import (
	"net/http"
	"os"
	"path/filepath"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"psearch/serving-go/internal/config"
)

func SetupRouter(router *gin.Engine, cfg *config.Config) {
	router.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"*"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
	}))

	router.Use(LoggerMiddleware())

	controller, err := NewController(cfg)
	if err != nil {
		panic(err)
	}

	router.GET("/health", controller.HealthCheck)
	router.GET("/healthz", controller.HealthCheck)
	router.GET("/api/health", controller.HealthCheck)
	router.GET("/api/info", controller.Info)
	router.GET("/api/config/embedding-version", controller.GetEmbeddingVersion)
	router.POST("/api/config/embedding-version", controller.SetEmbeddingVersion)
	router.GET("/search", controller.Search)
	router.POST("/search", controller.Search)
	router.GET("/api/search", controller.Search)
	router.POST("/api/search", controller.Search)

	if cfg.StaticDir != "" {
		if info, err := os.Stat(cfg.StaticDir); err == nil && info.IsDir() {
			router.NoRoute(func(c *gin.Context) {
				reqPath := filepath.Clean(c.Request.URL.Path)
				fullPath := filepath.Join(cfg.StaticDir, reqPath)
				if st, err := os.Stat(fullPath); err == nil && !st.IsDir() {
					c.File(fullPath)
					return
				}
				c.File(filepath.Join(cfg.StaticDir, "index.html"))
			})
			return
		}
	}

	router.GET("/", func(c *gin.Context) {
		c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(defaultStorefrontHTML))
	})
}

const defaultStorefrontHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>psearch — Hybrid Vector & Full-Text Search (Cloud Spanner)</title>
  <style>
    :root { --bg: #0f172a; --card: #1e293b; --accent: #38bdf8; --text: #f8fafc; --muted: #94a3b8; --green: #22c55e; }
    * { box-sizing: border-box; }
    body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: var(--bg); color: var(--text); }
    header { padding: 20px 32px; border-bottom: 1px solid #334155; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 12px; }
    .badge { background: #0284c7; color: #fff; padding: 4px 10px; border-radius: 999px; font-size: 12px; font-weight: 600; }
    main { max-width: 1200px; margin: 0 auto; padding: 24px 32px; }
    .search-bar { display: flex; gap: 12px; align-items: center; background: var(--card); padding: 16px; border-radius: 12px; border: 1px solid #334155; flex-wrap: wrap; }
    input[type="text"] { flex: 1; min-width: 240px; padding: 12px 16px; border-radius: 8px; border: 1px solid #475569; background: #0f172a; color: #fff; font-size: 16px; }
    .slider-wrap { display: flex; align-items: center; gap: 8px; font-size: 14px; color: var(--muted); }
    button { background: var(--accent); color: #0f172a; border: none; padding: 12px 20px; border-radius: 8px; font-weight: 700; cursor: pointer; }
    .meta { margin: 16px 0; color: var(--muted); font-size: 14px; display: flex; gap: 20px; }
    .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(280px, 1fr)); gap: 16px; }
    .card { background: var(--card); border: 1px solid #334155; border-radius: 12px; padding: 16px; display: flex; flex-direction: column; gap: 8px; }
    .card h3 { margin: 0; font-size: 16px; }
    .price { font-size: 20px; font-weight: 700; color: var(--accent); }
    .stock-in { color: var(--green); font-size: 12px; font-weight: 600; }
    .stock-out { color: #f87171; font-size: 12px; font-weight: 600; }
    .scores { font-family: monospace; font-size: 11px; color: var(--muted); background: #0f172a; padding: 6px 8px; border-radius: 6px; }
    select { padding: 10px 14px; border-radius: 8px; border: 1px solid #475569; background: #0f172a; color: #fff; font-size: 14px; }
    .checkbox-wrap { display: flex; align-items: center; gap: 6px; font-size: 14px; color: var(--muted); cursor: pointer; user-select: none; }
    input[type="checkbox"] { width: 16px; height: 16px; accent-color: var(--accent); cursor: pointer; }
  </style>
</head>
<body>
  <header>
    <div>
      <h1 style="margin:0;font-size:22px;">psearch — Enterprise Hybrid & Vector Search</h1>
      <div style="color:var(--muted);font-size:13px;">Powered by Cloud Spanner (RRF Vector + Full-Text Search) & Vertex AI</div>
    </div>
    <div style="display:flex;gap:8px;align-items:center;">
      <span class="badge" data-testid="repo-badge">Repo: GoogleCloudPlatform/psearch</span>
      <span class="badge" id="backend-badge">Spanner</span>
      <span class="badge" id="retrieval-badge">Retrieval: -</span>
    </div>
  </header>
  <main>
    <div class="search-bar">
      <input type="text" id="q" value="running shoes" placeholder="Search products (e.g. running shoes, tenis corrida, laptop, geladeira)..." />
      <div class="slider-wrap">
        <span>FTS (0.0)</span>
        <input type="range" id="alpha" min="0" max="1" step="0.1" value="0.5" oninput="document.getElementById('alpha-val').textContent=this.value" />
        <span>Vector (1.0) · α=<strong id="alpha-val">0.5</strong></span>
      </div>
      <select id="channel">
        <option value="">All Channels</option>
        <option value="web">web</option>
        <option value="mobile">mobile</option>
        <option value="store">store</option>
      </select>
      <label class="checkbox-wrap">
        <input type="checkbox" id="only_in_stock" /> Only in stock
      </label>
      <button onclick="runSearch()">Search</button>
    </div>
    <div class="meta" id="meta">Loading catalog...</div>
    <div class="grid" id="results"></div>
  </main>
  <script>
    async function runSearch() {
      const q = document.getElementById('q').value;
      const alpha = document.getElementById('alpha').value;
      const channel = document.getElementById('channel').value;
      const onlyInStock = document.getElementById('only_in_stock').checked;
      let url = '/api/search?q=' + encodeURIComponent(q) + '&alpha=' + encodeURIComponent(alpha);
      if (channel) {
        url += '&channel=' + encodeURIComponent(channel);
      }
      if (onlyInStock) {
        url += '&only_in_stock=true';
      }
      const res = await fetch(url);
      const data = await res.json();
      document.getElementById('backend-badge').textContent = 'Backend: ' + (data.backend_mode || 'spanner');
      if (document.getElementById('retrieval-badge')) {
        document.getElementById('retrieval-badge').textContent = 'Mode: ' + (data.retrieval_mode || 'hybrid');
      }
      document.getElementById('meta').textContent =
        'Found ' + data.total_found + ' products in ' + (data.latency_ms || 0).toFixed(2) + ' ms (effective_alpha=' + (data.effective_alpha !== undefined ? data.effective_alpha : alpha) + ', retrieval_mode=' + (data.retrieval_mode || 'n/a') + ')';
      const container = document.getElementById('results');
      container.innerHTML = (data.results || []).map(p => {
        const s = p.score || {};
        const isStockIn = p.availability === 'IN_STOCK';
        const stockClass = isStockIn ? 'stock-in' : 'stock-out';
        const stockBadge = isStockIn
          ? (p.availableQuantity !== undefined && p.availableQuantity !== null
              ? 'IN_STOCK (' + p.availableQuantity + ' unid.)'
              : 'IN_STOCK')
          : 'OUT_OF_STOCK';
        const finalScore = s.final_score !== undefined ? s.final_score : (s.hybrid !== undefined ? s.hybrid : 0);
        const vecScore = s.vector !== undefined ? s.vector : 0;
        const txtScore = s.text !== undefined ? s.text : 0;
        return '<div class="card">' +
          '<div style="display:flex;justify-content:space-between;"><span class="' + stockClass + '">' + stockBadge + '</span><span style="color:#94a3b8;font-size:12px;">' + (p.categories || []).join(', ') + '</span></div>' +
          '<h3>' + p.title + '</h3>' +
          '<div style="color:#94a3b8;font-size:13px;">' + (p.description || '') + '</div>' +
          '<div class="price">$' + ((p.priceInfo && p.priceInfo.price) || '0.00') + '</div>' +
          '<div class="scores">hybrid=' + finalScore.toFixed(4) + ' | vec=' + vecScore.toFixed(4) + ' | txt=' + txtScore.toFixed(4) + '</div>' +
        '</div>';
      }).join('');
    }
    runSearch();
  </script>
</body>
</html>`

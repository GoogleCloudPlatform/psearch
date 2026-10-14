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
	router.GET("/api/synonyms", controller.ListSynonyms)
	router.POST("/api/synonyms", controller.UpsertSynonym)
	router.GET("/api/autocomplete", controller.Autocomplete)
	router.GET("/api/rules", controller.ListRules)
	router.POST("/api/rules", controller.CreateRule)
	router.PUT("/api/rules/:id", controller.UpdateRule)
	router.DELETE("/api/rules/:id", controller.DeleteRule)
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
    .facet-group { margin-bottom: 14px; }
    .facet-title { font-size: 13px; font-weight: 600; color: #cbd5e1; margin-bottom: 6px; }
    .facet-list { font-size: 12px; color: var(--muted); display: flex; flex-direction: column; gap: 4px; }
    .facet-item { display: flex; justify-content: space-between; align-items: center; padding: 2px 0; }
    .facet-count { background: #0f172a; padding: 1px 6px; border-radius: 999px; font-size: 11px; color: #cbd5e1; }
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
      <span class="badge" id="tenant-badge" style="display:none;"></span>
      <span class="badge" id="locale-badge" style="display:none;"></span>
      <span class="badge" id="ltr-badge" style="display:none;"></span>
    </div>
  </header>
  <main>
    <div class="search-bar">
      <input type="text" id="q" list="q-suggestions" value="running shoes" placeholder="Search products (e.g. running shoes, tenis corrida, laptop, geladeira)..." autocomplete="off" onkeydown="if(event.key==='Enter') runSearch()" />
      <datalist id="q-suggestions"></datalist>
      <div class="slider-wrap">
        <span>FTS (0.0)</span>
        <input type="range" id="alpha" min="0" max="1" step="0.1" value="0.5" oninput="document.getElementById('alpha-val').textContent=this.value" />
        <span>Vector (1.0) · α=<strong id="alpha-val">0.5</strong></span>
      </div>
      <input type="text" id="tenant_id" value="default" placeholder="Tenant" style="flex:0 0 110px;min-width:90px;" title="Tenant ID" />
      <select id="locale" title="Locale">
        <option value="">All Locales</option>
        <option value="pt-BR">pt-BR</option>
        <option value="en-US">en-US</option>
        <option value="es-MX">es-MX</option>
      </select>
      <select id="channel">
        <option value="">All Channels</option>
        <option value="web">web</option>
        <option value="mobile">mobile</option>
        <option value="store">store</option>
      </select>
      <input type="text" id="cep" placeholder="CEP (01310-100)" style="flex:0 0 130px;min-width:110px;" title="Calcular Entrega" onkeydown="if(event.key==='Enter') runSearch()" />
      <label class="checkbox-wrap">
        <input type="checkbox" id="max_sla_24h" /> 24h
      </label>
      <label class="checkbox-wrap">
        <input type="checkbox" id="only_in_stock" /> Only in stock
      </label>
      <label class="checkbox-wrap">
        <input type="checkbox" id="ltr" /> Re-rank (LTR)
      </label>
      <button onclick="runSearch()">Search</button>
    </div>
    <div style="display:flex;gap:24px;margin-top:20px;align-items:flex-start;">
      <aside id="facet-sidebar" style="width:230px;flex-shrink:0;background:var(--card);padding:16px;border-radius:12px;border:1px solid #334155;">
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;">
          <h3 style="margin:0;font-size:15px;color:var(--accent);">Facets</h3>
          <span id="facets-latency" style="font-size:11px;color:var(--muted);"></span>
        </div>
        <div class="facet-group">
          <div class="facet-title">Department</div>
          <div class="facet-list" id="facets-cat-list">-</div>
        </div>
        <div class="facet-group">
          <div class="facet-title">Brands</div>
          <div class="facet-list" id="facets-brand-list">-</div>
        </div>
        <div class="facet-group">
          <div class="facet-title">Price Ranges</div>
          <div class="facet-list" id="facets-price-list">-</div>
        </div>
        <div class="facet-group">
          <div class="facet-title">Availability</div>
          <div class="facet-list" id="facets-avail-list">-</div>
        </div>
      </aside>
      <div style="flex:1;min-width:0;">
        <div class="meta" id="meta">Loading catalog...</div>
        <div id="expanded-terms" style="color:var(--muted);font-size:13px;margin:-8px 0 16px 0;display:none;"></div>
        <div class="grid" id="results"></div>
      </div>
    </div>
  </main>
  <script>
    async function runSearch() {
      const q = document.getElementById('q').value;
      const alpha = document.getElementById('alpha').value;
      const channel = document.getElementById('channel').value;
      const onlyInStock = document.getElementById('only_in_stock').checked;
      const tenantId = document.getElementById('tenant_id') ? document.getElementById('tenant_id').value : 'default';
      const locale = document.getElementById('locale') ? document.getElementById('locale').value : '';
      const cep = document.getElementById('cep') ? document.getElementById('cep').value.trim() : '';
      const maxSla24h = document.getElementById('max_sla_24h') ? document.getElementById('max_sla_24h').checked : false;
      const ltr = document.getElementById('ltr') ? document.getElementById('ltr').checked : false;
      let url = '/api/search?q=' + encodeURIComponent(q) + '&alpha=' + encodeURIComponent(alpha);
      if (channel) {
        url += '&channel=' + encodeURIComponent(channel);
      }
      if (onlyInStock) {
        url += '&only_in_stock=true';
      }
      if (tenantId) {
        url += '&tenant_id=' + encodeURIComponent(tenantId);
      }
      if (locale) {
        url += '&locale=' + encodeURIComponent(locale);
      }
      if (cep) {
        url += '&cep=' + encodeURIComponent(cep);
      }
      if (maxSla24h) {
        url += '&max_sla_hours=24';
      }
      if (ltr) {
        url += '&ltr=true';
      }
      const res = await fetch(url);
      const data = await res.json();
      if (!res.ok) {
        document.getElementById('meta').textContent = 'Error: ' + (data.error || ('Request failed with status ' + res.status));
        document.getElementById('results').innerHTML = '';
        return;
      }
      document.getElementById('backend-badge').textContent = 'Backend: ' + (data.backend_mode || 'spanner');
      if (document.getElementById('retrieval-badge')) {
        document.getElementById('retrieval-badge').textContent = 'Mode: ' + (data.retrieval_mode || 'hybrid');
      }
      if (document.getElementById('tenant-badge')) {
        if (data.tenant_id) {
          document.getElementById('tenant-badge').textContent = 'Tenant: ' + data.tenant_id;
          document.getElementById('tenant-badge').style.display = 'inline-block';
        } else {
          document.getElementById('tenant-badge').style.display = 'none';
        }
      }
      if (document.getElementById('locale-badge')) {
        if (data.locale) {
          document.getElementById('locale-badge').textContent = 'Locale: ' + data.locale;
          document.getElementById('locale-badge').style.display = 'inline-block';
        } else {
          document.getElementById('locale-badge').style.display = 'none';
        }
      }
      if (document.getElementById('ltr-badge')) {
        if (data.ltr) {
          document.getElementById('ltr-badge').textContent = 'LTR: ' + (data.ltr_model || 'active') + (data.ltr_latency_us !== undefined ? ' (' + data.ltr_latency_us + 'µs)' : '');
          document.getElementById('ltr-badge').style.display = 'inline-block';
        } else {
          document.getElementById('ltr-badge').style.display = 'none';
        }
      }
      const facetLat = data.facets_latency_ms !== undefined ? data.facets_latency_ms : (data.facets && data.facets.latency_ms);
      if (facetLat !== undefined && facetLat !== null) {
        document.getElementById('facets-latency').textContent = Number(facetLat).toFixed(1) + 'ms';
      } else {
        document.getElementById('facets-latency').textContent = '';
      }
      document.getElementById('meta').textContent =
        'Found ' + data.total_found + ' products in ' + (data.latency_ms || 0).toFixed(2) + ' ms (effective_alpha=' + (data.effective_alpha !== undefined ? data.effective_alpha : alpha) + ', retrieval_mode=' + (data.retrieval_mode || 'n/a') + (data.tenant_id ? ', tenant=' + data.tenant_id : '') + (data.locale ? ', locale=' + data.locale : '') + (facetLat !== undefined && facetLat !== null ? ', facets=' + Number(facetLat).toFixed(1) + 'ms' : '') + (data.ltr ? ', ltr=' + (data.ltr_model || 'active') + ' (' + (data.ltr_latency_us || 0) + 'µs)' : '') + ')';
      
      const expEl = document.getElementById('expanded-terms');
      if (expEl) {
        if (data.expanded_terms && data.expanded_terms.length > 0) {
          expEl.textContent = 'Also matching: ' + data.expanded_terms.join(', ');
          expEl.style.display = 'block';
        } else {
          expEl.textContent = '';
          expEl.style.display = 'none';
        }
      }
      
      const facets = data.facets || {};
      const renderFacetList = (list) => {
        if (!list || !list.length) return '<span style="color:#64748b;font-size:11px;">(none)</span>';
        return list.map(item => {
          const val = typeof item === 'object' ? item.value : item;
          const cnt = typeof item === 'object' ? item.count : 0;
          return '<div class="facet-item"><span>' + val + '</span><span class="facet-count">' + cnt + '</span></div>';
        }).join('');
      };
      document.getElementById('facets-cat-list').innerHTML = renderFacetList(facets.categories);
      document.getElementById('facets-brand-list').innerHTML = renderFacetList(facets.brands);
      document.getElementById('facets-price-list').innerHTML = renderFacetList(facets.price_ranges);
      document.getElementById('facets-avail-list').innerHTML = renderFacetList(facets.availability);
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
        const locBadge = p.locale
          ? '<span class="badge" style="background:#1e293b;border:1px solid #38bdf8;color:#38bdf8;font-size:11px;padding:2px 6px;">' + p.locale + '</span>'
          : '';
        const finalScore = s.final_score !== undefined ? s.final_score : (s.hybrid !== undefined ? s.hybrid : 0);
        const vecScore = s.vector !== undefined ? s.vector : 0;
        const txtScore = s.text !== undefined ? s.text : 0;
        const ltrScore = s.ltr_score !== undefined ? ' | ltr=' + s.ltr_score.toFixed(4) : '';
        return '<div class="card">' +
          '<div style="display:flex;justify-content:space-between;align-items:center;"><div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap;"><span class="' + stockClass + '">' + stockBadge + '</span>' + locBadge + '<span class="rank-badge" style="display:none;"></span><span class="delivery-badge" style="display:none;"></span></div><span style="color:#94a3b8;font-size:12px;">' + (p.categories || []).join(', ') + '</span></div>' +
          '<h3>' + p.title + '</h3>' +
          '<div style="color:#94a3b8;font-size:13px;">' + (p.description || '') + '</div>' +
          '<div class="price">$' + ((p.priceInfo && p.priceInfo.price) || '0.00') + '</div>' +
          '<div class="scores">hybrid=' + finalScore.toFixed(4) + ' | vec=' + vecScore.toFixed(4) + ' | txt=' + txtScore.toFixed(4) + ltrScore + '</div>' +
        '</div>';
      }).join('');
      (data.results || []).forEach((p, i) => {
        const card = container.children[i];
        if (!card) return;
        const s = p.score || {};
        if (s.stage1_rank !== undefined && s.stage2_rank !== undefined) {
          const rankEl = card.querySelector('.rank-badge');
          if (rankEl) {
            const diff = s.stage1_rank - s.stage2_rank;
            let rankText = s.stage1_rank + '→' + s.stage2_rank;
            if (diff > 0) {
              rankText += ' (↑' + diff + ')';
            } else if (diff < 0) {
              rankText += ' (↓' + Math.abs(diff) + ')';
            }
            rankEl.textContent = rankText;
            rankEl.style.display = 'inline-block';
            rankEl.style.background = '#1e293b';
            rankEl.style.border = '1px solid #a855f7';
            rankEl.style.color = '#d8b4fe';
            rankEl.style.fontSize = '11px';
            rankEl.style.padding = '2px 6px';
            rankEl.style.borderRadius = '4px';
          }
        }
        if (p.deliveryPromise && p.deliveryPromise.label) {
          const badge = card.querySelector('.delivery-badge');
          if (badge) {
            let labelText = p.deliveryPromise.label;
            if (p.deliveryPromise.free_shipping) {
              labelText += ' · Frete grátis';
            }
            badge.textContent = labelText;
            badge.style.display = 'inline-block';
            badge.style.background = '#064e3b';
            badge.style.border = '1px solid #10b981';
            badge.style.color = '#6ee7b7';
            badge.style.fontSize = '11px';
            badge.style.padding = '2px 6px';
            badge.style.borderRadius = '4px';
          }
        }
      });
    }
    const qInput = document.getElementById('q');
    const qDatalist = document.getElementById('q-suggestions');
    let autoTimer = null;
    if (qInput && qDatalist) {
      qInput.addEventListener('input', function() {
        if (autoTimer) clearTimeout(autoTimer);
        const val = qInput.value.trim();
        if (val.length < 2) {
          while (qDatalist.firstChild) qDatalist.removeChild(qDatalist.firstChild);
          return;
        }
        autoTimer = setTimeout(async function() {
          const tenantId = document.getElementById('tenant_id') ? document.getElementById('tenant_id').value : 'default';
          const locale = document.getElementById('locale') ? document.getElementById('locale').value : '';
          let autoUrl = '/api/autocomplete?q=' + encodeURIComponent(val) + '&limit=8';
          if (tenantId) autoUrl += '&tenant_id=' + encodeURIComponent(tenantId);
          if (locale) autoUrl += '&locale=' + encodeURIComponent(locale);
          try {
            const res = await fetch(autoUrl);
            if (!res.ok) return;
            const data = await res.json();
            while (qDatalist.firstChild) qDatalist.removeChild(qDatalist.firstChild);
            (data.suggestions || []).forEach(function(s) {
              const opt = document.createElement('option');
              opt.value = typeof s === 'object' ? s.text : s;
              qDatalist.appendChild(opt);
            });
          } catch (e) {
            // ignore
          }
        }, 150);
      });
    }
    runSearch();
  </script>
</body>
</html>`

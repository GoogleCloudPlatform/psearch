package api

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// the embedded storefront's inline JS must parse. A missing
// closing brace (shipped in rev 00024/00025) left the page stuck on "Loading catalog...".
func TestStorefrontInlineJSParses(t *testing.T) {
	scripts := regexp.MustCompile(`(?s)<script[^>]*>(.*?)</script>`).FindAllStringSubmatch(defaultStorefrontHTML, -1)
	if len(scripts) == 0 {
		t.Fatal("storefront has no inline <script>")
	}
	var js strings.Builder
	for _, m := range scripts {
		js.WriteString(m[1])
		js.WriteString("\n")
	}
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed; skipping JS syntax check")
	}
	path := filepath.Join(t.TempDir(), "storefront.js")
	if err := os.WriteFile(path, []byte(js.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(node, "--check", path).CombinedOutput(); err != nil {
		t.Fatalf("storefront inline JS has a syntax error:\n%s", out)
	}
}

func TestStorefrontShowAllCatalog(t *testing.T) {
	for _, frag := range []string{`id="show-all-btn"`, "Show All Catalog", "function showAllCatalog()", "&limit=100"} {
		if !strings.Contains(defaultStorefrontHTML, frag) {
			t.Errorf("storefront missing %q", frag)
		}
	}
	if strings.Contains(defaultStorefrontHTML, `value="running shoes"`) {
		t.Error("initial query must be empty so the catalog loads on page load")
	}
}

// empty / "*" queries browse the full catalog (filters still apply).
func TestCatalogBrowse(t *testing.T) {
	r := newTestRouter()
	for _, q := range []string{"q=&limit=100", "q=*&limit=100"} {
		resp := doSearch(t, r, q)
		if len(resp.Results) != 15 || resp.DidYouMean != "" || resp.QueryRelaxed {
			t.Fatalf("%s: results=%d did_you_mean=%q relaxed=%v", q, len(resp.Results), resp.DidYouMean, resp.QueryRelaxed)
		}
		if resp.Facets == nil || len(resp.Facets.Categories) == 0 {
			t.Fatalf("%s: catalog browse must return facets", q)
		}
		last := resp.Results[len(resp.Results)-1]
		if last.Availability != "OUT_OF_STOCK" {
			t.Fatalf("%s: out-of-stock products must be listed last", q)
		}
	}
	if pt := doSearch(t, r, "q=*&limit=100&locale=pt-BR"); len(pt.Results) != 5 {
		t.Fatalf("browse must honor filters: pt-BR=%d, want 5", len(pt.Results))
	}
}

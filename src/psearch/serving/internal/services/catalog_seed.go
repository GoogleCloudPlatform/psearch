package services

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"math"
	"strings"

	"psearch/serving-go/internal/models"
)

// SearchStore abstracts Spanner vs in-memory search so unit tests run in <1s and Cloud Run uses Spanner with graceful fallback.
type SearchStore interface {
	HybridSearch(query string, limit int, minScore float64, alpha float64) ([]models.SearchResult, string, error)
	BackendMode() string
	ProductCount() int
}

// DeterministicEmbedding computes a normalized 768-dim semantic vector from text tokens so vector similarity works deterministically.
func DeterministicEmbedding(text string, dim int) []float64 {
	if dim <= 0 {
		dim = 768
	}
	vec := make([]float64, dim)
	tokens := strings.Fields(strings.ToLower(text))
	if len(tokens) == 0 {
		tokens = []string{"empty"}
	}
	for _, tok := range tokens {
		// Expand common semantic clusters so vector search captures cross-lingual & conceptual similarity even in fallback mode
		expanded := []string{tok}
		switch tok {
		case "running", "corrida", "maratona", "run", "runner", "jogging", "esportivo":
			expanded = append(expanded, "semantic_sport_run", "semantic_footwear", "tenis")
		case "shoes", "shoe", "sneakers", "sneaker", "tenis", "tênis", "calcado", "calçado", "footwear":
			expanded = append(expanded, "semantic_footwear", "semantic_sport_run")
		case "laptop", "notebook", "macbook", "ultrabook", "computer", "computador", "pc", "dev":
			expanded = append(expanded, "semantic_compute_laptop", "semantic_electronics")
		case "phone", "smartphone", "celular", "iphone", "galaxy", "android", "5g":
			expanded = append(expanded, "semantic_mobile_phone", "semantic_electronics")
		case "headphones", "headphone", "fone", "earbuds", "bluetooth", "audio", "noise":
			expanded = append(expanded, "semantic_audio_headphone", "semantic_electronics")
		case "fridge", "refrigerator", "geladeira", "refrigerador", "inox", "frost":
			expanded = append(expanded, "semantic_appliance_fridge", "semantic_home")
		case "tv", "television", "televisao", "smarttv", "oled", "4k", "cinema":
			expanded = append(expanded, "semantic_tv_display", "semantic_electronics")
		case "coffee", "cafe", "café", "espresso", "cafeteira", "barista":
			expanded = append(expanded, "semantic_coffee", "semantic_home")
		}
		for _, item := range expanded {
			sum := sha256.Sum256([]byte(item))
			for i := 0; i < 8; i++ {
				idx := int(binary.LittleEndian.Uint32(sum[i*4:(i+1)*4])) % dim
				if idx < 0 {
					idx = -idx
				}
				sign := 1.0
				if sum[i]%2 == 1 {
					sign = -1.0
				}
				vec[idx] += sign * 1.0
			}
		}
	}
	var norm float64
	for _, v := range vec {
		norm += v * v
	}
	norm = math.Sqrt(norm)
	if norm > 0 {
		for i := range vec {
			vec[i] /= norm
		}
	}
	return vec
}

func CosineSimilarity(a, b []float64) float64 {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	var dot, na, nb float64
	for i := 0; i < n; i++ {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

func DefaultSeedProducts() []models.SearchResult {
	qty := func(v int) *int { return &v }
	items := []struct {
		id, title, desc, brand, cat string
		price                       float64
		stock                       int
		avail                       string
		channels                    []string
		locale                      string
	}{
		{"prod-001", "UltraBoost Pro Running Shoes", "Lightweight carbon-plate marathon running shoes with responsive cushioning", "Adidas", "Footwear", 189.90, 42, "IN_STOCK", []string{"web", "mobile", "store"}, "en-US"},
		{"prod-002", "Tênis de Corrida Nimbus Cloud", "Calçado esportivo de alta absorção de impacto para corrida de rua e maratona", "Asics", "Footwear", 169.90, 28, "IN_STOCK", []string{"web", "mobile"}, "pt-BR"},
		{"prod-003", "Air Zoom Pegasus Trail Runner", "All-terrain waterproof trail running sneaker with grip outsole", "Nike", "Footwear", 149.50, 19, "IN_STOCK", []string{"web", "mobile", "store"}, "en-US"},
		{"prod-004", "Classic Leather Urban Sneaker", "Casual everyday white leather shoes for streetwear comfort", "Reebok", "Footwear", 95.00, 0, "OUT_OF_STOCK", []string{"web"}, "en-US"},
		{"prod-005", "ProBook X1 Carbon Developer Laptop 14\"", "Ultra-lightweight 32GB RAM 1TB NVMe Linux & Cloud engineering notebook", "Lenovo", "Computers", 1499.00, 15, "IN_STOCK", []string{"web", "mobile"}, "en-US"},
		{"prod-006", "Notebook Gamer Predator RTX 4070", "Computador portátil de alta performance com tela 165Hz e 32GB DDR5", "Acer", "Computers", 1650.00, 9, "IN_STOCK", []string{"web", "mobile", "store"}, "pt-BR"},
		{"prod-007", "MacBook Air M3 15-inch Workstation", "Silent fanless laptop with 18-hour battery life and Liquid Retina display", "Apple", "Computers", 1399.00, 24, "IN_STOCK", []string{"web", "mobile", "store"}, "en-US"},
		{"prod-008", "QuietComfort Ultra Noise Cancelling Headphones", "Wireless Bluetooth over-ear headphones with spatial audio and 24h battery", "Bose", "Audio", 349.00, 31, "IN_STOCK", []string{"web", "mobile", "store"}, "en-US"},
		{"prod-009", "Fone de Ouvido Sem Fio WH-1000XM5", "Cancelamento de ruído ativo líder da indústria e chamadas cristalinas via Bluetooth", "Sony", "Audio", 329.00, 18, "IN_STOCK", []string{"web", "mobile"}, "pt-BR"},
		{"prod-010", "Galaxy S25 Ultra 5G 512GB Smartphone", "Flagship Android phone with 200MP camera, titanium frame and AI assistant", "Samsung", "Smartphones", 1199.00, 50, "IN_STOCK", []string{"web", "mobile", "store"}, "en-US"},
		{"prod-011", "Geladeira Inverse Frost Free Inox 540L", "Refrigerador inteligente com dispenser de água, compressor inverter econômico", "Brastemp", "Appliances", 899.00, 7, "IN_STOCK", []string{"web", "store"}, "pt-BR"},
		{"prod-012", "Smart French Door Refrigerator 600L", "Stainless steel energy-star kitchen fridge with dual ice maker", "LG", "Appliances", 1299.00, 0, "OUT_OF_STOCK", []string{"web"}, "en-US"},
		{"prod-013", "Cafeteira Espresso Barista Pro", "Máquina de café espresso com moedor integrado e vaporizador de leite", "Breville", "Appliances", 599.00, 14, "IN_STOCK", []string{"web", "mobile", "store"}, "pt-BR"},
		{"prod-014", "OLED 65\" 4K Dolby Vision Smart TV", "120Hz gaming and home theater cinema television with self-lit pixels", "LG", "TV & Video", 1599.00, 11, "IN_STOCK", []string{"web", "mobile", "store"}, "en-US"},
		{"prod-015", "Mechanical Wireless Ergonomic Keyboard", "Hot-swappable tactile switches with multi-device Bluetooth and USB-C", "Keychron", "Accessories", 119.00, 64, "IN_STOCK", []string{"web", "mobile"}, "en-US"},
	}

	out := make([]models.SearchResult, 0, len(items))
	for _, it := range items {
		out = append(out, models.SearchResult{
			ID:          it.id,
			Name:        fmt.Sprintf("projects/psearch-project/locations/global/catalogs/default_catalog/branches/0/products/%s", it.id),
			Title:       it.title,
			Description: it.desc,
			Brands:      []string{it.brand},
			Categories:  []string{it.cat},
			PriceInfo: models.PriceInfo{
				CurrencyCode:  "USD",
				Price:         fmt.Sprintf("%.2f", it.price),
				OriginalPrice: fmt.Sprintf("%.2f", it.price*1.15),
				NumericPrice:  it.price,
			},
			Availability:      it.avail,
			AvailableQuantity: qty(it.stock),
			Channels:          it.channels,
			Locale:            it.locale,
			TenantID:          "default",
			URI:               fmt.Sprintf("https://psearch.example.com/products/%s", it.id),
			Images: []models.Image{
				{Height: "400", Width: "400", URI: fmt.Sprintf("https://picsum.photos/seed/%s/400/400", it.id)},
			},
			Score:   map[string]float64{},
			Signals: seedSignals(it.id),
		})
	}
	return out
}

// seedSignals returns deterministic demo values for the 8 ADR-004 boost signals
// (mirrored by migration 016b for the Spanner catalog).
func seedSignals(id string) *models.BoostSignals {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	n := h.Sum32()
	pick := func(shift uint) float64 { return float64((n>>shift)%100) / 100 } // [0, 0.99]
	return &models.BoostSignals{
		Clicks:          int64(50 + n%950),
		Orders:          int64(5 + (n>>4)%95),
		DiscountPct:     float64((n >> 8) % 40),
		PopularityScore: pick(12),
		MarginPct:       float64(10 + (n>>16)%40),
		CTR:             0.01 + pick(20)*0.2,
		ConversionRate:  0.005 + pick(24)*0.1,
		RecencyScore:    pick(28),
	}
}

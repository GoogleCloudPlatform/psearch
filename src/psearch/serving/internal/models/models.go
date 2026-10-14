package models

type SearchRequest struct {
	Query       string   `json:"query" form:"q"`
	Limit       *int     `json:"limit,omitempty" form:"limit"`
	MinScore    *float64 `json:"min_score,omitempty" form:"min_score"`
	Alpha       *float64 `json:"alpha,omitempty" form:"alpha"`
	Category    string   `json:"category,omitempty" form:"category"`
	Brand       string   `json:"brand,omitempty" form:"brand"`
	OnlyInStock bool     `json:"only_in_stock,omitempty" form:"only_in_stock"`
	Channel     string   `json:"channel,omitempty" form:"channel"`
	TenantID    string   `json:"tenant_id,omitempty" form:"tenant_id"`
	Locale      string   `json:"locale,omitempty" form:"locale"`
	CEP         string   `json:"cep,omitempty" form:"cep"`
	LTR         bool     `json:"ltr,omitempty" form:"ltr"`
}

// SearchFilters are the catalog filters applied server-side (ADR-002, Issues #3/#4/#5).
type SearchFilters struct {
	OnlyInStock bool
	Channel     string
	Category    string // exact (case-insensitive) category match; empty = all (Issue #5 facet drill-down)
	Brand       string // exact (case-insensitive) brand match; empty = all
	TenantID    string // ADR-002 / Issue #4; empty = "default"
	Locale      string // ADR-002 / Issue #4 (pt-BR, en-US, es-MX); empty = all locales
}

type SearchResponse struct {
	Query            string         `json:"query"`
	EffectiveAlpha   float64        `json:"effective_alpha"`
	Results          []SearchResult `json:"results"`
	TotalFound       int            `json:"total_found"`
	Facets           *Facets        `json:"facets,omitempty"`
	LatencyMs        float64        `json:"latency_ms"`
	BackendMode      string         `json:"backend_mode"`
	EmbeddingVersion string         `json:"embedding_version,omitempty"`
	RetrievalMode    string         `json:"retrieval_mode,omitempty"`
	CacheHit         bool           `json:"cache_hit,omitempty"`
	FacetsLatencyMs  float64        `json:"facets_latency_ms,omitempty"`
	Candidates       int            `json:"candidates,omitempty"` // size of the facet candidate set (top 100)
	TenantID         string         `json:"tenant_id,omitempty"`  // effective tenant (ADR-002)
	Locale           string         `json:"locale,omitempty"`     // locale filter applied ("" = all)
}

type FacetBucket struct {
	Value string `json:"value"`
	Count int64  `json:"count"`
}

type Facets struct {
	Categories   []FacetBucket `json:"categories"`
	Brands       []FacetBucket `json:"brands"`
	PriceRanges  []FacetBucket `json:"price_ranges"`
	Availability []FacetBucket `json:"availability"`
}

type SearchResult struct {
	ID                string             `json:"id"`
	Name              string             `json:"name"`
	Title             string             `json:"title"`
	Description       string             `json:"description,omitempty"`
	Brands            []string           `json:"brands"`
	Categories        []string           `json:"categories"`
	PriceInfo         PriceInfo          `json:"priceInfo"`
	ColorInfo         *ColorInfo         `json:"colorInfo,omitempty"`
	Availability      string             `json:"availability"`
	AvailableQuantity *int               `json:"availableQuantity,omitempty"`
	AvailableTime     *string            `json:"availableTime,omitempty"`
	Channels          []string           `json:"channels,omitempty"`
	Locale            string             `json:"locale,omitempty"`
	TenantID          string             `json:"tenant_id,omitempty"`
	Images            []Image            `json:"images"`
	Sizes             []string           `json:"sizes"`
	RetrievableFields string             `json:"retrievableFields"`
	Attributes        []Attribute        `json:"attributes"`
	URI               string             `json:"uri"`
	Score             map[string]float64 `json:"score"`
	DeliveryPromise   *DeliveryPromise   `json:"deliveryPromise,omitempty"`
}

type DeliveryPromise struct {
	CEP          string  `json:"cep"`
	RegionCode   string  `json:"region_code"`
	CDID         string  `json:"cd_id"`
	SellerID     string  `json:"seller_id"`
	SLAHours     int     `json:"sla_hours"`
	FreeShipping bool    `json:"free_shipping"`
	ShippingCost float64 `json:"shipping_cost"`
}

type Image struct {
	Height string `json:"height"`
	Width  string `json:"width"`
	URI    string `json:"uri"`
}

type PriceInfo struct {
	Cost               string  `json:"cost"`
	CurrencyCode       string  `json:"currencyCode"`
	OriginalPrice      string  `json:"originalPrice"`
	Price              string  `json:"price"`
	NumericPrice       float64 `json:"numericPrice,omitempty"`
	PriceEffectiveTime string  `json:"priceEffectiveTime"`
	PriceExpireTime    string  `json:"priceExpireTime"`
}

type ColorInfo struct {
	ColorFamilies []string `json:"colorFamilies,omitempty"`
	Colors        []string `json:"colors,omitempty"`
}

type AttributeValue struct {
	Indexable  *string   `json:"indexable,omitempty"`
	Searchable *string   `json:"searchable,omitempty"`
	Text       []string  `json:"text,omitempty"`
	Numbers    []float64 `json:"numbers,omitempty"`
}

type Attribute struct {
	Key   string         `json:"key"`
	Value AttributeValue `json:"value"`
}

type HealthResponse struct {
	Status string `json:"status"`
}

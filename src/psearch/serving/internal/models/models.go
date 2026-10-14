package models

type SearchRequest struct {
	Query             string   `json:"query" form:"q"`
	Limit             *int     `json:"limit,omitempty" form:"limit"`
	MinScore          *float64 `json:"min_score,omitempty" form:"min_score"`
	Alpha             *float64 `json:"alpha,omitempty" form:"alpha"`
	Category          string   `json:"category,omitempty" form:"category"`
	Brand             string   `json:"brand,omitempty" form:"brand"`
	OnlyInStock       bool     `json:"only_in_stock,omitempty" form:"only_in_stock"`
	Channel           string   `json:"channel,omitempty" form:"channel"`
	TenantID          string   `json:"tenant_id,omitempty" form:"tenant_id"`
	Locale            string   `json:"locale,omitempty" form:"locale"`
	CEP               string   `json:"cep,omitempty" form:"cep"`
	LTR               bool     `json:"ltr,omitempty" form:"ltr"`
	MaxSLAHours       int64    `json:"max_sla_hours,omitempty" form:"max_sla_hours"`
	MinPrice          float64  `json:"min_price,omitempty" form:"min_price"`
	MaxPrice          float64  `json:"max_price,omitempty" form:"max_price"`
	ParseIntent       *bool    `json:"parse_intent,omitempty" form:"parse_intent"`
	PriceTable        string   `json:"price_table_id,omitempty" form:"price_table_id"`
	Color             string   `json:"color,omitempty" form:"color"`
	Size              string   `json:"size,omitempty" form:"size"`
	SortBy            string   `json:"sort_by,omitempty" form:"sort_by"`
	SessionID         string   `json:"session_id,omitempty" form:"session_id"`
	SessionProductIDs []string `json:"session_product_ids,omitempty" form:"session_product_ids"` // default true; false disables
}

// SearchFilters are the catalog filters applied server-side.
type SearchFilters struct {
	OnlyInStock bool
	Channel     string
	Category    string  // exact (case-insensitive) category match; empty = all (facet drill-down)
	Brand       string  // exact (case-insensitive) brand match; empty = all
	TenantID    string  // empty = "default"
	Locale      string  // (pt-BR, en-US, es-MX); empty = all locales
	Zip5        string  // 5-digit CEP prefix; empty = no delivery filter
	MaxSLAHours int64   // maximum delivery SLA in hours; 0 = any
	RelaxText   bool    // run the text leg with OR between terms
	PriceTable  string  // contract price table; "" = list price
	Color       string  // in-stock child SKU color; "" = any
	Size        string  // in-stock child SKU size; "" = any
	SortBy      string  // relevance | price_asc | price_desc
	MinPrice    float64 // minimum price (0 = none)
	MaxPrice    float64 // maximum price (0 = none)
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
	Candidates       int            `json:"candidates,omitempty"`     // size of the facet candidate set (top 100)
	TenantID         string         `json:"tenant_id,omitempty"`      // effective tenant
	Locale           string         `json:"locale,omitempty"`         // locale filter applied ("" = all)
	ExpandedTerms    []string       `json:"expanded_terms,omitempty"` // synonyms added at reduced weight
	AppliedRules     []string       `json:"applied_rules,omitempty"`  // merchandising rule_ids applied
	LTR              bool           `json:"ltr,omitempty"`            // stage-2 re-ranking applied
	LTRLatencyUS     int64          `json:"ltr_latency_us,omitempty"` // stage-2 in-memory latency (microseconds)
	LTRModel         string         `json:"ltr_model,omitempty"`
	ParsedQuery      *ParsedQuery   `json:"parsed_query,omitempty"`
	DidYouMean       string         `json:"did_you_mean,omitempty"`
	AutoCorrected    bool           `json:"auto_corrected,omitempty"`
	QueryRelaxed     bool           `json:"query_relaxed,omitempty"`
	PriceTableID     string         `json:"price_table_id,omitempty"`
	SortBy           string         `json:"sort_by,omitempty"`
	SessionID        string         `json:"session_id,omitempty"`
	Personalized     bool           `json:"personalized,omitempty"`
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
	Colors       []FacetBucket `json:"colors,omitempty"` // in-stock variant colors
	Sizes        []FacetBucket `json:"sizes,omitempty"`  // in-stock variant sizes
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
	Signals           *BoostSignals      `json:"boost_signals,omitempty"`
	Locale            string             `json:"locale,omitempty"`
	TenantID          string             `json:"tenant_id,omitempty"`
	Images            []Image            `json:"images"`
	Sizes             []string           `json:"sizes"`
	RetrievableFields string             `json:"retrievableFields"`
	Attributes        []Attribute        `json:"attributes"`
	URI               string             `json:"uri"`
	Score             map[string]float64 `json:"score"`
	DeliveryPromise   *DeliveryPromise   `json:"deliveryPromise,omitempty"`
	Variants          []Variant          `json:"variants,omitempty"` // child SKUs
	VariantCount      int                `json:"variant_count,omitempty"`
	AvailableColors   []string           `json:"available_colors,omitempty"`
	AvailableSizes    []string           `json:"available_sizes,omitempty"`
}

type DeliveryPromise struct {
	CEP          string  `json:"cep"`
	RegionCode   string  `json:"region_code"`
	CDID         string  `json:"cd_id"`
	SellerID     string  `json:"seller_id"`
	SLAHours     int64   `json:"sla_hours"`
	FreeShipping bool    `json:"free_shipping"`
	ShippingCost float64 `json:"shipping_cost"`
	Label        string  `json:"label"` // e.g. "Chega amanhã - CD São Paulo"
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
	BasePrice          float64 `json:"basePrice,omitempty"`    // list price before contract pricing
	PriceTableID       string  `json:"priceTableId,omitempty"` // contract table applied
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

// BoostSignals are the 8 business signals used for ranking boosts.
type BoostSignals struct {
	Clicks          int64   `json:"clicks"`
	Orders          int64   `json:"orders"`
	DiscountPct     float64 `json:"discount_pct"`
	PopularityScore float64 `json:"popularity_score"`
	MarginPct       float64 `json:"margin_pct"`
	CTR             float64 `json:"ctr"`
	ConversionRate  float64 `json:"conversion_rate"`
	RecencyScore    float64 `json:"recency_score"`
}

// ParsedQuery reports how a natural-language query was decomposed.
type ParsedQuery struct {
	OriginalQuery    string                 `json:"original_query"`
	CleanQuery       string                 `json:"clean_query"`
	ExtractedFilters map[string]interface{} `json:"extracted_filters"`
	IntentSource     string                 `json:"intent_source"` // parser | cache | gemini | none
}

// Variant is one child SKU of a parent product.
type Variant struct {
	SKUID     string `json:"sku_id"`
	Color     string `json:"color,omitempty"`
	Size      string `json:"size,omitempty"`
	Stock     int64  `json:"stock"`
	Available bool   `json:"available"`
}

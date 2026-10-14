package services

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"psearch/serving-go/internal/models"
)

// ---------------------------------------------------------------------------
// 8 boost signals (/ )
// ---------------------------------------------------------------------------

// DefaultBoostWeight scales the normalized [0,1] boost-signal score so it can
// reorder neighbouring results (adjacent RRF ranks differ by ~0.0003) without
// overwhelming relevance (max RRF ~0.033).
const DefaultBoostWeight = 0.002

// boostSignalSQL mirrors BoostSignalScore over products alias p; weights sum to 1.
const boostSignalSQL = `(
				0.15 * IFNULL(p.clicks, 0) / (IFNULL(p.clicks, 0) + 100)
				+ 0.20 * IFNULL(p.orders, 0) / (IFNULL(p.orders, 0) + 20)
				+ 0.10 * LEAST(GREATEST(IFNULL(p.discount_pct, 0), 0), 100) / 100
				+ 0.15 * LEAST(GREATEST(IFNULL(p.popularity_score, 0), 0), 1)
				+ 0.10 * LEAST(GREATEST(IFNULL(p.margin_pct, 0), 0), 100) / 100
				+ 0.10 * LEAST(GREATEST(IFNULL(p.ctr, 0), 0), 1)
				+ 0.10 * LEAST(GREATEST(IFNULL(p.conversion_rate, 0), 0), 1)
				+ 0.10 * LEAST(GREATEST(IFNULL(p.recency_score, 0), 0), 1))`

func clamp(v, lo, hi float64) float64 { return math.Max(lo, math.Min(hi, v)) }

// BoostSignalScore returns the normalized [0,1] business-signal score (same formula as boostSignalSQL).
func BoostSignalScore(s models.BoostSignals) float64 {
	c, o := float64(s.Clicks), float64(s.Orders)
	return 0.15*c/(c+100) +
		0.20*o/(o+20) +
		0.10*clamp(s.DiscountPct, 0, 100)/100 +
		0.15*clamp(s.PopularityScore, 0, 1) +
		0.10*clamp(s.MarginPct, 0, 100)/100 +
		0.10*clamp(s.CTR, 0, 1) +
		0.10*clamp(s.ConversionRate, 0, 1) +
		0.10*clamp(s.RecencyScore, 0, 1)
}

// ---------------------------------------------------------------------------
// Merchandising rules
// ---------------------------------------------------------------------------

// Rule is one row of merchandising_rules (migration 016).
type Rule struct {
	TenantID        string   `json:"tenant_id"`
	RuleID          string   `json:"rule_id"`
	Name            string   `json:"name"`
	QueryPattern    string   `json:"query_pattern"` // "" or "*" = every query; otherwise folded substring of the query
	TargetBrand     string   `json:"target_brand"`
	TargetCategory  string   `json:"target_category"`
	BoostMultiplier float64  `json:"boost_multiplier"` // >1 boosts, (0,1) demotes, 1 = neutral
	PinProductIDs   []string `json:"pin_product_ids"`
	BuryProductIDs  []string `json:"bury_product_ids"`
	Active          bool     `json:"active"`
}

// RuleStore persists rules.
type RuleStore interface {
	List(ctx context.Context, tenantID string) ([]Rule, error)
	Upsert(ctx context.Context, r Rule) error
	Delete(ctx context.Context, tenantID, ruleID string) error
}

// ruleBoost is passed to Spanner as ARRAY<STRUCT<brand, category, multiplier>>.
type ruleBoost struct {
	Brand      string  `spanner:"brand"`
	Category   string  `spanner:"category"`
	Multiplier float64 `spanner:"multiplier"`
}

// RuleSet is the merged effect of the active rules matching one query.
type RuleSet struct {
	Boosts  []ruleBoost
	Pins    []string
	Buries  []string
	Applied []string // rule_ids
}

// RuleService validates rules and matches them to queries (TTL-cached per tenant).
type RuleService struct {
	store RuleStore
	ttl   time.Duration
	mu    sync.Mutex
	cache map[string]ruleCacheEntry
}

type ruleCacheEntry struct {
	rules   []Rule
	expires time.Time
}

func NewRuleService(store RuleStore, ttl time.Duration) *RuleService {
	return &RuleService{store: store, ttl: ttl, cache: map[string]ruleCacheEntry{}}
}

func (s *RuleService) invalidate() {
	s.mu.Lock()
	s.cache = map[string]ruleCacheEntry{}
	s.mu.Unlock()
}

// List returns all rules for a tenant (cached).
func (s *RuleService) List(ctx context.Context, tenantID string) ([]Rule, error) {
	s.mu.Lock()
	if e, ok := s.cache[tenantID]; ok && time.Now().Before(e.expires) {
		s.mu.Unlock()
		return e.rules, nil
	}
	s.mu.Unlock()
	rules, err := s.store.List(ctx, tenantID)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.cache[tenantID] = ruleCacheEntry{rules: rules, expires: time.Now().Add(s.ttl)}
	s.mu.Unlock()
	return rules, nil
}

func newRuleID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "rule-" + hex.EncodeToString(b)
}

func cleanIDs(ids []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, id := range ids {
		if id = strings.TrimSpace(id); id != "" && !seen[id] && len(id) <= 1024 {
			seen[id] = true
			out = append(out, id)
		}
	}
	return out
}

// Save validates and upserts a rule (creates an ID when empty).
func (s *RuleService) Save(ctx context.Context, r Rule) (Rule, error) {
	r.TenantID = strings.TrimSpace(r.TenantID)
	if r.TenantID == "" {
		r.TenantID = "default"
	}
	if r.RuleID == "" {
		r.RuleID = newRuleID()
	}
	r.Name = strings.TrimSpace(r.Name)
	if r.Name == "" || len(r.Name) > 256 {
		return r, fmt.Errorf("name is required (<=256 chars)")
	}
	r.QueryPattern = normalizeTerm(r.QueryPattern)
	if r.QueryPattern == "*" {
		r.QueryPattern = ""
	}
	r.TargetBrand = strings.ToLower(strings.TrimSpace(r.TargetBrand))
	r.TargetCategory = strings.ToLower(strings.TrimSpace(r.TargetCategory))
	if r.BoostMultiplier == 0 {
		r.BoostMultiplier = 1
	}
	if r.BoostMultiplier < 0.1 || r.BoostMultiplier > 10 {
		return r, fmt.Errorf("boost_multiplier must be in [0.1, 10]")
	}
	r.PinProductIDs, r.BuryProductIDs = cleanIDs(r.PinProductIDs), cleanIDs(r.BuryProductIDs)
	if len(r.PinProductIDs) > 20 || len(r.BuryProductIDs) > 100 {
		return r, fmt.Errorf("at most 20 pins and 100 buries per rule")
	}
	if r.BoostMultiplier == 1 && len(r.PinProductIDs) == 0 && len(r.BuryProductIDs) == 0 {
		return r, fmt.Errorf("rule has no effect: set boost_multiplier != 1, pins or buries")
	}
	if r.BoostMultiplier != 1 && r.TargetBrand == "" && r.TargetCategory == "" {
		return r, fmt.Errorf("boost_multiplier requires target_brand or target_category")
	}
	if err := s.store.Upsert(ctx, r); err != nil {
		return r, err
	}
	s.invalidate()
	return r, nil
}

// Delete removes a rule.
func (s *RuleService) Delete(ctx context.Context, tenantID, ruleID string) error {
	if err := s.store.Delete(ctx, tenantID, ruleID); err != nil {
		return err
	}
	s.invalidate()
	return nil
}

// Match merges the active rules whose query_pattern matches the query.
// Errors degrade to "no rules" (search must not fail because of merchandising).
func (s *RuleService) Match(ctx context.Context, tenantID, query string) RuleSet {
	var rs RuleSet
	if s == nil {
		return rs
	}
	rules, err := s.List(ctx, tenantID)
	if err != nil {
		return rs
	}
	q := normalizeTerm(query)
	pinned, buried := map[string]bool{}, map[string]bool{}
	for _, r := range rules {
		if !r.Active || (r.QueryPattern != "" && !strings.Contains(q, r.QueryPattern)) {
			continue
		}
		rs.Applied = append(rs.Applied, r.RuleID)
		// Clamp defensively: rows written directly to Spanner bypass Save() validation,
		// and a multiplier <= 0 would make LN() fail the whole query.
		if m := clamp(r.BoostMultiplier, 0.1, 10); m != 1 && (r.TargetBrand != "" || r.TargetCategory != "") {
			rs.Boosts = append(rs.Boosts, ruleBoost{Brand: r.TargetBrand, Category: r.TargetCategory, Multiplier: m})
		}
		for _, id := range r.PinProductIDs {
			if !pinned[id] {
				pinned[id] = true
				rs.Pins = append(rs.Pins, id)
			}
		}
		for _, id := range r.BuryProductIDs {
			if !buried[id] && !pinned[id] {
				buried[id] = true
				rs.Buries = append(rs.Buries, id)
			}
		}
	}
	return rs
}

// Multiplier returns the product of the multipliers of boosts matching brand/category.
func (rs RuleSet) Multiplier(brand, category string) float64 {
	m := 1.0
	b, c := strings.ToLower(brand), strings.ToLower(category)
	for _, x := range rs.Boosts {
		if (x.Brand == "" || x.Brand == b) && (x.Category == "" || x.Category == c) {
			m *= x.Multiplier
		}
	}
	return m
}

// MemoryRuleStore is an in-process rule store (tests / memory mode).
type MemoryRuleStore struct {
	mu    sync.Mutex
	rules map[string]Rule
}

func NewMemoryRuleStore() *MemoryRuleStore { return &MemoryRuleStore{rules: map[string]Rule{}} }

func (m *MemoryRuleStore) List(_ context.Context, tenantID string) ([]Rule, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []Rule{}
	for _, r := range m.rules {
		if r.TenantID == tenantID {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RuleID < out[j].RuleID })
	return out, nil
}

func (m *MemoryRuleStore) Upsert(_ context.Context, r Rule) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rules[r.TenantID+"|"+r.RuleID] = r
	return nil
}

func (m *MemoryRuleStore) Delete(_ context.Context, tenantID, ruleID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.rules, tenantID+"|"+ruleID)
	return nil
}

// SpannerRuleStore persists rules in merchandising_rules (migration 016).
type SpannerRuleStore struct{ Client *spanner.Client }

var ruleColumns = []string{"tenant_id", "rule_id", "name", "query_pattern", "target_brand", "target_category",
	"boost_multiplier", "pin_product_ids", "bury_product_ids", "active", "updated_at"}

func (s SpannerRuleStore) List(ctx context.Context, tenantID string) ([]Rule, error) {
	iter := s.Client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT tenant_id, rule_id, IFNULL(name, ''), IFNULL(query_pattern, ''), IFNULL(target_brand, ''),
				IFNULL(target_category, ''), IFNULL(boost_multiplier, 1.0), IFNULL(pin_product_ids, []),
				IFNULL(bury_product_ids, []), IFNULL(active, FALSE)
			FROM merchandising_rules WHERE tenant_id = @tenant ORDER BY rule_id`,
		Params: map[string]interface{}{"tenant": tenantID},
	})
	defer iter.Stop()
	out := []Rule{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		var r Rule
		if err := row.Columns(&r.TenantID, &r.RuleID, &r.Name, &r.QueryPattern, &r.TargetBrand, &r.TargetCategory,
			&r.BoostMultiplier, &r.PinProductIDs, &r.BuryProductIDs, &r.Active); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
}

func (s SpannerRuleStore) Upsert(ctx context.Context, r Rule) error {
	_, err := s.Client.Apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdate("merchandising_rules", ruleColumns,
		[]interface{}{r.TenantID, r.RuleID, r.Name, r.QueryPattern, r.TargetBrand, r.TargetCategory,
			r.BoostMultiplier, r.PinProductIDs, r.BuryProductIDs, r.Active, spanner.CommitTimestamp})})
	return err
}

func (s SpannerRuleStore) Delete(ctx context.Context, tenantID, ruleID string) error {
	_, err := s.Client.Apply(ctx, []*spanner.Mutation{spanner.Delete("merchandising_rules", spanner.Key{tenantID, ruleID})})
	return err
}

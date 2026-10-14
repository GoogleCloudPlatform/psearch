package services

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	"psearch/serving-go/internal/config"
	"psearch/serving-go/internal/models"
)

// Session-aware personalization (ADR-007, Issue #20).

// SessionEvent is one row of user_session_events (migration 021).
type SessionEvent struct {
	TenantID  string    `json:"tenant_id"`
	SessionID string    `json:"session_id"`
	EventID   string    `json:"event_id"`
	ProductID string    `json:"product_id"`
	EventType string    `json:"event_type"` // click | purchase
	CreatedAt time.Time `json:"created_at"`
}

// SessionStore persists events.
type SessionStore interface {
	Add(ctx context.Context, e SessionEvent) error
	Recent(ctx context.Context, tenant, session string, n int) ([]SessionEvent, error) // newest first
}

const (
	sessionWindow        = 20
	DefaultSessionBoostW = 0.003
	purchaseWeight       = 3.0
	clickWeight          = 1.0
	recencyDecay         = 0.85
	maxEventsPerSession  = 500
)

var sessionIDRe = regexp.MustCompile(`^[A-Za-z0-9_-]{8,128}$`)

// ValidSessionID reports whether a client session token is acceptable (opaque, no PII).
func ValidSessionID(id string) bool { return sessionIDRe.MatchString(id) }

// SessionService records events and personalizes rankings.
type SessionService struct {
	store  SessionStore
	spanSv *SpannerService
	weight float64
}

func NewSessionService(store SessionStore, svc *SpannerService, weight float64) *SessionService {
	if weight <= 0 {
		weight = DefaultSessionBoostW
	}
	return &SessionService{store: store, spanSv: svc, weight: weight}
}

func newEventID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), hex.EncodeToString(b))
}

// Record validates and stores a click/purchase event.
func (s *SessionService) Record(ctx context.Context, e SessionEvent) (SessionEvent, error) {
	if e.TenantID == "" {
		e.TenantID = "default"
	}
	e.EventType = strings.ToLower(strings.TrimSpace(e.EventType))
	if !ValidSessionID(e.SessionID) {
		return e, fmt.Errorf("session_id must be 8-128 chars [A-Za-z0-9_-]")
	}
	if e.EventType != "click" && e.EventType != "purchase" {
		return e, fmt.Errorf("event_type must be click or purchase")
	}
	if e.ProductID == "" || len(e.ProductID) > 1024 {
		return e, fmt.Errorf("product_id is required")
	}
	e.EventID, e.CreatedAt = newEventID(), time.Now().UTC()
	return e, s.store.Add(ctx, e)
}

// Recent returns the newest events of a session.
func (s *SessionService) Recent(ctx context.Context, tenant, session string) ([]SessionEvent, error) {
	if tenant == "" {
		tenant = "default"
	}
	return s.store.Recent(ctx, tenant, session, sessionWindow)
}

// Centroid builds the normalized weighted mean of the session products' vectors
// (purchase x3, click x1, recency decay 0.85^i from newest).
func Centroid(events []SessionEvent, vectors map[string][]float64) []float64 {
	var c []float64
	for i, e := range events {
		v, ok := vectors[e.ProductID]
		if !ok || len(v) == 0 {
			continue
		}
		w := clickWeight
		if e.EventType == "purchase" {
			w = purchaseWeight
		}
		w *= math.Pow(recencyDecay, float64(i))
		if c == nil {
			c = make([]float64, len(v))
		}
		if len(v) != len(c) {
			continue
		}
		for j := range v {
			c[j] += w * v[j]
		}
	}
	return normalize(c)
}

func normalize(v []float64) []float64 {
	var n float64
	for _, x := range v {
		n += x * x
	}
	if n == 0 {
		return nil
	}
	n = math.Sqrt(n)
	out := make([]float64, len(v))
	for i, x := range v {
		out[i] = x / n
	}
	return out
}

// Personalize adds score.session_boost = w * max(0, cos(centroid, product)) to each
// result and re-sorts (availability, pins and buries keep precedence; an explicit
// price sort stays primary). Returns false when the session has no usable history.
func (s *SessionService) Personalize(ctx context.Context, tenant string, events []SessionEvent, results []models.SearchResult,
	pins, buries []string, sortBy string, useLTR bool) bool {
	if len(events) == 0 || len(results) == 0 {
		return false
	}
	ids := map[string]bool{}
	for _, e := range events {
		ids[e.ProductID] = true
	}
	for _, r := range results {
		ids[r.ID] = true
	}
	list := make([]string, 0, len(ids))
	for id := range ids {
		list = append(list, id)
	}
	vectors, err := s.spanSv.ProductVectors(ctx, list)
	if err != nil {
		return false
	}
	centroid := Centroid(events, vectors)
	if centroid == nil {
		return false
	}
	base := func(r models.SearchResult) float64 {
		if useLTR {
			return r.Score["ltr_score"]
		}
		return r.Score["final_score"]
	}
	for i := range results {
		b := 0.0
		if v, ok := vectors[results[i].ID]; ok && len(v) == len(centroid) {
			b = s.weight * math.Max(0, CosineSimilarity(centroid, v))
		}
		if results[i].Score == nil {
			results[i].Score = map[string]float64{}
		}
		results[i].Score["session_boost"] = b
		results[i].Score["personalized_score"] = base(results[i]) + b
	}
	priceSort := sortBy == "price_asc" || sortBy == "price_desc"
	sort.SliceStable(results, func(i, j int) bool {
		a, b := results[i], results[j]
		ua, ub := isUnavailable(a.Availability, a.AvailableQuantity), isUnavailable(b.Availability, b.AvailableQuantity)
		if ua != ub {
			return !ua
		}
		if pa, pb := pinRank(pins, a.ID), pinRank(pins, b.ID); pa != pb {
			return pa < pb
		}
		if ba, bb := containsID(buries, a.ID), containsID(buries, b.ID); ba != bb {
			return !ba
		}
		if priceSort && a.PriceInfo.NumericPrice != b.PriceInfo.NumericPrice {
			return (a.PriceInfo.NumericPrice < b.PriceInfo.NumericPrice) == (sortBy == "price_asc")
		}
		return a.Score["personalized_score"] > b.Score["personalized_score"]
	})
	return true
}

// ProductVectors returns the active embedding of each product: Spanner column
// (embedding_v2 for v2, embedding for v1) or the memory-mode deterministic vector.
func (s *SpannerService) ProductVectors(ctx context.Context, ids []string) (map[string][]float64, error) {
	out := map[string][]float64{}
	if s.client == nil {
		want := map[string]bool{}
		for _, id := range ids {
			want[id] = true
		}
		for _, p := range s.seedProducts {
			if want[p.ID] {
				docText := FoldAccents(strings.ToLower(p.Title + " " + p.Description + " " + strings.Join(p.Brands, " ") + " " + strings.Join(p.Categories, " ")))
				out[p.ID] = DeterministicEmbedding(docText, config.LegacyEmbeddingDimension)
			}
		}
		return out, nil
	}
	col := vectorColumnV1
	if s.config.UseGeminiEmbeddings() {
		col = vectorColumnV2
	}
	iter := s.client.Single().Query(ctx, spanner.Statement{
		SQL:    `SELECT product_id, ` + col + ` FROM products WHERE product_id IN UNNEST(@ids) AND ` + col + ` IS NOT NULL`,
		Params: map[string]interface{}{"ids": ids},
	})
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		var id string
		if col == vectorColumnV2 {
			var v []float32
			if err := row.Columns(&id, &v); err != nil {
				return nil, err
			}
			f := make([]float64, len(v))
			for i, x := range v {
				f[i] = float64(x)
			}
			out[id] = f
		} else {
			var v []float64
			if err := row.Columns(&id, &v); err != nil {
				return nil, err
			}
			out[id] = v
		}
	}
}

// MemorySessionStore keeps events in process (tests / memory mode).
type MemorySessionStore struct {
	mu     sync.Mutex
	events map[string][]SessionEvent // tenant|session -> newest last
}

func NewMemorySessionStore() *MemorySessionStore {
	return &MemorySessionStore{events: map[string][]SessionEvent{}}
}

func (m *MemorySessionStore) Add(_ context.Context, e SessionEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := e.TenantID + "|" + e.SessionID
	m.events[k] = append(m.events[k], e)
	if len(m.events[k]) > maxEventsPerSession {
		m.events[k] = m.events[k][len(m.events[k])-maxEventsPerSession:]
	}
	return nil
}

func (m *MemorySessionStore) Recent(_ context.Context, tenant, session string, n int) ([]SessionEvent, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	all := m.events[tenant+"|"+session]
	out := []SessionEvent{}
	for i := len(all) - 1; i >= 0 && len(out) < n; i-- {
		out = append(out, all[i])
	}
	return out, nil
}

// SpannerSessionStore uses user_session_events (migration 021).
type SpannerSessionStore struct{ Client *spanner.Client }

func (s SpannerSessionStore) Add(ctx context.Context, e SessionEvent) error {
	_, err := s.Client.Apply(ctx, []*spanner.Mutation{spanner.Insert("user_session_events",
		[]string{"tenant_id", "session_id", "event_id", "product_id", "event_type", "created_at"},
		[]interface{}{e.TenantID, e.SessionID, e.EventID, e.ProductID, e.EventType, spanner.CommitTimestamp})})
	return err
}

func (s SpannerSessionStore) Recent(ctx context.Context, tenant, session string, n int) ([]SessionEvent, error) {
	iter := s.Client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT tenant_id, session_id, event_id, product_id, event_type, created_at FROM user_session_events
			WHERE tenant_id = @tenant AND session_id = @session ORDER BY created_at DESC LIMIT @n`,
		Params: map[string]interface{}{"tenant": tenant, "session": session, "n": int64(n)},
	})
	defer iter.Stop()
	out := []SessionEvent{}
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			return out, nil
		}
		if err != nil {
			return nil, err
		}
		var e SessionEvent
		if err := row.Columns(&e.TenantID, &e.SessionID, &e.EventID, &e.ProductID, &e.EventType, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
}

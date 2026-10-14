package services

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/grpc/codes"
)

// CacheStore is the persistent (tier-2) embedding cache. Implemented by the
// Spanner embedding_cache table (migration 013); an interface for tests.
type CacheStore interface {
	Get(ctx context.Context, key, model string) ([]float32, bool, error)
	Put(ctx context.Context, key, model string, dims int, vec []float32) error
}

// CacheStats is reported in /api/info.
type CacheStats struct {
	MemoryHits  int64 `json:"memory_hits"`
	SpannerHits int64 `json:"spanner_hits"`
	Misses      int64 `json:"misses"`
	Entries     int   `json:"entries"`
	MaxEntries  int   `json:"max_entries"`
}

// EmbeddingCache is a two-tier query-embedding cache:
// tier 1 is an in-process LRU (sub-millisecond hits), tier 2 is the Spanner
// embedding_cache table shared by all instances. Store failures never fail a
// search: they are logged and treated as misses.
type EmbeddingCache struct {
	inner      QueryEmbedder
	store      CacheStore // may be nil
	model      string
	dims       int
	maxEntries int

	mu    sync.Mutex
	lru   *list.List
	index map[string]*list.Element

	memoryHits, spannerHits, misses atomic.Int64
	pendingWrites                   sync.WaitGroup
}

type cacheEntry struct {
	key string
	vec []float32
}

// NewEmbeddingCache wraps inner (the Gen AI embedding service).
func NewEmbeddingCache(inner QueryEmbedder, store CacheStore, model string, dims, maxEntries int) *EmbeddingCache {
	if maxEntries <= 0 {
		maxEntries = 1000
	}
	return &EmbeddingCache{inner: inner, store: store, model: model, dims: dims, maxEntries: maxEntries,
		lru: list.New(), index: make(map[string]*list.Element)}
}

// NormalizeQuery lowercases, trims and collapses whitespace.
func NormalizeQuery(text string) string {
	return strings.Join(strings.Fields(strings.ToLower(text)), " ")
}

// CacheKey is SHA-256 over normalized text, model and dimensions.
func CacheKey(text, model string, dims int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d", NormalizeQuery(text), model, dims)))
	return hex.EncodeToString(sum[:])
}

// EmbedQuery implements QueryEmbedder.
func (c *EmbeddingCache) EmbedQuery(ctx context.Context, text string) ([]float32, error) {
	vec, _, err := c.EmbedQueryWithMeta(ctx, text)
	return vec, err
}

// EmbedQueryWithMeta returns the embedding and whether it was served from cache.
func (c *EmbeddingCache) EmbedQueryWithMeta(ctx context.Context, text string) ([]float32, bool, error) {
	key := CacheKey(text, c.model, c.dims)
	if vec, ok := c.getMemory(key); ok {
		c.memoryHits.Add(1)
		return vec, true, nil
	}
	if c.store != nil {
		sctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		vec, ok, err := c.store.Get(sctx, key, c.model)
		cancel()
		if err != nil {
			log.Printf("WARN: embedding cache store read failed (treated as miss): %v", err)
		} else if ok && len(vec) == c.dims {
			c.spannerHits.Add(1)
			c.putMemory(key, vec)
			return vec, true, nil
		}
	}
	c.misses.Add(1)
	vec, err := c.inner.EmbedQuery(ctx, text)
	if err != nil {
		return nil, false, err
	}
	c.putMemory(key, vec)
	if c.store != nil {
		c.pendingWrites.Add(1)
		go func() {
			defer c.pendingWrites.Done()
			wctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := c.store.Put(wctx, key, c.model, c.dims, vec); err != nil {
				log.Printf("WARN: embedding cache store write failed: %v", err)
			}
		}()
	}
	return vec, false, nil
}

// Stats returns cache counters.
func (c *EmbeddingCache) Stats() CacheStats {
	c.mu.Lock()
	n := c.lru.Len()
	c.mu.Unlock()
	return CacheStats{MemoryHits: c.memoryHits.Load(), SpannerHits: c.spannerHits.Load(), Misses: c.misses.Load(), Entries: n, MaxEntries: c.maxEntries}
}

func (c *EmbeddingCache) getMemory(key string) ([]float32, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.index[key]; ok {
		c.lru.MoveToFront(el)
		return el.Value.(*cacheEntry).vec, true
	}
	return nil, false
}

func (c *EmbeddingCache) putMemory(key string, vec []float32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.index[key]; ok {
		el.Value.(*cacheEntry).vec = vec
		c.lru.MoveToFront(el)
		return
	}
	c.index[key] = c.lru.PushFront(&cacheEntry{key: key, vec: vec})
	for c.lru.Len() > c.maxEntries {
		oldest := c.lru.Back()
		c.lru.Remove(oldest)
		delete(c.index, oldest.Value.(*cacheEntry).key)
	}
}

// SpannerCacheStore persists embeddings in the embedding_cache table (migration 013).
type SpannerCacheStore struct{ Client *spanner.Client }

func (s SpannerCacheStore) Get(ctx context.Context, key, model string) ([]float32, bool, error) {
	row, err := s.Client.Single().ReadRow(ctx, "embedding_cache", spanner.Key{key, model}, []string{"embedding"})
	if spanner.ErrCode(err) == codes.NotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var vec []float32
	if err := row.Columns(&vec); err != nil {
		return nil, false, err
	}
	return vec, true, nil
}

func (s SpannerCacheStore) Put(ctx context.Context, key, model string, dims int, vec []float32) error {
	_, err := s.Client.Apply(ctx, []*spanner.Mutation{spanner.InsertOrUpdate("embedding_cache",
		[]string{"content_sha256", "model_id", "dimensions", "embedding", "created_at"},
		[]interface{}{key, model, int64(dims), vec, spanner.CommitTimestamp})})
	return err
}

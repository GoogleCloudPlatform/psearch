package services

import (
	"context"
	"errors"
	"sync"
	"testing"
)

type countingEmbedder struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (c *countingEmbedder) EmbedQuery(context.Context, string) ([]float32, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	if c.err != nil {
		return nil, c.err
	}
	return make([]float32, 4), nil
}

type memStore struct {
	mu     sync.Mutex
	data   map[string][]float32
	getErr error
	putErr error
	puts   int
}

func (m *memStore) Get(_ context.Context, key, model string) ([]float32, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.getErr != nil {
		return nil, false, m.getErr
	}
	v, ok := m.data[key+"|"+model]
	return v, ok, nil
}

func (m *memStore) Put(_ context.Context, key, model string, _ int, vec []float32) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.puts++
	if m.putErr != nil {
		return m.putErr
	}
	m.data[key+"|"+model] = vec
	return nil
}

func TestEmbeddingCache_MemoryHitAndNormalization(t *testing.T) {
	inner := &countingEmbedder{}
	c := NewEmbeddingCache(inner, nil, "m", 4, 10)
	if _, hit, err := c.EmbedQueryWithMeta(context.Background(), "Running  Shoes "); err != nil || hit {
		t.Fatalf("first call: hit=%v err=%v, want miss", hit, err)
	}
	if _, hit, _ := c.EmbedQueryWithMeta(context.Background(), "running shoes"); !hit {
		t.Fatal("normalized repeat should be a memory hit")
	}
	if inner.calls != 1 {
		t.Fatalf("inner embedder called %d times, want 1", inner.calls)
	}
	st := c.Stats()
	if st.MemoryHits != 1 || st.Misses != 1 || st.Entries != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestEmbeddingCache_LRUEviction(t *testing.T) {
	inner := &countingEmbedder{}
	c := NewEmbeddingCache(inner, nil, "m", 4, 2)
	ctx := context.Background()
	c.EmbedQuery(ctx, "a")
	c.EmbedQuery(ctx, "b")
	c.EmbedQuery(ctx, "a") // a most recent
	c.EmbedQuery(ctx, "c") // evicts b
	if c.Stats().Entries != 2 {
		t.Fatalf("entries = %d, want 2", c.Stats().Entries)
	}
	if _, hit, _ := c.EmbedQueryWithMeta(ctx, "a"); !hit {
		t.Fatal("a should still be cached")
	}
	if _, hit, _ := c.EmbedQueryWithMeta(ctx, "b"); hit {
		t.Fatal("b should have been evicted")
	}
}

func TestEmbeddingCache_SpannerTierAndWriteBack(t *testing.T) {
	store := &memStore{data: map[string][]float32{}}
	inner := &countingEmbedder{}
	c1 := NewEmbeddingCache(inner, store, "m", 4, 10)
	c1.EmbedQuery(context.Background(), "tenis")
	c1.pendingWrites.Wait()
	if store.puts != 1 {
		t.Fatalf("store puts = %d, want 1", store.puts)
	}
	// A fresh instance (empty LRU) is served from the persistent tier.
	c2 := NewEmbeddingCache(inner, store, "m", 4, 10)
	if _, hit, _ := c2.EmbedQueryWithMeta(context.Background(), "TENIS"); !hit {
		t.Fatal("second instance should hit the persistent tier")
	}
	if c2.Stats().SpannerHits != 1 || inner.calls != 1 {
		t.Fatalf("spanner_hits=%d inner calls=%d", c2.Stats().SpannerHits, inner.calls)
	}
}

func TestEmbeddingCache_StoreFailuresNeverFailSearch(t *testing.T) {
	store := &memStore{data: map[string][]float32{}, getErr: errors.New("spanner down"), putErr: errors.New("spanner down")}
	c := NewEmbeddingCache(&countingEmbedder{}, store, "m", 4, 10)
	vec, hit, err := c.EmbedQueryWithMeta(context.Background(), "q")
	c.pendingWrites.Wait()
	if err != nil || hit || len(vec) != 4 {
		t.Fatalf("store failure must degrade to a miss: vec=%d hit=%v err=%v", len(vec), hit, err)
	}
}

func TestEmbeddingCache_EmbedderErrorPropagatesAndIsNotCached(t *testing.T) {
	inner := &countingEmbedder{err: errors.New("403")}
	c := NewEmbeddingCache(inner, nil, "m", 4, 10)
	if _, _, err := c.EmbedQueryWithMeta(context.Background(), "q"); err == nil {
		t.Fatal("expected embedder error")
	}
	if c.Stats().Entries != 0 {
		t.Fatal("failed embeddings must not be cached")
	}
}

func TestCacheKeyIncludesModelAndDims(t *testing.T) {
	if CacheKey("x", "m1", 1024) == CacheKey("x", "m2", 1024) || CacheKey("x", "m1", 1024) == CacheKey("x", "m1", 768) {
		t.Fatal("cache key must vary by model and dimensions")
	}
}

func TestPlanVectorQuery_ReportsCacheHit(t *testing.T) {
	cache := NewEmbeddingCache(stubEmbedder{vec: make([]float32, 1024)}, nil, "gemini-embedding-2", 1024, 10)
	svc := &SpannerService{config: testConfig("v2"), embeddings: cache}
	if p := svc.planVectorQuery(context.Background(), "tenis"); p.cacheHit {
		t.Fatal("first query should not be a cache hit")
	}
	if p := svc.planVectorQuery(context.Background(), "tenis"); !p.cacheHit || p.lexicalOnly {
		t.Fatalf("repeat query should be a cache hit: %+v", p)
	}
}

func TestMemorySortIsDeterministic(t *testing.T) {
	svc := &SpannerService{config: testConfig("v1"), seedProducts: DefaultSeedProducts()}
	first := svc.memoryHybridSearch("running shoes", 15, 0, 0, emptyFilters())
	for i := 0; i < 5; i++ {
		again := svc.memoryHybridSearch("running shoes", 15, 0, 0, emptyFilters())
		for j := range first {
			if first[j].ID != again[j].ID {
				t.Fatalf("non-deterministic ordering at %d: %s vs %s", j, first[j].ID, again[j].ID)
			}
		}
	}
}

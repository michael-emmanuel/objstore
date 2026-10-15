// Package cache provides a cost-bounded LRU with optional TTL and a
// single-flight group for stampede protection.
//
// Two very different things are cached in this system, with different
// correctness arguments:
//
//   - Object bytes, keyed by immutable version/blob ID. Entries can never be
//     stale because the bytes behind an ID never change; eviction is purely
//     a memory decision.
//   - Metadata (bucket config, key -> current version). Entries CAN be stale
//     because "current version" changes; they carry a TTL and are
//     invalidated by the gateway that performs the write. Other gateways may
//     serve the old version until the TTL expires. docs/consistency.md
//     spells out exactly what that means for clients.
package cache

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// LRU is safe for concurrent use. Cost is caller-defined (bytes for object
// data, 1 for metadata entries).
type LRU[K comparable, V any] struct {
	mu       sync.Mutex
	maxCost  int64
	cost     int64
	ttl      time.Duration
	ll       *list.List
	items    map[K]*list.Element
	now      func() time.Time
	hits     atomic.Int64
	misses   atomic.Int64
	evicts   atomic.Int64
	expireds atomic.Int64
}

type entry[K comparable, V any] struct {
	key     K
	val     V
	cost    int64
	expires time.Time
}

// NewLRU creates a cache bounded by maxCost. ttl 0 disables expiry.
func NewLRU[K comparable, V any](maxCost int64, ttl time.Duration) *LRU[K, V] {
	return &LRU[K, V]{maxCost: maxCost, ttl: ttl, ll: list.New(), items: map[K]*list.Element{}, now: time.Now}
}

// Get returns the value and true on a fresh hit.
func (c *LRU[K, V]) Get(k K) (V, bool) {
	var zero V
	if c == nil || c.maxCost <= 0 {
		return zero, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[k]
	if !ok {
		c.misses.Add(1)
		return zero, false
	}
	e := el.Value.(*entry[K, V])
	if !e.expires.IsZero() && c.now().After(e.expires) {
		c.removeElement(el)
		c.expireds.Add(1)
		c.misses.Add(1)
		return zero, false
	}
	c.ll.MoveToFront(el)
	c.hits.Add(1)
	return e.val, true
}

// Add inserts or replaces k. Items costing more than maxCost are not cached.
func (c *LRU[K, V]) Add(k K, v V, cost int64) {
	if c == nil || c.maxCost <= 0 || cost > c.maxCost {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var exp time.Time
	if c.ttl > 0 {
		exp = c.now().Add(c.ttl)
	}
	if el, ok := c.items[k]; ok {
		e := el.Value.(*entry[K, V])
		c.cost += cost - e.cost
		e.val, e.cost, e.expires = v, cost, exp
		c.ll.MoveToFront(el)
	} else {
		c.items[k] = c.ll.PushFront(&entry[K, V]{key: k, val: v, cost: cost, expires: exp})
		c.cost += cost
	}
	for c.cost > c.maxCost {
		back := c.ll.Back()
		if back == nil {
			break
		}
		c.removeElement(back)
		c.evicts.Add(1)
	}
}

// Remove invalidates k.
func (c *LRU[K, V]) Remove(k K) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[k]; ok {
		c.removeElement(el)
	}
}

func (c *LRU[K, V]) removeElement(el *list.Element) {
	e := el.Value.(*entry[K, V])
	c.ll.Remove(el)
	delete(c.items, e.key)
	c.cost -= e.cost
}

// Stats is a snapshot of cache counters.
type Stats struct {
	Hits, Misses, Evictions, Expirations int64
	Entries                              int
	Cost, MaxCost                        int64
}

func (c *LRU[K, V]) Stats() Stats {
	if c == nil {
		return Stats{}
	}
	c.mu.Lock()
	n, cost := c.ll.Len(), c.cost
	c.mu.Unlock()
	return Stats{Hits: c.hits.Load(), Misses: c.misses.Load(), Evictions: c.evicts.Load(),
		Expirations: c.expireds.Load(), Entries: n, Cost: cost, MaxCost: c.maxCost}
}

// ---------------------------------------------------------------------------

// Group coalesces concurrent calls for the same key into one execution.
// Without it, a hot object that misses the cache (cold start, eviction,
// expiry) triggers one backend read per concurrent request: a stampede.
type Group[K comparable, V any] struct {
	mu    sync.Mutex
	calls map[K]*call[V]
	dedup atomic.Int64
}

type call[V any] struct {
	wg  sync.WaitGroup
	val V
	err error
}

// Do runs fn once per key among concurrent callers. shared reports whether
// this caller received another caller's result.
func (g *Group[K, V]) Do(k K, fn func() (V, error)) (v V, err error, shared bool) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = map[K]*call[V]{}
	}
	if c, ok := g.calls[k]; ok {
		g.mu.Unlock()
		g.dedup.Add(1)
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := &call[V]{}
	c.wg.Add(1)
	g.calls[k] = c
	g.mu.Unlock()

	defer func() {
		g.mu.Lock()
		delete(g.calls, k)
		g.mu.Unlock()
		c.wg.Done()
	}()
	c.val, c.err = fn()
	return c.val, c.err, false
}

// Deduplicated returns how many calls were served by another caller.
func (g *Group[K, V]) Deduplicated() int64 { return g.dedup.Load() }

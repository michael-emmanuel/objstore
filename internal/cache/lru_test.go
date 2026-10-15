package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestLRUEvictsLeastRecentlyUsedByCost(t *testing.T) {
	c := NewLRU[string, int](10, 0)
	c.Add("a", 1, 4)
	c.Add("b", 2, 4)
	c.Get("a") // a is now most recent
	c.Add("c", 3, 4)
	if _, ok := c.Get("b"); ok {
		t.Fatal("b should have been evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("a should survive")
	}
	st := c.Stats()
	if st.Evictions != 1 || st.Cost != 8 {
		t.Fatalf("stats %+v", st)
	}
	c.Add("huge", 9, 11) // larger than cache: ignored
	if _, ok := c.Get("huge"); ok {
		t.Fatal("oversized item cached")
	}
}

func TestLRUTTLAndRemove(t *testing.T) {
	now := time.Unix(0, 0)
	c := NewLRU[string, int](10, time.Second)
	c.now = func() time.Time { return now }
	c.Add("k", 1, 1)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("fresh entry missing")
	}
	now = now.Add(2 * time.Second)
	if _, ok := c.Get("k"); ok {
		t.Fatal("expired entry served")
	}
	c.Add("k", 2, 1)
	c.Remove("k")
	if _, ok := c.Get("k"); ok {
		t.Fatal("removed entry served")
	}
	if c.Stats().Expirations != 1 {
		t.Fatal("expiration not counted")
	}
}

func TestDisabledAndNil(t *testing.T) {
	var nilc *LRU[string, int]
	nilc.Add("a", 1, 1)
	if _, ok := nilc.Get("a"); ok {
		t.Fatal()
	}
	z := NewLRU[string, int](0, 0)
	z.Add("a", 1, 1)
	if _, ok := z.Get("a"); ok {
		t.Fatal("zero-size cache must not store")
	}
}

func TestLRUConcurrent(t *testing.T) {
	c := NewLRU[string, int](100, 0)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := fmt.Sprint((g * i) % 300)
				c.Add(k, i, 1)
				c.Get(k)
				if i%50 == 0 {
					c.Remove(k)
				}
			}
		}(g)
	}
	wg.Wait()
	if c.Stats().Cost > 100 {
		t.Fatal("cost bound violated")
	}
}

func TestGroupPreventsStampede(t *testing.T) {
	var g Group[string, int]
	var backend atomic.Int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			v, err, _ := g.Do("hot", func() (int, error) {
				backend.Add(1)
				time.Sleep(50 * time.Millisecond)
				return 42, nil
			})
			if v != 42 || err != nil {
				t.Error(v, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if n := backend.Load(); n > 3 {
		t.Fatalf("backend called %d times for 100 concurrent misses", n)
	}
	if g.Deduplicated() < 90 {
		t.Fatalf("only %d deduplicated", g.Deduplicated())
	}
}

func BenchmarkLRUGetParallel(b *testing.B) {
	c := NewLRU[int, int](1<<20, 0)
	for i := 0; i < 1024; i++ {
		c.Add(i, i, 1)
	}
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Get(i & 1023)
			i++
		}
	})
}

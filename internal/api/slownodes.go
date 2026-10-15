package api

import (
	"sort"
	"sync"
	"time"

	"github.com/michael-emmanuel/objstore/internal/placement"
)

// slowNodes remembers storage nodes that were recently dropped from a write
// for being too slow (stalled mid-stream, or straggling after the write
// quorum was reached), so that the next segments of the same upload (and other
// uploads) are not placed on them for a while.
//
// Without this, a node that is alive but slow (failing disk, saturated NIC)
// stays ACTIVE in membership, and every segment placed on it pays the full
// stall timeout before the replica is dropped: a 200 MB upload with 16 MiB
// segments and one slow node took 41.6 s in the local cluster (8 segments x
// 5 s stall timeout). Small segments hit the straggler timeout instead
// (they fit in socket buffers, so the producer never stalls): 16 segments x
// 2 s. With the penalty, only the first segment pays.
//
// This is local, soft state (a circuit breaker per gateway). It never
// reduces durability: if excluding penalized nodes leaves fewer than RF
// candidates, penalized nodes are re-admitted one at a time, the one with
// the fewest strikes first. A node earns a strike every time it is slow
// again, so a healthy node that straggled once under transient contention
// is re-admitted before a node that is persistently slow. (An earlier
// version fell back to unrestricted placement as soon as the exclusions left
// too few candidates; a single false positive on a small cluster then put
// the slow node back into every placement.)
type slowNodes struct {
	ttl   time.Duration
	now   func() time.Time
	mu    sync.Mutex
	nodes map[string]*penalty
}

type penalty struct {
	until   time.Time
	strikes int
}

func newSlowNodes(ttl time.Duration) *slowNodes {
	return &slowNodes{ttl: ttl, now: time.Now, nodes: map[string]*penalty{}}
}

// mark penalizes a node for ttl from now and adds a strike.
func (s *slowNodes) mark(id string) {
	if s.ttl <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p := s.nodes[id]
	if p == nil {
		p = &penalty{}
		s.nodes[id] = p
	}
	p.strikes++
	p.until = s.now().Add(s.ttl)
}

// penalized returns the currently penalized node IDs ordered by how readily
// they should be re-admitted: fewest strikes first, ties broken by ID so the
// order is deterministic. Expired penalties are forgotten (strikes reset).
func (s *slowNodes) penalized() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	ids := make([]string, 0, len(s.nodes))
	for id, p := range s.nodes {
		if now.After(p.until) {
			delete(s.nodes, id)
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		a, b := s.nodes[ids[i]].strikes, s.nodes[ids[j]].strikes
		if a != b {
			return a < b
		}
		return ids[i] < ids[j]
	})
	return ids
}

func (s *slowNodes) count() int { return len(s.penalized()) }

// place chooses rf nodes for key avoiding penalized nodes where possible.
func (s *slowNodes) place(m *placement.Map, key string, rf int) ([]placement.Node, error) {
	order := s.penalized()
	if len(order) == 0 {
		return m.Place(key, rf)
	}
	excl := make(map[string]bool, len(order))
	for _, id := range order {
		excl[id] = true
	}
	for i := 0; ; i++ {
		nodes, err := m.PlaceExcluding(key, rf, excl)
		if len(nodes) >= rf || i == len(order) {
			return nodes, err
		}
		delete(excl, order[i]) // re-admit the least-penalized node and retry
	}
}

// Package hashing maps keys to an ordered preference list of nodes.
//
// Two algorithms are implemented behind one interface so they can be
// compared under the same tests and benchmarks:
//
//   - Ring: consistent hashing with virtual nodes. O(log V) lookup.
//   - Rendezvous: highest-random-weight hashing. O(N) lookup, no vnodes.
//
// Both move ~1/(N+1) of keys when a node joins, unlike hash(key) % N which
// moves ~N/(N+1). The placement package layers failure-domain constraints
// on top of the preference order. docs/tradeoffs.md explains the choice.
package hashing

import (
	"hash/fnv"
	"math"
	"sort"
	"strconv"
)

// Member is a node participating in hashing. Weight scales its share of
// keys (e.g. proportional to disk capacity). Weight <= 0 is treated as 1.
type Member struct {
	ID     string
	Weight int
}

// Hasher yields nodes in preference order for a key. Walk calls fn with each
// distinct node ID, most-preferred first, until fn returns false or all
// nodes have been visited.
type Hasher interface {
	Walk(key string, fn func(nodeID string) bool)
	Len() int
}

// Hash64 is FNV-1a followed by the splitmix64 finalizer. FNV alone has weak
// avalanche on short, similar inputs ("node-1#0", "node-1#1"), which shows
// up as vnode clustering; the finalizer fixes that cheaply. It must be
// stable across processes and versions: changing it moves every object.
func Hash64(s string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(s))
	return mix64(h.Sum64())
}

func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// ---------------------------------------------------------------------------
// Consistent hash ring

type point struct {
	hash uint64
	node int32
}

// Ring is an immutable consistent-hash ring. Build a new ring on membership
// change; readers never need locks.
type Ring struct {
	points []point
	ids    []string
}

// DefaultVnodesPerWeight trades ring memory (16 bytes/point) for balance.
// Measured in TestBalance: 128 vnodes gives ~8% relative stddev of primary
// ownership across 20 nodes, 256 gives ~6%. Rendezvous gives ~1.5% with no
// vnodes but costs O(N log N) per lookup; see docs/tradeoffs.md.
const DefaultVnodesPerWeight = 256

// NewRing builds a ring with vnodesPerWeight*weight points per member.
func NewRing(members []Member, vnodesPerWeight int) *Ring {
	if vnodesPerWeight <= 0 {
		vnodesPerWeight = DefaultVnodesPerWeight
	}
	r := &Ring{ids: make([]string, len(members))}
	for i, m := range members {
		r.ids[i] = m.ID
		w := m.Weight
		if w <= 0 {
			w = 1
		}
		for v := 0; v < w*vnodesPerWeight; v++ {
			r.points = append(r.points, point{hash: Hash64(m.ID + "#" + strconv.Itoa(v)), node: int32(i)})
		}
	}
	sort.Slice(r.points, func(a, b int) bool { return r.points[a].hash < r.points[b].hash })
	return r
}

func (r *Ring) Len() int { return len(r.ids) }

// Walk visits distinct nodes clockwise from hash(key).
func (r *Ring) Walk(key string, fn func(string) bool) {
	if len(r.points) == 0 {
		return
	}
	h := Hash64(key)
	start := sort.Search(len(r.points), func(i int) bool { return r.points[i].hash >= h })
	seen := make([]bool, len(r.ids))
	found := 0
	for i := 0; i < len(r.points) && found < len(r.ids); i++ {
		p := r.points[(start+i)%len(r.points)]
		if seen[p.node] {
			continue
		}
		seen[p.node] = true
		found++
		if !fn(r.ids[p.node]) {
			return
		}
	}
}

// Ownership returns the fraction of the hash space owned (as primary) by
// each node. Used by tests and the capacity docs to reason about skew.
func (r *Ring) Ownership() map[string]float64 {
	out := map[string]float64{}
	if len(r.points) == 0 {
		return out
	}
	for i, p := range r.points {
		prev := r.points[(i-1+len(r.points))%len(r.points)].hash
		span := p.hash - prev // wraps correctly for i==0 with unsigned arithmetic
		out[r.ids[p.node]] += float64(span) / math.Pow(2, 64)
	}
	return out
}

// ---------------------------------------------------------------------------
// Rendezvous (highest random weight) hashing

// Rendezvous ranks every member by a weighted per-(key,node) score.
type Rendezvous struct {
	members []Member
	seeds   []uint64
}

func NewRendezvous(members []Member) *Rendezvous {
	r := &Rendezvous{members: append([]Member(nil), members...)}
	for _, m := range members {
		r.seeds = append(r.seeds, Hash64(m.ID))
	}
	return r
}

func (r *Rendezvous) Len() int { return len(r.members) }

// Walk visits all members by descending weighted score. Weighted rendezvous
// uses score = -w / ln(u), u uniform in (0,1), which gives each node a share
// of keys proportional to its weight.
func (r *Rendezvous) Walk(key string, fn func(string) bool) {
	type scored struct {
		score float64
		idx   int
	}
	kh := Hash64(key)
	s := make([]scored, len(r.members))
	for i, m := range r.members {
		w := m.Weight
		if w <= 0 {
			w = 1
		}
		x := mix64(kh ^ r.seeds[i])
		u := (float64(x>>11) + 0.5) / (1 << 53) // (0,1)
		s[i] = scored{score: -float64(w) / math.Log(u), idx: i}
	}
	sort.Slice(s, func(a, b int) bool { return s[a].score > s[b].score })
	for _, e := range s {
		if !fn(r.members[e.idx].ID) {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Modulo hashing (for comparison only)

// Modulo is hash(key) % N. It exists solely so tests and benchmarks can show
// why it is unsuitable: almost every key moves when N changes.
type Modulo struct{ ids []string }

func NewModulo(members []Member) *Modulo {
	m := &Modulo{}
	for _, x := range members {
		m.ids = append(m.ids, x.ID)
	}
	return m
}

func (m *Modulo) Len() int { return len(m.ids) }

func (m *Modulo) Walk(key string, fn func(string) bool) {
	if len(m.ids) == 0 {
		return
	}
	start := int(Hash64(key) % uint64(len(m.ids)))
	for i := 0; i < len(m.ids); i++ {
		if !fn(m.ids[(start+i)%len(m.ids)]) {
			return
		}
	}
}

// First returns the most-preferred node for key, or "" if empty.
func First(h Hasher, key string) string {
	var out string
	h.Walk(key, func(id string) bool { out = id; return false })
	return out
}

// TopN returns up to n nodes in preference order.
func TopN(h Hasher, key string, n int) []string {
	out := make([]string, 0, n)
	h.Walk(key, func(id string) bool {
		out = append(out, id)
		return len(out) < n
	})
	return out
}

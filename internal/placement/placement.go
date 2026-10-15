// Package placement decides which storage nodes should hold a blob.
//
// It combines two concerns:
//
//  1. A stable preference order per key (from package hashing), so that
//     membership changes move only a small fraction of data.
//  2. Failure-domain constraints, so replicas of one blob land in different
//     availability zones (then racks) when possible.
//
// A placement.Map is an immutable snapshot of cluster membership tagged with
// an epoch. The gateway and background workers each hold the most recent
// snapshot; metadata records the replicas that were *actually* written, so a
// read never depends on recomputing placement correctly.
package placement

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/michael-emmanuel/objstore/internal/hashing"
)

// NodeState is the lifecycle state of a storage node.
//
//	ACTIVE         -> eligible for new data and reads
//	SUSPECT        -> missed heartbeats; still readable, not used for new writes
//	DOWN           -> presumed failed; not readable; repair after a delay
//	DRAINING       -> operator-requested; readable, no new data, rebalancer migrates away
//	DECOMMISSIONED -> removed from hashing entirely
type NodeState string

const (
	StateActive         NodeState = "ACTIVE"
	StateSuspect        NodeState = "SUSPECT"
	StateDown           NodeState = "DOWN"
	StateDraining       NodeState = "DRAINING"
	StateDecommissioned NodeState = "DECOMMISSIONED"
)

// Node describes a storage node and its failure domains.
type Node struct {
	ID            string    `json:"id"`
	Addr          string    `json:"addr"` // base URL, e.g. http://node1:9100
	Region        string    `json:"region"`
	Zone          string    `json:"zone"`
	Rack          string    `json:"rack"`
	Weight        int       `json:"weight"`
	State         NodeState `json:"state"`
	StateSince    time.Time `json:"state_since"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	CapacityBytes int64     `json:"capacity_bytes"`
	UsedBytes     int64     `json:"used_bytes"`
	BlobCount     int64     `json:"blob_count"`
}

// Readable reports whether reads may be attempted against the node.
func (n Node) Readable() bool {
	return n.State == StateActive || n.State == StateSuspect || n.State == StateDraining
}

// Writable reports whether the node may receive new data.
func (n Node) Writable() bool {
	if n.State != StateActive {
		return false
	}
	// Keep headroom: a node above 95% full stops accepting new placements
	// so that repair and in-flight writes can still land somewhere.
	if n.CapacityBytes > 0 && float64(n.UsedBytes) >= 0.95*float64(n.CapacityBytes) {
		return false
	}
	return true
}

// Algorithm selects the hashing strategy.
type Algorithm string

const (
	AlgoRing       Algorithm = "ring"
	AlgoRendezvous Algorithm = "rendezvous"
)

// Map is an immutable membership + placement snapshot.
type Map struct {
	Epoch  uint64
	nodes  []Node
	byID   map[string]int
	hasher hashing.Hasher
}

var ErrInsufficientNodes = errors.New("insufficient writable nodes for requested replication factor")

// NewMap builds a snapshot. All nodes except DECOMMISSIONED participate in
// hashing; ineligible ones are skipped during selection rather than removed
// from the hash space. This keeps the preference list stable while a node is
// briefly SUSPECT or DOWN, so a flapping node does not reshuffle data.
func NewMap(epoch uint64, nodes []Node, algo Algorithm) *Map {
	m := &Map{Epoch: epoch, byID: map[string]int{}}
	sorted := append([]Node(nil), nodes...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })
	var members []hashing.Member
	for _, n := range sorted {
		m.byID[n.ID] = len(m.nodes)
		m.nodes = append(m.nodes, n)
		if n.State != StateDecommissioned {
			members = append(members, hashing.Member{ID: n.ID, Weight: n.Weight})
		}
	}
	switch algo {
	case AlgoRendezvous:
		m.hasher = hashing.NewRendezvous(members)
	default:
		m.hasher = hashing.NewRing(members, 0)
	}
	return m
}

// Nodes returns a copy of all nodes.
func (m *Map) Nodes() []Node { return append([]Node(nil), m.nodes...) }

// Node looks up a node by ID.
func (m *Map) Node(id string) (Node, bool) {
	i, ok := m.byID[id]
	if !ok {
		return Node{}, false
	}
	return m.nodes[i], true
}

// Place returns up to rf writable nodes for key, spreading replicas across
// zones first, then racks, then anything. It returns ErrInsufficientNodes
// (along with whatever it found) if fewer than rf writable nodes exist.
func (m *Map) Place(key string, rf int) ([]Node, error) {
	return m.PlaceExcluding(key, rf, nil)
}

// PlaceExcluding is Place with some nodes removed from consideration, used
// by repair to find a target that does not already hold the blob.
func (m *Map) PlaceExcluding(key string, rf int, exclude map[string]bool) ([]Node, error) {
	var cands []Node
	m.hasher.Walk(key, func(id string) bool {
		n := m.nodes[m.byID[id]]
		if n.Writable() && !exclude[id] {
			cands = append(cands, n)
		}
		return len(cands) < candidateLimit(rf)
	})
	return selectDiverse(cands, rf, nil), errIfShort(cands, rf)
}

// PlaceStable is the placement used by the rebalancer to decide where data
// SHOULD live. It differs from Place in one way: it ignores fullness, so a
// node crossing the 95% write threshold does not suddenly make all its data
// "misplaced". Without that hysteresis the rebalancer would drain a nearly
// full node, which then drops below the threshold, becomes desirable again,
// and receives the data back: oscillation. (The rebalancer separately
// refuses to move data TO a node that is nearly full.)
func (m *Map) PlaceStable(key string, rf int) []Node {
	var cands []Node
	m.hasher.Walk(key, func(id string) bool {
		n := m.nodes[m.byID[id]]
		if n.State == StateActive {
			cands = append(cands, n)
		}
		return len(cands) < candidateLimit(rf)
	})
	return selectDiverse(cands, rf, nil)
}

// Complete picks additional nodes for key so that, together with the nodes
// in have, the set reaches rf, preferring domains not already used by have.
func (m *Map) Complete(key string, rf int, have []Node) []Node {
	exclude := map[string]bool{}
	for _, h := range have {
		exclude[h.ID] = true
	}
	var cands []Node
	m.hasher.Walk(key, func(id string) bool {
		n := m.nodes[m.byID[id]]
		if n.Writable() && !exclude[id] {
			cands = append(cands, n)
		}
		return len(cands) < candidateLimit(rf)
	})
	need := rf - len(have)
	if need <= 0 {
		return nil
	}
	return selectDiverse(cands, need, have)
}

// candidateLimit bounds how far down the preference list we look. Walking
// every node would make placement O(N) per blob, which matters when repair
// and rebalancing re-evaluate placement for every segment in the cluster.
// 8x RF (min 32) has always been enough to find zone diversity in practice;
// if domains are extremely unbalanced the result degrades to "fewer zones",
// never to an error.
func candidateLimit(rf int) int {
	if rf*8 > 32 {
		return rf * 8
	}
	return 32
}

func errIfShort(c []Node, rf int) error {
	if len(c) < rf {
		return fmt.Errorf("%w: want %d, have %d", ErrInsufficientNodes, rf, len(c))
	}
	return nil
}

// selectDiverse walks candidates in hash-preference order in three passes:
// new zone, then new (zone, rack), then anything. Within each pass the hash
// order is preserved, so the result is deterministic for a given key and
// membership.
func selectDiverse(cands []Node, need int, already []Node) []Node {
	usedZone := map[string]bool{}
	usedRack := map[string]bool{}
	for _, n := range already {
		usedZone[n.Region+"/"+n.Zone] = true
		usedRack[n.Region+"/"+n.Zone+"/"+n.Rack] = true
	}
	picked := map[string]bool{}
	var out []Node
	take := func(n Node) {
		out = append(out, n)
		picked[n.ID] = true
		usedZone[n.Region+"/"+n.Zone] = true
		usedRack[n.Region+"/"+n.Zone+"/"+n.Rack] = true
	}
	for pass := 0; pass < 3 && len(out) < need; pass++ {
		for _, n := range cands {
			if len(out) >= need {
				break
			}
			if picked[n.ID] {
				continue
			}
			z := n.Region + "/" + n.Zone
			r := z + "/" + n.Rack
			switch {
			case pass == 0 && usedZone[z]:
				continue
			case pass == 1 && usedRack[r]:
				continue
			}
			take(n)
		}
	}
	return out
}

// DomainSpread reports how many distinct zones a set of node IDs spans.
func (m *Map) DomainSpread(ids []string) int {
	z := map[string]bool{}
	for _, id := range ids {
		if n, ok := m.Node(id); ok {
			z[n.Region+"/"+n.Zone] = true
		}
	}
	return len(z)
}

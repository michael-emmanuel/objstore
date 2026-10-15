package api

import (
	"fmt"
	"testing"
	"time"

	"github.com/michael-emmanuel/objstore/internal/placement"
)

func TestSlowNodesExpire(t *testing.T) {
	now := time.Unix(1000, 0)
	s := newSlowNodes(10 * time.Second)
	s.now = func() time.Time { return now }
	if len(s.penalized()) != 0 {
		t.Fatal("empty set should have no penalties")
	}
	s.mark("n1")
	if p := s.penalized(); len(p) != 1 || p[0] != "n1" {
		t.Fatalf("n1 should be penalized: %v", p)
	}
	now = now.Add(11 * time.Second)
	if len(s.penalized()) != 0 {
		t.Fatal("penalty should expire")
	}
}

func TestSlowNodesDisabled(t *testing.T) {
	s := newSlowNodes(-1)
	s.mark("n1")
	if len(s.penalized()) != 0 {
		t.Fatal("negative ttl must disable penalties")
	}
}

func testMap(n int) *placement.Map {
	zones := []string{"az-a", "az-b", "az-c"}
	var nodes []placement.Node
	for i := 1; i <= n; i++ {
		nodes = append(nodes, placement.Node{ID: fmt.Sprintf("node-%d", i), Zone: zones[(i-1)%3],
			Rack: fmt.Sprintf("r%d", i), Weight: 1, State: placement.StateActive})
	}
	return placement.NewMap(1, nodes, placement.AlgoRing)
}

func contains(nodes []placement.Node, id string) bool {
	for _, n := range nodes {
		if n.ID == id {
			return true
		}
	}
	return false
}

// Regression test for the CI failure of TestSlowNodePenalizedAcrossSegments:
// with 4 nodes and RF 3, a persistently slow node plus one healthy node that
// straggled once under contention are both penalized. Excluding both leaves
// only 2 candidates. The old code then fell back to unrestricted placement,
// putting the slow node back into most placements. Now the healthy node
// (fewer strikes) is re-admitted and the slow node stays out.
func TestPlaceReadmitsLeastPenalizedFirst(t *testing.T) {
	m := testMap(4)
	s := newSlowNodes(time.Minute)
	for i := 0; i < 3; i++ {
		s.mark("node-2") // persistently slow
	}
	s.mark("node-3") // one false positive
	for k := 0; k < 200; k++ {
		nodes, err := s.place(m, fmt.Sprintf("blob-%d", k), 3)
		if err != nil || len(nodes) != 3 {
			t.Fatalf("placement must still reach RF: %v %v", nodes, err)
		}
		if contains(nodes, "node-2") {
			t.Fatalf("slow node re-admitted before the less-penalized node: %v", nodes)
		}
	}
}

func TestPlaceNeverReducesReplicaCount(t *testing.T) {
	m := testMap(3)
	s := newSlowNodes(time.Minute)
	for _, id := range []string{"node-1", "node-2", "node-3"} {
		s.mark(id)
	}
	nodes, err := s.place(m, "blob", 3)
	if err != nil || len(nodes) != 3 {
		t.Fatalf("all nodes penalized: placement must still use all 3: %v %v", nodes, err)
	}
}

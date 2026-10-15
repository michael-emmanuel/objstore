package placement

import (
	"fmt"
	"testing"
)

func cluster() []Node {
	// 6 nodes: zones a,b,c with 2 racks each.
	var out []Node
	for i, z := range []string{"a", "a", "b", "b", "c", "c"} {
		out = append(out, Node{ID: fmt.Sprintf("n%d", i), Zone: z, Rack: fmt.Sprintf("r%d", i%2), Region: "r1", Weight: 1, State: StateActive})
	}
	return out
}

func TestPlaceSpreadsAcrossZones(t *testing.T) {
	for _, algo := range []Algorithm{AlgoRing, AlgoRendezvous} {
		m := NewMap(1, cluster(), algo)
		for i := 0; i < 2000; i++ {
			nodes, err := m.Place(fmt.Sprintf("blob-%d", i), 3)
			if err != nil || len(nodes) != 3 {
				t.Fatalf("%s: place: %v %v", algo, nodes, err)
			}
			ids := []string{nodes[0].ID, nodes[1].ID, nodes[2].ID}
			if m.DomainSpread(ids) != 3 {
				t.Fatalf("%s: replicas %v not spread over 3 zones", algo, ids)
			}
		}
	}
}

func TestPlaceFallsBackWhenFewerZones(t *testing.T) {
	nodes := []Node{
		{ID: "a1", Zone: "a", Rack: "1", State: StateActive},
		{ID: "a2", Zone: "a", Rack: "2", State: StateActive},
		{ID: "a3", Zone: "a", Rack: "2", State: StateActive},
		{ID: "b1", Zone: "b", Rack: "1", State: StateActive},
	}
	m := NewMap(1, nodes, AlgoRing)
	for i := 0; i < 500; i++ {
		got, err := m.Place(fmt.Sprintf("k%d", i), 3)
		if err != nil {
			t.Fatal(err)
		}
		ids := []string{got[0].ID, got[1].ID, got[2].ID}
		if m.DomainSpread(ids) != 2 {
			t.Fatalf("expected both zones used: %v", ids)
		}
		racksInA := map[string]bool{}
		for _, n := range got {
			if n.Zone == "a" {
				racksInA[n.Rack] = true
			}
		}
		if len(racksInA) == 1 && countZone(got, "a") == 2 {
			t.Fatalf("two replicas in zone a share a rack although another rack exists: %v", ids)
		}
	}
}

func countZone(ns []Node, z string) int {
	c := 0
	for _, n := range ns {
		if n.Zone == z {
			c++
		}
	}
	return c
}

func TestIneligibleNodesSkippedButStable(t *testing.T) {
	nodes := cluster()
	before := NewMap(1, nodes, AlgoRing)
	nodes[2].State = StateDraining
	nodes[4].State = StateDown
	after := NewMap(2, nodes, AlgoRing)
	changed := 0
	for i := 0; i < 1000; i++ {
		k := fmt.Sprintf("k%d", i)
		a, _ := before.Place(k, 3)
		b, _ := after.Place(k, 3)
		for _, n := range b {
			if n.ID == "n2" || n.ID == "n4" {
				t.Fatalf("placed on ineligible node %s", n.ID)
			}
		}
		if !sameIDs(a, b) {
			changed++
		}
	}
	if changed == 1000 {
		t.Fatal("every placement changed; expected only those touching n2/n4")
	}
}

func sameIDs(a, b []Node) bool {
	if len(a) != len(b) {
		return false
	}
	s := map[string]bool{}
	for _, n := range a {
		s[n.ID] = true
	}
	for _, n := range b {
		if !s[n.ID] {
			return false
		}
	}
	return true
}

func TestInsufficientNodes(t *testing.T) {
	m := NewMap(1, cluster()[:2], AlgoRing)
	got, err := m.Place("k", 3)
	if err == nil || len(got) != 2 {
		t.Fatalf("want 2 nodes + error, got %v %v", got, err)
	}
}

func TestFullNodeNotWritable(t *testing.T) {
	n := Node{ID: "x", State: StateActive, CapacityBytes: 100, UsedBytes: 96}
	if n.Writable() {
		t.Fatal("96% full node should not accept new placements")
	}
	if !n.Readable() {
		t.Fatal("full node must remain readable")
	}
}

func TestCompletePrefersNewDomains(t *testing.T) {
	m := NewMap(1, cluster(), AlgoRing)
	have := []Node{mustNode(m, "n0"), mustNode(m, "n2")} // zones a, b
	got := m.Complete("k", 3, have)
	if len(got) != 1 || got[0].Zone != "c" {
		t.Fatalf("expected a zone-c node, got %+v", got)
	}
}

func mustNode(m *Map, id string) Node {
	n, _ := m.Node(id)
	return n
}

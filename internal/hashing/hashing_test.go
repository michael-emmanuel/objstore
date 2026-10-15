package hashing

import (
	"fmt"
	"math"
	"testing"
)

func members(n int) []Member {
	out := make([]Member, n)
	for i := range out {
		out[i] = Member{ID: fmt.Sprintf("node-%d", i), Weight: 1}
	}
	return out
}

func keys(n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("bucket/object-%d", i)
	}
	return out
}

// movedFraction measures how many keys change primary owner when one node is
// added to a cluster of n.
func movedFraction(build func([]Member) Hasher, n int, ks []string) float64 {
	before := build(members(n))
	after := build(members(n + 1))
	moved := 0
	for _, k := range ks {
		if First(before, k) != First(after, k) {
			moved++
		}
	}
	return float64(moved) / float64(len(ks))
}

func TestKeyMovementOnNodeAdd(t *testing.T) {
	ks := keys(50000)
	const n = 10
	ideal := 1.0 / float64(n+1)

	ring := movedFraction(func(m []Member) Hasher { return NewRing(m, 0) }, n, ks)
	rdv := movedFraction(func(m []Member) Hasher { return NewRendezvous(m) }, n, ks)
	mod := movedFraction(func(m []Member) Hasher { return NewModulo(m) }, n, ks)
	t.Logf("keys moved adding node %d: ideal=%.3f ring=%.3f rendezvous=%.3f modulo=%.3f", n+1, ideal, ring, rdv, mod)

	if math.Abs(ring-ideal) > 0.03 {
		t.Errorf("ring moved %.3f, want ~%.3f", ring, ideal)
	}
	if math.Abs(rdv-ideal) > 0.02 {
		t.Errorf("rendezvous moved %.3f, want ~%.3f", rdv, ideal)
	}
	if mod < 0.8 {
		t.Errorf("modulo moved only %.3f; expected ~%.3f (this is why we don't use it)", mod, float64(n)/float64(n+1))
	}
}

// Moved keys must move TO the new node, never between old nodes.
func TestMovementOnlyToNewNode(t *testing.T) {
	for name, build := range map[string]func([]Member) Hasher{
		"ring":       func(m []Member) Hasher { return NewRing(m, 0) },
		"rendezvous": func(m []Member) Hasher { return NewRendezvous(m) },
	} {
		before, after := build(members(5)), build(members(6))
		for _, k := range keys(10000) {
			a, b := First(before, k), First(after, k)
			if a != b && b != "node-5" {
				t.Fatalf("%s: key %s moved %s -> %s (not to new node)", name, k, a, b)
			}
		}
	}
}

func loadStddev(h Hasher, n int, ks []string) (stddev, maxOverMean float64) {
	counts := map[string]int{}
	for _, k := range ks {
		counts[First(h, k)]++
	}
	mean := float64(len(ks)) / float64(n)
	var ss, mx float64
	for _, c := range counts {
		ss += (float64(c) - mean) * (float64(c) - mean)
		mx = math.Max(mx, float64(c))
	}
	return math.Sqrt(ss/float64(n)) / mean, mx / mean
}

func TestBalance(t *testing.T) {
	ks := keys(100000)
	for _, vn := range []int{1, 16, 128, 256} {
		sd, mx := loadStddev(NewRing(members(20), vn), 20, ks)
		t.Logf("ring vnodes=%-3d relative stddev=%.3f max/mean=%.2f", vn, sd, mx)
		if vn == 256 && sd > 0.09 {
			t.Errorf("ring with 128 vnodes too skewed: %.3f", sd)
		}
	}
	sd, mx := loadStddev(NewRendezvous(members(20)), 20, ks)
	t.Logf("rendezvous         relative stddev=%.3f max/mean=%.2f", sd, mx)
	if sd > 0.05 {
		t.Errorf("rendezvous too skewed: %.3f", sd)
	}
}

func TestWeights(t *testing.T) {
	m := []Member{{ID: "big", Weight: 3}, {ID: "small", Weight: 1}}
	ks := keys(40000)
	for name, h := range map[string]Hasher{"ring": NewRing(m, 0), "rendezvous": NewRendezvous(m)} {
		big := 0
		for _, k := range ks {
			if First(h, k) == "big" {
				big++
			}
		}
		frac := float64(big) / float64(len(ks))
		if math.Abs(frac-0.75) > 0.05 {
			t.Errorf("%s: weight-3 node got %.3f of keys, want ~0.75", name, frac)
		}
	}
}

func TestWalkVisitsAllDistinct(t *testing.T) {
	for name, h := range map[string]Hasher{"ring": NewRing(members(7), 8), "rendezvous": NewRendezvous(members(7)), "modulo": NewModulo(members(7))} {
		seen := map[string]bool{}
		h.Walk("k", func(id string) bool {
			if seen[id] {
				t.Fatalf("%s: duplicate %s", name, id)
			}
			seen[id] = true
			return true
		})
		if len(seen) != 7 {
			t.Fatalf("%s: visited %d nodes", name, len(seen))
		}
	}
	if First(NewRing(nil, 0), "k") != "" {
		t.Fatal("empty ring should yield nothing")
	}
}

func TestOwnershipSumsToOne(t *testing.T) {
	sum := 0.0
	for _, f := range NewRing(members(5), 64).Ownership() {
		sum += f
	}
	if math.Abs(sum-1) > 1e-9 {
		t.Fatalf("ownership sums to %f", sum)
	}
}

func BenchmarkRingTop3_100Nodes(b *testing.B)   { benchTop3(b, NewRing(members(100), 0)) }
func BenchmarkRingTop3_1000Nodes(b *testing.B)  { benchTop3(b, NewRing(members(1000), 0)) }
func BenchmarkRendezvousTop3_100(b *testing.B)  { benchTop3(b, NewRendezvous(members(100))) }
func BenchmarkRendezvousTop3_1000(b *testing.B) { benchTop3(b, NewRendezvous(members(1000))) }

func benchTop3(b *testing.B, h Hasher) {
	ks := keys(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		TopN(h, ks[i%len(ks)], 3)
	}
}

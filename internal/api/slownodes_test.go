package api

import (
	"testing"
	"time"
)

func TestSlowNodesExpire(t *testing.T) {
	now := time.Unix(1000, 0)
	s := newSlowNodes(10 * time.Second)
	s.now = func() time.Time { return now }
	if s.excluded() != nil {
		t.Fatal("empty set should be nil")
	}
	s.mark("n1")
	if ex := s.excluded(); !ex["n1"] || len(ex) != 1 {
		t.Fatalf("n1 should be penalized: %v", ex)
	}
	now = now.Add(11 * time.Second)
	if s.excluded() != nil {
		t.Fatal("penalty should expire")
	}
}

func TestSlowNodesDisabled(t *testing.T) {
	s := newSlowNodes(-1)
	s.mark("n1")
	if s.excluded() != nil {
		t.Fatal("negative ttl must disable penalties")
	}
}

package api

import (
	"sync"
	"time"
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
// candidates, placement ignores the penalty.
type slowNodes struct {
	ttl   time.Duration
	now   func() time.Time
	mu    sync.Mutex
	until map[string]time.Time
}

func newSlowNodes(ttl time.Duration) *slowNodes {
	return &slowNodes{ttl: ttl, now: time.Now, until: map[string]time.Time{}}
}

// mark penalizes a node for ttl from now.
func (s *slowNodes) mark(id string) {
	if s.ttl <= 0 {
		return
	}
	s.mu.Lock()
	s.until[id] = s.now().Add(s.ttl)
	s.mu.Unlock()
}

// excluded returns the currently penalized nodes (nil if none).
func (s *slowNodes) excluded() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.until) == 0 {
		return nil
	}
	now := s.now()
	var out map[string]bool
	for id, t := range s.until {
		if now.After(t) {
			delete(s.until, id)
			continue
		}
		if out == nil {
			out = map[string]bool{}
		}
		out[id] = true
	}
	return out
}

func (s *slowNodes) count() int { return len(s.excluded()) }

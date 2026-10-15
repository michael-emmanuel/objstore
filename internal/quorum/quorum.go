// Package quorum contains the N/W/R policy and a generic helper for running
// an operation against N participants and deciding success at a threshold.
//
// In this system the quorum applies to the DATA plane: a write succeeds when
// W replicas have durably committed the blob. Because blobs are immutable and
// content-verified, a read quorum on data cannot return "stale bytes" — a
// replica either has the exact blob or it does not. Staleness lives in the
// METADATA plane (which version is current), and is handled there by a single
// authoritative writer per shard, not by quorum. R is therefore used for
// optional read verification (GET ?consistency=quorum checks that R replicas
// hold a matching copy) rather than for version resolution. See
// docs/consistency.md for the full argument and its assumptions.
package quorum

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Config is a replication/quorum policy.
type Config struct {
	N int `json:"n"` // replication factor
	W int `json:"w"` // replicas that must durably ack before a write succeeds
	R int `json:"r"` // replicas consulted for a verified read
}

var ErrQuorumUnreachable = errors.New("quorum unreachable")

// Validate checks basic sanity. It deliberately does NOT require R+W>N:
// W=1 is a legitimate (if risky) choice for reproducible data. Overlaps
// reports whether the classic intersection property holds.
func (c Config) Validate() error {
	if c.N < 1 || c.W < 1 || c.R < 1 || c.W > c.N || c.R > c.N {
		return fmt.Errorf("invalid quorum config N=%d W=%d R=%d", c.N, c.W, c.R)
	}
	return nil
}

// Overlaps reports R+W>N: every read set intersects every write set, given a
// FIXED replica set. When the replica set changes (repair, rebalancing,
// sloppy quorums) the property no longer follows from arithmetic alone.
func (c Config) Overlaps() bool { return c.R+c.W > c.N }

// ForRF derives a majority policy for a replication factor.
func ForRF(rf int) Config {
	if rf < 1 {
		rf = 1
	}
	maj := rf/2 + 1
	return Config{N: rf, W: maj, R: maj}
}

// Outcome reports per-participant results.
type Outcome struct {
	Succeeded []int
	Errors    map[int]error
}

// Do runs fn(i) for i in [0,n) concurrently. It returns once `need` calls
// succeed (cancelling stragglers unless waitAll), or once success has become
// impossible. With waitAll it always waits for every call, which writers use
// to maximise the number of replicas that end up with data.
func Do(ctx context.Context, n, need int, waitAll bool, fn func(ctx context.Context, i int) error) (Outcome, error) {
	if need > n {
		return Outcome{Errors: map[int]error{}}, fmt.Errorf("%w: need %d of %d", ErrQuorumUnreachable, need, n)
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type res struct {
		i   int
		err error
	}
	ch := make(chan res, n) // buffered: senders never block, so no goroutine leaks
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ch <- res{i, fn(cctx, i)}
		}(i)
	}
	out := Outcome{Errors: map[int]error{}}
	for got := 0; got < n; got++ {
		r := <-ch
		if r.err == nil {
			out.Succeeded = append(out.Succeeded, r.i)
		} else {
			out.Errors[r.i] = r.err
		}
		if !waitAll && len(out.Succeeded) >= need {
			cancel()
			break
		}
		if len(out.Errors) > n-need {
			if !waitAll {
				cancel()
				break
			}
		}
	}
	if waitAll {
		wg.Wait()
	}
	if len(out.Succeeded) >= need {
		return out, nil
	}
	return out, fmt.Errorf("%w: %d of %d succeeded, needed %d", ErrQuorumUnreachable, len(out.Succeeded), n, need)
}

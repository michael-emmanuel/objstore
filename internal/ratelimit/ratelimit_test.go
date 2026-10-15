package ratelimit

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestAllowBurstThenRefill(t *testing.T) {
	now := time.Unix(0, 0)
	b := NewBucket(10, 5)
	b.now = func() time.Time { return now }
	b.last = now
	for i := 0; i < 5; i++ {
		if !b.Allow(1) {
			t.Fatalf("burst token %d denied", i)
		}
	}
	if b.Allow(1) {
		t.Fatal("allowed beyond burst")
	}
	now = now.Add(200 * time.Millisecond) // +2 tokens
	if !b.Allow(2) || b.Allow(1) {
		t.Fatal("refill incorrect")
	}
	now = now.Add(time.Hour)
	if b.Allow(6) {
		t.Fatal("tokens must cap at burst")
	}
}

func TestWaitHonoursContext(t *testing.T) {
	b := NewBucket(1, 1)
	b.Allow(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := b.Wait(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want deadline, got %v", err)
	}
}

func TestBandwidthReaderThrottles(t *testing.T) {
	// 200 KB/s with 20 KB burst; reading 100 KB should take ~400ms
	// (first 20 KB free from burst).
	b := NewBucket(200_000, 20_000)
	r := Reader(context.Background(), bytes.NewReader(make([]byte, 100_000)), b)
	start := time.Now()
	n, err := io.Copy(io.Discard, r)
	el := time.Since(start)
	if err != nil || n != 100_000 {
		t.Fatal(n, err)
	}
	if el < 300*time.Millisecond || el > 2*time.Second {
		t.Fatalf("elapsed %v, expected ~400ms", el)
	}
}

func TestUnlimited(t *testing.T) {
	var b *Bucket
	if !b.Allow(1e12) || b.Wait(context.Background(), 1e12) != nil {
		t.Fatal("nil bucket must be unlimited")
	}
	if !NewBucket(0, 0).Allow(1e12) {
		t.Fatal("zero rate must be unlimited")
	}
}

func TestSemaphore(t *testing.T) {
	s := NewSemaphore(2)
	r1, _ := s.TryAcquire()
	r2, _ := s.TryAcquire()
	if _, err := s.TryAcquire(); !errors.Is(err, ErrSaturated) {
		t.Fatal("third acquire should fail")
	}
	r1()
	r1() // idempotent release
	if s.InUse() != 1 {
		t.Fatalf("in use %d", s.InUse())
	}
	r3, err := s.TryAcquire()
	if err != nil {
		t.Fatal(err)
	}
	r2()
	r3()
}

package quorum

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfig(t *testing.T) {
	if !(Config{N: 3, W: 2, R: 2}).Overlaps() {
		t.Fatal("2+2>3 should overlap")
	}
	if (Config{N: 3, W: 1, R: 1}).Overlaps() {
		t.Fatal("1+1>3 should not overlap")
	}
	if err := (Config{N: 3, W: 4, R: 1}).Validate(); err == nil {
		t.Fatal("W>N must be invalid")
	}
	if c := ForRF(5); c.W != 3 || c.R != 3 {
		t.Fatalf("ForRF(5) = %+v", c)
	}
}

func TestDoReturnsAtQuorumAndCancelsStragglers(t *testing.T) {
	var cancelled atomic.Int32
	start := time.Now()
	out, err := Do(context.Background(), 3, 2, false, func(ctx context.Context, i int) error {
		if i == 2 {
			select {
			case <-ctx.Done():
				cancelled.Add(1)
				return ctx.Err()
			case <-time.After(5 * time.Second):
				return nil
			}
		}
		return nil
	})
	if err != nil || len(out.Succeeded) != 2 {
		t.Fatalf("out=%+v err=%v", out, err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("did not return at quorum")
	}
	time.Sleep(50 * time.Millisecond)
	if cancelled.Load() != 1 {
		t.Fatal("straggler not cancelled")
	}
}

func TestDoFailsFastWhenImpossible(t *testing.T) {
	boom := errors.New("boom")
	out, err := Do(context.Background(), 3, 2, false, func(ctx context.Context, i int) error {
		if i < 2 {
			return boom
		}
		<-ctx.Done()
		return ctx.Err()
	})
	if !errors.Is(err, ErrQuorumUnreachable) {
		t.Fatalf("want quorum error, got %v (%+v)", err, out)
	}
}

func TestDoWaitAll(t *testing.T) {
	var calls atomic.Int32
	out, err := Do(context.Background(), 3, 1, true, func(ctx context.Context, i int) error {
		time.Sleep(time.Duration(i) * 10 * time.Millisecond)
		calls.Add(1)
		if i == 1 {
			return errors.New("x")
		}
		return nil
	})
	if err != nil || calls.Load() != 3 || len(out.Succeeded) != 2 || len(out.Errors) != 1 {
		t.Fatalf("out=%+v err=%v calls=%d", out, err, calls.Load())
	}
}

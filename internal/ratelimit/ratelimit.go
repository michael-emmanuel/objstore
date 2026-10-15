// Package ratelimit provides token buckets for request rate and bandwidth,
// plus bounded concurrency semaphores.
//
// Request rate and bandwidth are separate dimensions on purpose: 10 req/s of
// 1 GiB objects is 10 GiB/s. A request limiter alone cannot stop that, and a
// bandwidth limiter alone cannot stop 100k req/s of HEADs hammering metadata.
package ratelimit

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"
)

// Bucket is a token bucket. Rate is tokens/second, Burst is capacity.
// A Rate <= 0 means unlimited.
type Bucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
}

func NewBucket(rate, burst float64) *Bucket {
	if burst <= 0 {
		burst = rate
	}
	return &Bucket{rate: rate, burst: burst, tokens: burst, now: time.Now, last: time.Now()}
}

func (b *Bucket) refill() {
	now := b.now()
	b.tokens += now.Sub(b.last).Seconds() * b.rate
	if b.tokens > b.burst {
		b.tokens = b.burst
	}
	b.last = now
}

// Allow takes n tokens if available, without waiting.
func (b *Bucket) Allow(n float64) bool {
	if b == nil || b.rate <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refill()
	if b.tokens >= n {
		b.tokens -= n
		return true
	}
	return false
}

// Wait blocks until n tokens are taken or ctx ends. Requests larger than the
// burst are paid for in burst-sized installments, so a huge request is
// throttled smoothly instead of being rejected outright.
func (b *Bucket) Wait(ctx context.Context, n float64) error {
	if b == nil || b.rate <= 0 {
		return nil
	}
	for n > 0 {
		take := n
		if take > b.burst {
			take = b.burst
		}
		if err := b.waitOne(ctx, take); err != nil {
			return err
		}
		n -= take
	}
	return nil
}

func (b *Bucket) waitOne(ctx context.Context, n float64) error {
	for {
		b.mu.Lock()
		b.refill()
		if b.tokens >= n {
			b.tokens -= n
			b.mu.Unlock()
			return nil
		}
		deficit := n - b.tokens
		d := time.Duration(deficit / b.rate * float64(time.Second))
		b.mu.Unlock()
		if d < time.Millisecond {
			d = time.Millisecond
		}
		t := time.NewTimer(d)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
}

// Reader throttles reads from r through bucket b (bytes as tokens). Because
// the reader simply reads slower, the backpressure propagates to TCP and the
// sending client slows down; no extra buffering is introduced.
func Reader(ctx context.Context, r io.Reader, b *Bucket) io.Reader {
	if b == nil || b.rate <= 0 {
		return r
	}
	return &limitedReader{ctx: ctx, r: r, b: b}
}

type limitedReader struct {
	ctx context.Context
	r   io.Reader
	b   *Bucket
}

func (l *limitedReader) Read(p []byte) (int, error) {
	// Cap individual reads so a single Read cannot borrow far beyond burst.
	if max := int(l.b.burst); max > 0 && len(p) > max {
		p = p[:max]
	}
	n, err := l.r.Read(p)
	if n > 0 {
		if werr := l.b.Wait(l.ctx, float64(n)); werr != nil {
			return n, werr
		}
	}
	return n, err
}

// Writer throttles writes through bucket b.
func Writer(ctx context.Context, w io.Writer, b *Bucket) io.Writer {
	if b == nil || b.rate <= 0 {
		return w
	}
	return &limitedWriter{ctx: ctx, w: w, b: b}
}

type limitedWriter struct {
	ctx context.Context
	w   io.Writer
	b   *Bucket
}

func (l *limitedWriter) Write(p []byte) (int, error) {
	written := 0
	for len(p) > 0 {
		chunk := p
		if max := int(l.b.burst); max > 0 && len(chunk) > max {
			chunk = chunk[:max]
		}
		if err := l.b.Wait(l.ctx, float64(len(chunk))); err != nil {
			return written, err
		}
		n, err := l.w.Write(chunk)
		written += n
		if err != nil {
			return written, err
		}
		p = p[n:]
	}
	return written, nil
}

// Semaphore bounds concurrency. Limit <= 0 means unlimited.
type Semaphore struct{ ch chan struct{} }

var ErrSaturated = errors.New("concurrency limit reached")

func NewSemaphore(limit int) *Semaphore {
	if limit <= 0 {
		return &Semaphore{}
	}
	return &Semaphore{ch: make(chan struct{}, limit)}
}

// TryAcquire returns a release func, or ErrSaturated immediately. Rejecting
// instead of queueing is deliberate for uploads: a queued 10 GB upload holds
// a connection and client buffers while doing nothing useful.
func (s *Semaphore) TryAcquire() (func(), error) {
	if s.ch == nil {
		return func() {}, nil
	}
	select {
	case s.ch <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-s.ch }) }, nil
	default:
		return nil, ErrSaturated
	}
}

// Acquire waits for a slot.
func (s *Semaphore) Acquire(ctx context.Context) (func(), error) {
	if s.ch == nil {
		return func() {}, nil
	}
	select {
	case s.ch <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-s.ch }) }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// InUse reports current holders.
func (s *Semaphore) InUse() int {
	if s.ch == nil {
		return 0
	}
	return len(s.ch)
}

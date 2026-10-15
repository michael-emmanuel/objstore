// Package replication implements the synchronous fan-out write used by the
// gateway: one incoming byte stream is copied to N storage nodes at once,
// with bounded memory and a write quorum.
//
//	client body ──read 256 KiB──▶ producer ──chunk──▶ [queue d=4] ─▶ pipe ─▶ HTTP PUT ─▶ node A
//	                                  │      ──chunk──▶ [queue d=4] ─▶ pipe ─▶ HTTP PUT ─▶ node B
//	                                  └ sha256  ──chunk──▶ [queue d=4] ─▶ pipe ─▶ HTTP PUT ─▶ node C
//
// Memory per segment write is bounded by about (QueueDepth+2) * ChunkSize
// per replica: chunks are shared (reference counted), not copied per
// replica. The producer only reads more from the client when every live
// replica has queue space, so the slowest live replica sets the pace and TCP
// flow control pushes back on the client. That is backpressure, end to end.
//
// A replica whose queue stays full for StallTimeout is dropped (its request
// is cancelled). If the remaining live replicas still satisfy the write
// quorum W, the write continues at the speed of the healthy ones and the
// dropped replica is recorded so repair can fix it later. If not, the write
// fails. This is the explicit trade: one slow disk must not slow every
// client write in the cluster, but we never acknowledge below W.
package replication

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/michael-emmanuel/objstore/internal/quorum"
	"github.com/michael-emmanuel/objstore/internal/storage"
	"github.com/michael-emmanuel/objstore/internal/storagenode"
)

// Target is a replica destination.
type Target struct {
	NodeID string
	Addr   string
}

// Options tunes the writer.
type Options struct {
	ChunkSize        int           // bytes read from the source per step (default 256 KiB)
	QueueDepth       int           // chunks buffered per replica (default 4)
	StallTimeout     time.Duration // drop a replica whose queue stays full this long (default 10s)
	StragglerTimeout time.Duration // after W acks, how long to wait for the rest (default 2s)
}

// Result describes the outcome for one blob.
type Result struct {
	BlobID  string
	Size    int64
	SHA256  string
	Acked   []string          // node IDs that durably committed matching bytes
	Failed  map[string]string // node ID -> reason
	Dropped []string          // node IDs dropped for being too slow
}

// Writer is safe for concurrent use.
type Writer struct {
	client *storagenode.Client
	opts   Options
	pool   sync.Pool
}

func NewWriter(c *storagenode.Client, opts Options) *Writer {
	if opts.ChunkSize <= 0 {
		opts.ChunkSize = 256 << 10
	}
	if opts.QueueDepth <= 0 {
		opts.QueueDepth = 4
	}
	if opts.StallTimeout <= 0 {
		opts.StallTimeout = 10 * time.Second
	}
	if opts.StragglerTimeout <= 0 {
		opts.StragglerTimeout = 2 * time.Second
	}
	w := &Writer{client: c, opts: opts}
	size := opts.ChunkSize
	w.pool.New = func() any { b := make([]byte, size); return &b }
	return w
}

type chunk struct {
	buf  *[]byte
	n    int
	refs atomic.Int32
}

func (w *Writer) release(c *chunk) {
	if c.refs.Add(-1) == 0 {
		w.pool.Put(c.buf)
	}
}

type replica struct {
	t        Target
	ch       chan *chunk
	cancel   context.CancelFunc
	done     chan struct{} // closed when the PUT returns
	pumpDone chan struct{}
	info     storage.BlobInfo
	err      error
	dead     bool // producer stopped sending to it
	dropped  bool
	aborted  atomic.Bool // pump must end the body with an error, never cleanly
}

// ErrSourceRead wraps errors reading the client stream (disconnects).
var ErrSourceRead = errors.New("reading source stream")

// Write streams src (until EOF) to all targets and returns once the write
// quorum is decided. need is W.
func (w *Writer) Write(ctx context.Context, blobID string, targets []Target, need int, src io.Reader, kind string) (Result, error) {
	if need < 1 {
		need = 1
	}
	res := Result{BlobID: blobID, Failed: map[string]string{}}
	if len(targets) < need {
		return res, fmt.Errorf("%w: %d targets for write quorum %d", quorum.ErrQuorumUnreachable, len(targets), need)
	}
	finished := make(chan *replica, len(targets))
	reps := make([]*replica, len(targets))
	for i, t := range targets {
		rctx, cancel := context.WithCancel(ctx)
		r := &replica{t: t, ch: make(chan *chunk, w.opts.QueueDepth), cancel: cancel,
			done: make(chan struct{}), pumpDone: make(chan struct{})}
		reps[i] = r
		pr, pw := io.Pipe()
		go func() { // uploader: one HTTP PUT per replica
			r.info, r.err = w.client.Put(rctx, r.t.Addr, blobID, pr, -1, "", kind)
			pr.CloseWithError(errors.New("upload ended")) // unblock the pump if the PUT ended early
			close(r.done)
			finished <- r
		}()
		go func() { // pump: queue -> pipe; always drains so chunks return to the pool
			defer close(r.pumpDone)
			broken := false
			for c := range r.ch {
				if !broken {
					if _, err := pw.Write((*c.buf)[:c.n]); err != nil {
						broken = true
					}
				}
				w.release(c)
			}
			if broken || r.aborted.Load() {
				// Ending the request body with an error makes the transport
				// abort the request mid-stream, so the node can never see a
				// clean EOF and commit a truncated blob.
				pw.CloseWithError(errors.New("replica aborted"))
			} else {
				pw.Close()
			}
		}()
	}

	h := sha256.New()
	var size int64
	live := len(reps)
	kill := func(r *replica, reason string, dropped bool) {
		if r.dead {
			return
		}
		r.dead, r.dropped = true, dropped
		r.aborted.Store(true)
		r.cancel()
		live--
		if dropped {
			res.Dropped = append(res.Dropped, r.t.NodeID)
			res.Failed[r.t.NodeID] = reason
		}
	}
	finish := func() {
		for _, r := range reps {
			close(r.ch)
		}
	}
	abort := func(err error) (Result, error) {
		for _, r := range reps {
			r.aborted.Store(true)
			r.cancel()
		}
		finish()
		for _, r := range reps {
			<-r.pumpDone
			<-r.done
		}
		return res, err
	}

	var srcErr error
	for {
		if err := ctx.Err(); err != nil {
			return abort(err)
		}
		buf := w.pool.Get().(*[]byte)
		n, rerr := fill(src, (*buf)[:w.opts.ChunkSize])
		if n > 0 {
			h.Write((*buf)[:n])
			size += int64(n)
			c := &chunk{buf: buf, n: n}
			c.refs.Store(int32(live) + 1) // +1 held by the producer during fan-out
			for _, r := range reps {
				if r.dead {
					continue
				}
				if ok, reason := w.send(ctx, r, c); !ok {
					w.release(c)
					kill(r, reason, reason == "stalled")
				}
			}
			w.release(c) // producer's reference
			if live < need {
				return abort(fmt.Errorf("%w: %d live replicas, need %d", quorum.ErrQuorumUnreachable, live, need))
			}
		} else {
			w.pool.Put(buf)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			srcErr = rerr
			break
		}
	}
	if srcErr != nil {
		return abort(fmt.Errorf("%w: %w", ErrSourceRead, srcErr))
	}

	res.Size = size
	res.SHA256 = hex.EncodeToString(h.Sum(nil))
	finish()

	// Collect acknowledgements. Once W replicas have acked, stragglers get
	// StragglerTimeout more, then are cancelled (and later repaired). This
	// bounds tail latency by the W-th fastest replica, not the slowest.
	acked := 0
	pending := len(reps)
	var straggler <-chan time.Time
	armed := false
	for pending > 0 {
		select {
		case r := <-finished:
			pending--
			switch {
			case r.dropped:
			case r.err != nil:
				if _, seen := res.Failed[r.t.NodeID]; !seen {
					res.Failed[r.t.NodeID] = r.err.Error()
				}
			case r.info.SHA256 != res.SHA256 || r.info.Size != size:
				res.Failed[r.t.NodeID] = "replica stored different bytes"
			default:
				res.Acked = append(res.Acked, r.t.NodeID)
				acked++
				if acked >= need && !armed && pending > 0 {
					armed = true
					t := time.NewTimer(w.opts.StragglerTimeout)
					defer t.Stop()
					straggler = t.C
				}
			}
		case <-straggler:
			for _, r := range reps {
				select {
				case <-r.done:
				default:
					res.Failed[r.t.NodeID] = "straggler cancelled after quorum"
					r.cancel()
				}
			}
			straggler = nil
		}
	}
	for _, r := range reps {
		<-r.pumpDone
		r.cancel()
	}
	if acked < need {
		return res, fmt.Errorf("%w: %d of %d replicas acknowledged, need %d", quorum.ErrQuorumUnreachable, acked, len(reps), need)
	}
	return res, nil
}

// fill reads until p is full or the source ends. Unlike io.ReadFull it
// passes io.ErrUnexpectedEOF from the source through as an error: net/http
// reports a client that disconnects before sending Content-Length bytes as
// io.ErrUnexpectedEOF, and treating that as a normal end would commit a
// truncated object.
func fill(r io.Reader, p []byte) (int, error) {
	n := 0
	for n < len(p) {
		m, err := r.Read(p[n:])
		n += m
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// send enqueues c for r, waiting at most StallTimeout for queue space.
func (w *Writer) send(ctx context.Context, r *replica, c *chunk) (bool, string) {
	select {
	case <-r.done:
		return false, "replica request ended"
	default:
	}
	select {
	case r.ch <- c:
		return true, ""
	default:
	}
	t := time.NewTimer(w.opts.StallTimeout)
	defer t.Stop()
	select {
	case r.ch <- c:
		return true, ""
	case <-r.done:
		return false, "replica request ended"
	case <-t.C:
		return false, "stalled"
	case <-ctx.Done():
		return false, "cancelled"
	}
}

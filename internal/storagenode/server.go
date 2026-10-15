// Package storagenode exposes a storage.Engine over HTTP (the data plane),
// heartbeats to the coordinator, scrubs data in the background and supports
// fault injection.
//
// A storage node knows nothing about buckets, keys, versions or tenants. It
// stores opaque immutable blobs by ID. That ignorance is the point: the data
// plane scales by adding nodes without any of them needing metadata.
package storagenode

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/michael-emmanuel/objstore/internal/membership"
	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/ratelimit"
	"github.com/michael-emmanuel/objstore/internal/storage"
)

// Faults are injectable failure behaviours (docs/fault-injection.md).
type Faults struct {
	FailWrites   bool   `json:"fail_writes"`    // every PUT/pull fails with 503
	FailReads    bool   `json:"fail_reads"`     // every GET/HEAD fails with 503
	WriteDelayMS int    `json:"write_delay_ms"` // delay before accepting a PUT
	ReadDelayMS  int    `json:"read_delay_ms"`  // delay before serving a GET
	ChunkDelayMS int    `json:"chunk_delay_ms"` // delay per 64 KiB read from a PUT body: a slow disk/replica
	Partitioned  bool   `json:"partitioned"`    // drop data-plane connections and stop heartbeating
	Capacity     *int64 `json:"capacity_bytes,omitempty"`
}

// Config for a node.
type Config struct {
	Node             placement.Node // ID, Addr, Zone, Rack, Region, Weight
	ScrubInterval    time.Duration  // pause between full scrub passes; 0 disables
	ScrubBytesPerSec float64        // scrub bandwidth budget
	// PeerHTTPClient is used for node-to-node pulls (repair, rebalance).
	// Deployments set it to a client that adds cluster authentication;
	// nil uses DefaultHTTPClient.
	PeerHTTPClient *http.Client
}

// Server is the storage node.
type Server struct {
	cfg     Config
	engine  *storage.Engine
	peers   *Client
	log     *slog.Logger
	faults  atomic.Pointer[Faults]
	mux     *http.ServeMux
	metrics *nodeMetrics
	tracer  *observability.Tracer
}

type nodeMetrics struct {
	reg       *observability.Registry
	ops       *observability.CounterVec
	latency   *observability.HistogramVec
	bytesIn   *observability.CounterVec
	bytesOut  *observability.CounterVec
	corrupt   *observability.CounterVec
	scrubbed  *observability.CounterVec
	scrubPass *observability.GaugeVec
}

func NewServer(cfg Config, engine *storage.Engine, log *slog.Logger) *Server {
	reg := observability.NewRegistry()
	m := &nodeMetrics{
		reg:       reg,
		ops:       reg.Counter("storage_node_ops_total", "Blob operations by type and result.", "op", "result"),
		latency:   reg.Histogram("storage_node_op_seconds", "Blob operation latency.", observability.LatencyBuckets, "op"),
		bytesIn:   reg.Counter("storage_node_bytes_in_total", "Bytes written (client writes vs repair/rebalance pulls).", "kind"),
		bytesOut:  reg.Counter("storage_node_bytes_out_total", "Bytes read.", "kind"),
		corrupt:   reg.Counter("storage_node_corruptions_total", "Blobs quarantined after failing integrity checks.", "source"),
		scrubbed:  reg.Counter("storage_node_scrubbed_bytes_total", "Bytes verified by the background scrubber."),
		scrubPass: reg.Gauge("storage_node_scrub_last_pass_unixtime", "Completion time of the last full scrub pass."),
	}
	reg.GaugeFunc("storage_node_used_bytes", "Committed bytes on disk.", func() float64 { return float64(engine.Stats().UsedBytes) })
	reg.GaugeFunc("storage_node_capacity_bytes", "Capacity (configured or filesystem).", func() float64 { return float64(engine.Stats().CapacityBytes) })
	reg.GaugeFunc("storage_node_inflight_bytes", "Bytes in uncommitted writes.", func() float64 { return float64(engine.Stats().InflightBytes) })
	reg.GaugeFunc("storage_node_blobs", "Committed blob count.", func() float64 { return float64(engine.Stats().BlobCount) })
	reg.GaugeFunc("storage_node_quarantined_blobs", "Blobs awaiting repair acknowledgement.", func() float64 { return float64(engine.Stats().Quarantined) })

	s := &Server{cfg: cfg, engine: engine, peers: NewClient(cfg.PeerHTTPClient), log: log, mux: http.NewServeMux(), metrics: m,
		tracer: &observability.Tracer{Log: log}}
	s.faults.Store(&Faults{})
	engine.OnCorrupt = func(id, reason string) {
		m.corrupt.With("read_path").Inc()
		log.Error("blob quarantined", "blob", id, "reason", reason)
	}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return observability.Middleware("storage-node", s.log, s.tracer, s.mux)
}

func (s *Server) Faults() Faults { return *s.faults.Load() }

func (s *Server) SetFaults(f Faults) {
	if f.Capacity != nil {
		s.engine.SetCapacity(*f.Capacity)
	}
	s.faults.Store(&f)
	s.log.Warn("faults updated", "faults", f)
}

func (s *Server) routes() {
	s.mux.HandleFunc("PUT /blobs/{id}", s.dataPlane(s.handlePut))
	s.mux.HandleFunc("GET /blobs/{id}", s.dataPlane(s.handleGet))
	s.mux.HandleFunc("HEAD /blobs/{id}", s.dataPlane(s.handleHead))
	s.mux.HandleFunc("DELETE /blobs/{id}", s.dataPlane(s.handleDelete))
	s.mux.HandleFunc("POST /blobs/{id}/pull", s.dataPlane(s.handlePull))
	s.mux.HandleFunc("POST /blobs/{id}/verify", s.dataPlane(s.handleVerify))
	s.mux.HandleFunc("GET /blobs", s.dataPlane(s.handleList))
	s.mux.HandleFunc("GET /quarantine", s.handleQuarantine)
	s.mux.HandleFunc("POST /quarantine/ack", s.handleQuarantineAck)
	s.mux.HandleFunc("GET /stats", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, s.engine.Stats()) })
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		if s.faults.Load().Partitioned {
			hijackClose(w)
			return
		}
		w.Write([]byte("ok\n"))
	})
	s.mux.Handle("GET /metrics", s.metrics.reg.Handler())
	s.mux.HandleFunc("GET /admin/faults", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, s.Faults()) })
	s.mux.HandleFunc("POST /admin/faults", func(w http.ResponseWriter, r *http.Request) {
		var f Faults
		if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.SetFaults(f)
		writeJSON(w, http.StatusOK, f)
	})
	s.mux.HandleFunc("POST /admin/corrupt/{id}", func(w http.ResponseWriter, r *http.Request) {
		off, _ := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
		if err := s.engine.CorruptByte(r.PathValue("id"), off); err != nil {
			writeErr(w, err)
			return
		}
		s.log.Warn("blob deliberately corrupted (fault injection)", "blob", r.PathValue("id"), "offset", off)
		w.WriteHeader(http.StatusNoContent)
	})
}

// dataPlane applies the partition fault to data-plane routes.
func (s *Server) dataPlane(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.faults.Load().Partitioned {
			hijackClose(w)
			return
		}
		h(w, r)
	}
}

func hijackClose(w http.ResponseWriter) {
	if hj, ok := unwrap(w).(http.Hijacker); ok {
		if c, _, err := hj.Hijack(); err == nil {
			c.Close()
			return
		}
	}
	http.Error(w, "partitioned", http.StatusServiceUnavailable)
}

func unwrap(w http.ResponseWriter) http.ResponseWriter {
	for {
		u, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return w
		}
		w = u.Unwrap()
	}
}

func sleepCtx(ctx context.Context, ms int) error {
	if ms <= 0 {
		return nil
	}
	t := time.NewTimer(time.Duration(ms) * time.Millisecond)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// slowReader models a slow disk: each read is delayed. It is what makes a
// replica "slow" for backpressure experiments.
//
// The delay is re-read from the live fault config on every read, so clearing
// the fault immediately speeds up in-flight writes. (A server cannot observe
// a client hang-up while bytes the client already sent are still buffered in
// the kernel, so a fixed delay would keep the handler busy for a long time.)
type slowReader struct {
	ctx context.Context
	r   io.Reader
	srv *Server
}

func (s *slowReader) Read(p []byte) (int, error) {
	if len(p) > 64<<10 {
		p = p[:64<<10]
	}
	if err := sleepCtx(s.ctx, s.srv.faults.Load().ChunkDelayMS); err != nil {
		return 0, err
	}
	return s.r.Read(p)
}

var errInjected = errors.New("injected fault")

func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	f := s.faults.Load()
	if f.FailWrites {
		s.metrics.ops.With("put", "injected_failure").Inc()
		writeErr(w, errInjected)
		return
	}
	if err := sleepCtx(r.Context(), f.WriteDelayMS); err != nil {
		return
	}
	expSize := int64(-1)
	if v := r.Header.Get("X-Expected-Size"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			http.Error(w, "bad X-Expected-Size", http.StatusBadRequest)
			return
		}
		expSize = n
	}
	var body io.Reader = r.Body
	if f.ChunkDelayMS > 0 {
		body = &slowReader{ctx: r.Context(), r: body, srv: s}
	}
	kind := r.Header.Get("X-Write-Kind")
	if kind == "" {
		kind = "client"
	}
	info, err := s.engine.Put(r.Context(), r.PathValue("id"), body, expSize, strings.ToLower(r.Header.Get("X-Expected-SHA256")))
	s.metrics.latency.With("put").Observe(time.Since(start).Seconds())
	if err != nil {
		s.metrics.ops.With("put", resultLabel(err)).Inc()
		if !errors.Is(err, context.Canceled) {
			s.log.Warn("blob put failed", "blob", r.PathValue("id"), "error", err, "request_id", observability.RequestID(r.Context()))
		}
		writeErr(w, err)
		return
	}
	s.metrics.ops.With("put", "ok").Inc()
	s.metrics.bytesIn.With(kind).Add(float64(info.Size))
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	f := s.faults.Load()
	if f.FailReads {
		s.metrics.ops.With("get", "injected_failure").Inc()
		writeErr(w, errInjected)
		return
	}
	if err := sleepCtx(r.Context(), f.ReadDelayMS); err != nil {
		return
	}
	off, length, err := parseRange(r.Header.Get("Range"))
	if err != nil {
		writeErr(w, storage.ErrRange)
		return
	}
	rc, info, err := s.engine.Get(r.PathValue("id"), off, length)
	if err != nil {
		s.metrics.ops.With("get", resultLabel(err)).Inc()
		writeErr(w, err)
		return
	}
	defer rc.Close()
	n := length
	if n < 0 || off+n > info.Size {
		n = info.Size - off
	}
	w.Header().Set("X-Blob-SHA256", info.SHA256)
	w.Header().Set("X-Blob-Size", strconv.FormatInt(info.Size, 10))
	w.Header().Set("Content-Length", strconv.FormatInt(n, 10))
	status := http.StatusOK
	if r.Header.Get("Range") != "" {
		status = http.StatusPartialContent
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", off, off+n-1, info.Size))
	}
	// Read the first block before committing to a status code, so corruption
	// at the start of the range is reported as a clean error response.
	first := make([]byte, 64<<10)
	fn, ferr := io.ReadFull(rc, first[:min(int64(len(first)), n)])
	if ferr != nil && !errors.Is(ferr, io.EOF) && !errors.Is(ferr, io.ErrUnexpectedEOF) {
		s.metrics.ops.With("get", resultLabel(ferr)).Inc()
		w.Header().Del("Content-Length")
		writeErr(w, ferr)
		return
	}
	w.WriteHeader(status)
	written, err := w.Write(first[:fn])
	if err == nil {
		var rest int64
		rest, err = io.Copy(w, rc)
		written += int(rest)
	}
	kind := r.Header.Get("X-Read-Kind")
	if kind == "" {
		kind = "client"
	}
	s.metrics.bytesOut.With(kind).Add(float64(written))
	s.metrics.latency.With("get").Observe(time.Since(start).Seconds())
	if err != nil {
		// Headers are already sent. Aborting the connection (rather than
		// ending the body cleanly) guarantees the receiver sees a short body
		// and cannot mistake a truncated/corrupt stream for a complete one.
		s.metrics.ops.With("get", resultLabel(err)).Inc()
		panic(http.ErrAbortHandler)
	}
	s.metrics.ops.With("get", "ok").Inc()
}

func (s *Server) handleHead(w http.ResponseWriter, r *http.Request) {
	if s.faults.Load().FailReads {
		writeErr(w, errInjected)
		return
	}
	info, err := s.engine.Stat(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	w.Header().Set("X-Blob-SHA256", info.SHA256)
	w.Header().Set("X-Blob-Size", strconv.FormatInt(info.Size, 10))
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	err := s.engine.Delete(r.PathValue("id"))
	s.metrics.ops.With("delete", resultLabel(err)).Inc()
	if err != nil && !errors.Is(err, storage.ErrNotFound) {
		writeErr(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent) // idempotent: deleting a missing blob succeeds
}

type pullReq struct {
	From   string `json:"from"`
	SHA256 string `json:"sha256"`
	Kind   string `json:"kind"` // repair | rebalance
}

// handlePull copies a blob from a peer: node-to-node transfer, so repair
// traffic does not flow through the repairer process. The expected SHA-256
// comes from metadata, so a corrupt source cannot produce a "repaired" copy.
func (s *Server) handlePull(w http.ResponseWriter, r *http.Request) {
	if s.faults.Load().FailWrites {
		writeErr(w, errInjected)
		return
	}
	var req pullReq
	if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil || req.From == "" || len(req.SHA256) != 64 {
		http.Error(w, "pull requires from and sha256", http.StatusBadRequest)
		return
	}
	id := r.PathValue("id")
	if info, err := s.engine.Stat(id); err == nil && info.SHA256 == req.SHA256 {
		writeJSON(w, http.StatusOK, info) // already present
		return
	}
	kind := req.Kind
	if kind == "" {
		kind = "repair"
	}
	rc, err := s.peers.Get(r.Context(), req.From, id, 0, -1, kind)
	if err != nil {
		writeErr(w, fmt.Errorf("pull source: %w", err))
		return
	}
	defer rc.Close()
	info, err := s.engine.Put(r.Context(), id, rc, -1, req.SHA256)
	if err != nil {
		s.metrics.ops.With("pull", resultLabel(err)).Inc()
		writeErr(w, err)
		return
	}
	s.metrics.ops.With("pull", "ok").Inc()
	s.metrics.bytesIn.With(kind).Add(float64(info.Size))
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleVerify(w http.ResponseWriter, r *http.Request) {
	info, err := s.engine.Verify(r.PathValue("id"))
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleList(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 10000 {
		limit = 1000
	}
	blobs, err := s.engine.List(r.URL.Query().Get("after"), limit)
	if err != nil {
		writeErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, blobs)
}

func (s *Server) handleQuarantine(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.engine.Quarantined())
}

func (s *Server) handleQuarantineAck(w http.ResponseWriter, r *http.Request) {
	var ids []string
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&ids); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.engine.AckQuarantine(ids)
	w.WriteHeader(http.StatusNoContent)
}

// RunHeartbeat reports liveness and usage to the coordinator.
func (s *Server) RunHeartbeat(ctx context.Context, c *membership.Client, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	failures := 0
	for {
		if !s.faults.Load().Partitioned {
			st := s.engine.Stats()
			n := s.cfg.Node
			n.CapacityBytes, n.UsedBytes, n.BlobCount = st.CapacityBytes, st.UsedBytes, st.BlobCount
			hctx, cancel := context.WithTimeout(ctx, every*2)
			_, err := c.Heartbeat(hctx, n)
			cancel()
			if err != nil {
				if failures%10 == 0 {
					s.log.Warn("heartbeat failed", "error", err)
				}
				failures++
			} else {
				failures = 0
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// RunScrubber verifies every blob's block CRCs and SHA-256 at a bounded
// rate, quarantining corrupt blobs so the repairer can replace them. This
// is the defence against silent corruption of cold data that no client
// reads.
func (s *Server) RunScrubber(ctx context.Context) {
	if s.cfg.ScrubInterval <= 0 {
		return
	}
	limiter := ratelimit.NewBucket(s.cfg.ScrubBytesPerSec, s.cfg.ScrubBytesPerSec)
	for {
		after := ""
		for {
			page, err := s.engine.List(after, 500)
			if err != nil || len(page) == 0 {
				break
			}
			for _, b := range page {
				if err := limiter.Wait(ctx, float64(b.Size)); err != nil {
					return
				}
				if _, err := s.engine.Verify(b.ID); err != nil && errors.Is(err, storage.ErrCorrupt) {
					s.metrics.corrupt.With("scrubber").Inc()
				}
				s.metrics.scrubbed.With().Add(float64(b.Size))
			}
			after = page[len(page)-1].ID
		}
		s.metrics.scrubPass.With().Set(float64(time.Now().Unix()))
		if err := sleepCtx(ctx, int(s.cfg.ScrubInterval/time.Millisecond)); err != nil {
			return
		}
	}
}

// parseRange accepts "", "bytes=a-b", "bytes=a-" (internal callers only).
func parseRange(h string) (int64, int64, error) {
	if h == "" {
		return 0, -1, nil
	}
	spec, ok := strings.CutPrefix(h, "bytes=")
	if !ok {
		return 0, 0, errors.New("bad range")
	}
	a, b, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, errors.New("bad range")
	}
	start, err := strconv.ParseInt(a, 10, 64)
	if err != nil || start < 0 {
		return 0, 0, errors.New("bad range")
	}
	if b == "" {
		return start, -1, nil
	}
	end, err := strconv.ParseInt(b, 10, 64)
	if err != nil || end < start {
		return 0, 0, errors.New("bad range")
	}
	return start, end - start + 1, nil
}

func resultLabel(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, storage.ErrNotFound):
		return "not_found"
	case errors.Is(err, storage.ErrDiskFull):
		return "disk_full"
	case errors.Is(err, storage.ErrCorrupt):
		return "corrupt"
	case errors.Is(err, storage.ErrChecksumMismatch), errors.Is(err, storage.ErrSizeMismatch):
		return "integrity"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return "cancelled"
	default:
		return "error"
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, err error) {
	code := http.StatusInternalServerError
	switch {
	case errors.Is(err, storage.ErrNotFound):
		code = http.StatusNotFound
	case errors.Is(err, storage.ErrConflict):
		code = http.StatusConflict
	case errors.Is(err, storage.ErrBusy), errors.Is(err, errInjected):
		code = http.StatusServiceUnavailable
	case errors.Is(err, storage.ErrDiskFull):
		code = http.StatusInsufficientStorage
	case errors.Is(err, storage.ErrChecksumMismatch), errors.Is(err, storage.ErrSizeMismatch):
		code = http.StatusUnprocessableEntity
	case errors.Is(err, storage.ErrCorrupt):
		code = http.StatusInternalServerError
	case errors.Is(err, storage.ErrInvalidID):
		code = http.StatusBadRequest
	case errors.Is(err, storage.ErrRange):
		code = http.StatusRequestedRangeNotSatisfiable
	case errors.Is(err, ErrUnreachable):
		code = http.StatusBadGateway
	}
	http.Error(w, err.Error(), code)
}

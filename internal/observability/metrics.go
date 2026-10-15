// Package observability provides metrics (Prometheus text exposition),
// structured logging helpers, request IDs and lightweight tracing.
//
// It is intentionally dependency-free so the whole repository builds with
// the standard library only. The metric types and wire format are
// compatible with Prometheus scraping; replacing this package with the
// official client_golang and OpenTelemetry SDKs is listed under
// "What Would I Replace?" in docs/tradeoffs.md.
package observability

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// Registry holds metric families and renders them.
type Registry struct {
	mu       sync.Mutex
	families []family
	names    map[string]bool
}

type family interface {
	write(w io.Writer)
	name() string
}

func NewRegistry() *Registry { return &Registry{names: map[string]bool{}} }

func (r *Registry) register(f family) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.names[f.name()] {
		panic("duplicate metric " + f.name())
	}
	r.names[f.name()] = true
	r.families = append(r.families, f)
}

// Handler serves the Prometheus text format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		r.Write(w)
	})
}

// Write renders every family.
func (r *Registry) Write(w io.Writer) {
	r.mu.Lock()
	fs := append([]family(nil), r.families...)
	r.mu.Unlock()
	sort.Slice(fs, func(i, j int) bool { return fs[i].name() < fs[j].name() })
	for _, f := range fs {
		f.write(w)
	}
}

func labelString(names, values []string, extra ...string) string {
	if len(names) == 0 && len(extra) == 0 {
		return ""
	}
	var parts []string
	for i, n := range names {
		parts = append(parts, fmt.Sprintf("%s=%q", n, values[i]))
	}
	for i := 0; i+1 < len(extra); i += 2 {
		parts = append(parts, fmt.Sprintf("%s=%q", extra[i], extra[i+1]))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

type series[T any] struct {
	mu     sync.RWMutex
	labels []string
	m      map[string]*T
	vals   map[string][]string
	mk     func() *T
}

func (s *series[T]) get(values []string) *T {
	if len(values) != len(s.labels) {
		panic(fmt.Sprintf("expected %d label values, got %d", len(s.labels), len(values)))
	}
	key := strings.Join(values, "\xff")
	s.mu.RLock()
	v, ok := s.m[key]
	s.mu.RUnlock()
	if ok {
		return v
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if v, ok = s.m[key]; ok {
		return v
	}
	v = s.mk()
	s.m[key] = v
	s.vals[key] = append([]string(nil), values...)
	return v
}

func (s *series[T]) each(fn func(values []string, v *T)) {
	s.mu.RLock()
	keys := make([]string, 0, len(s.m))
	for k := range s.m {
		keys = append(keys, k)
	}
	s.mu.RUnlock()
	sort.Strings(keys)
	for _, k := range keys {
		s.mu.RLock()
		v, vals := s.m[k], s.vals[k]
		s.mu.RUnlock()
		fn(vals, v)
	}
}

// ---------------------------------------------------------------------------

// Counter is a monotonically increasing value.
type Counter struct{ bits atomic.Uint64 }

func (c *Counter) Add(v float64) {
	for {
		old := c.bits.Load()
		if c.bits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			return
		}
	}
}
func (c *Counter) Inc()           { c.Add(1) }
func (c *Counter) Value() float64 { return math.Float64frombits(c.bits.Load()) }

type CounterVec struct {
	n, help string
	s       series[Counter]
}

func (r *Registry) Counter(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{n: name, help: help, s: series[Counter]{labels: labels, m: map[string]*Counter{}, vals: map[string][]string{}, mk: func() *Counter { return &Counter{} }}}
	r.register(c)
	return c
}
func (c *CounterVec) With(values ...string) *Counter { return c.s.get(values) }
func (c *CounterVec) name() string                   { return c.n }
func (c *CounterVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n", c.n, c.help, c.n)
	c.s.each(func(vals []string, v *Counter) {
		fmt.Fprintf(w, "%s%s %v\n", c.n, labelString(c.s.labels, vals), v.Value())
	})
}

// Gauge can go up and down.
type Gauge struct{ bits atomic.Uint64 }

func (g *Gauge) Set(v float64) { g.bits.Store(math.Float64bits(v)) }
func (g *Gauge) Add(v float64) {
	for {
		old := g.bits.Load()
		if g.bits.CompareAndSwap(old, math.Float64bits(math.Float64frombits(old)+v)) {
			return
		}
	}
}
func (g *Gauge) Value() float64 { return math.Float64frombits(g.bits.Load()) }

type GaugeVec struct {
	n, help string
	s       series[Gauge]
}

func (r *Registry) Gauge(name, help string, labels ...string) *GaugeVec {
	g := &GaugeVec{n: name, help: help, s: series[Gauge]{labels: labels, m: map[string]*Gauge{}, vals: map[string][]string{}, mk: func() *Gauge { return &Gauge{} }}}
	r.register(g)
	return g
}
func (g *GaugeVec) With(values ...string) *Gauge { return g.s.get(values) }
func (g *GaugeVec) name() string                 { return g.n }
func (g *GaugeVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", g.n, g.help, g.n)
	g.s.each(func(vals []string, v *Gauge) {
		fmt.Fprintf(w, "%s%s %v\n", g.n, labelString(g.s.labels, vals), v.Value())
	})
}

// GaugeFunc samples a value at scrape time (e.g. cache size, disk usage).
type gaugeFunc struct {
	n, help string
	fn      func() float64
}

func (r *Registry) GaugeFunc(name, help string, fn func() float64) {
	r.register(&gaugeFunc{n: name, help: help, fn: fn})
}
func (g *gaugeFunc) name() string { return g.n }
func (g *gaugeFunc) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n%s %v\n", g.n, g.help, g.n, g.n, g.fn())
}

// CounterFunc samples a monotonic value at scrape time.
type counterFunc struct {
	n, help string
	fn      func() float64
}

func (r *Registry) CounterFunc(name, help string, fn func() float64) {
	r.register(&counterFunc{n: name, help: help, fn: fn})
}
func (c *counterFunc) name() string { return c.n }
func (c *counterFunc) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s counter\n%s %v\n", c.n, c.help, c.n, c.n, c.fn())
}

// Histogram tracks a distribution in fixed buckets. Percentiles (p50/p95/p99)
// are computed from buckets, either by Prometheus histogram_quantile() or by
// Quantile() below for the JSON admin endpoints.
type Histogram struct {
	mu      sync.Mutex
	bounds  []float64
	counts  []uint64
	sum     float64
	samples uint64
}

func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.bounds, v)
	h.mu.Lock()
	h.counts[i]++
	h.sum += v
	h.samples++
	h.mu.Unlock()
}

// Quantile estimates the q-quantile by linear interpolation within a bucket.
func (h *Histogram) Quantile(q float64) float64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.samples == 0 {
		return math.NaN()
	}
	rank := q * float64(h.samples)
	var cum uint64
	for i, c := range h.counts {
		if float64(cum+c) >= rank {
			lo := 0.0
			if i > 0 {
				lo = h.bounds[i-1]
			}
			if i >= len(h.bounds) {
				return lo // +Inf bucket: best we can say is "at least lo"
			}
			if c == 0 {
				return h.bounds[i]
			}
			return lo + (h.bounds[i]-lo)*(rank-float64(cum))/float64(c)
		}
		cum += c
	}
	return h.bounds[len(h.bounds)-1]
}

func (h *Histogram) Count() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.samples
}

type HistogramVec struct {
	n, help string
	s       series[Histogram]
}

// LatencyBuckets covers 0.5ms .. 60s, roughly x2 per bucket.
var LatencyBuckets = []float64{.0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}

// SizeBuckets covers 1 KiB .. 16 GiB.
var SizeBuckets = []float64{1 << 10, 16 << 10, 256 << 10, 1 << 20, 16 << 20, 64 << 20, 256 << 20, 1 << 30, 4 << 30, 16 << 30}

func (r *Registry) Histogram(name, help string, buckets []float64, labels ...string) *HistogramVec {
	b := append([]float64(nil), buckets...)
	sort.Float64s(b)
	h := &HistogramVec{n: name, help: help, s: series[Histogram]{labels: labels, m: map[string]*Histogram{}, vals: map[string][]string{},
		mk: func() *Histogram { return &Histogram{bounds: b, counts: make([]uint64, len(b)+1)} }}}
	r.register(h)
	return h
}
func (h *HistogramVec) With(values ...string) *Histogram { return h.s.get(values) }
func (h *HistogramVec) name() string                     { return h.n }
func (h *HistogramVec) Each(fn func(labels []string, h *Histogram)) {
	h.s.each(fn)
}
func (h *HistogramVec) write(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s histogram\n", h.n, h.help, h.n)
	h.s.each(func(vals []string, x *Histogram) {
		x.mu.Lock()
		var cum uint64
		for i, b := range x.bounds {
			cum += x.counts[i]
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.n, labelString(h.s.labels, vals, "le", fmt.Sprint(b)), cum)
		}
		cum += x.counts[len(x.bounds)]
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.n, labelString(h.s.labels, vals, "le", "+Inf"), cum)
		fmt.Fprintf(w, "%s_sum%s %v\n%s_count%s %d\n", h.n, labelString(h.s.labels, vals), x.sum, h.n, labelString(h.s.labels, vals), x.samples)
		x.mu.Unlock()
	})
}

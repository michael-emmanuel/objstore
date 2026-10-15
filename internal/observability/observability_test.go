package observability

import (
	"bytes"
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegistryExposition(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("requests_total", "Requests.", "op", "code")
	c.With("GET", "200").Add(3)
	g := r.Gauge("inflight", "In flight.")
	g.With().Set(2)
	h := r.Histogram("latency_seconds", "Latency.", []float64{0.1, 1}, "op")
	h.With("GET").Observe(0.05)
	h.With("GET").Observe(0.5)
	h.With("GET").Observe(5)
	r.GaugeFunc("cache_bytes", "Cache.", func() float64 { return 7 })
	var b bytes.Buffer
	r.Write(&b)
	out := b.String()
	for _, want := range []string{
		`requests_total{op="GET",code="200"} 3`,
		`inflight 2`,
		`latency_seconds_bucket{op="GET",le="0.1"} 1`,
		`latency_seconds_bucket{op="GET",le="1"} 2`,
		`latency_seconds_bucket{op="GET",le="+Inf"} 3`,
		`latency_seconds_count{op="GET"} 3`,
		`cache_bytes 7`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestQuantile(t *testing.T) {
	r := NewRegistry()
	h := r.Histogram("x", "x", []float64{1, 2, 3, 4}).With()
	if !math.IsNaN(h.Quantile(0.5)) {
		t.Fatal("empty histogram should be NaN")
	}
	for i := 0; i < 100; i++ {
		h.Observe(float64(i%4) + 0.5)
	}
	if q := h.Quantile(0.5); q < 1.5 || q > 2.5 {
		t.Fatalf("p50 %v", q)
	}
}

func TestTracePropagation(t *testing.T) {
	tr := &Tracer{Log: DiscardLogger()}
	ctx, parent := tr.Start(context.Background(), "root")
	ctx = WithRequestID(ctx, "rid-1")
	h := http.Header{}
	Inject(ctx, h)
	remote := Extract(context.Background(), h)
	_, child := tr.Start(remote, "child")
	if child.TraceID != parent.TraceID || child.ParentID != parent.SpanID {
		t.Fatalf("trace not propagated: %+v vs %+v", child, parent)
	}
	if RequestID(remote) != "rid-1" {
		t.Fatal("request id not propagated")
	}
	var nilTracer *Tracer
	_, s := nilTracer.Start(context.Background(), "x")
	s.End(nil) // must not panic
}

func TestMiddlewareSetsRequestID(t *testing.T) {
	h := Middleware("test", DiscardLogger(), nil, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if RequestID(r.Context()) == "" {
			t.Error("no request id in context")
		}
		w.WriteHeader(204)
	}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "/", nil))
	if rr.Header().Get("X-Request-Id") == "" || rr.Code != 204 {
		t.Fatal("missing request id header")
	}
}

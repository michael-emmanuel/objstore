package observability

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"time"
)

// NewLogger returns a JSON structured logger. level: debug|info|warn|error.
func NewLogger(service, level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	default:
		l = slog.LevelInfo
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: l})).With("service", service)
}

// DiscardLogger is used in tests.
func DiscardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type ctxKey int

const (
	spanKey ctxKey = iota
	requestIDKey
)

// Span is a minimal trace span compatible with W3C traceparent propagation.
// Ending a span emits one structured log record; a log pipeline (or an
// OpenTelemetry collector reading logs) can reassemble traces by trace_id.
type Span struct {
	TraceID  string
	SpanID   string
	ParentID string
	Name     string
	start    time.Time
	attrs    []any
	log      *slog.Logger
}

// Tracer creates spans. A nil *Tracer is valid and produces no output.
type Tracer struct {
	Log *slog.Logger
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Start begins a child of the span in ctx, or a new trace.
func (t *Tracer) Start(ctx context.Context, name string, attrs ...any) (context.Context, *Span) {
	s := &Span{Name: name, SpanID: randHex(8), start: time.Now(), attrs: attrs}
	if t != nil {
		s.log = t.Log
	}
	if p, ok := ctx.Value(spanKey).(*Span); ok {
		s.TraceID, s.ParentID = p.TraceID, p.SpanID
	} else {
		s.TraceID = randHex(16)
	}
	return context.WithValue(ctx, spanKey, s), s
}

// SetAttr adds attributes recorded at End.
func (s *Span) SetAttr(attrs ...any) {
	if s != nil {
		s.attrs = append(s.attrs, attrs...)
	}
}

// End records the span. Spans are logged at debug level so tracing can be
// enabled per process with -log-level=debug without code changes.
func (s *Span) End(err error) {
	if s == nil || s.log == nil {
		return
	}
	args := []any{"trace_id", s.TraceID, "span_id", s.SpanID, "parent_id", s.ParentID,
		"span", s.Name, "duration_ms", float64(time.Since(s.start).Microseconds()) / 1000}
	args = append(args, s.attrs...)
	if err != nil {
		args = append(args, "error", err.Error())
	}
	s.log.Debug("span", args...)
}

// SpanFrom returns the active span, if any.
func SpanFrom(ctx context.Context) *Span {
	s, _ := ctx.Value(spanKey).(*Span)
	return s
}

// Inject writes a W3C traceparent header for the active span.
func Inject(ctx context.Context, h http.Header) {
	if s := SpanFrom(ctx); s != nil {
		h.Set("traceparent", "00-"+s.TraceID+"-"+s.SpanID+"-01")
	}
	if id := RequestID(ctx); id != "" {
		h.Set("X-Request-Id", id)
	}
}

// Extract returns a context whose active span is the remote parent
// described by the traceparent header (if valid).
func Extract(ctx context.Context, h http.Header) context.Context {
	if id := h.Get("X-Request-Id"); id != "" && len(id) <= 128 {
		ctx = context.WithValue(ctx, requestIDKey, id)
	}
	parts := strings.Split(h.Get("traceparent"), "-")
	if len(parts) != 4 || len(parts[1]) != 32 || len(parts[2]) != 16 {
		return ctx
	}
	return context.WithValue(ctx, spanKey, &Span{TraceID: parts[1], SpanID: parts[2]})
}

// RequestID returns the request ID carried by ctx.
func RequestID(ctx context.Context) string {
	s, _ := ctx.Value(requestIDKey).(string)
	return s
}

// WithRequestID attaches a request ID.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// StatusRecorder captures status and bytes written for access logs/metrics
// while still exposing Flush and the underlying writer for
// http.ResponseController (write deadlines).
type StatusRecorder struct {
	http.ResponseWriter
	Status int
	Bytes  int64
}

func (s *StatusRecorder) WriteHeader(code int) {
	if s.Status == 0 {
		s.Status = code
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *StatusRecorder) Write(b []byte) (int, error) {
	if s.Status == 0 {
		s.Status = http.StatusOK
	}
	n, err := s.ResponseWriter.Write(b)
	s.Bytes += int64(n)
	return n, err
}

func (s *StatusRecorder) Flush() {
	if f, ok := s.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (s *StatusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

// Middleware assigns request IDs, extracts trace context, starts a server
// span and writes one access-log line per request.
func Middleware(service string, log *slog.Logger, tracer *Tracer, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := Extract(r.Context(), r.Header)
		rid := RequestID(ctx)
		if rid == "" {
			rid = randHex(8)
			ctx = WithRequestID(ctx, rid)
		}
		ctx, span := tracer.Start(ctx, service+" "+r.Method, "path", r.URL.Path)
		w.Header().Set("X-Request-Id", rid)
		rec := &StatusRecorder{ResponseWriter: w}
		start := time.Now()
		next.ServeHTTP(rec, r.WithContext(ctx))
		if rec.Status == 0 {
			rec.Status = http.StatusOK
		}
		span.SetAttr("status", rec.Status)
		span.End(nil)
		lvl := slog.LevelDebug // data-plane internals are noisy; gateway logs at info
		if service == "gateway" || rec.Status >= 500 {
			lvl = slog.LevelInfo
		}
		log.Log(ctx, lvl, "request", "request_id", rid, "trace_id", span.TraceID, "method", r.Method,
			"path", r.URL.Path, "status", rec.Status, "bytes_out", rec.Bytes,
			"duration_ms", float64(time.Since(start).Microseconds())/1000, "remote", r.RemoteAddr)
	})
}

// Package api is the gateway: the public HTTP API, authentication and
// tenant limits, and the coordination of the metadata plane and data plane
// for every request.
//
// The gateway is stateless (apart from caches). Any number of gateways can
// run behind a load balancer; they share nothing but the metadata service
// and the storage nodes.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/michael-emmanuel/objstore/internal/cache"
	"github.com/michael-emmanuel/objstore/internal/membership"
	"github.com/michael-emmanuel/objstore/internal/metadata"
	"github.com/michael-emmanuel/objstore/internal/object"
	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/quorum"
	"github.com/michael-emmanuel/objstore/internal/quota"
	"github.com/michael-emmanuel/objstore/internal/ratelimit"
	"github.com/michael-emmanuel/objstore/internal/replication"
	"github.com/michael-emmanuel/objstore/internal/storagenode"
)

// MetaStore is the metadata plane as seen by the gateway.
type MetaStore interface {
	metadata.Store
	ShardFor(bucket string) int
}

// Config holds gateway tunables. Zero values get sensible defaults.
type Config struct {
	Zone string // the gateway's own zone, for zone-local reads

	WriteQuorum int // W; 0 = majority of the object's RF
	ReadQuorum  int // R used by ?consistency=quorum; 0 = majority

	SegmentSize      int64 // simple PUTs are split into segments of this size (default 64 MiB)
	ChunkSize        int
	QueueDepth       int
	StallTimeout     time.Duration // drop replicas that stall this long
	SlowNodePenalty  time.Duration // avoid placing on a node for this long after it stalled (default 30s; <0 disables)
	StragglerTimeout time.Duration
	PendingTTL       time.Duration // GC protection for uncommitted blobs

	MetaCacheTTL         time.Duration // 0 disables the metadata cache
	MetaCacheEntries     int64
	ObjectCacheBytes     int64 // 0 disables the hot-object cache
	ObjectCacheMaxObject int64

	MaxInflightUploads int           // global load shedding; 0 = unlimited
	ClientIdleTimeout  time.Duration // abort transfers whose client makes no progress
	MetadataTimeout    time.Duration

	MinPartSize int64
	MaxPartSize int64
}

func (c *Config) defaults() {
	if c.SegmentSize <= 0 {
		c.SegmentSize = 64 << 20
	}
	if c.SlowNodePenalty == 0 {
		c.SlowNodePenalty = 30 * time.Second
	}
	if c.PendingTTL <= 0 {
		c.PendingTTL = 6 * time.Hour
	}
	if c.MetaCacheEntries <= 0 {
		c.MetaCacheEntries = 100_000
	}
	if c.ObjectCacheMaxObject <= 0 {
		c.ObjectCacheMaxObject = 1 << 20
	}
	if c.ClientIdleTimeout <= 0 {
		c.ClientIdleTimeout = 60 * time.Second
	}
	if c.MetadataTimeout <= 0 {
		c.MetadataTimeout = 5 * time.Second
	}
	if c.MinPartSize <= 0 {
		c.MinPartSize = 5 << 20
	}
	if c.MaxPartSize <= 0 {
		c.MaxPartSize = 5 << 30
	}
}

// Gateway serves the object API.
type Gateway struct {
	cfg     Config
	meta    MetaStore
	members membership.Source
	coord   *membership.Client // may be nil (admin node-state changes unavailable)
	nodes   *storagenode.Client
	writer  *replication.Writer
	tenants *quota.Manager
	log     *slog.Logger
	tracer  *observability.Tracer

	metaCache   *cache.LRU[string, object.Version]
	bucketCache *cache.LRU[string, metadata.Bucket]
	objCache    *cache.LRU[string, []byte]
	objFlight   cache.Group[string, []byte]
	uploadSlots *ratelimit.Semaphore
	slow        *slowNodes

	m   *metrics
	mux *http.ServeMux

	routePut, routeGet, routeHead, routeDelete, routeVersions http.HandlerFunc
}

// Deps are the gateway's collaborators.
type Deps struct {
	Meta    MetaStore
	Members membership.Source
	Coord   *membership.Client
	Nodes   *storagenode.Client
	Tenants *quota.Manager
	Log     *slog.Logger
}

func New(cfg Config, d Deps) *Gateway {
	cfg.defaults()
	g := &Gateway{
		cfg: cfg, meta: d.Meta, members: d.Members, coord: d.Coord, nodes: d.Nodes, tenants: d.Tenants, log: d.Log,
		tracer: &observability.Tracer{Log: d.Log},
		writer: replication.NewWriter(d.Nodes, replication.Options{ChunkSize: cfg.ChunkSize, QueueDepth: cfg.QueueDepth,
			StallTimeout: cfg.StallTimeout, StragglerTimeout: cfg.StragglerTimeout}),
		uploadSlots: ratelimit.NewSemaphore(cfg.MaxInflightUploads),
		slow:        newSlowNodes(cfg.SlowNodePenalty),
		mux:         http.NewServeMux(),
	}
	if cfg.MetaCacheTTL > 0 {
		g.metaCache = cache.NewLRU[string, object.Version](cfg.MetaCacheEntries, cfg.MetaCacheTTL)
		g.bucketCache = cache.NewLRU[string, metadata.Bucket](10_000, cfg.MetaCacheTTL*5)
	}
	if cfg.ObjectCacheBytes > 0 {
		g.objCache = cache.NewLRU[string, []byte](cfg.ObjectCacheBytes, 0)
	}
	g.m = newMetrics(g)
	g.routes()
	return g
}

// Handler returns the root handler with request IDs, tracing and access logs.
//
// Object routes are dispatched by hand from the ESCAPED path rather than by
// http.ServeMux, because ServeMux cleans paths: a key such as "a/../b" or
// "a//b" would be redirected or rewritten. Object keys are opaque strings,
// not filesystem paths, so they must reach the handler byte-for-byte.
func (g *Gateway) Handler() http.Handler {
	return observability.Middleware("gateway", g.log, g.tracer, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if bucket, kind, key, ok := splitObjectPath(r.URL.EscapedPath()); ok {
			r.SetPathValue("bucket", bucket)
			r.SetPathValue("key", key)
			if h := g.objectRoute(kind, r.Method); h != nil {
				h(w, r)
				return
			}
			w.Header().Set("Allow", "GET, HEAD, PUT, DELETE")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		g.mux.ServeHTTP(w, r)
	}))
}

// splitObjectPath parses /v1/buckets/{bucket}/{objects|versions}/{key...}.
func splitObjectPath(p string) (bucket, kind, key string, ok bool) {
	rest, found := strings.CutPrefix(p, "/v1/buckets/")
	if !found {
		return
	}
	bucket, rest, found = strings.Cut(rest, "/")
	if !found {
		return
	}
	kind, rawKey, found := strings.Cut(rest, "/")
	if !found || rawKey == "" || (kind != "objects" && kind != "versions") {
		return
	}
	k, err := url.PathUnescape(rawKey)
	if err != nil {
		return
	}
	return bucket, kind, k, true
}

func (g *Gateway) objectRoute(kind, method string) http.HandlerFunc {
	if kind == "versions" {
		if method == http.MethodGet {
			return g.routeVersions
		}
		return nil
	}
	switch method {
	case http.MethodPut:
		return g.routePut
	case http.MethodGet:
		return g.routeGet
	case http.MethodHead:
		return g.routeHead
	case http.MethodDelete:
		return g.routeDelete
	}
	return nil
}

func (g *Gateway) routes() {
	h := g.mux.HandleFunc
	h("PUT /v1/buckets/{bucket}", g.instrument("create_bucket", g.handleCreateBucket))
	h("GET /v1/buckets/{bucket}", g.instrument("get_bucket", g.handleGetBucket))
	// Object routes ({bucket}/objects/{key...}) are dispatched in Handler.
	g.routePut = g.instrument("put", g.handlePut)
	g.routeGet = g.instrument("get", g.handleGet)
	g.routeHead = g.instrument("head", g.handleGet)
	g.routeDelete = g.instrument("delete", g.handleDelete)
	g.routeVersions = g.instrument("list_versions", g.handleListVersions)
	h("GET /v1/buckets/{bucket}/objects", g.instrument("list", g.handleList))
	h("POST /v1/buckets/{bucket}/multipart", g.instrument("mpu_create", g.handleCreateUpload))
	h("POST /v1/multipart/{upload_id}/parts", g.instrument("mpu_part", g.handleUploadPart))
	h("GET /v1/multipart/{upload_id}", g.instrument("mpu_get", g.handleGetUpload))
	h("POST /v1/multipart/{upload_id}/complete", g.instrument("mpu_complete", g.handleCompleteUpload))
	h("DELETE /v1/multipart/{upload_id}", g.instrument("mpu_abort", g.handleAbortUpload))

	h("GET /admin/cluster", g.admin(g.handleCluster))
	h("POST /admin/nodes/{id}/state", g.admin(g.handleNodeState))
	h("GET /admin/nodes/{id}/drain", g.admin(g.handleDrainStatus))
	h("GET /admin/objects/{bucket}/{key...}", g.admin(g.handleInspect))
	h("GET /admin/stats", g.admin(g.handleStats))

	g.mux.Handle("GET /metrics", g.m.reg.Handler())
	h("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
}

// ---------------------------------------------------------------------------
// Errors

var (
	errUnauthenticated = errors.New("missing or invalid credentials")
	errAccessDenied    = errors.New("access denied")
	errBadDigest       = errors.New("content checksum does not match X-Checksum-SHA256")
	errBadRequest      = errors.New("bad request")
	errNoReplicas      = errors.New("no replica could serve the data")
	errOverloaded      = errors.New("gateway overloaded, retry later")
)

type apiErr struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

func classify(err error) (int, string) {
	var mbe *http.MaxBytesError
	switch {
	case errors.Is(err, errUnauthenticated):
		return http.StatusUnauthorized, "Unauthenticated"
	case errors.Is(err, errAccessDenied):
		return http.StatusForbidden, "AccessDenied"
	case errors.Is(err, metadata.ErrNoSuchBucket):
		return http.StatusNotFound, "NoSuchBucket"
	case errors.Is(err, metadata.ErrNoSuchUpload):
		return http.StatusNotFound, "NoSuchUpload"
	case errors.Is(err, metadata.ErrNotFound):
		return http.StatusNotFound, "NoSuchKey"
	case errors.Is(err, metadata.ErrBucketExists):
		return http.StatusConflict, "BucketAlreadyExists"
	case errors.Is(err, metadata.ErrInvalidPart):
		return http.StatusBadRequest, "InvalidPart"
	case errors.Is(err, errBadDigest):
		return http.StatusBadRequest, "BadDigest"
	case errors.Is(err, metadata.ErrInvalidArgument), errors.Is(err, object.ErrInvalidBucket),
		errors.Is(err, object.ErrInvalidKey), errors.Is(err, object.ErrInvalidMetadata), errors.Is(err, errBadRequest):
		return http.StatusBadRequest, "InvalidArgument"
	case errors.Is(err, metadata.ErrQuotaExceeded), errors.Is(err, quota.ErrQuotaExceeded):
		return http.StatusForbidden, "QuotaExceeded"
	case errors.Is(err, quota.ErrObjectTooLarge), errors.As(err, &mbe):
		return http.StatusRequestEntityTooLarge, "EntityTooLarge"
	case errors.Is(err, quota.ErrRateLimited), errors.Is(err, quota.ErrTooManyUploads), errors.Is(err, quota.ErrTooManyReads):
		return http.StatusTooManyRequests, "SlowDown"
	case errors.Is(err, errOverloaded):
		return http.StatusServiceUnavailable, "SlowDown"
	case errors.Is(err, metadata.ErrUnavailable), errors.Is(err, metadata.ErrReadOnly):
		return http.StatusServiceUnavailable, "MetadataUnavailable"
	case errors.Is(err, quorum.ErrQuorumUnreachable), errors.Is(err, placement.ErrInsufficientNodes):
		return http.StatusServiceUnavailable, "InsufficientReplicas"
	case errors.Is(err, errNoReplicas):
		return http.StatusServiceUnavailable, "DataUnavailable"
	case errors.Is(err, replication.ErrSourceRead):
		return http.StatusBadRequest, "IncompleteBody"
	case errors.Is(err, context.Canceled):
		return 499, "ClientClosedRequest"
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout, "Timeout"
	default:
		return http.StatusInternalServerError, "InternalError"
	}
}

func (g *Gateway) fail(w http.ResponseWriter, r *http.Request, err error) {
	status, code := classify(err)
	if status >= 500 && status != http.StatusServiceUnavailable {
		g.log.Error("request failed", "error", err, "request_id", observability.RequestID(r.Context()), "path", r.URL.Path)
	} else if status == http.StatusServiceUnavailable {
		g.log.Warn("request unavailable", "error", err, "request_id", observability.RequestID(r.Context()))
	}
	if status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable {
		w.Header().Set("Retry-After", "1")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if r.Method != http.MethodHead {
		json.NewEncoder(w).Encode(map[string]apiErr{"error": {Code: code, Message: err.Error(), RequestID: observability.RequestID(r.Context())}})
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

// ---------------------------------------------------------------------------
// Auth

type principal struct {
	tenant    *quota.Tenant // nil for anonymous
	anonymous bool
}

func (g *Gateway) authenticate(r *http.Request) (principal, error) {
	h := r.Header.Get("Authorization")
	if h == "" {
		return principal{anonymous: true}, nil
	}
	tok, ok := strings.CutPrefix(h, "Bearer ")
	if !ok || tok == "" {
		return principal{}, errUnauthenticated
	}
	// Tokens are compared by SHA-256 digest via map lookup; the digest, not
	// the secret, is what the process holds in memory and config.
	t, ok := g.tenants.ByTokenHash(hashToken(tok))
	if !ok {
		return principal{}, errUnauthenticated
	}
	return principal{tenant: t}, nil
}

// authorize checks the principal against bucket ownership/ACL. Anonymous
// principals may read public-read buckets only.
func (g *Gateway) authorize(p principal, b metadata.Bucket, write bool) error {
	if p.anonymous {
		if b.PublicRead && !write {
			return nil
		}
		return errUnauthenticated
	}
	if p.tenant.ID == b.Tenant || p.tenant.Admin {
		return nil
	}
	if b.PublicRead && !write {
		return nil
	}
	return errAccessDenied
}

// admitTenant applies the per-tenant request rate limit.
func (g *Gateway) admitTenant(p principal) error {
	if p.tenant == nil {
		return nil
	}
	if !p.tenant.AllowRequest() {
		g.m.rateLimited.With(p.tenant.ID, "requests").Inc()
		return quota.ErrRateLimited
	}
	return nil
}

func (g *Gateway) admin(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p, err := g.authenticate(r)
		if err == nil && (p.anonymous || !p.tenant.Admin) {
			err = errAccessDenied
		}
		if err != nil {
			g.fail(w, r, err)
			return
		}
		h(w, r)
	}
}

func (g *Gateway) metaCtx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, g.cfg.MetadataTimeout)
}

// bucket looks up bucket config (cached).
func (g *Gateway) bucket(ctx context.Context, name string) (metadata.Bucket, error) {
	if b, ok := g.bucketCache.Get(name); ok {
		return b, nil
	}
	mctx, cancel := g.metaCtx(ctx)
	defer cancel()
	start := time.Now()
	b, err := g.meta.GetBucket(mctx, name)
	g.m.metaLatency.With("get_bucket").Observe(time.Since(start).Seconds())
	if err != nil {
		return b, err
	}
	g.bucketCache.Add(name, b, 1)
	return b, nil
}

// request bundles the common prologue: authenticate, rate-limit, load and
// authorize the bucket.
func (g *Gateway) prologue(r *http.Request, write bool) (principal, metadata.Bucket, error) {
	p, err := g.authenticate(r)
	if err != nil {
		return p, metadata.Bucket{}, err
	}
	if err := g.admitTenant(p); err != nil {
		return p, metadata.Bucket{}, err
	}
	name := r.PathValue("bucket")
	if err := object.ValidateBucket(name); err != nil {
		return p, metadata.Bucket{}, err
	}
	b, err := g.bucket(r.Context(), name)
	if err != nil {
		return p, b, err
	}
	return p, b, g.authorize(p, b, write)
}

// ---------------------------------------------------------------------------
// Buckets

type bucketRequest struct {
	Versioning        bool                `json:"versioning"`
	PublicRead        bool                `json:"public_read"`
	ReplicationFactor int                 `json:"replication_factor"`
	StorageClass      object.StorageClass `json:"storage_class"`
}

func (g *Gateway) handleCreateBucket(w http.ResponseWriter, r *http.Request) {
	p, err := g.authenticate(r)
	if err == nil && p.anonymous {
		err = errUnauthenticated
	}
	if err == nil {
		err = g.admitTenant(p)
	}
	if err != nil {
		g.fail(w, r, err)
		return
	}
	var req bucketRequest
	if r.ContentLength != 0 {
		if err := json.NewDecoder(io.LimitReader(r.Body, 4096)).Decode(&req); err != nil {
			g.fail(w, r, errors.Join(errBadRequest, err))
			return
		}
	}
	if req.StorageClass == object.ClassReduced && req.ReplicationFactor == 0 {
		req.ReplicationFactor = 2
	}
	b := metadata.Bucket{Name: r.PathValue("bucket"), Tenant: p.tenant.ID, Versioning: req.Versioning,
		PublicRead: req.PublicRead, ReplicationFactor: req.ReplicationFactor, StorageClass: req.StorageClass}
	ctx, cancel := g.metaCtx(r.Context())
	defer cancel()
	if err := g.meta.CreateBucket(ctx, b); err != nil {
		g.fail(w, r, err)
		return
	}
	g.audit(r, p, "create_bucket", b.Name, "")
	created, _ := g.meta.GetBucket(ctx, b.Name)
	writeJSON(w, http.StatusOK, created)
}

func (g *Gateway) handleGetBucket(w http.ResponseWriter, r *http.Request) {
	_, b, err := g.prologue(r, false)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, b)
}

// audit writes one structured record per mutating operation. In production
// this stream goes to append-only storage separate from operational logs.
func (g *Gateway) audit(r *http.Request, p principal, action, bucket, key string, extra ...any) {
	tenant := "anonymous"
	if p.tenant != nil {
		tenant = p.tenant.ID
	}
	args := []any{"audit", true, "action", action, "tenant", tenant, "bucket", bucket, "key", key,
		"request_id", observability.RequestID(r.Context()), "remote", r.RemoteAddr}
	g.log.Info("audit", append(args, extra...)...)
}

// ---------------------------------------------------------------------------
// Metrics

type metrics struct {
	reg          *observability.Registry
	requests     *observability.CounterVec
	latency      *observability.HistogramVec
	inflight     *observability.GaugeVec
	bytesIn      *observability.CounterVec
	bytesOut     *observability.CounterVec
	replBytes    *observability.CounterVec
	underRepl    *observability.CounterVec
	replicaDrops *observability.CounterVec
	failovers    *observability.CounterVec
	metaLatency  *observability.HistogramVec
	rateLimited  *observability.CounterVec
	objSize      *observability.HistogramVec
}

func newMetrics(g *Gateway) *metrics {
	reg := observability.NewRegistry()
	m := &metrics{
		reg:          reg,
		requests:     reg.Counter("gateway_requests_total", "API requests by operation and status code.", "op", "code"),
		latency:      reg.Histogram("gateway_request_seconds", "API request latency (time to last byte).", observability.LatencyBuckets, "op"),
		inflight:     reg.Gauge("gateway_inflight_requests", "Requests in progress.", "op"),
		bytesIn:      reg.Counter("gateway_ingress_bytes_total", "Object bytes received from clients."),
		bytesOut:     reg.Counter("gateway_egress_bytes_total", "Object bytes sent to clients.", "source"),
		replBytes:    reg.Counter("gateway_replication_bytes_total", "Bytes written to storage nodes (ingress x replicas)."),
		underRepl:    reg.Counter("gateway_under_replicated_writes_total", "Writes acknowledged with fewer than RF replicas (repair will follow)."),
		replicaDrops: reg.Counter("gateway_replica_write_failures_total", "Replica writes that failed or were dropped.", "reason"),
		failovers:    reg.Counter("gateway_read_failovers_total", "Reads that switched to another replica."),
		metaLatency:  reg.Histogram("gateway_metadata_seconds", "Metadata call latency.", observability.LatencyBuckets, "op"),
		rateLimited:  reg.Counter("gateway_rate_limited_total", "Requests rejected by tenant limits.", "tenant", "kind"),
		objSize:      reg.Histogram("gateway_object_size_bytes", "Sizes of uploaded objects.", observability.SizeBuckets),
	}
	cacheGauges := func(prefix, what string, st func() cache.Stats) {
		reg.CounterFunc(prefix+"_hits_total", what+" cache hits.", func() float64 { return float64(st().Hits) })
		reg.CounterFunc(prefix+"_misses_total", what+" cache misses.", func() float64 { return float64(st().Misses) })
		reg.CounterFunc(prefix+"_evictions_total", what+" cache evictions.", func() float64 { return float64(st().Evictions) })
		reg.GaugeFunc(prefix+"_cost", what+" cache size (bytes for objects, entries for metadata).", func() float64 { return float64(st().Cost) })
	}
	cacheGauges("gateway_object_cache", "Hot-object", g.objCache.Stats)
	cacheGauges("gateway_metadata_cache", "Object metadata", g.metaCache.Stats)
	reg.CounterFunc("gateway_object_cache_coalesced_total", "Cache misses served by another in-flight load (stampede protection).",
		func() float64 { return float64(g.objFlight.Deduplicated()) })
	reg.GaugeFunc("gateway_membership_epoch", "Membership epoch of the current placement map.",
		func() float64 { return float64(g.members.Map().Epoch) })
	reg.GaugeFunc("gateway_penalized_nodes", "Nodes this gateway avoids for new placements after they stalled a write.",
		func() float64 { return float64(g.slow.count()) })
	reg.GaugeFunc("gateway_inflight_uploads", "Uploads holding a global upload slot.", func() float64 { return float64(g.uploadSlots.InUse()) })
	return m
}

func (g *Gateway) instrument(op string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		gauge := g.m.inflight.With(op)
		gauge.Add(1)
		rec, ok := w.(*observability.StatusRecorder)
		if !ok {
			rec = &observability.StatusRecorder{ResponseWriter: w}
		}
		// Deferred so that streams aborted with http.ErrAbortHandler are
		// still counted.
		defer func() {
			gauge.Add(-1)
			status := rec.Status
			if status == 0 {
				status = http.StatusOK
			}
			name := op
			if r.Method == http.MethodHead {
				name = "head"
			}
			g.m.requests.With(name, statusClass(status)).Inc()
			g.m.latency.With(name).Observe(time.Since(start).Seconds())
		}()
		h(rec, r)
	}
}

func statusClass(s int) string {
	switch {
	case s < 300:
		return "2xx"
	case s < 400:
		return "3xx"
	case s == 404:
		return "404"
	case s == 429:
		return "429"
	case s < 500:
		return "4xx"
	case s == 503:
		return "503"
	default:
		return "5xx"
	}
}

func finite(f float64) float64 {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return 0
	}
	return f
}

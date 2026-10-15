package metadata

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/michael-emmanuel/objstore/internal/object"
	"github.com/michael-emmanuel/objstore/internal/observability"
)

// The metadata RPC is JSON over HTTP: POST /rpc/<Method>. It is simple to
// debug with curl, which matters more here than wire efficiency; metadata
// messages are small and bounded (see object.Max* limits).

type errBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

var errCodes = map[string]error{
	"not_found":        ErrNotFound,
	"bucket_exists":    ErrBucketExists,
	"no_such_bucket":   ErrNoSuchBucket,
	"no_such_upload":   ErrNoSuchUpload,
	"invalid_part":     ErrInvalidPart,
	"quota_exceeded":   ErrQuotaExceeded,
	"invalid_argument": ErrInvalidArgument,
	"unavailable":      ErrUnavailable,
	"read_only":        ErrReadOnly,
}

func codeFor(err error) (string, int) {
	for code, e := range errCodes {
		if errors.Is(err, e) {
			switch e {
			case ErrNotFound, ErrNoSuchBucket, ErrNoSuchUpload:
				return code, http.StatusNotFound
			case ErrBucketExists:
				return code, http.StatusConflict
			case ErrUnavailable, ErrReadOnly:
				return code, http.StatusServiceUnavailable
			default:
				return code, http.StatusBadRequest
			}
		}
	}
	return "internal", http.StatusInternalServerError
}

// Faults lets fault-injection experiments make the metadata service fail or
// slow down without killing the process.
type Faults struct {
	FailReads  bool `json:"fail_reads"`
	FailWrites bool `json:"fail_writes"`
	DelayMS    int  `json:"delay_ms"`
}

// Server exposes a Store over HTTP.
type Server struct {
	store  Store
	faults atomic.Pointer[Faults]
	mux    *http.ServeMux
}

func NewServer(store Store) *Server {
	s := &Server{store: store, mux: http.NewServeMux()}
	s.faults.Store(&Faults{})
	s.routes()
	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// Mux allows the hosting binary to add more routes (membership, metrics).
func (s *Server) Mux() *http.ServeMux { return s.mux }

func (s *Server) SetFaults(f Faults) { s.faults.Store(&f) }

func rpc[Req, Resp any](s *Server, name string, write bool, fn func(context.Context, Req) (Resp, error)) {
	s.mux.HandleFunc("POST /rpc/"+name, func(w http.ResponseWriter, r *http.Request) {
		f := s.faults.Load()
		if f.DelayMS > 0 {
			select {
			case <-time.After(time.Duration(f.DelayMS) * time.Millisecond):
			case <-r.Context().Done():
				return
			}
		}
		if (write && f.FailWrites) || (!write && f.FailReads) {
			writeErr(w, fmt.Errorf("%w: injected fault", ErrUnavailable))
			return
		}
		var req Req
		if err := json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(&req); err != nil {
			writeErr(w, fmt.Errorf("%w: %v", ErrInvalidArgument, err))
			return
		}
		resp, err := fn(r.Context(), req)
		if err != nil {
			writeErr(w, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})
}

func writeErr(w http.ResponseWriter, err error) {
	code, status := codeFor(err)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(errBody{Code: code, Message: err.Error()})
}

type (
	nameReq    struct{ Name string }
	pendingReq struct {
		Bucket  string
		BlobIDs []string
		TTL     time.Duration
	}
	commitReq struct {
		Version object.Version
		Opts    CommitOptions
	}
	getReq  struct{ Bucket, Key, VersionID string }
	partReq struct {
		UploadID string
		Part     Part
	}
	completeReq struct {
		UploadID  string
		Parts     []CompletePart
		VersionID string
		Opts      CommitOptions
	}
	scanReq struct {
		Cursor string
		Limit  int
	}
	scanResp struct {
		Segments []SegmentRef
		Next     string
	}
	replicasReq struct {
		Bucket, BlobID string
		Add, Remove    []string
	}
	idsReq  struct{ IDs []string }
	timeReq struct {
		OlderThan time.Time
		Limit     int
	}
	garbageReq struct{ Items []GarbageItem }
	empty      struct{}
)

func (s *Server) routes() {
	st := s.store
	rpc(s, "CreateBucket", true, func(ctx context.Context, b Bucket) (empty, error) { return empty{}, st.CreateBucket(ctx, b) })
	rpc(s, "GetBucket", false, func(ctx context.Context, r nameReq) (Bucket, error) { return st.GetBucket(ctx, r.Name) })
	rpc(s, "RegisterPending", true, func(ctx context.Context, r pendingReq) (empty, error) {
		return empty{}, st.RegisterPending(ctx, r.Bucket, r.BlobIDs, r.TTL)
	})
	rpc(s, "Commit", true, func(ctx context.Context, r commitReq) (object.Version, error) {
		return st.Commit(ctx, r.Version, r.Opts)
	})
	rpc(s, "Get", false, func(ctx context.Context, r getReq) (object.Version, error) {
		return st.Get(ctx, r.Bucket, r.Key, r.VersionID)
	})
	rpc(s, "Delete", true, func(ctx context.Context, r getReq) (object.Version, error) { return st.Delete(ctx, r.Bucket, r.Key) })
	rpc(s, "DeleteVersion", true, func(ctx context.Context, r getReq) (empty, error) {
		return empty{}, st.DeleteVersion(ctx, r.Bucket, r.Key, r.VersionID)
	})
	rpc(s, "List", false, func(ctx context.Context, r ListRequest) (ListResult, error) { return st.List(ctx, r) })
	rpc(s, "ListVersions", false, func(ctx context.Context, r getReq) ([]object.Version, error) {
		return st.ListVersions(ctx, r.Bucket, r.Key)
	})
	rpc(s, "CreateUpload", true, func(ctx context.Context, u Upload) (empty, error) { return empty{}, st.CreateUpload(ctx, u) })
	rpc(s, "GetUpload", false, func(ctx context.Context, r nameReq) (Upload, error) { return st.GetUpload(ctx, r.Name) })
	rpc(s, "PutPart", true, func(ctx context.Context, r partReq) (empty, error) {
		return empty{}, st.PutPart(ctx, r.UploadID, r.Part)
	})
	rpc(s, "CompleteUpload", true, func(ctx context.Context, r completeReq) (object.Version, error) {
		return st.CompleteUpload(ctx, r.UploadID, r.Parts, r.VersionID, r.Opts)
	})
	rpc(s, "AbortUpload", true, func(ctx context.Context, r nameReq) (empty, error) { return empty{}, st.AbortUpload(ctx, r.Name) })
	rpc(s, "ScanSegments", false, func(ctx context.Context, r scanReq) (scanResp, error) {
		segs, next, err := st.ScanSegments(ctx, r.Cursor, r.Limit)
		return scanResp{segs, next}, err
	})
	rpc(s, "UpdateReplicas", true, func(ctx context.Context, r replicasReq) (empty, error) {
		return empty{}, st.UpdateReplicas(ctx, r.Bucket, r.BlobID, r.Add, r.Remove)
	})
	rpc(s, "CheckRefs", false, func(ctx context.Context, r idsReq) (map[string]BlobRef, error) { return st.CheckRefs(ctx, r.IDs) })
	rpc(s, "ReplicaCount", false, func(ctx context.Context, r nameReq) (int, error) { return st.ReplicaCount(ctx, r.Name) })
	rpc(s, "PeekGarbage", false, func(ctx context.Context, r timeReq) ([]GarbageItem, error) {
		return st.PeekGarbage(ctx, r.OlderThan, r.Limit)
	})
	rpc(s, "AckGarbage", true, func(ctx context.Context, r garbageReq) (empty, error) { return empty{}, st.AckGarbage(ctx, r.Items) })
	rpc(s, "ExpireUploads", true, func(ctx context.Context, r timeReq) (int, error) { return st.ExpireUploads(ctx, r.OlderThan) })
	rpc(s, "PurgeTombstones", true, func(ctx context.Context, r timeReq) (int, error) { return st.PurgeTombstones(ctx, r.OlderThan) })
	rpc(s, "Usage", false, func(ctx context.Context, r nameReq) ([]int64, error) { return st.Usage(ctx, r.Name) })
	rpc(s, "Stats", false, func(ctx context.Context, _ empty) ([]Stats, error) { return st.Stats(ctx) })

	s.mux.HandleFunc("POST /admin/faults", func(w http.ResponseWriter, r *http.Request) {
		var f Faults
		if err := json.NewDecoder(r.Body).Decode(&f); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.SetFaults(f)
		json.NewEncoder(w).Encode(f)
	})
	s.mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok\n")) })
}

// ---------------------------------------------------------------------------

// Client is a Store backed by a remote metadata Server.
type Client struct {
	base string
	hc   *http.Client
}

func NewClient(baseURL string, hc *http.Client) *Client {
	if hc == nil {
		hc = &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 64}}
	}
	return &Client{base: baseURL, hc: hc}
}

func call[Req, Resp any](ctx context.Context, c *Client, name string, req Req) (Resp, error) {
	var zero Resp
	b, err := json.Marshal(req)
	if err != nil {
		return zero, err
	}
	hr, err := http.NewRequestWithContext(ctx, http.MethodPost, c.base+"/rpc/"+name, bytes.NewReader(b))
	if err != nil {
		return zero, err
	}
	hr.Header.Set("Content-Type", "application/json")
	observability.Inject(ctx, hr.Header)
	resp, err := c.hc.Do(hr)
	if err != nil {
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}
		return zero, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		var eb errBody
		json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&eb)
		if e, ok := errCodes[eb.Code]; ok {
			return zero, fmt.Errorf("%w: %s", e, eb.Message)
		}
		return zero, fmt.Errorf("metadata %s: http %d: %s", name, resp.StatusCode, eb.Message)
	}
	var out Resp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return zero, fmt.Errorf("%w: decode %s: %v", ErrUnavailable, name, err)
	}
	return out, nil
}

func (c *Client) CreateBucket(ctx context.Context, b Bucket) error {
	_, err := call[Bucket, empty](ctx, c, "CreateBucket", b)
	return err
}
func (c *Client) GetBucket(ctx context.Context, name string) (Bucket, error) {
	return call[nameReq, Bucket](ctx, c, "GetBucket", nameReq{name})
}
func (c *Client) RegisterPending(ctx context.Context, bucket string, ids []string, ttl time.Duration) error {
	_, err := call[pendingReq, empty](ctx, c, "RegisterPending", pendingReq{bucket, ids, ttl})
	return err
}
func (c *Client) Commit(ctx context.Context, v object.Version, o CommitOptions) (object.Version, error) {
	return call[commitReq, object.Version](ctx, c, "Commit", commitReq{v, o})
}
func (c *Client) Get(ctx context.Context, bucket, key, vid string) (object.Version, error) {
	return call[getReq, object.Version](ctx, c, "Get", getReq{bucket, key, vid})
}
func (c *Client) Delete(ctx context.Context, bucket, key string) (object.Version, error) {
	return call[getReq, object.Version](ctx, c, "Delete", getReq{Bucket: bucket, Key: key})
}
func (c *Client) DeleteVersion(ctx context.Context, bucket, key, vid string) error {
	_, err := call[getReq, empty](ctx, c, "DeleteVersion", getReq{bucket, key, vid})
	return err
}
func (c *Client) List(ctx context.Context, r ListRequest) (ListResult, error) {
	return call[ListRequest, ListResult](ctx, c, "List", r)
}
func (c *Client) ListVersions(ctx context.Context, bucket, key string) ([]object.Version, error) {
	return call[getReq, []object.Version](ctx, c, "ListVersions", getReq{Bucket: bucket, Key: key})
}
func (c *Client) CreateUpload(ctx context.Context, u Upload) error {
	_, err := call[Upload, empty](ctx, c, "CreateUpload", u)
	return err
}
func (c *Client) GetUpload(ctx context.Context, id string) (Upload, error) {
	return call[nameReq, Upload](ctx, c, "GetUpload", nameReq{id})
}
func (c *Client) PutPart(ctx context.Context, id string, p Part) error {
	_, err := call[partReq, empty](ctx, c, "PutPart", partReq{id, p})
	return err
}
func (c *Client) CompleteUpload(ctx context.Context, id string, parts []CompletePart, vid string, o CommitOptions) (object.Version, error) {
	return call[completeReq, object.Version](ctx, c, "CompleteUpload", completeReq{id, parts, vid, o})
}
func (c *Client) AbortUpload(ctx context.Context, id string) error {
	_, err := call[nameReq, empty](ctx, c, "AbortUpload", nameReq{id})
	return err
}
func (c *Client) ScanSegments(ctx context.Context, cursor string, limit int) ([]SegmentRef, string, error) {
	r, err := call[scanReq, scanResp](ctx, c, "ScanSegments", scanReq{cursor, limit})
	return r.Segments, r.Next, err
}
func (c *Client) UpdateReplicas(ctx context.Context, bucket, blobID string, add, remove []string) error {
	_, err := call[replicasReq, empty](ctx, c, "UpdateReplicas", replicasReq{bucket, blobID, add, remove})
	return err
}
func (c *Client) CheckRefs(ctx context.Context, ids []string) (map[string]BlobRef, error) {
	return call[idsReq, map[string]BlobRef](ctx, c, "CheckRefs", idsReq{ids})
}
func (c *Client) ReplicaCount(ctx context.Context, node string) (int, error) {
	return call[nameReq, int](ctx, c, "ReplicaCount", nameReq{node})
}
func (c *Client) PeekGarbage(ctx context.Context, olderThan time.Time, limit int) ([]GarbageItem, error) {
	return call[timeReq, []GarbageItem](ctx, c, "PeekGarbage", timeReq{olderThan, limit})
}
func (c *Client) AckGarbage(ctx context.Context, items []GarbageItem) error {
	_, err := call[garbageReq, empty](ctx, c, "AckGarbage", garbageReq{items})
	return err
}
func (c *Client) ExpireUploads(ctx context.Context, olderThan time.Time) (int, error) {
	return call[timeReq, int](ctx, c, "ExpireUploads", timeReq{OlderThan: olderThan})
}
func (c *Client) PurgeTombstones(ctx context.Context, olderThan time.Time) (int, error) {
	return call[timeReq, int](ctx, c, "PurgeTombstones", timeReq{OlderThan: olderThan})
}
func (c *Client) Usage(ctx context.Context, tenant string) ([]int64, error) {
	return call[nameReq, []int64](ctx, c, "Usage", nameReq{tenant})
}
func (c *Client) Stats(ctx context.Context) ([]Stats, error) {
	return call[empty, []Stats](ctx, c, "Stats", empty{})
}

var _ Store = (*Client)(nil)

// Package client is a small Go client for the object storage API. It
// streams request and response bodies; it never buffers whole objects.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client talks to a gateway.
type Client struct {
	Base  string
	Token string
	HC    *http.Client
}

func New(base, token string) *Client {
	return &Client{Base: strings.TrimRight(base, "/"), Token: token, HC: &http.Client{
		Transport: &http.Transport{MaxIdleConnsPerHost: 256, DisableCompression: true, ResponseHeaderTimeout: 60 * time.Second},
	}}
}

// APIError is a non-2xx response.
type APIError struct {
	Status    int
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

func (e *APIError) Error() string {
	return fmt.Sprintf("%d %s: %s (request %s)", e.Status, e.Code, e.Message, e.RequestID)
}

// IsStatus reports whether err is an APIError with the given status.
func IsStatus(err error, status int) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Status == status
}

func (c *Client) req(ctx context.Context, method, path string, q url.Values, body io.Reader) (*http.Request, error) {
	u := c.Base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	r, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}
	if c.Token != "" {
		r.Header.Set("Authorization", "Bearer "+c.Token)
	}
	return r, nil
}

func (c *Client) do(r *http.Request, okStatus ...int) (*http.Response, error) {
	resp, err := c.HC.Do(r)
	if err != nil {
		return nil, err
	}
	for _, s := range okStatus {
		if resp.StatusCode == s {
			return resp, nil
		}
	}
	defer resp.Body.Close()
	ae := &APIError{Status: resp.StatusCode, RequestID: resp.Header.Get("X-Request-Id")}
	var wrap struct {
		Error *APIError `json:"error"`
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if json.Unmarshal(b, &wrap) == nil && wrap.Error != nil {
		wrap.Error.Status = resp.StatusCode
		return nil, wrap.Error
	}
	ae.Message = string(bytes.TrimSpace(b))
	return nil, ae
}

func objPath(bucket, key string) string {
	return "/v1/buckets/" + url.PathEscape(bucket) + "/objects/" + escapeKey(key)
}

// escapeKey escapes each path segment but keeps '/' separators.
func escapeKey(key string) string {
	parts := strings.Split(key, "/")
	for i, p := range parts {
		parts[i] = url.PathEscape(p)
	}
	return strings.Join(parts, "/")
}

// BucketOptions configure a new bucket.
type BucketOptions struct {
	Versioning        bool   `json:"versioning"`
	PublicRead        bool   `json:"public_read"`
	ReplicationFactor int    `json:"replication_factor,omitempty"`
	StorageClass      string `json:"storage_class,omitempty"`
}

func (c *Client) CreateBucket(ctx context.Context, name string, opts BucketOptions) error {
	b, _ := json.Marshal(opts)
	r, err := c.req(ctx, http.MethodPut, "/v1/buckets/"+url.PathEscape(name), nil, bytes.NewReader(b))
	if err != nil {
		return err
	}
	resp, err := c.do(r, http.StatusOK)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// PutOptions are optional PUT parameters.
type PutOptions struct {
	ContentType string
	SHA256      string // hex; server verifies end-to-end
	Meta        map[string]string
}

// PutResult mirrors the gateway response.
type PutResult struct {
	VersionID         string `json:"version_id"`
	ETag              string `json:"etag"`
	Size              int64  `json:"size"`
	Segments          int    `json:"segments"`
	PartNumber        int    `json:"part_number"`
	ReplicationFactor int    `json:"replication_factor"`
	WriteQuorum       int    `json:"write_quorum"`
	MinReplicasAcked  int    `json:"min_replicas_acked"`
	UnderReplicated   bool   `json:"under_replicated"`
}

// Put streams body. size < 0 sends chunked transfer encoding.
func (c *Client) Put(ctx context.Context, bucket, key string, body io.Reader, size int64, o PutOptions) (PutResult, error) {
	r, err := c.req(ctx, http.MethodPut, objPath(bucket, key), nil, body)
	if err != nil {
		return PutResult{}, err
	}
	r.ContentLength = size
	if size == 0 {
		r.Body = http.NoBody
	}
	if o.ContentType != "" {
		r.Header.Set("Content-Type", o.ContentType)
	}
	if o.SHA256 != "" {
		r.Header.Set("X-Checksum-SHA256", o.SHA256)
	}
	for k, v := range o.Meta {
		r.Header.Set("X-Meta-"+k, v)
	}
	resp, err := c.do(r, http.StatusOK)
	if err != nil {
		return PutResult{}, err
	}
	defer resp.Body.Close()
	var out PutResult
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

// ObjectInfo is parsed from response headers.
type ObjectInfo struct {
	Size          int64
	ContentLength int64
	ETag          string
	VersionID     string
	ContentType   string
	Cache         string
	Meta          map[string]string
	Status        int
}

func info(resp *http.Response) ObjectInfo {
	oi := ObjectInfo{ETag: strings.Trim(resp.Header.Get("ETag"), `"`), VersionID: resp.Header.Get("X-Version-Id"),
		ContentType: resp.Header.Get("Content-Type"), ContentLength: resp.ContentLength, Cache: resp.Header.Get("X-Cache"),
		Status: resp.StatusCode, Meta: map[string]string{}}
	oi.Size = resp.ContentLength
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		if i := strings.LastIndexByte(cr, '/'); i >= 0 {
			oi.Size, _ = strconv.ParseInt(cr[i+1:], 10, 64)
		}
	}
	for k, v := range resp.Header {
		if name, ok := strings.CutPrefix(k, "X-Meta-"); ok {
			oi.Meta[strings.ToLower(name)] = v[0]
		}
	}
	return oi
}

// GetOptions are optional GET parameters.
type GetOptions struct {
	VersionID   string
	RangeStart  int64
	RangeEnd    int64 // inclusive; used only when HasRange
	HasRange    bool
	Consistency string // "quorum" to verify R replicas before reading
}

// Get returns a streaming body; the caller must Close it.
func (c *Client) Get(ctx context.Context, bucket, key string, o GetOptions) (io.ReadCloser, ObjectInfo, error) {
	q := url.Values{}
	if o.VersionID != "" {
		q.Set("version_id", o.VersionID)
	}
	if o.Consistency != "" {
		q.Set("consistency", o.Consistency)
	}
	r, err := c.req(ctx, http.MethodGet, objPath(bucket, key), q, nil)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	if o.HasRange {
		r.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", o.RangeStart, o.RangeEnd))
	}
	resp, err := c.do(r, http.StatusOK, http.StatusPartialContent)
	if err != nil {
		return nil, ObjectInfo{}, err
	}
	return resp.Body, info(resp), nil
}

func (c *Client) Head(ctx context.Context, bucket, key string) (ObjectInfo, error) {
	r, err := c.req(ctx, http.MethodHead, objPath(bucket, key), nil, nil)
	if err != nil {
		return ObjectInfo{}, err
	}
	resp, err := c.do(r, http.StatusOK)
	if err != nil {
		return ObjectInfo{}, err
	}
	resp.Body.Close()
	return info(resp), nil
}

func (c *Client) Delete(ctx context.Context, bucket, key string) error {
	r, err := c.req(ctx, http.MethodDelete, objPath(bucket, key), nil, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(r, http.StatusNoContent)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// ListOptions control LIST.
type ListOptions struct {
	Prefix, Delimiter, ContinuationToken string
	MaxKeys                              int
}

type ListedObject struct {
	Key          string    `json:"key"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	VersionID    string    `json:"version_id"`
	LastModified time.Time `json:"last_modified"`
}

type ListPage struct {
	Objects               []ListedObject `json:"objects"`
	CommonPrefixes        []string       `json:"common_prefixes"`
	IsTruncated           bool           `json:"is_truncated"`
	NextContinuationToken string         `json:"next_continuation_token"`
}

func (c *Client) List(ctx context.Context, bucket string, o ListOptions) (ListPage, error) {
	q := url.Values{}
	if o.Prefix != "" {
		q.Set("prefix", o.Prefix)
	}
	if o.Delimiter != "" {
		q.Set("delimiter", o.Delimiter)
	}
	if o.ContinuationToken != "" {
		q.Set("continuation_token", o.ContinuationToken)
	}
	if o.MaxKeys > 0 {
		q.Set("max_keys", strconv.Itoa(o.MaxKeys))
	}
	r, err := c.req(ctx, http.MethodGet, "/v1/buckets/"+url.PathEscape(bucket)+"/objects", q, nil)
	if err != nil {
		return ListPage{}, err
	}
	resp, err := c.do(r, http.StatusOK)
	if err != nil {
		return ListPage{}, err
	}
	defer resp.Body.Close()
	var p ListPage
	return p, json.NewDecoder(resp.Body).Decode(&p)
}

// CreateUpload starts a multipart upload.
func (c *Client) CreateUpload(ctx context.Context, bucket, key, contentType string) (string, error) {
	b, _ := json.Marshal(map[string]string{"key": key, "content_type": contentType})
	r, err := c.req(ctx, http.MethodPost, "/v1/buckets/"+url.PathEscape(bucket)+"/multipart", nil, bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	resp, err := c.do(r, http.StatusOK)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var out struct {
		UploadID string `json:"upload_id"`
	}
	return out.UploadID, json.NewDecoder(resp.Body).Decode(&out)
}

// UploadPart sends one part; returns its ETag (SHA-256 hex).
func (c *Client) UploadPart(ctx context.Context, uploadID string, n int, body io.Reader, size int64, sha256hex string) (PutResult, error) {
	q := url.Values{"part_number": {strconv.Itoa(n)}}
	r, err := c.req(ctx, http.MethodPost, "/v1/multipart/"+url.PathEscape(uploadID)+"/parts", q, body)
	if err != nil {
		return PutResult{}, err
	}
	r.ContentLength = size
	if sha256hex != "" {
		r.Header.Set("X-Checksum-SHA256", sha256hex)
	}
	resp, err := c.do(r, http.StatusOK)
	if err != nil {
		return PutResult{}, err
	}
	defer resp.Body.Close()
	var out PutResult
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

// Part identifies an uploaded part for completion.
type Part struct {
	Number int    `json:"part_number"`
	ETag   string `json:"etag"`
}

func (c *Client) CompleteUpload(ctx context.Context, uploadID string, parts []Part) (PutResult, error) {
	b, _ := json.Marshal(map[string]any{"parts": parts})
	r, err := c.req(ctx, http.MethodPost, "/v1/multipart/"+url.PathEscape(uploadID)+"/complete", nil, bytes.NewReader(b))
	if err != nil {
		return PutResult{}, err
	}
	resp, err := c.do(r, http.StatusOK)
	if err != nil {
		return PutResult{}, err
	}
	defer resp.Body.Close()
	var out PutResult
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

func (c *Client) AbortUpload(ctx context.Context, uploadID string) error {
	r, err := c.req(ctx, http.MethodDelete, "/v1/multipart/"+url.PathEscape(uploadID), nil, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(r, http.StatusNoContent)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// Raw performs an arbitrary request (admin endpoints, demos).
func (c *Client) Raw(ctx context.Context, method, path string, body any, out any) error {
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	r, err := c.req(ctx, method, path, nil, rd)
	if err != nil {
		return err
	}
	resp, err := c.do(r, http.StatusOK, http.StatusNoContent)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if out != nil && resp.StatusCode == http.StatusOK {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

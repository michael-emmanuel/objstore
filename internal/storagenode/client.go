package storagenode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/storage"
)

// ErrUnreachable wraps transport-level failures (connection refused, reset,
// timeout): the node may be down, partitioned or overloaded.
var ErrUnreachable = errors.New("storage node unreachable")

// Client is the data-plane client used by gateways, repairers, the GC and
// by nodes themselves for peer pulls.
type Client struct {
	hc *http.Client
}

// NewClient builds a client. Note there is deliberately no overall request
// timeout: a 100 GB transfer legitimately takes minutes. Liveness is bounded
// by dial/header timeouts and by the caller's context, and stalls inside a
// transfer are handled by the stall detector in the replication writer.
func NewClient(hc *http.Client) *Client {
	if hc == nil {
		hc = DefaultHTTPClient()
	}
	return &Client{hc: hc}
}

// DefaultHTTPClient returns the transport tuning used for data-plane traffic,
// so callers that wrap it (for example to add cluster authentication) keep
// the same timeouts and connection pooling.
func DefaultHTTPClient() *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: time.Second,
		DisableCompression:    true, // object bytes are opaque; compression would only cost CPU
	}}
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	observability.Inject(req.Context(), req.Header)
	resp, err := c.hc.Do(req)
	if err != nil {
		if req.Context().Err() != nil {
			return nil, req.Context().Err()
		}
		return nil, fmt.Errorf("%w: %v", ErrUnreachable, err)
	}
	return resp, nil
}

func statusErr(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	msg := string(bytes.TrimSpace(b))
	var base error
	switch resp.StatusCode {
	case http.StatusNotFound:
		base = storage.ErrNotFound
	case http.StatusConflict:
		base = storage.ErrConflict
	case http.StatusInsufficientStorage:
		base = storage.ErrDiskFull
	case http.StatusUnprocessableEntity:
		base = storage.ErrChecksumMismatch
	case http.StatusRequestedRangeNotSatisfiable:
		base = storage.ErrRange
	case http.StatusServiceUnavailable, http.StatusBadGateway:
		base = ErrUnreachable
	default:
		if bytes.Contains(b, []byte("integrity")) {
			base = storage.ErrCorrupt
		} else {
			base = fmt.Errorf("storage node http %d", resp.StatusCode)
		}
	}
	return fmt.Errorf("%w: %s", base, msg)
}

// Put streams body as blob id. size < 0 means unknown (chunked encoding).
func (c *Client) Put(ctx context.Context, addr, id string, body io.Reader, size int64, sha256hex, kind string) (storage.BlobInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, addr+"/blobs/"+id, body)
	if err != nil {
		return storage.BlobInfo{}, err
	}
	if size >= 0 {
		req.ContentLength = size
		req.Header.Set("X-Expected-Size", strconv.FormatInt(size, 10))
	}
	if sha256hex != "" {
		req.Header.Set("X-Expected-SHA256", sha256hex)
	}
	if kind != "" {
		req.Header.Set("X-Write-Kind", kind)
	}
	resp, err := c.do(req)
	if err != nil {
		return storage.BlobInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return storage.BlobInfo{}, statusErr(resp)
	}
	var info storage.BlobInfo
	return info, json.NewDecoder(resp.Body).Decode(&info)
}

// Get opens a range [off, off+length) of a blob; length < 0 means to end.
// The returned reader fails (rather than returning io.EOF early) if the
// node aborts the stream, so truncation is always visible to the caller.
func (c *Client) Get(ctx context.Context, addr, id string, off, length int64, kind string) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, addr+"/blobs/"+id, nil)
	if err != nil {
		return nil, err
	}
	if off > 0 || length >= 0 {
		if length >= 0 {
			if length == 0 {
				return io.NopCloser(bytes.NewReader(nil)), nil
			}
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+length-1))
		} else {
			req.Header.Set("Range", fmt.Sprintf("bytes=%d-", off))
		}
	}
	if kind != "" {
		req.Header.Set("X-Read-Kind", kind)
	}
	resp, err := c.do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		defer resp.Body.Close()
		return nil, statusErr(resp)
	}
	return resp.Body, nil
}

// Head returns size and checksum without reading data.
func (c *Client) Head(ctx context.Context, addr, id string) (storage.BlobInfo, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, addr+"/blobs/"+id, nil)
	if err != nil {
		return storage.BlobInfo{}, err
	}
	resp, err := c.do(req)
	if err != nil {
		return storage.BlobInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		if resp.StatusCode == http.StatusNotFound {
			return storage.BlobInfo{}, storage.ErrNotFound
		}
		return storage.BlobInfo{}, statusErr(resp)
	}
	size, _ := strconv.ParseInt(resp.Header.Get("X-Blob-Size"), 10, 64)
	return storage.BlobInfo{ID: id, Size: size, SHA256: resp.Header.Get("X-Blob-SHA256")}, nil
}

// Delete removes a blob; deleting a missing blob succeeds.
func (c *Client) Delete(ctx context.Context, addr, id string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, addr+"/blobs/"+id, nil)
	if err != nil {
		return err
	}
	resp, err := c.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		return statusErr(resp)
	}
	return nil
}

// Pull asks the node at addr to copy blob id from the node at from.
func (c *Client) Pull(ctx context.Context, addr, id, from, sha256hex, kind string) (storage.BlobInfo, error) {
	b, _ := json.Marshal(pullReq{From: from, SHA256: sha256hex, Kind: kind})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, addr+"/blobs/"+id+"/pull", bytes.NewReader(b))
	if err != nil {
		return storage.BlobInfo{}, err
	}
	resp, err := c.do(req)
	if err != nil {
		return storage.BlobInfo{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return storage.BlobInfo{}, statusErr(resp)
	}
	var info storage.BlobInfo
	return info, json.NewDecoder(resp.Body).Decode(&info)
}

func getJSON[T any](ctx context.Context, c *Client, method, url string, body any) (T, error) {
	var zero T
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return zero, err
	}
	resp, err := c.do(req)
	if err != nil {
		return zero, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return zero, statusErr(resp)
	}
	if resp.StatusCode == http.StatusNoContent {
		return zero, nil
	}
	var out T
	return out, json.NewDecoder(resp.Body).Decode(&out)
}

// List pages through committed blobs on a node.
func (c *Client) List(ctx context.Context, addr, after string, limit int) ([]storage.BlobInfo, error) {
	return getJSON[[]storage.BlobInfo](ctx, c, http.MethodGet, fmt.Sprintf("%s/blobs?after=%s&limit=%d", addr, after, limit), nil)
}

func (c *Client) Quarantined(ctx context.Context, addr string) (map[string]string, error) {
	return getJSON[map[string]string](ctx, c, http.MethodGet, addr+"/quarantine", nil)
}

func (c *Client) AckQuarantine(ctx context.Context, addr string, ids []string) error {
	_, err := getJSON[struct{}](ctx, c, http.MethodPost, addr+"/quarantine/ack", ids)
	return err
}

func (c *Client) Stats(ctx context.Context, addr string) (storage.Stats, error) {
	return getJSON[storage.Stats](ctx, c, http.MethodGet, addr+"/stats", nil)
}

func (c *Client) SetFaults(ctx context.Context, addr string, f Faults) error {
	_, err := getJSON[Faults](ctx, c, http.MethodPost, addr+"/admin/faults", f)
	return err
}

func (c *Client) Corrupt(ctx context.Context, addr, id string, offset int64) error {
	_, err := getJSON[struct{}](ctx, c, http.MethodPost, fmt.Sprintf("%s/admin/corrupt/%s?offset=%d", addr, id, offset), nil)
	return err
}

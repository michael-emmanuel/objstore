package membership

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/michael-emmanuel/objstore/internal/placement"
)

// Client talks to the coordinator's membership endpoints.
type Client struct {
	Base string
	HC   *http.Client
}

func NewClient(base string) *Client {
	return &Client{Base: base, HC: &http.Client{Timeout: 5 * time.Second}}
}

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.Base+path, rd)
	if err != nil {
		return err
	}
	resp, err := c.HC.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("membership %s %s: %d %s", method, path, resp.StatusCode, bytes.TrimSpace(b))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func (c *Client) Heartbeat(ctx context.Context, n placement.Node) (placement.Node, error) {
	var out placement.Node
	err := c.do(ctx, http.MethodPost, "/v1/nodes/heartbeat", n, &out)
	return out, err
}

func (c *Client) View(ctx context.Context) (View, error) {
	var v View
	err := c.do(ctx, http.MethodGet, "/v1/nodes", nil, &v)
	return v, err
}

func (c *Client) SetState(ctx context.Context, id string, s placement.NodeState) error {
	return c.do(ctx, http.MethodPost, "/v1/nodes/"+id+"/state", map[string]any{"state": s}, nil)
}

// Source returns the latest placement map.
type Source interface {
	Map() *placement.Map
}

// Watcher keeps a placement.Map fresh by polling the coordinator. When the
// coordinator is unreachable it keeps serving the last good snapshot: the
// data plane should not stop because the control plane blinked.
type Watcher struct {
	client *Client
	algo   placement.Algorithm
	cur    atomic.Pointer[placement.Map]
	log    *slog.Logger
	fails  atomic.Int64
}

func NewWatcher(c *Client, algo placement.Algorithm, log *slog.Logger) *Watcher {
	w := &Watcher{client: c, algo: algo, log: log}
	w.cur.Store(placement.NewMap(0, nil, algo))
	return w
}

func (w *Watcher) Map() *placement.Map { return w.cur.Load() }

// Refresh fetches once; it only rebuilds the map when the epoch changed.
// Node usage (capacity/used) is refreshed on every poll so fullness-based
// placement decisions stay current.
func (w *Watcher) Refresh(ctx context.Context) error {
	v, err := w.client.View(ctx)
	if err != nil {
		if w.fails.Add(1)%10 == 1 {
			w.log.Warn("membership refresh failed; using last snapshot", "error", err, "epoch", w.Map().Epoch)
		}
		return err
	}
	w.fails.Store(0)
	w.cur.Store(placement.NewMap(v.Epoch, v.Nodes, w.algo))
	return nil
}

// Run polls until ctx is cancelled.
func (w *Watcher) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		w.Refresh(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Static is a Source for tests.
type Static struct{ M atomic.Pointer[placement.Map] }

func (s *Static) Map() *placement.Map { return s.M.Load() }

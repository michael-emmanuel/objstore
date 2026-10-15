package storagenode

import (
	"bytes"
	"net/http/httptest"
	"testing"

	"github.com/michael-emmanuel/objstore/internal/clusterauth"
	"github.com/michael-emmanuel/objstore/internal/object"
	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/placement"
	"github.com/michael-emmanuel/objstore/internal/storage"
)

// Regression test: repair and rebalance make the TARGET node pull from the
// source node, so the node's own peer client must authenticate. A node
// built without PeerHTTPClient cannot pull from an authenticated peer.
func TestPullBetweenAuthenticatedNodes(t *testing.T) {
	const token = "test-cluster-token-0123456789"
	start := func(peer bool) *httptest.Server {
		eng, err := storage.Open(storage.Options{Root: t.TempDir(), BlockSize: 4096})
		if err != nil {
			t.Fatal(err)
		}
		cfg := Config{Node: placement.Node{ID: "n"}}
		if peer {
			cfg.PeerHTTPClient = clusterauth.Wrap(DefaultHTTPClient(), token)
		}
		ts := httptest.NewServer(clusterauth.Middleware(token, NewServer(cfg, eng, observability.DiscardLogger()).Handler()))
		t.Cleanup(ts.Close)
		return ts
	}
	src, good, bad := start(true), start(true), start(false)
	c := NewClient(clusterauth.Wrap(DefaultHTTPClient(), token))
	id := object.SegmentBlobID("b", "k", "v", 1)
	d, sum := data(20_000)
	if _, err := c.Put(bctx, src.URL, id, bytes.NewReader(d), -1, sum, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Pull(bctx, good.URL, id, src.URL, sum, "repair"); err != nil {
		t.Fatalf("authenticated pull failed: %v", err)
	}
	if _, err := c.Pull(bctx, bad.URL, id, src.URL, sum, "repair"); err == nil {
		t.Fatal("pull by a node without credentials should be rejected by the source")
	}
	if _, err := NewClient(nil).Head(bctx, src.URL, id); err == nil {
		t.Fatal("unauthenticated data-plane access should be rejected")
	}
}

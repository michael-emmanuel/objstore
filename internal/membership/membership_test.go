package membership

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/placement"
)

func TestFailureDetectorStates(t *testing.T) {
	now := time.Unix(1000, 0)
	r, _ := NewRegistry(Options{SuspectAfter: 3 * time.Second, DownAfter: 10 * time.Second,
		Now: func() time.Time { return now }, Log: observability.DiscardLogger()})
	r.Heartbeat(placement.Node{ID: "n1", Addr: "http://n1"})
	e0, _ := r.Snapshot()
	state := func() placement.NodeState { _, ns := r.Snapshot(); return ns[0].State }

	now = now.Add(2 * time.Second)
	r.Tick()
	if state() != placement.StateActive {
		t.Fatal("suspected too early")
	}
	now = now.Add(2 * time.Second)
	r.Tick()
	if state() != placement.StateSuspect {
		t.Fatal("not suspect after 4s")
	}
	now = now.Add(7 * time.Second)
	r.Tick()
	if state() != placement.StateDown {
		t.Fatal("not down after 11s")
	}
	r.Heartbeat(placement.Node{ID: "n1", Addr: "http://n1"})
	if state() != placement.StateActive {
		t.Fatal("did not recover on heartbeat")
	}
	if e1, _ := r.Snapshot(); e1 <= e0 {
		t.Fatal("epoch must advance on state changes")
	}
}

func TestOperatorStatesSurviveHeartbeatsAndRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "members.json")
	r, _ := NewRegistry(Options{StatePath: path, Log: observability.DiscardLogger()})
	r.Heartbeat(placement.Node{ID: "n1", Addr: "http://n1", Zone: "a"})
	if err := r.SetState("n1", placement.StateDraining); err != nil {
		t.Fatal(err)
	}
	r.Heartbeat(placement.Node{ID: "n1", Addr: "http://n1", Zone: "a"})
	if _, ns := r.Snapshot(); ns[0].State != placement.StateDraining {
		t.Fatal("heartbeat overrode DRAINING")
	}
	if err := r.SetState("n1", placement.StateDown); err == nil {
		t.Fatal("operator must not set DOWN")
	}
	r2, _ := NewRegistry(Options{StatePath: path, Log: observability.DiscardLogger()})
	if _, ns := r2.Snapshot(); len(ns) != 1 || ns[0].State != placement.StateDraining || ns[0].Zone != "a" {
		t.Fatalf("state not persisted: %+v", ns)
	}
}

func TestHTTPAndWatcher(t *testing.T) {
	r, _ := NewRegistry(Options{Log: observability.DiscardLogger()})
	mux := http.NewServeMux()
	r.Register(mux)
	srv := httptest.NewServer(mux)
	c := NewClient(srv.URL)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		if _, err := c.Heartbeat(ctx, placement.Node{ID: id, Addr: "http://" + id, Zone: id}); err != nil {
			t.Fatal(err)
		}
	}
	w := NewWatcher(c, placement.AlgoRing, observability.DiscardLogger())
	if err := w.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := w.Map().Place("k", 3); len(got) != 3 {
		t.Fatal("watcher map not usable")
	}
	if err := c.SetState(ctx, "b", placement.StateDraining); err != nil {
		t.Fatal(err)
	}
	if err := c.SetState(ctx, "zzz", placement.StateDraining); err == nil {
		t.Fatal("unknown node accepted")
	}
	srv.Close()
	// Coordinator gone: keep last snapshot.
	if err := w.Refresh(ctx); err == nil {
		t.Fatal("expected refresh error")
	}
	if len(w.Map().Nodes()) != 3 {
		t.Fatal("lost snapshot when coordinator unreachable")
	}
}

package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"

	"github.com/michael-emmanuel/objstore/internal/observability"
	"github.com/michael-emmanuel/objstore/internal/placement"
)

// Admin endpoints require a tenant with "admin": true.

func (g *Gateway) handleCluster(w http.ResponseWriter, r *http.Request) {
	m := g.members.Map()
	type nodeView struct {
		placement.Node
		UsedPct float64 `json:"used_pct"`
	}
	var out []nodeView
	for _, n := range m.Nodes() {
		pct := 0.0
		if n.CapacityBytes > 0 {
			pct = 100 * float64(n.UsedBytes) / float64(n.CapacityBytes)
		}
		out = append(out, nodeView{n, pct})
	}
	writeJSON(w, http.StatusOK, map[string]any{"epoch": m.Epoch, "nodes": out})
}

func (g *Gateway) handleNodeState(w http.ResponseWriter, r *http.Request) {
	if g.coord == nil {
		g.fail(w, r, errors.New("no coordinator configured"))
		return
	}
	var body struct {
		State placement.NodeState `json:"state"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 1024)).Decode(&body); err != nil {
		g.fail(w, r, errors.Join(errBadRequest, err))
		return
	}
	id := r.PathValue("id")
	if body.State == placement.StateDecommissioned {
		// Refuse to decommission a node that metadata still references:
		// that would turn a planned removal into an unplanned repair.
		n, err := g.meta.ReplicaCount(r.Context(), id)
		if err != nil {
			g.fail(w, r, err)
			return
		}
		if n > 0 && r.URL.Query().Get("force") != "true" {
			writeJSON(w, http.StatusConflict, map[string]any{"error": map[string]any{
				"code": "NodeNotDrained", "message": "node still holds referenced replicas; drain first or pass force=true",
				"replicas": n}})
			return
		}
	}
	if err := g.coord.SetState(r.Context(), id, body.State); err != nil {
		g.fail(w, r, errors.Join(errBadRequest, err))
		return
	}
	g.audit(r, principal{}, "set_node_state", "", "", "node", id, "state", body.State)
	w.WriteHeader(http.StatusNoContent)
}

func (g *Gateway) handleDrainStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	n, err := g.meta.ReplicaCount(r.Context(), id)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	node, _ := g.members.Map().Node(id)
	writeJSON(w, http.StatusOK, map[string]any{"node": id, "state": node.State, "referenced_replicas": n,
		"safe_to_remove": n == 0})
}

// handleInspect shows the full metadata record of an object: segments,
// blob IDs, checksums and replica locations, plus each replica node's state.
func (g *Gateway) handleInspect(w http.ResponseWriter, r *http.Request) {
	v, err := g.lookup(r.Context(), r.PathValue("bucket"), r.PathValue("key"), r.URL.Query().Get("version_id"))
	if err != nil {
		g.fail(w, r, err)
		return
	}
	m := g.members.Map()
	type rep struct {
		Node  string              `json:"node"`
		State placement.NodeState `json:"state"`
		Zone  string              `json:"zone"`
	}
	segs := []map[string]any{}
	for _, s := range v.Segments {
		var reps []rep
		for _, id := range s.Replicas {
			n, _ := m.Node(id)
			reps = append(reps, rep{id, n.State, n.Zone})
		}
		desired, _ := m.Place(s.BlobID, v.ReplicationFactor)
		var want []string
		for _, n := range desired {
			want = append(want, n.ID)
		}
		sort.Strings(want)
		segs = append(segs, map[string]any{"index": s.Index, "blob_id": s.BlobID, "offset": s.Offset, "size": s.Size,
			"sha256": s.SHA256, "replicas": reps, "desired_placement": want, "zones_spanned": m.DomainSpread(s.Replicas)})
	}
	v.Segments = nil
	writeJSON(w, http.StatusOK, map[string]any{"version": v, "segments": segs, "membership_epoch": m.Epoch})
}

func (g *Gateway) handleStats(w http.ResponseWriter, r *http.Request) {
	st, err := g.meta.Stats(r.Context())
	if err != nil {
		g.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"metadata_shards": st,
		"object_cache":    g.objCache.Stats(),
		"metadata_cache":  g.metaCache.Stats(),
		"latency_seconds": g.latencySummary(),
	})
}

func (g *Gateway) latencySummary() map[string]map[string]float64 {
	out := map[string]map[string]float64{}
	g.m.latency.Each(func(labels []string, h *observability.Histogram) {
		out[labels[0]] = map[string]float64{"count": float64(h.Count()), "p50": finite(h.Quantile(0.5)),
			"p95": finite(h.Quantile(0.95)), "p99": finite(h.Quantile(0.99))}
	})
	return out
}

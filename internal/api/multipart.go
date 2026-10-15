package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/michael-emmanuel/objstore/internal/metadata"
	"github.com/michael-emmanuel/objstore/internal/object"
)

// Multipart upload lifecycle:
//
//	POST /v1/buckets/{b}/multipart            -> upload_id   (metadata only)
//	POST /v1/multipart/{id}/parts?part_number=N  (repeatable, parallel, retryable)
//	POST /v1/multipart/{id}/complete          -> object becomes visible atomically
//	DELETE /v1/multipart/{id}                 -> abort; parts become garbage
//
// Each part is written as its own replicated blob. Completion does not copy
// data: it publishes a version whose segments ARE the part blobs. A failed
// 10 MB part of a 100 GB upload is retried alone.

type createUploadRequest struct {
	Key         string            `json:"key"`
	ContentType string            `json:"content_type"`
	Metadata    map[string]string `json:"metadata"`
}

func (g *Gateway) handleCreateUpload(w http.ResponseWriter, r *http.Request) {
	p, b, err := g.prologue(r, true)
	if err == nil && p.anonymous {
		err = errUnauthenticated
	}
	if err != nil {
		g.fail(w, r, err)
		return
	}
	var req createUploadRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 16<<10)).Decode(&req); err != nil {
		g.fail(w, r, errors.Join(errBadRequest, err))
		return
	}
	if req.ContentType == "" {
		req.ContentType = "application/octet-stream"
	}
	if err := object.ValidateKey(req.Key); err != nil {
		g.fail(w, r, err)
		return
	}
	if err := object.ValidateUserMeta(req.Metadata, req.ContentType); err != nil {
		g.fail(w, r, err)
		return
	}
	u := metadata.Upload{ID: b.Name + "~" + object.RandomID(16), Bucket: b.Name, Key: req.Key,
		ContentType: req.ContentType, UserMeta: req.Metadata}
	ctx, cancel := g.metaCtx(r.Context())
	defer cancel()
	if err := g.meta.CreateUpload(ctx, u); err != nil {
		g.fail(w, r, err)
		return
	}
	g.audit(r, p, "create_upload", b.Name, req.Key, "upload_id", u.ID)
	writeJSON(w, http.StatusOK, map[string]any{"upload_id": u.ID, "bucket": b.Name, "key": req.Key,
		"min_part_size": g.cfg.MinPartSize, "max_part_size": g.cfg.MaxPartSize})
}

// uploadPrologue authenticates and loads the upload, checking ownership.
func (g *Gateway) uploadPrologue(r *http.Request) (principal, metadata.Upload, error) {
	p, err := g.authenticate(r)
	if err == nil && p.anonymous {
		err = errUnauthenticated
	}
	if err == nil {
		err = g.admitTenant(p)
	}
	if err != nil {
		return p, metadata.Upload{}, err
	}
	ctx, cancel := g.metaCtx(r.Context())
	defer cancel()
	u, err := g.meta.GetUpload(ctx, r.PathValue("upload_id"))
	if err != nil {
		return p, u, err
	}
	if u.Tenant != p.tenant.ID && !p.tenant.Admin {
		return p, u, errAccessDenied
	}
	return p, u, nil
}

func (g *Gateway) handleUploadPart(w http.ResponseWriter, r *http.Request) {
	p, u, err := g.uploadPrologue(r)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	n, err := strconv.Atoi(r.URL.Query().Get("part_number"))
	if err != nil || n < 1 || n > metadata.MaxPartNumber {
		g.fail(w, r, fmt.Errorf("%w: part_number must be 1..%d", errBadRequest, metadata.MaxPartNumber))
		return
	}
	wantSHA := strings.ToLower(r.Header.Get("X-Checksum-SHA256"))
	if r.ContentLength > g.cfg.MaxPartSize {
		g.fail(w, r, fmt.Errorf("%w: part larger than %d bytes", errBadRequest, g.cfg.MaxPartSize))
		return
	}
	if r.ContentLength > 0 {
		if err := g.tenants.PreCheck(r.Context(), p.tenant, r.ContentLength); err != nil {
			g.fail(w, r, err)
			return
		}
	}
	release, err := g.acquireUpload(p)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	defer release()

	// A fresh attempt nonce per request: a retry never reuses a blob ID that
	// an earlier, possibly half-acknowledged attempt might have created.
	blobID := object.PartBlobID(u.Bucket, u.Key, u.ID, n, object.RandomID(8))
	body := g.uploadBody(w, r, p, g.cfg.MaxPartSize)
	res, err := g.writeBlob(r.Context(), u.Bucket, blobID, u.ReplicationFactor, body)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	if r.ContentLength >= 0 && res.Size != r.ContentLength {
		g.fail(w, r, fmt.Errorf("%w: received %d of %d bytes", errBadRequest, res.Size, r.ContentLength))
		return
	}
	if wantSHA != "" && wantSHA != res.SHA256 {
		g.fail(w, r, errBadDigest)
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), g.cfg.MetadataTimeout)
	defer cancel()
	if err := g.meta.PutPart(ctx, u.ID, metadata.Part{Number: n, BlobID: blobID, Size: res.Size, SHA256: res.SHA256, Replicas: res.Acked}); err != nil {
		g.fail(w, r, err)
		return
	}
	g.m.bytesIn.With().Add(float64(res.Size))
	w.Header().Set("ETag", `"`+res.SHA256+`"`)
	writeJSON(w, http.StatusOK, putResult{Bucket: u.Bucket, Key: u.Key, PartNumber: n, ETag: res.SHA256, Size: res.Size,
		ReplicationFactor: u.ReplicationFactor, WriteQuorum: g.writeQuorum(u.ReplicationFactor),
		MinReplicasAcked: len(res.Acked), UnderReplicated: len(res.Acked) < u.ReplicationFactor})
}

func (g *Gateway) handleGetUpload(w http.ResponseWriter, r *http.Request) {
	_, u, err := g.uploadPrologue(r)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	type part struct {
		Number int       `json:"part_number"`
		ETag   string    `json:"etag"`
		Size   int64     `json:"size"`
		At     time.Time `json:"uploaded_at"`
	}
	parts := []part{}
	for _, p := range u.Parts {
		parts = append(parts, part{p.Number, p.SHA256, p.Size, p.UploadedAt})
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i].Number < parts[j].Number })
	writeJSON(w, http.StatusOK, map[string]any{"upload_id": u.ID, "bucket": u.Bucket, "key": u.Key, "created_at": u.CreatedAt, "parts": parts})
}

type completeRequest struct {
	Parts []metadata.CompletePart `json:"parts"`
}

func (g *Gateway) handleCompleteUpload(w http.ResponseWriter, r *http.Request) {
	p, u, err := g.uploadPrologue(r)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	var req completeRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 2<<20)).Decode(&req); err != nil {
		g.fail(w, r, errors.Join(errBadRequest, err))
		return
	}
	ceiling, err := g.tenants.ShardCeiling(r.Context(), p.tenant, g.meta.ShardFor(u.Bucket))
	if err != nil {
		g.fail(w, r, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), g.cfg.MetadataTimeout)
	defer cancel()
	v, err := g.meta.CompleteUpload(ctx, u.ID, req.Parts, object.NewVersionID(time.Now()), metadata.CommitOptions{QuotaBytes: ceiling})
	if err != nil {
		g.fail(w, r, err)
		return
	}
	g.invalidate(v.Bucket, v.Key)
	p.tenant.Invalidate()
	g.m.objSize.With().Observe(float64(v.Size))
	g.audit(r, p, "complete_upload", v.Bucket, v.Key, "upload_id", u.ID, "version_id", v.VersionID, "size", v.Size)
	w.Header().Set("ETag", `"`+v.ETag+`"`)
	w.Header().Set("X-Version-Id", v.VersionID)
	writeJSON(w, http.StatusOK, map[string]any{"bucket": v.Bucket, "key": v.Key, "version_id": v.VersionID,
		"etag": v.ETag, "size": v.Size, "parts": len(v.Segments)})
}

func (g *Gateway) handleAbortUpload(w http.ResponseWriter, r *http.Request) {
	p, u, err := g.uploadPrologue(r)
	if err != nil {
		g.fail(w, r, err)
		return
	}
	ctx, cancel := g.metaCtx(r.Context())
	defer cancel()
	if err := g.meta.AbortUpload(ctx, u.ID); err != nil {
		g.fail(w, r, err)
		return
	}
	g.audit(r, p, "abort_upload", u.Bucket, u.Key, "upload_id", u.ID)
	w.WriteHeader(http.StatusNoContent)
}

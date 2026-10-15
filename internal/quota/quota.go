// Package quota enforces per-tenant limits at the gateway: request rate,
// upload/download bandwidth, concurrent transfers and stored bytes.
//
// Storage quota is the subtle one. Usage is tracked authoritatively by the
// metadata shards (updated atomically with each commit), but a tenant's
// buckets may live on several shards. The gateway therefore:
//
//  1. pre-checks Content-Length against cached global usage (cheap, early
//     rejection before any bytes are written), and
//  2. passes each shard a per-shard ceiling at commit time:
//     ceiling = quota - (usage on all OTHER shards, cached <= UsageTTL).
//
// The commit itself is atomic within a shard, so a single shard can never
// exceed its ceiling. Across shards, concurrent commits within the cache
// window can overshoot the global quota by at most the bytes committed
// elsewhere in that window. That is a deliberate "soft quota" trade-off:
// exact global enforcement would need a cross-shard transaction or a
// central counter on every PUT. docs/tradeoffs.md (Tenant quotas).
package quota

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/michael-emmanuel/objstore/internal/config"
	"github.com/michael-emmanuel/objstore/internal/ratelimit"
)

var (
	ErrQuotaExceeded  = errors.New("storage quota exceeded")
	ErrObjectTooLarge = errors.New("object exceeds tenant maximum object size")
	ErrRateLimited    = errors.New("request rate limit exceeded")
	ErrTooManyUploads = errors.New("too many concurrent uploads")
	ErrTooManyReads   = errors.New("too many concurrent downloads")
)

// UsageSource returns per-shard usage for a tenant.
type UsageSource interface {
	Usage(ctx context.Context, tenant string) ([]int64, error)
}

// Tenant holds the live limiter state for one tenant.
type Tenant struct {
	config.Tenant
	Requests  *ratelimit.Bucket
	Upload    *ratelimit.Bucket
	Download  *ratelimit.Bucket
	Uploads   *ratelimit.Semaphore
	Downloads *ratelimit.Semaphore

	mu       sync.Mutex
	usage    []int64
	usageAt  time.Time
	usageErr error
}

// Manager owns all tenants.
type Manager struct {
	byID     map[string]*Tenant
	byToken  map[string]*Tenant
	usage    UsageSource
	UsageTTL time.Duration
}

func NewManager(tenants []config.Tenant, usage UsageSource) *Manager {
	m := &Manager{byID: map[string]*Tenant{}, byToken: map[string]*Tenant{}, usage: usage, UsageTTL: 2 * time.Second}
	for _, ct := range tenants {
		l := ct.Limits
		burst := l.RequestBurst
		if burst <= 0 {
			burst = l.RequestsPerSec
		}
		t := &Tenant{
			Tenant:    ct,
			Requests:  ratelimit.NewBucket(l.RequestsPerSec, burst),
			Upload:    ratelimit.NewBucket(l.UploadBytesPerSec, l.UploadBytesPerSec/4),
			Download:  ratelimit.NewBucket(l.DownloadBytesPerSec, l.DownloadBytesPerSec/4),
			Uploads:   ratelimit.NewSemaphore(l.MaxConcurrentUploads),
			Downloads: ratelimit.NewSemaphore(l.MaxConcurrentDownloads),
		}
		m.byID[ct.ID] = t
		m.byToken[ct.TokenSHA256] = t
	}
	return m
}

// ByTokenHash authenticates a request.
func (m *Manager) ByTokenHash(h string) (*Tenant, bool) {
	t, ok := m.byToken[h]
	return t, ok
}

func (m *Manager) ByID(id string) (*Tenant, bool) {
	t, ok := m.byID[id]
	return t, ok
}

// AllowRequest charges one request token.
func (t *Tenant) AllowRequest() bool { return t.Requests.Allow(1) }

// CheckObjectSize enforces the per-object size limit when the size is known.
func (t *Tenant) CheckObjectSize(n int64) error {
	if t.Limits.MaxObjectBytes > 0 && n > t.Limits.MaxObjectBytes {
		return ErrObjectTooLarge
	}
	return nil
}

func (m *Manager) usageFor(ctx context.Context, t *Tenant) ([]int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if time.Since(t.usageAt) < m.UsageTTL && t.usage != nil {
		return t.usage, t.usageErr
	}
	u, err := m.usage.Usage(ctx, t.ID)
	t.usage, t.usageErr, t.usageAt = u, err, time.Now()
	return u, err
}

// Invalidate forces the next check to re-read usage (after a commit).
func (t *Tenant) Invalidate() {
	t.mu.Lock()
	t.usageAt = time.Time{}
	t.mu.Unlock()
}

// PreCheck rejects an upload of `adding` bytes that would obviously exceed
// the quota given current (cached) usage.
func (m *Manager) PreCheck(ctx context.Context, t *Tenant, adding int64) error {
	if t.Limits.MaxStorageBytes <= 0 {
		return nil
	}
	u, err := m.usageFor(ctx, t)
	if err != nil {
		return err
	}
	var total int64
	for _, x := range u {
		total += x
	}
	if total+adding > t.Limits.MaxStorageBytes {
		return ErrQuotaExceeded
	}
	return nil
}

// ShardCeiling returns the CommitOptions.QuotaBytes value for a commit on
// shard. 0 means unlimited.
func (m *Manager) ShardCeiling(ctx context.Context, t *Tenant, shard int) (int64, error) {
	if t.Limits.MaxStorageBytes <= 0 {
		return 0, nil
	}
	u, err := m.usageFor(ctx, t)
	if err != nil {
		return 0, err
	}
	other := int64(0)
	for i, x := range u {
		if i != shard {
			other += x
		}
	}
	c := t.Limits.MaxStorageBytes - other
	if c <= 0 {
		c = 1 // any positive commit will be rejected
	}
	return c, nil
}

// Usage returns total cached usage (for admin/metrics).
func (m *Manager) Usage(ctx context.Context, t *Tenant) (int64, error) {
	u, err := m.usageFor(ctx, t)
	var total int64
	for _, x := range u {
		total += x
	}
	return total, err
}

// Tenants lists tenants.
func (m *Manager) Tenants() []*Tenant {
	out := make([]*Tenant, 0, len(m.byID))
	for _, t := range m.byID {
		out = append(out, t)
	}
	return out
}

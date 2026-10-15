package quota

import (
	"context"
	"errors"
	"testing"

	"github.com/michael-emmanuel/objstore/internal/config"
)

type fakeUsage struct{ u []int64 }

func (f *fakeUsage) Usage(context.Context, string) ([]int64, error) { return f.u, nil }

func TestQuotaChecks(t *testing.T) {
	src := &fakeUsage{u: []int64{300, 500}}
	m := NewManager([]config.Tenant{{ID: "t", TokenSHA256: config.HashToken("secret-token-123456"),
		Limits: config.Limits{MaxStorageBytes: 1000, MaxObjectBytes: 100, RequestsPerSec: 2}}}, src)
	ten, ok := m.ByTokenHash(config.HashToken("secret-token-123456"))
	if !ok {
		t.Fatal("lookup by token failed")
	}
	ctx := context.Background()
	if err := m.PreCheck(ctx, ten, 200); err != nil {
		t.Fatal(err)
	}
	if err := m.PreCheck(ctx, ten, 201); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatal("precheck should reject")
	}
	if c, _ := m.ShardCeiling(ctx, ten, 0); c != 500 {
		t.Fatalf("shard 0 ceiling %d, want 1000-500", c)
	}
	if err := ten.CheckObjectSize(101); !errors.Is(err, ErrObjectTooLarge) {
		t.Fatal("object size limit")
	}
	if !ten.AllowRequest() || !ten.AllowRequest() || ten.AllowRequest() {
		t.Fatal("request rate limit not applied")
	}
	src.u = []int64{0, 0}
	if err := m.PreCheck(ctx, ten, 900); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatal("usage should be cached within TTL")
	}
	ten.Invalidate()
	if err := m.PreCheck(ctx, ten, 900); err != nil {
		t.Fatal("invalidate did not refresh usage")
	}
}

func TestResolveTenants(t *testing.T) {
	t.Setenv("TEST_TOKEN", "abcdefghijklmnopqrstuvwxyz")
	ts, err := config.ResolveTenants([]config.Tenant{{ID: "a", TokenEnv: "TEST_TOKEN"}})
	if err != nil || ts[0].TokenSHA256 != config.HashToken("abcdefghijklmnopqrstuvwxyz") {
		t.Fatal(err)
	}
	if _, err := config.ResolveTenants([]config.Tenant{{ID: "a"}}); err == nil {
		t.Fatal("tenant without token accepted")
	}
	t.Setenv("SHORT", "x")
	if _, err := config.ResolveTenants([]config.Tenant{{ID: "a", TokenEnv: "SHORT"}}); err == nil {
		t.Fatal("short token accepted")
	}
}

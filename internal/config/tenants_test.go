package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveTenantsFromEnv(t *testing.T) {
	t.Setenv("TEST_TOKEN_A", "token-a-0123456789abcdef")
	ts, err := ResolveTenants([]Tenant{{ID: "a", TokenEnv: "TEST_TOKEN_A"}})
	if err != nil {
		t.Fatal(err)
	}
	if ts[0].TokenSHA256 != HashToken("token-a-0123456789abcdef") {
		t.Fatal("token_env not resolved to its hash")
	}
}

func TestResolveTenantsRejectsUnsafeConfig(t *testing.T) {
	t.Setenv("SHORT", "short")
	t.Setenv("EMPTY", "")
	h := HashToken("some-token-0123456789")
	cases := map[string][]Tenant{
		"missing credential": {{ID: "a"}},
		"short token":        {{ID: "a", TokenEnv: "SHORT"}},
		"empty env":          {{ID: "a", TokenEnv: "EMPTY"}},
		"duplicate id":       {{ID: "a", TokenSHA256: h}, {ID: "a", TokenSHA256: HashToken("other-token-0123456789")}},
		"shared token":       {{ID: "a", TokenSHA256: h}, {ID: "b", TokenSHA256: h}},
		"bad id":             {{ID: "a/b", TokenSHA256: h}},
	}
	for name, in := range cases {
		if _, err := ResolveTenants(in); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

// The shipped example config must load and must never contain a plaintext
// token: only environment-variable names or hashes.
func TestShippedTenantsFileHasNoSecrets(t *testing.T) {
	path := filepath.Join("..", "..", "deploy", "tenants.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Skip("deploy/tenants.json not present")
	}
	if strings.Contains(string(b), `"token"`) {
		t.Fatal("tenants.json contains a plaintext token field")
	}
	for _, env := range []string{"OBJSTORE_TOKEN_ALICE", "OBJSTORE_TOKEN_BOB", "OBJSTORE_TOKEN_ADMIN"} {
		t.Setenv(env, env+"-value-for-test")
	}
	if _, err := LoadTenants(path); err != nil {
		t.Fatal(err)
	}
}

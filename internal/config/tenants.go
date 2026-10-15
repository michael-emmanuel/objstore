// Package config loads tenant definitions and shared flag helpers.
//
// Secrets policy: tenant files contain only SHA-256 hashes of API tokens, or
// the NAME of an environment variable holding the token. No token is ever
// written to the repository or to logs.
package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// Limits are per-tenant resource limits. Zero means unlimited.
type Limits struct {
	MaxStorageBytes        int64   `json:"max_storage_bytes"`
	MaxObjectBytes         int64   `json:"max_object_bytes"`
	UploadBytesPerSec      float64 `json:"upload_bytes_per_sec"`
	DownloadBytesPerSec    float64 `json:"download_bytes_per_sec"`
	RequestsPerSec         float64 `json:"requests_per_sec"`
	RequestBurst           float64 `json:"request_burst"`
	MaxConcurrentUploads   int     `json:"max_concurrent_uploads"`
	MaxConcurrentDownloads int     `json:"max_concurrent_downloads"`
}

// Tenant is an isolated customer. Buckets belong to exactly one tenant.
type Tenant struct {
	ID          string `json:"id"`
	TokenSHA256 string `json:"token_sha256,omitempty"`
	TokenEnv    string `json:"token_env,omitempty"`
	Admin       bool   `json:"admin,omitempty"`
	Limits      Limits `json:"limits"`
}

// HashToken returns the hex SHA-256 of a token.
func HashToken(tok string) string {
	s := sha256.Sum256([]byte(tok))
	return hex.EncodeToString(s[:])
}

// LoadTenants reads a JSON array of tenants and resolves token_env entries.
func LoadTenants(path string) ([]Tenant, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var ts []Tenant
	if err := json.Unmarshal(b, &ts); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return ResolveTenants(ts)
}

// ResolveTenants validates tenants and turns token_env into token_sha256.
func ResolveTenants(ts []Tenant) ([]Tenant, error) {
	seen := map[string]bool{}
	hashes := map[string]bool{}
	for i := range ts {
		t := &ts[i]
		if t.ID == "" || strings.ContainsAny(t.ID, " /") {
			return nil, fmt.Errorf("tenant %d: invalid id %q", i, t.ID)
		}
		if seen[t.ID] {
			return nil, fmt.Errorf("duplicate tenant %q", t.ID)
		}
		seen[t.ID] = true
		if t.TokenEnv != "" {
			tok := os.Getenv(t.TokenEnv)
			if tok == "" {
				return nil, fmt.Errorf("tenant %q: environment variable %s is empty", t.ID, t.TokenEnv)
			}
			if len(tok) < 16 {
				return nil, fmt.Errorf("tenant %q: token too short (min 16 chars)", t.ID)
			}
			t.TokenSHA256 = HashToken(tok)
		}
		if len(t.TokenSHA256) != 64 {
			return nil, fmt.Errorf("tenant %q: needs token_sha256 or token_env", t.ID)
		}
		if hashes[t.TokenSHA256] {
			return nil, errors.New("two tenants share a token")
		}
		hashes[t.TokenSHA256] = true
	}
	return ts, nil
}

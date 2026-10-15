// Package clusterauth authenticates cluster-internal traffic (gateway,
// repairer, rebalancer and GC talking to storage nodes, metadata shards and
// the coordinator) with a shared secret.
//
// Storage nodes have no notion of tenants: anything that can reach a node's
// data-plane port can read or delete any blob. Without this check, network
// reachability would be the only access control on the data plane.
//
// This is deliberately the minimum viable mechanism, and its limits are
// documented in docs/architecture.md ("Security"):
//
//   - One token for the whole cluster: no per-service identity, so a
//     compromised gateway can impersonate a repairer. Production would use
//     mTLS with per-service certificates (SPIFFE-style identities) and
//     authorize by caller role.
//   - The token travels in a header; without TLS on internal links it is
//     visible to anyone who can sniff the network. All binaries accept
//     -tls-cert/-tls-key.
//
// The token is read from the CLUSTER_TOKEN environment variable and is never
// logged or stored on disk by this code.
package clusterauth

import (
	"crypto/subtle"
	"net/http"
)

// Header carries the shared cluster token.
const Header = "X-Cluster-Token"

// EnvVar names the environment variable that holds the token.
const EnvVar = "CLUSTER_TOKEN"

// MinTokenLen rejects trivially guessable tokens at startup.
const MinTokenLen = 16

// Middleware rejects requests that do not carry the token. Liveness and
// metrics endpoints stay open so that load balancers and Prometheus do not
// need the secret. An empty token disables the check (local development
// only; binaries log a warning).
func Middleware(token string, next http.Handler) http.Handler {
	if token == "" {
		return next
	}
	want := []byte(token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && (r.URL.Path == "/healthz" || r.URL.Path == "/metrics") {
			next.ServeHTTP(w, r)
			return
		}
		got := []byte(r.Header.Get(Header))
		if subtle.ConstantTimeCompare(got, want) != 1 {
			http.Error(w, "cluster authentication required", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// Transport adds the token to every outgoing request.
type Transport struct {
	Token string
	Base  http.RoundTripper
}

func (t *Transport) RoundTrip(r *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	if t.Token == "" {
		return base.RoundTrip(r)
	}
	// RoundTrippers must not mutate the caller's request.
	r2 := r.Clone(r.Context())
	r2.Header.Set(Header, t.Token)
	return base.RoundTrip(r2)
}

// Wrap returns a shallow copy of hc whose transport adds the token.
func Wrap(hc *http.Client, token string) *http.Client {
	if hc == nil {
		hc = &http.Client{}
	}
	c := *hc
	c.Transport = &Transport{Token: token, Base: hc.Transport}
	return &c
}

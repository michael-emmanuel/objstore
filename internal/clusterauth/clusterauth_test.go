package clusterauth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMiddlewareAndTransport(t *testing.T) {
	const tok = "0123456789abcdef-secret"
	h := Middleware(tok, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	srv := httptest.NewServer(h)
	defer srv.Close()

	status := func(c *http.Client, method, path string) int {
		req, _ := http.NewRequest(method, srv.URL+path, nil)
		resp, err := c.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	plain := srv.Client()
	authed := Wrap(srv.Client(), tok)
	wrong := Wrap(srv.Client(), "wrong-token-wrong-token")

	if got := status(plain, "GET", "/blobs/x"); got != http.StatusUnauthorized {
		t.Fatalf("unauthenticated data-plane request: %d", got)
	}
	if got := status(wrong, "DELETE", "/blobs/x"); got != http.StatusUnauthorized {
		t.Fatalf("wrong token: %d", got)
	}
	if got := status(authed, "DELETE", "/blobs/x"); got != http.StatusNoContent {
		t.Fatalf("authenticated request: %d", got)
	}
	for _, p := range []string{"/healthz", "/metrics"} {
		if got := status(plain, "GET", p); got != http.StatusNoContent {
			t.Fatalf("%s should be open: %d", p, got)
		}
	}
	// /healthz is only exempt for GET.
	if got := status(plain, "POST", "/healthz"); got != http.StatusUnauthorized {
		t.Fatalf("POST /healthz: %d", got)
	}
}

func TestEmptyTokenDisablesCheck(t *testing.T) {
	h := Middleware("", http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/blobs/x", nil))
	if rec.Code != http.StatusOK {
		t.Fatal(rec.Code)
	}
}

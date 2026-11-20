package k8sauth

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestTokenSource_EnvOverride(t *testing.T) {
	dir := t.TempDir()
	envTokenFile := filepath.Join(dir, "env-token")
	if err := os.WriteFile(envTokenFile, []byte("env-sa-token"), 0o600); err != nil {
		t.Fatalf("failed to write env token: %v", err)
	}

	t.Setenv("AERIAL_SA_TOKEN_PATH", envTokenFile)
	ts := NewTokenSource("", nil)
	tok, err := ts.Token()
	if err != nil {
		t.Fatalf("Token() error = %v", err)
	}
	if tok != "env-sa-token" {
		t.Fatalf("Token() = %q, want %q", tok, "env-sa-token")
	}
}

func TestServiceAccountTransport_RoundTrip(t *testing.T) {
	t.Parallel()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("valid-sa-jwt"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	rt := &staticRoundTripper{status: http.StatusOK, body: "{}"}
	transport := NewServiceAccountTransport(rt, tokenFile)

	req, _ := http.NewRequest(http.MethodGet, "http://internal.service/test", http.NoBody)
	resp, err := transport.RoundTrip(req)
	if err != nil {
		t.Fatalf("RoundTrip() error = %v", err)
	}
	defer resp.Body.Close()

	if got := rt.captured.Header.Get("Authorization"); got != "Bearer valid-sa-jwt" {
		t.Fatalf("Authorization header = %q, want %q", got, "Bearer valid-sa-jwt")
	}
}

func TestServiceAccountTransport_TokenError(t *testing.T) {
	t.Parallel()

	tokenFile := filepath.Join(t.TempDir(), "missing-token")
	rt := &staticRoundTripper{status: http.StatusOK, body: "{}"}
	transport := NewServiceAccountTransport(rt, tokenFile)

	req, _ := http.NewRequest(http.MethodGet, "http://internal.service/test", http.NoBody)
	resp, err := transport.RoundTrip(req)
	if err == nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		t.Fatalf("expected error from RoundTrip(), got nil")
	}
}

func TestServiceAccountTransport_DefaultBase(t *testing.T) {
	t.Parallel()

	var capturedAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("default-base-token"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}

	transport := NewServiceAccountTransport(nil, tokenFile)
	client := &http.Client{Transport: transport}

	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("client.Get() error = %v", err)
	}
	defer resp.Body.Close()

	if capturedAuth != "Bearer default-base-token" {
		t.Fatalf("Authorization header = %q, want %q", capturedAuth, "Bearer default-base-token")
	}
}

type idleCloseRecorder struct {
	closed bool
}

func (*idleCloseRecorder) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, nil
}

func (r *idleCloseRecorder) CloseIdleConnections() {
	r.closed = true
}

func TestServiceAccountTransport_CloseIdleConnections(t *testing.T) {
	t.Parallel()

	recorder := &idleCloseRecorder{}
	transport := NewServiceAccountTransport(recorder, "/nonexistent")
	client := &http.Client{Transport: transport}
	client.CloseIdleConnections()

	if !recorder.closed {
		t.Fatalf("expected base transport CloseIdleConnections() to be called")
	}
}

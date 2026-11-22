package k8sauth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeTokenFile writes content to path, failing the test on error.
func writeTokenFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write token file: %v", err)
	}
}

// tokenErrText returns err's text with path replaced by TOKEN, or "" for a
// nil error.
func tokenErrText(err error, path string) string {
	if err == nil {
		return ""
	}
	return strings.ReplaceAll(err.Error(), path, "TOKEN")
}

func TestTokenSource(t *testing.T) {
	// Not parallel: the env row sets AERIAL_SA_TOKEN_PATH.
	tests := []struct {
		name     string
		initial  string
		viaEnv   bool
		nilClock bool
		update   string
		advance  time.Duration
		want     string
		wantErr  string
	}{
		{name: "initial read with nil clock", initial: "tok-1", nilClock: true, want: "tok-1"},
		{name: "cached within reload ttl", initial: "tok-1", update: "tok-2", advance: 30 * time.Second, want: "tok-1"},
		{name: "reloaded after reload ttl", initial: "tok-1", update: "tok-2", advance: 61 * time.Second, want: "tok-2"},
		{name: "empty path reads AERIAL_SA_TOKEN_PATH", initial: "env-sa-token", viaEnv: true, want: "env-sa-token"},
		{name: "missing file", wantErr: "read serviceaccount token from TOKEN: open TOKEN: no such file or directory"},
		{name: "whitespace-only file", initial: "  \t\n  ", wantErr: "serviceaccount token at TOKEN is empty"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "token")
			if tc.initial != "" {
				writeTokenFile(t, path, tc.initial)
			}
			now := time.Date(2026, 11, 20, 1, 0, 0, 0, time.UTC)
			clock := func() time.Time { return now }
			if tc.nilClock {
				clock = nil
			}
			arg := path
			if tc.viaEnv {
				t.Setenv("AERIAL_SA_TOKEN_PATH", path)
				arg = ""
			}

			ts := NewTokenSource(arg, clock)
			got, err := ts.Token()
			if tc.update != "" {
				if err != nil || got != tc.initial {
					t.Fatalf("initial Token() = (%q, %v), want (%q, nil)", got, err, tc.initial)
				}
				writeTokenFile(t, path, tc.update)
				now = now.Add(tc.advance)
				got, err = ts.Token()
			}
			if gotErr := tokenErrText(err, path); got != tc.want || gotErr != tc.wantErr {
				t.Fatalf("Token() = (%q, %q), want (%q, %q)", got, gotErr, tc.want, tc.wantErr)
			}
		})
	}
}

func TestServiceAccountTransport_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		token    string
		base     http.RoundTripper
		wantAuth string
		wantErr  string
	}{
		{name: "explicit base gets bearer header", token: "valid-sa-jwt", base: &http.Transport{}, wantAuth: "Bearer valid-sa-jwt"},
		{name: "nil base uses default transport", token: "default-base-token", wantAuth: "Bearer default-base-token"},
		{
			name: "missing token fails before sending", base: &http.Transport{},
			wantErr: "k8sauth: obtain serviceaccount token: read serviceaccount token from TOKEN: open TOKEN: no such file or directory",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			gotAuth := make(chan string, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotAuth <- r.Header.Get("Authorization")
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			path := filepath.Join(t.TempDir(), "token")
			if tc.token != "" {
				writeTokenFile(t, path, tc.token)
			}
			transport := NewServiceAccountTransport(tc.base, path)
			defer (&http.Client{Transport: transport}).CloseIdleConnections()

			req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, srv.URL, http.NoBody)
			if err != nil {
				t.Fatalf("NewRequest: %v", err)
			}
			resp, err := transport.RoundTrip(req)
			if err == nil {
				_ = resp.Body.Close()
			}
			if gotErr := tokenErrText(err, path); gotErr != tc.wantErr {
				t.Fatalf("RoundTrip() error = %q, want %q", gotErr, tc.wantErr)
			}
			auth := ""
			select {
			case auth = <-gotAuth:
			default:
			}
			if auth != tc.wantAuth {
				t.Fatalf("Authorization header = %q, want %q", auth, tc.wantAuth)
			}
		})
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

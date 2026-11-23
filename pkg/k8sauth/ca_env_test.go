package k8sauth

import (
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestWriteError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantBody   string
	}{
		{name: "validation error is 400", err: &ValidationError{Message: "bad target"}, wantStatus: http.StatusBadRequest, wantBody: "{\"error\":\"bad target\"}\n"},
		{name: "unauthorized error is 401", err: &UnauthorizedError{Message: "no token"}, wantStatus: http.StatusUnauthorized, wantBody: "{\"error\":\"no token\"}\n"},
		{name: "forbidden error is 403", err: &ForbiddenError{Message: "denied"}, wantStatus: http.StatusForbidden, wantBody: "{\"error\":\"denied\"}\n"},
		{name: "wrapped forbidden error is 403", err: fmt.Errorf("authorize: %w", &ForbiddenError{Message: "denied"}), wantStatus: http.StatusForbidden, wantBody: "{\"error\":\"authorize: denied\"}\n"},
		{name: "plain error is 500 with generic body", err: errors.New("token review: connection refused"), wantStatus: http.StatusInternalServerError, wantBody: "{\"error\":\"internal error\"}\n"},
		{name: "wrapped plain error is 500 with generic body", err: fmt.Errorf("authorize: %w", errors.New("boom")), wantStatus: http.StatusInternalServerError, wantBody: "{\"error\":\"internal error\"}\n"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			WriteError(rec, tc.err)
			if rec.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if got := rec.Header().Get("Content-Type"); got != "application/json" {
				t.Errorf("Content-Type = %q, want application/json", got)
			}
			if got := rec.Body.String(); got != tc.wantBody {
				t.Errorf("body = %q, want %q", got, tc.wantBody)
			}
		})
	}
}

func TestApplyEnvOverride(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		env         string
		start       bool
		wantEnabled bool
		wantErr     bool
	}{
		{name: "empty leaves enabled config alone", env: "", start: true, wantEnabled: true},
		{name: "blank leaves disabled config alone", env: "  ", start: false, wantEnabled: false},
		{name: "true enables", env: "true", start: false, wantEnabled: true},
		{name: "FALSE disables", env: "FALSE", start: true, wantEnabled: false},
		{name: "bad value errors and keeps config", env: "maybe", start: true, wantEnabled: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{Enabled: tc.start}
			getenv := func(key string) string {
				if key == "K8S_AUTH_ENABLED" {
					return tc.env
				}
				return ""
			}
			err := ApplyEnvOverride(&cfg, getenv)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if cfg.Enabled != tc.wantEnabled {
				t.Errorf("Enabled = %v, want %v", cfg.Enabled, tc.wantEnabled)
			}
		})
	}
}

func writeTestCA(t *testing.T, dir, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func TestLoadCAPool(t *testing.T) {
	t.Parallel()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(srv.Close)
	validPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	dir := t.TempDir()

	tests := []struct {
		name       string
		path       string
		wantPool   bool
		wantTrusts bool
	}{
		{name: "empty path", path: ""},
		{name: "missing file", path: filepath.Join(dir, "missing.crt")},
		{name: "empty file", path: writeTestCA(t, dir, "empty.crt", nil)},
		{name: "garbage file", path: writeTestCA(t, dir, "garbage.crt", []byte("not pem"))},
		{name: "valid pem", path: writeTestCA(t, dir, "valid.crt", validPEM), wantPool: true, wantTrusts: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := LoadCAPool(tc.path)
			if (pool != nil) != tc.wantPool {
				t.Fatalf("pool non-nil = %v, want %v", pool != nil, tc.wantPool)
			}
			if pool == nil {
				return
			}
			_, err := srv.Certificate().Verify(x509.VerifyOptions{Roots: pool})
			if (err == nil) != tc.wantTrusts {
				t.Errorf("verify err = %v, wantTrusts %v", err, tc.wantTrusts)
			}
		})
	}
}

func TestDefaultK8sHTTPClient(t *testing.T) {
	t.Parallel()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(srv.Close)
	validPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	validDir := t.TempDir()
	writeTestCA(t, validDir, "ca.crt", validPEM)
	garbageDir := t.TempDir()
	writeTestCA(t, garbageDir, "ca.crt", []byte("not pem"))

	tests := []struct {
		name        string
		tokenPath   string
		wantRootCAs bool
	}{
		{name: "token dir ca.crt is trusted", tokenPath: filepath.Join(validDir, "token"), wantRootCAs: true},
		{name: "garbage token dir ca.crt leaves default roots", tokenPath: filepath.Join(garbageDir, "token")},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := defaultK8sHTTPClient(tc.tokenPath)
			if client.Timeout != 10*time.Second {
				t.Errorf("Timeout = %v, want 10s", client.Timeout)
			}
			transport, ok := client.Transport.(*http.Transport)
			if !ok {
				t.Fatalf("transport type = %T", client.Transport)
			}
			gotRootCAs := transport.TLSClientConfig != nil && transport.TLSClientConfig.RootCAs != nil
			if gotRootCAs != tc.wantRootCAs {
				t.Fatalf("RootCAs set = %v, want %v", gotRootCAs, tc.wantRootCAs)
			}
			if !tc.wantRootCAs {
				return
			}
			if _, err := srv.Certificate().Verify(x509.VerifyOptions{Roots: transport.TLSClientConfig.RootCAs}); err != nil {
				t.Errorf("cluster CA not trusted: %v", err)
			}
		})
	}
}

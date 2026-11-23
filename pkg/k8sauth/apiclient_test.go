package k8sauth

import (
	"crypto/tls"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func TestNewAPIServerClient(t *testing.T) {
	t.Parallel()

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	t.Cleanup(srv.Close)
	validPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	dir := t.TempDir()
	validCA := writeTestCA(t, dir, "valid.crt", validPEM)
	garbageCA := writeTestCA(t, dir, "garbage.crt", []byte("not pem"))
	base := srv.Client()
	base.Timeout = 7 * time.Second

	tests := []struct {
		name          string
		apiServer     string
		base          *http.Client
		caPath        string
		wantSameBase  bool
		wantTimeout   time.Duration
		wantTransport bool
		wantRootCAs   bool
	}{
		{name: "http server returns base", apiServer: "http://kube.test", base: base, caPath: validCA, wantSameBase: true, wantTimeout: 7 * time.Second},
		{name: "http server nil base gets 15s client", apiServer: "http://kube.test", wantTimeout: 15 * time.Second},
		{name: "empty server nil base gets 15s client", apiServer: "", wantTimeout: 15 * time.Second},
		{name: "https server keeps base timeout and trusts CA", apiServer: srv.URL, base: base, caPath: validCA, wantTimeout: 7 * time.Second, wantTransport: true, wantRootCAs: true},
		{name: "https server nil base gets 15s with CA", apiServer: srv.URL, caPath: validCA, wantTimeout: 15 * time.Second, wantTransport: true, wantRootCAs: true},
		{name: "https server garbage CA leaves default roots", apiServer: srv.URL, base: base, caPath: garbageCA, wantTimeout: 7 * time.Second, wantTransport: true},
		{name: "https server missing CA leaves default roots", apiServer: srv.URL, caPath: filepath.Join(dir, "missing.crt"), wantTimeout: 15 * time.Second, wantTransport: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := NewAPIServerClient(tc.apiServer, tc.base, tc.caPath)
			if (client == tc.base) != tc.wantSameBase {
				t.Fatalf("returned base = %v, want %v", client == tc.base, tc.wantSameBase)
			}
			if client.Timeout != tc.wantTimeout {
				t.Errorf("Timeout = %v, want %v", client.Timeout, tc.wantTimeout)
			}
			if tc.wantSameBase {
				return
			}
			transport, ok := client.Transport.(*http.Transport)
			if ok != tc.wantTransport {
				t.Fatalf("dedicated transport = %v (%T), want %v", ok, client.Transport, tc.wantTransport)
			}
			if !ok {
				return
			}
			if transport.Proxy == nil || transport.DialContext == nil || transport.IdleConnTimeout != 90*time.Second {
				t.Errorf("transport proxy/dial/idle = %v/%v/%v, want set/set/90s", transport.Proxy != nil, transport.DialContext != nil, transport.IdleConnTimeout)
			}
			gotRootCAs := transport.TLSClientConfig != nil && transport.TLSClientConfig.RootCAs != nil
			if gotRootCAs != tc.wantRootCAs {
				t.Fatalf("RootCAs set = %v, want %v", gotRootCAs, tc.wantRootCAs)
			}
			if !tc.wantRootCAs {
				return
			}
			assertTrustedTLSClient(t, client, transport, srv.URL)
		})
	}
}

func assertTrustedTLSClient(t *testing.T, client *http.Client, transport *http.Transport, url string) {
	t.Helper()
	if transport.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion = %x, want TLS 1.2", transport.TLSClientConfig.MinVersion)
	}
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET trusted server: %v", err)
	}
	resp.Body.Close()
	client.CloseIdleConnections()
	if resp.StatusCode != http.StatusNoContent {
		t.Errorf("status = %d, want 204", resp.StatusCode)
	}
}

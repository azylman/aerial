package main

import (
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoadK8sCertPool(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	emptyFile := filepath.Join(tempDir, "empty.crt")
	_ = os.WriteFile(emptyFile, []byte(""), 0o644)
	invalidFile := filepath.Join(tempDir, "invalid.crt")
	_ = os.WriteFile(invalidFile, []byte("not pem"), 0o644)

	mockTLSServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(mockTLSServer.Close)
	validPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: mockTLSServer.Certificate().Raw,
	})
	validFile := filepath.Join(tempDir, "valid.crt")
	_ = os.WriteFile(validFile, validPEM, 0o644)

	tests := []struct {
		name       string
		path       string
		wantLoaded bool
	}{
		{name: "empty path returns nil", path: "", wantLoaded: false},
		{name: "nonexistent file returns nil", path: filepath.Join(tempDir, "nonexistent.crt"), wantLoaded: false},
		{name: "empty file returns nil", path: emptyFile, wantLoaded: false},
		{name: "invalid pem returns nil", path: invalidFile, wantLoaded: false},
		{name: "valid pem returns cert pool", path: validFile, wantLoaded: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := loadK8sCertPool(tc.path)
			if tc.wantLoaded && pool == nil {
				t.Errorf("expected non-nil cert pool for %s", tc.path)
			}
			if !tc.wantLoaded && pool != nil {
				t.Errorf("expected nil cert pool for %s", tc.path)
			}
		})
	}
}

func TestGetKubeServiceAccountToken(t *testing.T) {
	tempDir := t.TempDir()
	tokenFile := filepath.Join(tempDir, "sa-token")
	_ = os.WriteFile(tokenFile, []byte("  projected-sa-token  \n"), 0o600)

	origTokenPath := kubeTokenPath
	t.Cleanup(func() { kubeTokenPath = origTokenPath })

	tests := []struct {
		name          string
		explicitToken string
		tokenPath     string
		wantToken     string
	}{
		{name: "returns explicit token when set", explicitToken: "explicit-token", tokenPath: tokenFile, wantToken: "explicit-token"},
		{name: "reads from token file when explicit token empty", explicitToken: "", tokenPath: tokenFile, wantToken: "projected-sa-token"},
		{name: "returns empty string when token file missing", explicitToken: "", tokenPath: filepath.Join(tempDir, "missing-token"), wantToken: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kubeTokenPath = tc.tokenPath
			got := getKubeServiceAccountToken(tc.explicitToken)
			if got != tc.wantToken {
				t.Errorf("getKubeServiceAccountToken() = %q, want %q", got, tc.wantToken)
			}
		})
	}
}

func TestGetK8sHTTPClient(t *testing.T) {
	tempDir := t.TempDir()
	mockTLSServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(mockTLSServer.Close)
	validPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: mockTLSServer.Certificate().Raw,
	})
	validCAFile := filepath.Join(tempDir, "ca.crt")
	_ = os.WriteFile(validCAFile, validPEM, 0o644)

	origCAPath := kubeCAPath
	t.Cleanup(func() { kubeCAPath = origCAPath })

	tests := []struct {
		name          string
		apiServer     string
		caPath        string
		hasHTTPClient bool
		wantCustom    bool
		wantRootCA    bool
	}{
		{
			name:          "custom client reused for http endpoint",
			apiServer:     "http://localhost:8080",
			caPath:        validCAFile,
			hasHTTPClient: true,
			wantCustom:    true,
		},
		{
			name:          "https endpoint builds client with root CA",
			apiServer:     "https://kubernetes.default.svc",
			caPath:        validCAFile,
			hasHTTPClient: true,
			wantRootCA:    true,
		},
		{
			name:          "https endpoint without root CA file builds transport without root CA",
			apiServer:     "https://kubernetes.default.svc",
			caPath:        filepath.Join(tempDir, "missing.crt"),
			hasHTTPClient: false,
			wantRootCA:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			kubeCAPath = tc.caPath
			var customClient *http.Client
			if tc.hasHTTPClient {
				customClient = &http.Client{Transport: &zeroCallTransport{}, Timeout: 42 * time.Millisecond}
			}
			client := getK8sHTTPClient(tc.apiServer, customClient)
			if tc.wantCustom {
				if client != customClient {
					t.Fatalf("expected custom client to be returned, got %v", client)
				}
				return
			}
			tr, ok := client.Transport.(*http.Transport)
			if !ok || tr == nil {
				t.Fatalf("expected *http.Transport, got %T", client.Transport)
			}
			hasRootCA := tr.TLSClientConfig != nil && tr.TLSClientConfig.RootCAs != nil
			if hasRootCA != tc.wantRootCA {
				t.Errorf("hasRootCA = %v, want %v", hasRootCA, tc.wantRootCA)
			}
		})
	}
}

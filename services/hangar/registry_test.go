package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEnsureDockerAuth_TableDriven(t *testing.T) {
	dummyPAT := "ghp_mock_token_12345"

	t.Run("nil_and_empty_daemon", func(t *testing.T) {
		var nilDaemon *SyncDaemon
		for _, err := range []error{nilDaemon.EnsureDockerAuth(), (&SyncDaemon{pat: ""}).EnsureDockerAuth(), NewDaemon(DaemonConfig{PAT: ""}).EnsureDockerAuth()} {
			if err != nil {
				t.Errorf("expected nil error for empty/nil daemon, got %v", err)
			}
		}
	})

	t.Run("real_fs_callbacks", func(t *testing.T) {
		tmpDir := t.TempDir()
		lookup := func(k string) string {
			if k == "DOCKER_CONFIG" {
				return filepath.Join(tmpDir, ".docker")
			}
			return ""
		}
		if err := EnsureDockerAuthPath(dummyPAT, "", lookup, nil, nil, nil); err != nil {
			t.Fatalf("expected success writing docker config, got %v", err)
		}
		if content, err := os.ReadFile(filepath.Join(tmpDir, ".docker", "config.json")); err != nil || !strings.Contains(string(content), "ghcr.io") {
			t.Errorf("expected docker config with ghcr.io, got %s, err: %v", string(content), err)
		}
	})

	t.Run("daemon_EnsureDockerAuth_fs", func(t *testing.T) {
		tempDir := t.TempDir()
		t.Setenv("DOCKER_CONFIG", tempDir)
		d := &SyncDaemon{pat: dummyPAT, configDir: "/share/aerial-config", repoUrls: map[string]string{"/share/aerial-config": "https://github.com/azylman/aerial-config.git"}}
		if err := d.EnsureDockerAuth(); err != nil {
			t.Fatalf("daemon.EnsureDockerAuth failed: %v", err)
		}
		if data, err := os.ReadFile(filepath.Join(tempDir, "config.json")); err != nil || !strings.Contains(string(data), base64.StdEncoding.EncodeToString([]byte("azylman:"+dummyPAT))) {
			t.Errorf("expected valid auth in written config, got %s, err: %v", string(data), err)
		}
	})

	t.Run("mock_callbacks_table", func(t *testing.T) {
		tests := []struct {
			name, pat, repoURL, wantErrSub, wantAuthUser string
			mkdirErr, readErr, writeErr                  error
		}{
			{name: "empty PAT in EnsureDockerAuthPath is no-op", pat: "", repoURL: "https://github.com/azylman/aerial-config.git"},
			{name: "mkdir failure returns error", pat: dummyPAT, repoURL: "https://github.com/azylman/aerial-config.git", mkdirErr: errors.New("disk permission denied"), wantErrSub: "disk permission denied"},
			{name: "read error other than not-exist returns error", pat: dummyPAT, repoURL: "https://github.com/azylman/aerial-config.git", readErr: errors.New("read fault"), wantErrSub: "read fault"},
			{name: "write failure returns error", pat: dummyPAT, repoURL: "https://github.com/azylman/aerial-config.git", readErr: os.ErrNotExist, writeErr: errors.New("write fault"), wantErrSub: "write fault"},
			{name: "successful write creates valid docker config file with 0600 mode", pat: dummyPAT, repoURL: "https://github.com/custom-org/aerial-config.git", readErr: os.ErrNotExist, wantAuthUser: "custom-org"},
		}

		for _, tc := range tests {
			t.Run(tc.name, func(t *testing.T) {
				var writtenPath string
				var writtenData []byte
				var writtenMode os.FileMode
				err := EnsureDockerAuthPath(
					tc.pat,
					tc.repoURL,
					func(k string) string {
						if k == "DOCKER_CONFIG" {
							return "/mock/docker"
						}
						return ""
					},
					func(string) ([]byte, error) { return nil, tc.readErr },
					func(path string, data []byte, mode os.FileMode) error {
						writtenPath, writtenData, writtenMode = path, data, mode
						return tc.writeErr
					},
					func(string, os.FileMode) error { return tc.mkdirErr },
				)
				if tc.wantErrSub != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
						t.Fatalf("expected error containing %q, got %v", tc.wantErrSub, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if tc.wantAuthUser != "" {
					if filepath.ToSlash(writtenPath) != "/mock/docker/config.json" || writtenMode != 0o600 {
						t.Errorf("expected /mock/docker/config.json and 0600, got %s and %v", writtenPath, writtenMode)
					}
					var parsed struct {
						Auths map[string]struct {
							Auth string `json:"auth"`
						} `json:"auths"`
					}
					if err := json.Unmarshal(writtenData, &parsed); err != nil {
						t.Fatalf("failed to unmarshal written config: %v", err)
					}
					if entry, ok := parsed.Auths["ghcr.io"]; !ok || entry.Auth != base64.StdEncoding.EncodeToString([]byte(tc.wantAuthUser+":"+tc.pat)) {
						t.Errorf("unexpected ghcr auth entry: %+v", entry)
					}
				}
			})
		}
	})
}

type mockOCIOptions struct {
	ProbeErr     error
	ProbeStatus  int
	ProbeHeaders map[string]string

	ChallengeAuthHeader string

	TokenErr      error
	TokenStatus   int
	TokenBody     string
	ExpectedPAT   string
	ExpectedScope string

	AuthErr        error
	AuthStatus     int
	AuthHeaders    map[string]string
	ExpectedBearer string
}

func newMockOCIRegistryClient(opts mockOCIOptions) *http.Client {
	return &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				// Check token endpoint
				if strings.Contains(req.URL.Path, "/token") || strings.HasSuffix(req.URL.Path, "/token") || req.URL.Host == "auth.docker.io" {
					if opts.TokenErr != nil {
						return nil, opts.TokenErr
					}
					if opts.ExpectedPAT != "" {
						_, pass, ok := req.BasicAuth()
						if !ok || pass != opts.ExpectedPAT {
							return &http.Response{
								StatusCode: http.StatusUnauthorized,
								Header:     make(http.Header),
								Body:       ioNopCloser(strings.NewReader("bad basic auth")),
							}, nil
						}
					}
					if opts.ExpectedScope != "" {
						if req.URL.Query().Get("scope") != opts.ExpectedScope {
							return &http.Response{
								StatusCode: http.StatusBadRequest,
								Header:     make(http.Header),
								Body:       ioNopCloser(strings.NewReader("unexpected scope")),
							}, nil
						}
					}
					status := opts.TokenStatus
					if status == 0 {
						status = http.StatusOK
					}
					body := opts.TokenBody
					if body == "" && status == http.StatusOK {
						body = `{"token":"mock-token"}`
					}
					return &http.Response{
						StatusCode: status,
						Header:     make(http.Header),
						Body:       ioNopCloser(strings.NewReader(body)),
					}, nil
				}

				authHeader := req.Header.Get("Authorization")

				// Probe request without Authorization
				if authHeader == "" {
					if opts.ChallengeAuthHeader != "" {
						h := make(http.Header)
						h.Set("WWW-Authenticate", opts.ChallengeAuthHeader)
						return &http.Response{
							StatusCode: http.StatusUnauthorized,
							Header:     h,
							Body:       ioNopCloser(strings.NewReader("unauthorized")),
						}, nil
					}
					if opts.ProbeErr != nil {
						return nil, opts.ProbeErr
					}
					status := opts.ProbeStatus
					if status == 0 {
						status = http.StatusOK
					}
					h := make(http.Header)
					for k, v := range opts.ProbeHeaders {
						h.Set(k, v)
					}
					return &http.Response{
						StatusCode: status,
						Header:     h,
						Body:       ioNopCloser(strings.NewReader("")),
					}, nil
				}

				// Authenticated request
				if opts.ExpectedBearer != "" && authHeader != "Bearer "+opts.ExpectedBearer {
					h := make(http.Header)
					if opts.ChallengeAuthHeader != "" {
						h.Set("WWW-Authenticate", opts.ChallengeAuthHeader)
					}
					return &http.Response{
						StatusCode: http.StatusUnauthorized,
						Header:     h,
						Body:       ioNopCloser(strings.NewReader("invalid bearer")),
					}, nil
				}

				if opts.AuthErr != nil {
					return nil, opts.AuthErr
				}

				status := opts.AuthStatus
				if status == 0 {
					status = http.StatusOK
				}
				h := make(http.Header)
				for k, v := range opts.AuthHeaders {
					h.Set(k, v)
				}
				return &http.Response{
					StatusCode: status,
					Header:     h,
					Body:       ioNopCloser(strings.NewReader("")),
				}, nil
			},
		},
	}
}

func TestGetRemoteImageDigest_TableDriven(t *testing.T) {
	t.Parallel()
	validDigest := "sha256:abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"

	tests := []struct {
		name       string
		imageRef   string
		pat        string
		clientOpts mockOCIOptions
		wantDigest string
		wantErr    bool
	}{
		{
			name:     "invalid image reference returns error",
			imageRef: "invalid reference with spaces",
			wantErr:  true,
		},
		{
			name:     "direct 200 with Docker-Content-Digest",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus:  http.StatusOK,
				ProbeHeaders: map[string]string{"Docker-Content-Digest": validDigest},
			},
			wantDigest: validDigest,
		},
		{
			name:     "direct 200 with ETag fallback",
			imageRef: "pgvector/pgvector:pg16",
			clientOpts: mockOCIOptions{
				ProbeStatus:  http.StatusOK,
				ProbeHeaders: map[string]string{"ETag": `W/"` + validDigest + `"`},
			},
			wantDigest: validDigest,
		},
		{
			name:     "401 challenge exchange with GHCR and PAT",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			pat:      "secret-pat",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://ghcr.io/token",service="ghcr.io",scope="repository:azylman/aerial-brain:pull"`,
				ExpectedPAT:         "secret-pat",
				TokenBody:           `{"token":"test-ghcr-token"}`,
				ExpectedBearer:      "test-ghcr-token",
				AuthStatus:          http.StatusOK,
				AuthHeaders:         map[string]string{"Docker-Content-Digest": validDigest},
			},
			wantDigest: validDigest,
		},
		{
			name:     "401 challenge exchange with Docker Hub synthesized scope and access_token",
			imageRef: "postgres:16",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`,
				ExpectedScope:       "repository:library/postgres:pull",
				TokenBody:           `{"access_token":"hub-token"}`,
				ExpectedBearer:      "hub-token",
				AuthStatus:          http.StatusOK,
				AuthHeaders:         map[string]string{"Docker-Content-Digest": validDigest},
			},
			wantDigest: validDigest,
		},
		{
			name:     "network failure on initial HEAD",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeErr: errors.New("simulated dial timeout"),
			},
			wantErr: true,
		},
		{
			name:     "401 with malformed authenticate header",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Basic realm="foo"`,
			},
			wantErr: true,
		},
		{
			name:     "401 with invalid token realm url",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="http://[::1]:namedport",service="test"`,
			},
			wantErr: true,
		},
		{
			name:     "401 with token network error",
			imageRef: "postgres:16",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`,
				TokenErr:            errors.New("token endpoint refused"),
			},
			wantErr: true,
		},
		{
			name:     "401 with token HTTP 500 error",
			imageRef: "postgres:16",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`,
				TokenStatus:         http.StatusInternalServerError,
				TokenBody:           "internal error",
			},
			wantErr: true,
		},
		{
			name:     "401 with invalid token JSON",
			imageRef: "postgres:16",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`,
				TokenStatus:         http.StatusOK,
				TokenBody:           "invalid-json",
			},
			wantErr: true,
		},
		{
			name:     "401 with empty token payload",
			imageRef: "postgres:16",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`,
				TokenStatus:         http.StatusOK,
				TokenBody:           `{"token":""}`,
			},
			wantErr: true,
		},
		{
			name:     "401 with authenticated HEAD network failure",
			imageRef: "postgres:16",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io"`,
				TokenBody:           `{"token":"abc"}`,
				AuthErr:             errors.New("auth head failed"),
			},
			wantErr: true,
		},
		{
			name:     "non-200 manifest status returns error",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusNotFound,
			},
			wantErr: true,
		},
		{
			name:     "200 OK without valid digest or etag returns error",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus:  http.StatusOK,
				ProbeHeaders: map[string]string{"ETag": `"not-a-sha256"`},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := NewDaemon(DaemonConfig{
				PAT:            tc.pat,
				RegistryClient: newMockOCIRegistryClient(tc.clientOpts),
			})
			digest, err := d.GetRemoteImageDigest(context.Background(), tc.imageRef)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetRemoteImageDigest() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && digest != tc.wantDigest {
				t.Errorf("GetRemoteImageDigest() = %q, want %q", digest, tc.wantDigest)
			}
		})
	}
}

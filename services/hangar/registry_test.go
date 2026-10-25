package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
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
	ProbeBody    string

	ChallengeAuthHeader string

	TokenErr       error
	TokenStatus    int
	TokenBody      string
	ExpectedPAT    string
	ExpectedScope  string
	OnTokenRequest func(req *http.Request)

	AuthErr          error
	AuthStatus       int
	AuthHeaders      map[string]string
	AuthBody         string
	ExpectedBearer   string
	ChildManifestMap map[string]string
	BlobMap          map[string]string

	OnRequest func(req *http.Request) (*http.Response, error, bool)
}

func newMockOCIRegistryClient(opts mockOCIOptions) *http.Client {
	return &http.Client{
		Transport: &roundTripperFunc{
			fn: func(req *http.Request) (*http.Response, error) {
				if opts.OnRequest != nil {
					if resp, err, handled := opts.OnRequest(req); handled {
						return resp, err
					}
				}

				// Check token endpoint
				if strings.Contains(req.URL.Path, "/token") || strings.HasSuffix(req.URL.Path, "/token") || req.URL.Host == "auth.docker.io" {
					if opts.OnTokenRequest != nil {
						opts.OnTokenRequest(req)
					}
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

				// Check child manifests
				if opts.ChildManifestMap != nil {
					for dgst, manifest := range opts.ChildManifestMap {
						if strings.HasSuffix(req.URL.Path, "/"+dgst) {
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
								Body:       ioNopCloser(strings.NewReader(manifest)),
							}, nil
						}
					}
				}

				// Check config blobs
				if opts.BlobMap != nil {
					for dgst, blob := range opts.BlobMap {
						if strings.HasSuffix(req.URL.Path, "/"+dgst) {
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
								Body:       ioNopCloser(strings.NewReader(blob)),
							}, nil
						}
					}
				}

				// Probe request without Authorization
				if authHeader == "" {
					if opts.ChallengeAuthHeader != "" {
						h := make(http.Header)
						h.Set("Www-Authenticate", opts.ChallengeAuthHeader)
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
						Body:       ioNopCloser(strings.NewReader(opts.ProbeBody)),
					}, nil
				}

				// Authenticated request
				if opts.ExpectedBearer != "" && authHeader != "Bearer "+opts.ExpectedBearer {
					h := make(http.Header)
					if opts.ChallengeAuthHeader != "" {
						h.Set("Www-Authenticate", opts.ChallengeAuthHeader)
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

				// Check child manifests
				if opts.ChildManifestMap != nil {
					for dgst, manifest := range opts.ChildManifestMap {
						if strings.HasSuffix(req.URL.Path, "/"+dgst) {
							h := make(http.Header)
							for k, v := range opts.AuthHeaders {
								h.Set(k, v)
							}
							return &http.Response{
								StatusCode: http.StatusOK,
								Header:     h,
								Body:       ioNopCloser(strings.NewReader(manifest)),
							}, nil
						}
					}
				}

				// Check config blobs
				if opts.BlobMap != nil {
					for dgst, blob := range opts.BlobMap {
						if strings.HasSuffix(req.URL.Path, "/"+dgst) {
							h := make(http.Header)
							for k, v := range opts.AuthHeaders {
								h.Set(k, v)
							}
							return &http.Response{
								StatusCode: http.StatusOK,
								Header:     h,
								Body:       ioNopCloser(strings.NewReader(blob)),
							}, nil
						}
					}
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
					Body:       ioNopCloser(strings.NewReader(opts.AuthBody)),
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

func TestCheckGitHubRepoBuildInProgress_TableDriven(t *testing.T) {
	ctx := context.Background()

	var dNil *SyncDaemon
	if dNil.CheckGitHubRepoBuildInProgress(ctx, "aerial") {
		t.Errorf("expected false for nil daemon")
	}

	tests := []struct {
		name      string
		repo      string
		pat       string
		respCode  int
		respBody  string
		clientErr error
		want      bool
	}{
		{name: "empty repo slug", repo: "", want: false},
		{
			name: "in-progress workflow run exists", repo: "azylman/aerial", pat: "test-pat",
			respCode: http.StatusOK, respBody: `{"total_count": 1, "workflow_runs": [{}]}`, want: true,
		},
		{
			name: "queued workflow run exists", repo: "aerial", pat: "test-pat",
			respCode: http.StatusOK, respBody: `{"total_count": 2, "workflow_runs": [{},{}]}`, want: true,
		},
		{
			name: "zero runs", repo: "azylman/mirrormere",
			respCode: http.StatusOK, respBody: `{"total_count": 0, "workflow_runs": []}`, want: false,
		},
		{
			name: "HTTP 502 error", repo: "aerial",
			respCode: http.StatusBadGateway, respBody: "error", want: false,
		},
		{
			name: "client transport error", repo: "aerial",
			clientErr: errors.New("network timeout"), want: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := &SyncDaemon{
				pat: tc.pat,
				registryClient: &http.Client{
					Transport: &roundTripperFunc{
						fn: func(req *http.Request) (*http.Response, error) {
							if tc.clientErr != nil {
								return nil, tc.clientErr
							}
							return &http.Response{
								StatusCode: tc.respCode,
								Header:     make(http.Header),
								Body:       ioNopCloser(strings.NewReader(tc.respBody)),
							}, nil
						},
					},
				},
			}
			got := d.CheckGitHubRepoBuildInProgress(ctx, tc.repo)
			if got != tc.want {
				t.Errorf("CheckGitHubRepoBuildInProgress(%q) = %v, want %v", tc.repo, got, tc.want)
			}
		})
	}
}

func TestGetRemoteImageRevision_TableDriven(t *testing.T) {
	t.Parallel()
	expectedSHA := "0123456789abcdef0123456789abcdef01234567"
	childDigest := "sha256:1111111111111111111111111111111111111111111111111111111111111111"
	configBlobDigest := "sha256:2222222222222222222222222222222222222222222222222222222222222222"

	indexJSON := fmt.Sprintf(`{
		"schemaVersion": 2,
		"mediaType": "application/vnd.oci.image.index.v1+json",
		"manifests": [
			{
				"mediaType": "application/vnd.oci.image.manifest.v1+json",
				"digest": "%s",
				"size": 1024,
				"platform": { "architecture": "amd64", "os": "linux" }
			}
		]
	}`, childDigest)

	manifestJSON := fmt.Sprintf(`{
		"schemaVersion": 2,
		"mediaType": "application/vnd.oci.image.manifest.v1+json",
		"config": {
			"mediaType": "application/vnd.oci.image.config.v1+json",
			"digest": "%s",
			"size": 512
		},
		"layers": []
	}`, configBlobDigest)

	configJSON := fmt.Sprintf(`{
		"config": {
			"Labels": {
				"org.opencontainers.image.revision": "%s"
			}
		}
	}`, expectedSHA)

	var child401Sent bool

	tests := []struct {
		name       string
		imageRef   string
		clientOpts mockOCIOptions
		wantRev    string
		wantErr    bool
	}{
		{
			name:     "invalid image reference returns error",
			imageRef: "invalid reference with spaces",
			wantErr:  true,
		},
		{
			name:     "401 with existingBearer clears token and retries",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:azylman/aerial-brain:pull"`,
				AuthBody:            indexJSON,
				ChildManifestMap: map[string]string{
					childDigest: manifestJSON,
				},
				BlobMap: map[string]string{
					configBlobDigest: configJSON,
				},
				OnRequest: func(req *http.Request) (*http.Response, error, bool) {
					if strings.Contains(req.URL.Path, childDigest) && !child401Sent {
						child401Sent = true
						h := make(http.Header)
						h.Set("Www-Authenticate", `Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:azylman/aerial-brain:pull"`)
						return &http.Response{
							StatusCode: http.StatusUnauthorized,
							Header:     h,
							Body:       ioNopCloser(strings.NewReader("stale bearer")),
						}, nil, true
					}
					return nil, nil, false
				},
			},
			wantRev: expectedSHA,
		},
		{
			name:     "Case 1: OCI Index to Child Manifest to Config Blob",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusOK,
				ProbeBody:   indexJSON,
				ChildManifestMap: map[string]string{
					childDigest: manifestJSON,
				},
				BlobMap: map[string]string{
					configBlobDigest: configJSON,
				},
			},
			wantRev: expectedSHA,
		},
		{
			name:     "Case 2: Direct Manifest with aerial.commit_sha annotations without fetching blob",
			imageRef: "ghcr.io/azylman/aerial-webhooks-router:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusOK,
				ProbeBody: fmt.Sprintf(`{
					"schemaVersion": 2,
					"mediaType": "application/vnd.oci.image.manifest.v1+json",
					"config": { "digest": "sha256:unused" },
					"annotations": { "aerial.commit_sha": "%s" }
				}`, expectedSHA),
			},
			wantRev: expectedSHA,
		},
		{
			name:     "Case 3: 401 challenge exchange + token + config blob",
			imageRef: "redis:latest",
			clientOpts: mockOCIOptions{
				ChallengeAuthHeader: `Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:library/redis:pull"`,
				ExpectedScope:       "repository:library/redis:pull",
				TokenBody:           `{"token":"secret-bearer-token"}`,
				ExpectedBearer:      "secret-bearer-token",
				AuthStatus:          http.StatusOK,
				AuthBody: fmt.Sprintf(`{
					"schemaVersion": 2,
					"mediaType": "application/vnd.oci.image.manifest.v1+json",
					"config": { "digest": "%s" }
				}`, configBlobDigest),
				BlobMap: map[string]string{
					configBlobDigest: fmt.Sprintf(`{"config":{"Labels":{"org.label-schema.vcs-ref":"%s"}}}`, expectedSHA),
				},
			},
			wantRev: expectedSHA,
		},
		{
			name:     "Case 4: Index with annotations directly resolves revision",
			imageRef: "ghcr.io/azylman/aerial:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusOK,
				ProbeBody: fmt.Sprintf(`{
					"schemaVersion": 2,
					"manifests": [
						{
							"digest": "%s",
							"platform": {"os": "linux", "architecture": "amd64"}
						}
					],
					"annotations": {
						"org.opencontainers.image.revision": "%s"
					}
				}`, childDigest, expectedSHA),
			},
			wantRev: expectedSHA,
		},
		{
			name:     "missing labels in config blob returns error",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusOK,
				ProbeBody: fmt.Sprintf(`{
					"schemaVersion": 2,
					"config": { "digest": "%s" }
				}`, configBlobDigest),
				BlobMap: map[string]string{
					configBlobDigest: `{"config":{"Labels":{}}}`,
				},
			},
			wantErr: true,
		},
		{
			name:     "child manifest request fails with non-200",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusOK,
				ProbeBody:   indexJSON,
				AuthStatus:  http.StatusNotFound,
				AuthBody:    "child manifest not found",
			},
			wantErr: true,
		},
		{
			name:     "manifest missing config digest and annotations returns error",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusOK,
				ProbeBody:   `{ "schemaVersion": 2 }`,
			},
			wantErr: true,
		},
		{
			name:     "blob fetch non-200 returns error",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusOK,
				ProbeBody: fmt.Sprintf(`{
					"schemaVersion": 2,
					"config": { "digest": "%s" }
				}`, configBlobDigest),
				AuthStatus: http.StatusBadGateway,
				AuthBody:   "gateway error on blob",
			},
			wantErr: true,
		},
		{
			name:     "probe 500 error returns error",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeStatus: http.StatusInternalServerError,
				ProbeBody:   "server internal error",
			},
			wantErr: true,
		},
		{
			name:     "probe network error returns error",
			imageRef: "ghcr.io/azylman/aerial-brain:latest",
			clientOpts: mockOCIOptions{
				ProbeErr: errors.New("network dial timeout"),
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			d := NewDaemon(DaemonConfig{RegistryClient: newMockOCIRegistryClient(tc.clientOpts)})
			rev, err := d.GetRemoteImageRevision(context.Background(), tc.imageRef)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GetRemoteImageRevision() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && rev != tc.wantRev {
				t.Errorf("GetRemoteImageRevision() = %q, want %q", rev, tc.wantRev)
			}
		})
	}
}

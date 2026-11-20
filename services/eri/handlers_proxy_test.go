package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/queue"
)

type proxyTestCase struct {
	name             string
	method           string
	reqPath          string
	reqQuery         string
	reqBody          string
	configuredURL    string
	upstreamStatus   int
	upstreamBody     string
	expectProxied    bool
	expectedStatus   int
	expectedUpstream string
	expectedQuery    string
}

type capturedProxyReq struct {
	path    string
	query   string
	headers http.Header
}

func assertProxiedRequest(t *testing.T, tc proxyTestCase, captured []capturedProxyReq) {
	t.Helper()
	if len(captured) == 0 {
		t.Fatalf("expected request to be proxied to upstream, but upstream received 0 requests")
	}
	for i := range captured {
		if len(captured) <= i {
			t.Fatalf("guard failed: captured index out of bounds: len=%d, i=%d", len(captured), i)
		}
		req := captured[i]
		if req.path != tc.expectedUpstream {
			t.Errorf("expected upstream path %q, got %q", tc.expectedUpstream, req.path)
		}
		if req.query != tc.expectedQuery {
			t.Errorf("expected upstream query %q, got %q", tc.expectedQuery, req.query)
		}
		if req.headers.Get("X-Custom-Header") != "proxy-test-val" {
			t.Errorf("expected X-Custom-Header to be forwarded, got %q", req.headers.Get("X-Custom-Header"))
		}
		for _, stripped := range []string{"Authorization", "Proxy-Authorization", "Cookie", "Set-Cookie", "X-Api-Key"} {
			if req.headers.Get(stripped) != "" {
				t.Errorf("expected %s header to be stripped, got %q", stripped, req.headers.Get(stripped))
			}
		}
	}
}

func assertBadGatewayError(t *testing.T, body []byte) {
	t.Helper()
	var errResp map[string]string
	if err := json.Unmarshal(body, &errResp); err != nil {
		t.Fatalf("expected JSON error response, got error: %v", err)
	}
	if !strings.Contains(errResp["error"], "Upstream proxy error") {
		t.Errorf("expected error message to contain 'Upstream proxy error', got: %v", errResp["error"])
	}
}

func executeProxyTest(t *testing.T, tc proxyTestCase) {
	t.Helper()
	var mu sync.Mutex
	var captured []capturedProxyReq

	upstreamServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		captured = append(captured, capturedProxyReq{
			path:    r.URL.Path,
			query:   r.URL.RawQuery,
			headers: r.Header.Clone(),
		})
		mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(tc.upstreamStatus)
		_, _ = io.WriteString(w, tc.upstreamBody)
	}))
	defer upstreamServer.Close()

	targetIndexerURL := tc.configuredURL
	if targetIndexerURL == "INDEXER_PLACEHOLDER" {
		targetIndexerURL = upstreamServer.URL
	}

	var pool *queue.WorkerPool
	if targetIndexerURL != "" {
		pool = &queue.WorkerPool{
			TranscriptsIndexerURL: targetIndexerURL,
		}
	}

	store := dbtest.NewFakeStore()
	mux := SetupBrainMuxWithEmbedder(store, store, pool, nil)

	reqURL := tc.reqPath
	if tc.reqQuery != "" {
		reqURL += "?" + tc.reqQuery
	}
	req := httptest.NewRequest(tc.method, reqURL, strings.NewReader(tc.reqBody))
	req.Header.Set("X-Custom-Header", "proxy-test-val")
	req.Header.Set("Authorization", "Bearer secret-token")
	req.Header.Set("Cookie", "session=secret")
	req.Header.Set("Proxy-Authorization", "Basic credentials")
	req.Header.Set("X-Api-Key", "secret-key")
	rec := httptest.NewRecorder()

	mux.ServeHTTP(rec, req)

	if rec.Code != tc.expectedStatus {
		t.Fatalf("expected status %d, got %d (body: %s)", tc.expectedStatus, rec.Code, rec.Body.String())
	}

	mu.Lock()
	defer mu.Unlock()

	switch {
	case tc.expectProxied:
		assertProxiedRequest(t, tc, captured)
	case strings.HasPrefix(targetIndexerURL, "http://127.0.0.1:0"):
		assertBadGatewayError(t, rec.Body.Bytes())
	default:
		if len(captured) > 0 {
			t.Fatalf("expected request to NOT be proxied, but upstream received %d requests", len(captured))
		}
	}
}

func proxySuccessCases() []proxyTestCase {
	return []proxyTestCase{
		{
			name:             "SearchProxiedWithQueryParamsAndEmptyStack",
			method:           http.MethodGet,
			reqPath:          "/transcripts/search",
			reqQuery:         "q=test&limit=5",
			configuredURL:    "INDEXER_PLACEHOLDER",
			upstreamStatus:   http.StatusOK,
			upstreamBody:     `{"results":[],"total":0}`,
			expectProxied:    true,
			expectedStatus:   http.StatusOK,
			expectedUpstream: "/transcripts/search",
			expectedQuery:    "q=test&limit=5",
		},
		{
			name:             "ApiSearchProxiedPreservesAllParams",
			method:           http.MethodGet,
			reqPath:          "/api/transcripts/search",
			reqQuery:         "stack=custom&query=foobar&mode=fts&session=s-123&tool=bash&limit=20&since=2026-01-01T00:00:00Z",
			configuredURL:    "INDEXER_PLACEHOLDER",
			upstreamStatus:   http.StatusOK,
			upstreamBody:     `{"results":[],"total":0}`,
			expectProxied:    true,
			expectedStatus:   http.StatusOK,
			expectedUpstream: "/transcripts/search",
			expectedQuery:    "stack=custom&query=foobar&mode=fts&session=s-123&tool=bash&limit=20&since=2026-01-01T00:00:00Z",
		},
		{
			name:             "StatsProxiedWithEmptyStack",
			method:           http.MethodGet,
			reqPath:          "/transcripts/stats",
			reqQuery:         "",
			configuredURL:    "INDEXER_PLACEHOLDER",
			upstreamStatus:   http.StatusOK,
			upstreamBody:     `{"total_sessions":0,"total_steps":0,"stacks":[],"status":"ok"}`,
			expectProxied:    true,
			expectedStatus:   http.StatusOK,
			expectedUpstream: "/transcripts/stats",
			expectedQuery:    "",
		},
		{
			name:             "ApiStatsProxiedWithExplicitStack",
			method:           http.MethodGet,
			reqPath:          "/api/transcripts/stats",
			reqQuery:         "stack=miorine",
			configuredURL:    "INDEXER_PLACEHOLDER",
			upstreamStatus:   http.StatusOK,
			upstreamBody:     `{"total_sessions":10,"total_steps":50,"stacks":[{"stack":"miorine","sessions":10,"steps":50}],"status":"ok"}`,
			expectProxied:    true,
			expectedStatus:   http.StatusOK,
			expectedUpstream: "/transcripts/stats",
			expectedQuery:    "stack=miorine",
		},
		{
			name:             "StepsProxiedWithSessionAndStep",
			method:           http.MethodGet,
			reqPath:          "/transcripts/steps",
			reqQuery:         "session=sess-abc&step=42&context=3",
			configuredURL:    "INDEXER_PLACEHOLDER",
			upstreamStatus:   http.StatusOK,
			upstreamBody:     `{"steps":[],"total":0}`,
			expectProxied:    true,
			expectedStatus:   http.StatusOK,
			expectedUpstream: "/transcripts/steps",
			expectedQuery:    "session=sess-abc&step=42&context=3",
		},
		{
			name:             "ApiStepsProxiedWithStackPreserved",
			method:           http.MethodGet,
			reqPath:          "/api/transcripts/steps",
			reqQuery:         "stack=suletta&session=sess-xyz&step=1",
			configuredURL:    "INDEXER_PLACEHOLDER",
			upstreamStatus:   http.StatusOK,
			upstreamBody:     `{"steps":[],"total":0}`,
			expectProxied:    true,
			expectedStatus:   http.StatusOK,
			expectedUpstream: "/transcripts/steps",
			expectedQuery:    "stack=suletta&session=sess-xyz&step=1",
		},
	}
}

func proxyFallbackAndErrorCases() []proxyTestCase {
	return []proxyTestCase{
		{
			name:             "SearchFallbackWhenIndexerURLNotConfigured",
			method:           http.MethodGet,
			reqPath:          "/transcripts/search",
			reqQuery:         "q=localtest",
			configuredURL:    "",
			expectProxied:    false,
			expectedStatus:   http.StatusOK,
			expectedUpstream: "",
			expectedQuery:    "",
		},
		{
			name:             "StatsFallbackWhenIndexerURLNotConfigured",
			method:           http.MethodGet,
			reqPath:          "/transcripts/stats",
			reqQuery:         "",
			configuredURL:    "",
			expectProxied:    false,
			expectedStatus:   http.StatusOK,
			expectedUpstream: "",
			expectedQuery:    "",
		},
		{
			name:             "StepsFallbackWhenIndexerURLNotConfigured",
			method:           http.MethodGet,
			reqPath:          "/transcripts/steps",
			reqQuery:         "session=s1&step=0",
			configuredURL:    "",
			expectProxied:    false,
			expectedStatus:   http.StatusOK,
			expectedUpstream: "",
			expectedQuery:    "",
		},
		{
			name:             "UpstreamBadGatewayOnError",
			method:           http.MethodGet,
			reqPath:          "/transcripts/search",
			reqQuery:         "q=error",
			configuredURL:    "http://127.0.0.1:0", // Unreachable port triggers BadGateway
			expectProxied:    false,
			expectedStatus:   http.StatusBadGateway,
			expectedUpstream: "",
			expectedQuery:    "",
		},
		{
			name:             "MethodNotAllowedPostWhenProxied",
			method:           http.MethodPost,
			reqPath:          "/transcripts/search",
			reqQuery:         "q=test",
			configuredURL:    "INDEXER_PLACEHOLDER",
			expectProxied:    false,
			expectedStatus:   http.StatusMethodNotAllowed,
			expectedUpstream: "",
			expectedQuery:    "",
		},
		{
			name:             "MethodNotAllowedPutWhenProxied",
			method:           http.MethodPut,
			reqPath:          "/transcripts/stats",
			configuredURL:    "INDEXER_PLACEHOLDER",
			expectProxied:    false,
			expectedStatus:   http.StatusMethodNotAllowed,
			expectedUpstream: "",
			expectedQuery:    "",
		},
		{
			name:             "MethodNotAllowedDeleteWhenProxied",
			method:           http.MethodDelete,
			reqPath:          "/transcripts/steps",
			reqQuery:         "session=s1&step=0",
			configuredURL:    "INDEXER_PLACEHOLDER",
			expectProxied:    false,
			expectedStatus:   http.StatusMethodNotAllowed,
			expectedUpstream: "",
			expectedQuery:    "",
		},
	}
}

func proxyEndpointCases() []proxyTestCase {
	cases := proxySuccessCases()
	return append(cases, proxyFallbackAndErrorCases()...)
}

func TestTranscriptsProxy(t *testing.T) {
	t.Parallel()

	for _, tc := range proxyEndpointCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			executeProxyTest(t, tc)
		})
	}
}

func TestResolveTranscriptsIndexerURL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		poolURL     string
		cfgURL      string
		hasPool     bool
		hasCfg      bool
		expectedURL string
	}{
		{
			name:        "PoolTakesPrecedenceOverConfig",
			poolURL:     "http://pool-indexer:8080/",
			cfgURL:      "http://cfg-indexer:8080/",
			hasPool:     true,
			hasCfg:      true,
			expectedURL: "http://pool-indexer:8080",
		},
		{
			name:        "ConfigUsedWhenPoolURLBlank",
			poolURL:     "   ",
			cfgURL:      "http://cfg-indexer:8080///",
			hasPool:     true,
			hasCfg:      true,
			expectedURL: "http://cfg-indexer:8080",
		},
		{
			name:        "ConfigUsedWhenPoolNil",
			poolURL:     "",
			cfgURL:      "http://cfg-indexer:8080",
			hasPool:     false,
			hasCfg:      true,
			expectedURL: "http://cfg-indexer:8080",
		},
		{
			name:        "EmptyWhenBothEmptyOrNil",
			poolURL:     "",
			cfgURL:      "",
			hasPool:     false,
			hasCfg:      false,
			expectedURL: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var pool *queue.WorkerPool
			if tc.hasPool {
				pool = &queue.WorkerPool{
					TranscriptsIndexerURL: tc.poolURL,
				}
			}

			var cfg *config.Config
			if tc.hasCfg {
				cfg = config.NewFromData(&config.ConfigData{
					Transcripts: config.TranscriptsConfig{
						IndexerURL: tc.cfgURL,
					},
				})
			}

			got := resolveTranscriptsIndexerURL(cfg, pool)
			if got != tc.expectedURL {
				t.Errorf("expected URL %q, got %q", tc.expectedURL, got)
			}
		})
	}
}

func TestHandleInternalGuardScan(t *testing.T) {
	tests := []struct {
		name         string
		method       string
		reqBody      string
		wantStatus   int
		wantDecision string
	}{
		{
			name:       "method not allowed",
			method:     http.MethodGet,
			reqBody:    "",
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "invalid json",
			method:     http.MethodPost,
			reqBody:    "not-json",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:         "clean diff allowed",
			method:       http.MethodPost,
			reqBody:      `{"diff":"+ clean line of code"}`,
			wantStatus:   http.StatusOK,
			wantDecision: "allow",
		},
		{
			name:         "secret in diff denied",
			method:       http.MethodPost,
			reqBody:      `{"diff":"+ export GITHUB_PAT=ghp_012345678901234567890123456789012345"}`,
			wantStatus:   http.StatusOK,
			wantDecision: "deny",
		},
		{
			name:         "clean text allowed",
			method:       http.MethodPost,
			reqBody:      `{"text":"hello world"}`,
			wantStatus:   http.StatusOK,
			wantDecision: "allow",
		},
		{
			name:         "secret in text denied",
			method:       http.MethodPost,
			reqBody:      `{"text":"my key is ghp_012345678901234567890123456789012345"}`,
			wantStatus:   http.StatusOK,
			wantDecision: "deny",
		},
		{
			name:         "internal domain ignored when requested",
			method:       http.MethodPost,
			reqBody:      `{"diff":"+ http://eri.aerial","ignore_internal_domain":true}`,
			wantStatus:   http.StatusOK,
			wantDecision: "allow",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			handler := handleInternalGuardScan()
			req := httptest.NewRequest(tc.method, "/internal/guard/scan", strings.NewReader(tc.reqBody))
			rec := httptest.NewRecorder()
			handler(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if tc.wantDecision != "" {
				var resp GuardScanResponse
				if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
					t.Fatalf("failed to decode response: %v", err)
				}
				if resp.Decision != tc.wantDecision {
					t.Fatalf("decision = %q, want %q", resp.Decision, tc.wantDecision)
				}
			}
		})
	}
}

func TestResolveConfigPAT(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "token ghp_test123" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"authenticated":true}`))
	}))
	defer ts.Close()

	t.Setenv("GITHUB_API_URL", ts.URL)

	tests := []struct {
		name       string
		hasCfg     bool
		nilCur     bool
		pat        string
		wantStatus int
		wantSubstr string
	}{
		{
			name:       "nil config returns 503",
			hasCfg:     false,
			wantStatus: http.StatusServiceUnavailable,
			wantSubstr: "GitHub PAT not configured",
		},
		{
			name:       "nil current config returns 503",
			hasCfg:     true,
			nilCur:     true,
			wantStatus: http.StatusServiceUnavailable,
			wantSubstr: "GitHub PAT not configured",
		},
		{
			name:       "empty pat returns 503",
			hasCfg:     true,
			pat:        "",
			wantStatus: http.StatusServiceUnavailable,
			wantSubstr: "GitHub PAT not configured",
		},
		{
			name:       "valid pat forwards auth header and returns 200",
			hasCfg:     true,
			pat:        "ghp_test123",
			wantStatus: http.StatusOK,
			wantSubstr: "authenticated",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var cfg *config.Config
			if tc.hasCfg {
				if tc.nilCur {
					cfg = &config.Config{}
				} else {
					cfg = config.NewFromData(&config.ConfigData{
						GitHubPAT: tc.pat,
					})
				}
			}
			handler := handleInternalGitHubAPIProxy(cfg)
			req := httptest.NewRequest(http.MethodGet, "/internal/github-api/user", http.NoBody)
			rec := httptest.NewRecorder()
			handler(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tc.wantStatus)
			}
			if !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Fatalf("body = %q, want %q", rec.Body.String(), tc.wantSubstr)
			}
		})
	}
}

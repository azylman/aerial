package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type captureTransport struct {
	mu          sync.Mutex
	capturedReq *http.Request
	err         error
}

func (c *captureTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.err != nil {
		return nil, c.err
	}
	c.capturedReq = req
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader("")),
	}, nil
}

func TestForwardRawWebhook_TransportErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		nilServer     bool
		nilClient     bool
		baseURL       string
		clientErr     error
		headers       http.Header
		wantSent      bool
		wantHeaders   map[string]string
		wantNoHeaders []string
	}{
		{name: "nil server", nilServer: true, baseURL: "http://127.0.0.1:4020"},
		{name: "nil client", nilClient: true, baseURL: "http://127.0.0.1:4020"},
		{name: "empty baseURL", baseURL: "", wantSent: false},
		{name: "invalid URL format", baseURL: "://bad url", wantSent: false},
		{name: "client Do error", baseURL: "http://127.0.0.1:4020", clientErr: errors.New("connection failed"), wantSent: false},
		{
			name:    "strips host connection and content-length headers",
			baseURL: "http://127.0.0.1:4020",
			headers: http.Header{
				"Host":           {"evil.com"},
				"Connection":     {"close"},
				"Content-Length": {"10"},
				"X-Custom":       {"custom-val"},
			},
			wantSent: true,
			wantHeaders: map[string]string{
				"X-Custom": "custom-val",
			},
			wantNoHeaders: []string{"Host", "Connection", "Content-Length"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			transport := &captureTransport{err: tc.clientErr}
			client := &http.Client{Transport: transport}
			if tc.nilClient {
				client = nil
			}

			var srv *RouterServer
			if !tc.nilServer {
				srv = &RouterServer{
					httpClient: client,
				}
			}

			srv.forwardRawWebhook(context.Background(), tc.baseURL, "/hook", tc.headers, []byte(`{"test":true}`))

			transport.mu.Lock()
			defer transport.mu.Unlock()

			if tc.wantSent {
				if transport.capturedReq == nil {
					t.Fatalf("expected request to be sent, but capturedReq is nil")
				}
				for k, v := range tc.wantHeaders {
					if got := transport.capturedReq.Header.Get(k); got != v {
						t.Errorf("header %q = %q, want %q", k, got, v)
					}
				}
				for _, k := range tc.wantNoHeaders {
					if got := transport.capturedReq.Header.Get(k); got != "" {
						t.Errorf("expected header %q to be stripped, got %q", k, got)
					}
				}
			} else if tc.clientErr == nil && transport.capturedReq != nil {
				t.Fatalf("expected no request to be sent, but capturedReq is not nil")
			}
		})
	}
}

func TestForwardRawWebhookToBot_Routing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nilServer   bool
		forwardURLs map[string]string
		botID       string
		targetBotID string
		wantSent    bool
	}{
		{name: "nil server", nilServer: true, targetBotID: "bot_a"},
		{name: "empty forward URLs", forwardURLs: nil, targetBotID: "bot_a"},
		{name: "target bot matches self", forwardURLs: map[string]string{"aerial": "http://127.0.0.1:9090"}, botID: "aerial", targetBotID: "aerial", wantSent: false},
		{name: "target bot not found in map", forwardURLs: map[string]string{"bot_b": "http://127.0.0.1:9090"}, botID: "aerial", targetBotID: "bot_a", wantSent: false},
		{name: "target bot found and forwarded", forwardURLs: map[string]string{"bot_a": "target"}, botID: "aerial", targetBotID: "bot_a", wantSent: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()

			urls := tc.forwardURLs
			if tc.wantSent {
				urls = map[string]string{"bot_a": ts.URL}
			}

			var srv *RouterServer
			if !tc.nilServer {
				srv = &RouterServer{
					httpClient: ts.Client(),
					cfgSrc: fixedConfig(Config{
						BotID:              tc.botID,
						WebhookForwardURLs: urls,
					}),
				}
			}

			srv.forwardRawWebhookToBot(context.Background(), tc.targetBotID, "/hook", nil, []byte(`{}`))

			if tc.wantSent && calls.Load() != 1 {
				t.Errorf("expected 1 forward call, got %d", calls.Load())
			} else if !tc.wantSent && calls.Load() != 0 {
				t.Errorf("expected 0 forward calls, got %d", calls.Load())
			}
		})
	}
}

func TestForwardRawWebhookToPeers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nilServer   bool
		forwardURLs map[string]string
		wantCalls   int
	}{
		{name: "nil server", nilServer: true, wantCalls: 0},
		{name: "empty forward URLs", forwardURLs: nil, wantCalls: 0},
		{name: "forwards to peers skipping self", forwardURLs: map[string]string{"aerial": "self", "peer": "target"}, wantCalls: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()

			urls := tc.forwardURLs
			if tc.wantCalls > 0 {
				urls = map[string]string{"aerial": "self", "peer": ts.URL}
			}

			var srv *RouterServer
			if !tc.nilServer {
				srv = &RouterServer{
					httpClient: ts.Client(),
					cfgSrc: fixedConfig(Config{
						BotID:              "aerial",
						WebhookForwardURLs: urls,
					}),
				}
			}

			srv.forwardRawWebhookToPeers(context.Background(), "/hook", nil, []byte(`{}`))

			if int(calls.Load()) != tc.wantCalls {
				t.Fatalf("calls = %d, want %d", calls.Load(), tc.wantCalls)
			}
		})
	}
}

func TestTeeGitHubWebhook_PayloadFallbacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nilServer   bool
		forwardURLs map[string]string
		event       string
		payload     string
		contentType string
		wantCalls   int
	}{
		{name: "nil server", nilServer: true, event: "push"},
		{name: "empty forward URLs", forwardURLs: nil, event: "push"},
		{
			name:        "missing content type defaults to application/json",
			forwardURLs: map[string]string{"peer": "target"},
			event:       "push",
			payload:     `{"ref":"refs/heads/main","after":"sha1"}`,
			contentType: "",
			wantCalls:   1,
		},
		{
			name:        "push with head_commit id fallback",
			forwardURLs: map[string]string{"peer": "target"},
			event:       "push",
			payload:     `{"ref":"refs/heads/feature","head_commit":{"id":"commit_sha"}}`,
			contentType: "application/json",
			wantCalls:   1,
		},
		{
			name:        "pull_request invalid json payload returns early",
			forwardURLs: map[string]string{"peer": "target"},
			event:       "pull_request",
			payload:     `not-json`,
			wantCalls:   0,
		},
		{
			name:        "push invalid json payload returns early",
			forwardURLs: map[string]string{"peer": "target"},
			event:       "push",
			payload:     `not-json`,
			wantCalls:   0,
		},
		{
			name:        "check_run invalid json payload returns early",
			forwardURLs: map[string]string{"peer": "target"},
			event:       "check_run",
			payload:     `not-json`,
			wantCalls:   0,
		},
		{
			name:        "workflow_run invalid json payload returns early",
			forwardURLs: map[string]string{"peer": "target"},
			event:       "workflow_run",
			payload:     `not-json`,
			wantCalls:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			var recContentType string
			var mu sync.Mutex

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				mu.Lock()
				recContentType = r.Header.Get("Content-Type")
				mu.Unlock()
				w.WriteHeader(http.StatusOK)
			}))
			defer ts.Close()

			urls := tc.forwardURLs
			if len(urls) > 0 {
				urls = map[string]string{"peer": ts.URL}
			}

			var srv *RouterServer
			if !tc.nilServer {
				srv = &RouterServer{
					httpClient: ts.Client(),
					cfgSrc: fixedConfig(Config{
						BotID:              "aerial",
						WebhookForwardURLs: urls,
					}),
				}
			}

			req := httptest.NewRequest(http.MethodPost, "/api/webhooks/github", strings.NewReader(tc.payload))
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			srv.teeGitHubWebhook(context.Background(), "/api/webhooks/github", req, tc.event, []byte(tc.payload))

			if int(calls.Load()) != tc.wantCalls {
				t.Errorf("calls = %d, want %d", calls.Load(), tc.wantCalls)
			}
			if tc.wantCalls > 0 && tc.contentType == "" {
				mu.Lock()
				defer mu.Unlock()
				if recContentType != "application/json" {
					t.Errorf("expected Content-Type application/json, got %q", recContentType)
				}
			}
		})
	}
}

func TestIsEventOwnedLocally(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		nilServer  bool
		nilReg     bool
		prNum      int
		sha        string
		ownedPRs   map[int]bool
		ownedSHAs  map[string]int
		resolveErr error
		wantOwned  bool
	}{
		{name: "nil server", nilServer: true, prNum: 101, wantOwned: false},
		{name: "nil registry", nilReg: true, prNum: 101, wantOwned: false},
		{name: "prNum > 0 and owned locally", prNum: 101, ownedPRs: map[int]bool{101: true}, wantOwned: true},
		{name: "prNum > 0 and unowned, empty sha", prNum: 101, sha: "", wantOwned: false},
		{name: "zero sha returns false", prNum: 0, sha: "0000000000000000000000000000000000000000", wantOwned: false},
		{name: "resolve error returns false", prNum: 0, sha: "sha123", ownedSHAs: map[string]int{"sha123": 105}, ownedPRs: map[int]bool{105: true}, resolveErr: errors.New("db error"), wantOwned: false},
		{name: "sha resolves to owned PR", prNum: 0, sha: "sha_owned", ownedSHAs: map[string]int{"sha_owned": 105}, ownedPRs: map[int]bool{105: true}, wantOwned: true},
		{name: "sha resolves to unowned", prNum: 0, sha: "sha_unowned", wantOwned: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.nilServer {
				var srv *RouterServer
				if got := srv.isEventOwnedLocally(context.Background(), "azylman/aerial", tc.prNum, tc.sha); got != tc.wantOwned {
					t.Fatalf("isEventOwnedLocally got %v, want %v", got, tc.wantOwned)
				}
				return
			}
			var reg PRRegistryUpdater
			if !tc.nilReg {
				r := &ownedMapRegistry{
					ownedPRs:  tc.ownedPRs,
					ownedSHAs: tc.ownedSHAs,
				}
				r.resolveErr = tc.resolveErr
				reg = r
			}
			srv := newTestRouter(Config{}, nil, reg, nil)
			if got := srv.isEventOwnedLocally(context.Background(), "azylman/aerial", tc.prNum, tc.sha); got != tc.wantOwned {
				t.Fatalf("isEventOwnedLocally got %v, want %v", got, tc.wantOwned)
			}
		})
	}
}

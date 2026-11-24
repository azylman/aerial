package ollama

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type errRoundTripper struct{}

func (errRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("airgapped test transport: connection refused")
}

func TestNewDynamicClient_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cfg         Config
		nilCfgFn    bool
		wantBaseURL string
	}{
		{"static default values", Config{}, false, ""},
		{
			"static custom base url with trailing slash",
			Config{BaseURL: "http://senses.aerial:8080/", Model: "nomic-embed-text", NumCtx: 1024},
			false,
			"http://senses.aerial:8080",
		},
		{"dynamic with nil func", Config{}, true, ""},
		{"dynamic with valid func", Config{BaseURL: "http://pjev.aerial:8080"}, false, "http://pjev.aerial:8080"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var client *Client
			if tc.nilCfgFn {
				client = NewDynamicClient(nil)
			} else {
				client = NewDynamicClient(func() Config { return tc.cfg })
			}

			if got := client.config().BaseURL; got != tc.wantBaseURL {
				t.Errorf("config().BaseURL = %q, want %q", got, tc.wantBaseURL)
			}

			client.SetRetryDelay(5 * time.Millisecond)
			if client.retryDelay != 5*time.Millisecond {
				t.Errorf("retryDelay = %v, want 5ms", client.retryDelay)
			}
		})
	}
}

type embeddingGuardCase struct {
	name          string
	text          string
	emptyBaseURL  bool
	nilHTTPClient bool
	rt            http.RoundTripper
	cancelCtx     bool
	wantErrSubstr string
	wantEmbedding []float32
}

func TestGenerateEmbedding_GuardsAndTransport(t *testing.T) {
	t.Parallel()

	tests := []embeddingGuardCase{
		{name: "empty text validation error", text: "   ", wantErrSubstr: "text cannot be empty"},
		{name: "unconfigured base URL fails fast with zero network IO", text: "test prompt", emptyBaseURL: true, wantErrSubstr: "ollama base URL is not configured"},
		{name: "default http client fallback when nil", text: "default client check", nilHTTPClient: true, wantEmbedding: []float32{0.77}},
		{name: "transport error surfaces as request error", text: "test prompt", rt: errRoundTripper{}, wantErrSubstr: "request error"},
		{name: "canceled context aborts immediately before call", text: "context cancellation", cancelCtx: true, wantErrSubstr: "context canceled"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.cancelCtx {
				cancel()
			}

			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"embedding": [0.77]}`))
			}))
			defer server.Close()

			baseURL := server.URL
			if tc.emptyBaseURL {
				baseURL = ""
			}
			client := NewDynamicClient(func() Config {
				return Config{BaseURL: baseURL}
			})
			switch {
			case tc.rt != nil:
				client.SetHTTPClient(&http.Client{Transport: tc.rt})
			case tc.nilHTTPClient:
				client.SetHTTPClient(nil)
			default:
				client.SetHTTPClient(server.Client())
			}

			emb, err := client.GenerateEmbedding(ctx, tc.text, false, 1)
			if tc.wantErrSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
					t.Fatalf("GenerateEmbedding() error = %v, want substring %q", err, tc.wantErrSubstr)
				}
			} else {
				if err != nil {
					t.Fatalf("GenerateEmbedding() unexpected error = %v", err)
				}
				if !reflect.DeepEqual(emb, tc.wantEmbedding) {
					t.Errorf("GenerateEmbedding() got = %v, want = %v", emb, tc.wantEmbedding)
				}
			}
		})
	}
}

type generateEmbeddingTestCase struct {
	name          string
	text          string
	isQuery       bool
	maxRetries    int
	retryDelay    time.Duration
	cancelInDelay bool
	handlerStatus int
	handlerBody   string
	failAttempts  int
	wantErrSubstr string
	wantEmbedding []float32
	wantAttempts  int
}

func runGenerateEmbeddingCase(t *testing.T, tc generateEmbeddingTestCase) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attemptCount int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := atomic.AddInt64(&attemptCount, 1)

		if tc.isQuery {
			var req embeddingRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if !strings.HasPrefix(req.Prompt, "query: ") {
				http.Error(w, `expected prompt to have prefix "query: "`, http.StatusBadRequest)
				return
			}
		}

		if tc.cancelInDelay && idx == 1 {
			cancel()
		}

		if tc.failAttempts > 0 && int(idx) <= tc.failAttempts {
			http.Error(w, "server temporary failure", http.StatusServiceUnavailable)
			return
		}

		status := tc.handlerStatus
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(tc.handlerBody))
	}))
	defer server.Close()

	client := NewDynamicClient(func() Config {
		return Config{
			BaseURL:     server.URL,
			QueryPrefix: "query: ",
		}
	})
	client.SetHTTPClient(server.Client())
	client.SetRetryDelay(tc.retryDelay)

	emb, err := client.GenerateEmbedding(ctx, tc.text, tc.isQuery, tc.maxRetries)
	assertGenerateEmbeddingResult(t, tc, emb, err, attemptCount)
}

func assertGenerateEmbeddingResult(t *testing.T, tc generateEmbeddingTestCase, emb []float32, err error, attemptCount int64) {
	t.Helper()
	if tc.wantErrSubstr != "" {
		if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
			t.Fatalf("GenerateEmbedding() error = %v, want substring %q", err, tc.wantErrSubstr)
		}
	} else {
		if err != nil {
			t.Fatalf("GenerateEmbedding() unexpected error = %v", err)
		}
		if !reflect.DeepEqual(emb, tc.wantEmbedding) {
			t.Errorf("GenerateEmbedding() got = %v, want = %v", emb, tc.wantEmbedding)
		}
	}

	if tc.wantAttempts > 0 && int(attemptCount) != tc.wantAttempts {
		t.Errorf("server request attempts = %d, want = %d", attemptCount, tc.wantAttempts)
	}
}

func TestGenerateEmbedding_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []generateEmbeddingTestCase{
		{
			name: "successful embedding standard text", text: "search document query", maxRetries: 1, handlerBody: `{"embedding": [0.1, 0.2, 0.3]}`,
			wantEmbedding: []float32{0.1, 0.2, 0.3}, wantAttempts: 1,
		},
		{
			name: "successful embedding query prefix", text: "semantic search target", isQuery: true, maxRetries: 1,
			handlerBody: `{"embedding": [0.5, 0.6]}`, wantEmbedding: []float32{0.5, 0.6}, wantAttempts: 1,
		},
		{
			name: "client 400 error returns immediately without retry", text: "invalid payload triggering 400", maxRetries: 3,
			handlerStatus: http.StatusBadRequest, handlerBody: `{"error":"bad request payload"}`, wantErrSubstr: "ollama HTTP 400", wantAttempts: 1,
		},
		{
			name: "server 500 error retries with delay and succeeds", text: "retry with positive delay", maxRetries: 2, retryDelay: time.Microsecond,
			failAttempts: 1, handlerBody: `{"embedding": [0.42]}`, wantEmbedding: []float32{0.42}, wantAttempts: 2,
		},
		{
			name: "server 500 error default maxRetries 3 exhausted", text: "retry until failure default retries", retryDelay: time.Microsecond,
			handlerStatus: http.StatusInternalServerError, handlerBody: `{"error":"internal server error"}`, wantErrSubstr: "failed after 4 attempts", wantAttempts: 4,
		},
		{
			name: "empty embedding slice retried", text: "empty embedding response", maxRetries: 1, handlerBody: `{"embedding": []}`,
			wantErrSubstr: "empty embedding returned", wantAttempts: 2,
		},
		{
			name: "response has explicit error field", text: "model error", maxRetries: 1, handlerBody: `{"error": "model out of memory"}`,
			wantErrSubstr: "ollama error: model out of memory", wantAttempts: 2,
		},
		{name: "malformed json response", text: "garbage response", maxRetries: 1, handlerBody: `not-valid-json`, wantErrSubstr: "failed to parse JSON", wantAttempts: 2},
		{
			name: "cancel during retry delay", text: "cancel in retry delay", maxRetries: 2, retryDelay: 200 * time.Millisecond, cancelInDelay: true,
			failAttempts: 2, handlerStatus: http.StatusInternalServerError, handlerBody: `{"error":"fail"}`, wantErrSubstr: "context canceled", wantAttempts: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runGenerateEmbeddingCase(t, tc)
		})
	}
}

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

func TestNewClientAndAccessors_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cfg         Config
		useDynamic  bool
		nilCfgFn    bool
		wantBaseURL string
	}{
		{
			name:        "static default values",
			cfg:         Config{},
			useDynamic:  false,
			wantBaseURL: DefaultBaseURL,
		},
		{
			name: "static custom base url with trailing slash",
			cfg: Config{
				BaseURL: "http://custom-ollama:11434/",
				Model:   "nomic-embed-text",
				NumCtx:  1024,
			},
			useDynamic:  false,
			wantBaseURL: "http://custom-ollama:11434",
		},
		{
			name:        "dynamic with nil func",
			useDynamic:  true,
			nilCfgFn:    true,
			wantBaseURL: DefaultBaseURL,
		},
		{
			name: "dynamic with valid func",
			cfg: Config{
				BaseURL: "http://dynamic-host:11434",
			},
			useDynamic:  true,
			nilCfgFn:    false,
			wantBaseURL: "http://dynamic-host:11434",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var client *Client
			if tc.useDynamic {
				if tc.nilCfgFn {
					client = NewDynamicClient(nil)
				} else {
					client = NewDynamicClient(func() Config { return tc.cfg })
				}
			} else {
				client = NewClient(tc.cfg)
			}

			if got := client.BaseURL(); got != tc.wantBaseURL {
				t.Errorf("BaseURL() = %q, want %q", got, tc.wantBaseURL)
			}

			customHC := &http.Client{Timeout: 10 * time.Second}
			client.SetHTTPClient(customHC)
			if client.HTTPClient() != customHC {
				t.Errorf("HTTPClient() did not return configured client")
			}

			client.SetRetryDelay(5 * time.Millisecond)
			if client.RetryDelay() != 5*time.Millisecond {
				t.Errorf("RetryDelay() = %v, want 5ms", client.RetryDelay())
			}
		})
	}
}

func TestGenerateEmbedding_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		text          string
		isQuery       bool
		queryPrefix   string
		maxRetries    int
		retryDelay    time.Duration
		nilHttpClient bool
		cancelInDelay bool
		handlerStatus int
		handlerBody   string
		failAttempts  int
		cancelCtx     bool
		wantErrSubstr string
		wantEmbedding []float32
		wantAttempts  int
	}{
		{
			name:          "empty text validation error",
			text:          "   ",
			wantErrSubstr: "text cannot be empty",
		},
		{
			name:          "successful embedding standard text",
			text:          "search document query",
			isQuery:       false,
			maxRetries:    1,
			handlerStatus: http.StatusOK,
			handlerBody:   `{"embedding": [0.1, 0.2, 0.3]}`,
			wantEmbedding: []float32{0.1, 0.2, 0.3},
			wantAttempts:  1,
		},
		{
			name:          "successful embedding query prefix",
			text:          "semantic search target",
			isQuery:       true,
			queryPrefix:   "query: ",
			maxRetries:    1,
			handlerStatus: http.StatusOK,
			handlerBody:   `{"embedding": [0.5, 0.6]}`,
			wantEmbedding: []float32{0.5, 0.6},
			wantAttempts:  1,
		},
		{
			name:          "client 400 error returns immediately without retry",
			text:          "invalid payload triggering 400",
			maxRetries:    3,
			handlerStatus: http.StatusBadRequest,
			handlerBody:   `{"error":"bad request payload"}`,
			wantErrSubstr: "ollama HTTP 400",
			wantAttempts:  1,
		},
		{
			name:          "server 500 error retries with delay and succeeds",
			text:          "retry with positive delay",
			maxRetries:    2,
			retryDelay:    time.Microsecond,
			failAttempts:  1,
			handlerStatus: http.StatusOK,
			handlerBody:   `{"embedding": [0.42]}`,
			wantEmbedding: []float32{0.42},
			wantAttempts:  2,
		},
		{
			name:          "server 500 error default maxRetries 3 exhausted",
			text:          "retry until failure default retries",
			maxRetries:    0,
			retryDelay:    time.Microsecond,
			handlerStatus: http.StatusInternalServerError,
			handlerBody:   `{"error":"internal server error"}`,
			wantErrSubstr: "failed after 4 attempts",
			wantAttempts:  4,
		},
		{
			name:          "empty embedding slice retried",
			text:          "empty embedding response",
			maxRetries:    1,
			handlerStatus: http.StatusOK,
			handlerBody:   `{"embedding": []}`,
			wantErrSubstr: "empty embedding returned",
			wantAttempts:  2,
		},
		{
			name:          "response has explicit error field",
			text:          "model error",
			maxRetries:    1,
			handlerStatus: http.StatusOK,
			handlerBody:   `{"error": "model out of memory"}`,
			wantErrSubstr: "ollama error: model out of memory",
			wantAttempts:  2,
		},
		{
			name:          "malformed json response",
			text:          "garbage response",
			maxRetries:    1,
			handlerStatus: http.StatusOK,
			handlerBody:   `not-valid-json`,
			wantErrSubstr: "failed to parse JSON",
			wantAttempts:  2,
		},
		{
			name:          "canceled context aborts immediately before call",
			text:          "context cancellation",
			cancelCtx:     true,
			maxRetries:    2,
			handlerStatus: http.StatusInternalServerError,
			handlerBody:   `{"error":"temporary error"}`,
			wantErrSubstr: "context canceled",
			wantAttempts:  0,
		},
		{
			name:          "cancel during retry delay",
			text:          "cancel in retry delay",
			maxRetries:    2,
			retryDelay:    200 * time.Millisecond,
			cancelInDelay: true,
			failAttempts:  2,
			handlerStatus: http.StatusInternalServerError,
			handlerBody:   `{"error":"fail"}`,
			wantErrSubstr: "context canceled",
			wantAttempts:  1,
		},
		{
			name:          "default http client fallback when nil",
			text:          "default client check",
			maxRetries:    1,
			nilHttpClient: true,
			handlerStatus: http.StatusOK,
			handlerBody:   `{"embedding": [0.77]}`,
			wantEmbedding: []float32{0.77},
			wantAttempts:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var attemptCount int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				idx := atomic.AddInt64(&attemptCount, 1)

				if tc.isQuery && tc.queryPrefix != "" {
					var req embeddingRequest
					_ = json.NewDecoder(r.Body).Decode(&req)
					if !strings.HasPrefix(req.Prompt, tc.queryPrefix) {
						http.Error(w, fmt.Sprintf("expected prompt to have prefix %q", tc.queryPrefix), http.StatusBadRequest)
						return
					}
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

			client := NewClient(Config{
				BaseURL:     server.URL,
				QueryPrefix: tc.queryPrefix,
			})
			if !tc.nilHttpClient {
				client.SetHTTPClient(server.Client())
			} else {
				client.SetHTTPClient(nil)
			}
			client.SetRetryDelay(tc.retryDelay)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if tc.cancelCtx {
				cancel()
			}
			if tc.cancelInDelay {
				go func() {
					time.Sleep(10 * time.Millisecond)
					cancel()
				}()
			}

			emb, err := client.GenerateEmbedding(ctx, tc.text, tc.isQuery, tc.maxRetries)
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
		})
	}
}

func TestGenerateEmbedding_NetworkError_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		baseURL       string
		maxRetries    int
		wantErrSubstr string
	}{
		{
			name:          "unreachable connection refused",
			baseURL:       "http://127.0.0.1:59999",
			maxRetries:    1,
			wantErrSubstr: "request error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := NewClient(Config{
				BaseURL: tc.baseURL,
			})
			client.SetRetryDelay(0)

			_, err := client.GenerateEmbedding(context.Background(), "test prompt", false, tc.maxRetries)
			if err == nil || !strings.Contains(err.Error(), tc.wantErrSubstr) {
				t.Fatalf("GenerateEmbedding() error = %v, want substring %q", err, tc.wantErrSubstr)
			}
		})
	}
}

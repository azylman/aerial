package transcript

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

type roundTripperFunc func(req *http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func mockJSONResponse(statusCode int, body string) *http.Response {
	return &http.Response{
		StatusCode: statusCode,
		Body:       io.NopCloser(strings.NewReader(body)),
		Header:     make(http.Header),
	}
}

type mockCloseErrReader struct {
	io.Reader
}

func (m *mockCloseErrReader) Close() error {
	return errors.New("simulated close error")
}

func TestClient_NewEmbedderAndConfig(t *testing.T) {
	cfgCustom := config.NewFromData(&config.ConfigData{
		Ollama: config.OllamaConfig{
			BaseURL: "http://custom:11434/",
			Model:   "nomic-embed-text",
			NumCtx:  1024,
		},
	})
	cfgEmpty := config.NewFromData(nil)

	tests := []struct {
		name        string
		client      *Client
		wantBaseURL string
		wantRoots   []string
		wantNumCtx  int
	}{
		{"NewNilConfig", NewEmbedder(nil), DefaultOllamaURL, nil, 512},
		{"NewCustomConfig", NewEmbedder(cfgCustom, " /r1 ", "", " /r2 "), "http://custom:11434", []string{"/r1", "/r2"}, 1024},
		{"NilClient", nil, DefaultOllamaURL, nil, 512},
		{"EmptyClient", &Client{}, DefaultOllamaURL, nil, 512},
		{"EmptyDataConfig", NewEmbedder(cfgEmpty), DefaultOllamaURL, nil, 512},
		{"UninitializedConfig", NewEmbedder(&config.Config{}), DefaultOllamaURL, nil, 512},
		{"ClientWithNilConfig", &Client{cfg: nil}, DefaultOllamaURL, nil, 512},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.client.BaseURL(); got != tc.wantBaseURL {
				t.Errorf("BaseURL() = %q, want %q", got, tc.wantBaseURL)
			}
			roots := tc.client.Roots()
			if len(roots) != len(tc.wantRoots) {
				t.Fatalf("Roots() len = %d, want %d", len(roots), len(tc.wantRoots))
			}
			for i, r := range roots {
				if r != tc.wantRoots[i] {
					t.Errorf("Roots()[%d] = %q, want %q", i, r, tc.wantRoots[i])
				}
			}
			if len(roots) > 0 {
				roots[0] = "mutated"
				if tc.client.Roots()[0] == "mutated" {
					t.Errorf("Roots() is not a defensive copy")
				}
			}
			cfg := tc.client.getOllamaConfig()
			if cfg.NumCtx != tc.wantNumCtx {
				t.Errorf("getOllamaConfig().NumCtx = %d, want %d", cfg.NumCtx, tc.wantNumCtx)
			}
		})
	}
}

func TestGenerateEmbedding(t *testing.T) {
	tests := []struct {
		name         string
		text         string
		isQuery      bool
		maxRetries   int
		queryPrefix  string
		model        string
		numCtx       int
		httpStatus   int
		httpBody     string
		netErr       bool
		closeErr     bool
		cancelCtx    bool
		nilHTTP      bool
		badURL       bool
		wantLen      int
		wantErr      bool
		wantSubstr   string
		expectPrompt string
	}{
		{
			name:       "EmptyText",
			text:       "",
			wantErr:    true,
			wantSubstr: "text cannot be empty",
		},
		{
			name:         "SuccessDocument",
			text:         "hello world",
			isQuery:      false,
			maxRetries:   1,
			httpStatus:   http.StatusOK,
			httpBody:     `{"embedding":[0.1, 0.2, 0.3]}`,
			wantLen:      3,
			expectPrompt: "hello world",
		},
		{
			name:         "SuccessQueryWithPrefix",
			text:         "user search",
			isQuery:      true,
			queryPrefix:  "query: ",
			httpStatus:   http.StatusOK,
			httpBody:     `{"embedding":[0.5, 0.6]}`,
			wantLen:      2,
			expectPrompt: "query: user search",
		},
		{
			name:         "SuccessCustomModelAndCtx",
			text:         "custom test",
			model:        "custom-model",
			numCtx:       1024,
			httpStatus:   http.StatusOK,
			httpBody:     `{"embedding":[1.0]}`,
			wantLen:      1,
			expectPrompt: "custom test",
		},
		{
			name:       "HTTP500Error",
			text:       "fail 500",
			httpStatus: http.StatusInternalServerError,
			httpBody:   "internal error",
			wantErr:    true,
			wantSubstr: "ollama HTTP 500",
		},
		{
			name:       "APIErrorResponse",
			text:       "fail api error",
			httpStatus: http.StatusOK,
			httpBody:   `{"error":"model not loaded"}`,
			wantErr:    true,
			wantSubstr: "model not loaded",
		},
		{
			name:       "EmptyEmbeddingResponse",
			text:       "fail empty emb",
			httpStatus: http.StatusOK,
			httpBody:   `{"embedding":[]}`,
			wantErr:    true,
			wantSubstr: "empty embedding returned",
		},
		{
			name:       "InvalidJSON",
			text:       "fail json",
			httpStatus: http.StatusOK,
			httpBody:   "not-valid-json",
			wantErr:    true,
		},
		{
			name:       "NetworkErrorNegativeRetries",
			text:       "fail net",
			maxRetries: -1,
			netErr:     true,
			wantErr:    true,
			wantSubstr: "connection refused",
		},
		{
			name:      "ContextCancelled",
			text:      "cancelled",
			cancelCtx: true,
			wantErr:   true,
		},
		{
			name:       "BodyCloseErrorIgnored",
			text:       "close err",
			httpStatus: http.StatusOK,
			httpBody:   `{"embedding":[0.7, 0.8]}`,
			closeErr:   true,
			wantLen:    2,
		},
		{
			name:      "NilHTTPClientFallbackCancelled",
			text:      "nil http",
			nilHTTP:   true,
			cancelCtx: true,
			wantErr:   true,
		},
		{
			name:    "BadURLRequestError",
			text:    "bad url",
			badURL:  true,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var receivedPrompt string
			rt := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if tc.netErr {
					return nil, errors.New("connection refused")
				}
				var reqBody embeddingRequest
				_ = json.NewDecoder(req.Body).Decode(&reqBody)
				receivedPrompt = reqBody.Prompt

				resp := mockJSONResponse(tc.httpStatus, tc.httpBody)
				if tc.closeErr {
					resp.Body = &mockCloseErrReader{Reader: strings.NewReader(tc.httpBody)}
				}
				return resp, nil
			})

			baseURL := "http://mock-ollama:11434"
			if tc.badURL {
				baseURL = "http://\x7f"
			}
			cfg := config.NewFromData(&config.ConfigData{
				Ollama: config.OllamaConfig{
					BaseURL:     baseURL,
					Model:       tc.model,
					QueryPrefix: tc.queryPrefix,
					NumCtx:      tc.numCtx,
				},
			})
			client := NewEmbedder(cfg)
			client.httpClient = &http.Client{Transport: rt}
			client.retryDelay = -1

			if tc.nilHTTP {
				client.httpClient = nil
			}

			ctx := context.Background()
			if tc.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}

			emb, err := client.GenerateEmbedding(ctx, tc.text, tc.isQuery, tc.maxRetries)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				if tc.wantSubstr != "" && !strings.Contains(err.Error(), tc.wantSubstr) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.wantSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(emb) != tc.wantLen {
				t.Errorf("len(emb) = %d, want %d", len(emb), tc.wantLen)
			}
			if tc.expectPrompt != "" && receivedPrompt != tc.expectPrompt {
				t.Errorf("prompt = %q, want %q", receivedPrompt, tc.expectPrompt)
			}
		})
	}
}

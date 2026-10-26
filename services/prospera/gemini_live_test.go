package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestDialGeminiLive(t *testing.T) {
	t.Parallel()

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.RawQuery, "key=my-test-key") {
			http.Error(w, "missing key", http.StatusUnauthorized)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
	}))
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	tests := []struct {
		name    string
		url     string
		apiKey  string
		wantErr bool
	}{
		{
			name:    "successful dial with key param appended",
			url:     wsURL,
			apiKey:  "my-test-key",
			wantErr: false,
		},
		{
			name:    "successful dial with existing query param",
			url:     wsURL + "?foo=bar",
			apiKey:  "my-test-key",
			wantErr: false,
		},
		{
			name:    "unauthorized dial error",
			url:     wsURL,
			apiKey:  "wrong-key",
			wantErr: true,
		},
		{
			name:    "invalid url dial error",
			url:     "http://invalid.url:99999/ws",
			apiKey:  "key",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			conn, err := DialGeminiLive(context.Background(), tc.url, tc.apiKey)
			if (err != nil) != tc.wantErr {
				t.Fatalf("DialGeminiLive(%q, %q) err = %v, wantErr = %v", tc.url, tc.apiKey, err, tc.wantErr)
			}
			if conn != nil {
				_ = conn.Close()
			}
		})
	}
}

func TestBuildGeminiMessages(t *testing.T) {
	t.Parallel()

	// 1. Setup message
	msg1 := BuildGeminiSetupMessage("gemini-3.8-live", "system instructions")
	if msg1.Setup.Model != "models/gemini-3.8-live" {
		t.Errorf("model = %q, want models/gemini-3.8-live", msg1.Setup.Model)
	}
	if len(msg1.Setup.Tools) == 0 || len(msg1.Setup.Tools[0].FunctionDeclarations) != 4 {
		t.Errorf("expected 4 tool declarations, got %d", len(msg1.Setup.Tools[0].FunctionDeclarations))
	}
	if msg1.Setup.InputAudioTranscription == nil || msg1.Setup.OutputAudioTranscription == nil {
		t.Error("expected InputAudioTranscription and OutputAudioTranscription enabled in setup")
	}

	// 2. Audio chunk
	chunk := BuildGeminiAudioChunk([]byte{0x01, 0x02})
	if len(chunk.RealtimeInput.MediaChunks) == 0 || chunk.RealtimeInput.MediaChunks[0].Data == "" {
		t.Error("expected non-empty audio chunk")
	}

	// 3. Client content
	content := BuildGeminiClientContent("instruction", "")
	if len(content.ClientContent.Turns) == 0 || content.ClientContent.Turns[0].Role != "user" {
		t.Error("expected user role client content")
	}

	// 4. Tool response
	toolResp := BuildGeminiToolResponse("call_1", map[string]any{"result": "ok"})
	if len(toolResp.ToolResponse.FunctionResponses) == 0 || toolResp.ToolResponse.FunctionResponses[0].ID != "call_1" {
		t.Error("expected function response for call_1")
	}
}

func TestHTTPContextConsultClient_ConsultCalendar(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		overrideURL string
		statusCode  int
		respBody    string
		wantSubstr  string
		wantErr     bool
	}{
		{
			name:       "calendar multi-line",
			statusCode: http.StatusOK,
			respBody:   `{"jsonrpc":"2.0","id":1,"result":{"content":[{"type":"text","text":"Line 1"},{"type":"text","text":"Line 2"}]}}`,
			wantSubstr: "Line 1\nLine 2",
		},
		{
			name:       "calendar 500 error",
			statusCode: http.StatusInternalServerError,
			respBody:   "internal server error",
			wantErr:    true,
		},
		{
			name:       "calendar RPC error payload",
			statusCode: http.StatusOK,
			respBody:   `{"jsonrpc":"2.0","id":1,"error":{"message":"tool execution failed"}}`,
			wantErr:    true,
		},
		{
			name:       "calendar empty results",
			statusCode: http.StatusOK,
			respBody:   `{"jsonrpc":"2.0","id":1,"result":{"content":[]}}`,
			wantSubstr: "No matching",
		},
		{
			name:       "calendar bad json payload",
			statusCode: http.StatusOK,
			respBody:   `not-json`,
			wantErr:    true,
		},
		{
			name:        "calendar network error",
			overrideURL: "http://127.0.0.1:1",
			wantErr:     true,
		},
		{
			name:        "calendar request creation error",
			overrideURL: ":\x7f",
			wantErr:     true,
		},
		{
			name:        "calendar empty url returns unavailable",
			overrideURL: "none",
			wantSubstr:  "unavailable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calURL := tc.overrideURL
			if tc.overrideURL == "none" {
				calURL = ""
			} else if calURL == "" {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if tc.statusCode != 0 {
						w.WriteHeader(tc.statusCode)
					}
					_, _ = w.Write([]byte(tc.respBody))
				}))
				t.Cleanup(server.Close)
				calURL = server.URL
			}

			client := NewHTTPContextConsultClient(calURL, "")
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			res, err := client.ConsultCalendar(ctx, "q", "", "")
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantSubstr != "" && !strings.Contains(res, tc.wantSubstr) {
				t.Fatalf("result %q does not contain %q", res, tc.wantSubstr)
			}
		})
	}
}

func TestHTTPContextConsultClient_ConsultMemory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		overrideURL string
		statusCode  int
		respBody    string
		wantSubstr  string
		wantErr     bool
	}{
		{
			name:       "memory success",
			statusCode: http.StatusOK,
			respBody:   "Fact 1: User prefers coffee\nFact 2: Office on 3rd floor",
			wantSubstr: "User prefers coffee",
		},
		{
			name:       "memory 500 error",
			statusCode: http.StatusInternalServerError,
			respBody:   "internal error",
			wantErr:    true,
		},
		{
			name:       "memory empty body",
			statusCode: http.StatusOK,
			respBody:   "   ",
			wantSubstr: "No relevant",
		},
		{
			name:        "memory network error",
			overrideURL: "http://127.0.0.1:1",
			wantErr:     true,
		},
		{
			name:        "memory request creation error",
			overrideURL: ":\x7f",
			wantErr:     true,
		},
		{
			name:        "memory empty url returns unavailable",
			overrideURL: "none",
			wantSubstr:  "unavailable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			memURL := tc.overrideURL
			if tc.overrideURL == "none" {
				memURL = ""
			} else if memURL == "" {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "text/plain")
					if tc.statusCode != 0 {
						w.WriteHeader(tc.statusCode)
					}
					_, _ = w.Write([]byte(tc.respBody))
				}))
				t.Cleanup(server.Close)
				memURL = server.URL
			}

			client := NewHTTPContextConsultClient("", memURL)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()

			res, err := client.ConsultMemory(ctx, "q")
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantSubstr != "" && !strings.Contains(res, tc.wantSubstr) {
				t.Fatalf("result %q does not contain %q", res, tc.wantSubstr)
			}
		})
	}
}

func WithConsultHTTPClient(client *http.Client) ConsultOption {
	return func(c *HTTPContextConsultClient) {
		c.httpClient = client
	}
}

func TestWithConsultHTTPClient(t *testing.T) {
	t.Parallel()
	hc := &http.Client{}
	client := NewHTTPContextConsultClient("http://cal", "http://mem", WithConsultHTTPClient(hc))
	if client.httpClient != hc {
		t.Errorf("httpClient = %v, want %v", client.httpClient, hc)
	}
}

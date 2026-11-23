package gemini

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/services/prospera/internal/fakes"
)

func TestMain(m *testing.M) {
	fakes.InstallTestTracing()
	os.Exit(m.Run())
}

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
	if msg1.Setup.GenerationConfig.SpeechConfig.VoiceConfig.PrebuiltVoiceConfig.VoiceName != "Kore" {
		t.Errorf("voice = %q, want Kore", msg1.Setup.GenerationConfig.SpeechConfig.VoiceConfig.PrebuiltVoiceConfig.VoiceName)
	}
	if msg1.Setup.GenerationConfig.SpeechConfig.LanguageCode != "en-US" {
		t.Errorf("languageCode = %q, want en-US", msg1.Setup.GenerationConfig.SpeechConfig.LanguageCode)
	}

	msgNative := BuildGeminiSetupMessage("gemini-2.5-flash-native-audio-latest", "system instructions", "   ", "   ")
	if !msgNative.Setup.GenerationConfig.EnableAffectiveDialog {
		t.Error("expected EnableAffectiveDialog=true for native-audio model")
	}
	if msgNative.Setup.GenerationConfig.SpeechConfig.VoiceConfig.PrebuiltVoiceConfig.VoiceName != "Kore" {
		t.Errorf("fallback voice = %q, want Kore", msgNative.Setup.GenerationConfig.SpeechConfig.VoiceConfig.PrebuiltVoiceConfig.VoiceName)
	}
	if msgNative.Setup.GenerationConfig.SpeechConfig.LanguageCode != "en-US" {
		t.Errorf("fallback languageCode = %q, want en-US", msgNative.Setup.GenerationConfig.SpeechConfig.LanguageCode)
	}

	msgCustomVoice := BuildGeminiSetupMessage("models/gemini-3.8-live", "system instructions", "Aoede", "es-US")
	if msgCustomVoice.Setup.GenerationConfig.SpeechConfig.VoiceConfig.PrebuiltVoiceConfig.VoiceName != "Aoede" {
		t.Errorf("custom voice = %q, want Aoede", msgCustomVoice.Setup.GenerationConfig.SpeechConfig.VoiceConfig.PrebuiltVoiceConfig.VoiceName)
	}
	if msgCustomVoice.Setup.GenerationConfig.SpeechConfig.LanguageCode != "es-US" {
		t.Errorf("custom languageCode = %q, want es-US", msgCustomVoice.Setup.GenerationConfig.SpeechConfig.LanguageCode)
	}

	// 2. Audio chunk
	chunk := BuildGeminiAudioChunk([]byte{0x01, 0x02})
	if chunk.RealtimeInput.Audio == nil || chunk.RealtimeInput.Audio.Data == "" || chunk.RealtimeInput.Audio.MimeType != "audio/pcm;rate=16000" {
		t.Error("expected non-empty realtimeInput.audio chunk with 16kHz PCM mimeType")
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

// newCalendarMCPServer serves google-mcp's get_events tool from an in-process
// go-sdk streamable server. It replies with parts (isError when fail is set)
// and records the arguments of the last call.
func newCalendarMCPServer(t *testing.T, parts []string, fail bool, gotArgs *atomic.Value) string {
	t.Helper()
	srv := mcp.NewServer(&mcp.Implementation{Name: "fake-google-mcp", Version: "1.0"}, nil)
	srv.AddTool(&mcp.Tool{Name: "get_events", InputSchema: map[string]any{"type": "object"}}, func(_ context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		var args map[string]any
		if len(req.Params.Arguments) > 0 {
			if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
				t.Errorf("json.Unmarshal arguments: %v", err)
			}
		}
		gotArgs.Store(args)
		content := make([]mcp.Content, 0, len(parts))
		for _, p := range parts {
			content = append(content, &mcp.TextContent{Text: p})
		}
		return &mcp.CallToolResult{IsError: fail, Content: content}, nil
	})
	ts := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil))
	t.Cleanup(ts.Close)
	return ts.URL
}

func TestHTTPContextConsultClient_ConsultCalendar(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		url         string
		startISO    string
		endISO      string
		parts       []string
		fail        bool
		wantText    string
		wantErr     string
		wantTimeMin string
		wantTimeMax string
		wantSpan    time.Duration
		wantNoCall  bool
	}{
		{name: "explicit range trimmed and forwarded, parts joined", startISO: " 2026-11-01T00:00:00Z ", endISO: "2026-11-02T00:00:00Z", parts: []string{"Line 1", "Line 2"}, wantText: "Line 1Line 2", wantTimeMin: "2026-11-01T00:00:00Z", wantTimeMax: "2026-11-02T00:00:00Z", wantSpan: 24 * time.Hour},
		{name: "default range is the next 30 days", parts: []string{"Sunday is free"}, wantText: "Sunday is free", wantSpan: 720 * time.Hour},
		{name: "empty result falls back", startISO: "2026-11-01T00:00:00Z", endISO: "2026-11-02T00:00:00Z", wantText: "No matching calendar events found.", wantTimeMin: "2026-11-01T00:00:00Z", wantTimeMax: "2026-11-02T00:00:00Z", wantSpan: 24 * time.Hour},
		{name: "isError tool result is an error", startISO: "2026-11-01T00:00:00Z", endISO: "2026-11-02T00:00:00Z", parts: []string{"quota exceeded"}, fail: true, wantErr: "tool returned error: quota exceeded", wantTimeMin: "2026-11-01T00:00:00Z", wantTimeMax: "2026-11-02T00:00:00Z", wantSpan: 24 * time.Hour},
		{name: "closed endpoint fails session init", url: "closed", wantErr: "session init failed", wantNoCall: true},
		{name: "empty url returns unavailable", url: "none", wantText: "Calendar consultation unavailable (no Google MCP configured)", wantNoCall: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotArgs atomic.Value
			calURL := newCalendarMCPServer(t, tc.parts, tc.fail, &gotArgs)
			switch tc.url {
			case "none":
				calURL = ""
			case "closed":
				dead := httptest.NewServer(http.NotFoundHandler())
				dead.Close()
				calURL = dead.URL
			}
			client := NewHTTPContextConsultClient(calURL, "")
			t.Cleanup(func() {
				if err := client.Close(); err != nil {
					t.Errorf("Close() err = %v", err)
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			res, err := client.ConsultCalendar(ctx, "q", tc.startISO, tc.endISO)
			assertCalendarResult(t, res, err, tc.wantText, tc.wantErr)
			args, _ := gotArgs.Load().(map[string]any)
			assertGetEventsArgs(t, args, tc.wantNoCall, tc.wantTimeMin, tc.wantTimeMax, tc.wantSpan)
		})
	}
}

func assertCalendarResult(t *testing.T, res string, err error, wantText, wantErr string) {
	t.Helper()
	if wantErr != "" {
		if err == nil || !strings.Contains(err.Error(), wantErr) {
			t.Fatalf("ConsultCalendar() err = %v, want containing %q", err, wantErr)
		}
		return
	}
	if err != nil || res != wantText {
		t.Fatalf("ConsultCalendar() = (%q, %v), want %q", res, err, wantText)
	}
}

// assertGetEventsArgs checks the recorded get_events arguments: exact bounds
// when given, max_results 25, and an RFC3339 range spanning wantSpan.
func assertGetEventsArgs(t *testing.T, args map[string]any, wantNoCall bool, wantTimeMin, wantTimeMax string, wantSpan time.Duration) {
	t.Helper()
	if wantNoCall {
		if args != nil {
			t.Fatalf("get_events called with %v, want no call", args)
		}
		return
	}
	tMin, _ := args["time_min"].(string)
	tMax, _ := args["time_max"].(string)
	if (wantTimeMin != "" && tMin != wantTimeMin) || (wantTimeMax != "" && tMax != wantTimeMax) || args["max_results"] != float64(25) {
		t.Fatalf("get_events args = %v, want time_min %q time_max %q max_results 25", args, wantTimeMin, wantTimeMax)
	}
	start, errMin := time.Parse(time.RFC3339, tMin)
	end, errMax := time.Parse(time.RFC3339, tMax)
	if errMin != nil || errMax != nil || end.Sub(start) != wantSpan {
		t.Fatalf("get_events range %q..%q, want RFC3339 span %v", tMin, tMax, wantSpan)
	}
}

func TestHTTPContextConsultClient_Close(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		client *HTTPContextConsultClient
	}{
		{name: "nil client", client: nil},
		{name: "zero value without mcp client", client: &HTTPContextConsultClient{}},
		{name: "constructed without a session", client: NewHTTPContextConsultClient("http://127.0.0.1:1", "")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.client.Close(); err != nil {
				t.Fatalf("Close() err = %v", err)
			}
		})
	}
}

func TestHTTPContextConsultClient_ConsultMemory(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		overrideURL  string
		statusCode   int
		respBody     string
		truncateBody bool
		wantSubstr   string
		wantErr      bool
	}{
		{name: "memory success", statusCode: http.StatusOK, respBody: "Fact 1: User prefers coffee\nFact 2: Office on 3rd floor", wantSubstr: "User prefers coffee"},
		{name: "memory 500 error", statusCode: http.StatusInternalServerError, respBody: "internal error", wantErr: true},
		{name: "memory empty body", statusCode: http.StatusOK, respBody: "   ", wantSubstr: "No relevant"},
		{name: "memory network error", overrideURL: "http://127.0.0.1:1", wantErr: true},
		{name: "memory request creation error", overrideURL: ":\x7f", wantErr: true},
		{name: "memory empty url returns unavailable", overrideURL: "none", wantSubstr: "unavailable"},
		{name: "memory truncated body read error", statusCode: http.StatusOK, respBody: `{"partial`, truncateBody: true, wantErr: true},
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
					if tc.truncateBody {
						w.Header().Set("Content-Length", "100")
					}
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

func withTestCovenURL(url string) ConsultOption {
	return func(c *HTTPContextConsultClient) { c.covenURL = url }
}

func TestNewHTTPContextConsultClient_TraceHeaders(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		withParent bool
	}{
		{name: "coven consult continues the caller trace", withParent: true},
		{name: "coven consult roots a trace without a caller"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got http.Header
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Clone()
				_, _ = w.Write([]byte("remembered"))
			}))
			t.Cleanup(srv.Close)
			ctx, parentTraceID := fakes.TraceCaller(tc.withParent)
			client := NewHTTPContextConsultClient("", "", withTestCovenURL(srv.URL))
			if _, err := client.ConsultMemory(ctx, "dentist"); err != nil {
				t.Fatalf("ConsultMemory: %v", err)
			}
			id := fakes.HeaderTraceID(got)
			if id == "" || (id == parentTraceID) != tc.withParent {
				t.Errorf("traceparent %q, caller trace %q, want continues=%v", got.Get("Traceparent"), parentTraceID, tc.withParent)
			}
		})
	}
}

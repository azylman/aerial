package httpkit

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestNormalizeMCPToolPrefix_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		body      string
		oldPrefix string
		newPrefix string
		wantBody  string
	}{
		{
			name:      "empty old prefix returns untouched",
			body:      `{"method":"tools/call","params":{"name":"scheduler_run"}}`,
			oldPrefix: "",
			newPrefix: "",
			wantBody:  `{"method":"tools/call","params":{"name":"scheduler_run"}}`,
		},
		{
			name:      "body does not contain prefix",
			body:      `{"method":"tools/call","params":{"name":"unrelated_tool"}}`,
			oldPrefix: "scheduler_",
			newPrefix: "",
			wantBody:  `{"method":"tools/call","params":{"name":"unrelated_tool"}}`,
		},
		{
			name:      "compact name without space",
			body:      `{"method":"tools/call","params":{"name":"memory_memory_search"}}`,
			oldPrefix: "memory_memory_",
			newPrefix: "memory_",
			wantBody:  `{"method":"tools/call","params":{"name":"memory_search"}}`,
		},
		{
			name:      "single space after colon",
			body:      `{"method":"tools/call","params":{"name": "scheduler_schedule_once"}}`,
			oldPrefix: "scheduler_",
			newPrefix: "",
			wantBody:  `{"method":"tools/call","params":{"name": "schedule_once"}}`,
		},
		{
			name:      "double space after colon",
			body:      `{"method":"tools/call","params":{"name":  "scheduler_schedule_once"}}`,
			oldPrefix: "scheduler_",
			newPrefix: "",
			wantBody:  `{"method":"tools/call","params":{"name": "schedule_once"}}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := NormalizeMCPToolPrefix([]byte(tc.body), tc.oldPrefix, tc.newPrefix)
			if string(got) != tc.wantBody {
				t.Errorf("NormalizeMCPToolPrefix() = %s, want %s", string(got), tc.wantBody)
			}
		})
	}
}

type serveMCPStreamTestCase struct {
	name           string
	method         string
	body           string
	readyBanner    string
	oldToolPrefix  string
	newToolPrefix  string
	wantStatus     int
	wantBodySubstr string
	wantHeaderCT   string
	startSpan      string
	wantSpanName   string
	wantSpanAttrs  map[string]string
}

func runServeMCPStreamCase(t *testing.T, tc serveMCPStreamTestCase) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()), sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("tp.Shutdown error: %v", err)
		}
	})

	streamHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if strings.Contains(string(b), `"name":"list"`) {
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte("invoked:list")); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("stream-handled")); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	req := httptest.NewRequest(tc.method, "/mcp", strings.NewReader(tc.body))
	rec := httptest.NewRecorder()
	if tc.startSpan != "" {
		ctx, span := tp.Tracer("test").Start(req.Context(), tc.startSpan)
		req = req.WithContext(ctx)
		ServeMCPStream(rec, req, streamHandler, tc.readyBanner, tc.oldToolPrefix, tc.newToolPrefix)
		span.End()
	} else {
		ServeMCPStream(rec, req, streamHandler, tc.readyBanner, tc.oldToolPrefix, tc.newToolPrefix)
	}

	assertServeMCPStreamResult(t, tc, rec, sr)
}

func assertServeMCPStreamResult(t *testing.T, tc serveMCPStreamTestCase, rec *httptest.ResponseRecorder, sr *tracetest.SpanRecorder) {
	t.Helper()
	spans := sr.Ended()
	if tc.startSpan == "" && len(spans) != 0 {
		t.Errorf("ServeMCPStream without an active span recorded %d spans, want 0", len(spans))
	}
	if tc.startSpan != "" {
		if len(spans) != 1 {
			t.Fatalf("recorded %d spans, want 1", len(spans))
		}
		if spans[0].Name() != tc.wantSpanName {
			t.Errorf("span name = %q, want %q", spans[0].Name(), tc.wantSpanName)
		}
		got := map[string]string{}
		for _, kv := range spans[0].Attributes() {
			got[string(kv.Key)] = kv.Value.AsString()
		}
		if !reflect.DeepEqual(got, tc.wantSpanAttrs) {
			t.Errorf("span attrs = %v, want %v", got, tc.wantSpanAttrs)
		}
	}

	if rec.Code != tc.wantStatus {
		t.Errorf("ServeMCPStream status = %d, want %d", rec.Code, tc.wantStatus)
	}
	if !strings.Contains(rec.Body.String(), tc.wantBodySubstr) {
		t.Errorf("ServeMCPStream body = %q, want substring %q", rec.Body.String(), tc.wantBodySubstr)
	}
	if tc.wantHeaderCT != "" && rec.Header().Get("Content-Type") != tc.wantHeaderCT {
		t.Errorf("ServeMCPStream Content-Type = %q, want %q", rec.Header().Get("Content-Type"), tc.wantHeaderCT)
	}
}

func TestServeMCPStream_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []serveMCPStreamTestCase{
		{
			name:           "get readiness banner",
			method:         http.MethodGet,
			readyBanner:    "Memory Service Ready\n",
			wantStatus:     http.StatusOK,
			wantBodySubstr: "Memory Service Ready\n",
			wantHeaderCT:   "text/plain; charset=utf-8",
		},
		{
			name:           "put disallowed method",
			method:         http.MethodPut,
			wantStatus:     http.StatusMethodNotAllowed,
			wantBodySubstr: "Method not allowed",
		},
		{
			name:           "post empty body rejected",
			method:         http.MethodPost,
			body:           "   ",
			wantStatus:     http.StatusBadRequest,
			wantBodySubstr: "Empty request body",
		},
		{
			name:           "post valid body with tool normalization",
			method:         http.MethodPost,
			body:           `{"jsonrpc":"2.0","method":"tools/call","params":{"name":"scheduler_list"}}`,
			oldToolPrefix:  "scheduler_",
			newToolPrefix:  "",
			wantStatus:     http.StatusOK,
			wantBodySubstr: "invoked:list",
		},
		{
			name:           "tools/call annotates active span without arguments",
			method:         http.MethodPost,
			body:           `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"scheduler_list","arguments":{"note":"ghp_leak123"}}}`,
			oldToolPrefix:  "scheduler_",
			wantStatus:     http.StatusOK,
			wantBodySubstr: "invoked:list",
			startSpan:      "POST /mcp",
			wantSpanName:   "mcp tools/call",
			wantSpanAttrs:  map[string]string{"mcp.method.name": "tools/call", "gen_ai.tool.name": "list"},
		},
		{
			name:           "non-tool method sets method only",
			method:         http.MethodPost,
			body:           `{"jsonrpc":"2.0","id":2,"method":"initialize","params":{"clientInfo":{"name":"agy"}}}`,
			wantStatus:     http.StatusOK,
			wantBodySubstr: "stream-handled",
			startSpan:      "POST /mcp",
			wantSpanName:   "mcp initialize",
			wantSpanAttrs:  map[string]string{"mcp.method.name": "initialize"},
		},
		{
			name:           "unparseable body leaves span untouched",
			method:         http.MethodPost,
			body:           `not json`,
			wantStatus:     http.StatusOK,
			wantBodySubstr: "stream-handled",
			startSpan:      "POST /mcp",
			wantSpanName:   "POST /mcp",
			wantSpanAttrs:  map[string]string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runServeMCPStreamCase(t, tc)
		})
	}
}

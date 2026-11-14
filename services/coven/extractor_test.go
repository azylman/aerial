package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"
)

func TestHTTPBrainLLMClient(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		statusCode int
		body       string
		prompt     string
		rawURL     string
		parent     bool
		wantErr    bool
		wantResp   string
	}{
		{
			name:       "TraceparentPropagatedFromParentSpan",
			statusCode: http.StatusOK,
			body:       `{"response":"ok"}`,
			prompt:     "hello",
			parent:     true,
			wantResp:   "ok",
		},
		{
			name:       "Success200",
			statusCode: http.StatusOK,
			body:       `{"response":"extracted fact output"}`,
			prompt:     "hello",
			wantResp:   "extracted fact output",
		},
		{
			name:       "Non200Status",
			statusCode: http.StatusBadRequest,
			body:       `error prompt too long`,
			prompt:     "hello",
			wantErr:    true,
		},
		{
			name:       "InvalidJSONResponse",
			statusCode: http.StatusOK,
			body:       `not-json`,
			prompt:     "hello",
			wantErr:    true,
		},
		{
			name:       "DefaultURLFallback",
			statusCode: http.StatusOK,
			rawURL:     "http://127.0.0.1:0/bad",
			prompt:     "hello",
			wantErr:    true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var srv *httptest.Server
			var gotTraceparent string
			if tc.statusCode != 0 {
				srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					gotTraceparent = r.Header.Get("Traceparent")
					if r.Header.Get("Content-Type") != "application/json" {
						http.Error(w, "bad content type", http.StatusBadRequest)
						return
					}
					var req brainLLMRequest
					_ = json.NewDecoder(r.Body).Decode(&req)
					w.WriteHeader(tc.statusCode)
					_, _ = w.Write([]byte(tc.body))
				}))
				defer srv.Close()
			}

			targetURL := tc.rawURL
			if srv != nil {
				targetURL = srv.URL
			}
			callCtx := ctx
			const parentTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
			if tc.parent {
				traceID, _ := trace.TraceIDFromHex(parentTraceID)
				spanID, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
				callCtx = trace.ContextWithSpanContext(ctx, trace.NewSpanContext(trace.SpanContextConfig{
					TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled,
				}))
			}
			fn := HTTPBrainLLMClient(targetURL, nil)
			resp, err := fn(callCtx, tc.prompt)
			if tc.parent && !strings.HasPrefix(gotTraceparent, "00-"+parentTraceID+"-") {
				t.Errorf("traceparent = %q, want trace ID %s", gotTraceparent, parentTraceID)
			}
			if (err != nil) != tc.wantErr {
				t.Fatalf("HTTPBrainLLMClient() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr && resp != tc.wantResp {
				t.Errorf("resp = %q, want %q", resp, tc.wantResp)
			}
		})
	}
}

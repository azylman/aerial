package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/azylman/aerial/pkg/tracing"
)

func TestInitPostgres(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	srv := &VanadisServer{}

	// invalid postgres URL
	_, err := initPostgres(ctx, srv, ":::invalid-url")
	if err == nil {
		t.Fatal("expected error on invalid postgres url")
	}
}

func TestVanadisServer_StoreHTTPEndpointErrors(t *testing.T) {
	t.Parallel()

	sErr := newMemoryVanadisStore()
	sErr.getErr = errors.New("simulated get err")

	srv := &VanadisServer{store: sErr}
	mux := srv.Routes()

	doReq := func(method, path, body string) int {
		var r *http.Request
		if body != "" {
			r = httptest.NewRequest(method, path, strings.NewReader(body))
		} else {
			r = httptest.NewRequest(method, path, http.NoBody)
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, r)
		return rec.Code
	}

	if code := doReq(http.MethodPost, "/chunks/attach-pr", `{"repo":"aerial","pr_number":101}`); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on AttachChunkPR err, got %d", code)
	}
	if code := doReq(http.MethodGet, "/chunks/c1", ""); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on GetChunk err, got %d", code)
	}
	if code := doReq(http.MethodGet, "/builds/b1", ""); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on GetBuild err, got %d", code)
	}
	if code := doReq(http.MethodPost, "/builds/b1/cancel", ""); code != http.StatusInternalServerError {
		t.Errorf("expected 500 on CancelBuild err, got %d", code)
	}
}

func TestValidVanadisChunkID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		id        string
		wantValid bool
	}{
		{
			name:      "valid",
			id:        "valid-chunk_1",
			wantValid: true,
		},
		{
			name:      "empty",
			id:        "",
			wantValid: false,
		},
		{
			name:      "too_long",
			id:        strings.Repeat("a", 121),
			wantValid: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isValidVanadisChunkID(tc.id); got != tc.wantValid {
				t.Fatalf("isValidVanadisChunkID(%q) = %v, want %v", tc.id, got, tc.wantValid)
			}
		})
	}
}

func TestRoutes_ServerSpanPropagatesToRestate(t *testing.T) {
	// Not parallel: swaps the global tracer provider and propagator.
	prevTP, prevProp := otel.GetTracerProvider(), otel.GetTextMapPropagator()
	t.Cleanup(func() { otel.SetTracerProvider(prevTP); otel.SetTextMapPropagator(prevProp) })
	otel.SetTracerProvider(sdktrace.NewTracerProvider())
	otel.SetTextMapPropagator(propagation.TraceContext{})

	tests := []struct {
		name            string
		traceparent     string
		wantTraceID     string
		wantStatus      int
		wantRestateCall bool
	}{
		{"inbound trace continues into restate", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "4bf92f3577b34da6a3ce929d0e0e4736", http.StatusAccepted, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var gotTP string
			restate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotTP = r.Header.Get("Traceparent")
				_, _ = w.Write([]byte(`{"id":"b1","thread_id":"123456789012345678"}`))
			}))
			defer restate.Close()
			srv := &VanadisServer{
				store:      newMemoryVanadisStore(),
				restateCfg: RestateConfig{RestateIngressURL: restate.URL},
				httpClient: tracing.Client(restate.Client()),
			}
			body := `{"thread_id":"123456789012345678","objective":"o","chunks":[{"id":"c1","repo":"aerial","title":"t","instructions":"i","target_files":[],"target_symbols":[],"acceptance_criteria":[],"depends_on":[]}]}`
			req := httptest.NewRequest(http.MethodPost, "/builds", strings.NewReader(body))
			req.Header.Set("Traceparent", tc.traceparent)
			rec := httptest.NewRecorder()
			srv.Routes().ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
			parts := strings.Split(gotTP, "-")
			if len(parts) != 4 || parts[1] != tc.wantTraceID {
				t.Errorf("restate traceparent = %q, want trace id %s", gotTP, tc.wantTraceID)
			}
		})
	}
}

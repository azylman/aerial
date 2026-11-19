package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
)

// spanRecorder captures every span ended through the global provider; tests filter by trace id.
var spanRecorder = tracetest.NewSpanRecorder()

func TestMain(m *testing.M) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	os.Exit(m.Run())
}

func setupMockDispatcherServer(t *testing.T, expectedPath string, status int, respBody string, closeServer bool, nilDispatcher bool, testName string) (*HTTPPromptDispatcher, *httptest.Server, context.Context) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("expected Content-Type application/json, got %q", r.Header.Get("Content-Type"))
		}
		if r.URL.Path != expectedPath {
			t.Errorf("expected path %s, got %q", expectedPath, r.URL.Path)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(respBody))
	}))

	var dispatcher *HTTPPromptDispatcher
	switch {
	case nilDispatcher:
		dispatcher = nil
	case testName == "default http client used when httpClient is nil":
		dispatcher = &HTTPPromptDispatcher{brainURL: server.URL}
	case testName == "empty brain url fails fast":
		dispatcher = &HTTPPromptDispatcher{}
	default:
		serverURL := server.URL + "/"
		if closeServer {
			server.Close()
		}
		dispatcher = NewHTTPPromptDispatcher(serverURL)
	}

	ctx := context.Background()
	return dispatcher, server, ctx
}

type promptDispatcherTestCase struct {
	name            string
	serverStatus    int
	serverResponse  string
	req             PromptDispatchRequest
	closeServer     bool
	nilDispatcher   bool
	wantErr         bool
	wantErrContains string
	wantChannelID   string
}

func runPromptDispatcherCase(t *testing.T, tc promptDispatcherTestCase) {
	t.Helper()
	disp, server, ctx := setupMockDispatcherServer(t, "/prompt", tc.serverStatus, tc.serverResponse, tc.closeServer, tc.nilDispatcher, tc.name)
	defer server.Close()

	got, err := disp.DispatchPrompt(ctx, tc.req)
	switch {
	case tc.wantErr:
		if err == nil {
			t.Fatal("DispatchPrompt() expected error, got nil")
		}
		if tc.wantErrContains != "" && !strings.Contains(err.Error(), tc.wantErrContains) {
			t.Fatalf("DispatchPrompt() err = %v, want error containing %q", err, tc.wantErrContains)
		}
	default:
		if err != nil {
			t.Fatalf("DispatchPrompt() unexpected error: %v", err)
		}
		if got.ChannelID != tc.wantChannelID {
			t.Errorf("resp.ChannelID = %q, want %q", got.ChannelID, tc.wantChannelID)
		}
	}
}

func TestHTTPPromptDispatcher(t *testing.T) {
	tests := []promptDispatcherTestCase{
		{
			name:           "success accepted 202",
			serverStatus:   http.StatusAccepted,
			serverResponse: `{"status":"accepted","channel_id":"123456789","message_id":"msg-1"}`,
			req: PromptDispatchRequest{
				Prompt:    "echo test",
				ChannelID: "123456789",
				Title:     "Daily Check",
				MessageID: "run-1",
				Effort:    "low",
			},
			wantChannelID: "123456789",
		},
		{
			name:           "success ok 200",
			serverStatus:   http.StatusOK,
			serverResponse: `{"status":"ok","channel_id":"123456789","message_id":"msg-2"}`,
			req: PromptDispatchRequest{
				Prompt:    "echo test 2",
				ChannelID: "123456789",
			},
			wantChannelID: "123456789",
		},
		{
			name:           "server returns error json 400",
			serverStatus:   http.StatusBadRequest,
			serverResponse: `{"error":"prompt cannot be empty"}`,
			req: PromptDispatchRequest{
				ChannelID: "123456789",
			},
			wantErr: true,
		},
		{
			name:           "server returns non-json error 500",
			serverStatus:   http.StatusInternalServerError,
			serverResponse: `Internal Server Error`,
			req: PromptDispatchRequest{
				Prompt: "fail",
			},
			wantErr: true,
		},
		{
			name:           "server returns invalid json on 200",
			serverStatus:   http.StatusOK,
			serverResponse: `not valid json`,
			req: PromptDispatchRequest{
				Prompt: "test invalid json",
			},
			wantErr: true,
		},
		{
			name:        "closed server network error",
			closeServer: true,
			req: PromptDispatchRequest{
				Prompt: "test closed",
			},
			wantErr: true,
		},
		{
			name:          "nil dispatcher returns error",
			nilDispatcher: true,
			req:           PromptDispatchRequest{Prompt: "test"},
			wantErr:       true,
		},
		{
			name:           "default http client used when httpClient is nil",
			serverStatus:   http.StatusAccepted,
			serverResponse: `{"status":"accepted","channel_id":"nil-client-chan","message_id":"msg-nc"}`,
			req: PromptDispatchRequest{
				Prompt:    "nil client test",
				ChannelID: "nil-client-chan",
			},
			wantChannelID: "nil-client-chan",
		},
		{
			name: "empty brain url fails fast",
			req: PromptDispatchRequest{
				Prompt: "test empty url",
			},
			wantErr:         true,
			wantErrContains: "brain URL is not configured",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runPromptDispatcherCase(t, tc)
		})
	}
}

type mockErrBus struct {
	pubsub.Bus
}

func (*mockErrBus) Publish(ctx context.Context, subject, msgID string, data any) error {
	return errors.New("simulated bus error")
}

type dispatchTestCase struct {
	name            string
	nilDispatcher   bool
	withBus         bool
	errBus          bool
	req             DispatchRequest
	wantErr         bool
	wantErrContains string
	wantStatus      string
}

func runDispatchCase(t *testing.T, tc dispatchTestCase) {
	t.Helper()
	var disp *HTTPPromptDispatcher
	if !tc.nilDispatcher {
		disp = NewHTTPPromptDispatcher("http://eri:8080")
		if tc.withBus {
			memBus := pubsub.NewMemoryBus()
			defer memBus.Close()
			_ = pubsub.EnsureCoreStreams(context.Background(), memBus)
			disp.SetEventBus(memBus)
		} else if tc.errBus {
			disp.SetEventBus(&mockErrBus{})
		}
	}
	ctx := context.Background()
	resp, err := disp.Dispatch(ctx, tc.req)
	if tc.wantErr {
		if err == nil {
			t.Fatal("Dispatch() expected error, got nil")
		}
		if tc.wantErrContains != "" && !strings.Contains(err.Error(), tc.wantErrContains) {
			t.Fatalf("Dispatch() err = %v, want error containing %q", err, tc.wantErrContains)
		}
		return
	}
	if err != nil {
		t.Fatalf("Dispatch() unexpected error: %v", err)
	}
	if resp.Status != tc.wantStatus {
		t.Errorf("resp.Status = %q, want %q", resp.Status, tc.wantStatus)
	}
	if resp.DispatchID == "" {
		t.Error("expected non-empty DispatchID")
	}
}

func TestHTTPPromptDispatcher_Dispatch(t *testing.T) {
	tests := []dispatchTestCase{
		{
			name:            "nil dispatcher returns error",
			nilDispatcher:   true,
			req:             DispatchRequest{Source: "heartbeat", Prompt: "test"},
			wantErr:         true,
			wantErrContains: "dispatcher is nil",
		},
		{
			name:            "empty source returns error",
			withBus:         true,
			req:             DispatchRequest{Source: "  ", Prompt: "test"},
			wantErr:         true,
			wantErrContains: "source cannot be empty",
		},
		{
			name:            "empty prompt returns error",
			withBus:         true,
			req:             DispatchRequest{Source: "heartbeat", Prompt: "  "},
			wantErr:         true,
			wantErrContains: "prompt cannot be empty",
		},
		{
			name:            "bus not configured returns error",
			withBus:         false,
			req:             DispatchRequest{Source: "heartbeat", Prompt: "test"},
			wantErr:         true,
			wantErrContains: "event bus is not configured",
		},
		{
			name:            "bus publish error returns error",
			errBus:          true,
			req:             DispatchRequest{Source: "heartbeat", Prompt: "test"},
			wantErr:         true,
			wantErrContains: "simulated bus error",
		},
		{
			name:       "success accepted with bus",
			withBus:    true,
			req:        DispatchRequest{Source: "heartbeat", SessionID: "hb-1", TargetID: "123456789", Prompt: "echo test", Effort: "low", SkipIfActive: true},
			wantStatus: "accepted",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runDispatchCase(t, tc)
		})
	}
}

func TestHTTPPromptDispatcher_NilGuards(t *testing.T) {
	var nilDisp *HTTPPromptDispatcher
	if nilDisp.EventBus() != nil {
		t.Error("expected nil bus")
	}
	nilDisp.SetEventBus(nil)
}

func TestHTTPPromptDispatcher_Dispatch_EventBus(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	done := make(chan struct{}, 1)
	var gotArgs pubsub.EriDispatchJobArgs
	var gotSubject string
	_, err := memBus.Subscribe(ctx, "aerial.jobs.>", "test-sub", func(_ context.Context, msg pubsub.Message) error {
		gotSubject = msg.Subject()
		_ = json.Unmarshal(msg.Data(), &gotArgs)
		select {
		case done <- struct{}{}:
		default:
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"accepted","dispatch_id":"mock-id"}`))
	}))
	defer srv.Close()

	disp := NewHTTPPromptDispatcher(srv.URL)
	disp.SetEventBus(memBus)

	req := DispatchRequest{
		ID:           "disp-eb-1",
		Source:       "heartbeat",
		SessionID:    "heartbeat-1",
		TargetID:     "chan-1",
		Prompt:       "run prompt",
		Effort:       "medium",
		SkipIfActive: true,
	}
	resp, err := disp.Dispatch(ctx, req)
	if err != nil {
		t.Fatalf("Dispatch() unexpected error: %v", err)
	}
	if resp.Status != "accepted" || resp.DispatchID != "disp-eb-1" {
		t.Errorf("resp = %+v, want accepted/disp-eb-1", resp)
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for dispatched job message")
	}

	if gotSubject != pubsub.SubjectEriDispatchJob {
		t.Errorf("subject = %q, want %q", gotSubject, pubsub.SubjectEriDispatchJob)
	}
	if gotArgs.ID != "disp-eb-1" || gotArgs.Source != "heartbeat" || gotArgs.Prompt != "run prompt" {
		t.Errorf("gotArgs = %+v", gotArgs)
	}

	// Dispatch with empty ID defaults to generated UUID
	reqEmptyID := DispatchRequest{
		Source: "heartbeat",
		Prompt: "auto id prompt",
	}
	respEmptyID, err := disp.Dispatch(ctx, reqEmptyID)
	if err != nil {
		t.Fatalf("Dispatch() with empty ID error: %v", err)
	}
	if respEmptyID.DispatchID == "" {
		t.Errorf("expected auto-generated dispatch ID")
	}

	// Dispatch with empty prompt returns error
	if _, err := disp.Dispatch(ctx, DispatchRequest{Source: "heartbeat", Prompt: "   "}); err == nil {
		t.Errorf("expected error for empty prompt")
	}

	// Dispatch with empty source returns error
	if _, err := disp.Dispatch(ctx, DispatchRequest{Source: "", Prompt: "valid prompt"}); err == nil {
		t.Errorf("expected error for empty source")
	}
}

func TestHTTPPromptDispatcher_ConcurrentBusSafety(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"accepted","dispatch_id":"mock-id"}`))
	}))
	defer srv.Close()

	disp := NewHTTPPromptDispatcher(srv.URL)
	var wg sync.WaitGroup
	const iterations = 50

	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := range iterations {
			if i%2 == 0 {
				disp.SetEventBus(memBus)
			} else {
				disp.SetEventBus(nil)
			}
		}
	}()

	go func() {
		defer wg.Done()
		for range iterations {
			_ = disp.EventBus()
		}
	}()

	go func() {
		defer wg.Done()
		for i := range iterations {
			_, _ = disp.Dispatch(ctx, DispatchRequest{
				ID:     fmt.Sprintf("disp-race-%d", i),
				Source: "heartbeat",
				Prompt: "test prompt",
			})
		}
	}()

	wg.Wait()
}

func TestNewHTTPPromptDispatcher_Validation(t *testing.T) {
	t.Run("empty URL panics", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic for empty brainURL, got nil")
			}
		}()
		NewHTTPPromptDispatcher("   ")
	})
}

type errReadCloser struct{}

func (errReadCloser) Read([]byte) (int, error) { return 0, errors.New("simulated read failure") }

func (errReadCloser) Close() error { return nil }

type failBodyRoundTripper struct{}

func (failBodyRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Body: errReadCloser{}, Header: make(http.Header)}, nil
}

func TestPostBrainJSON_RequestErrors(t *testing.T) {
	tests := []struct {
		name            string
		dispatcher      *HTTPPromptDispatcher
		req             any
		wantErrContains string
	}{
		{name: "request build fails", dispatcher: &HTTPPromptDispatcher{brainURL: "http://127.0.0.1:1\nbad"}, req: PromptDispatchRequest{Prompt: "x"}, wantErrContains: "failed to create http request"},
		{name: "response body read fails", dispatcher: &HTTPPromptDispatcher{brainURL: "http://127.0.0.1:1", httpClient: &http.Client{Transport: failBodyRoundTripper{}}}, req: PromptDispatchRequest{Prompt: "x"}, wantErrContains: "failed to read response body"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := postBrainJSON[any, PromptDispatchResponse](context.Background(), tc.dispatcher, "/prompt", "prompt dispatch", tc.req)
			if err == nil {
				t.Fatal("postBrainJSON() expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErrContains) {
				t.Errorf("postBrainJSON() err = %v, want containing %q", err, tc.wantErrContains)
			}
		})
	}
}

// recordedParent starts and ends a span on its own in-memory recorder and returns a context
// carrying it plus the trace id the recorder saw.
func recordedParent(t *testing.T) (context.Context, string) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(context.Background(), "parent")
	span.End()
	return ctx, sr.Ended()[0].SpanContext().TraceID().String()
}

func traceparentTraceID(tp string) string {
	sc := trace.SpanContextFromContext(tracing.ContextWithTraceparent(context.Background(), tp))
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

func TestPostBrainJSON_Traceparent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		withParent bool
	}{
		{name: "continues the fire span's trace", withParent: true},
		{name: "starts a trace when the caller has none"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Get("Traceparent")
				_, _ = w.Write([]byte(`{"status":"accepted"}`))
			}))
			defer srv.Close()
			parentCtx, parentTraceID := recordedParent(t)
			ctx := context.Background()
			if tc.withParent {
				ctx = parentCtx
			}
			if _, err := postBrainJSON[PromptDispatchRequest, PromptDispatchResponse](ctx, NewHTTPPromptDispatcher(srv.URL), "/prompt", "prompt dispatch", PromptDispatchRequest{Prompt: "heartbeat"}); err != nil {
				t.Fatalf("postBrainJSON: %v", err)
			}
			id := traceparentTraceID(got)
			if id == "" {
				t.Fatalf("request carried no valid traceparent: %q", got)
			}
			if (id == parentTraceID) != tc.withParent {
				t.Errorf("trace id = %s, parent %s, want continues=%v", id, parentTraceID, tc.withParent)
			}
		})
	}
}

type traceFireTestCase struct {
	name         string
	scheduleType string
	scheduleID   string
	withParent   bool
	dispatchErr  error
	wantStatus   codes.Code
}

func findSpanBySpanID(spanID trace.SpanID) sdktrace.ReadOnlySpan {
	for _, s := range spanRecorder.Ended() {
		if s.SpanContext().SpanID() == spanID {
			return s
		}
	}
	return nil
}

func assertSpanStatusAndAttrs(t *testing.T, span sdktrace.ReadOnlySpan, tc traceFireTestCase) {
	t.Helper()
	if span.Status().Code != tc.wantStatus || (tc.wantStatus == codes.Error && span.Status().Description != "dispatch failed") {
		t.Errorf("status = %+v, want %v with generic description", span.Status(), tc.wantStatus)
	}
	attrs := map[string]string{}
	for _, kv := range span.Attributes() {
		attrs[string(kv.Key)] = kv.Value.String()
	}
	want := map[string]string{"haro.schedule.type": tc.scheduleType, "haro.schedule.id": tc.scheduleID}
	if len(attrs) != len(want) || attrs["haro.schedule.type"] != want["haro.schedule.type"] || attrs["haro.schedule.id"] != want["haro.schedule.id"] {
		t.Errorf("attributes = %v, want exactly %v", attrs, want)
	}
}

func runTraceFireCase(t *testing.T, tc traceFireTestCase) {
	t.Helper()
	parentCtx, parentTraceID := recordedParent(t)
	ctx := context.Background()
	if tc.withParent {
		ctx = parentCtx
	}
	var dispatchSC trace.SpanContext
	_, err := TraceFire(ctx, tc.scheduleType, tc.scheduleID, func(ctx context.Context) (DispatchResponse, error) {
		dispatchSC = trace.SpanContextFromContext(ctx)
		return DispatchResponse{}, tc.dispatchErr
	})
	if !errors.Is(err, tc.dispatchErr) {
		t.Fatalf("TraceFire err = %v, want %v", err, tc.dispatchErr)
	}
	span := findSpanBySpanID(dispatchSC.SpanID())
	if span == nil {
		t.Fatal("fire span was not recorded")
	}
	if span.Name() != "haro.fire "+tc.scheduleType || span.SpanKind() != trace.SpanKindProducer {
		t.Errorf("span = %q kind %v", span.Name(), span.SpanKind())
	}
	if (span.SpanContext().TraceID().String() == parentTraceID) != tc.withParent {
		t.Errorf("trace id = %s, parent %s, want continues=%v", span.SpanContext().TraceID(), parentTraceID, tc.withParent)
	}
	assertSpanStatusAndAttrs(t, span, tc)
}

func TestTraceFire(t *testing.T) {
	t.Parallel()
	tests := []traceFireTestCase{
		{name: "cron fire roots a trace", scheduleType: "cron", scheduleID: "cron-1", wantStatus: codes.Unset},
		{name: "event trigger continues the webhook trace", scheduleType: "event_watch", scheduleID: "trig-1", withParent: true, wantStatus: codes.Unset},
		{name: "dispatch failure marks the span without error text", scheduleType: "heartbeat", scheduleID: "hb-1", dispatchErr: errors.New("brain /dispatch returned HTTP 500: secret prompt echo"), wantStatus: codes.Error},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTraceFireCase(t, tc)
		})
	}
}

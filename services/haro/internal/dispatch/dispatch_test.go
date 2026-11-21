package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

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

type dispatchTestCase struct {
	name            string
	nilDispatcher   bool
	serverStatus    int
	serverResponse  string
	closeServer     bool
	req             DispatchRequest
	wantErr         bool
	wantErrContains string
	wantStatus      string
	wantDispatchID  string
}

func runDispatchCase(t *testing.T, tc dispatchTestCase) {
	t.Helper()
	var disp *HTTPPromptDispatcher
	if !tc.nilDispatcher {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/dispatch" {
				t.Errorf("expected path /dispatch, got %q", r.URL.Path)
			}
			w.WriteHeader(tc.serverStatus)
			_, _ = w.Write([]byte(tc.serverResponse))
		}))
		defer server.Close()
		if tc.closeServer {
			server.Close()
		}
		disp = NewHTTPPromptDispatcher(server.URL)
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
	if tc.wantDispatchID != "" && resp.DispatchID != tc.wantDispatchID {
		t.Errorf("resp.DispatchID = %q, want %q", resp.DispatchID, tc.wantDispatchID)
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
			req:             DispatchRequest{Source: "  ", Prompt: "test"},
			wantErr:         true,
			wantErrContains: "source cannot be empty",
		},
		{
			name:            "empty prompt returns error",
			req:             DispatchRequest{Source: "heartbeat", Prompt: "  "},
			wantErr:         true,
			wantErrContains: "prompt cannot be empty",
		},
		{
			name:            "server returns error json 400",
			serverStatus:    http.StatusBadRequest,
			serverResponse:  `{"error":"invalid dispatch"}`,
			req:             DispatchRequest{Source: "heartbeat", Prompt: "test"},
			wantErr:         true,
			wantErrContains: "invalid dispatch",
		},
		{
			name:            "server returns non-json error 500",
			serverStatus:    http.StatusInternalServerError,
			serverResponse:  `Internal Server Error`,
			req:             DispatchRequest{Source: "heartbeat", Prompt: "test"},
			wantErr:         true,
			wantErrContains: "returned HTTP 500",
		},
		{
			name:            "closed server network error",
			closeServer:     true,
			req:             DispatchRequest{Source: "heartbeat", Prompt: "test"},
			wantErr:         true,
			wantErrContains: "dispatch failed",
		},
		{
			name:           "success accepted 202",
			serverStatus:   http.StatusAccepted,
			serverResponse: `{"status":"accepted","dispatch_id":"disp-1"}`,
			req:            DispatchRequest{Source: "heartbeat", SessionID: "hb-1", TargetID: "123456789", Prompt: "echo test", Effort: "low", SkipIfActive: true},
			wantStatus:     "accepted",
			wantDispatchID: "disp-1",
		},
		{
			name:           "success ok 200",
			serverStatus:   http.StatusOK,
			serverResponse: `{"status":"ok","dispatch_id":"disp-2"}`,
			req:            DispatchRequest{Source: "heartbeat", Prompt: "echo test 2"},
			wantStatus:     "ok",
			wantDispatchID: "disp-2",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runDispatchCase(t, tc)
		})
	}
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

	t.Run("constructor with restate opts", func(t *testing.T) {
		d := NewHTTPPromptDispatcher("http://eri:8080", "http://restate:8080/", "agent-alpha")
		if d.brainURL != "http://eri:8080" {
			t.Errorf("brainURL = %q, want http://eri:8080", d.brainURL)
		}
		if d.restateURL != "http://restate:8080" {
			t.Errorf("restateURL = %q, want http://restate:8080", d.restateURL)
		}
		if d.agentName != "agent-alpha" {
			t.Errorf("agentName = %q, want agent-alpha", d.agentName)
		}
	})
}

func TestServiceNameHelpers(t *testing.T) {
	tests := []struct {
		agent        string
		wantThread   string
		wantDispatch string
	}{
		{"", "ThreadSession", "DispatchSession"},
		{"aerial", "ThreadSession", "DispatchSession"},
		{"AERIAL", "ThreadSession", "DispatchSession"},
		{"  aerial  ", "ThreadSession", "DispatchSession"},
		{"agent-alpha", "ThreadSession-agent-alpha", "DispatchSession-agent-alpha"},
		{"Agent-Beta", "ThreadSession-agent-beta", "DispatchSession-agent-beta"},
		{"agent-1_test", "ThreadSession-agent-1_test", "DispatchSession-agent-1_test"},
		{"invalid/name", "ThreadSession", "DispatchSession"},
		{"../traversal", "ThreadSession", "DispatchSession"},
		{"a@b#c", "ThreadSession", "DispatchSession"},
	}
	for _, tc := range tests {
		t.Run("agent_"+tc.agent, func(t *testing.T) {
			if got := ThreadSessionServiceName(tc.agent); got != tc.wantThread {
				t.Errorf("ThreadSessionServiceName(%q) = %q, want %q", tc.agent, got, tc.wantThread)
			}
			if got := DispatchSessionServiceName(tc.agent); got != tc.wantDispatch {
				t.Errorf("DispatchSessionServiceName(%q) = %q, want %q", tc.agent, got, tc.wantDispatch)
			}
		})
	}
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

func TestPostRestateJSON_RequestErrors(t *testing.T) {
	tests := []struct {
		name            string
		dispatcher      *HTTPPromptDispatcher
		endpoint        string
		req             any
		wantErrContains string
	}{
		{name: "nil dispatcher", dispatcher: nil, endpoint: "http://restate:8080", req: map[string]any{"p": 1}, wantErrContains: "dispatcher is nil"},
		{name: "invalid json payload", dispatcher: &HTTPPromptDispatcher{}, endpoint: "http://restate:8080", req: make(chan int), wantErrContains: "failed to marshal"},
		{name: "request build fails", dispatcher: &HTTPPromptDispatcher{}, endpoint: "http://127.0.0.1:1\nbad", req: map[string]any{"p": 1}, wantErrContains: "failed to create http request"},
		{name: "network do fails", dispatcher: &HTTPPromptDispatcher{httpClient: &http.Client{Transport: failBodyRoundTripper{}}}, endpoint: "http://restate:8080", req: map[string]any{"p": 1}, wantErrContains: "failed to read response body"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.dispatcher.postRestateJSON(context.Background(), tc.endpoint, "idemp-key", "restate op", tc.req)
			if err == nil {
				t.Fatal("postRestateJSON() expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErrContains) {
				t.Errorf("postRestateJSON() err = %v, want containing %q", err, tc.wantErrContains)
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

type restatePromptTestCase struct {
	name            string
	serverStatus    int
	serverResponse  string
	agent           string
	reqPrompt       PromptDispatchRequest
	wantPath        string
	wantIdempotency string
	wantErr         bool
	wantErrContains string
	wantStatus      string
	wantChannelID   string
}

func runRestatePromptTest(t *testing.T, tc restatePromptTestCase) {
	t.Helper()
	var gotPath, gotIdempotency string
	var gotBody PromptDispatchRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotIdempotency = r.Header.Get("Idempotency-Key")
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type = %q, want application/json", r.Header.Get("Content-Type"))
		}
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(tc.serverStatus)
		_, _ = w.Write([]byte(tc.serverResponse))
	}))
	defer server.Close()

	disp := NewHTTPPromptDispatcher("http://eri:8080", server.URL, tc.agent)
	resp, err := disp.DispatchPrompt(context.Background(), tc.reqPrompt)
	if tc.wantErr {
		if err == nil {
			t.Fatal("DispatchPrompt() expected error, got nil")
		}
		if tc.wantErrContains != "" && !strings.Contains(err.Error(), tc.wantErrContains) {
			t.Fatalf("DispatchPrompt() err = %v, want containing %q", err, tc.wantErrContains)
		}
		return
	}
	if err != nil {
		t.Fatalf("DispatchPrompt() unexpected error: %v", err)
	}
	if tc.wantPath != "" && gotPath != tc.wantPath {
		t.Errorf("path = %q, want %q", gotPath, tc.wantPath)
	}
	if tc.wantIdempotency != "" && gotIdempotency != tc.wantIdempotency {
		t.Errorf("idempotency = %q, want %q", gotIdempotency, tc.wantIdempotency)
	}
	if resp.Status != tc.wantStatus {
		t.Errorf("resp.Status = %q, want %q", resp.Status, tc.wantStatus)
	}
	if tc.wantChannelID != "" && resp.ChannelID != tc.wantChannelID {
		t.Errorf("resp.ChannelID = %q, want %q", resp.ChannelID, tc.wantChannelID)
	}
	if resp.ChannelID != "" && gotBody.ChannelID != resp.ChannelID {
		t.Errorf("gotBody.ChannelID = %q, want %q", gotBody.ChannelID, resp.ChannelID)
	}
	if resp.MessageID != "" && gotBody.MessageID != resp.MessageID {
		t.Errorf("gotBody.MessageID = %q, want %q", gotBody.MessageID, resp.MessageID)
	}
}

func TestHTTPPromptDispatcher_RestatePrompt(t *testing.T) {
	t.Parallel()
	tests := []restatePromptTestCase{
		{
			name:           "dispatch prompt default aerial",
			serverStatus:   http.StatusOK,
			serverResponse: `{"status":"accepted","channel_id":"ch-123","message_id":"msg-abc"}`,
			agent:          "aerial",
			reqPrompt: PromptDispatchRequest{
				ChannelID: "ch-123",
				MessageID: "msg-abc",
				Prompt:    "hello restate",
			},
			wantPath:        "/ThreadSession/ch-123/Enqueue",
			wantIdempotency: "prompt:ch-123:msg-abc",
			wantStatus:      "accepted",
			wantChannelID:   "ch-123",
		},
		{
			name:           "dispatch prompt custom agent from dispatcher and empty body",
			serverStatus:   http.StatusOK,
			serverResponse: ``,
			agent:          "agent-alpha",
			reqPrompt: PromptDispatchRequest{
				ChannelID: "ch-456",
				MessageID: "msg-456",
				Prompt:    "hello alpha",
			},
			wantPath:        "/ThreadSession-agent-alpha/ch-456/Enqueue",
			wantIdempotency: "prompt:ch-456:msg-456",
			wantStatus:      "accepted",
			wantChannelID:   "ch-456",
		},
		{
			name:           "dispatch prompt per-request agent override",
			serverStatus:   http.StatusOK,
			serverResponse: `{"status":"accepted","channel_id":"ch-789","message_id":"msg-789"}`,
			agent:          "aerial",
			reqPrompt: PromptDispatchRequest{
				ChannelID: "ch-789",
				MessageID: "msg-789",
				Prompt:    "hello beta",
				Agent:     "agent-beta",
			},
			wantPath:        "/ThreadSession-agent-beta/ch-789/Enqueue",
			wantIdempotency: "prompt:ch-789:msg-789",
			wantStatus:      "accepted",
			wantChannelID:   "ch-789",
		},
		{
			name:           "dispatch prompt auto-generated message id",
			serverStatus:   http.StatusOK,
			serverResponse: `{"status":"accepted"}`,
			agent:          "",
			reqPrompt: PromptDispatchRequest{
				ChannelID: "ch-auto",
				Prompt:    "auto msg id",
			},
			wantPath:      "/ThreadSession/ch-auto/Enqueue",
			wantStatus:    "accepted",
			wantChannelID: "ch-auto",
		},
		{
			name:  "dispatch prompt missing channel_id fails",
			agent: "aerial",
			reqPrompt: PromptDispatchRequest{
				Prompt: "no channel",
			},
			wantErr:         true,
			wantErrContains: "channel_id cannot be empty",
		},
		{
			name:           "dispatch prompt error response message",
			serverStatus:   http.StatusInternalServerError,
			serverResponse: `{"message":"something went wrong in restate"}`,
			agent:          "aerial",
			reqPrompt: PromptDispatchRequest{
				ChannelID: "ch-err",
				Prompt:    "trigger err",
			},
			wantPath:        "/ThreadSession/ch-err/Enqueue",
			wantErr:         true,
			wantErrContains: "restate prompt dispatch error (500): something went wrong in restate",
		},
		{
			name:           "dispatch prompt error response fallback field",
			serverStatus:   http.StatusBadRequest,
			serverResponse: `{"error":"invalid prompt"}`,
			agent:          "aerial",
			reqPrompt: PromptDispatchRequest{
				ChannelID: "ch-err-2",
				Prompt:    "trigger err 2",
			},
			wantPath:        "/ThreadSession/ch-err-2/Enqueue",
			wantErr:         true,
			wantErrContains: "restate prompt dispatch error (400): invalid prompt",
		},
		{
			name:           "dispatch prompt raw status error",
			serverStatus:   http.StatusBadGateway,
			serverResponse: `bad gateway upstream`,
			agent:          "aerial",
			reqPrompt: PromptDispatchRequest{
				ChannelID: "ch-err-3",
				Prompt:    "trigger err 3",
			},
			wantPath:        "/ThreadSession/ch-err-3/Enqueue",
			wantErr:         true,
			wantErrContains: "restate prompt dispatch returned HTTP 502: bad gateway upstream",
		},
		{
			name:           "dispatch prompt invalid json on 200",
			serverStatus:   http.StatusOK,
			serverResponse: `invalid-json`,
			agent:          "aerial",
			reqPrompt: PromptDispatchRequest{
				ChannelID: "ch-inv",
				Prompt:    "inv",
			},
			wantPath:        "/ThreadSession/ch-inv/Enqueue",
			wantErr:         true,
			wantErrContains: "failed to parse prompt dispatch restate response",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runRestatePromptTest(t, tc)
		})
	}
}

type restateDispatchTestCase struct {
	name            string
	serverStatus    int
	serverResponse  string
	agent           string
	reqDispatch     DispatchRequest
	wantPath        string
	wantIdempotency string
	wantErr         bool
	wantErrContains string
	wantStatus      string
	wantDispatchID  string
}

func runRestateDispatchTest(t *testing.T, tc restateDispatchTestCase) {
	t.Helper()
	var capturedURL string
	var capturedID string
	ts := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		capturedURL = req.URL.String()
		capturedID = req.Header.Get("Idempotency-Key")
		if ct := req.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("expected json content-type, got %s", ct)
		}
		rw.WriteHeader(tc.serverStatus)
		_, _ = rw.Write([]byte(tc.serverResponse))
	}))
	defer ts.Close()

	d := NewHTTPPromptDispatcher("http://eri:8080", ts.URL, tc.agent)
	actual, err := d.Dispatch(context.Background(), tc.reqDispatch)
	if tc.wantErr {
		if err == nil {
			t.Fatal("Dispatch() expected error, got nil")
		}
		if tc.wantErrContains != "" && !strings.Contains(err.Error(), tc.wantErrContains) {
			t.Fatalf("Dispatch() error %v does not contain %q", err, tc.wantErrContains)
		}
		return
	}
	if err != nil {
		t.Fatalf("Dispatch() failed unexpectedly: %v", err)
	}
	if tc.wantPath != "" && capturedURL != tc.wantPath {
		t.Errorf("url = %q, want %q", capturedURL, tc.wantPath)
	}
	if tc.wantIdempotency != "" && capturedID != tc.wantIdempotency {
		t.Errorf("key = %q, want %q", capturedID, tc.wantIdempotency)
	}
	if actual.Status != tc.wantStatus {
		t.Errorf("status = %q, want %q", actual.Status, tc.wantStatus)
	}
	if tc.wantDispatchID != "" && actual.DispatchID != tc.wantDispatchID {
		t.Errorf("dispatch ID = %q, want %q", actual.DispatchID, tc.wantDispatchID)
	}
}

func TestHTTPPromptDispatcher_RestateDispatch(t *testing.T) {
	t.Parallel()
	tests := []restateDispatchTestCase{
		{
			name:           "dispatch task default session_id",
			serverStatus:   http.StatusOK,
			serverResponse: `{"status":"accepted","dispatch_id":"disp-1"}`,
			agent:          "aerial",
			reqDispatch: DispatchRequest{
				ID:        "disp-1",
				Source:    "heartbeat",
				SessionID: "sess-custom",
				Prompt:    "check something",
			},
			wantPath:        "/DispatchSession/sess-custom/Enqueue",
			wantIdempotency: "disp-1",
			wantStatus:      "accepted",
			wantDispatchID:  "disp-1",
		},
		{
			name:           "dispatch task fallback session key from id agent-alpha",
			serverStatus:   http.StatusOK,
			serverResponse: ``,
			agent:          "agent-alpha",
			reqDispatch: DispatchRequest{
				ID:     "disp-2",
				Source: "vanadis",
				Prompt: "build chunk",
			},
			wantPath:        "/DispatchSession-agent-alpha/dispatch-disp-2/Enqueue",
			wantIdempotency: "disp-2",
			wantStatus:      "accepted",
			wantDispatchID:  "disp-2",
		},
		{
			name:           "dispatch task per-request agent override",
			serverStatus:   http.StatusOK,
			serverResponse: `{"status":"accepted","dispatch_id":"disp-3"}`,
			agent:          "aerial",
			reqDispatch: DispatchRequest{
				ID:     "disp-3",
				Source: "heartbeat",
				Prompt: "build chunk override",
				Agent:  "agent-beta",
			},
			wantPath:        "/DispatchSession-agent-beta/dispatch-disp-3/Enqueue",
			wantIdempotency: "disp-3",
			wantStatus:      "accepted",
			wantDispatchID:  "disp-3",
		},
		{
			name:           "dispatch task auto id generated when empty",
			serverStatus:   http.StatusOK,
			serverResponse: `{"status":"accepted"}`,
			agent:          "aerial",
			reqDispatch: DispatchRequest{
				Source: "heartbeat",
				Prompt: "auto id dispatch",
			},
			wantStatus: "accepted",
		},
		{
			name:           "dispatch task restate error",
			serverStatus:   http.StatusConflict,
			serverResponse: `{"message":"active turn running"}`,
			agent:          "aerial",
			reqDispatch: DispatchRequest{
				ID:     "disp-conflict",
				Source: "heartbeat",
				Prompt: "conflict test",
			},
			wantPath:        "/DispatchSession/dispatch-disp-conflict/Enqueue",
			wantIdempotency: "disp-conflict",
			wantErr:         true,
			wantErrContains: "restate dispatch error (409): active turn running",
		},
		{
			name:           "dispatch task invalid json on 200",
			serverStatus:   http.StatusOK,
			serverResponse: `not-json`,
			agent:          "aerial",
			reqDispatch: DispatchRequest{
				ID:     "disp-bad-json",
				Source: "heartbeat",
				Prompt: "bad json",
			},
			wantPath:        "/DispatchSession/dispatch-bad-json/Enqueue",
			wantIdempotency: "disp-bad-json",
			wantErr:         true,
			wantErrContains: "failed to parse dispatch restate response",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runRestateDispatchTest(t, tc)
		})
	}
}

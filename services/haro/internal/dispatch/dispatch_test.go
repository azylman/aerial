package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/pkg/tracing"
)

// spanRecorder captures every span ended through the global provider; tests filter by trace id.
var spanRecorder = tracetest.NewSpanRecorder()

func TestMain(m *testing.M) {
	otel.SetTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spanRecorder)))
	otel.SetTextMapPropagator(propagation.TraceContext{})
	os.Exit(m.Run())
}

// dispatchServerURL in a row's brain or restate column is replaced with the recording server's URL.
const dispatchServerURL = "{server}"

type dispatchRequest struct {
	path        string
	idempotency string
	contentType string
	body        []byte
}

type dispatchRecording struct {
	mu   sync.Mutex
	last dispatchRequest
}

func (r *dispatchRecording) snapshot() dispatchRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last
}

// newDispatchRecordingServer replies with status/body and records the last request.
func newDispatchRecordingServer(t *testing.T, status int, body string) (*httptest.Server, *dispatchRecording) {
	t.Helper()
	rec := &dispatchRecording{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqBody, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read request body: %v", err)
		}
		rec.mu.Lock()
		rec.last = dispatchRequest{path: r.URL.Path, idempotency: r.Header.Get("Idempotency-Key"), contentType: r.Header.Get("Content-Type"), body: reqBody}
		rec.mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server, rec
}

var (
	nilDispatcher = dispatcherTarget{nilDispatcher: true}
	closedBrain   = dispatcherTarget{brain: dispatchServerURL + "/", closeServer: true}
)

// dispatcherTarget is the dispatcher-construction half of a row: brain/restate URLs (dispatchServerURL
// expands to the recording server), the agent, a struct literal without http client (nilClient), a nil
// dispatcher, or a server closed before the call.
type dispatcherTarget struct {
	brain         string
	restate       string
	agent         string
	nilClient     bool
	nilDispatcher bool
	closeServer   bool
}

func newTargetDispatcher(t *testing.T, target dispatcherTarget, status int, body string) (*HTTPPromptDispatcher, *dispatchRecording) {
	t.Helper()
	server, rec := newDispatchRecordingServer(t, status, body)
	if target.closeServer {
		server.Close()
	}
	brain := strings.ReplaceAll(target.brain, dispatchServerURL, server.URL)
	restate := strings.ReplaceAll(target.restate, dispatchServerURL, server.URL)
	switch {
	case target.nilDispatcher:
		return nil, rec
	case target.nilClient:
		return &HTTPPromptDispatcher{brainURL: brain, restateURL: restate, agentName: target.agent}, rec
	default:
		return NewHTTPPromptDispatcher(brain, restate, target.agent), rec
	}
}

func assertDispatchRecording(t *testing.T, rec dispatchRequest, wantPath, wantIdempotency string) {
	t.Helper()
	if wantPath != "" && rec.path != wantPath {
		t.Errorf("path = %q, want %q", rec.path, wantPath)
	}
	if wantIdempotency != "" && rec.idempotency != wantIdempotency {
		t.Errorf("Idempotency-Key = %q, want %q", rec.idempotency, wantIdempotency)
	}
	if rec.path != "" && rec.contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", rec.contentType)
	}
}

func assertDispatchErr(t *testing.T, err error, wantErr string) bool {
	t.Helper()
	if wantErr == "" {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		return false
	}
	if err == nil || !strings.Contains(err.Error(), wantErr) {
		t.Fatalf("err = %v, want error containing %q", err, wantErr)
	}
	return true
}

func TestHTTPPromptDispatcherDispatchPrompt(t *testing.T) {
	t.Parallel()
	brain := dispatcherTarget{brain: dispatchServerURL + "/"}
	restate := func(agent string) dispatcherTarget {
		return dispatcherTarget{brain: "http://eri:8080", restate: dispatchServerURL, agent: agent}
	}
	tests := []struct {
		name            string
		target          dispatcherTarget
		status          int
		body            string
		req             PromptDispatchRequest
		wantPath        string
		wantIdempotency string
		wantErr         string
		wantStatus      string
		wantChannelID   string
	}{
		{
			name: "brain success accepted 202", target: brain, status: http.StatusAccepted,
			body:     `{"status":"accepted","channel_id":"123456789","message_id":"run-1"}`,
			req:      PromptDispatchRequest{Prompt: "echo test", ChannelID: "123456789", Title: "Daily Check", MessageID: "run-1", Effort: "low"},
			wantPath: "/prompt", wantStatus: "accepted", wantChannelID: "123456789",
		},
		{
			name: "brain success ok 200", target: brain, status: http.StatusOK, body: `{"status":"ok","channel_id":"123456789"}`,
			req: PromptDispatchRequest{Prompt: "echo test 2", ChannelID: "123456789"}, wantPath: "/prompt", wantStatus: "ok", wantChannelID: "123456789",
		},
		{
			name: "brain error json 400", target: brain, status: http.StatusBadRequest, body: `{"error":"prompt cannot be empty"}`,
			req: PromptDispatchRequest{ChannelID: "123456789"}, wantPath: "/prompt", wantErr: "brain /prompt error (400): prompt cannot be empty",
		},
		{
			name: "brain non-json error 500", target: brain, status: http.StatusInternalServerError, body: `Internal Server Error`,
			req: PromptDispatchRequest{Prompt: "fail"}, wantPath: "/prompt", wantErr: "brain /prompt returned HTTP 500: Internal Server Error",
		},
		{
			name: "brain invalid json on 200", target: brain, status: http.StatusOK, body: `not valid json`,
			req: PromptDispatchRequest{Prompt: "test invalid json"}, wantPath: "/prompt", wantErr: "failed to parse prompt dispatch response",
		},
		{
			name: "brain closed server network error", target: closedBrain,
			req: PromptDispatchRequest{Prompt: "test closed"}, wantErr: "prompt dispatch failed",
		},
		{name: "nil dispatcher returns error", target: nilDispatcher, req: PromptDispatchRequest{Prompt: "test"}, wantErr: "dispatcher is nil"},
		{
			name:   "default http client used when httpClient is nil",
			target: dispatcherTarget{brain: dispatchServerURL, nilClient: true}, status: http.StatusAccepted,
			body:     `{"status":"accepted","channel_id":"nil-client-chan","message_id":"msg-nc"}`,
			req:      PromptDispatchRequest{Prompt: "nil client test", ChannelID: "nil-client-chan", MessageID: "msg-nc"},
			wantPath: "/prompt", wantStatus: "accepted", wantChannelID: "nil-client-chan",
		},
		{
			name: "empty brain url fails fast", target: dispatcherTarget{nilClient: true},
			req: PromptDispatchRequest{Prompt: "test empty url"}, wantErr: "brain URL is not configured",
		},
		{
			name: "restate default aerial", target: restate("aerial"), status: http.StatusOK,
			body:     `{"status":"accepted","channel_id":"ch-123","message_id":"msg-abc"}`,
			req:      PromptDispatchRequest{ChannelID: "ch-123", MessageID: "msg-abc", Prompt: "hello restate"},
			wantPath: "/ThreadSession/ch-123/Enqueue", wantIdempotency: "prompt:ch-123:msg-abc", wantStatus: "accepted", wantChannelID: "ch-123",
		},
		{
			name: "restate custom agent from dispatcher and empty body", target: restate("agent-alpha"), status: http.StatusOK,
			req:      PromptDispatchRequest{ChannelID: "ch-456", MessageID: "msg-456", Prompt: "hello alpha"},
			wantPath: "/ThreadSession-agent-alpha/ch-456/Enqueue", wantIdempotency: "prompt:ch-456:msg-456", wantStatus: "accepted", wantChannelID: "ch-456",
		},
		{
			name: "restate per-request agent override", target: restate("aerial"), status: http.StatusOK,
			body:     `{"status":"accepted","channel_id":"ch-789","message_id":"msg-789"}`,
			req:      PromptDispatchRequest{ChannelID: "ch-789", MessageID: "msg-789", Prompt: "hello beta", Agent: "agent-beta"},
			wantPath: "/ThreadSession-agent-beta/ch-789/Enqueue", wantIdempotency: "prompt:ch-789:msg-789", wantStatus: "accepted", wantChannelID: "ch-789",
		},
		{
			name: "restate auto-generated message id", target: restate(""), status: http.StatusOK, body: `{"status":"accepted"}`,
			req:      PromptDispatchRequest{ChannelID: "ch-auto", Prompt: "auto msg id"},
			wantPath: "/ThreadSession/ch-auto/Enqueue", wantStatus: "accepted", wantChannelID: "ch-auto",
		},
		{
			name: "restate missing channel_id fails", target: restate("aerial"),
			req: PromptDispatchRequest{Prompt: "no channel"}, wantErr: "channel_id cannot be empty",
		},
		{
			name: "restate error response message", target: restate("aerial"), status: http.StatusInternalServerError,
			body: `{"message":"something went wrong in restate"}`,
			req:  PromptDispatchRequest{ChannelID: "ch-err", Prompt: "trigger err"}, wantPath: "/ThreadSession/ch-err/Enqueue",
			wantErr: "restate prompt dispatch error (500): something went wrong in restate",
		},
		{
			name: "restate error response fallback field", target: restate("aerial"), status: http.StatusBadRequest, body: `{"error":"invalid prompt"}`,
			req: PromptDispatchRequest{ChannelID: "ch-err-2", Prompt: "trigger err 2"}, wantPath: "/ThreadSession/ch-err-2/Enqueue",
			wantErr: "restate prompt dispatch error (400): invalid prompt",
		},
		{
			name: "restate raw status error", target: restate("aerial"), status: http.StatusBadGateway, body: `bad gateway upstream`,
			req: PromptDispatchRequest{ChannelID: "ch-err-3", Prompt: "trigger err 3"}, wantPath: "/ThreadSession/ch-err-3/Enqueue",
			wantErr: "restate prompt dispatch returned HTTP 502: bad gateway upstream",
		},
		{
			name: "restate invalid json on 200", target: restate("aerial"), status: http.StatusOK, body: `invalid-json`,
			req: PromptDispatchRequest{ChannelID: "ch-inv", Prompt: "inv"}, wantPath: "/ThreadSession/ch-inv/Enqueue",
			wantErr: "failed to parse prompt dispatch restate response",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp, rec := newTargetDispatcher(t, tc.target, tc.status, tc.body)
			resp, err := disp.DispatchPrompt(context.Background(), tc.req)
			got := rec.snapshot()
			assertDispatchRecording(t, got, tc.wantPath, tc.wantIdempotency)
			if assertDispatchErr(t, err, tc.wantErr) {
				return
			}
			if resp.Status != tc.wantStatus || resp.ChannelID != tc.wantChannelID {
				t.Errorf("resp = %+v, want status %q channel %q", resp, tc.wantStatus, tc.wantChannelID)
			}
			var sent PromptDispatchRequest
			if err := json.Unmarshal(got.body, &sent); err != nil {
				t.Fatalf("decode sent body: %v", err)
			}
			if sent.ChannelID != resp.ChannelID || sent.MessageID != resp.MessageID {
				t.Errorf("sent ids = %q/%q, want response ids %q/%q", sent.ChannelID, sent.MessageID, resp.ChannelID, resp.MessageID)
			}
		})
	}
}

func TestHTTPPromptDispatcherDispatch(t *testing.T) {
	t.Parallel()
	brain := dispatcherTarget{brain: dispatchServerURL}
	restate := func(agent string) dispatcherTarget {
		return dispatcherTarget{brain: "http://eri:8080", restate: dispatchServerURL, agent: agent}
	}
	hb := eriwire.BrainDispatchRequest{Source: "heartbeat", Prompt: "test"}
	tests := []struct {
		name            string
		target          dispatcherTarget
		status          int
		body            string
		req             eriwire.BrainDispatchRequest
		wantPath        string
		wantIdempotency string
		wantErr         string
		wantStatus      string
		wantDispatchID  string
	}{
		{name: "nil dispatcher returns error", target: nilDispatcher, req: hb, wantErr: "dispatcher is nil"},
		{name: "empty source returns error", target: brain, req: eriwire.BrainDispatchRequest{Source: "  ", Prompt: "test"}, wantErr: "source cannot be"},
		{name: "empty prompt returns error", target: brain, req: eriwire.BrainDispatchRequest{Source: "heartbeat", Prompt: " "}, wantErr: "prompt cannot"},
		{
			name: "brain error json 400", target: brain, status: http.StatusBadRequest, body: `{"error":"invalid dispatch"}`,
			req: hb, wantPath: "/dispatch", wantErr: "brain /dispatch error (400): invalid dispatch",
		},
		{
			name: "brain non-json error 500", target: brain, status: http.StatusInternalServerError, body: `Internal Server Error`,
			req: hb, wantPath: "/dispatch", wantErr: "brain /dispatch returned HTTP 500: Internal Server Error",
		},
		{name: "brain closed server network error", target: closedBrain, req: hb, wantErr: "dispatch failed: "},
		{
			name: "brain success accepted 202", target: brain, status: http.StatusAccepted, body: `{"status":"accepted","dispatch_id":"disp-1"}`,
			req: eriwire.BrainDispatchRequest{
				Source: "heartbeat", SessionID: "hb-1", TargetID: "123456789", Prompt: "echo test", Effort: "low", SkipIfActive: true,
			},
			wantPath: "/dispatch", wantStatus: "accepted", wantDispatchID: "disp-1",
		},
		{
			name: "brain success ok 200", target: brain, status: http.StatusOK, body: `{"status":"ok","dispatch_id":"disp-2"}`,
			req: eriwire.BrainDispatchRequest{Source: "heartbeat", Prompt: "echo test 2"}, wantPath: "/dispatch", wantStatus: "ok", wantDispatchID: "disp-2",
		},
		{
			name: "restate default session_id", target: restate("aerial"), status: http.StatusOK, body: `{"status":"accepted","dispatch_id":"disp-1"}`,
			req:      eriwire.BrainDispatchRequest{ID: "disp-1", Source: "heartbeat", SessionID: "sess-custom", Prompt: "check something"},
			wantPath: "/DispatchSession/sess-custom/Enqueue", wantIdempotency: "disp-1", wantStatus: "accepted", wantDispatchID: "disp-1",
		},
		{
			name: "restate fallback session key from id agent-alpha", target: restate("agent-alpha"), status: http.StatusOK,
			req:      eriwire.BrainDispatchRequest{ID: "disp-2", Source: "vanadis", Prompt: "build chunk"},
			wantPath: "/DispatchSession-agent-alpha/dispatch-disp-2/Enqueue", wantIdempotency: "disp-2", wantStatus: "accepted", wantDispatchID: "disp-2",
		},
		{
			name: "restate per-request agent override", target: restate("aerial"), status: http.StatusOK, body: `{"status":"accepted","dispatch_id":"disp-3"}`,
			req:      eriwire.BrainDispatchRequest{ID: "disp-3", Source: "heartbeat", Prompt: "build chunk override", Agent: "agent-beta"},
			wantPath: "/DispatchSession-agent-beta/dispatch-disp-3/Enqueue", wantIdempotency: "disp-3", wantStatus: "accepted", wantDispatchID: "disp-3",
		},
		{
			name: "restate auto id generated when empty", target: restate("aerial"), status: http.StatusOK, body: `{"status":"accepted"}`,
			req: eriwire.BrainDispatchRequest{Source: "heartbeat", Prompt: "auto id dispatch"}, wantStatus: "accepted",
		},
		{
			name: "restate conflict error", target: restate("aerial"), status: http.StatusConflict, body: `{"message":"active turn running"}`,
			req:      eriwire.BrainDispatchRequest{ID: "disp-conflict", Source: "heartbeat", Prompt: "conflict test"},
			wantPath: "/DispatchSession/dispatch-disp-conflict/Enqueue", wantIdempotency: "disp-conflict",
			wantErr: "restate dispatch error (409): active turn running",
		},
		{
			name: "restate invalid json on 200", target: restate("aerial"), status: http.StatusOK, body: `not-json`,
			req:      eriwire.BrainDispatchRequest{ID: "disp-bad-json", Source: "heartbeat", Prompt: "bad json"},
			wantPath: "/DispatchSession/dispatch-disp-bad-json/Enqueue", wantIdempotency: "disp-bad-json",
			wantErr: "failed to parse dispatch restate response",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			disp, rec := newTargetDispatcher(t, tc.target, tc.status, tc.body)
			resp, err := disp.Dispatch(context.Background(), tc.req)
			assertDispatchRecording(t, rec.snapshot(), tc.wantPath, tc.wantIdempotency)
			if assertDispatchErr(t, err, tc.wantErr) {
				return
			}
			if resp.Status != tc.wantStatus {
				t.Errorf("resp.Status = %q, want %q", resp.Status, tc.wantStatus)
			}
			if tc.wantDispatchID != "" && resp.DispatchID != tc.wantDispatchID {
				t.Errorf("resp.DispatchID = %q, want %q", resp.DispatchID, tc.wantDispatchID)
			}
		})
	}
}

func TestNewHTTPPromptDispatcher(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		brainURL    string
		restateOpts []string
		wantPanic   bool
		wantBrain   string
		wantRestate string
		wantAgent   string
	}{
		{name: "empty URL panics", brainURL: "   ", wantPanic: true},
		{name: "brain only defaults agent", brainURL: " http://eri:8080 ", wantBrain: "http://eri:8080", wantAgent: "aerial"},
		{
			name: "restate opts trimmed", brainURL: "http://eri:8080", restateOpts: []string{"http://restate:8080/", "agent-alpha"},
			wantBrain: "http://eri:8080", wantRestate: "http://restate:8080", wantAgent: "agent-alpha",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				if r := recover(); (r != nil) != tc.wantPanic {
					t.Errorf("recover() = %v, wantPanic %v", r, tc.wantPanic)
				}
			}()
			d := NewHTTPPromptDispatcher(tc.brainURL, tc.restateOpts...)
			if d.brainURL != tc.wantBrain || d.restateURL != tc.wantRestate || d.agentName != tc.wantAgent {
				t.Errorf("dispatcher = {%q %q %q}, want {%q %q %q}", d.brainURL, d.restateURL, d.agentName, tc.wantBrain, tc.wantRestate, tc.wantAgent)
			}
		})
	}
}

func TestServiceNameHelpers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		agent        string
		wantThread   string
		wantDispatch string
	}{
		{"", "ThreadSession", "DispatchSession"},
		{"agent-alpha", "ThreadSession-agent-alpha", "DispatchSession-agent-alpha"},
	}
	for _, tc := range tests {
		t.Run("agent_"+tc.agent, func(t *testing.T) {
			t.Parallel()
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

func TestRestateRequestError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		err     error
		wantErr string
	}{
		{name: "http error with message", err: &restateingress.HTTPError{Status: 409, Message: "conflict", Body: `{"message":"conflict"}`}, wantErr: "restate dispatch error (409): conflict"},
		{name: "http error without message", err: &restateingress.HTTPError{Status: 502, Body: "bad gateway"}, wantErr: "restate dispatch returned HTTP 502: bad gateway"},
		{name: "url parse error", err: &url.Error{Op: "parse", URL: "http://bad host", Err: errors.New("invalid character in host name")}, wantErr: `failed to create http request: parse "http://bad host": invalid character in host name`},
		{name: "transport error", err: &url.Error{Op: "Post", URL: "http://restate:8080", Err: errors.New("connection refused")}, wantErr: `dispatch failed: Post "http://restate:8080": connection refused`},
		{name: "read error", err: errors.New("simulated read failure"), wantErr: "failed to read response body: simulated read failure"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := restateRequestError("dispatch", tc.err)
			if got.Error() != tc.wantErr {
				t.Errorf("restateRequestError() = %q, want %q", got.Error(), tc.wantErr)
			}
			if !errors.Is(got, tc.err) && !strings.HasPrefix(tc.wantErr, "restate ") {
				t.Errorf("restateRequestError() does not wrap %v", tc.err)
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
	_, err := TraceFire(ctx, tc.scheduleType, tc.scheduleID, func(ctx context.Context) (eriwire.DispatchEnqueueResponse, error) {
		dispatchSC = trace.SpanContextFromContext(ctx)
		return eriwire.DispatchEnqueueResponse{}, tc.dispatchErr
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

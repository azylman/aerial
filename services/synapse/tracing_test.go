package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/tracing"
	"github.com/bwmarrin/discordgo"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// recordedParent starts and ends a span on an in-memory recorder and returns a context carrying
// it plus the trace id the recorder saw, the parent every outbound call must continue.
func recordedParent(t *testing.T) (context.Context, string) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sr))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	ctx, span := tp.Tracer("test").Start(context.Background(), "parent")
	span.End()
	ended := sr.Ended()
	if len(ended) != 1 {
		t.Fatalf("recorded %d parent spans, want 1", len(ended))
	}
	return ctx, ended[0].SpanContext().TraceID().String()
}

// traceparentTraceID parses a traceparent header value; "" when absent or malformed.
func traceparentTraceID(tp string) string {
	sc := trace.SpanContextFromContext(tracing.ContextWithTraceparent(context.Background(), tp))
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// assertRootTraceparent checks a persisted job traceparent names a valid, sampled trace.
func assertRootTraceparent(t *testing.T, tp string) {
	t.Helper()
	sc := trace.SpanContextFromContext(tracing.ContextWithTraceparent(context.Background(), tp))
	if !sc.IsValid() || !sc.IsSampled() {
		t.Errorf("job traceparent = %q, want a valid sampled trace context", tp)
	}
}

// traceparentServer records the traceparent of every request and answers with status/body.
type traceparentServer struct {
	*httptest.Server

	mu   sync.Mutex
	seen []string
}

func newTraceparentServer(t *testing.T, failFirst int, body string) *traceparentServer {
	t.Helper()
	s := &traceparentServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.seen = append(s.seen, r.Header.Get("Traceparent"))
		n := len(s.seen)
		s.mu.Unlock()
		if n <= failFirst {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *traceparentServer) traceIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.seen))
	for i, tp := range s.seen {
		out[i] = traceparentTraceID(tp)
	}
	return out
}

// assertTraceIDs checks every request carried a traceparent continuing (or not) the parent trace.
func assertTraceIDs(t *testing.T, got []string, wantRequests int, parentTraceID string, continuesParent bool) {
	t.Helper()
	if len(got) != wantRequests {
		t.Fatalf("got %d requests, want %d", len(got), wantRequests)
	}
	for i, id := range got {
		if id == "" {
			t.Errorf("request %d carried no valid traceparent", i)
		}
		if (id == parentTraceID) != continuesParent {
			t.Errorf("request %d trace id = %s, parent %s, want continues=%v", i, id, parentTraceID, continuesParent)
		}
	}
}

func runTraceparentDispatchCase(t *testing.T, target string, withParent bool, failFirst int, nilClient bool, wantRequests int) {
	t.Helper()
	var respBody string
	switch target {
	case "brain":
		respBody = `{"status":"queued"}`
	case "discord":
		respBody = `{"status":"ok"}`
	case "widget":
		respBody = `{"status":"ok","message_id":"msg-widget"}`
	}
	srv := newTraceparentServer(t, failFirst, respBody)
	parentCtx, parentTraceID := recordedParent(t)
	ctx := context.Background()
	if withParent {
		ctx = parentCtx
	}
	d := NewDefaultOutboundDispatcher(srv.URL, srv.Client())
	d.retryDelay = time.Millisecond
	switch target {
	case "brain":
		if err := d.postBrainWithRetry(ctx, "/dispatch", "dispatch", "brain dispatch", "", 3, time.Millisecond, false, DispatchRequest{Source: "webhook"}); err != nil {
			t.Fatalf("postBrainWithRetry: %v", err)
		}
	case "discord":
		client := d.discordMsgClient
		if nilClient {
			client = nil
		}
		if err := d.postDiscordInternalJSON(ctx, client, time.Minute, "/internal/discord/message", "discord message ingress", "discord ingress", DiscordMessageIngressRequest{}); err != nil {
			t.Fatalf("postDiscordInternalJSON: %v", err)
		}
	case "widget":
		if msgID, err := d.DispatchWidget(ctx, StatusWidgetRequest{ChannelID: "1555405874565091380", ID: "widget-1"}); err != nil || msgID != "msg-widget" {
			t.Fatalf("DispatchWidget = %q, %v", msgID, err)
		}
	}
	assertTraceIDs(t, srv.traceIDs(), wantRequests, parentTraceID, withParent)
}

func TestPostBrainWithRetry_Traceparent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		withParent   bool
		failFirst    int
		wantRequests int
	}{
		{name: "continues the caller's trace", withParent: true, wantRequests: 1},
		{name: "every retry attempt stays in the caller's trace", withParent: true, failFirst: 1, wantRequests: 2},
		{name: "starts a trace when the caller has none", wantRequests: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTraceparentDispatchCase(t, "brain", tc.withParent, tc.failFirst, false, tc.wantRequests)
		})
	}
}

func TestPostDiscordInternalJSON_Traceparent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		withParent bool
		nilClient  bool
	}{
		{name: "continues the gateway trace", withParent: true},
		{name: "default client also propagates", withParent: true, nilClient: true},
		{name: "starts a trace when the caller has none"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTraceparentDispatchCase(t, "discord", tc.withParent, 0, tc.nilClient, 1)
		})
	}
}

func TestDispatchWidget_Traceparent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		withParent bool
	}{
		{name: "widget post continues the caller's trace", withParent: true},
		{name: "starts a trace when the caller has none"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTraceparentDispatchCase(t, "widget", tc.withParent, 0, false, 1)
		})
	}
}

func TestDiscordInteractionWorker_ResumesGatewayTrace(t *testing.T) {
	t.Parallel()
	const persistedTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	tests := []struct {
		name            string
		traceparent     string
		continuesParent bool
	}{
		{name: "eri request continues the persisted gateway trace", traceparent: "00-" + persistedTraceID + "-00f067aa0ba902b7-01", continuesParent: true},
		{name: "jobs enqueued before tracing start a fresh trace", traceparent: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newTraceparentServer(t, 0, `{"status":"ok"}`)
			w := &DiscordInteractionWorker{server: newTestRouter(Config{}, nil, nil, NewDefaultOutboundDispatcher(srv.URL, srv.Client()))}
			job := &river.Job[DiscordInteractionArgs]{
				JobRow: &rivertype.JobRow{ID: 1, Attempt: 1, MaxAttempts: 1},
				Args: DiscordInteractionArgs{
					InteractionID: "inter-traced",
					Interaction:   &discordgo.Interaction{ID: "inter-traced", Type: discordgo.InteractionApplicationCommand},
					Traceparent:   tc.traceparent,
				},
			}
			if err := w.Work(context.Background(), job); err != nil {
				t.Fatalf("Work: %v", err)
			}
			assertTraceIDs(t, srv.traceIDs(), 1, persistedTraceID, tc.continuesParent)
		})
	}
}

func TestRegistryHaroClient_Traceparent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		injectClient bool // production passes the router's plain client via WithHaroClient
	}{
		{name: "injected client is wrapped and propagates", injectClient: true},
		{name: "default client propagates"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newTraceparentServer(t, 0, `{"status":"ok","matched":true}`)
			var client *http.Client
			if tc.injectClient {
				client = srv.Client()
			}
			reg := NewPostgresPRRegistry(nil, WithHaroClient(srv.URL, client))
			ctx, parentTraceID := recordedParent(t)
			if matched, err := reg.CheckEventTriggers(ctx, "flux", []string{"aerial"}, "deployed"); err != nil || !matched {
				t.Fatalf("CheckEventTriggers = %v, %v", matched, err)
			}
			assertTraceIDs(t, srv.traceIDs(), 1, parentTraceID, true)
		})
	}
}

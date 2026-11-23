package router

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauthtest"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/pkg/tracing"
	"github.com/azylman/aerial/services/synapse/internal/config"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
)

// --- Shared Test Mocks & Helpers ---

type mockEventBus struct {
	pubsub.Bus

	mu                   sync.Mutex
	insertedJobs         []GitHubWebhookArgs
	insertedDeployJobs   []DeployWebhookArgs
	insertedDiscordJobs  []DiscordIngressArgs
	insertedInteractJobs []DiscordInteractionArgs
	publishedPREvents    []pubsub.PRLifecycleEvent
	// publishedTraceparents records the W3C traceparent the real bus injects into
	// each published message's NATS headers.
	publishedTraceparents []string
	// subjects and events record every publish subject and each event-trigger payload.
	subjects []string
	events   []pubsub.ExternalEvent
	err      error
}

func (m *mockEventBus) Publish(ctx context.Context, subject, msgID string, data any) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return m.err
	}
	m.publishedTraceparents = append(m.publishedTraceparents, tracing.Traceparent(ctx))
	m.subjects = append(m.subjects, subject)
	m.recordPublishedEvents(data)
	m.recordPublishedJobs(data)
	return nil
}

func (m *mockEventBus) recordPublishedEvents(data any) {
	switch a := data.(type) {
	case pubsub.ExternalEvent:
		m.events = append(m.events, a)
	case pubsub.PRLifecycleEvent:
		m.publishedPREvents = append(m.publishedPREvents, a)
	case *pubsub.PRLifecycleEvent:
		if a != nil {
			m.publishedPREvents = append(m.publishedPREvents, *a)
		}
	}
}

func (m *mockEventBus) recordPublishedJobs(data any) {
	switch a := data.(type) {
	case GitHubWebhookArgs:
		m.insertedJobs = append(m.insertedJobs, a)
	case *GitHubWebhookArgs:
		if a != nil {
			m.insertedJobs = append(m.insertedJobs, *a)
		}
	case DeployWebhookArgs:
		m.insertedDeployJobs = append(m.insertedDeployJobs, a)
	case *DeployWebhookArgs:
		if a != nil {
			m.insertedDeployJobs = append(m.insertedDeployJobs, *a)
		}
	case DiscordIngressArgs:
		m.insertedDiscordJobs = append(m.insertedDiscordJobs, a)
	case *DiscordIngressArgs:
		if a != nil {
			m.insertedDiscordJobs = append(m.insertedDiscordJobs, *a)
		}
	case DiscordInteractionArgs:
		m.insertedInteractJobs = append(m.insertedInteractJobs, a)
	case *DiscordInteractionArgs:
		if a != nil {
			m.insertedInteractJobs = append(m.insertedInteractJobs, *a)
		}
	}
}

func (m *mockEventBus) publishCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.subjects)
}

type errReader struct{}

func (errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("simulated read error")
}

type zeroCallTransport struct {
	calls atomic.Int32
}

func (z *zeroCallTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	z.calls.Add(1)
	return nil, errors.New("unexpected outbound HTTP call")
}

func newTestRouter(cfg Config, client *http.Client, disp OutboundDispatcher, bus ...pubsub.Bus) *RouterServer {
	if client == nil && cfg.GitHubAPIURL == "" && cfg.KubeAPIServer == "" {
		client = &http.Client{Transport: &zeroCallTransport{}}
	}
	srv := NewRouterServer(fixedConfig(cfg), client)
	if disp != nil {
		srv.SetDispatcher(disp)
	}
	if len(bus) > 0 && bus[0] != nil {
		srv.SetEventBus(bus[0])
	}
	return srv
}

func doHTTPRequest(handler http.Handler, method, path string, body io.Reader, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, body)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

type fixedConfigSource struct{ cfg Config }

func (f *fixedConfigSource) Current() *Config { return &f.cfg }

func fixedConfig(cfg Config) ConfigSource { return &fixedConfigSource{cfg: cfg} }

func TestK8sAuthEnabled(t *testing.T) {
	t.Parallel()
	authCfg := k8sauthtest.Config(t, &k8sauthtest.RoundTripper{})
	tests := []struct {
		name string
		srv  *RouterServer
		want bool
	}{
		{"no config source", &RouterServer{}, false},
		{"config disables k8s auth", &RouterServer{cfgSrc: fixedConfig(Config{})}, false},
		{"config enables k8s auth", &RouterServer{cfgSrc: fixedConfig(Config{K8sAuthEnabled: true, K8sAuthConfig: authCfg})}, true},
		{"enabled authenticator without config", &RouterServer{authenticator: k8sauthtest.Authenticator(t, &k8sauthtest.RoundTripper{})}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.srv.k8sAuthEnabled(); got != tc.want {
				t.Errorf("k8sAuthEnabled() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestGetAuthenticator pins the lazy authenticator: built once from the
// config when k8s auth is enabled, then returned unchanged.
func TestGetAuthenticator(t *testing.T) {
	t.Parallel()
	authCfg := k8sauthtest.Config(t, &k8sauthtest.RoundTripper{})
	tests := []struct {
		name    string
		srv     *RouterServer
		wantNil bool
	}{
		{"no config source", &RouterServer{}, true},
		{"config disables k8s auth", &RouterServer{cfgSrc: fixedConfig(Config{})}, true},
		{"config enables k8s auth", &RouterServer{cfgSrc: fixedConfig(Config{K8sAuthEnabled: true, K8sAuthConfig: authCfg})}, false},
		{"preset authenticator", &RouterServer{authenticator: k8sauthtest.Authenticator(t, &k8sauthtest.RoundTripper{})}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			first := tc.srv.getAuthenticator()
			if (first == nil) != tc.wantNil {
				t.Fatalf("getAuthenticator() = %v, want nil %v", first, tc.wantNil)
			}
			if second := tc.srv.getAuthenticator(); second != first {
				t.Errorf("second getAuthenticator() = %p, want the cached %p", second, first)
			}
		})
	}
}

// collabAlertRouter is a router with one collaborator CI alert rule for
// acme/mirrormere that mentions @user for author "user".
func collabAlertRouter() *RouterServer {
	return &RouterServer{cfgSrc: fixedConfig(Config{
		GitHubOwner:          "acme",
		CollaboratorCIAlerts: []CollaboratorCIAlertRule{{Repo: "acme/mirrormere", ChannelID: "123", Authors: map[string]string{"user": "@user"}}},
	})}
}

func TestHasCollaboratorCIAlertsForRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		srv  *RouterServer
		repo string
		want bool
	}{
		{"nil router", nil, "mirrormere", false},
		{"nil config source", &RouterServer{}, "mirrormere", false},
		{"configured repo", collabAlertRouter(), "mirrormere", true},
		{"unconfigured repo", collabAlertRouter(), "aerial", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.srv.hasCollaboratorCIAlertsForRepo(tc.repo); got != tc.want {
				t.Errorf("hasCollaboratorCIAlertsForRepo(%q) = %v, want %v", tc.repo, got, tc.want)
			}
		})
	}
}

func TestLookupCollaboratorCIAlert(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		srv         *RouterServer
		author      string
		wantChannel string
		wantMention string
		wantOK      bool
	}{
		{name: "nil router", srv: nil, author: "user"},
		{name: "nil config source", srv: &RouterServer{}, author: "user"},
		{name: "configured author", srv: collabAlertRouter(), author: "user", wantChannel: "123", wantMention: "@user", wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ch, mention, ok := tc.srv.lookupCollaboratorCIAlert("mirrormere", tc.author)
			if ch != tc.wantChannel || mention != tc.wantMention || ok != tc.wantOK {
				t.Errorf("lookupCollaboratorCIAlert = (%q, %q, %v), want (%q, %q, %v)", ch, mention, ok, tc.wantChannel, tc.wantMention, tc.wantOK)
			}
		})
	}
}

func TestServerEndpoints_HealthMetricsAndCoverage(t *testing.T) {
	oldCountersFn := httpkit.WriteCountersDirFn
	oldMetaFn := httpkit.WriteMetaDirFn
	t.Cleanup(func() {
		httpkit.WriteCountersDirFn = oldCountersFn
		httpkit.WriteMetaDirFn = oldMetaFn
	})

	metrics.RecordJobEnqueued("", "", "")
	metrics.RecordJobRetry("", "", "")
	metrics.RecordJobDropped("", "", "")
	metrics.SetDiscordGatewayLatency(0.042)

	tests := []struct {
		name          string
		method        string
		path          string
		mockMetaErr   error
		mockCountErr  error
		queueStatsErr bool
		wantCode      int
		wantBodySubs  []string
	}{
		{
			name:         "/healthz returns 200 and json",
			method:       http.MethodGet,
			path:         "/healthz",
			wantCode:     http.StatusOK,
			wantBodySubs: []string{`"status":"ok"`, `"service":"synapse"`},
		},
		{
			name:     "/metrics returns 200 and prometheus metrics with queue stats and gateway latency",
			method:   http.MethodGet,
			path:     "/metrics",
			wantCode: http.StatusOK,
			wantBodySubs: []string{
				"synapse_queue_depth",
				"synapse_discord_gateway_latency_seconds 0.042",
			},
		},
		{
			name:          "/metrics with queue stats error logs and still returns 200",
			method:        http.MethodGet,
			path:          "/metrics",
			queueStatsErr: true,
			wantCode:      http.StatusOK,
			wantBodySubs:  []string{"synapse_discord_gateway_latency_seconds 0.042"},
		},
		{
			name:     "/debug/coverage/flush calls flush and returns 200",
			method:   http.MethodGet,
			path:     "/debug/coverage/flush",
			wantCode: http.StatusOK,
		},
		{
			name:        "/debug/coverage/flush meta error still returns 200",
			method:      http.MethodGet,
			path:        "/debug/coverage/flush",
			mockMetaErr: errors.New("simulated meta flush error"),
			wantCode:    http.StatusOK,
		},
		{
			name:         "/debug/coverage/flush counters error returns 500",
			method:       http.MethodGet,
			path:         "/debug/coverage/flush",
			mockCountErr: errors.New("simulated counters flush error"),
			wantCode:     http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			httpkit.WriteMetaDirFn = func(dir string) error {
				return tc.mockMetaErr
			}
			httpkit.WriteCountersDirFn = func(dir string) error {
				return tc.mockCountErr
			}

			srv := NewRouterServer(fixedConfig(Config{Port: "4020"}), nil)
			mockQ := &pubsubtest.ConsumerStats{Pending: map[string]int64{"synapse-github": 4}, Inflight: map[string]int64{"synapse-github": 1}}
			if tc.queueStatsErr {
				mockQ.Err = errors.New("simulated queue query error")
			}
			srv.queueStats = mockQ
			srv.SetGatewayLatencyFn(func() time.Duration {
				return 42 * time.Millisecond
			})

			rec := doHTTPRequest(srv.Routes(), tc.method, tc.path, nil, nil)
			if rec.Code != tc.wantCode {
				t.Fatalf("code = %d, want %d (body: %s)", rec.Code, tc.wantCode, rec.Body.String())
			}
			for _, sub := range tc.wantBodySubs {
				if !strings.Contains(rec.Body.String(), sub) {
					t.Errorf("body missing %q: %s", sub, rec.Body.String())
				}
			}
		})
	}

	rec := httptest.NewRecorder()
	httpkit.WriteJSON(rec, http.StatusOK, make(chan int))
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("writeJSON code = %d, want 500", rec.Code)
	}
}

func TestConsumerStatsQuerier(t *testing.T) {
	t.Parallel()
	var nilSrv *RouterServer
	if nilSrv.consumerStatsQuerier() != nil {
		t.Errorf("expected nil for nil server")
	}
	srv := NewRouterServer(fixedConfig(Config{}), nil)
	if srv.consumerStatsQuerier() != nil {
		t.Errorf("expected nil when no stats or bus configured")
	}
	memBus := pubsub.NewMemoryBus()
	srv.SetEventBus(memBus)
	if srv.consumerStatsQuerier() == nil {
		t.Errorf("expected bus stats querier when bus implements consumerStatsQuerier")
	}
	mockQ := &pubsubtest.ConsumerStats{}
	srv.queueStats = mockQ
	if srv.consumerStatsQuerier() != mockQ {
		t.Errorf("expected queueStats override")
	}
}

func newTestReloadable(initial, next Config) *httpkit.Reloadable[Config] {
	return httpkit.NewReloadable(&initial, func() (*Config, error) {
		reloaded := next
		return &reloaded, nil
	}, config.RestartRequired)
}

// testRestateURL is a never-dialed Restate ingress for lifecycle tests: the
// server only needs restate_url to pass startup validation.
const testRestateURL = "http://restate.test:8080"

func TestRunServerLifecycle(t *testing.T) {
	tests := []runServerLifecycleCase{
		{
			name:             "runServer start, loopback reload, and graceful cancel shutdown",
			config:           Config{Port: "0", RestateURL: testRestateURL},
			wantGraceful:     true,
			wantReloadStatus: httpkit.ReloadUnchanged,
		},
		{
			name:             "runServer live reload applies non-restart field",
			config:           Config{Port: "0", RestateURL: testRestateURL},
			reloadConfig:     &Config{Port: "0", RestateURL: testRestateURL, SystemChannelID: "123456789012345678"},
			wantGraceful:     true,
			wantReloadStatus: httpkit.ReloadApplied,
		},
		{
			name:    "runServer invalid port -1 returns error",
			config:  Config{Port: "-1", RestateURL: testRestateURL},
			wantErr: true,
		},
		{
			name:      "runApp pre-canceled context returns clean or server closed",
			config:    Config{Port: "0", RestateURL: testRestateURL},
			preCancel: true,
			isRunApp:  true,
		},
		{
			name:         "runApp exits cleanly when reload requires restart",
			config:       Config{Port: "0", RestateURL: testRestateURL},
			reloadConfig: &Config{Port: "4021", RestateURL: testRestateURL},
			isRunApp:     true,
		},
		{
			name:    "runServer without restate url returns error",
			config:  Config{Port: "0"},
			wantErr: true,
		},
		{
			name:     "runApp invalid port returns error",
			config:   Config{Port: "-1", RestateURL: testRestateURL},
			isRunApp: true,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			next := tc.config
			if tc.reloadConfig != nil {
				next = *tc.reloadConfig
			}
			cfgSrc := newTestReloadable(tc.config, next)
			if tc.wantGraceful {
				runServerGracefulCase(t, tc, cfgSrc, next)
				return
			}
			if tc.isRunApp {
				runServerAppCase(t, tc, cfgSrc)
				return
			}
			err := runServer(context.Background(), cfgSrc, nil)
			if (err != nil) != tc.wantErr {
				t.Errorf("runServer() err = %v, wantErr = %v", err, tc.wantErr)
			}
		})
	}
}

type runServerLifecycleCase struct {
	name             string
	config           Config
	reloadConfig     *Config
	preCancel        bool
	isRunApp         bool
	wantErr          bool
	wantGraceful     bool
	wantReloadStatus httpkit.ReloadStatus
}

func runServerGracefulCase(t *testing.T, tc runServerLifecycleCase, cfgSrc *httpkit.Reloadable[Config], next Config) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan string, 1)
	done := make(chan error, 1)

	go func() {
		done <- runServer(ctx, cfgSrc, func(addr string) {
			ready <- addr
		})
	}()

	var addr string
	select {
	case addr = <-ready:
		if addr == "" {
			t.Errorf("expected non-empty listening address")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for server to start")
	}

	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("SplitHostPort(%q): %v", addr, err)
	}
	resp, err := http.Post("http://"+net.JoinHostPort("127.0.0.1", port)+"/-/reload", "application/json", nil)
	if err != nil {
		t.Fatalf("POST /-/reload: %v", err)
	}
	var got struct {
		Status httpkit.ReloadStatus `json:"status"`
	}
	decodeErr := json.NewDecoder(resp.Body).Decode(&got)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK || decodeErr != nil || got.Status != tc.wantReloadStatus {
		t.Errorf("POST /-/reload = %d %q (decode err %v), want 200 %q", resp.StatusCode, got.Status, decodeErr, tc.wantReloadStatus)
	}
	if gotSys := cfgSrc.Current().SystemChannelID; gotSys != next.SystemChannelID {
		t.Errorf("SystemChannelID after reload = %q, want %q", gotSys, next.SystemChannelID)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("expected clean shutdown, got error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timeout waiting for server shutdown")
	}
}

func runServerAppCase(t *testing.T, tc runServerLifecycleCase, cfgSrc *httpkit.Reloadable[Config]) {
	t.Helper()
	ctxC, cancelC := context.WithCancel(context.Background())
	if tc.preCancel {
		cancelC()
	} else {
		defer cancelC()
	}
	if tc.reloadConfig != nil {
		if status, err := cfgSrc.Reload(); status != httpkit.ReloadRestarting || err != nil {
			t.Fatalf("Reload() = %q, %v; want %q", status, err, httpkit.ReloadRestarting)
		}
	}
	err := runApp(ctxC, cfgSrc)
	if (err != nil) != tc.wantErr && !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("unexpected error from runApp: %v", err)
	}
}

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
	}
	srv := newTraceparentServer(t, failFirst, respBody)
	parentCtx, parentTraceID := recordedParent(t)
	ctx := context.Background()
	if withParent {
		ctx = parentCtx
	}
	client := srv.Client()
	if nilClient {
		client = nil
	}
	d := NewDefaultOutboundDispatcher(srv.URL, client)
	d.SetRetryConfig(time.Millisecond, 6)
	switch target {
	case "brain":
		if err := d.DispatchBackground(ctx, DispatchRequest{Source: "webhook"}); err != nil {
			t.Fatalf("DispatchBackground: %v", err)
		}
	case "discord":
		if err := d.DispatchDiscordMessage(ctx, DiscordMessageIngressRequest{}); err != nil {
			t.Fatalf("DispatchDiscordMessage: %v", err)
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

func TestDiscordInteractionWorker_ResumesGatewayTrace(t *testing.T) {
	t.Parallel()
	const persistedTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	const persistedTraceparent = "00-" + persistedTraceID + "-00f067aa0ba902b7-01"
	tests := []struct {
		name              string
		headerTraceparent string
		bodyTraceparent   string
		continuesParent   bool
	}{
		{name: "eri request continues the header gateway trace", headerTraceparent: persistedTraceparent, continuesParent: true},
		{name: "legacy body traceparent decodes and is ignored", bodyTraceparent: persistedTraceparent},
		{name: "jobs without header context start a fresh trace"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := newTraceparentServer(t, 0, `{"status":"ok"}`)
			router := newTestRouter(Config{}, nil, NewDefaultOutboundDispatcher(srv.URL, srv.Client()))
			body := fmt.Sprintf(`{"interaction_id":"inter-traced","interaction":{"id":"inter-traced","type":2,"data":{"id":"cmd-1","name":"ask"}},"traceparent":%q}`, tc.bodyTraceparent)
			msg := &pubsubtest.Message{Payload: []byte(body), Subj: "aerial.jobs.synapse.discord_interaction", Delivered: 1}
			ctx := tracing.ContextWithTraceparent(context.Background(), tc.headerTraceparent)
			if err := router.HandleDiscordInteractionJob(ctx, msg); err != nil {
				t.Fatalf("HandleDiscordInteractionJob: %v", err)
			}
			assertTraceIDs(t, srv.traceIDs(), 1, persistedTraceID, tc.continuesParent)
		})
	}
}

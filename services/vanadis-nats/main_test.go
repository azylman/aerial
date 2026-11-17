package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/tracing"
)

type testMsg struct {
	data      []byte
	subject   string
	header    nats.Header
	delivered uint64
	acked     atomic.Bool
	naked     atomic.Bool
	nakDelay  time.Duration
	nakErr    error
	termed    atomic.Bool
	termErr   error
}

func (m *testMsg) Data() []byte { return m.data }

func (m *testMsg) Subject() string { return m.subject }

func (m *testMsg) Header() nats.Header {
	if m.header == nil {
		m.header = nats.Header{}
	}
	return m.header
}

func (m *testMsg) NumDelivered() uint64 { return m.delivered }

func (m *testMsg) Ack() error {
	m.acked.Store(true)
	return nil
}

func (m *testMsg) NakWithDelay(d time.Duration) error {
	m.naked.Store(true)
	m.nakDelay = d
	return m.nakErr
}

func (m *testMsg) Term() error {
	m.termed.Store(true)
	return m.termErr
}

func (*testMsg) Snooze(time.Duration) error { return nil }

func (*testMsg) InProgress() error { return nil }

type testBus struct {
	ensureErr   error
	consumeErrs map[string]error
	subscribed  map[string]bool
	mu          sync.Mutex
	closed      bool
}

func (*testBus) Publish(context.Context, string, string, any) error { return nil }

func (*testBus) Subscribe(context.Context, string, string, pubsub.Handler) (pubsub.Subscription, error) {
	return nil, nil
}

func (b *testBus) ConsumeQueue(_ context.Context, subject, queue string, _ pubsub.Handler) (pubsub.Subscription, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.consumeErrs != nil {
		if err, ok := b.consumeErrs[subject]; ok && err != nil {
			return nil, err
		}
	}
	if b.subscribed == nil {
		b.subscribed = make(map[string]bool)
	}
	b.subscribed[subject+":"+queue] = true
	return &testSub{}, nil
}

func (b *testBus) EnsureCoreStreams(context.Context) error { return b.ensureErr }

func (b *testBus) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed = true
	return nil
}

type testSub struct{}

func (*testSub) Unsubscribe() error { return nil }

func TestLoadConfig(t *testing.T) {
	tmpDir := t.TempDir()
	validYAML := filepath.Join(tmpDir, "config.yaml")
	require.NoError(t, os.WriteFile(validYAML, []byte("port: \"5050\"\nnats_url: \"nats://file:4222\"\nrestate_ingress_url: \"http://restate-file:8080/\"\n"), 0o600))

	invalidYAML := filepath.Join(tmpDir, "invalid.yaml")
	require.NoError(t, os.WriteFile(invalidYAML, []byte("invalid: ["), 0o600))

	fallbackYAML := filepath.Join(tmpDir, "fallback.yaml")
	require.NoError(t, os.WriteFile(fallbackYAML, []byte("port: \"4033\"\n"), 0o600))

	tests := []struct {
		name          string
		configPath    string
		envPort       string
		envNATS       string
		envRestate    string
		fallbackPaths []string
		wantPort      string
		wantNATS      string
		wantRestate   string
	}{
		{
			name:        "defaults when empty",
			wantPort:    "4027",
			wantNATS:    "",
			wantRestate: "http://restate.restate.svc.cluster.local:8080",
		},
		{
			name:        "loaded from yaml file",
			configPath:  validYAML,
			wantPort:    "5050",
			wantNATS:    "nats://file:4222",
			wantRestate: "http://restate-file:8080",
		},
		{
			name:        "env overrides file",
			configPath:  validYAML,
			envPort:     "6060",
			envNATS:     "nats://env:4222",
			envRestate:  "http://restate-env:8080/",
			wantPort:    "6060",
			wantNATS:    "nats://env:4222",
			wantRestate: "http://restate-env:8080",
		},
		{
			name:        "invalid yaml falls back to defaults",
			configPath:  invalidYAML,
			wantPort:    "4027",
			wantNATS:    "",
			wantRestate: "http://restate.restate.svc.cluster.local:8080",
		},
		{
			name:          "fallback config path matches",
			configPath:    "",
			fallbackPaths: []string{filepath.Join(tmpDir, "nonexistent.yaml"), fallbackYAML},
			wantPort:      "4033",
			wantNATS:      "",
			wantRestate:   "http://restate.restate.svc.cluster.local:8080",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("PORT", tc.envPort)
			t.Setenv("NATS_URL", tc.envNATS)
			t.Setenv("RESTATE_INGRESS_URL", tc.envRestate)

			if len(tc.fallbackPaths) > 0 {
				origFallbacks := fallbackConfigPaths
				defer func() { fallbackConfigPaths = origFallbacks }()
				fallbackConfigPaths = tc.fallbackPaths
			}

			cfg := LoadConfig(tc.configPath)
			assert.Equal(t, tc.wantPort, cfg.Port)
			assert.Equal(t, tc.wantNATS, cfg.NATSURL)
			assert.Equal(t, tc.wantRestate, cfg.RestateIngressURL)
		})
	}
}

func TestLoadConfigFromEnv(t *testing.T) {
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "from_env.yaml")
	require.NoError(t, os.WriteFile(configFile, []byte("port: \"4029\"\n"), 0o600))

	t.Setenv("CONFIG_PATH", configFile)
	t.Setenv("PORT", "")
	cfg := LoadConfigFromEnv()
	assert.Equal(t, "4029", cfg.Port)
}

func TestNATSBridge_StartConsumers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bus        pubsub.Bus
		wantErrMsg string
	}{
		{
			name:       "nil bus returns error",
			bus:        nil,
			wantErrMsg: "pubsub bus is nil",
		},
		{
			name:       "ensure core streams error",
			bus:        &testBus{ensureErr: errors.New("stream err")},
			wantErrMsg: "ensure core streams",
		},
		{
			name:       "consume queue dispatch error",
			bus:        &testBus{consumeErrs: map[string]error{"aerial.dispatch.*": errors.New("dispatch queue err")}},
			wantErrMsg: "subscribe vanadis-dispatch queue",
		},
		{
			name:       "consume queue pr-events error",
			bus:        &testBus{consumeErrs: map[string]error{"aerial.pr.>": errors.New("pr queue err")}},
			wantErrMsg: "subscribe vanadis-pr-events queue",
		},
		{
			name:       "consume queue widget-events error",
			bus:        &testBus{consumeErrs: map[string]error{"aerial.coalesced.widget_sync.>": errors.New("widget queue err")}},
			wantErrMsg: "subscribe vanadis-widget queue",
		},
		{
			name:       "successful consumers registration",
			bus:        &testBus{},
			wantErrMsg: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			bridge := NewNATSBridge(Config{}, tc.bus, nil)
			err := bridge.StartConsumers(context.Background())
			if tc.wantErrMsg == "" {
				require.NoError(t, err)
				tb, ok := tc.bus.(*testBus)
				if ok {
					tb.mu.Lock()
					assert.True(t, tb.subscribed["aerial.dispatch.*:vanadis-dispatch"])
					assert.True(t, tb.subscribed["aerial.pr.>:vanadis-pr-events"])
					assert.True(t, tb.subscribed["aerial.coalesced.widget_sync.>:vanadis-widget"])
					tb.mu.Unlock()
				}
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			}
		})
	}
}

type prTestCase struct {
	name          string
	msgData       string
	msgHeader     nats.Header
	delivered     uint64
	serverStatus  int
	closeServer   bool
	overrideURL   string
	wantErr       bool
	wantNaked     bool
	wantTermed    bool
	wantNakDelay  time.Duration
	wantRequests  int
	wantKey       string
	wantRepo      string
	wantEventName string
	wantPRNumber  int
}

func runPRTestCase(t *testing.T, tc prTestCase) {
	t.Helper()
	var reqCount int
	var capturedKey string
	var capturedPath string
	var capturedEvent pubsub.PRLifecycleEvent

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount++
		capturedKey = r.Header.Get("Idempotency-Key")
		capturedPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &capturedEvent)

		if tc.serverStatus != 0 {
			w.WriteHeader(tc.serverStatus)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	serverURL := ts.URL
	if tc.overrideURL != "" {
		serverURL = tc.overrideURL
	} else if tc.closeServer {
		ts.Close()
	}

	bridge := NewNATSBridge(Config{RestateIngressURL: serverURL}, &testBus{}, ts.Client())

	msg := &testMsg{
		data:      []byte(tc.msgData),
		subject:   "aerial.pr.opened",
		header:    tc.msgHeader,
		delivered: tc.delivered,
	}

	err := bridge.HandlePRLifecycleEvent(context.Background(), msg)
	if tc.wantErr {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}

	assert.Equal(t, tc.wantRequests, reqCount)

	assert.Equal(t, tc.wantNaked, msg.naked.Load())
	assert.Equal(t, tc.wantTermed, msg.termed.Load())
	if tc.wantNakDelay > 0 {
		assert.Equal(t, tc.wantNakDelay, msg.nakDelay)
	}

	if tc.wantRequests > 0 {
		assert.Equal(t, "/BuildRouter/RoutePREvent/send", capturedPath)
		if tc.wantKey != "" {
			assert.Equal(t, tc.wantKey, capturedKey)
		}
		if tc.wantRepo != "" {
			assert.Equal(t, tc.wantRepo, capturedEvent.Repo)
		}
		if tc.wantEventName != "" {
			assert.Equal(t, tc.wantEventName, capturedEvent.Event)
		}
		if tc.wantPRNumber > 0 {
			assert.Equal(t, tc.wantPRNumber, capturedEvent.PRNumber)
		}
	}
}

func TestHandlePRLifecycleEvent_Validation(t *testing.T) {
	t.Parallel()

	tests := []prTestCase{
		{
			name:         "malformed json returns nil",
			msgData:      "{not-json",
			wantRequests: 0,
			wantErr:      false,
		},
		{
			name:         "empty repo returns nil",
			msgData:      `{"event":"opened","repo":"","pr_number":10}`,
			wantRequests: 0,
			wantErr:      false,
		},
		{
			name:         "zero pr_number returns nil",
			msgData:      `{"event":"opened","repo":"azylman/aerial","pr_number":0}`,
			wantRequests: 0,
			wantErr:      false,
		},
		{
			name:          "restate 202 accepted",
			msgData:       `{"event":"opened","repo":"azylman/aerial","pr_number":10}`,
			serverStatus:  http.StatusAccepted,
			wantRequests:  1,
			wantRepo:      "azylman/aerial",
			wantEventName: "opened",
			wantPRNumber:  10,
			wantErr:       false,
		},
		{
			name:          "restate 400 terminal ack",
			msgData:       `{"event":"synchronize","repo":"azylman/aerial","pr_number":10}`,
			serverStatus:  http.StatusBadRequest,
			wantRequests:  1,
			wantRepo:      "azylman/aerial",
			wantEventName: "synchronize",
			wantPRNumber:  10,
			wantErr:       false,
		},
		{
			name:          "restate 404 terminal ack",
			msgData:       `{"event":"check_updated","repo":"azylman/aerial","pr_number":10}`,
			serverStatus:  http.StatusNotFound,
			wantRequests:  1,
			wantRepo:      "azylman/aerial",
			wantEventName: "check_updated",
			wantPRNumber:  10,
			wantErr:       false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runPRTestCase(t, tc)
		})
	}
}

func TestHandlePRLifecycleEvent_ErrorsAndRetries(t *testing.T) {
	t.Parallel()

	tests := []prTestCase{
		{
			name:          "restate 500 attempt 1 naks with 1s",
			msgData:       `{"event":"opened","repo":"azylman/aerial","pr_number":10}`,
			delivered:     1,
			serverStatus:  http.StatusInternalServerError,
			wantRequests:  1,
			wantRepo:      "azylman/aerial",
			wantEventName: "opened",
			wantPRNumber:  10,
			wantErr:       true,
			wantNaked:     true,
			wantNakDelay:  1 * time.Second,
		},
		{
			name:          "restate 500 attempt 2 naks with 2s",
			msgData:       `{"event":"opened","repo":"azylman/aerial","pr_number":10}`,
			delivered:     2,
			serverStatus:  http.StatusInternalServerError,
			wantRequests:  1,
			wantRepo:      "azylman/aerial",
			wantEventName: "opened",
			wantPRNumber:  10,
			wantErr:       true,
			wantNaked:     true,
			wantNakDelay:  2 * time.Second,
		},
		{
			name:          "restate 500 attempt 3 terms",
			msgData:       `{"event":"opened","repo":"azylman/aerial","pr_number":10}`,
			delivered:     3,
			serverStatus:  http.StatusInternalServerError,
			wantRequests:  1,
			wantRepo:      "azylman/aerial",
			wantEventName: "opened",
			wantPRNumber:  10,
			wantErr:       true,
			wantTermed:    true,
		},
		{
			name:         "transport error naks with delay",
			msgData:      `{"event":"opened","repo":"azylman/aerial","pr_number":10}`,
			delivered:    1,
			closeServer:  true,
			wantRequests: 0,
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 1 * time.Second,
		},
		{
			name:         "request creation error on invalid url",
			msgData:      `{"event":"opened","repo":"azylman/aerial","pr_number":10}`,
			delivered:    1,
			overrideURL:  "://invalid-url\x7f",
			wantRequests: 0,
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 1 * time.Second,
		},
		{
			name:          "custom MsgID header used as idempotency key",
			msgData:       `{"event":"opened","repo":"azylman/aerial","pr_number":10}`,
			msgHeader:     nats.Header{pubsub.HeaderMsgID: []string{"custom-pr-id-123"}},
			serverStatus:  http.StatusOK,
			wantRequests:  1,
			wantRepo:      "azylman/aerial",
			wantEventName: "opened",
			wantPRNumber:  10,
			wantKey:       "custom-pr-id-123",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runPRTestCase(t, tc)
		})
	}
}

type dispatchTestCase struct {
	name           string
	msgData        string
	msgHeader      nats.Header
	delivered      uint64
	serverStatus   int
	closeServer    bool
	wantErr        bool
	wantNaked      bool
	wantTermed     bool
	wantNakDelay   time.Duration
	wantRequests   int
	wantKey        string
	wantDispatchID string
	wantChunkID    string
	wantStatus     string
}

func runDispatchTestCase(t *testing.T, tc dispatchTestCase) {
	t.Helper()
	var reqCount int
	var capturedKey string
	var capturedPath string
	var capturedEvent pubsub.DispatchOutcomeEvent

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount++
		capturedKey = r.Header.Get("Idempotency-Key")
		capturedPath = r.URL.Path
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &capturedEvent)

		if tc.serverStatus != 0 {
			w.WriteHeader(tc.serverStatus)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	serverURL := ts.URL
	if tc.closeServer {
		ts.Close()
	}

	bridge := NewNATSBridge(Config{RestateIngressURL: serverURL}, &testBus{}, ts.Client())

	msg := &testMsg{
		data:      []byte(tc.msgData),
		subject:   "aerial.dispatch.completed",
		header:    tc.msgHeader,
		delivered: tc.delivered,
	}

	err := bridge.HandleDispatchOutcomeEvent(context.Background(), msg)
	if tc.wantErr {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}

	assert.Equal(t, tc.wantRequests, reqCount)

	assert.Equal(t, tc.wantNaked, msg.naked.Load())
	assert.Equal(t, tc.wantTermed, msg.termed.Load())
	if tc.wantNakDelay > 0 {
		assert.Equal(t, tc.wantNakDelay, msg.nakDelay)
	}

	if tc.wantRequests > 0 {
		assert.Equal(t, "/BuildRouter/RouteDispatchOutcome/send", capturedPath)
		if tc.wantKey != "" {
			assert.Equal(t, tc.wantKey, capturedKey)
		}
		if tc.wantDispatchID != "" {
			assert.Equal(t, tc.wantDispatchID, capturedEvent.DispatchID)
		}
		if tc.wantChunkID != "" {
			assert.Equal(t, tc.wantChunkID, capturedEvent.VanadisChunkID)
		}
		if tc.wantStatus != "" {
			assert.Equal(t, tc.wantStatus, capturedEvent.Status)
		}
	}
}

func TestHandleDispatchOutcomeEvent_Filtering(t *testing.T) {
	t.Parallel()

	tests := []dispatchTestCase{
		{
			name:         "malformed json returns nil",
			msgData:      "{bad-json",
			wantRequests: 0,
			wantErr:      false,
		},
		{
			name:         "healthcheck-ping ignored",
			msgData:      `{"dispatch_id":"healthcheck-ping","vanadis_chunk_id":"chk-1","status":"completed"}`,
			wantRequests: 0,
			wantErr:      false,
		},
		{
			name:         "empty vanadis_chunk_id ignored",
			msgData:      `{"dispatch_id":"disp-1","vanadis_chunk_id":"","status":"completed"}`,
			wantRequests: 0,
			wantErr:      false,
		},
		{
			name:         "unhandled status ignored",
			msgData:      `{"dispatch_id":"disp-1","vanadis_chunk_id":"chk-1","status":"running"}`,
			wantRequests: 0,
			wantErr:      false,
		},
		{
			name:           "completed status forwards to restate",
			msgData:        `{"dispatch_id":"disp-1","vanadis_chunk_id":"chk-1","status":"completed"}`,
			serverStatus:   http.StatusOK,
			wantRequests:   1,
			wantDispatchID: "disp-1",
			wantChunkID:    "chk-1",
			wantStatus:     "completed",
			wantErr:        false,
			wantKey:        "dispatch:disp-1:chk-1:completed",
		},
		{
			name:           "failed status forwards to restate",
			msgData:        `{"dispatch_id":"disp-2","vanadis_chunk_id":"chk-2","status":"failed"}`,
			serverStatus:   http.StatusOK,
			wantRequests:   1,
			wantDispatchID: "disp-2",
			wantChunkID:    "chk-2",
			wantStatus:     "failed",
			wantErr:        false,
			wantKey:        "dispatch:disp-2:chk-2:failed",
		},
		{
			name:           "timeout status forwards to restate",
			msgData:        `{"dispatch_id":"disp-3","vanadis_chunk_id":"chk-3","status":"timeout"}`,
			serverStatus:   http.StatusOK,
			wantRequests:   1,
			wantDispatchID: "disp-3",
			wantChunkID:    "chk-3",
			wantStatus:     "timeout",
			wantErr:        false,
			wantKey:        "dispatch:disp-3:chk-3:timeout",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDispatchTestCase(t, tc)
		})
	}
}

func TestHandleDispatchOutcomeEvent_Errors(t *testing.T) {
	t.Parallel()

	tests := []dispatchTestCase{
		{
			name:           "restate 400 terminal ack",
			msgData:        `{"dispatch_id":"disp-1","vanadis_chunk_id":"chk-1","status":"completed"}`,
			serverStatus:   http.StatusBadRequest,
			wantRequests:   1,
			wantDispatchID: "disp-1",
			wantChunkID:    "chk-1",
			wantStatus:     "completed",
			wantErr:        false,
		},
		{
			name:           "restate 500 attempt 1 naks with delay",
			msgData:        `{"dispatch_id":"disp-1","vanadis_chunk_id":"chk-1","status":"completed"}`,
			delivered:      1,
			serverStatus:   http.StatusInternalServerError,
			wantRequests:   1,
			wantDispatchID: "disp-1",
			wantChunkID:    "chk-1",
			wantStatus:     "completed",
			wantErr:        true,
			wantNaked:      true,
			wantNakDelay:   1 * time.Second,
		},
		{
			name:           "restate 500 attempt 3 terms",
			msgData:        `{"dispatch_id":"disp-1","vanadis_chunk_id":"chk-1","status":"completed"}`,
			delivered:      3,
			serverStatus:   http.StatusInternalServerError,
			wantRequests:   1,
			wantDispatchID: "disp-1",
			wantChunkID:    "chk-1",
			wantStatus:     "completed",
			wantErr:        true,
			wantTermed:     true,
		},
		{
			name:         "transport error naks with delay",
			msgData:      `{"dispatch_id":"disp-1","vanadis_chunk_id":"chk-1","status":"completed"}`,
			delivered:    1,
			closeServer:  true,
			wantRequests: 0,
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 1 * time.Second,
		},
		{
			name:           "custom MsgID header takes precedence",
			msgData:        `{"dispatch_id":"disp-1","vanadis_chunk_id":"chk-1","status":"completed"}`,
			msgHeader:      nats.Header{pubsub.HeaderMsgID: []string{"custom-dispatch-key"}},
			serverStatus:   http.StatusOK,
			wantRequests:   1,
			wantDispatchID: "disp-1",
			wantChunkID:    "chk-1",
			wantStatus:     "completed",
			wantKey:        "custom-dispatch-key",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDispatchTestCase(t, tc)
		})
	}
}

type widgetSyncTestCase struct {
	name          string
	msgData       string
	msgHeader     nats.Header
	delivered     uint64
	serverStatus  int
	wantErr       bool
	wantNaked     bool
	wantNakDelay  time.Duration
	wantRequests  int
	wantKey       string
	wantPath      string
	wantReconcile map[string]string
	wantPREvent   pubsub.PRLifecycleEvent
}

func runWidgetSyncTestCase(t *testing.T, tc widgetSyncTestCase) {
	t.Helper()
	var reqCount int
	var capturedKey string
	var capturedPath string
	var capturedBody []byte

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount++
		capturedKey = r.Header.Get("Idempotency-Key")
		capturedPath = r.URL.Path
		capturedBody, _ = io.ReadAll(r.Body)

		if tc.serverStatus != 0 {
			w.WriteHeader(tc.serverStatus)
		} else {
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer ts.Close()

	bridge := NewNATSBridge(Config{RestateIngressURL: ts.URL}, &testBus{}, ts.Client())

	msg := &testMsg{
		data:      []byte(tc.msgData),
		subject:   "aerial.coalesced.widget_sync.build.b-1",
		header:    tc.msgHeader,
		delivered: tc.delivered,
	}

	err := bridge.HandleWidgetSyncEvent(context.Background(), msg)
	if tc.wantErr {
		require.Error(t, err)
	} else {
		require.NoError(t, err)
	}

	assert.Equal(t, tc.wantRequests, reqCount)
	assert.Equal(t, tc.wantNaked, msg.naked.Load())
	if tc.wantNakDelay > 0 {
		assert.Equal(t, tc.wantNakDelay, msg.nakDelay)
	}

	if tc.wantRequests > 0 {
		assert.Equal(t, tc.wantPath, capturedPath)
		if tc.wantKey != "" {
			assert.Equal(t, tc.wantKey, capturedKey)
		}
		if tc.wantReconcile != nil {
			var m map[string]string
			require.NoError(t, json.Unmarshal(capturedBody, &m))
			assert.Equal(t, tc.wantReconcile, m)
		}
		if tc.wantPREvent.Repo != "" {
			var pe pubsub.PRLifecycleEvent
			require.NoError(t, json.Unmarshal(capturedBody, &pe))
			assert.Equal(t, tc.wantPREvent.Event, pe.Event)
			assert.Equal(t, tc.wantPREvent.Repo, pe.Repo)
			assert.Equal(t, tc.wantPREvent.PRNumber, pe.PRNumber)
		}
	}
}

func TestHandleWidgetSyncEvent(t *testing.T) {
	t.Parallel()

	tests := []widgetSyncTestCase{
		{
			name:         "malformed json returns nil",
			msgData:      "{not-valid-json",
			wantRequests: 0,
			wantErr:      false,
		},
		{
			name:         "empty args returns nil",
			msgData:      `{}`,
			wantRequests: 0,
			wantErr:      false,
		},
		{
			name:         "build_id routing to Reconcile send with fallback key",
			msgData:      `{"build_id":"build-987"}`,
			serverStatus: http.StatusOK,
			wantRequests: 1,
			wantPath:     "/Build/build-987/Reconcile/send",
			wantKey:      "widget:build:build-987",
			wantReconcile: map[string]string{
				"reason": "widget_sync",
			},
			wantErr: false,
		},
		{
			name:         "build_id routing with HeaderMsgID precedence",
			msgData:      `{"build_id":"build-987"}`,
			msgHeader:    nats.Header{pubsub.HeaderMsgID: []string{"custom-build-msgid"}},
			wantRequests: 1,
			wantPath:     "/Build/build-987/Reconcile/send",
			wantKey:      "custom-build-msgid",
			wantReconcile: map[string]string{
				"reason": "widget_sync",
			},
			wantErr: false,
		},
		{
			name:         "build_id routing with Idempotency-Key header precedence",
			msgData:      `{"build_id":"build-987"}`,
			msgHeader:    nats.Header{"Idempotency-Key": []string{"custom-build-idem"}},
			wantRequests: 1,
			wantPath:     "/Build/build-987/Reconcile/send",
			wantKey:      "custom-build-idem",
			wantReconcile: map[string]string{
				"reason": "widget_sync",
			},
			wantErr: false,
		},
		{
			name:         "repo and pr_number routing to RoutePREvent send with fallback key",
			msgData:      `{"repo":"azylman/aerial","pr_number":42}`,
			serverStatus: http.StatusOK,
			wantRequests: 1,
			wantPath:     "/BuildRouter/RoutePREvent/send",
			wantKey:      "widget:pr:azylman/aerial:42",
			wantPREvent: pubsub.PRLifecycleEvent{
				Event:    "widget_sync",
				Repo:     "azylman/aerial",
				PRNumber: 42,
			},
			wantErr: false,
		},
		{
			name:         "repo and pr_number routing with HeaderMsgID precedence",
			msgData:      `{"repo":"azylman/aerial","pr_number":42}`,
			msgHeader:    nats.Header{pubsub.HeaderMsgID: []string{"custom-pr-msgid"}},
			wantRequests: 1,
			wantPath:     "/BuildRouter/RoutePREvent/send",
			wantKey:      "custom-pr-msgid",
			wantPREvent: pubsub.PRLifecycleEvent{
				Event:    "widget_sync",
				Repo:     "azylman/aerial",
				PRNumber: 42,
			},
			wantErr: false,
		},
		{
			name:         "repo without positive pr_number returns nil",
			msgData:      `{"repo":"azylman/aerial","pr_number":0}`,
			wantRequests: 0,
			wantErr:      false,
		},
		{
			name:         "pr_number without repo returns nil",
			msgData:      `{"repo":"","pr_number":42}`,
			wantRequests: 0,
			wantErr:      false,
		},
		{
			name:         "restate 500 attempt 1 naks with delay",
			msgData:      `{"build_id":"build-err"}`,
			delivered:    1,
			serverStatus: http.StatusInternalServerError,
			wantRequests: 1,
			wantPath:     "/Build/build-err/Reconcile/send",
			wantErr:      true,
			wantNaked:    true,
			wantNakDelay: 1 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runWidgetSyncTestCase(t, tc)
		})
	}
}

func TestSettleFailedDelivery_Direct(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		delivered    uint64
		nakErr       error
		termErr      error
		wantNaked    bool
		wantTermed   bool
		wantNakDelay time.Duration
	}{
		{
			name:         "delivered 1 with nak error logs warning",
			delivered:    1,
			nakErr:       errors.New("nak network err"),
			wantNaked:    true,
			wantNakDelay: 1 * time.Second,
		},
		{
			name:       "delivered 3 with term error logs warning",
			delivered:  3,
			termErr:    errors.New("term network err"),
			wantTermed: true,
		},
		{
			name:       "delivered 5 terms",
			delivered:  5,
			wantTermed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg := &testMsg{
				delivered: tc.delivered,
				nakErr:    tc.nakErr,
				termErr:   tc.termErr,
			}
			settleFailedDelivery(msg)
			assert.Equal(t, tc.wantNaked, msg.naked.Load())
			assert.Equal(t, tc.wantTermed, msg.termed.Load())
			if tc.wantNakDelay > 0 {
				assert.Equal(t, tc.wantNakDelay, msg.nakDelay)
			}
		})
	}
}

func TestIdempotencyKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		isPR       bool
		headerKey  string
		headerVal  string
		prEvent    pubsub.PRLifecycleEvent
		dispEvent  pubsub.DispatchOutcomeEvent
		wantResult string
	}{
		{
			name:       "pr with Idempotency-Key header",
			isPR:       true,
			headerKey:  "Idempotency-Key",
			headerVal:  "idem-456",
			wantResult: "idem-456",
		},
		{
			name:       "pr zero occurred_at fallback",
			isPR:       true,
			prEvent:    pubsub.PRLifecycleEvent{Repo: "azylman/aerial", PRNumber: 42, Event: "opened", HeadSHA: "abc", CheckName: "ci"},
			wantResult: "pr:azylman/aerial:42:opened:abc:ci",
		},
		{
			name:       "pr non-zero occurred_at fallback",
			isPR:       true,
			prEvent:    pubsub.PRLifecycleEvent{Repo: "azylman/aerial", PRNumber: 42, Event: "opened", HeadSHA: "abc", CheckName: "ci", OccurredAt: time.Unix(100, 200)},
			wantResult: "pr:azylman/aerial:42:opened:abc:ci:100000000200",
		},
		{
			name:       "dispatch with Idempotency-Key header",
			isPR:       false,
			headerKey:  "Idempotency-Key",
			headerVal:  "disp-idem-456",
			wantResult: "disp-idem-456",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var msg *testMsg
			if tc.headerKey != "" {
				msg = &testMsg{
					header: nats.Header{tc.headerKey: []string{tc.headerVal}},
				}
			}
			var subMsg pubsub.Message
			if msg != nil {
				subMsg = msg
			}
			if tc.isPR {
				got := prIdempotencyKey(subMsg, tc.prEvent)
				assert.Equal(t, tc.wantResult, got)
			} else {
				got := dispatchIdempotencyKey(subMsg, tc.dispEvent)
				assert.Equal(t, tc.wantResult, got)
			}
		})
	}
}

func TestSetupHealthMux(t *testing.T) {
	origMeta, origCounters := httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn
	t.Cleanup(func() {
		httpkit.WriteMetaDirFn, httpkit.WriteCountersDirFn = origMeta, origCounters
	})
	httpkit.WriteMetaDirFn = func(string) error { return nil }
	httpkit.WriteCountersDirFn = func(string) error { return nil }

	mux := SetupHealthMux()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
		wantBody   string
	}{
		{
			name:       "GET /health",
			method:     http.MethodGet,
			path:       "/health",
			wantStatus: http.StatusOK,
			wantBody:   `{"status":"ok"}`,
		},
		{
			name:       "GET /healthz",
			method:     http.MethodGet,
			path:       "/healthz",
			wantStatus: http.StatusOK,
			wantBody:   `{"status":"ok"}`,
		},
		{
			name:       "GET /metrics",
			method:     http.MethodGet,
			path:       "/metrics",
			wantStatus: http.StatusOK,
		},
		{
			name:       "POST /debug/coverage/flush",
			method:     http.MethodPost,
			path:       "/debug/coverage/flush",
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, http.NoBody)
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			assert.Equal(t, tc.wantStatus, rec.Code)
			if tc.wantBody != "" {
				assert.JSONEq(t, tc.wantBody, rec.Body.String())
			}
		})
	}
}

func getFreePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	parts := strings.Split(ln.Addr().String(), ":")
	return parts[len(parts)-1]
}

func TestRunApp(t *testing.T) {
	tests := []struct {
		name          string
		cfg           Config
		fakeBus       *testBus
		connectErr    error
		useDefaultBus bool
		cancelEarly   bool
		wantErrMsg    string
	}{
		{
			name: "empty nats url returns error from default bus factory",
			cfg: Config{
				Port:    "0",
				NATSURL: "",
			},
			useDefaultBus: true,
			wantErrMsg:    "nats_url is required",
		},
		{
			name: "invalid health port causes listen error",
			cfg: Config{
				Port:    "invalid-port",
				NATSURL: "nats://localhost:4222",
			},
			fakeBus:    &testBus{},
			wantErrMsg: "listening on health port",
		},
		{
			name: "connect to nats failure",
			cfg: Config{
				Port:    "0",
				NATSURL: "nats://invalid:4222",
			},
			connectErr: errors.New("cannot connect"),
			wantErrMsg: "connect to nats",
		},
		{
			name: "start consumers failure",
			cfg: Config{
				Port:    "0",
				NATSURL: "nats://localhost:4222",
			},
			fakeBus:    &testBus{ensureErr: errors.New("streams fail")},
			wantErrMsg: "start consumers",
		},
		{
			name: "clean shutdown on cancel",
			cfg: Config{
				Port:    "0",
				NATSURL: "nats://localhost:4222",
			},
			fakeBus:     &testBus{},
			cancelEarly: true,
			wantErrMsg:  "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			oldBusFn := newBusFn
			defer func() { newBusFn = oldBusFn }()

			if !tc.useDefaultBus {
				newBusFn = func(context.Context, string) (pubsub.Bus, error) {
					if tc.connectErr != nil {
						return nil, tc.connectErr
					}
					if tc.fakeBus != nil {
						return tc.fakeBus, nil
					}
					return &testBus{}, nil
				}
			}

			cfg := tc.cfg
			if cfg.Port == "0" {
				cfg.Port = getFreePort(t)
			}

			ctx, cancel := context.WithCancel(context.Background())
			if tc.cancelEarly {
				cancel()
			} else {
				defer cancel()
			}

			err := RunApp(ctx, cfg)
			if tc.wantErrMsg == "" {
				assert.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			}
		})
	}
}

func TestRunMain(t *testing.T) {
	origLoad, origRun, origExit, origTracing := loadConfigFn, runAppFn, exitFn, initTracingFn
	defer func() {
		loadConfigFn, runAppFn, exitFn, initTracingFn = origLoad, origRun, origExit, origTracing
	}()

	loadConfigFn = func() Config {
		return Config{Port: "4027", NATSURL: "nats://localhost:4222"}
	}
	initTracingFn = func(context.Context, string, string) (tracing.Config, func()) {
		return tracing.Config{}, func() {}
	}

	tests := []struct {
		name       string
		runAppErr  error
		wantErr    bool
		wantErrMsg string
	}{
		{
			name:      "success case",
			runAppErr: nil,
			wantErr:   false,
		},
		{
			name:       "failure case returns error",
			runAppErr:  errors.New("fatal run app error"),
			wantErr:    true,
			wantErrMsg: "fatal run app error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runAppFn = func(context.Context, Config) error {
				return tc.runAppErr
			}

			err := runMain()
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErrMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestMain_Execution(t *testing.T) {
	origLoad, origRun, origExit, origTracing := loadConfigFn, runAppFn, exitFn, initTracingFn
	t.Cleanup(func() {
		loadConfigFn, runAppFn, exitFn, initTracingFn = origLoad, origRun, origExit, origTracing
	})

	loadConfigFn = func() Config {
		return Config{Port: "4027", NATSURL: "nats://localhost:4222"}
	}
	initTracingFn = func(context.Context, string, string) (tracing.Config, func()) {
		return tracing.Config{}, func() {}
	}

	runAppFn = func(context.Context, Config) error {
		return nil
	}
	var exitCalled bool
	exitFn = func(string, ...any) {
		exitCalled = true
	}
	main()
	assert.False(t, exitCalled)

	runAppFn = func(context.Context, Config) error {
		return errors.New("startup error")
	}
	main()
	assert.True(t, exitCalled)
}

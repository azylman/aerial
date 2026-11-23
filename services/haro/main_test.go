package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/haro/internal/config"
	"github.com/azylman/aerial/services/haro/internal/dispatch"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/server"
	"github.com/azylman/aerial/services/haro/internal/store"
	"github.com/azylman/aerial/services/haro/internal/testutil"
	"github.com/azylman/aerial/services/haro/internal/tools"
)

// TestMain_Execution checks main wires config.LoadConfig and its package vars
// into httpkit.RunService; the bootstrap branches are covered by httpkit.
func TestMain_Execution(t *testing.T) {
	oldRun, oldExit, oldPath := runAppFn, exitFn, configPath
	t.Cleanup(installTestTracing)
	t.Cleanup(func() { runAppFn, exitFn, configPath = oldRun, oldExit, oldPath })

	testCfg := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(testCfg, []byte("port: \"4005\"\ntimezone: \"America/Los_Angeles\"\neri_url: \"http://127.0.0.1:59484\"\nvanadis_url: \"http://127.0.0.1:59485\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	configPath = testCfg
	t.Setenv("CONFIG_PATH", testCfg)
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")

	gotPort := ""
	exitCalled := false
	exitFn = func(string, ...any) { exitCalled = true }
	runAppFn = func(_ context.Context, cfg *config.Config) error {
		gotPort = cfg.Port
		return nil
	}
	main()
	if gotPort != "4005" {
		t.Errorf("runAppFn cfg.Port = %q, want %q", gotPort, "4005")
	}
	if exitCalled {
		t.Error("exitFn called on clean run")
	}
}

func TestRunApp_Lifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dsn := pgtest.NewDSN(t)
	port := freeLoopbackPort(t)

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunApp(ctx, &config.Config{
			Port:        port,
			DatabaseURL: dsn,
			EriURL:      "http://127.0.0.1:59484",
			VanadisURL:  "http://127.0.0.1:59485",
		})
	}()

	var resp *http.Response
	for start := time.Now(); time.Since(start) < 3*time.Second; {
		var err error
		if resp, err = http.Get("http://127.0.0.1:" + port + "/health"); err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		<-time.After(20 * time.Millisecond)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("health check failed, status: %v", resp)
	}
	_ = resp.Body.Close()

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("unexpected error from RunApp shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for RunApp shutdown")
	}

	// A cancelled context makes store.OpenDB give up after its first
	// failed attempt instead of retrying an unreachable server.
	cancelledCtx, cancelOpen := context.WithCancel(context.Background())
	cancelOpen()
	failures := []struct {
		name      string
		cancelled bool
		cfg       *config.Config
	}{
		{"unreachable postgres", true, &config.Config{Port: "9999", DatabaseURL: "postgres://invalid:pass@127.0.0.1:59996/db?sslmode=disable", EriURL: "http://127.0.0.1:59484"}},
		{"nil config", false, nil},
		{"empty port", false, &config.Config{Port: "", DatabaseURL: dsn, EriURL: "http://127.0.0.1:59484"}},
		{"listen failure on invalid port", false, &config.Config{Port: "-1", DatabaseURL: pgtest.NewDSN(t), EriURL: "http://127.0.0.1:59484"}},
		{"empty eri url", false, &config.Config{Port: "8080", DatabaseURL: dsn, EriURL: ""}},
	}
	for _, tc := range failures {
		runCtx := context.Background()
		if tc.cancelled {
			runCtx = cancelledCtx
		}
		if err := RunApp(runCtx, tc.cfg); err == nil {
			t.Errorf("%s: expected error running app with %v", tc.name, tc.cfg)
		}
	}
}

// TestRunApp_TickerFailurePropagated also routes NATS through a bus whose
// Close fails, so the shutdown path logs the close error and still returns
// the ticker failure.
func TestRunApp_TickerFailurePropagated(t *testing.T) {
	ctx := t.Context()
	dsn := pgtest.NewDSN(t)
	port := freeLoopbackPort(t)
	oldBus := newBusFn
	t.Cleanup(func() { newBusFn = oldBus })
	newBusFn = func(context.Context, string) (pubsub.Bus, error) {
		return &pubsubtest.FaultBus{Bus: pubsub.NewMemoryBus(), CloseErr: errors.New("close boom")}, nil
	}

	err := RunApp(ctx, &config.Config{
		Port:        port,
		DatabaseURL: dsn,
		EriURL:      "http://127.0.0.1:59484",
		NATSURL:     "nats://fault-bus",
		RestatePort: "invalid-port-name",
	})
	if err == nil {
		t.Fatal("expected RunApp to return error when Restate server cannot start, got nil")
	}
}

// freeLoopbackPort returns a port the kernel just handed out on 127.0.0.1:0;
// RunApp binds ":"+Port itself, so the test cannot pass a listener in.
func freeLoopbackPort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen 127.0.0.1:0: %v", err)
	}
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	_ = ln.Close()
	return port
}

func TestNewBusFn_FastFailure(t *testing.T) {
	if _, err := newBusFn(context.Background(), "://bad-url"); err == nil {
		t.Error("expected error connecting bus to malformed url")
	}
}

func TestHaro_EventsConsumerIntegration(t *testing.T) {
	db := testutil.NewDB(t)
	ctx := context.Background()

	if err := store.InsertEventTrigger(ctx, db, model.EventTrigger{
		ID:        "trig-int-1",
		ChannelID: "100000000000000001",
		Source:    "flux",
		Target:    "aerial",
		EventType: "sync_success",
		Prompt:    "Deploy succeeded",
		Effort:    "low",
	}); err != nil {
		t.Fatalf("InsertEventTrigger failed: %v", err)
	}

	mockDisp := &mockDispatcher{response: dispatch.PromptDispatchResponse{Status: "ok", MessageID: "msg-ext-int"}}
	srv := server.NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, db), mockDisp)

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	wrapped := &pubsubtest.FaultBus{Bus: memBus, Handled: make(chan error, 1)}
	bus := initHaroJetStreamWithFactory(ctx, srv, db, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return wrapped, nil
	})
	if bus == nil {
		t.Fatal("expected non-nil bus")
	}

	ev := pubsub.ExternalEvent{
		Source:        "flux",
		EventType:     "sync_success",
		Targets:       []string{"aerial"},
		ContextHeader: "Deployment update",
	}
	if err := memBus.Publish(ctx, "aerial.events.flux.sync_success", "ev-1", ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	awaitBusHandled(t, wrapped.Handled, "event delivery")

	verifyTriggerState(ctx, t, db, ev, false)
}

func TestHaro_EventsConsumerIntegration_RestateIngress(t *testing.T) {
	db := testutil.NewDB(t)
	ctx := context.Background()

	if err := store.InsertEventTrigger(ctx, db, model.EventTrigger{
		ID:        "trig-restate-1",
		ChannelID: "100000000000000001",
		Source:    "flux",
		Target:    "aerial",
		EventType: "sync_success",
		Prompt:    "Deploy succeeded",
		Effort:    "low",
	}); err != nil {
		t.Fatalf("InsertEventTrigger failed: %v", err)
	}

	var capturedPath string
	var capturedKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedKey = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	mockDisp := &mockDispatcher{response: dispatch.PromptDispatchResponse{Status: "ok", MessageID: "msg-ext-int"}}
	s := server.NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, db), mockDisp)

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	wrapped := &pubsubtest.FaultBus{Bus: memBus, Handled: make(chan error, 1)}
	bus := initHaroJetStreamWithFactory(ctx, s, db, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return wrapped, nil
	}, srv.URL)
	if bus == nil {
		t.Fatal("expected non-nil bus")
	}

	ev := pubsub.ExternalEvent{
		EventID:       "ev-restate-1",
		Source:        "flux",
		EventType:     "sync_success",
		Targets:       []string{"aerial"},
		ContextHeader: "Deployment update",
	}
	if err := memBus.Publish(ctx, "aerial.events.flux.sync_success", "ev-1", ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	awaitBusHandled(t, wrapped.Handled, "event delivery")

	wantPath := "/HaroEventWatch/trig-restate-1/Fire"
	if capturedPath != wantPath {
		t.Errorf("capturedPath = %q, want %q", capturedPath, wantPath)
	}
	wantKey := "haro:event:trig-restate-1:ev-restate-1"
	if capturedKey != wantKey {
		t.Errorf("capturedKey = %q, want %q", capturedKey, wantKey)
	}
}

// awaitBusHandled waits for one handler result reported by a pubsubtest.FaultBus
// and fails the test when the handler errored or never ran.
func awaitBusHandled(t *testing.T, handled <-chan error, what string) {
	t.Helper()
	select {
	case err := <-handled:
		if err != nil {
			t.Fatalf("%s handler error = %v", what, err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func verifyTriggerState(ctx context.Context, t *testing.T, db *sql.DB, ev pubsub.ExternalEvent, wantInDB bool) {
	t.Helper()
	placeholders, args, ok := store.BuildEventTriggerFilterArgs(ev.Source, ev.Targets, ev.EventType)
	if !ok {
		t.Fatal("invalid event trigger filter args")
	}
	rawQuery := "SELECT EXISTS(SELECT 1 FROM event_triggers WHERE source = $1 AND event_type = $2 AND target IN (" + placeholders + "))"
	var matched bool
	if err := db.QueryRowContext(ctx, rawQuery, args...).Scan(&matched); err != nil {
		t.Fatalf("query event_triggers failed: %v", err)
	}
	if matched != wantInDB {
		t.Errorf("trigger in db = %v, want %v", matched, wantInDB)
	}
}

func TestHaro_MemoryBusIntegration(t *testing.T) {
	t.Parallel()
	db := testutil.NewDB(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC)

	hb := model.HeartbeatSchedule{
		ID:          "hb-bus-1",
		TargetID:    "1234567890",
		TitlePrefix: "Bus HB",
		CronExpr:    "*/5 * * * *",
		Prompt:      "Bus prompt",
		Timezone:    "UTC",
		NextRunAt:   now,
		Enabled:     true,
	}
	if err := store.InsertHeartbeatSchedule(ctx, db, hb); err != nil {
		t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
	}

	run := model.ScheduleRun{
		ID:           "run-bus-1",
		MessageID:    "heartbeat:hb-bus-1:2026-11-22T10:00:00Z",
		ScheduleID:   "hb-bus-1",
		ScheduleType: "heartbeat",
		TargetID:     "1234567890",
		ThreadID:     "thread-hb-bus-1",
		Title:        "Bus Title",
		Prompt:       "Bus Prompt",
		Status:       "running",
		StartedAt:    now,
	}
	if err := store.CreateScheduleRun(ctx, db, run); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	wrapped := &pubsubtest.FaultBus{Bus: memBus, Handled: make(chan error, 1)}
	bus := initHaroJetStreamWithFactory(ctx, nil, db, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return wrapped, nil
	})
	if bus == nil {
		t.Fatal("expected non-nil bus from initHaroJetStreamWithFactory")
	}

	ev := pubsub.DispatchOutcomeEvent{
		DispatchID:  "heartbeat:hb-bus-1:2026-11-22T10:00:00Z",
		Status:      "completed",
		StartedAt:   now,
		CompletedAt: now.Add(3 * time.Second),
		DurationMs:  3000,
	}
	if err := memBus.Publish(ctx, "aerial.dispatch.completed", "run-bus-1", ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	awaitBusHandled(t, wrapped.Handled, "message delivery")

	updatedRun, err := store.GetScheduleRunByDispatchID(ctx, db, "heartbeat:hb-bus-1:2026-11-22T10:00:00Z")
	if err != nil {
		t.Fatalf("GetScheduleRunByDispatchID failed: %v", err)
	}
	if updatedRun.Status != "completed" {
		t.Errorf("updatedRun.Status = %q, want completed", updatedRun.Status)
	}

	updatedHB, err := store.GetHeartbeatSchedule(ctx, db, "hb-bus-1")
	if err != nil {
		t.Fatalf("GetHeartbeatSchedule failed: %v", err)
	}
	if updatedHB.LastCompletedAt == nil || !updatedHB.LastCompletedAt.Equal(now) {
		t.Errorf("updatedHB.LastCompletedAt = %v, want %v", updatedHB.LastCompletedAt, now)
	}

	// Verify haro-oneshot consumer consumes from SubjectHaroOneShotJob
	oneShotJob := pubsub.HaroOneShotJobArgs{
		ID:       "os-bus-integration",
		ThreadID: "th-bus-oneshot",
		Prompt:   "bus oneshot prompt",
		Effort:   "high",
		RunAt:    now.Add(10 * time.Minute),
	}
	if err = memBus.Publish(ctx, pubsub.SubjectHaroOneShotJob, "os-bus-integration", oneShotJob); err != nil {
		t.Fatalf("Publish one-shot: %v", err)
	}
	awaitBusHandled(t, wrapped.Handled, "one-shot delivery")
	osSched, err := store.GetOneShotSchedule(ctx, db, "os-bus-integration")
	if err != nil {
		t.Fatalf("GetOneShotSchedule failed: %v", err)
	}
	if osSched.Prompt != "bus oneshot prompt" || osSched.Effort != "high" {
		t.Errorf("unexpected oneshot schedule: %+v", osSched)
	}
}

func TestInitHaroJetStreamWithFactory(t *testing.T) {
	t.Parallel()
	errBoom := errors.New("boom")
	tests := []struct {
		name                                    string
		natsURL                                 string
		factoryErr, ensureErr, queueErr, subErr error
		withServer                              bool
		wantNilBus                              bool
	}{
		{name: "empty nats url", natsURL: "  ", wantNilBus: true},
		{name: "factory error", natsURL: "nats://mock", factoryErr: errBoom, wantNilBus: true},
		{name: "ensure streams error", natsURL: "nats://mock", ensureErr: errBoom},
		{name: "queue subscribe error", natsURL: "nats://mock", queueErr: errBoom},
		{name: "events subscribe error", natsURL: "nats://mock", subErr: errBoom, withServer: true},
		{name: "success with server", natsURL: "nats://mock", withServer: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := testutil.NewDB(t)
			memBus := pubsub.NewMemoryBus()
			t.Cleanup(func() { _ = memBus.Close() })
			fb := &pubsubtest.FaultBus{Bus: memBus, EnsureErr: tc.ensureErr, ConsumeErr: tc.queueErr, SubscribeErr: tc.subErr}
			var srv *server.Server
			if tc.withServer {
				srv = server.NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, db))
			}
			factory := func(context.Context, string) (pubsub.Bus, error) {
				if tc.factoryErr != nil {
					return nil, tc.factoryErr
				}
				return fb, nil
			}
			bus := initHaroJetStreamWithFactory(t.Context(), srv, db, tc.natsURL, factory)
			if gotNil := bus == nil; gotNil != tc.wantNilBus {
				t.Errorf("bus == nil is %v, want %v", gotNil, tc.wantNilBus)
			}
		})
	}
}

func TestHaro_InitJetStreamEdgeCases(t *testing.T) {
	memBus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = memBus.Close() })
	closedDB := testutil.NewClosedDB(t)
	serverClosed := server.NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, closedDB))
	captureBus := &handlerCaptureBus{Bus: memBus}
	if bus := initHaroJetStreamWithFactory(t.Context(), serverClosed, nil, "nats://mock", func(context.Context, string) (pubsub.Bus, error) {
		return captureBus, nil
	}); bus == nil || captureBus.captured == nil {
		t.Fatal("expected captured subscribe handler")
	}
	evBytes, err := json.Marshal(pubsub.ExternalEvent{EventID: "ev-retry-test", Source: "camera", EventType: "motion", Targets: []string{"driveway"}})
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}

	tests := []struct {
		name         string
		data         []byte
		numDelivered uint64
		wantErr      string
	}{
		{"db failure below delivery cap naks", evBytes, 1, "sql: database is closed"},
		{"db failure at delivery cap drops", evBytes, 5, ""},
		{"bad json drops", []byte("invalid-json"), 1, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := captureBus.captured(t.Context(), &pubsubtest.Message{Payload: tc.data, Delivered: tc.numDelivered})
			gotErr := ""
			if err != nil {
				gotErr = err.Error()
			}
			if gotErr != tc.wantErr {
				t.Errorf("handler error = %q, want %q", gotErr, tc.wantErr)
			}
		})
	}
}

type handlerCaptureBus struct {
	pubsub.Bus

	captured pubsub.Handler
}

func (c *handlerCaptureBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	c.captured = handler
	return c.Bus.Subscribe(ctx, subject, durable, handler)
}

type mockDispatcher struct {
	response dispatch.PromptDispatchResponse
}

func (m *mockDispatcher) DispatchPrompt(context.Context, dispatch.PromptDispatchRequest) (dispatch.PromptDispatchResponse, error) {
	return m.response, nil
}

func (*mockDispatcher) Dispatch(context.Context, eriwire.BrainDispatchRequest) (eriwire.DispatchEnqueueResponse, error) {
	return eriwire.DispatchEnqueueResponse{}, nil
}

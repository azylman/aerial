package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/haro/internal/config"
	"github.com/azylman/aerial/services/haro/internal/dispatch"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/server"
	"github.com/azylman/aerial/services/haro/internal/store"
	"github.com/azylman/aerial/services/haro/internal/testutil"
	"github.com/azylman/aerial/services/haro/internal/tools"
)

func TestMain_Execution(t *testing.T) {
	oldRun, oldExit, oldPath := runAppFn, exitFn, configPath
	t.Cleanup(installTestTracing)
	defer func() {
		runAppFn, exitFn, configPath = oldRun, oldExit, oldPath
	}()

	tmpDir := t.TempDir()
	testCfg := filepath.Join(tmpDir, "config.yaml")
	_ = os.WriteFile(testCfg, []byte("port: \"4005\"\ntimezone: \"America/Los_Angeles\"\neri_url: \"http://127.0.0.1:59484\"\nvanadis_url: \"http://127.0.0.1:59485\"\n"), 0o644)
	configPath = testCfg
	t.Setenv("CONFIG_PATH", testCfg)
	t.Setenv("DATABASE_URL", "postgres://user:pass@localhost:5432/db")

	runCalled := false
	runAppFn = func(ctx context.Context, cfg *config.Config) error {
		runCalled = true
		return nil
	}
	main()
	if !runCalled {
		t.Error("expected runAppFn called")
	}

	exitCalled := false
	exitFn = func(string, ...any) { exitCalled = true }
	runAppFn = func(ctx context.Context, cfg *config.Config) error { return http.ErrServerClosed }
	main()
	if exitCalled {
		t.Error("did not expect exitFn on http.ErrServerClosed")
	}

	runAppFn = func(ctx context.Context, cfg *config.Config) error { return context.DeadlineExceeded }
	main()
	if !exitCalled {
		t.Error("expected exitFn on server error")
	}

	exitCalled = false
	configPath = filepath.Join(tmpDir, "nonexistent.yaml")
	t.Setenv("CONFIG_PATH", configPath)
	main()
	if !exitCalled {
		t.Error("expected exitFn on config load error")
	}
}

func TestRunApp_Lifecycle(t *testing.T) {
	origOpenDBFn := openDBFn
	openDBFn = func(ctx context.Context, cfg *config.Config) (*sql.DB, error) {
		if cfg == nil {
			return nil, context.DeadlineExceeded
		}
		return testutil.OpenSQLiteTestDB(cfg.DatabaseURL) //nolint:contextcheck // test sqlite opener takes no ctx (frozen testutil seam); wrapper must match the openDBFn signature.
	}
	defer func() { openDBFn = origOpenDBFn }()

	ctx, cancel := context.WithCancel(context.Background())
	tempDB := filepath.Join(t.TempDir(), "runapp.db")
	port := freeLoopbackPort(t)

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunApp(ctx, &config.Config{
			Port:        port,
			DatabaseURL: tempDB,
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

	realOpenDBFn := openDBFn
	openDBFn = func(ctx context.Context, cfg *config.Config) (*sql.DB, error) {
		if cfg != nil && strings.HasPrefix(strings.TrimSpace(cfg.DatabaseURL), "postgres://") {
			return nil, context.DeadlineExceeded
		}
		return realOpenDBFn(ctx, cfg)
	}
	defer func() { openDBFn = origOpenDBFn }()

	for _, cfg := range []*config.Config{
		{Port: "9999", DatabaseURL: "postgres://invalid:pass@127.0.0.1:59996/db?sslmode=disable", EriURL: "http://127.0.0.1:59484"},
		nil,
		{Port: "", DatabaseURL: ":memory:", EriURL: "http://127.0.0.1:59484"},
		{Port: "-1", DatabaseURL: ":memory:", EriURL: "http://127.0.0.1:59484"},
		{Port: "8080", DatabaseURL: ":memory:", EriURL: ""},
	} {
		if err := RunApp(context.Background(), cfg); err == nil {
			t.Errorf("expected error running app with %v", cfg)
		}
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
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	_ = ln.Close()
	return port
}

func TestNewBusFn_FastFailure(t *testing.T) {
	if _, err := newBusFn(context.Background(), "://bad-url"); err == nil {
		t.Error("expected error connecting bus to malformed url")
	}
}

func TestHaro_EventsConsumerIntegration(t *testing.T) {
	db := setupTestDB(t.Context(), t)
	ctx := context.Background()

	_ = store.InsertEventTrigger(ctx, db, model.EventTrigger{
		ID:        "trig-int-1",
		ChannelID: "100000000000000001",
		Source:    "flux",
		Target:    "aerial",
		EventType: "sync_success",
		Prompt:    "Deploy succeeded",
		Effort:    "low",
	})

	done := make(chan struct{})
	mockDisp := &mockDispatcher{response: dispatch.PromptDispatchResponse{Status: "ok", MessageID: "msg-ext-int"}}
	srv := server.NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, db), mockDisp)

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	wrapped := &ackSubscribeBus{Bus: memBus, onHandled: func() { close(done) }}
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

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event delivery")
	}

	verifyTriggerState(ctx, t, db, ev, false)
	if wrapped.lastErr != nil {
		t.Errorf("expected nil error on successful event handling, got %v", wrapped.lastErr)
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

type ackSubscribeBus struct {
	pubsub.Bus

	lastErr   error
	onHandled func()
}

func (a *ackSubscribeBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	return a.Bus.Subscribe(ctx, subject, durable, func(mCtx context.Context, msg pubsub.Message) error {
		defer a.onHandled()
		err := handler(mCtx, msg)
		a.lastErr = err
		return err
	})
}

func TestHaro_MemoryBusIntegration(t *testing.T) {
	t.Parallel()
	db := setupTestDB(t.Context(), t)
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

	done := make(chan struct{})
	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	wrapped := &ackBus{Bus: memBus, onHandled: func() { close(done) }}
	bus := initHaroJetStreamWithFactory(ctx, nil, db, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return wrapped, nil
	})
	if bus == nil {
		t.Fatal("expected non-nil bus from initHaroJetStreamWithFactory")
	}

	ev := pubsub.DispatchOutcomeEvent{
		DispatchID:  "run-bus-1",
		Status:      "completed",
		StartedAt:   now,
		CompletedAt: now.Add(3 * time.Second),
		DurationMs:  3000,
	}
	if err := memBus.Publish(ctx, "aerial.dispatch.completed", "run-bus-1", ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message delivery")
	}

	updatedRun, err := store.GetScheduleRun(ctx, db, "run-bus-1")
	if err != nil {
		t.Fatalf("GetScheduleRun failed: %v", err)
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
	oneshotDone := make(chan struct{})
	wrapped.onHandled = func() { close(oneshotDone) }
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
	select {
	case <-oneshotDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for one-shot delivery")
	}
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
			db := setupTestDB(t.Context(), t)
			memBus := pubsub.NewMemoryBus()
			t.Cleanup(func() { _ = memBus.Close() })
			fb := &failBus{Bus: memBus, ensureErr: tc.ensureErr, queueErr: tc.queueErr, subErr: tc.subErr}
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
	closedDB, err := testutil.OpenSQLiteTestDB(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLiteTestDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := server.NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, closedDB))
	captureBus := &handlerCaptureBus{Bus: memBus}
	if bus := initHaroJetStreamWithFactory(t.Context(), serverClosed, nil, "nats://mock", func(context.Context, string) (pubsub.Bus, error) {
		return captureBus, nil
	}); bus == nil || captureBus.captured == nil {
		t.Fatal("expected captured subscribe handler")
	}
	evBytes, _ := json.Marshal(pubsub.ExternalEvent{EventID: "ev-retry-test", Source: "camera", EventType: "motion", Targets: []string{"driveway"}})

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
			err := captureBus.captured(t.Context(), &memoryMsg{data: tc.data, numDelivered: tc.numDelivered})
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

type memoryMsg struct {
	data         []byte
	subject      string
	naked        bool
	numDelivered uint64
}

func (m *memoryMsg) Data() []byte      { return m.data }
func (m *memoryMsg) Subject() string   { return m.subject }
func (*memoryMsg) Header() nats.Header { return nil }
func (m *memoryMsg) NumDelivered() uint64 {
	if m.numDelivered > 0 {
		return m.numDelivered
	}
	return 1
}
func (*memoryMsg) Ack() error                           { return nil }
func (m *memoryMsg) NakWithDelay(_ time.Duration) error { m.naked = true; return nil }
func (*memoryMsg) Term() error                          { return nil }
func (*memoryMsg) Snooze(_ time.Duration) error         { return nil }
func (*memoryMsg) InProgress() error                    { return nil }

type ackBus struct {
	pubsub.Bus

	onHandled func()
}

func (a *ackBus) ConsumeQueue(ctx context.Context, subject, queue string, handler pubsub.Handler) (pubsub.Subscription, error) {
	return a.Bus.ConsumeQueue(ctx, subject, queue, func(mCtx context.Context, msg pubsub.Message) error {
		defer a.onHandled()
		return handler(mCtx, msg)
	})
}

// failBus injects declarative errors into the bus calls initHaroJetStreamWithFactory makes.
type failBus struct {
	pubsub.Bus

	ensureErr, queueErr, subErr error
}

func (f *failBus) EnsureCoreStreams(ctx context.Context) error {
	if f.ensureErr != nil {
		return f.ensureErr
	}
	return f.Bus.EnsureCoreStreams(ctx)
}

func (f *failBus) ConsumeQueue(ctx context.Context, subject, queue string, handler pubsub.Handler) (pubsub.Subscription, error) {
	if f.queueErr != nil {
		return nil, f.queueErr
	}
	return f.Bus.ConsumeQueue(ctx, subject, queue, handler)
}

func (f *failBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	if f.subErr != nil {
		return nil, f.subErr
	}
	return f.Bus.Subscribe(ctx, subject, durable, handler)
}

type handlerCaptureBus struct {
	pubsub.Bus

	captured pubsub.Handler
}

func (c *handlerCaptureBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	c.captured = handler
	return c.Bus.Subscribe(ctx, subject, durable, handler)
}

func setupTestDB(_ context.Context, t *testing.T) *sql.DB {
	t.Helper()
	db, err := testutil.OpenSQLiteTestDB(filepath.Join(t.TempDir(), "haro.db")) //nolint:contextcheck // testutil seam takes no ctx by design
	if err != nil {
		t.Fatalf("setupTestDB OpenSQLiteTestDB failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
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

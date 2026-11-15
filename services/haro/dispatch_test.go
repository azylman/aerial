package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/nats-io/nats.go"
)

type memoryMsg struct {
	data         []byte
	subject      string
	naked        bool
	numDelivered uint64
}

func (m *memoryMsg) Data() []byte        { return m.data }
func (m *memoryMsg) Subject() string     { return m.subject }
func (m *memoryMsg) Header() nats.Header { return nil }
func (m *memoryMsg) NumDelivered() uint64 {
	if m.numDelivered > 0 {
		return m.numDelivered
	}
	return 1
}
func (m *memoryMsg) Ack() error                         { return nil }
func (m *memoryMsg) NakWithDelay(_ time.Duration) error { m.naked = true; return nil }
func (m *memoryMsg) Term() error                        { return nil }
func (m *memoryMsg) Snooze(_ time.Duration) error       { return nil }
func (m *memoryMsg) InProgress() error                  { return nil }

func TestHeartbeatDB_AdvanceLastCompletedAtMonotonic(t *testing.T) {
	t.Parallel()
	db := setupTestDB(t)
	ctx := context.Background()

	hb := HeartbeatSchedule{
		ID:          "hb-mono-1",
		TargetID:    "1234567890",
		TitlePrefix: "Mono Test",
		CronExpr:    "*/5 * * * *",
		Prompt:      "Check something",
		Timezone:    "UTC",
		NextRunAt:   time.Now().UTC(),
		Enabled:     true,
	}
	if err := InsertHeartbeatSchedule(ctx, db, hb); err != nil {
		t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
	}

	// 1. Initial schedule has nil last_completed_at
	loaded, err := GetHeartbeatSchedule(ctx, db, "hb-mono-1")
	if err != nil {
		t.Fatalf("GetHeartbeatSchedule failed: %v", err)
	}
	if loaded.LastCompletedAt != nil {
		t.Fatalf("expected nil LastCompletedAt initially, got %v", loaded.LastCompletedAt)
	}

	// 2. Advance to T1
	t1 := time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC)
	if err := AdvanceHeartbeatLastCompletedAt(ctx, db, "hb-mono-1", t1); err != nil {
		t.Fatalf("AdvanceHeartbeatLastCompletedAt T1 failed: %v", err)
	}
	loaded, err = GetHeartbeatSchedule(ctx, db, "hb-mono-1")
	if err != nil {
		t.Fatalf("GetHeartbeatSchedule after T1 failed: %v", err)
	}
	if loaded.LastCompletedAt == nil || !loaded.LastCompletedAt.Equal(t1) {
		t.Fatalf("expected LastCompletedAt = %v, got %v", t1, loaded.LastCompletedAt)
	}

	// 3. Advance to T2 (later)
	t2 := time.Date(2026, 11, 14, 2, 0, 0, 0, time.UTC)
	if err := AdvanceHeartbeatLastCompletedAt(ctx, db, "hb-mono-1", t2); err != nil {
		t.Fatalf("AdvanceHeartbeatLastCompletedAt T2 failed: %v", err)
	}
	loaded, err = GetHeartbeatSchedule(ctx, db, "hb-mono-1")
	if err != nil {
		t.Fatalf("GetHeartbeatSchedule after T2 failed: %v", err)
	}
	if loaded.LastCompletedAt == nil || !loaded.LastCompletedAt.Equal(t2) {
		t.Fatalf("expected LastCompletedAt = %v, got %v", t2, loaded.LastCompletedAt)
	}

	// 4. Attempt advance to earlier timestamp T0 (monotonic invariant: should stay T2)
	t0 := time.Date(2026, 11, 14, 0, 30, 0, 0, time.UTC)
	if err := AdvanceHeartbeatLastCompletedAt(ctx, db, "hb-mono-1", t0); err != nil {
		t.Fatalf("AdvanceHeartbeatLastCompletedAt T0 failed: %v", err)
	}
	loaded, err = GetHeartbeatSchedule(ctx, db, "hb-mono-1")
	if err != nil {
		t.Fatalf("GetHeartbeatSchedule after T0 failed: %v", err)
	}
	if loaded.LastCompletedAt == nil || !loaded.LastCompletedAt.Equal(t2) {
		t.Fatalf("expected LastCompletedAt to remain %v, got %v", t2, loaded.LastCompletedAt)
	}
}

func outcomeEvent(id, source, status, errStr string, start, end time.Time, dur int64) *pubsub.DispatchOutcomeEvent {
	return &pubsub.DispatchOutcomeEvent{
		DispatchID:  id,
		Source:      source,
		Status:      status,
		Error:       errStr,
		StartedAt:   start,
		CompletedAt: end,
		DurationMs:  dur,
	}
}

func TestHandleDispatchOutcome_Scenarios(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC)

	tests := []struct {
		name                string
		rawPayload          string
		seedScheduleType    string
		seedScheduleID      string
		seedRunID           string
		seedRunStatus       string
		event               *pubsub.DispatchOutcomeEvent
		wantErr             bool
		wantRunStatus       string
		wantRunError        string
		wantWatermarkTime   *time.Time
		wantCheckCalculated bool
		wantNaked           bool
	}{
		{
			name:       "corrupt json returns nil to ack poison pill",
			rawPayload: `{"status": corrupt-json`,
		},
		{
			name:             "healthcheck ping is ignored",
			seedScheduleType: "heartbeat",
			seedScheduleID:   "hb-ping",
			seedRunID:        "healthcheck-ping",
			event:            outcomeEvent("healthcheck-ping", "", "completed", "", time.Time{}, time.Time{}, 0),
			wantRunStatus:    "running",
		},
		{
			name:      "unknown dispatch id naks with delay for retry",
			event:     outcomeEvent("disp-unknown", "", "completed", "", time.Time{}, time.Time{}, 0),
			wantNaked: true,
		},
		{
			name:  "non-heartbeat source like vanadis is ignored without nak",
			event: outcomeEvent("disp-vanadis-chunk", "vanadis", "completed", "", time.Time{}, time.Time{}, 0),
		},
		{
			name:              "heartbeat completed updates run and advances watermark",
			seedScheduleType:  "heartbeat",
			seedScheduleID:    "hb-disp-1",
			seedRunID:         "run-hb-1",
			event:             outcomeEvent("run-hb-1", "", "completed", "", now, now.Add(5*time.Second), 5000),
			wantRunStatus:     "completed",
			wantWatermarkTime: &now,
		},
		{
			name:             "heartbeat failed updates run and does not advance watermark",
			seedScheduleType: "heartbeat",
			seedScheduleID:   "hb-disp-2",
			seedRunID:        "run-hb-2",
			event:            outcomeEvent("run-hb-2", "", "failed", "connection refused", now, now.Add(2*time.Second), 2000),
			wantRunStatus:    "failed",
			wantRunError:     "connection refused",
		},
		{
			name:             "heartbeat skipped_active updates run and does not advance watermark",
			seedScheduleType: "heartbeat",
			seedScheduleID:   "hb-disp-skip",
			seedRunID:        "run-hb-skip",
			event:            outcomeEvent("run-hb-skip", "", "skipped_active", "", now, now, 0),
			wantRunStatus:    "skipped_active",
		},
		{
			name:             "cron completed updates run without touching heartbeat watermark",
			seedScheduleType: "cron",
			seedScheduleID:   "cron-disp-1",
			seedRunID:        "run-cron-1",
			event:            outcomeEvent("run-cron-1", "", "completed", "", now, now.Add(10*time.Second), 10000),
			wantRunStatus:    "completed",
		},
		{
			name:                "heartbeat completed with zero duration and zero completedAt calculates both",
			seedScheduleType:    "heartbeat",
			seedScheduleID:      "hb-disp-zero",
			seedRunID:           "run-hb-zero",
			event:               outcomeEvent("run-hb-zero", "", "completed", "", now, time.Time{}, 0),
			wantRunStatus:       "completed",
			wantWatermarkTime:   &now,
			wantCheckCalculated: true,
		},
		{
			name:             "terminal completed run ignores subsequent outcome event",
			seedScheduleType: "heartbeat",
			seedScheduleID:   "hb-disp-term",
			seedRunID:        "run-hb-term",
			seedRunStatus:    "completed",
			event:            outcomeEvent("run-hb-term", "", "failed", "late failure", time.Time{}, time.Time{}, 0),
			wantRunStatus:    "completed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := setupTestDB(t)
			ctx := context.Background()
			seedDispatchHeartbeat(ctx, t, db, tc.seedScheduleType, tc.seedScheduleID, now)
			seedDispatchRun(ctx, t, db, tc.seedRunID, tc.seedScheduleType, tc.seedScheduleID, tc.seedRunStatus, now)

			msg := &memoryMsg{data: dispatchPayload(t, tc.rawPayload, tc.event), subject: "aerial.dispatch.completed"}
			err := HandleDispatchOutcome(ctx, db, msg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleDispatchOutcome() error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantNaked != msg.naked {
				t.Errorf("msg.naked = %v, want %v", msg.naked, tc.wantNaked)
			}

			assertRunStatus(ctx, t, db, tc.seedRunID, tc.wantRunStatus, tc.wantRunError, tc.wantCheckCalculated)
			assertHeartbeatWatermark(ctx, t, db, tc.seedScheduleType, tc.seedScheduleID, tc.wantWatermarkTime)
		})
	}
}

// seedDispatchHeartbeat inserts a heartbeat schedule when the row seeds one; it is a no-op for other schedule types.
func seedDispatchHeartbeat(ctx context.Context, t *testing.T, db *sql.DB, scheduleType, scheduleID string, now time.Time) {
	t.Helper()
	if scheduleID == "" || scheduleType != "heartbeat" {
		return
	}
	hb := HeartbeatSchedule{
		ID:          scheduleID,
		TargetID:    "1234567890",
		TitlePrefix: "HB Test",
		CronExpr:    "*/5 * * * *",
		Prompt:      "prompt",
		Timezone:    "UTC",
		NextRunAt:   now,
		Enabled:     true,
	}
	if err := InsertHeartbeatSchedule(ctx, db, hb); err != nil {
		t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
	}
}

// seedDispatchRun inserts a schedule run with the given status (defaulting to "running") when runID is set.
func seedDispatchRun(ctx context.Context, t *testing.T, db *sql.DB, runID, scheduleType, scheduleID, runStatus string, now time.Time) {
	t.Helper()
	if runID == "" {
		return
	}
	if runStatus == "" {
		runStatus = "running"
	}
	run := ScheduleRun{
		ID:           runID,
		ScheduleID:   scheduleID,
		ScheduleType: scheduleType,
		TargetID:     "1234567890",
		ThreadID:     "thread-" + scheduleID,
		Title:        "Run Title",
		Prompt:       "Run Prompt",
		Status:       runStatus,
		StartedAt:    now,
	}
	if err := CreateScheduleRun(ctx, db, run); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}
}

// dispatchPayload returns the raw payload verbatim when set, otherwise the JSON-encoded event (nil when neither is set).
func dispatchPayload(t *testing.T, raw string, event *pubsub.DispatchOutcomeEvent) []byte {
	t.Helper()
	if raw != "" {
		return []byte(raw)
	}
	if event == nil {
		return nil
	}
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Marshal event: %v", err)
	}
	return payload
}

// assertRunStatus checks the stored run's status, error, and derived timings; it is a no-op when runID or wantStatus is empty.
func assertRunStatus(ctx context.Context, t *testing.T, db *sql.DB, runID, wantStatus, wantError string, wantTimingsDerived bool) {
	t.Helper()
	if runID == "" || wantStatus == "" {
		return
	}
	run, err := GetScheduleRun(ctx, db, runID)
	if err != nil {
		t.Fatalf("GetScheduleRun failed: %v", err)
	}
	if run.Status != wantStatus {
		t.Errorf("run.Status = %q, want %q", run.Status, wantStatus)
	}
	if wantError != "" && run.Error != wantError {
		t.Errorf("run.Error = %q, want %q", run.Error, wantError)
	}
	if !wantTimingsDerived {
		return
	}
	if run.CompletedAt == nil || run.CompletedAt.IsZero() {
		t.Errorf("expected non-zero run.CompletedAt")
	}
	if run.DurationMs <= 0 {
		t.Errorf("expected run.DurationMs > 0, got %d", run.DurationMs)
	}
}

// assertHeartbeatWatermark checks a seeded heartbeat's LastCompletedAt equals want (nil means it must stay unset);
// it is a no-op when the row seeded no heartbeat schedule.
func assertHeartbeatWatermark(ctx context.Context, t *testing.T, db *sql.DB, scheduleType, scheduleID string, want *time.Time) {
	t.Helper()
	if scheduleID == "" || scheduleType != "heartbeat" {
		return
	}
	hb, err := GetHeartbeatSchedule(ctx, db, scheduleID)
	if err != nil {
		t.Fatalf("GetHeartbeatSchedule failed: %v", err)
	}
	switch {
	case want == nil && hb.LastCompletedAt != nil:
		t.Errorf("expected hb.LastCompletedAt = nil, got %v", hb.LastCompletedAt)
	case want != nil && (hb.LastCompletedAt == nil || !hb.LastCompletedAt.Equal(*want)):
		t.Errorf("hb.LastCompletedAt = %v, want %v", hb.LastCompletedAt, *want)
	}
}

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

func TestHaro_MemoryBusIntegration(t *testing.T) {
	t.Parallel()
	db := setupTestDB(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC)

	hb := HeartbeatSchedule{
		ID:          "hb-bus-1",
		TargetID:    "1234567890",
		TitlePrefix: "Bus HB",
		CronExpr:    "*/5 * * * *",
		Prompt:      "Bus prompt",
		Timezone:    "UTC",
		NextRunAt:   now,
		Enabled:     true,
	}
	if err := InsertHeartbeatSchedule(ctx, db, hb); err != nil {
		t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
	}

	run := ScheduleRun{
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
	if err := CreateScheduleRun(ctx, db, run); err != nil {
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

	updatedRun, err := GetScheduleRun(ctx, db, "run-bus-1")
	if err != nil {
		t.Fatalf("GetScheduleRun failed: %v", err)
	}
	if updatedRun.Status != "completed" {
		t.Errorf("updatedRun.Status = %q, want completed", updatedRun.Status)
	}

	updatedHB, err := GetHeartbeatSchedule(ctx, db, "hb-bus-1")
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
	if err := memBus.Publish(ctx, pubsub.SubjectHaroOneShotJob, "os-bus-integration", oneShotJob); err != nil {
		t.Fatalf("Publish one-shot: %v", err)
	}
	select {
	case <-oneshotDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for one-shot delivery")
	}
	osSched, err := GetOneShotSchedule(ctx, db, "os-bus-integration")
	if err != nil {
		t.Fatalf("GetOneShotSchedule failed: %v", err)
	}
	if osSched.Prompt != "bus oneshot prompt" || osSched.Effort != "high" {
		t.Errorf("unexpected oneshot schedule: %+v", osSched)
	}
}

func TestHandleDispatchOutcome_EdgeCasesAndCoverage(t *testing.T) {
	t.Parallel()
	db := setupTestDB(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 14, 2, 0, 0, 0, time.UTC)

	// 1. HandleDispatchOutcome database == nil or msg == nil
	if err := HandleDispatchOutcome(ctx, nil, &memoryMsg{}); err == nil {
		t.Error("expected error for nil database")
	}
	if err := HandleDispatchOutcome(ctx, db, nil); err == nil {
		t.Error("expected error for nil msg")
	}

	// 2. Database error during GetScheduleRun returns error
	closedDB := setupTestDB(t)
	closedDB.Close()
	failEvBytes, _ := json.Marshal(pubsub.DispatchOutcomeEvent{DispatchID: "run-on-closed-db", Status: "completed"})
	if err := HandleDispatchOutcome(ctx, closedDB, &memoryMsg{data: failEvBytes}); err == nil {
		t.Error("expected error from HandleDispatchOutcome on closed DB")
	}

	// 3. Empty dispatch ID returns nil
	emptyEvent := pubsub.DispatchOutcomeEvent{DispatchID: "   "}
	emptyBytes, _ := json.Marshal(emptyEvent)
	if err := HandleDispatchOutcome(ctx, db, &memoryMsg{data: emptyBytes}); err != nil {
		t.Errorf("expected nil for empty dispatch id, got %v", err)
	}

	// 4. Fallback durationMs and startedAt from run.StartedAt when ev.StartedAt is zero
	hb := HeartbeatSchedule{
		ID:          "hb-fallback-1",
		TargetID:    "1234567890",
		TitlePrefix: "Fallback HB",
		CronExpr:    "*/5 * * * *",
		Prompt:      "Fallback prompt",
		Timezone:    "UTC",
		NextRunAt:   now,
		Enabled:     true,
	}
	if err := InsertHeartbeatSchedule(ctx, db, hb); err != nil {
		t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
	}

	run := ScheduleRun{
		ID:           "run-fallback-1",
		ScheduleID:   "hb-fallback-1",
		ScheduleType: "heartbeat",
		TargetID:     "1234567890",
		ThreadID:     "thread-hb-fb",
		Title:        "FB Title",
		Prompt:       "FB Prompt",
		Status:       "running",
		StartedAt:    now,
	}
	if err := CreateScheduleRun(ctx, db, run); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}

	evZeroStart := pubsub.DispatchOutcomeEvent{
		DispatchID:  "run-fallback-1",
		Status:      "completed",
		CompletedAt: now.Add(4 * time.Second),
		DurationMs:  0, // tests duration fallback to run.StartedAt
	}
	evBytes, _ := json.Marshal(evZeroStart)
	if err := HandleDispatchOutcome(ctx, db, &memoryMsg{data: evBytes}); err != nil {
		t.Fatalf("HandleDispatchOutcome fallback failed: %v", err)
	}

	updatedRun, err := GetScheduleRun(ctx, db, "run-fallback-1")
	if err != nil {
		t.Fatalf("GetScheduleRun failed: %v", err)
	}
	if updatedRun.DurationMs != 4000 {
		t.Errorf("expected DurationMs = 4000, got %d", updatedRun.DurationMs)
	}
	updatedHB, err := GetHeartbeatSchedule(ctx, db, "hb-fallback-1")
	if err != nil {
		t.Fatalf("GetHeartbeatSchedule failed: %v", err)
	}
	if updatedHB.LastCompletedAt == nil || !updatedHB.LastCompletedAt.Equal(now) {
		t.Errorf("expected LastCompletedAt = %v, got %v", now, updatedHB.LastCompletedAt)
	}
}

func TestHaroDB_RunAndWatermarkValidationGaps(t *testing.T) {
	t.Parallel()
	db := setupTestDB(t)
	ctx := context.Background()

	// AdvanceHeartbeatLastCompletedAt error paths
	if err := AdvanceHeartbeatLastCompletedAt(ctx, nil, "hb-1", time.Now()); err == nil {
		t.Error("expected error for nil db")
	}
	if err := AdvanceHeartbeatLastCompletedAt(ctx, db, "", time.Now()); err == nil {
		t.Error("expected error for empty id")
	}

	// GetScheduleRun error paths
	if _, err := GetScheduleRun(ctx, nil, "r1"); err == nil {
		t.Error("expected error for nil db")
	}
	if _, err := GetScheduleRun(ctx, db, ""); err == nil {
		t.Error("expected error for empty id")
	}
	if _, err := GetScheduleRun(ctx, db, "non-existent-run"); err == nil {
		t.Error("expected error for non-existent run")
	}

	// initHaroJetStream empty and mock factory scenarios
	if bus := initHaroJetStream(ctx, nil, db, ""); bus != nil {
		t.Error("expected nil bus for empty natsURL")
	}
	errFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return nil, errors.New("connection failed")
	}
	if bus := initHaroJetStreamWithFactory(ctx, nil, db, "nats://mock", errFactory); bus != nil {
		t.Error("expected nil bus when factory returns error")
	}

	// EnsureCoreStreams failure branch in initHaroJetStreamWithFactory
	errStreamBus := &failEnsureBus{Bus: pubsub.NewMemoryBus()}
	defer errStreamBus.Close()
	if bus := initHaroJetStreamWithFactory(ctx, nil, db, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return errStreamBus, nil
	}); bus == nil {
		t.Error("expected non-nil bus despite EnsureCoreStreams error")
	}

	// ConsumeQueue failure branch in initHaroJetStreamWithFactory
	errQueueBus := &failQueueBus{Bus: pubsub.NewMemoryBus()}
	defer errQueueBus.Close()
	if bus := initHaroJetStreamWithFactory(ctx, nil, db, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return errQueueBus, nil
	}); bus == nil {
		t.Error("expected non-nil bus despite ConsumeQueue error")
	}

	// Subscribe failure branch for haro-events in initHaroJetStreamWithFactory
	errSubBus := &failSubBus{Bus: pubsub.NewMemoryBus()}
	defer errSubBus.Close()
	dummyServer := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, db))
	if bus := initHaroJetStreamWithFactory(ctx, dummyServer, db, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return errSubBus, nil
	}); bus == nil {
		t.Error("expected non-nil bus despite Subscribe error")
	}

	// configureServerEventBus with dispatcher
	disp := NewHTTPPromptDispatcher("http://localhost:8080")
	serverWithDisp := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, db), disp)
	memBusInit := pubsub.NewMemoryBus()
	defer memBusInit.Close()
	if bus := initHaroJetStreamWithFactory(ctx, serverWithDisp, db, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return memBusInit, nil
	}); bus == nil {
		t.Error("expected non-nil bus")
	}
	if disp.EventBus() == nil {
		t.Error("expected event bus to be configured on dispatcher")
	}
}

type failEnsureBus struct {
	pubsub.Bus
}

func (f *failEnsureBus) EnsureCoreStreams(_ context.Context) error {
	return errors.New("simulated stream creation failure")
}

type failQueueBus struct {
	pubsub.Bus
}

func (f *failQueueBus) ConsumeQueue(_ context.Context, _, _ string, _ pubsub.Handler) (pubsub.Subscription, error) {
	return nil, errors.New("simulated queue subscription failure")
}

type failSubBus struct {
	pubsub.Bus
}

func (f *failSubBus) Subscribe(_ context.Context, _, _ string, _ pubsub.Handler) (pubsub.Subscription, error) {
	return nil, errors.New("simulated subscription failure")
}

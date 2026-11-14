package main

import (
	"context"
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

func TestHandleDispatchOutcome_Scenarios(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC)

	tests := []struct {
		name                string
		rawPayload          string
		seedScheduleType    string
		seedScheduleID      string
		seedRunID           string
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
			wantErr:    false,
		},
		{
			name:             "healthcheck ping is ignored",
			seedScheduleType: "heartbeat",
			seedScheduleID:   "hb-ping",
			seedRunID:        "healthcheck-ping",
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID: "healthcheck-ping",
				Status:     "completed",
			},
			wantErr:           false,
			wantRunStatus:     "running",
			wantWatermarkTime: nil,
		},
		{
			name: "unknown dispatch id naks with delay for retry",
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID: "disp-unknown",
				Status:     "completed",
			},
			wantErr:   false,
			wantNaked: true,
		},
		{
			name: "non-heartbeat source like vanadis is ignored without nak",
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID: "disp-vanadis-chunk",
				Source:     "vanadis",
				Status:     "completed",
			},
			wantErr:   false,
			wantNaked: false,
		},
		{
			name:             "heartbeat completed updates run and advances watermark",
			seedScheduleType: "heartbeat",
			seedScheduleID:   "hb-disp-1",
			seedRunID:        "run-hb-1",
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID:  "run-hb-1",
				Status:      "completed",
				StartedAt:   now,
				CompletedAt: now.Add(5 * time.Second),
				DurationMs:  5000,
			},
			wantErr:           false,
			wantRunStatus:     "completed",
			wantWatermarkTime: &now,
		},
		{
			name:             "heartbeat failed updates run and does not advance watermark",
			seedScheduleType: "heartbeat",
			seedScheduleID:   "hb-disp-2",
			seedRunID:        "run-hb-2",
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID:  "run-hb-2",
				Status:      "failed",
				Error:       "connection refused",
				StartedAt:   now,
				CompletedAt: now.Add(2 * time.Second),
				DurationMs:  2000,
			},
			wantErr:           false,
			wantRunStatus:     "failed",
			wantRunError:      "connection refused",
			wantWatermarkTime: nil,
		},
		{
			name:             "cron completed updates run without touching heartbeat watermark",
			seedScheduleType: "cron",
			seedScheduleID:   "cron-disp-1",
			seedRunID:        "run-cron-1",
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID:  "run-cron-1",
				Status:      "completed",
				StartedAt:   now,
				CompletedAt: now.Add(10 * time.Second),
				DurationMs:  10000,
			},
			wantErr:       false,
			wantRunStatus: "completed",
		},
		{
			name:             "heartbeat completed with zero duration and zero completedAt calculates both",
			seedScheduleType: "heartbeat",
			seedScheduleID:   "hb-disp-zero",
			seedRunID:        "run-hb-zero",
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID:  "run-hb-zero",
				Status:      "completed",
				StartedAt:   now,
				CompletedAt: time.Time{},
				DurationMs:  0,
			},
			wantErr:             false,
			wantRunStatus:       "completed",
			wantWatermarkTime:   &now,
			wantCheckCalculated: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := setupTestDB(t)
			ctx := context.Background()

			if tc.seedScheduleID != "" && tc.seedScheduleType == "heartbeat" {
				hb := HeartbeatSchedule{
					ID:          tc.seedScheduleID,
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

			if tc.seedRunID != "" {
				run := ScheduleRun{
					ID:           tc.seedRunID,
					ScheduleID:   tc.seedScheduleID,
					ScheduleType: tc.seedScheduleType,
					TargetID:     "1234567890",
					ThreadID:     "thread-" + tc.seedScheduleID,
					Title:        "Run Title",
					Prompt:       "Run Prompt",
					Status:       "running",
					StartedAt:    now,
				}
				if err := CreateScheduleRun(ctx, db, run); err != nil {
					t.Fatalf("CreateScheduleRun failed: %v", err)
				}
			}

			var payload []byte
			if tc.rawPayload != "" {
				payload = []byte(tc.rawPayload)
			} else if tc.event != nil {
				var err error
				payload, err = json.Marshal(tc.event)
				if err != nil {
					t.Fatalf("Marshal event: %v", err)
				}
			}

			msg := &memoryMsg{data: payload, subject: "aerial.dispatch.completed"}
			err := HandleDispatchOutcome(ctx, db, msg)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleDispatchOutcome() error = %v, wantErr = %v", err, tc.wantErr)
			}

			if tc.wantNaked != msg.naked {
				t.Errorf("msg.naked = %v, want %v", msg.naked, tc.wantNaked)
			}

			if tc.seedRunID != "" && tc.wantRunStatus != "" {
				run, err := GetScheduleRun(ctx, db, tc.seedRunID)
				if err != nil {
					t.Fatalf("GetScheduleRun failed: %v", err)
				}
				if run.Status != tc.wantRunStatus {
					t.Errorf("run.Status = %q, want %q", run.Status, tc.wantRunStatus)
				}
				if tc.wantRunError != "" && run.Error != tc.wantRunError {
					t.Errorf("run.Error = %q, want %q", run.Error, tc.wantRunError)
				}
				if tc.wantCheckCalculated {
					if run.CompletedAt == nil || run.CompletedAt.IsZero() {
						t.Errorf("expected non-zero run.CompletedAt")
					}
					if run.DurationMs <= 0 {
						t.Errorf("expected run.DurationMs > 0, got %d", run.DurationMs)
					}
				}
			}

			if tc.seedScheduleID != "" && tc.seedScheduleType == "heartbeat" {
				hb, err := GetHeartbeatSchedule(ctx, db, tc.seedScheduleID)
				if err != nil {
					t.Fatalf("GetHeartbeatSchedule failed: %v", err)
				}
				if tc.wantWatermarkTime != nil {
					if hb.LastCompletedAt == nil || !hb.LastCompletedAt.Equal(*tc.wantWatermarkTime) {
						t.Errorf("hb.LastCompletedAt = %v, want %v", hb.LastCompletedAt, *tc.wantWatermarkTime)
					}
				} else {
					if hb.LastCompletedAt != nil {
						t.Errorf("expected hb.LastCompletedAt = nil, got %v", hb.LastCompletedAt)
					}
				}
			}
		})
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

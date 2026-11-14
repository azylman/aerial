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
	data    []byte
	subject string
}

func (m *memoryMsg) Data() []byte                       { return m.data }
func (m *memoryMsg) Subject() string                    { return m.subject }
func (m *memoryMsg) Header() nats.Header                { return nil }
func (m *memoryMsg) NumDelivered() uint64               { return 1 }
func (m *memoryMsg) Ack() error                         { return nil }
func (m *memoryMsg) NakWithDelay(_ time.Duration) error { return nil }
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
		name              string
		rawPayload        string
		seedScheduleType  string
		seedScheduleID    string
		seedRunID         string
		event             *pubsub.DispatchOutcomeEvent
		wantErr           bool
		wantRunStatus     string
		wantRunError      string
		wantWatermarkTime *time.Time
	}{
		{
			name:       "corrupt json returns nil to ack poison pill",
			rawPayload: `{"status": corrupt-json`,
			wantErr:    false,
		},
		{
			name: "healthcheck ping is ignored",
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID: "healthcheck-ping",
				Status:     "completed",
			},
			wantErr: false,
		},
		{
			name: "unknown dispatch id returns nil without error",
			event: &pubsub.DispatchOutcomeEvent{
				DispatchID: "disp-unknown",
				Status:     "completed",
			},
			wantErr: false,
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
			wantErr:           false,
			wantRunStatus:     "completed",
			wantWatermarkTime: &now,
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

	bus := pubsub.NewMemoryBus()
	defer bus.Close()

	if err := pubsub.EnsureCoreStreams(ctx, bus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	done := make(chan struct{})
	_, err := bus.Subscribe(ctx, "aerial.dispatch.*", "haro-dispatch", func(msgCtx context.Context, msg pubsub.Message) error {
		defer close(done)
		return HandleDispatchOutcome(msgCtx, db, msg)
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ev := pubsub.DispatchOutcomeEvent{
		DispatchID:  "run-bus-1",
		Status:      "completed",
		StartedAt:   now,
		CompletedAt: now.Add(3 * time.Second),
		DurationMs:  3000,
	}
	if err := bus.Publish(ctx, "aerial.dispatch.completed", "run-bus-1", ev); err != nil {
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

	// 1. Ticker nil and Start(nil)
	var nilTicker *Ticker
	if err := nilTicker.HandleDispatchOutcome(ctx, &memoryMsg{}); err == nil {
		t.Error("expected error for nil ticker, got nil")
	}
	nilTicker.Start(ctx) // should not panic

	// 2. HandleDispatchOutcome database == nil or msg == nil
	if err := HandleDispatchOutcome(ctx, nil, &memoryMsg{}); err == nil {
		t.Error("expected error for nil database")
	}
	if err := HandleDispatchOutcome(ctx, db, nil); err == nil {
		t.Error("expected error for nil msg")
	}

	// 3. Empty dispatch ID returns nil
	emptyEvent := pubsub.DispatchOutcomeEvent{DispatchID: "   "}
	emptyBytes, _ := json.Marshal(emptyEvent)
	if err := HandleDispatchOutcome(ctx, db, &memoryMsg{data: emptyBytes}); err != nil {
		t.Errorf("expected nil for empty dispatch id, got %v", err)
	}

	// 4. Valid ticker HandleDispatchOutcome delegates properly
	dispatcher := NewHTTPPromptDispatcher("http://localhost:8080")
	ticker := NewTicker(db, dispatcher, 30*time.Second)
	if err := ticker.HandleDispatchOutcome(ctx, &memoryMsg{data: emptyBytes}); err != nil {
		t.Errorf("expected nil from ticker.HandleDispatchOutcome, got %v", err)
	}

	// 5. Fallback durationMs and startedAt from run.StartedAt when ev.StartedAt is zero
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

	// CreateScheduleRun error paths
	if err := CreateScheduleRun(ctx, nil, ScheduleRun{ID: "r1"}); err == nil {
		t.Error("expected error for nil db")
	}
	if err := CreateScheduleRun(ctx, db, ScheduleRun{ID: ""}); err == nil {
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
	if bus := initHaroJetStream(ctx, db, ""); bus != nil {
		t.Error("expected nil bus for empty natsURL")
	}
	errFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return nil, errors.New("connection failed")
	}
	if bus := initHaroJetStreamWithFactory(ctx, db, "nats://mock", errFactory); bus != nil {
		t.Error("expected nil bus when factory returns error")
	}
	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	successFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return memBus, nil
	}
	if bus := initHaroJetStreamWithFactory(ctx, db, "nats://mock", successFactory); bus == nil {
		t.Error("expected non-nil bus when factory succeeds")
	} else {
		// Exercise subscription callback registered in initHaroJetStreamWithFactory
		_ = bus.Publish(ctx, "aerial.dispatch.completed", "noop-ping", pubsub.DispatchOutcomeEvent{DispatchID: "healthcheck-ping"})
	}

	// Exercise default newBusFn error path with invalid URL format
	if _, err := newBusFn(ctx, "://bad-url"); err == nil {
		t.Error("expected error from newBusFn with bad URL")
	}

	// recoverFailedEventTrigger persistent vs non-persistent
	server := NewServer(NewToolHandler(&Config{}, db), nil)
	server.recoverFailedEventTrigger(ctx, EventTrigger{ID: "trig-persist", Persistent: true})
	server.recoverFailedEventTrigger(ctx, EventTrigger{ID: "trig-nonpersist", Persistent: false})

	// deleteByID emptyErrMsg fallback
	if _, err := deleteByID(ctx, db, "DELETE FROM heartbeat_schedules WHERE id = $1", "", "", ""); err == nil {
		t.Error("expected error for empty id in deleteByID")
	}
}

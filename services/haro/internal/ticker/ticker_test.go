package ticker

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/haro/internal/dispatch"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
	"github.com/azylman/aerial/services/haro/internal/testutil"
)

func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m))
}

func openTickerTestDB(t *testing.T) *sql.DB {
	t.Helper()
	return testutil.NewDB(t)
}

// failTriggerSQL returns DDL that makes every op (UPDATE or DELETE) on table
// raise msg, sabotaging only the calling test's private database.
func failTriggerSQL(table, op, msg string) string {
	return fmt.Sprintf(`CREATE FUNCTION fail_%[1]s_%[2]s() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION '%[3]s'; END$$;
CREATE TRIGGER fail_%[1]s_%[2]s BEFORE %[2]s ON %[1]s FOR EACH ROW EXECUTE FUNCTION fail_%[1]s_%[2]s();`, table, op, msg)
}

type fakeDispatcher struct {
	dispatched         []dispatch.PromptDispatchRequest
	response           dispatch.PromptDispatchResponse
	err                error
	dispatchedRequests []eriwire.BrainDispatchRequest
	dispatchResponse   eriwire.DispatchEnqueueResponse
	dispatchErr        error
	echoDispatchID     bool
	onDispatch         func()
}

func (m *fakeDispatcher) DispatchPrompt(_ context.Context, req dispatch.PromptDispatchRequest) (dispatch.PromptDispatchResponse, error) {
	m.dispatched = append(m.dispatched, req)
	if m.onDispatch != nil {
		m.onDispatch()
	}
	return m.response, m.err
}

func (m *fakeDispatcher) Dispatch(_ context.Context, req eriwire.BrainDispatchRequest) (eriwire.DispatchEnqueueResponse, error) {
	m.dispatchedRequests = append(m.dispatchedRequests, req)
	if m.onDispatch != nil {
		m.onDispatch()
	}
	if m.echoDispatchID {
		resp := m.dispatchResponse
		resp.DispatchID = req.ID
		return resp, m.dispatchErr
	}
	return m.dispatchResponse, m.dispatchErr
}

type dispatchOutcomeGuardCase struct {
	name          string
	rawPayload    string
	event         *pubsub.DispatchOutcomeEvent
	nilDatabase   bool
	nilMessage    bool
	closeDatabase bool
	sabotageSQL   string
	numDelivered  uint64
	wantErr       bool
	wantNaked     bool
}

type dispatchOutcomeTestCase struct {
	name                string
	seedScheduleType    string
	seedScheduleID      string
	seedRunID           string
	seedDispatchID      string
	seedRunStatus       string
	orphanRun           bool
	event               *pubsub.DispatchOutcomeEvent
	wantRunStatus       string
	wantRunError        string
	wantDurationMs      int64
	wantWatermarkTime   *time.Time
	wantCheckCalculated bool
}

func seedOutcomeHeartbeat(ctx context.Context, t *testing.T, db *sql.DB, scheduleType, scheduleID string, now time.Time) {
	t.Helper()
	if scheduleID == "" || scheduleType != model.ScheduleTypeHeartbeat {
		return
	}
	hb := model.HeartbeatSchedule{
		ID: scheduleID, TargetID: "1234567890", TitlePrefix: "HB Test",
		CronExpr: "*/5 * * * *", Prompt: "prompt", Timezone: "UTC",
		NextRunAt: now, Enabled: true,
	}
	if err := store.InsertHeartbeatSchedule(ctx, db, hb); err != nil {
		t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
	}
}

func seedOutcomeRun(ctx context.Context, t *testing.T, db *sql.DB, runID, dispatchID, scheduleType, scheduleID, status string, startedAt time.Time) {
	t.Helper()
	if runID == "" {
		return
	}
	if status == "" {
		status = "running"
	}
	run := model.ScheduleRun{
		ID: runID, ScheduleID: scheduleID, ScheduleType: scheduleType, MessageID: cmp.Or(dispatchID, runID),
		TargetID: "1234567890", ThreadID: "thread-" + scheduleID,
		Title: "Run Title", Prompt: "Run Prompt", Status: status, StartedAt: startedAt,
	}
	if err := store.CreateScheduleRun(ctx, db, run); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}
}

func runDispatchOutcomeGuardCase(t *testing.T, tc dispatchOutcomeGuardCase, now time.Time) {
	t.Helper()
	db := openTickerTestDB(t)
	if tc.closeDatabase {
		db.Close()
	}
	ctx := context.Background()

	database := db
	if tc.nilDatabase {
		database = nil
	}
	om := &pubsubtest.Message{Payload: outcomePayload(t, tc.rawPayload, tc.event), Subj: "aerial.dispatch.completed", Delivered: max(tc.numDelivered, 1)}
	var msg pubsub.Message = om
	if tc.nilMessage {
		msg = nil
	}

	if !tc.nilDatabase && !tc.closeDatabase && tc.sabotageSQL != "" {
		seedOutcomeHeartbeat(ctx, t, db, model.ScheduleTypeHeartbeat, "hb-upd-fail", now)
		seedOutcomeRun(ctx, t, db, "run-upd-fail", "", model.ScheduleTypeHeartbeat, "hb-upd-fail", "running", now)
		if _, err := db.ExecContext(ctx, tc.sabotageSQL); err != nil {
			t.Fatalf("sabotage SQL failed: %v", err)
		}
	}

	err := HandleDispatchOutcome(ctx, database, msg)
	if (err != nil) != tc.wantErr {
		t.Fatalf("HandleDispatchOutcome() error = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.nilDatabase || tc.nilMessage || tc.closeDatabase {
		return
	}
	if _, got := om.NakDelay(); got != tc.wantNaked {
		t.Errorf("msg naked = %v, want %v (calls %v)", got, tc.wantNaked, om.Calls())
	}
}

func runDispatchOutcomeCase(t *testing.T, tc dispatchOutcomeTestCase, now time.Time) {
	t.Helper()
	db := openTickerTestDB(t)
	ctx := context.Background()

	om := &pubsubtest.Message{Payload: outcomePayload(t, "", tc.event), Subj: "aerial.dispatch.completed", Delivered: 1}

	heartbeatID := tc.seedScheduleID
	if tc.orphanRun {
		heartbeatID = ""
	}
	seedOutcomeHeartbeat(ctx, t, db, tc.seedScheduleType, heartbeatID, now)
	seedOutcomeRun(ctx, t, db, tc.seedRunID, tc.seedDispatchID, tc.seedScheduleType, tc.seedScheduleID, tc.seedRunStatus, now)

	err := HandleDispatchOutcome(ctx, db, om)
	if err != nil {
		t.Fatalf("HandleDispatchOutcome() error = %v", err)
	}
	if tc.name == "dispatch id equal only to run row id is not resolved and naks" {
		if _, got := om.NakDelay(); !got {
			t.Errorf("msg naked = %v, want true", got)
		}
	}

	assertOutcomeRun(ctx, t, db, tc)
	assertOutcomeWatermark(ctx, t, db, tc)
}

func outcomePayload(t *testing.T, raw string, event *pubsub.DispatchOutcomeEvent) []byte {
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

func assertOutcomeRun(ctx context.Context, t *testing.T, db *sql.DB, tc dispatchOutcomeTestCase) {
	t.Helper()
	if tc.seedRunID == "" || tc.wantRunStatus == "" {
		return
	}
	run, err := store.GetScheduleRunByDispatchID(ctx, db, cmp.Or(tc.seedDispatchID, tc.seedRunID))
	if err != nil {
		t.Fatalf("GetScheduleRunByDispatchID failed: %v", err)
	}
	if run.Status != tc.wantRunStatus {
		t.Errorf("run.Status = %q, want %q", run.Status, tc.wantRunStatus)
	}
	if tc.wantRunError != "" && run.Error != tc.wantRunError {
		t.Errorf("run.Error = %q, want %q", run.Error, tc.wantRunError)
	}
	if tc.wantDurationMs >= 0 && run.DurationMs != tc.wantDurationMs {
		t.Errorf("run.DurationMs = %d, want %d", run.DurationMs, tc.wantDurationMs)
	}
	if tc.wantCheckCalculated && (run.CompletedAt == nil || run.CompletedAt.IsZero() || run.DurationMs <= 0) {
		t.Errorf("expected derived completedAt and positive duration, got %+v", run)
	}
}

func assertOutcomeWatermark(ctx context.Context, t *testing.T, db *sql.DB, tc dispatchOutcomeTestCase) {
	t.Helper()
	if tc.orphanRun || tc.seedScheduleID == "" || tc.seedScheduleType != model.ScheduleTypeHeartbeat {
		return
	}
	hb, err := store.GetHeartbeatSchedule(ctx, db, tc.seedScheduleID)
	if err != nil {
		t.Fatalf("GetHeartbeatSchedule failed: %v", err)
	}
	switch {
	case tc.wantWatermarkTime == nil && hb.LastCompletedAt != nil:
		t.Errorf("expected hb.LastCompletedAt = nil, got %v", hb.LastCompletedAt)
	case tc.wantWatermarkTime != nil && (hb.LastCompletedAt == nil || !hb.LastCompletedAt.Equal(*tc.wantWatermarkTime)):
		t.Errorf("hb.LastCompletedAt = %v, want %v", hb.LastCompletedAt, tc.wantWatermarkTime)
	}
}

func TestHandleDispatchOutcome_GuardsAndNak(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC)

	tests := []dispatchOutcomeGuardCase{
		{name: "corrupt json returns nil to ack poison pill", rawPayload: `{"status": corrupt-json`},
		{name: "unknown dispatch id naks with delay for retry", event: &pubsub.DispatchOutcomeEvent{DispatchID: "disp-unknown", Status: "completed"}, wantNaked: true},
		{
			name:         "unknown dispatch id delivered five times is dropped without nak",
			event:        &pubsub.DispatchOutcomeEvent{DispatchID: "disp-unknown", Status: "completed"},
			numDelivered: 5,
		},
		{
			name:  "non-heartbeat source like vanadis is ignored without nak",
			event: &pubsub.DispatchOutcomeEvent{DispatchID: "disp-vanadis-chunk", Source: "vanadis", Status: "completed"},
		},
		{name: "nil database returns error", nilDatabase: true, wantErr: true},
		{name: "nil message returns error", nilMessage: true, wantErr: true},
		{
			name:          "closed database returns query error",
			closeDatabase: true,
			event:         &pubsub.DispatchOutcomeEvent{DispatchID: "run-on-closed-db", Status: "completed"},
			wantErr:       true,
		},
		{
			name:        "outcome update failure returns error",
			event:       &pubsub.DispatchOutcomeEvent{DispatchID: "run-upd-fail", Status: "completed", StartedAt: now, CompletedAt: now.Add(time.Second)},
			sabotageSQL: failTriggerSQL("schedule_runs", "UPDATE", "forced update failure"),
			wantErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDispatchOutcomeGuardCase(t, tc, now)
		})
	}
}

func TestHandleDispatchOutcome(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC)

	tests := []dispatchOutcomeTestCase{
		{
			name: "healthcheck ping is ignored", seedScheduleType: "heartbeat", seedScheduleID: "hb-ping", seedRunID: "healthcheck-ping",
			event:         &pubsub.DispatchOutcomeEvent{DispatchID: "healthcheck-ping", Status: "completed"},
			wantRunStatus: "running", wantDurationMs: -1,
		},
		{
			name:             "heartbeat completed updates run and advances watermark",
			seedScheduleType: "heartbeat", seedScheduleID: "hb-disp-1", seedRunID: "run-hb-1",
			event:         &pubsub.DispatchOutcomeEvent{DispatchID: "run-hb-1", Status: "completed", StartedAt: now, CompletedAt: now.Add(5 * time.Second), DurationMs: 5000},
			wantRunStatus: "completed", wantDurationMs: 5000, wantWatermarkTime: &now,
		},
		{
			name:             "heartbeat completed with missing schedule updates orphan run and continues",
			seedScheduleType: "heartbeat", seedScheduleID: "hb-orphan", seedRunID: "run-hb-orphan", orphanRun: true,
			event:         &pubsub.DispatchOutcomeEvent{DispatchID: "run-hb-orphan", Status: "completed", StartedAt: now, CompletedAt: now.Add(5 * time.Second), DurationMs: 5000},
			wantRunStatus: "completed", wantDurationMs: 5000,
		},
		{
			name:             "heartbeat failed updates run and does not advance watermark",
			seedScheduleType: "heartbeat", seedScheduleID: "hb-disp-2", seedRunID: "run-hb-2",
			event:         &pubsub.DispatchOutcomeEvent{DispatchID: "run-hb-2", Status: "failed", Error: "connection refused", StartedAt: now, CompletedAt: now.Add(2 * time.Second), DurationMs: 2000},
			wantRunStatus: "failed", wantRunError: "connection refused", wantDurationMs: 2000,
		},
		{
			name:             "heartbeat skipped_active updates run and does not advance watermark",
			seedScheduleType: "heartbeat", seedScheduleID: "hb-disp-skip", seedRunID: "run-hb-skip",
			event:         &pubsub.DispatchOutcomeEvent{DispatchID: "run-hb-skip", Status: "skipped_active", StartedAt: now, CompletedAt: now},
			wantRunStatus: "skipped_active", wantDurationMs: -1,
		},
		{
			name:             "cron completed updates run without touching heartbeat watermark",
			seedScheduleType: "cron", seedScheduleID: "cron-disp-1", seedRunID: "run-cron-1",
			event:         &pubsub.DispatchOutcomeEvent{DispatchID: "run-cron-1", Status: "completed", StartedAt: now, CompletedAt: now.Add(10 * time.Second), DurationMs: 10000},
			wantRunStatus: "completed", wantDurationMs: 10000,
		},
		{
			name:             "heartbeat completed with zero duration and zero completedAt calculates both",
			seedScheduleType: "heartbeat", seedScheduleID: "hb-disp-zero", seedRunID: "run-hb-zero",
			event:         &pubsub.DispatchOutcomeEvent{DispatchID: "run-hb-zero", Status: "completed", StartedAt: now},
			wantRunStatus: "completed", wantWatermarkTime: &now, wantCheckCalculated: true, wantDurationMs: -1,
		},
		{
			name:             "terminal completed run ignores subsequent outcome event",
			seedScheduleType: "heartbeat", seedScheduleID: "hb-disp-term", seedRunID: "run-hb-term", seedRunStatus: "completed",
			event:         &pubsub.DispatchOutcomeEvent{DispatchID: "run-hb-term", Status: "failed", Error: "late failure"},
			wantRunStatus: "completed", wantDurationMs: -1,
		},
		{
			name:             "uuid-keyed heartbeat run resolves via stored dispatch id",
			seedScheduleType: "heartbeat", seedScheduleID: "hb-uuid", seedRunID: "6f1c2a52-0d2b-5b8e-9a51-3f1f6b7f9c10", seedDispatchID: "heartbeat:hb-uuid:2026-11-14T01:00:00Z",
			event:         &pubsub.DispatchOutcomeEvent{DispatchID: "heartbeat:hb-uuid:2026-11-14T01:00:00Z", Source: "heartbeat", Status: "completed", StartedAt: now, CompletedAt: now.Add(3 * time.Second), DurationMs: 3000},
			wantRunStatus: "completed", wantDurationMs: 3000, wantWatermarkTime: &now,
		},
		{
			name:             "dispatch id equal only to run row id is not resolved and naks",
			seedScheduleType: "heartbeat", seedScheduleID: "hb-id-only", seedRunID: "run-hb-id-only", seedDispatchID: "heartbeat:hb-id-only:2026-11-14T01:00:00Z",
			event:         &pubsub.DispatchOutcomeEvent{DispatchID: "run-hb-id-only", Source: "heartbeat", Status: "completed", StartedAt: now},
			wantRunStatus: "running", wantDurationMs: -1,
		},
		{name: "empty dispatch id returns nil", event: &pubsub.DispatchOutcomeEvent{DispatchID: "   "}, wantDurationMs: -1},
		{
			name:             "zero event startedAt falls back to run startedAt for duration and watermark",
			seedScheduleType: "heartbeat", seedScheduleID: "hb-fallback-1", seedRunID: "run-fallback-1",
			event:         &pubsub.DispatchOutcomeEvent{DispatchID: "run-fallback-1", Status: "completed", CompletedAt: now.Add(4 * time.Second)},
			wantRunStatus: "completed", wantDurationMs: 4000, wantWatermarkTime: &now,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDispatchOutcomeCase(t, tc, now)
		})
	}
}

func TestHeartbeatRunLifecycle(t *testing.T) {
	t.Parallel()
	due1 := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	fired1 := time.Date(2026, 11, 22, 10, 1, 0, 0, time.UTC)
	due2 := time.Date(2026, 11, 22, 10, 5, 0, 0, time.UTC)
	fired2 := time.Date(2026, 11, 22, 10, 5, 30, 0, time.UTC)
	priorWatermark := time.Date(2026, 11, 21, 8, 0, 0, 0, time.UTC)

	tests := []struct {
		name               string
		scheduleID         string
		seedWatermark      *time.Time
		outcomeStatus      string
		wantDispatchID     string
		wantFirstMark      string
		wantRunStatus      string
		wantSecondDispatch bool
		wantSecondMark     string
	}{
		{
			name: "completed outcome finalizes run and next tick uses its watermark", scheduleID: "hb-life-ok", outcomeStatus: "completed",
			wantDispatchID: "heartbeat:hb-life-ok:2026-11-22T10:00:00Z", wantFirstMark: "none (initial run)", wantRunStatus: "completed",
			wantSecondDispatch: true, wantSecondMark: "2026-11-22T10:01:00Z",
		},
		{
			name: "failed outcome finalizes run but keeps initial watermark", scheduleID: "hb-life-fail", outcomeStatus: "failed",
			wantDispatchID: "heartbeat:hb-life-fail:2026-11-22T10:00:00Z", wantFirstMark: "none (initial run)", wantRunStatus: "failed",
			wantSecondDispatch: true, wantSecondMark: "none (initial run)",
		},
		{
			name: "existing watermark is sent on the first fire", scheduleID: "hb-life-prior", seedWatermark: &priorWatermark, outcomeStatus: "completed",
			wantDispatchID: "heartbeat:hb-life-prior:2026-11-22T10:00:00Z", wantFirstMark: "2026-11-21T08:00:00Z", wantRunStatus: "completed",
			wantSecondDispatch: true, wantSecondMark: "2026-11-22T10:01:00Z",
		},
		{
			name: "no outcome leaves run running and next tick skips", scheduleID: "hb-life-none",
			wantDispatchID: "heartbeat:hb-life-none:2026-11-22T10:00:00Z", wantFirstMark: "none (initial run)", wantRunStatus: "running",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			db := openTickerTestDB(t)
			seedLifecycleHeartbeat(ctx, t, db, tc.scheduleID, due1, tc.seedWatermark)
			disp := &fakeDispatcher{dispatchResponse: eriwire.DispatchEnqueueResponse{Status: eriwire.StatusAccepted}, echoDispatchID: true}
			s := NewHaroSchedule(db, disp)

			fireHeartbeatAt(t, s, tc.scheduleID, due1, fired1)
			if len(disp.dispatchedRequests) != 1 {
				t.Fatalf("first fire dispatched %d requests, want 1", len(disp.dispatchedRequests))
			}
			if got := disp.dispatchedRequests[0].ID; got != tc.wantDispatchID {
				t.Errorf("dispatch ID = %q, want %q", got, tc.wantDispatchID)
			}
			assertWatermark(t, disp.dispatchedRequests[0].Prompt, tc.wantFirstMark)

			deliverLifecycleOutcome(ctx, t, db, tc.wantDispatchID, tc.outcomeStatus, fired1)
			run, err := store.GetScheduleRunByDispatchID(ctx, db, tc.wantDispatchID)
			if err != nil {
				t.Fatalf("GetScheduleRunByDispatchID: %v", err)
			}
			if run.Status != tc.wantRunStatus {
				t.Errorf("run status = %q, want %q", run.Status, tc.wantRunStatus)
			}

			fireHeartbeatAt(t, s, tc.scheduleID, due2, fired2)
			gotSecond := len(disp.dispatchedRequests) == 2
			if gotSecond != tc.wantSecondDispatch {
				t.Fatalf("second fire dispatched = %v, want %v", gotSecond, tc.wantSecondDispatch)
			}
			if gotSecond {
				assertWatermark(t, disp.dispatchedRequests[1].Prompt, tc.wantSecondMark)
			}
		})
	}
}

func seedLifecycleHeartbeat(ctx context.Context, t *testing.T, db *sql.DB, scheduleID string, due time.Time, watermark *time.Time) {
	t.Helper()
	if err := store.InsertHeartbeatSchedule(ctx, db, model.HeartbeatSchedule{
		ID: scheduleID, TargetID: "100000000000000003", CronExpr: "*/5 * * * *",
		Prompt: "Prompt heartbeat", Timezone: "UTC", NextRunAt: due, Enabled: true,
	}); err != nil {
		t.Fatalf("InsertHeartbeatSchedule: %v", err)
	}
	if watermark == nil {
		return
	}
	if err := store.AdvanceHeartbeatLastCompletedAt(ctx, db, scheduleID, *watermark); err != nil {
		t.Fatalf("AdvanceHeartbeatLastCompletedAt: %v", err)
	}
}

// deliverLifecycleOutcome feeds eri's dispatch outcome for dispatchID through HandleDispatchOutcome; empty status delivers nothing.
func deliverLifecycleOutcome(ctx context.Context, t *testing.T, db *sql.DB, dispatchID, status string, startedAt time.Time) {
	t.Helper()
	if status == "" {
		return
	}
	payload, err := json.Marshal(pubsub.DispatchOutcomeEvent{DispatchID: dispatchID, Source: "heartbeat", Status: status, StartedAt: startedAt, CompletedAt: startedAt.Add(30 * time.Second), DurationMs: 30000})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	msg := &pubsubtest.Message{Payload: payload, Subj: "aerial.dispatch." + status, Delivered: 1}
	if err := HandleDispatchOutcome(ctx, db, msg); err != nil {
		t.Fatalf("HandleDispatchOutcome: %v", err)
	}
	if _, naked := msg.NakDelay(); naked {
		t.Fatal("outcome was nakked, want resolved")
	}
}

func assertWatermark(t *testing.T, prompt, wantMark string) {
	t.Helper()
	if !strings.Contains(prompt, "Last Successful Check Started At: "+wantMark) {
		t.Errorf("prompt watermark missing %q", wantMark)
	}
}

func TestCalculateOutcomeTiming(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC)
	tests := []struct {
		name             string
		event            pubsub.DispatchOutcomeEvent
		runStartedAt     time.Time
		wantDurationMs   int64
		wantStartedAtRun bool
		wantCompletedSet bool
	}{
		{
			name:         "explicit timestamps and duration pass through",
			event:        pubsub.DispatchOutcomeEvent{StartedAt: start, CompletedAt: start.Add(5 * time.Second), DurationMs: 5000},
			runStartedAt: start, wantDurationMs: 5000, wantCompletedSet: true,
		},
		{
			name:         "zero completedAt falls back to wall clock",
			event:        pubsub.DispatchOutcomeEvent{StartedAt: start, DurationMs: 2000},
			runStartedAt: start, wantDurationMs: 2000, wantCompletedSet: true,
		},
		{
			name:         "zero startedAt falls back to run start",
			event:        pubsub.DispatchOutcomeEvent{CompletedAt: start.Add(4 * time.Second)},
			runStartedAt: start, wantDurationMs: 4000, wantStartedAtRun: true, wantCompletedSet: true,
		},
		{
			name:         "zero duration derives from completed minus started",
			event:        pubsub.DispatchOutcomeEvent{StartedAt: start, CompletedAt: start.Add(3 * time.Second)},
			runStartedAt: start, wantDurationMs: 3000, wantCompletedSet: true,
		},
		{
			name:         "negative duration derives from completed minus started",
			event:        pubsub.DispatchOutcomeEvent{StartedAt: start, CompletedAt: start.Add(2 * time.Second), DurationMs: -7},
			runStartedAt: start, wantDurationMs: 2000, wantCompletedSet: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			completedAt, durationMs, startedAt := calculateOutcomeTiming(tc.event, tc.runStartedAt)
			if tc.wantCompletedSet && completedAt.IsZero() {
				t.Error("expected non-zero completedAt")
			}
			if durationMs != tc.wantDurationMs {
				t.Errorf("durationMs = %d, want %d", durationMs, tc.wantDurationMs)
			}
			if tc.wantStartedAtRun && !startedAt.Equal(tc.runStartedAt) {
				t.Errorf("startedAt = %v, want run start %v", startedAt, tc.runStartedAt)
			}
			if !tc.wantStartedAtRun && !tc.event.StartedAt.IsZero() && !startedAt.Equal(tc.event.StartedAt) {
				t.Errorf("startedAt = %v, want event start %v", startedAt, tc.event.StartedAt)
			}
		})
	}
}

func TestAdvanceHeartbeatWatermark(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC)
	tests := []struct {
		name           string
		seedSchedule   bool
		scheduleID     string
		scheduleType   string
		status         string
		startedAt      time.Time
		wantWatermark  *time.Time
		wantNoSchedule bool
	}{
		{name: "completed heartbeat advances watermark", seedSchedule: true, scheduleID: "hb-wm-1", scheduleType: model.ScheduleTypeHeartbeat, status: model.StatusCompleted, startedAt: now, wantWatermark: &now},
		{name: "failed heartbeat leaves watermark unset", seedSchedule: true, scheduleID: "hb-wm-2", scheduleType: model.ScheduleTypeHeartbeat, status: model.StatusFailed, startedAt: now},
		{name: "completed cron leaves heartbeat watermark unset", seedSchedule: true, scheduleID: "hb-wm-3", scheduleType: model.ScheduleTypeCron, status: model.StatusCompleted, startedAt: now},
		{name: "zero startedAt leaves watermark unset", seedSchedule: true, scheduleID: "hb-wm-4", scheduleType: model.ScheduleTypeHeartbeat, status: model.StatusCompleted},
		{name: "missing schedule logs without failing", scheduleID: "hb-wm-missing", scheduleType: model.ScheduleTypeHeartbeat, status: model.StatusCompleted, startedAt: now, wantNoSchedule: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTickerTestDB(t)
			ctx := context.Background()
			if tc.seedSchedule {
				hb := model.HeartbeatSchedule{
					ID: tc.scheduleID, TargetID: "1234567890", TitlePrefix: "WM",
					CronExpr: "*/5 * * * *", Prompt: "p", Timezone: "UTC",
					NextRunAt: now, Enabled: true,
				}
				if err := store.InsertHeartbeatSchedule(ctx, db, hb); err != nil {
					t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
				}
			}
			run := &model.ScheduleRun{ScheduleID: tc.scheduleID, ScheduleType: tc.scheduleType}
			advanceHeartbeatWatermark(ctx, db, run, tc.status, tc.startedAt)
			if tc.wantNoSchedule {
				return
			}
			loaded, err := store.GetHeartbeatSchedule(ctx, db, tc.scheduleID)
			if err != nil {
				t.Fatalf("GetHeartbeatSchedule failed: %v", err)
			}
			switch {
			case tc.wantWatermark == nil && loaded.LastCompletedAt != nil:
				t.Errorf("expected LastCompletedAt = nil, got %v", loaded.LastCompletedAt)
			case tc.wantWatermark != nil && (loaded.LastCompletedAt == nil || !loaded.LastCompletedAt.Equal(*tc.wantWatermark)):
				t.Errorf("LastCompletedAt = %v, want %v", loaded.LastCompletedAt, tc.wantWatermark)
			}
		})
	}
}

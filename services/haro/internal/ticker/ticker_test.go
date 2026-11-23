package ticker

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

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

func TestNewTicker(t *testing.T) {
	t.Parallel()
	db := openTickerTestDB(t)
	mock := &fakeDispatcher{}

	tests := []struct {
		name         string
		interval     time.Duration
		wantInterval time.Duration
	}{
		{name: "positive interval is kept", interval: 5 * time.Second, wantInterval: 5 * time.Second},
		{name: "zero interval defaults to 30s", interval: 0, wantInterval: 30 * time.Second},
		{name: "negative interval defaults to 30s", interval: -time.Second, wantInterval: 30 * time.Second},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := NewTicker(db, mock, tc.interval)
			if got == nil {
				t.Fatal("NewTicker returned nil")
			}
			if got.interval != tc.wantInterval {
				t.Errorf("interval = %v, want %v", got.interval, tc.wantInterval)
			}
		})
	}
}

type evaluateDueSchedulesTestCase struct {
	name                string
	setupCron           *model.CronSchedule
	setupOneShot        *model.OneShotSchedule
	dispatcherStatus    string
	responseChannelID   string
	responseMessageID   string
	dispatcherErr       error
	evaluateNowOffset   time.Duration
	wantDispatched      int
	wantRunStatus       string
	wantRunError        string
	wantThreadID        string
	wantMessageNonEmpty bool
	wantDeterministicID string
}

func runEvaluateDueSchedulesCase(t *testing.T, tc evaluateDueSchedulesTestCase) {
	t.Helper()
	db := openTickerTestDB(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC).Add(tc.evaluateNowOffset)

	var scheduleID string
	if tc.setupCron != nil {
		scheduleID = tc.setupCron.ID
		if err := store.InsertCronSchedule(ctx, db, *tc.setupCron); err != nil {
			t.Fatalf("InsertCronSchedule() err = %v", err)
		}
	}
	if tc.setupOneShot != nil {
		scheduleID = tc.setupOneShot.ID
		if err := store.InsertOneShotSchedule(ctx, db, *tc.setupOneShot); err != nil {
			t.Fatalf("InsertOneShotSchedule() err = %v", err)
		}
	}

	mock := &fakeDispatcher{
		response: dispatch.PromptDispatchResponse{
			Status:    tc.dispatcherStatus,
			ChannelID: tc.responseChannelID,
			MessageID: tc.responseMessageID,
		},
		err: tc.dispatcherErr,
		dispatchResponse: eriwire.DispatchEnqueueResponse{
			Status:     tc.dispatcherStatus,
			DispatchID: tc.responseMessageID,
		},
		dispatchErr: tc.dispatcherErr,
	}

	ticker := NewTicker(db, mock, 30*time.Second)
	ticker.EvaluateDueSchedules(ctx, now)

	totalDispatched := len(mock.dispatched) + len(mock.dispatchedRequests)
	if totalDispatched != tc.wantDispatched {
		t.Fatalf("dispatched count = %d, want %d", totalDispatched, tc.wantDispatched)
	}

	if tc.wantRunStatus == "" {
		return
	}
	row := db.QueryRowContext(ctx, "SELECT id, status, error, thread_id, message_id FROM schedule_runs WHERE schedule_id = $1", scheduleID)
	var runID, status, errMsg, threadID, messageID string
	if err := row.Scan(&runID, &status, &errMsg, &threadID, &messageID); err != nil {
		t.Fatalf("failed to query schedule_runs for %s: %v", scheduleID, err)
	}
	if status != tc.wantRunStatus {
		t.Errorf("schedule_run status = %q, want %q", status, tc.wantRunStatus)
	}
	if tc.wantRunError != "" && !strings.Contains(errMsg, tc.wantRunError) {
		t.Errorf("schedule_run error = %q, want substring %q", errMsg, tc.wantRunError)
	}
	if tc.wantThreadID != "" && threadID != tc.wantThreadID {
		t.Errorf("schedule_run thread_id = %q, want %q", threadID, tc.wantThreadID)
	}
	if tc.wantMessageNonEmpty && messageID == "" {
		t.Error("schedule_run message_id is empty, want generated run ID fallback")
	}
	if tc.wantDeterministicID != "" && runID != tc.wantDeterministicID {
		t.Errorf("schedule_run id = %q, want deterministic %q", runID, tc.wantDeterministicID)
	}
	if tc.wantDeterministicID != "" && len(mock.dispatched) > 0 && mock.dispatched[0].MessageID != tc.wantDeterministicID {
		t.Errorf("dispatched prompt MessageID = %q, want deterministic %q", mock.dispatched[0].MessageID, tc.wantDeterministicID)
	}
}

func TestTicker_EvaluateDueSchedules(t *testing.T) {
	t.Parallel()
	tests := []evaluateDueSchedulesTestCase{
		{
			name: "due cron dispatched successfully",
			setupCron: &model.CronSchedule{
				ID: "cron-eval-1", TargetID: "123456789", TitlePrefix: "Eval Title",
				CronExpr: "0 12 * * *", Prompt: "Daily Eval", Timezone: "UTC",
				NextRunAt: time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC), Enabled: true, Effort: "medium",
			},
			dispatcherStatus: "accepted", responseChannelID: "123456789", responseMessageID: "msg-1",
			wantDispatched: 1, wantRunStatus: "completed",
			wantDeterministicID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("cron:cron-eval-1:"+time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC).Format(time.RFC3339Nano))).String(),
		},
		{
			name: "due cron with invalid cron expression uses fallback next run and dispatches",
			setupCron: &model.CronSchedule{
				ID: "cron-eval-invalid-expr", TargetID: "123456789", TitlePrefix: "Invalid Expr",
				CronExpr: "not a valid cron", Prompt: "Fallback Eval", Timezone: "UTC",
				NextRunAt: time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC), Enabled: true, Effort: "medium",
			},
			dispatcherStatus: "accepted", responseChannelID: "123456789", responseMessageID: "msg-invalid",
			wantDispatched: 1, wantRunStatus: "completed",
		},
		{
			name: "due cron with empty response fields falls back to target and run IDs",
			setupCron: &model.CronSchedule{
				ID: "cron-eval-empty-resp", TargetID: "123456789", TitlePrefix: "Empty Resp",
				CronExpr: "0 12 * * *", Prompt: "Empty Resp Eval", Timezone: "UTC",
				NextRunAt: time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC), Enabled: true, Effort: "medium",
			},
			dispatcherStatus: "accepted", responseChannelID: "", responseMessageID: "",
			wantDispatched: 1, wantRunStatus: "completed",
			wantThreadID: "123456789", wantMessageNonEmpty: true,
		},
		{
			name: "due cron stale over 24h skips dispatch",
			setupCron: &model.CronSchedule{
				ID: "cron-eval-stale", TargetID: "123456789", TitlePrefix: "Stale Title",
				CronExpr: "0 12 * * *", Prompt: "Stale Check", Timezone: "UTC",
				NextRunAt: time.Date(2026, 10, 22, 10, 0, 0, 0, time.UTC), Enabled: true, Effort: "low",
			},
			wantDispatched: 0, wantRunStatus: "failed", wantRunError: "stale schedule (>24h overdue)",
		},
		{
			name: "due cron dispatch error records failed run",
			setupCron: &model.CronSchedule{
				ID: "cron-eval-err", TargetID: "123456789", TitlePrefix: "Error Title",
				CronExpr: "0 12 * * *", Prompt: "Error Check", Timezone: "UTC",
				NextRunAt: time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC), Enabled: true, Effort: "high",
			},
			dispatcherErr:  fmt.Errorf("connection refused"),
			wantDispatched: 1, wantRunStatus: "failed", wantRunError: "connection refused",
		},
		{
			name: "due one shot dispatched successfully",
			setupOneShot: &model.OneShotSchedule{
				ID: "oneshot-eval-1", ThreadID: "987654321", Prompt: "Remind me",
				RunAt: time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC), Effort: "low",
			},
			dispatcherStatus: "accepted", responseChannelID: "987654321", responseMessageID: "msg-os-1",
			wantDispatched: 1, wantRunStatus: "completed",
			wantDeterministicID: uuid.NewSHA1(uuid.NameSpaceOID, []byte("one_shot:oneshot-eval-1:"+time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC).Format(time.RFC3339Nano))).String(),
		},
		{
			name: "due one shot with empty response fields falls back to thread and run IDs",
			setupOneShot: &model.OneShotSchedule{
				ID: "oneshot-eval-empty-resp", ThreadID: "987654321", Prompt: "Remind me empty",
				RunAt: time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC), Effort: "low",
			},
			dispatcherStatus: "accepted", responseChannelID: "", responseMessageID: "",
			wantDispatched: 1, wantRunStatus: "completed",
			wantThreadID: "987654321", wantMessageNonEmpty: true,
		},
		{
			name: "due one shot stale over 24h skips dispatch",
			setupOneShot: &model.OneShotSchedule{
				ID: "oneshot-eval-stale", ThreadID: "987654321", Prompt: "Stale Reminder",
				RunAt: time.Date(2026, 10, 21, 10, 0, 0, 0, time.UTC), Effort: "low",
			},
			wantDispatched: 0, wantRunStatus: "failed", wantRunError: "stale schedule (>24h overdue)",
		},
		{
			name: "due one shot dispatch error records failed run",
			setupOneShot: &model.OneShotSchedule{
				ID: "oneshot-eval-err", ThreadID: "987654321", Prompt: "Error Reminder",
				RunAt: time.Date(2026, 10, 23, 11, 59, 0, 0, time.UTC), Effort: "medium",
			},
			dispatcherErr:  fmt.Errorf("network timeout"),
			wantDispatched: 1, wantRunStatus: "failed", wantRunError: "network timeout",
		},
		{
			name:              "no due schedules dispatches nothing",
			wantDispatched:    0,
			evaluateNowOffset: -48 * time.Hour,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runEvaluateDueSchedulesCase(t, tc)
		})
	}
}

func TestTicker_Pruning(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name         string
		lastPrune    time.Time
		evaluateAt   time.Time
		wantPruneRun bool
	}{
		{name: "zero last prune triggers pruning", evaluateAt: now, wantPruneRun: true},
		{name: "recent prune skips pruning", lastPrune: now.Add(-time.Hour), evaluateAt: now, wantPruneRun: false},
		{name: "day-old prune triggers pruning", lastPrune: now.Add(-25 * time.Hour), evaluateAt: now, wantPruneRun: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTickerTestDB(t)
			mock := &fakeDispatcher{response: dispatch.PromptDispatchResponse{Status: "ok"}}
			ticker := NewTicker(db, mock, 30*time.Second)
			ticker.lastPrune = tc.lastPrune

			ticker.EvaluateDueSchedules(context.Background(), tc.evaluateAt)
			if tc.wantPruneRun && ticker.lastPrune.IsZero() {
				t.Error("expected lastPrune to be updated, got zero")
			}
			if !tc.wantPruneRun && !ticker.lastPrune.Equal(tc.lastPrune) {
				t.Errorf("expected lastPrune unchanged at %v, got %v", tc.lastPrune, ticker.lastPrune)
			}

			prevPrune := ticker.lastPrune
			ticker.EvaluateDueSchedules(context.Background(), tc.evaluateAt.Add(time.Minute))
			if !prevPrune.IsZero() && !ticker.lastPrune.Equal(prevPrune) {
				t.Errorf("expected lastPrune unchanged at %v, got %v", prevPrune, ticker.lastPrune)
			}
		})
	}
}

func TestTicker_Start(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		nilTicker      bool
		zeroValue      bool
		interval       time.Duration
		useTimeout     bool
		timeout        time.Duration
		seedDueAfter   time.Duration
		wantDispatched int
	}{
		{name: "nil ticker returns immediately", nilTicker: true},
		{name: "cancelled context stops after initial evaluate", interval: 10 * time.Millisecond},
		{name: "zero value ticker stops on cancelled context", zeroValue: true},
		{name: "tick fires schedule due after initial evaluate", interval: 10 * time.Millisecond, useTimeout: true, timeout: 300 * time.Millisecond, seedDueAfter: 50 * time.Millisecond, wantDispatched: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTickerTestDB(t)
			mock := &fakeDispatcher{response: dispatch.PromptDispatchResponse{Status: "ok", MessageID: "msg-tick"}}

			if tc.seedDueAfter > 0 {
				seed := faultCron("cron-tick", time.Now().UTC().Add(tc.seedDueAfter))
				if err := store.InsertCronSchedule(context.Background(), db, *seed); err != nil {
					t.Fatalf("InsertCronSchedule failed: %v", err)
				}
			}

			var ticker *Ticker
			switch {
			case tc.nilTicker:
			case tc.zeroValue:
				ticker = &Ticker{}
			default:
				ticker = NewTicker(db, mock, tc.interval)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.useTimeout {
				var timeoutCancel context.CancelFunc
				timeout := tc.timeout
				if timeout <= 0 {
					timeout = 100 * time.Millisecond
				}
				ctx, timeoutCancel = context.WithTimeout(context.Background(), timeout)
				defer timeoutCancel()
			} else {
				cancel()
			}
			doneCh := make(chan struct{})
			var startErr error
			go func() {
				defer close(doneCh)
				startErr = ticker.Start(ctx)
			}()

			select {
			case <-doneCh:
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for ticker.Start() to stop")
			}
			if startErr != nil && !errors.Is(startErr, context.Canceled) {
				t.Fatalf("unexpected start error: %v", startErr)
			}

			if got := len(mock.dispatched) + len(mock.dispatchedRequests); got != tc.wantDispatched {
				t.Errorf("dispatched = %d, want %d", got, tc.wantDispatched)
			}
		})
	}
}

type evaluateDueHeartbeatsTestCase struct {
	name              string
	hbID              string
	cronExpr          string
	nextRunAt         time.Time
	dispatcherStatus  string
	dispatcherErr     error
	seedRunningRun    bool
	wantDispatched    bool
	wantSkippedRun    bool
	wantStaleRun      bool
	wantFailedRun     bool
	wantSessionPrefix string
}

func assertHeartbeatDispatch(t *testing.T, mock *fakeDispatcher, tc evaluateDueHeartbeatsTestCase) {
	t.Helper()
	if !tc.wantDispatched {
		if len(mock.dispatchedRequests) > 0 {
			t.Errorf("dispatcher should NOT be called for %s, called %d times", tc.hbID, len(mock.dispatchedRequests))
		}
		return
	}
	if len(mock.dispatchedRequests) == 0 {
		t.Errorf("expected dispatcher to be called for %s", tc.hbID)
		return
	}
	req := mock.dispatchedRequests[len(mock.dispatchedRequests)-1]
	if !strings.Contains(req.Prompt, "Raw monitoring prompt for "+tc.hbID) {
		t.Errorf("dispatcher prompt missing raw prompt:\n%s", req.Prompt)
	}
	if !strings.Contains(req.Prompt, `<\HEARTBEAT_INSTRUCTIONS>`) {
		t.Errorf("dispatcher prompt missing <\\HEARTBEAT_INSTRUCTIONS> wrapper:\n%s", req.Prompt)
	}
	if req.SessionID != "" {
		t.Errorf("req.SessionID = %q, want empty string", req.SessionID)
	}
	if req.Source != model.ScheduleTypeHeartbeat {
		t.Errorf("req.Source = %q, want 'heartbeat'", req.Source)
	}
	if !req.SkipIfActive {
		t.Error("expected req.SkipIfActive = true")
	}
	wantRunID := deterministicScheduleRunID(model.ScheduleTypeHeartbeat, tc.hbID, tc.nextRunAt)
	if req.ID != wantRunID {
		t.Errorf("req.ID = %q, want deterministic %q", req.ID, wantRunID)
	}
}

func assertHeartbeatScheduleRuns(t *testing.T, db *sql.DB, tc evaluateDueHeartbeatsTestCase) {
	t.Helper()
	var runCount int
	var runStatus, runPrompt, runType string
	if err := db.QueryRow("SELECT COUNT(*), COALESCE(MAX(status), ''), COALESCE(MAX(prompt), ''), COALESCE(MAX(schedule_type), '') FROM schedule_runs WHERE schedule_id = $1", tc.hbID).Scan(&runCount, &runStatus, &runPrompt, &runType); err != nil {
		t.Fatalf("query schedule_runs: %v", err)
	}

	if tc.seedRunningRun {
		if runCount != 1 || runStatus != "running" {
			t.Errorf("expected 1 running run for seeded active heartbeat %s, got count=%d, status=%s", tc.hbID, runCount, runStatus)
		}
		return
	}

	if tc.wantSkippedRun && runCount != 0 {
		t.Errorf("expected 0 runs for skipped active heartbeat %s, got count=%d", tc.hbID, runCount)
	}
	if (tc.wantStaleRun || tc.wantFailedRun) && (runCount == 0 || runStatus != model.StatusFailed) {
		t.Errorf("expected failed run for %s, got count=%d, status=%s", tc.hbID, runCount, runStatus)
	}
	if tc.wantDispatched && tc.dispatcherErr == nil && !tc.wantSkippedRun && (runCount == 0 || runStatus != "running") {
		t.Errorf("expected running run for %s, got count=%d, status=%s", tc.hbID, runCount, runStatus)
	}
	if runCount > 0 {
		if runType != model.ScheduleTypeHeartbeat {
			t.Errorf("expected schedule_type='heartbeat', got %s", runType)
		}
		if strings.Contains(runPrompt, `HEARTBEAT_INSTRUCTIONS`) {
			t.Errorf("schedule_runs.prompt must NOT contain HEARTBEAT_INSTRUCTIONS, got:\n%s", runPrompt)
		}
	}
}

func runEvaluateDueHeartbeatsCase(t *testing.T, tc evaluateDueHeartbeatsTestCase, now time.Time) {
	t.Helper()
	db := openTickerTestDB(t)
	ctx := context.Background()

	hb := model.HeartbeatSchedule{
		ID:          tc.hbID,
		TargetID:    "1562847268833337377",
		TitlePrefix: "Test HB",
		CronExpr:    tc.cronExpr,
		Prompt:      "Raw monitoring prompt for " + tc.hbID,
		Timezone:    "UTC",
		Effort:      "low",
		NextRunAt:   tc.nextRunAt,
		Enabled:     true,
		CreatedAt:   now.Add(-48 * time.Hour),
	}
	if err := store.InsertHeartbeatSchedule(ctx, db, hb); err != nil {
		t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
	}

	if tc.seedRunningRun {
		seededRun := model.ScheduleRun{
			ID: "existing-" + tc.hbID, ScheduleID: tc.hbID, ScheduleType: model.ScheduleTypeHeartbeat,
			MessageID: "existing-msg-" + tc.hbID, TargetID: hb.TargetID, ThreadID: "heartbeat-" + tc.hbID,
			Title: hb.TitlePrefix, Prompt: hb.Prompt, Status: "running",
			StartedAt: now.Add(-5 * time.Minute), Effort: hb.Effort,
		}
		if err := store.CreateScheduleRun(ctx, db, seededRun); err != nil {
			t.Fatalf("CreateScheduleRun seeded run failed: %v", err)
		}
	}

	dispStatus := "ok"
	if tc.dispatcherStatus != "" {
		dispStatus = tc.dispatcherStatus
	}
	mock := &fakeDispatcher{
		response:         dispatch.PromptDispatchResponse{Status: dispStatus, MessageID: "msg-dispatched"},
		err:              tc.dispatcherErr,
		dispatchResponse: eriwire.DispatchEnqueueResponse{Status: dispStatus, DispatchID: "msg-dispatched"},
		dispatchErr:      tc.dispatcherErr,
	}
	ticker := NewTicker(db, mock, 30*time.Second)
	ticker.EvaluateDueSchedules(ctx, now)

	assertHeartbeatDispatch(t, mock, tc)
	assertHeartbeatScheduleRuns(t, db, tc)

	after, err := store.GetHeartbeatSchedule(ctx, db, tc.hbID)
	if err != nil {
		t.Fatalf("failed to reload heartbeat %s: %v", tc.hbID, err)
	}
	if !after.NextRunAt.After(tc.nextRunAt) {
		t.Errorf("next_run_at was not advanced: before=%v, after=%v", tc.nextRunAt, after.NextRunAt)
	}
}

func TestTicker_EvaluateDueHeartbeats(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 23, 14, 0, 0, 0, time.UTC)

	tests := []evaluateDueHeartbeatsTestCase{
		{
			name: "nominal due heartbeat dispatched", hbID: "hb-nominal",
			cronExpr: "*/5 * * * *", nextRunAt: now.Add(-1 * time.Minute),
			wantDispatched: true, wantSessionPrefix: "heartbeat-hb-nominal",
		},
		{
			name: "active thread turn skipped without schedule run", hbID: "hb-coalesced",
			cronExpr: "*/5 * * * *", nextRunAt: now.Add(-1 * time.Minute),
			dispatcherStatus: "skipped_active", wantDispatched: true, wantSkippedRun: true,
		},
		{
			name: "in-flight schedule run skipped without dispatch", hbID: "hb-in-flight",
			cronExpr: "*/5 * * * *", nextRunAt: now.Add(-1 * time.Minute),
			seedRunningRun: true, wantDispatched: false,
		},
		{
			name: "stale heartbeat overdue by more than 24h", hbID: "hb-stale",
			cronExpr: "0 * * * *", nextRunAt: now.Add(-25 * time.Hour),
			wantDispatched: false, wantStaleRun: true,
		},
		{
			name: "dispatch failure recorded in schedule_runs", hbID: "hb-fail",
			cronExpr: "*/10 * * * *", nextRunAt: now.Add(-2 * time.Minute),
			dispatcherErr: fmt.Errorf("brain down"), wantDispatched: true, wantFailedRun: true,
		},
		{
			name: "invalid cron expression uses 24h fallback", hbID: "hb-bad-cron",
			cronExpr: "invalid cron string", nextRunAt: now.Add(-1 * time.Minute),
			wantDispatched: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runEvaluateDueHeartbeatsCase(t, tc, now)
		})
	}
}

type dispatchOutcomeTestCase struct {
	name                string
	rawPayload          string
	seedScheduleType    string
	seedScheduleID      string
	seedRunID           string
	seedDispatchID      string
	seedRunStatus       string
	seedRunStartedAt    time.Time
	orphanRun           bool
	event               *pubsub.DispatchOutcomeEvent
	nilDatabase         bool
	nilMessage          bool
	closeDatabase       bool
	sabotageSQL         string
	numDelivered        uint64
	wantErr             bool
	wantRunStatus       string
	wantRunError        string
	wantDurationMs      int64
	wantWatermarkTime   *time.Time
	wantCheckCalculated bool
	wantNaked           bool
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

func runDispatchOutcomeCase(t *testing.T, tc dispatchOutcomeTestCase, now time.Time) {
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

	heartbeatID := tc.seedScheduleID
	if tc.orphanRun {
		heartbeatID = ""
	}
	if !tc.nilDatabase && !tc.closeDatabase {
		seedOutcomeHeartbeat(ctx, t, db, tc.seedScheduleType, heartbeatID, now)
		startedAt := now
		if !tc.seedRunStartedAt.IsZero() {
			startedAt = tc.seedRunStartedAt
		}
		seedOutcomeRun(ctx, t, db, tc.seedRunID, tc.seedDispatchID, tc.seedScheduleType, tc.seedScheduleID, tc.seedRunStatus, startedAt)
		if tc.sabotageSQL != "" {
			if _, err := db.ExecContext(ctx, tc.sabotageSQL); err != nil {
				t.Fatalf("sabotage SQL failed: %v", err)
			}
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

func TestHandleDispatchOutcome(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 14, 1, 0, 0, 0, time.UTC)

	tests := []dispatchOutcomeTestCase{
		{name: "corrupt json returns nil to ack poison pill", rawPayload: `{"status": corrupt-json`, wantDurationMs: -1},
		{
			name: "healthcheck ping is ignored", seedScheduleType: "heartbeat", seedScheduleID: "hb-ping", seedRunID: "healthcheck-ping",
			event:         &pubsub.DispatchOutcomeEvent{DispatchID: "healthcheck-ping", Status: "completed"},
			wantRunStatus: "running", wantDurationMs: -1,
		},
		{name: "unknown dispatch id naks with delay for retry", event: &pubsub.DispatchOutcomeEvent{DispatchID: "disp-unknown", Status: "completed"}, wantNaked: true, wantDurationMs: -1},
		{
			name:         "unknown dispatch id delivered five times is dropped without nak",
			event:        &pubsub.DispatchOutcomeEvent{DispatchID: "disp-unknown", Status: "completed"},
			numDelivered: 5, wantDurationMs: -1,
		},
		{
			name:           "non-heartbeat source like vanadis is ignored without nak",
			event:          &pubsub.DispatchOutcomeEvent{DispatchID: "disp-vanadis-chunk", Source: "vanadis", Status: "completed"},
			wantDurationMs: -1,
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
			wantRunStatus: "running", wantNaked: true, wantDurationMs: -1,
		},
		{name: "nil database returns error", nilDatabase: true, wantErr: true, wantDurationMs: -1},
		{name: "nil message returns error", nilMessage: true, wantErr: true, wantDurationMs: -1},
		{
			name: "closed database returns query error", closeDatabase: true,
			event:   &pubsub.DispatchOutcomeEvent{DispatchID: "run-on-closed-db", Status: "completed"},
			wantErr: true, wantDurationMs: -1,
		},
		{
			name:             "outcome update failure returns error",
			seedScheduleType: "heartbeat", seedScheduleID: "hb-upd-fail", seedRunID: "run-upd-fail",
			event:       &pubsub.DispatchOutcomeEvent{DispatchID: "run-upd-fail", Status: "completed", StartedAt: now, CompletedAt: now.Add(time.Second)},
			sabotageSQL: failTriggerSQL("schedule_runs", "UPDATE", "forced update failure"),
			wantErr:     true, wantDurationMs: -1,
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

type evaluateFaultTestCase struct {
	name         string
	seedCron     *model.CronSchedule
	seedOneShot  *model.OneShotSchedule
	seedBeat     *model.HeartbeatSchedule
	sabotageSQL  string
	wantDispatch int
	wantRuns     int
}

func runEvaluateFaultCase(t *testing.T, tc evaluateFaultTestCase, now time.Time) {
	t.Helper()
	db := openTickerTestDB(t)
	ctx := context.Background()

	if tc.seedCron != nil {
		if err := store.InsertCronSchedule(ctx, db, *tc.seedCron); err != nil {
			t.Fatalf("InsertCronSchedule failed: %v", err)
		}
	}
	if tc.seedOneShot != nil {
		if err := store.InsertOneShotSchedule(ctx, db, *tc.seedOneShot); err != nil {
			t.Fatalf("InsertOneShotSchedule failed: %v", err)
		}
	}
	if tc.seedBeat != nil {
		if err := store.InsertHeartbeatSchedule(ctx, db, *tc.seedBeat); err != nil {
			t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
		}
	}
	if tc.sabotageSQL != "" {
		if _, err := db.ExecContext(ctx, tc.sabotageSQL); err != nil {
			t.Fatalf("sabotage SQL failed: %v", err)
		}
	}

	mock := &fakeDispatcher{
		response:         dispatch.PromptDispatchResponse{Status: "ok", MessageID: "msg-x"},
		dispatchResponse: eriwire.DispatchEnqueueResponse{Status: "ok", DispatchID: "msg-x"},
	}
	NewTicker(db, mock, 30*time.Second).EvaluateDueSchedules(ctx, now)

	if got := len(mock.dispatched) + len(mock.dispatchedRequests); got != tc.wantDispatch {
		t.Errorf("dispatched = %d, want %d", got, tc.wantDispatch)
	}
	var runCount int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM schedule_runs").Scan(&runCount); err != nil {
		if tc.wantRuns != 0 {
			t.Fatalf("counting schedule_runs failed: %v", err)
		}
		return
	}
	if runCount != tc.wantRuns {
		t.Errorf("schedule_runs count = %d, want %d", runCount, tc.wantRuns)
	}
}

func faultCron(id string, nextRunAt time.Time) *model.CronSchedule {
	return &model.CronSchedule{
		ID: id, TargetID: "123456789", TitlePrefix: "Fault", CronExpr: "0 12 * * *",
		Prompt: "fault", Timezone: "UTC", NextRunAt: nextRunAt, Enabled: true, Effort: "low",
	}
}

func faultOneShot(id string, runAt time.Time) *model.OneShotSchedule {
	return &model.OneShotSchedule{ID: id, ThreadID: "987654321", Prompt: "fault", RunAt: runAt, Effort: "low"}
}

func faultHeartbeat(id string, nextRunAt time.Time) *model.HeartbeatSchedule {
	return &model.HeartbeatSchedule{
		ID: id, TargetID: "1562847268833337377", TitlePrefix: "Fault", CronExpr: "*/5 * * * *",
		Prompt: "fault", Timezone: "UTC", NextRunAt: nextRunAt, Enabled: true,
	}
}

func TestTicker_EvaluateFaults(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	due := now.Add(-time.Minute)
	stale := now.Add(-25 * time.Hour)

	tests := []evaluateFaultTestCase{
		{
			name: "cron advance failure skips dispatch without run", seedCron: faultCron("cron-adv", due),
			sabotageSQL:  failTriggerSQL("cron_schedules", "UPDATE", "forced advance failure"),
			wantDispatch: 0, wantRuns: 0,
		},
		{
			name: "stale cron advance failure still records stale run", seedCron: faultCron("cron-adv-stale", stale),
			sabotageSQL:  failTriggerSQL("cron_schedules", "UPDATE", "forced advance failure"),
			wantDispatch: 0, wantRuns: 1,
		},
		{
			name: "cron run create failure dispatches without recording", seedCron: faultCron("cron-create", due),
			sabotageSQL: "DROP TABLE schedule_runs", wantDispatch: 1, wantRuns: 0,
		},
		{
			name: "stale cron run create failure records nothing", seedCron: faultCron("cron-create-stale", stale),
			sabotageSQL: "DROP TABLE schedule_runs", wantDispatch: 0, wantRuns: 0,
		},
		{
			name: "one-shot delete failure skips dispatch without run", seedOneShot: faultOneShot("os-del", due),
			sabotageSQL:  failTriggerSQL("one_shot_schedules", "DELETE", "forced delete failure"),
			wantDispatch: 0, wantRuns: 0,
		},
		{
			name: "one-shot run create failure dispatches without recording", seedOneShot: faultOneShot("os-create", due),
			sabotageSQL: "DROP TABLE schedule_runs", wantDispatch: 1, wantRuns: 0,
		},
		{
			name: "stale one-shot run create failure records nothing", seedOneShot: faultOneShot("os-create-stale", stale),
			sabotageSQL: "DROP TABLE schedule_runs", wantDispatch: 0, wantRuns: 0,
		},
		{
			name: "heartbeat advance failure skips dispatch without run", seedBeat: faultHeartbeat("hb-adv", due),
			sabotageSQL:  failTriggerSQL("heartbeat_schedules", "UPDATE", "forced advance failure"),
			wantDispatch: 0, wantRuns: 0,
		},
		{
			name: "stale heartbeat advance failure still records stale run", seedBeat: faultHeartbeat("hb-adv-stale", stale),
			sabotageSQL:  failTriggerSQL("heartbeat_schedules", "UPDATE", "forced advance failure"),
			wantDispatch: 0, wantRuns: 1,
		},
		{
			name: "heartbeat run create failure dispatches without recording", seedBeat: faultHeartbeat("hb-create", due),
			sabotageSQL: "DROP TABLE schedule_runs", wantDispatch: 1, wantRuns: 0,
		},
		{
			name: "stale heartbeat run create failure records nothing", seedBeat: faultHeartbeat("hb-create-stale", stale),
			sabotageSQL: "DROP TABLE schedule_runs", wantDispatch: 0, wantRuns: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runEvaluateFaultCase(t, tc, now)
		})
	}
}

func seedCancelSweep(t *testing.T, db *sql.DB, kind string, ids []string, due time.Time) {
	t.Helper()
	ctx := context.Background()
	switch kind {
	case "cron":
		for _, id := range ids {
			if err := store.InsertCronSchedule(ctx, db, *faultCron(id, due)); err != nil {
				t.Fatalf("InsertCronSchedule failed: %v", err)
			}
		}
	case "oneshot":
		for _, id := range ids {
			if err := store.InsertOneShotSchedule(ctx, db, *faultOneShot(id, due)); err != nil {
				t.Fatalf("InsertOneShotSchedule failed: %v", err)
			}
		}
	default:
		for _, id := range ids {
			if err := store.InsertHeartbeatSchedule(ctx, db, *faultHeartbeat(id, due)); err != nil {
				t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
			}
		}
	}
}

func TestTicker_EvaluateCancelledMidLoop(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
	due := now.Add(-time.Minute)
	tests := []struct {
		name string
		kind string
	}{
		{name: "cancel during cron sweep stops after first dispatch", kind: "cron"},
		{name: "cancel during one-shot sweep stops after first dispatch", kind: "oneshot"},
		{name: "cancel during heartbeat sweep stops after first dispatch", kind: "heartbeat"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTickerTestDB(t)
			seedIDs := []string{tc.kind + "-first", tc.kind + "-second"}
			seedCancelSweep(t, db, tc.kind, seedIDs, due)

			evalCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			mock := &fakeDispatcher{
				response:         dispatch.PromptDispatchResponse{Status: "ok", MessageID: "msg-x"},
				dispatchResponse: eriwire.DispatchEnqueueResponse{Status: "ok", DispatchID: "msg-x"},
			}
			calls := 0
			mock.onDispatch = func() {
				calls++
				if calls == 1 {
					cancel()
				}
			}
			NewTicker(db, mock, 30*time.Second).EvaluateDueSchedules(evalCtx, now)

			if got := len(mock.dispatched) + len(mock.dispatchedRequests); got != 1 {
				t.Errorf("dispatched = %d, want exactly 1 before cancellation", got)
			}
			var secondRuns int
			if err := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM schedule_runs WHERE schedule_id = $1", seedIDs[1]).Scan(&secondRuns); err != nil {
				t.Fatalf("counting second schedule runs failed: %v", err)
			}
			if secondRuns != 0 {
				t.Errorf("second schedule has %d runs, want 0 (sweep should stop)", secondRuns)
			}
		})
	}
}

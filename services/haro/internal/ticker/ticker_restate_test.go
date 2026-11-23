package ticker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/mock"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/services/haro/internal/dispatch"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
)

type mockRunContext struct {
	context.Context
}

func (mockRunContext) Log() *slog.Logger         { return slog.Default() }
func (mockRunContext) Request() *restate.Request { return &restate.Request{ID: "req-1"} }

func setupObjectMockContext(t *testing.T, key string, initialGen int64) (*mocks.MockContext, *mocks.MockClient) {
	t.Helper()
	mc := mocks.NewMockContext(t)
	mc.On("Key").Return(key).Maybe()
	mc.On("Request").Return(&restate.Request{ID: "inv-" + key}).Maybe()

	currentGen := initialGen
	mc.On("Get", "gen", mock.Anything).Run(func(args mock.Arguments) {
		if outPtr, ok := args.Get(1).(*int64); ok && outPtr != nil {
			*outPtr = currentGen
		}
	}).Return(true, nil).Maybe()

	mc.On("Set", "gen", mock.Anything).Run(func(args mock.Arguments) {
		if val, ok := args.Get(1).(int64); ok {
			currentGen = val
		}
	}).Return().Maybe()

	var armedDueAt string
	mc.On("Get", "armed_due_at", mock.Anything).Run(func(args mock.Arguments) {
		if outPtr, ok := args.Get(1).(*string); ok && outPtr != nil {
			*outPtr = armedDueAt
		}
	}).Return(true, nil).Maybe()

	mc.On("Set", "armed_due_at", mock.Anything).Run(func(args mock.Arguments) {
		if val, ok := args.Get(1).(string); ok {
			armedDueAt = val
		}
	}).Return().Maybe()

	mc.On("Clear", mock.Anything).Run(func(args mock.Arguments) {
		if key, ok := args.Get(0).(string); ok && key == "armed_due_at" {
			armedDueAt = ""
		}
	}).Return().Maybe()

	mc.On("ClearAll").Run(func(mock.Arguments) {
		currentGen, armedDueAt = 0, ""
	}).Return().Maybe()

	runHandler := func(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
		res, err := fn(mockRunContext{Context: context.Background()})
		if err != nil {
			if restate.IsTerminalError(err) {
				return restate.AsTerminalError(err)
			}
			// Real Restate retries a non-terminal Run error; the mock surfaces it once.
			return restate.ToTerminalError(err, restate.WithErrorCode(mockRetryableCode))
		}
		if output != nil && res != nil {
			outVal := reflect.ValueOf(output)
			resVal := reflect.ValueOf(res)
			if outVal.Kind() == reflect.Pointer && !outVal.IsNil() && outVal.Elem().Type() == resVal.Type() {
				outVal.Elem().Set(resVal)
			}
		}
		return nil
	}
	mc.On("Run", mock.Anything, mock.Anything).Return(runHandler).Maybe()
	mc.On("Run", mock.Anything, mock.Anything, mock.Anything).Return(runHandler).Maybe()

	mClient := mocks.NewMockClient(t)
	mInv := mocks.NewMockInvocation(t)
	mc.On("Object", mock.Anything, mock.Anything, mock.Anything).Return(mClient).Maybe()
	mClient.On("Send", mock.Anything).Return(mInv).Maybe()
	mClient.On("Send", mock.Anything, mock.Anything).Return(mInv).Maybe()
	mClient.On("Send", mock.Anything, mock.Anything, mock.Anything).Return(mInv).Maybe()

	return mc, mClient
}

// fireKeys returns the idempotency keys of every Fire self-send recorded on client.
func fireKeys(client *mocks.MockClient) []string {
	var keys []string
	for _, call := range client.Calls {
		if call.Method != "Send" {
			continue
		}
		for _, arg := range call.Arguments[1:] {
			if text := strings.Trim(fmt.Sprint(arg), "{}"); strings.HasPrefix(text, "haro:fire:") {
				keys = append(keys, text)
			}
		}
	}
	return keys
}

// seedArmSchedule inserts a schedule of seedType ("cron", "oneshot", "heartbeat"; "" seeds nothing) due at runAt.
func seedArmSchedule(ctx context.Context, t *testing.T, db *sql.DB, seedType, scheduleID string, runAt time.Time, enabled bool) {
	t.Helper()
	switch seedType {
	case "cron":
		if err := store.InsertCronSchedule(ctx, db, model.CronSchedule{
			ID:        scheduleID,
			TargetID:  "100000000000000001",
			CronExpr:  "0 * * * *",
			Prompt:    "Check hourly",
			Timezone:  "UTC",
			NextRunAt: runAt,
			Enabled:   enabled,
		}); err != nil {
			t.Fatalf("InsertCronSchedule failed: %v", err)
		}
	case "oneshot":
		if err := store.InsertOneShotSchedule(ctx, db, model.OneShotSchedule{
			ID:       scheduleID,
			ThreadID: "100000000000000002",
			Prompt:   "Remind me",
			RunAt:    runAt,
		}); err != nil {
			t.Fatalf("InsertOneShotSchedule failed: %v", err)
		}
	case "heartbeat":
		if err := store.InsertHeartbeatSchedule(ctx, db, model.HeartbeatSchedule{
			ID:        scheduleID,
			TargetID:  "100000000000000003",
			CronExpr:  "*/5 * * * *",
			Prompt:    "Heartbeat check",
			Timezone:  "UTC",
			NextRunAt: runAt,
			Enabled:   enabled,
		}); err != nil {
			t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
		}
	}
}

func TestHaroSchedule_Arm(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name         string
		scheduleType string
		seedType     string
		enabled      bool
		future       bool
		wantArmed    bool
		defaultClock bool
		wantCode     int
	}{
		{
			name:         "existing enabled cron schedule arms future fire",
			scheduleType: model.ScheduleTypeCron,
			seedType:     "cron",
			enabled:      true,
			future:       true,
			wantArmed:    true,
		},
		{
			name:         "existing enabled one-shot schedule arms past fire with 0 delay",
			scheduleType: model.ScheduleTypeOneShot,
			seedType:     "oneshot",
			enabled:      true,
			future:       false,
			wantArmed:    true,
		},
		{
			name:         "existing enabled one-shot schedule arms future fire with delay",
			scheduleType: model.ScheduleTypeOneShot,
			seedType:     "oneshot",
			enabled:      true,
			future:       true,
			wantArmed:    true,
		},
		{
			name:         "existing enabled heartbeat schedule arms future fire",
			scheduleType: model.ScheduleTypeHeartbeat,
			seedType:     "heartbeat",
			enabled:      true,
			future:       true,
			wantArmed:    true,
		},
		{
			name:         "empty scheduleType discovers cron schedule",
			scheduleType: "",
			seedType:     "cron",
			enabled:      true,
			future:       true,
			wantArmed:    true,
		},
		{
			name:         "disabled cron schedule fences and ends terminally with 412",
			scheduleType: model.ScheduleTypeCron,
			seedType:     "cron",
			enabled:      false,
			future:       true,
			wantArmed:    false,
			wantCode:     http.StatusPreconditionFailed,
		},
		{
			name:         "missing schedule clears state and ends terminally with 404",
			scheduleType: model.ScheduleTypeCron,
			seedType:     "",
			wantArmed:    false,
			wantCode:     http.StatusNotFound,
		},
		{
			name:         "cron schedule arms with default clock when nowFn unset",
			scheduleType: model.ScheduleTypeCron,
			seedType:     "cron",
			enabled:      true,
			future:       true,
			wantArmed:    true,
			defaultClock: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTickerTestDB(t)
			ctx := context.Background()
			scheduleID := "arm-" + tc.name

			runAt := now.Add(time.Hour)
			if !tc.future {
				runAt = now.Add(-time.Minute)
			}

			seedArmSchedule(ctx, t, db, tc.seedType, scheduleID, runAt, tc.enabled)

			mc, mClient := setupObjectMockContext(t, scheduleID, 0)
			objCtx := restate.WithMockContext(mc)

			dispatcher := &fakeDispatcher{}
			s := NewHaroSchedule(db, dispatcher)
			if !tc.defaultClock {
				s.nowFn = func() time.Time { return now }
			}

			_, err := s.Arm(objCtx, ArmRequest{ScheduleType: tc.scheduleType})
			if got := terminalCode(err); got != tc.wantCode || (err != nil && tc.wantCode == 0) {
				t.Fatalf("Arm err = %v, want terminal code %d", err, tc.wantCode)
			}

			wantKeys := 0
			if tc.wantArmed {
				wantKeys = 1
			}
			if got := fireKeys(mClient); len(got) != wantKeys {
				t.Errorf("Fire sends = %v, want %d", got, wantKeys)
			}
			if tc.wantCode == http.StatusPreconditionFailed {
				mc.AssertCalled(t, "Set", "gen", int64(1))
			}
			if got := clearedAll(mc); got != (tc.wantCode == http.StatusNotFound) {
				t.Errorf("state cleared = %v, want %v", got, tc.wantCode == http.StatusNotFound)
			}
		})
	}
}

func TestHaroSchedule_Arm_Idempotent(t *testing.T) {
	t.Parallel()
	db := openTickerTestDB(t)
	ctx := context.Background()
	scheduleID := "arm-idempotent"
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)

	if err := store.InsertCronSchedule(ctx, db, model.CronSchedule{
		ID:        scheduleID,
		TargetID:  "100000000000000001",
		CronExpr:  "0 * * * *",
		Prompt:    "Hourly check",
		Timezone:  "UTC",
		NextRunAt: now.Add(time.Hour),
		Enabled:   true,
	}); err != nil {
		t.Fatalf("InsertCronSchedule failed: %v", err)
	}

	mc, mClient := setupObjectMockContext(t, scheduleID, 0)
	objCtx := restate.WithMockContext(mc)

	s := NewHaroSchedule(db, &fakeDispatcher{})
	s.nowFn = func() time.Time { return now }

	// Re-arming the same tick (e.g. after a lost Fire) sends Fire again under the
	// same key, so Restate dedupes a pending Fire and re-creates a lost one. The
	// generation is never bumped, so the pending Fire stays valid.
	for range 2 {
		if _, err := s.Arm(objCtx, ArmRequest{ScheduleType: model.ScheduleTypeCron}); err != nil {
			t.Fatalf("Arm err: %v", err)
		}
	}
	want := []string{"haro:fire:arm-idempotent:0:2026-11-22T11:00:00Z", "haro:fire:arm-idempotent:0:2026-11-22T11:00:00Z"}
	if got := fireKeys(mClient); !reflect.DeepEqual(got, want) {
		t.Errorf("Fire keys = %v, want %v", got, want)
	}
	mc.AssertNotCalled(t, "Set", "gen", mock.Anything)
}

func TestHaroSchedule_LoadScheduleRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// nil DB returns Found: false
	s := NewHaroSchedule(nil, nil)
	row, err := s.loadScheduleRow(ctx, "any", "cron")
	if err != nil || row.Found {
		t.Errorf("expected found: false, nil err, got %v, %v", row.Found, err)
	}

	// closed DB returns DB error for each distinct schedule type branch
	db := openTickerTestDB(t)
	_ = db.Close()
	s2 := NewHaroSchedule(db, nil)
	for _, schedType := range []string{"cron", "one_shot", "heartbeat"} {
		if _, err := s2.loadScheduleRow(ctx, "any", schedType); err == nil {
			t.Errorf("expected error on closed db for %s, got nil", schedType)
		}
	}
}

func TestHaroSchedule_ArmAndFire_ContextErrors(t *testing.T) {
	t.Parallel()
	mc := mocks.NewMockContext(t)
	mc.On("Key").Return("sched-err-1").Maybe()
	mc.On("Get", "gen", mock.Anything).Return(false, restate.TerminalErrorf("storage get failed")).Maybe()
	objCtx := restate.WithMockContext(mc)

	s := NewHaroSchedule(nil, nil)
	if _, err := s.Arm(objCtx, ArmRequest{ScheduleType: "cron"}); err == nil {
		t.Error("expected error from Arm on Get failure")
	}
	if _, err := s.Fire(objCtx, FireRequest{Generation: 1}); err == nil {
		t.Error("expected error from Fire on Get failure")
	}
}

func TestHaroSchedule_Cancel(t *testing.T) {
	t.Parallel()
	mc, mClient := setupObjectMockContext(t, "cancel-test-1", 5)
	objCtx := restate.WithMockContext(mc)
	restate.Set(objCtx, "armed_due_at", "2026-11-22T11:00:00Z")

	if _, err := NewHaroSchedule(nil, nil).Cancel(objCtx, struct{}{}); err != nil {
		t.Fatalf("Cancel unexpected err: %v", err)
	}
	gen, genErr := restate.Get[int64](objCtx, "gen")
	armed, armedErr := restate.Get[string](objCtx, "armed_due_at")
	if genErr != nil || armedErr != nil || gen != 0 || armed != "" || !clearedAll(mc) {
		t.Errorf("after Cancel gen=%d armed=%q cleared=%v (errs %v, %v); want all state cleared", gen, armed, clearedAll(mc), genErr, armedErr)
	}
	if keys := fireKeys(mClient); len(keys) != 0 {
		t.Errorf("Cancel sent Fire %v, want none", keys)
	}
}

// TestHaroSchedule_CancelThenFire runs Cancel (which clears all state), an
// optional re-Arm, then the Fire that was pending before Cancel.
func TestHaroSchedule_CancelThenFire(t *testing.T) {
	t.Parallel()
	tick := time.Date(2026, 11, 22, 11, 0, 0, 0, time.UTC)
	tests := []struct {
		name           string
		id             string
		initialGen     int64
		seed           string
		rearm          bool
		fireDue        time.Time
		wantDispatched int
		wantCode       int
		wantFireKeys   []string
	}{
		{name: "re-enabled for the same tick: pending older-generation Fire still fires", id: "c-same", initialGen: 2, seed: "enabled", fireDue: tick, wantDispatched: 1, wantFireKeys: []string{"haro:fire:c-same:0:2026-11-22T12:00:00Z"}},
		{name: "deleted schedule: pending Fire ends terminally with 404", id: "c-gone", initialGen: 2, fireDue: tick, wantCode: http.StatusNotFound},
		{name: "disabled schedule: pending Fire ends terminally with 412", id: "c-off", initialGen: 2, seed: "disabled", fireDue: tick, wantCode: http.StatusPreconditionFailed},
		{name: "re-armed for a new tick: pending Fire for the old tick is a no-op", id: "c-moved", initialGen: 2, seed: "enabled", rearm: true, fireDue: tick.Add(-30 * time.Minute), wantFireKeys: []string{"haro:fire:c-moved:0:2026-11-22T11:00:00Z"}},
		{name: "re-armed for the same tick: Arm reuses the generation-0 Fire key", id: "c-rearm", seed: "enabled", rearm: true, fireDue: tick, wantDispatched: 1, wantFireKeys: []string{"haro:fire:c-rearm:0:2026-11-22T11:00:00Z", "haro:fire:c-rearm:0:2026-11-22T12:00:00Z"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTickerTestDB(t)
			if tc.seed != "" {
				if err := store.InsertCronSchedule(context.Background(), db, model.CronSchedule{ID: tc.id, TargetID: "100000000000000001", CronExpr: "0 * * * *", Prompt: "Hourly check", Timezone: "UTC", NextRunAt: tick, Enabled: tc.seed == "enabled"}); err != nil {
					t.Fatalf("InsertCronSchedule failed: %v", err)
				}
			}
			mc, mClient := setupObjectMockContext(t, tc.id, tc.initialGen)
			objCtx := restate.WithMockContext(mc)
			restate.Set(objCtx, "armed_due_at", tc.fireDue.Format(time.RFC3339Nano))

			disp := &fakeDispatcher{response: dispatch.PromptDispatchResponse{Status: "ok", ChannelID: "100000000000000001", MessageID: "msg-cancel"}}
			s := NewHaroSchedule(db, disp)
			s.nowFn = func() time.Time { return tc.fireDue }

			if _, err := s.Cancel(objCtx, struct{}{}); err != nil {
				t.Fatalf("Cancel err: %v", err)
			}
			if tc.rearm {
				if _, err := s.Arm(objCtx, ArmRequest{ScheduleType: model.ScheduleTypeCron}); err != nil {
					t.Fatalf("Arm err: %v", err)
				}
			}
			_, err := s.Fire(objCtx, FireRequest{Generation: tc.initialGen, ScheduleType: model.ScheduleTypeCron, DueAt: tc.fireDue})
			if got := terminalCode(err); got != tc.wantCode || (err != nil && tc.wantCode == 0) {
				t.Fatalf("Fire err = %v, want terminal code %d", err, tc.wantCode)
			}
			if len(disp.dispatched) != tc.wantDispatched {
				t.Errorf("dispatched %d prompts, want %d", len(disp.dispatched), tc.wantDispatched)
			}
			if got := fireKeys(mClient); !reflect.DeepEqual(got, tc.wantFireKeys) {
				t.Errorf("Fire keys = %v, want %v", got, tc.wantFireKeys)
			}
		})
	}
}

// clearedAll reports whether the handler cleared all object state.
func clearedAll(mc *mocks.MockContext) bool {
	for _, call := range mc.Calls {
		if call.Method == "ClearAll" {
			return true
		}
	}
	return false
}

// mockRetryableCode marks a retryable Run error the object mock surfaced once
// (real Restate would keep retrying it).
const mockRetryableCode = 599

// terminalCode returns the Restate terminal code of err, or 0 when err is nil or retryable.
func terminalCode(err error) int {
	if err == nil || !restate.IsTerminalError(err) {
		return 0
	}
	if code := int(restate.AsTerminalError(err).Code()); code != mockRetryableCode {
		return code
	}
	return 0
}

func TestHaroSchedule_RecordRunStep(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		closedDB bool
		skipped  bool
		status   string
		wantErr  bool
		wantCode int
	}{
		{name: "closed db is a retryable error", closedDB: true, status: model.StatusCompleted, wantErr: true},
		{name: "skipped active writes nothing", closedDB: true, skipped: true, status: model.StatusSkippedActive},
		{name: "failed run is recorded", status: model.StatusFailed},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTickerTestDB(t)
			if tc.closedDB {
				_ = db.Close()
			}
			s := NewHaroSchedule(db, nil)
			now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
			rc := mockRunContext{Context: context.Background()}
			dispRes := DispatchStepResult{Status: tc.status, SkippedActive: tc.skipped, DispatchedAt: now, Error: "boom"}
			loaded := ScheduleRow{ScheduleType: model.ScheduleTypeCron, TargetID: "100000000000000001"}
			runID := deterministicScheduleRunID(model.ScheduleTypeCron, "rec-"+tc.name, now)
			err := s.recordRunStep(rc, loaded, model.ScheduleTypeCron, "rec-"+tc.name, runID, dispRes, now)
			if (err != nil) != tc.wantErr {
				t.Fatalf("recordRunStep err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := terminalCode(err); got != tc.wantCode {
				t.Errorf("terminal code = %d, want %d", got, tc.wantCode)
			}
		})
	}
}

func TestHaroSchedule_FinalizeStep(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		schedType   string
		closedDB    bool
		wantErr     bool
		wantCode    int
		wantOneShot bool
	}{
		{name: "delete oneshot closed db is retryable", schedType: model.ScheduleTypeOneShot, closedDB: true, wantErr: true},
		{name: "advance recurring closed db is retryable", schedType: model.ScheduleTypeCron, closedDB: true, wantErr: true},
		{name: "advance deleted cron is terminal 404", schedType: model.ScheduleTypeCron, wantErr: true, wantCode: http.StatusNotFound},
		{name: "advance deleted heartbeat is terminal 404", schedType: model.ScheduleTypeHeartbeat, wantErr: true, wantCode: http.StatusNotFound},
		{name: "already deleted oneshot finalizes", schedType: model.ScheduleTypeOneShot, wantOneShot: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTickerTestDB(t)
			if tc.closedDB {
				_ = db.Close()
			}
			s := NewHaroSchedule(db, nil)
			rc := mockRunContext{Context: context.Background()}
			loaded := ScheduleRow{ScheduleType: tc.schedType, CronExpr: "0 * * * *", Timezone: "UTC"}
			res, err := s.finalizeStep(rc, loaded, tc.schedType, "missing-"+tc.name, time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC))
			if (err != nil) != tc.wantErr {
				t.Fatalf("finalizeStep err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := terminalCode(err); got != tc.wantCode {
				t.Errorf("terminal code = %d, want %d", got, tc.wantCode)
			}
			if res.IsOneShot != tc.wantOneShot {
				t.Errorf("IsOneShot = %v, want %v", res.IsOneShot, tc.wantOneShot)
			}
		})
	}
}

func TestBackfillActiveSchedules_PostErrors(t *testing.T) {
	t.Parallel()
	db := openTickerTestDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := store.InsertCronSchedule(ctx, db, model.CronSchedule{
		ID:        "c-backfill-fail",
		TargetID:  "100000000000000001",
		CronExpr:  "0 * * * *",
		Prompt:    "Cron prompt",
		NextRunAt: now.Add(time.Hour),
		Enabled:   true,
	}); err != nil {
		t.Fatalf("InsertCronSchedule failed: %v", err)
	}

	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	tk := NewTicker(db, nil)
	tk.SetRestateConfig("9080", srv.URL, "", "")
	tk.httpClient = srv.Client()

	if err := tk.BackfillActiveSchedules(ctx); err == nil {
		t.Fatal("expected error on post failure, got nil")
	}
	if got := hits.Load(); got != 1 {
		t.Errorf("expected 1 post attempt, got %d", got)
	}
}

type fireTestCase struct {
	name           string
	scheduleType   string
	seedType       string
	generation     int64
	reqGen         int64
	enabled        bool
	stale          bool
	hasActiveHB    bool
	skippedActive  bool
	nilDispatcher  bool
	dispatchErr    error
	wantErr        bool
	wantDispatched bool
	wantDeleted    bool
	wantFenced     bool
	wantCleared    bool
	wantCode       int
	wantRunStatus  string
	wantRearmed    bool
	superseded     bool
}

func seedScheduleForFireTest(ctx context.Context, t *testing.T, db *sql.DB, tc fireTestCase, now time.Time, scheduleID string, runAt time.Time) {
	t.Helper()
	switch tc.seedType {
	case "cron":
		cronExpr := "*/5 * * * *"
		if strings.Contains(tc.name, "invalid cron expr") {
			cronExpr = "not-a-cron"
		}
		if err := store.InsertCronSchedule(ctx, db, model.CronSchedule{
			ID:        scheduleID,
			TargetID:  "100000000000000001",
			CronExpr:  cronExpr,
			Prompt:    "Prompt cron",
			Timezone:  "UTC",
			NextRunAt: runAt,
			Enabled:   tc.enabled,
		}); err != nil {
			t.Fatalf("InsertCronSchedule failed: %v", err)
		}
	case "oneshot":
		if err := store.InsertOneShotSchedule(ctx, db, model.OneShotSchedule{
			ID:       scheduleID,
			ThreadID: "100000000000000002",
			Prompt:   "Prompt oneshot",
			RunAt:    runAt,
		}); err != nil {
			t.Fatalf("InsertOneShotSchedule failed: %v", err)
		}
	case "heartbeat":
		cronExpr := "*/5 * * * *"
		if strings.Contains(tc.name, "invalid cron expr") {
			cronExpr = "not-a-cron"
		}
		if err := store.InsertHeartbeatSchedule(ctx, db, model.HeartbeatSchedule{
			ID:        scheduleID,
			TargetID:  "100000000000000003",
			CronExpr:  cronExpr,
			Prompt:    "Prompt heartbeat",
			Timezone:  "UTC",
			NextRunAt: runAt,
			Enabled:   tc.enabled,
		}); err != nil {
			t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
		}
		if tc.hasActiveHB {
			if err := store.CreateScheduleRun(ctx, db, model.ScheduleRun{
				ID:           "run-active-" + scheduleID,
				ScheduleID:   scheduleID,
				ScheduleType: model.ScheduleTypeHeartbeat,
				TargetID:     "100000000000000003",
				ThreadID:     "thread-active",
				Status:       "running",
				StartedAt:    now.Add(-2 * time.Minute),
			}); err != nil {
				t.Fatalf("CreateScheduleRun failed: %v", err)
			}
		}
	}
}

func verifyFireDispatches(t *testing.T, tc fireTestCase, disp dispatch.PromptDispatcher) {
	t.Helper()
	if tc.nilDispatcher || disp == nil {
		return
	}
	fDisp, ok := disp.(*fakeDispatcher)
	if !ok {
		t.Fatal("expected *fakeDispatcher")
	}
	dispatchedCount := len(fDisp.dispatched) + len(fDisp.dispatchedRequests)
	if tc.wantDispatched && dispatchedCount == 0 {
		t.Error("expected prompt dispatch, got none")
	}
	if !tc.wantDispatched && dispatchedCount > 0 {
		t.Errorf("expected no prompt dispatch, got %d", dispatchedCount)
	}
}

func buildFireDispatcher(tc fireTestCase, runAt *time.Time) dispatch.PromptDispatcher {
	if tc.nilDispatcher {
		return nil
	}
	hbStatus := model.StatusCompleted
	if tc.skippedActive {
		hbStatus = eriwire.StatusSkippedActive
	}
	fDisp := &fakeDispatcher{
		response: dispatch.PromptDispatchResponse{
			Status:    model.StatusCompleted,
			ChannelID: "100000000000000001",
			MessageID: "msg-1",
		},
		dispatchResponse: eriwire.DispatchEnqueueResponse{
			Status:     hbStatus,
			DispatchID: "disp-hb-1",
		},
		dispatchErr: tc.dispatchErr,
		err:         tc.dispatchErr,
	}
	if tc.name == "cron response with empty channel and message ID and zero dueAt" {
		fDisp.response.ChannelID = ""
		fDisp.response.MessageID = ""
		*runAt = time.Time{}
	}
	return fDisp
}

func assertFireOutcome(ctx context.Context, t *testing.T, tc fireTestCase, err error, mc *mocks.MockContext, mClient *mocks.MockClient, db *sql.DB, scheduleID string, disp dispatch.PromptDispatcher) {
	t.Helper()
	if tc.wantErr && err == nil {
		t.Fatal("expected error, got nil")
	}
	if !tc.wantErr && err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := terminalCode(err); got != tc.wantCode {
		t.Errorf("terminal code = %d, want %d (err %v)", got, tc.wantCode, err)
	}

	verifyFireDispatches(t, tc, disp)

	if tc.wantRunStatus != "" {
		run, runErr := store.GetScheduleRunByDispatchID(ctx, db, deterministicScheduleRunID(tc.scheduleType, scheduleID, time.Date(2026, 11, 22, 9, 59, 0, 0, time.UTC)))
		if runErr != nil {
			t.Fatalf("expected recorded run: %v", runErr)
		}
		if run.Status != tc.wantRunStatus {
			t.Errorf("run status = %q, want %q", run.Status, tc.wantRunStatus)
		}
	}
	if tc.wantRearmed && len(fireKeys(mClient)) != 1 {
		t.Errorf("expected next Fire to be armed, got %v", fireKeys(mClient))
	}

	assertFireState(t, tc, mc)

	if tc.wantDeleted && tc.seedType == "oneshot" {
		_, getErr := store.GetOneShotSchedule(ctx, db, scheduleID)
		if getErr == nil {
			t.Error("expected one-shot schedule to be deleted, still found")
		}
	}

	if !tc.wantErr && tc.wantDispatched && (tc.scheduleType == model.ScheduleTypeCron || tc.scheduleType == model.ScheduleTypeHeartbeat) && len(fireKeys(mClient)) == 0 {
		t.Error("expected next Fire to be armed, got none")
	}
}

// assertFireState checks whether Fire fenced the generation or cleared all state.
func assertFireState(t *testing.T, tc fireTestCase, mc *mocks.MockContext) {
	t.Helper()
	if tc.wantFenced {
		mc.AssertCalled(t, "Set", "gen", tc.generation+1)
	}
	if got := clearedAll(mc); got != tc.wantCleared {
		t.Errorf("state cleared = %v, want %v", got, tc.wantCleared)
	}
}

func fireSkipAndErrorTestCases() []fireTestCase {
	return []fireTestCase{
		{
			name:           "generation mismatch is no-op",
			scheduleType:   model.ScheduleTypeCron,
			seedType:       "cron",
			generation:     2,
			reqGen:         1,
			enabled:        true,
			wantDispatched: false,
		},
		{
			name:         "schedule missing clears state and ends terminally with 404",
			scheduleType: model.ScheduleTypeCron,
			seedType:     "",
			generation:   1,
			reqGen:       1,
			wantCleared:  true,
			wantErr:      true,
			wantCode:     http.StatusNotFound,
		},
		{
			name:         "schedule disabled is fenced and ends terminally with 412",
			scheduleType: model.ScheduleTypeCron,
			seedType:     "cron",
			generation:   1,
			reqGen:       1,
			enabled:      false,
			wantFenced:   true,
			wantErr:      true,
			wantCode:     http.StatusPreconditionFailed,
		},
		{
			name:           "stale cron schedule >24h records skipped_stale and advances without dispatching",
			scheduleType:   model.ScheduleTypeCron,
			seedType:       "cron",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			stale:          true,
			wantDispatched: false,
		},
		{
			name:           "stale heartbeat schedule >24h records skipped_stale and advances without dispatching",
			scheduleType:   model.ScheduleTypeHeartbeat,
			seedType:       "heartbeat",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			stale:          true,
			wantDispatched: false,
		},
		{
			name:           "stale oneshot schedule >24h records skipped_stale and deletes",
			scheduleType:   model.ScheduleTypeOneShot,
			seedType:       "oneshot",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			stale:          true,
			wantDispatched: false,
			wantDeleted:    true,
			wantCleared:    true,
		},
		{
			name:           "heartbeat with active run records skipped_active and advances without dispatching",
			scheduleType:   model.ScheduleTypeHeartbeat,
			seedType:       "heartbeat",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			hasActiveHB:    true,
			wantDispatched: false,
		},
		{
			name:           "heartbeat dispatch returns skipped_active status",
			scheduleType:   model.ScheduleTypeHeartbeat,
			seedType:       "heartbeat",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			skippedActive:  true,
			wantDispatched: true,
		},
		{
			name:          "cron with nil dispatcher returns error",
			scheduleType:  model.ScheduleTypeCron,
			seedType:      "cron",
			generation:    1,
			reqGen:        1,
			enabled:       true,
			nilDispatcher: true,
			wantErr:       true,
		},
		{
			name:          "heartbeat with nil dispatcher returns error",
			scheduleType:  model.ScheduleTypeHeartbeat,
			seedType:      "heartbeat",
			generation:    1,
			reqGen:        1,
			enabled:       true,
			nilDispatcher: true,
			wantErr:       true,
		},
		{
			name:           "dispatch error returns error to retry Step 2",
			scheduleType:   model.ScheduleTypeCron,
			seedType:       "cron",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			dispatchErr:    errors.New("eri connection failed"),
			wantErr:        true,
			wantDispatched: true,
		},
		{
			name:           "heartbeat dispatch error returns error",
			scheduleType:   model.ScheduleTypeHeartbeat,
			seedType:       "heartbeat",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			dispatchErr:    errors.New("heartbeat eri dispatch failed"),
			wantErr:        true,
			wantDispatched: true,
		},
	}
}

// firePermanentFailureTestCases cover superseded ticks and eri 4xx rejections, which
// must neither retry nor strand the schedule.
func firePermanentFailureTestCases() []fireTestCase {
	return []fireTestCase{
		{
			name:         "fire for a superseded due tick is a no-op",
			scheduleType: model.ScheduleTypeCron,
			seedType:     "cron",
			generation:   1,
			reqGen:       1,
			enabled:      true,
			superseded:   true,
		},
		{
			name:           "cron eri 4xx records a failed run and arms the next tick",
			scheduleType:   model.ScheduleTypeCron,
			seedType:       "cron",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			dispatchErr:    restate.ToTerminalError(errors.New("restate prompt dispatch error (400): bad channel"), restate.WithErrorCode(http.StatusBadRequest)),
			wantDispatched: true,
			wantRunStatus:  model.StatusFailed,
			wantRearmed:    true,
		},
		{
			name:           "heartbeat eri 4xx records a failed run and arms the next tick",
			scheduleType:   model.ScheduleTypeHeartbeat,
			seedType:       "heartbeat",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			dispatchErr:    restate.ToTerminalError(errors.New("restate dispatch error (404): unknown agent"), restate.WithErrorCode(http.StatusNotFound)),
			wantDispatched: true,
			wantRearmed:    true,
		},
		{
			name:           "one-shot eri 4xx records a failed run and deletes the schedule",
			scheduleType:   model.ScheduleTypeOneShot,
			seedType:       "oneshot",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			dispatchErr:    restate.ToTerminalError(errors.New("restate prompt dispatch error (400): bad channel"), restate.WithErrorCode(http.StatusBadRequest)),
			wantDispatched: true,
			wantRunStatus:  model.StatusFailed,
			wantDeleted:    true,
			wantCleared:    true,
		},
	}
}

func fireSuccessTestCases() []fireTestCase {
	return []fireTestCase{
		{
			name:           "cron response with empty channel and message ID and zero dueAt",
			scheduleType:   model.ScheduleTypeCron,
			seedType:       "cron",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			wantDispatched: true,
		},
		{
			name:           "cron with invalid cron expr in DB advances with 24h fallback",
			scheduleType:   model.ScheduleTypeCron,
			seedType:       "cron",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			wantDispatched: true,
		},
		{
			name:           "heartbeat with invalid cron expr in DB advances with 24h fallback",
			scheduleType:   model.ScheduleTypeHeartbeat,
			seedType:       "heartbeat",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			wantDispatched: true,
		},
		{
			name:           "one-shot schedule success deletes schedule and clears state",
			scheduleType:   model.ScheduleTypeOneShot,
			seedType:       "oneshot",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			wantDispatched: true,
			wantDeleted:    true,
			wantCleared:    true,
		},
		{
			name:           "cron schedule success advances next_run_at and arms next Fire",
			scheduleType:   model.ScheduleTypeCron,
			seedType:       "cron",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			wantDispatched: true,
		},
		{
			name:           "heartbeat schedule success advances next_run_at and arms next Fire",
			scheduleType:   model.ScheduleTypeHeartbeat,
			seedType:       "heartbeat",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			wantDispatched: true,
		},
	}
}

func fireTestCases() []fireTestCase {
	cases := append(fireSkipAndErrorTestCases(), firePermanentFailureTestCases()...)
	return append(cases, fireSuccessTestCases()...)
}

func TestHaroSchedule_Fire(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)

	for _, tc := range fireTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTickerTestDB(t)
			ctx := context.Background()
			scheduleID := "fire-" + tc.name

			runAt := now.Add(-time.Minute)
			if tc.stale {
				runAt = now.Add(-25 * time.Hour)
			}

			seedScheduleForFireTest(ctx, t, db, tc, now, scheduleID, runAt)

			mc, mClient := setupObjectMockContext(t, scheduleID, tc.generation)
			objCtx := restate.WithMockContext(mc)
			if tc.superseded {
				restate.Set(objCtx, "armed_due_at", runAt.Add(time.Hour).Format(time.RFC3339Nano))
			}

			disp := buildFireDispatcher(tc, &runAt)
			s := NewHaroSchedule(db, disp)
			s.nowFn = func() time.Time { return now }

			_, err := s.Fire(objCtx, FireRequest{
				Generation:   tc.reqGen,
				ScheduleType: tc.scheduleType,
				DueAt:        runAt,
			})

			assertFireOutcome(ctx, t, tc, err, mc, mClient, db, scheduleID, disp)
		})
	}
}

func TestRegisterDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		statusCode int
		emptyAdmin bool
		badURL     bool
		wantErr    bool
	}{
		{name: "empty admin url is no-op", emptyAdmin: true, wantErr: false},
		{name: "successful registration 200", statusCode: http.StatusOK, wantErr: false},
		{name: "successful registration 201", statusCode: http.StatusCreated, wantErr: false},
		{name: "failed registration 500", statusCode: http.StatusInternalServerError, wantErr: true},
		{name: "invalid url returns error", badURL: true, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/deployments" {
					http.NotFound(w, r)
					return
				}
				if ct := r.Header.Get("Content-Type"); ct != "application/json" {
					t.Errorf("Content-Type = %q, want application/json", ct)
				}
				var req struct {
					URI   string `json:"uri"`
					Force bool   `json:"force"`
				}
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Errorf("decode request body err = %v", err)
				}
				if req.URI != "http://haro:9080" || !req.Force {
					t.Errorf("payload = %+v, want uri=http://haro:9080, force=true", req)
				}
				w.WriteHeader(tc.statusCode)
			}))
			defer srv.Close()

			adminURL := srv.URL
			if tc.badURL {
				adminURL = "://bad-url"
			} else if tc.emptyAdmin {
				adminURL = ""
			}

			err := RegisterDeployment(context.Background(), adminURL, "http://haro:9080", srv.Client())
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestBackfillActiveSchedules(t *testing.T) {
	t.Parallel()
	db := openTickerTestDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	for _, c := range []model.CronSchedule{
		{ID: "c-backfill-1", TargetID: "100000000000000001", CronExpr: "0 * * * *", Prompt: "Cron prompt", NextRunAt: now.Add(time.Hour), Enabled: true},
		{ID: "c-backfill-disabled", TargetID: "100000000000000001", CronExpr: "0 * * * *", Prompt: "Cron prompt disabled", NextRunAt: now.Add(time.Hour), Enabled: false},
	} {
		if err := store.InsertCronSchedule(ctx, db, c); err != nil {
			t.Fatalf("InsertCronSchedule failed: %v", err)
		}
	}
	if err := store.InsertOneShotSchedule(ctx, db, model.OneShotSchedule{ID: "o-backfill-1", ThreadID: "100000000000000002", Prompt: "OneShot prompt", RunAt: now.Add(time.Hour)}); err != nil {
		t.Fatalf("InsertOneShotSchedule failed: %v", err)
	}
	for _, hb := range []model.HeartbeatSchedule{
		{ID: "hb-backfill-1", TargetID: "100000000000000003", CronExpr: "*/5 * * * *", Prompt: "Heartbeat prompt", NextRunAt: now.Add(time.Hour), Enabled: true},
		{ID: "hb-backfill-disabled", TargetID: "100000000000000003", CronExpr: "*/5 * * * *", Prompt: "Heartbeat disabled", NextRunAt: now.Add(time.Hour), Enabled: false},
	} {
		if err := store.InsertHeartbeatSchedule(ctx, db, hb); err != nil {
			t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
		}
	}

	called := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	tk := NewTicker(db, nil)
	tk.SetRestateConfig("9080", srv.URL, "", "")
	tk.httpClient = srv.Client()

	err := tk.BackfillActiveSchedules(ctx)
	if err != nil {
		t.Fatalf("BackfillActiveSchedules err: %v", err)
	}
	if called != 3 {
		t.Errorf("expected 3 arm calls, got %d", called)
	}

	// Failure branch on closed DB
	_ = db.Close()
	if err := tk.BackfillActiveSchedules(ctx); err == nil {
		t.Error("expected error on closed db")
	}
}

func TestTicker_StartRestateServer_Error(t *testing.T) {
	t.Parallel()
	db := openTickerTestDB(t)
	tk := NewTicker(db, &fakeDispatcher{})

	_, err := tk.startRestateServer(context.Background(), "invalid-port-name")
	if err == nil {
		t.Error("expected listen error on invalid port, got nil")
	}
}

// fireHeartbeatAt runs one HaroSchedule.Fire for a heartbeat due at dueAt with the clock at now.
func fireHeartbeatAt(t *testing.T, s *HaroSchedule, scheduleID string, dueAt, now time.Time) {
	t.Helper()
	mc, client := setupObjectMockContext(t, scheduleID, 1)
	s.nowFn = func() time.Time { return now }
	if _, err := s.Fire(restate.WithMockContext(mc), FireRequest{Generation: 1, ScheduleType: model.ScheduleTypeHeartbeat, DueAt: dueAt}); err != nil {
		t.Fatalf("Fire(%s) error = %v", dueAt.Format(time.RFC3339), err)
	}
	// Every heartbeat tick, dispatched or skipped, re-arms the next Fire.
	if len(fireKeys(client)) != 1 {
		t.Fatalf("Fire(%s) re-armed %v, want exactly one next Fire", dueAt.Format(time.RFC3339), fireKeys(client))
	}
}

func TestTicker_Start(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		nilTicker bool
		noDB      bool
		port      string
		wantErr   string
	}{
		{name: "nil ticker returns immediately", nilTicker: true},
		{name: "missing database is an error", noDB: true, port: "0", wantErr: "database is required"},
		{name: "invalid restate port is an error", port: "invalid-port-name", wantErr: "restate server listen"},
		{name: "registers, arms and serves until cancelled", port: "0"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			registered := make(chan struct{}, 1)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/deployments" {
					select {
					case registered <- struct{}{}:
					default:
					}
				}
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(srv.Close)

			var tk *Ticker
			if !tc.nilTicker {
				db := openTickerTestDB(t)
				if tc.noDB {
					db = nil
				}
				tk = NewTicker(db, &fakeDispatcher{})
				tk.SetRestateConfig(tc.port, srv.URL, srv.URL, "http://localhost:9080")
				tk.httpClient = srv.Client()
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- tk.Start(ctx) }()

			if tc.port == "0" && !tc.noDB {
				<-registered
				cancel()
			}
			err := <-done
			if tc.wantErr == "" && err != nil {
				t.Fatalf("Start err = %v, want nil", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("Start err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// startupStub is a Restate admin + ingress stand-in that fails the first
// regFailures registrations and armFailures arm posts (-1 fails every one),
// calling onRegFailure on each failed registration.
type startupStub struct {
	regFailures  int32
	armFailures  int32
	onRegFailure func()
	regAttempts  atomic.Int32
	armPosts     atomic.Int32
}

func (s *startupStub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/deployments" {
		s.serveRegister(w)
		return
	}
	if n := s.armPosts.Add(1); s.armFailures < 0 || n <= s.armFailures {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusAccepted)
}

func (s *startupStub) serveRegister(w http.ResponseWriter) {
	if n := s.regAttempts.Add(1); s.regFailures >= 0 && n > s.regFailures {
		w.WriteHeader(http.StatusOK)
		return
	}
	s.onRegFailure()
	w.WriteHeader(http.StatusInternalServerError)
}

// TestTicker_ArmOnStartup drives the startup phase against a stub Restate
// admin/ingress that fails the first regFailures registrations and
// armFailures arm posts (-1 fails every one).
func TestTicker_ArmOnStartup(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	tests := []struct {
		name            string
		regFailures     int32
		armFailures     int32
		serverErr       bool
		cancelled       bool
		sabotageSQL     string
		wantErr         string
		wantRegAttempts int32
		wantArmPosts    int32
		wantPruned      bool
	}{
		{name: "registration retried until accepted, then arms and prunes", regFailures: 2, wantRegAttempts: 3, wantArmPosts: 1, wantPruned: true},
		{name: "backfill retried after an ingress failure without re-registering", armFailures: 1, wantRegAttempts: 1, wantArmPosts: 2, wantPruned: true},
		{name: "restate server failure during retries is returned", regFailures: -1, serverErr: true, wantErr: "restate server error", wantRegAttempts: 1},
		{name: "prune failure is logged and startup still completes", sabotageSQL: failTriggerSQL("schedule_runs", "DELETE", "forced prune failure"), wantRegAttempts: 1, wantArmPosts: 1},
		{name: "cancelled context during retries stops without pruning", regFailures: -1, cancelled: true, wantRegAttempts: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTickerTestDB(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			seedStartupRows(ctx, t, db, now, tc.sabotageSQL)

			serverErr := make(chan error, 1)
			onRegFailure := func() {}
			if tc.serverErr {
				onRegFailure = func() { reportOnce(serverErr, errors.New("listener closed")) }
			}
			if tc.cancelled {
				onRegFailure = cancel
			}
			stub := &startupStub{regFailures: tc.regFailures, armFailures: tc.armFailures, onRegFailure: onRegFailure}
			srv := httptest.NewServer(stub)
			t.Cleanup(srv.Close)

			tk := NewTicker(db, &fakeDispatcher{})
			tk.SetRestateConfig("9080", srv.URL, srv.URL, "http://localhost:9080")
			tk.httpClient = srv.Client()
			tk.startupBackoff = time.Millisecond
			if tc.regFailures < 0 {
				// Never-ending failures must stop on the server error or cancellation, not a retry.
				tk.startupBackoff = time.Hour
			}

			if err := tk.armOnStartup(ctx, serverErr); !errMatches(err, tc.wantErr) {
				t.Fatalf("armOnStartup err = %v, want %q", err, tc.wantErr)
			}
			if got := stub.regAttempts.Load(); got != tc.wantRegAttempts {
				t.Errorf("registration attempts = %d, want %d", got, tc.wantRegAttempts)
			}
			if got := stub.armPosts.Load(); got != tc.wantArmPosts {
				t.Errorf("arm posts = %d, want %d", got, tc.wantArmPosts)
			}
			assertStartupPrune(t, db, tc.wantPruned)
		})
	}
}

// seedStartupRows inserts one active cron plus a 40-day-old and a 1-hour-old run.
// A non-empty sabotageSQL then runs against the database.
func seedStartupRows(ctx context.Context, t *testing.T, db *sql.DB, now time.Time, sabotageSQL string) {
	t.Helper()
	defer func() {
		if sabotageSQL == "" {
			return
		}
		if _, err := db.ExecContext(ctx, sabotageSQL); err != nil {
			t.Fatalf("sabotage SQL: %v", err)
		}
	}()
	if err := store.InsertCronSchedule(ctx, db, model.CronSchedule{ID: "c-startup", TargetID: "100000000000000001", CronExpr: "0 * * * *", Prompt: "Prompt", Timezone: "UTC", NextRunAt: now.Add(time.Hour), Enabled: true}); err != nil {
		t.Fatalf("InsertCronSchedule failed: %v", err)
	}
	for _, run := range []model.ScheduleRun{
		{ID: "run-old", ScheduleID: "c-startup", ScheduleType: model.ScheduleTypeCron, MessageID: "msg-old", Status: model.StatusCompleted, StartedAt: now.Add(-40 * 24 * time.Hour)},
		{ID: "run-new", ScheduleID: "c-startup", ScheduleType: model.ScheduleTypeCron, MessageID: "msg-new", Status: model.StatusCompleted, StartedAt: now.Add(-time.Hour)},
	} {
		if err := store.CreateScheduleRun(ctx, db, run); err != nil {
			t.Fatalf("CreateScheduleRun failed: %v", err)
		}
	}
}

// assertStartupPrune checks the old run was pruned (or kept) and the recent one kept.
func assertStartupPrune(t *testing.T, db *sql.DB, wantPruned bool) {
	t.Helper()
	_, oldErr := store.GetScheduleRunByDispatchID(context.Background(), db, "msg-old")
	if pruned := oldErr != nil; pruned != wantPruned {
		t.Errorf("old run pruned = %v, want %v (lookup err %v)", pruned, wantPruned, oldErr)
	}
	if _, newErr := store.GetScheduleRunByDispatchID(context.Background(), db, "msg-new"); newErr != nil {
		t.Errorf("recent run lookup err = %v, want kept", newErr)
	}
}

// reportOnce sends err on ch unless a value is already pending.
func reportOnce(ch chan<- error, err error) {
	select {
	case ch <- err:
	default:
	}
}

// errMatches reports whether err is nil when want is empty, or contains want otherwise.
func errMatches(err error, want string) bool {
	if want == "" {
		return err == nil
	}
	return err != nil && strings.Contains(err.Error(), want)
}

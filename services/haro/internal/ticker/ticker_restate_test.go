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
	"github.com/azylman/aerial/pkg/pubsub"
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

	runHandler := func(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
		res, err := fn(mockRunContext{Context: context.Background()})
		if err != nil {
			return restate.ToTerminalError(err)
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

	return mc, mClient
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
			name:         "disabled cron schedule clears state without arming",
			scheduleType: model.ScheduleTypeCron,
			seedType:     "cron",
			enabled:      false,
			future:       true,
			wantArmed:    false,
		},
		{
			name:         "missing schedule clears state without arming",
			scheduleType: model.ScheduleTypeCron,
			seedType:     "",
			wantArmed:    false,
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

			switch tc.seedType {
			case "cron":
				if err := store.InsertCronSchedule(ctx, db, model.CronSchedule{
					ID:        scheduleID,
					TargetID:  "100000000000000001",
					CronExpr:  "0 * * * *",
					Prompt:    "Check hourly",
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
					Enabled:   tc.enabled,
				}); err != nil {
					t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
				}
			}

			mc, mClient := setupObjectMockContext(t, scheduleID, 0)
			objCtx := restate.WithMockContext(mc)

			dispatcher := &fakeDispatcher{}
			s := NewHaroSchedule(db, dispatcher)
			if !tc.defaultClock {
				s.nowFn = func() time.Time { return now }
			}

			_, err := s.Arm(objCtx, ArmRequest{ScheduleType: tc.scheduleType})
			if err != nil {
				t.Fatalf("Arm unexpected err: %v", err)
			}

			if tc.wantArmed {
				mClient.AssertCalled(t, "Send", mock.Anything, mock.Anything)
			} else {
				mClient.AssertNotCalled(t, "Send", mock.Anything, mock.Anything)
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

	// First Arm: arms Fire, sets gen to 1
	if _, err := s.Arm(objCtx, ArmRequest{ScheduleType: model.ScheduleTypeCron}); err != nil {
		t.Fatalf("first Arm err: %v", err)
	}
	mc.AssertCalled(t, "Set", "gen", int64(1))
	mClient.AssertCalled(t, "Send", mock.Anything, mock.Anything)

	// Second Arm with identical NextRunAt: idempotent early return, does not bump gen or re-send Fire
	if _, err := s.Arm(objCtx, ArmRequest{ScheduleType: model.ScheduleTypeCron}); err != nil {
		t.Fatalf("second Arm err: %v", err)
	}
	mc.AssertNotCalled(t, "Set", "gen", int64(2))
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

func TestHaroSchedule_ArmAndCancel_ContextErrors(t *testing.T) {
	t.Parallel()
	mc := mocks.NewMockContext(t)
	mc.On("Key").Return("sched-err-1").Maybe()
	mc.On("Get", "gen", mock.Anything).Return(false, restate.TerminalErrorf("storage get failed")).Maybe()
	objCtx := restate.WithMockContext(mc)

	s := NewHaroSchedule(nil, nil)
	if _, err := s.Arm(objCtx, ArmRequest{ScheduleType: "cron"}); err == nil {
		t.Error("expected error from Arm on Get failure")
	}
	if _, err := s.Cancel(objCtx, struct{}{}); err == nil {
		t.Error("expected error from Cancel on Get failure")
	}
	if _, err := s.Fire(objCtx, FireRequest{Generation: 1}); err == nil {
		t.Error("expected error from Fire on Get failure")
	}
}

func TestHaroSchedule_Cancel(t *testing.T) {
	t.Parallel()
	scheduleID := "cancel-test-1"
	mc, mClient := setupObjectMockContext(t, scheduleID, 5)
	if mClient == nil {
		t.Fatal("expected non-nil mock client")
	}
	objCtx := restate.WithMockContext(mc)

	s := NewHaroSchedule(nil, nil)
	_, err := s.Cancel(objCtx, struct{}{})
	if err != nil {
		t.Fatalf("Cancel unexpected err: %v", err)
	}
	mc.AssertCalled(t, "Set", "gen", int64(6))
}

func TestHaroSchedule_CancelArmFencing(t *testing.T) {
	t.Parallel()
	db := openTickerTestDB(t)
	ctx := context.Background()
	scheduleID := "sched-fence-test"
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
	if mClient == nil {
		t.Fatal("expected non-nil mock client")
	}
	objCtx := restate.WithMockContext(mc)

	disp := &fakeDispatcher{
		response: dispatch.PromptDispatchResponse{
			Status:    "ok",
			ChannelID: "100000000000000001",
			MessageID: "msg-fenced",
		},
	}
	s := NewHaroSchedule(db, disp)
	s.nowFn = func() time.Time { return now }

	// 1. Arm sets gen=1 and schedules Fire(gen=1)
	if _, err := s.Arm(objCtx, ArmRequest{ScheduleType: model.ScheduleTypeCron}); err != nil {
		t.Fatalf("first Arm err: %v", err)
	}
	mc.AssertCalled(t, "Set", "gen", int64(1))

	// 2. Cancel sets gen=2 without clearing state
	if _, err := s.Cancel(objCtx, struct{}{}); err != nil {
		t.Fatalf("Cancel err: %v", err)
	}
	mc.AssertCalled(t, "Set", "gen", int64(2))

	// 3. Arm again sets gen=3 and schedules Fire(gen=3)
	if _, err := s.Arm(objCtx, ArmRequest{ScheduleType: model.ScheduleTypeCron}); err != nil {
		t.Fatalf("second Arm err: %v", err)
	}
	mc.AssertCalled(t, "Set", "gen", int64(3))

	// 4. Stale Fire with Generation=1 should be rejected (no dispatch, no error)
	if _, err := s.Fire(objCtx, FireRequest{Generation: 1, ScheduleType: model.ScheduleTypeCron, DueAt: now.Add(time.Hour)}); err != nil {
		t.Fatalf("stale Fire err: %v", err)
	}
	if len(disp.dispatched) != 0 {
		t.Errorf("expected stale Fire(gen=1) to be rejected, but dispatched %d times", len(disp.dispatched))
	}

	// 5. Valid Fire with Generation=3 should succeed and dispatch
	if _, err := s.Fire(objCtx, FireRequest{Generation: 3, ScheduleType: model.ScheduleTypeCron, DueAt: now.Add(time.Hour)}); err != nil {
		t.Fatalf("valid Fire err: %v", err)
	}
	if len(disp.dispatched) != 1 {
		t.Errorf("expected valid Fire(gen=3) to dispatch 1 prompt, got %d", len(disp.dispatched))
	}
}

func TestHaroSchedule_FinalizeStep_DBErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		schedType  string
		loadedType string
		skipped    bool
		scheduleID string
		runID      string
	}{
		{"create run db error", model.ScheduleTypeCron, model.ScheduleTypeCron, false, "c-1", "r-1"},
		{"delete oneshot db error", model.ScheduleTypeOneShot, model.ScheduleTypeOneShot, true, "o-1", "r-2"},
		{"advance recurring db error", model.ScheduleTypeCron, model.ScheduleTypeCron, true, "c-1", "r-3"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTickerTestDB(t)
			_ = db.Close()
			s := NewHaroSchedule(db, nil)
			now := time.Now().UTC()
			rc := mockRunContext{Context: context.Background()}
			dispRes := DispatchStepResult{Status: model.StatusCompleted, SkippedActive: tc.skipped}
			loaded := ScheduleRow{ScheduleType: tc.loadedType}
			if _, err := s.finalizeStep(rc, loaded, tc.schedType, tc.scheduleID, tc.runID, dispRes, now); err == nil {
				t.Errorf("expected error on %s with closed db, got nil", tc.name)
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

	tk := NewTicker(db, nil, 30*time.Second)
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

	verifyFireDispatches(t, tc, disp)

	if tc.wantFenced {
		mc.AssertCalled(t, "Set", "gen", tc.generation+1)
	}

	if tc.wantDeleted && tc.seedType == "oneshot" {
		_, getErr := store.GetOneShotSchedule(ctx, db, scheduleID)
		if getErr == nil {
			t.Error("expected one-shot schedule to be deleted, still found")
		}
	}

	if !tc.wantErr && tc.wantDispatched && (tc.scheduleType == model.ScheduleTypeCron || tc.scheduleType == model.ScheduleTypeHeartbeat) {
		mClient.AssertCalled(t, "Send", mock.Anything, mock.Anything)
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
			name:         "schedule missing is fenced and returns nil",
			scheduleType: model.ScheduleTypeCron,
			seedType:     "",
			generation:   1,
			reqGen:       1,
			wantFenced:   true,
		},
		{
			name:         "schedule disabled is fenced and returns nil",
			scheduleType: model.ScheduleTypeCron,
			seedType:     "cron",
			generation:   1,
			reqGen:       1,
			enabled:      false,
			wantFenced:   true,
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
			wantFenced:     true,
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
			name:           "one-shot schedule success deletes schedule and fences generation",
			scheduleType:   model.ScheduleTypeOneShot,
			seedType:       "oneshot",
			generation:     1,
			reqGen:         1,
			enabled:        true,
			wantDispatched: true,
			wantDeleted:    true,
			wantFenced:     true,
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
	return append(fireSkipAndErrorTestCases(), fireSuccessTestCases()...)
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

type eventWatchTestCase struct {
	name           string
	seedTrigger    bool
	persistent     bool
	preConsume     bool
	closedDB       bool
	nilDispatcher  bool
	dispatchErr    error
	wantErr        bool
	wantDispatched bool
	wantDeleted    bool
}

func verifyEventWatchOutcome(ctx context.Context, t *testing.T, db *sql.DB, tc eventWatchTestCase, triggerID string, disp dispatch.PromptDispatcher) {
	t.Helper()
	if !tc.nilDispatcher && disp != nil {
		fDisp, ok := disp.(*fakeDispatcher)
		if !ok {
			t.Fatal("expected *fakeDispatcher")
		}
		if tc.wantDispatched && len(fDisp.dispatched) == 0 {
			t.Error("expected prompt dispatch, got none")
		}
		if !tc.wantDispatched && len(fDisp.dispatched) > 0 {
			t.Errorf("expected no prompt dispatch, got %d", len(fDisp.dispatched))
		}
	}

	if tc.seedTrigger {
		_, getErr := store.GetEventTrigger(ctx, db, triggerID)
		if tc.wantDeleted && getErr == nil {
			t.Error("expected trigger to be deleted, still found")
		}
		if !tc.wantDeleted && getErr != nil {
			t.Errorf("expected trigger to exist, got err: %v", getErr)
		}
	}
}

func seedEventWatchTriggerForTest(ctx context.Context, t *testing.T, db *sql.DB, tc eventWatchTestCase, triggerID string, now time.Time) {
	t.Helper()
	if tc.closedDB {
		_ = db.Close()
		return
	}
	if !tc.seedTrigger {
		return
	}
	if err := store.InsertEventTrigger(ctx, db, model.EventTrigger{
		ID:         triggerID,
		ChannelID:  "100000000000000001",
		Source:     "flux",
		Target:     "aerial",
		EventType:  "sync_success",
		Prompt:     "Deploy finished",
		Persistent: tc.persistent,
	}); err != nil {
		t.Fatalf("InsertEventTrigger failed: %v", err)
	}
	if tc.preConsume {
		consumed, cErr := store.ConsumeEventTrigger(ctx, db, triggerID, now)
		if cErr != nil || !consumed {
			t.Fatalf("ConsumeEventTrigger setup failed: consumed=%v, err=%v", consumed, cErr)
		}
	}
}

func TestHaroEventWatch_Fire(t *testing.T) {
	t.Parallel()
	tests := []eventWatchTestCase{
		{
			name:           "missing trigger is no-op",
			seedTrigger:    false,
			wantDispatched: false,
		},
		{
			name:          "nil dispatcher returns error",
			seedTrigger:   true,
			nilDispatcher: true,
			wantErr:       true,
		},
		{
			name:           "dispatch error returns error and leaves trigger untouched",
			seedTrigger:    true,
			dispatchErr:    errors.New("dispatch failed"),
			wantErr:        true,
			wantDispatched: true,
			wantDeleted:    false,
		},
		{
			name:           "non-persistent trigger dispatch success deletes trigger",
			seedTrigger:    true,
			persistent:     false,
			wantDispatched: true,
			wantDeleted:    true,
		},
		{
			name:           "persistent trigger dispatch success updates updated_at without deleting",
			seedTrigger:    true,
			persistent:     true,
			wantDispatched: true,
			wantDeleted:    false,
		},
		{
			name:           "persistent trigger within 5s cooldown skips dispatch",
			seedTrigger:    true,
			persistent:     true,
			preConsume:     true,
			wantDispatched: false,
			wantDeleted:    false,
		},
		{
			name:     "closed db returns error",
			closedDB: true,
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTickerTestDB(t)
			ctx := context.Background()
			triggerID := "trig-" + tc.name

			now := time.Now().UTC()
			seedEventWatchTriggerForTest(ctx, t, db, tc, triggerID, now)

			mc, mClient := setupObjectMockContext(t, triggerID, 0)
			if mClient == nil {
				t.Fatal("expected non-nil mock client")
			}
			objCtx := restate.WithMockContext(mc)

			var disp dispatch.PromptDispatcher
			if !tc.nilDispatcher {
				disp = &fakeDispatcher{
					response: dispatch.PromptDispatchResponse{
						Status:    model.StatusCompleted,
						ChannelID: "100000000000000001",
						MessageID: "msg-event-1",
					},
					err: tc.dispatchErr,
				}
			}

			w := NewHaroEventWatch(db, disp)
			w.nowFn = func() time.Time { return now }
			_, err := w.Fire(objCtx, EventWatchFireRequest{
				TriggerID:     triggerID,
				ContextHeader: "Flux sync",
				EventID:       "ev-1",
			})

			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			verifyEventWatchOutcome(ctx, t, db, tc, triggerID, disp)
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

	tk := NewTicker(db, nil, 30*time.Second)
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

type externalEventTestCase struct {
	name         string
	source       string
	targets      []string
	nilDB        bool
	seedTrigger  bool
	triggerCount int
	statusCode   int
	wantErr      bool
}

func seedExternalEventTriggersForTest(ctx context.Context, t *testing.T, db *sql.DB, tc externalEventTestCase, now time.Time) {
	t.Helper()
	if tc.name == "closed db returns error" && db != nil {
		_ = db.Close()
		return
	}
	if tc.seedTrigger {
		if err := store.InsertEventTrigger(ctx, db, model.EventTrigger{
			ID:        "trig-ext-" + tc.name,
			ChannelID: "100000000000000001",
			Source:    tc.source,
			Target:    "aerial",
			EventType: "sync_success",
			Prompt:    "Prompt",
			CreatedAt: now,
		}); err != nil {
			t.Fatalf("InsertEventTrigger failed: %v", err)
		}
	}
	for i := range tc.triggerCount {
		if err := store.InsertEventTrigger(ctx, db, model.EventTrigger{
			ID:        fmt.Sprintf("trig-ext-%s-%d", tc.name, i),
			ChannelID: "100000000000000001",
			Source:    tc.source,
			Target:    "aerial",
			EventType: "sync_success",
			Prompt:    "Prompt",
			CreatedAt: now,
		}); err != nil {
			t.Fatalf("InsertEventTrigger failed: %v", err)
		}
	}
}

func TestHandleExternalEvent(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()

	tests := []externalEventTestCase{
		{name: "healthcheck source is no-op", source: "healthcheck", targets: []string{"a"}},
		{name: "empty targets is no-op", source: "flux", targets: []string{}},
		{name: "nil db returns error", source: "flux", targets: []string{"a"}, nilDB: true, wantErr: true},
		{name: "closed db returns error", source: "flux", targets: []string{"a"}, statusCode: http.StatusOK, wantErr: true},
		{
			name:        "matching trigger success",
			source:      "flux",
			targets:     []string{"aerial"},
			seedTrigger: true,
			statusCode:  http.StatusOK,
			wantErr:     false,
		},
		{
			name:        "matching trigger post failure",
			source:      "flux",
			targets:     []string{"aerial"},
			seedTrigger: true,
			statusCode:  http.StatusInternalServerError,
			wantErr:     true,
		},
		{
			name:         "multiple triggers continues loop on error",
			source:       "flux",
			targets:      []string{"aerial"},
			triggerCount: 2,
			statusCode:   http.StatusInternalServerError,
			wantErr:      true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var db *sql.DB
			ctx := context.Background()
			if !tc.nilDB {
				db = openTickerTestDB(t)
			}
			seedExternalEventTriggersForTest(ctx, t, db, tc, now)

			called := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				called++
				w.WriteHeader(tc.statusCode)
			}))
			defer srv.Close()

			ev := pubsub.ExternalEvent{
				EventID:       "ev-123",
				Source:        tc.source,
				Targets:       tc.targets,
				EventType:     "sync_success",
				ContextHeader: "Sync",
			}

			err := HandleExternalEvent(ctx, db, srv.URL, ev, srv.Client())
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.triggerCount > 0 && called != tc.triggerCount {
				t.Errorf("called = %d, want %d", called, tc.triggerCount)
			}
		})
	}
}

func TestPruneOldScheduleRuns_NilCases(t *testing.T) {
	t.Parallel()
	var nilTk *Ticker
	nilTk.PruneOldScheduleRuns(context.Background(), time.Now())
	nilTk.SetRestateConfig("9080", "", "", "")

	tkNoDB := NewTicker(nil, nil, 30*time.Second)
	tkNoDB.PruneOldScheduleRuns(context.Background(), time.Now())
}

func TestTicker_EvaluateDueSchedules_RestateSkipsPolling(t *testing.T) {
	t.Parallel()
	db := openTickerTestDB(t)
	ctx := context.Background()
	now := time.Now().UTC()

	if err := store.InsertCronSchedule(ctx, db, model.CronSchedule{
		ID:        "c-due-restate-skip",
		TargetID:  "100000000000000001",
		CronExpr:  "0 * * * *",
		Prompt:    "Due cron prompt",
		NextRunAt: now.Add(-time.Minute),
		Enabled:   true,
	}); err != nil {
		t.Fatalf("InsertCronSchedule failed: %v", err)
	}

	disp := &fakeDispatcher{
		response: dispatch.PromptDispatchResponse{Status: "ok", MessageID: "msg-due"},
	}

	backfilled := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		backfilled++
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	// When restatePort is configured, EvaluateDueSchedules skips polling evaluation
	// and periodically reconciles active schedules via BackfillActiveSchedules
	tk := NewTicker(db, disp, 30*time.Second)
	tk.SetRestateConfig("9080", srv.URL, "", "")
	tk.httpClient = srv.Client()
	tk.EvaluateDueSchedules(ctx, now)

	if len(disp.dispatched) > 0 {
		t.Errorf("expected 0 dispatches when restate is enabled, got %d", len(disp.dispatched))
	}
	if backfilled != 1 {
		t.Errorf("expected 1 backfill arm call, got %d", backfilled)
	}

	// When restatePort is empty, EvaluateDueSchedules evaluates due schedules
	tkLegacy := NewTicker(db, disp, 30*time.Second)
	tkLegacy.EvaluateDueSchedules(ctx, now)

	if len(disp.dispatched) != 1 {
		t.Errorf("expected 1 dispatch when restate is disabled, got %d", len(disp.dispatched))
	}
}

func TestTicker_StartRestateLifecycle(t *testing.T) {
	t.Parallel()
	db := openTickerTestDB(t)
	ctx, cancel := context.WithCancel(context.Background())

	registered := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case registered <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	tk := NewTicker(db, &fakeDispatcher{}, 30*time.Second)
	// Bind to port "0" (OS-assigned ephemeral port) with admin and ingress URLs configured
	tk.SetRestateConfig("0", srv.URL, srv.URL, "http://localhost:0")
	tk.httpClient = srv.Client()

	done := make(chan struct{})
	var startErr error
	go func() {
		startErr = tk.Start(ctx)
		close(done)
	}()

	select {
	case <-registered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Start to register")
	}
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Start to shut down")
	}
	if startErr != nil && !errors.Is(startErr, context.Canceled) {
		t.Fatalf("unexpected start error: %v", startErr)
	}
}

func TestTicker_EnsureRegisteredAndBackfilled_Retry(t *testing.T) {
	t.Parallel()
	db := openTickerTestDB(t)
	ctx := context.Background()

	var attempts atomic.Int32
	var backfilled atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/deployments" {
			if attempts.Add(1) == 1 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		if backfilled.Add(1) == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	tk := NewTicker(db, &fakeDispatcher{}, 30*time.Second)
	tk.SetRestateConfig("9080", srv.URL, srv.URL, "http://localhost:9080")
	tk.httpClient = srv.Client()

	if err := store.InsertCronSchedule(ctx, db, model.CronSchedule{
		ID:        "c-retry-reg-1",
		TargetID:  "100000000000000001",
		CronExpr:  "0 * * * *",
		Prompt:    "Prompt",
		NextRunAt: time.Now().UTC().Add(time.Hour),
		Enabled:   true,
	}); err != nil {
		t.Fatalf("InsertCronSchedule failed: %v", err)
	}

	// Attempt 1: registration fails, backfill must not run
	tk.ensureRegisteredAndBackfilled(ctx)
	if attempts.Load() != 1 {
		t.Errorf("expected 1 registration attempt, got %d", attempts.Load())
	}
	if backfilled.Load() != 0 {
		t.Errorf("expected 0 backfill calls on failed registration, got %d", backfilled.Load())
	}
	if tk.restateRegistered {
		t.Error("expected restateRegistered to be false after failure")
	}
	if tk.restateBackfilled {
		t.Error("expected restateBackfilled to be false after failure")
	}

	// Attempt 2: registration succeeds, backfill fails
	tk.ensureRegisteredAndBackfilled(ctx)
	if attempts.Load() != 2 {
		t.Errorf("expected 2 registration attempts, got %d", attempts.Load())
	}
	if backfilled.Load() != 1 {
		t.Errorf("expected 1 backfill call, got %d", backfilled.Load())
	}
	if !tk.restateRegistered {
		t.Error("expected restateRegistered to be true after success")
	}
	if tk.restateBackfilled {
		t.Error("expected restateBackfilled to be false after backfill failure")
	}

	// Attempt 3: registration skipped (already registered), backfill retries and succeeds
	tk.ensureRegisteredAndBackfilled(ctx)
	if attempts.Load() != 2 {
		t.Errorf("expected registration attempts to remain 2, got %d", attempts.Load())
	}
	if backfilled.Load() != 2 {
		t.Errorf("expected 2 backfill calls, got %d", backfilled.Load())
	}
	if !tk.restateBackfilled {
		t.Error("expected restateBackfilled to be true after backfill success")
	}

	// Attempt 4: both registration and backfill are skipped
	tk.ensureRegisteredAndBackfilled(ctx)
	if attempts.Load() != 2 {
		t.Errorf("expected registration attempts to remain 2, got %d", attempts.Load())
	}
	if backfilled.Load() != 2 {
		t.Errorf("expected backfill calls to remain 2, got %d", backfilled.Load())
	}

	// Nil ticker safety check
	var nilTk *Ticker
	nilTk.ensureRegisteredAndBackfilled(ctx)
}

func TestTicker_StartRestateServer_Error(t *testing.T) {
	t.Parallel()
	db := openTickerTestDB(t)
	tk := NewTicker(db, &fakeDispatcher{}, 30*time.Second)

	_, err := tk.startRestateServer(context.Background(), "invalid-port-name")
	if err == nil {
		t.Error("expected listen error on invalid port, got nil")
	}
}

func TestTicker_Start_Error(t *testing.T) {
	t.Parallel()
	db := openTickerTestDB(t)
	tk := NewTicker(db, &fakeDispatcher{}, 30*time.Second)
	tk.SetRestateConfig("invalid-port-name", "http://localhost:8080", "http://localhost:9070", "http://localhost:invalid")

	err := tk.Start(context.Background())
	if err == nil {
		t.Fatal("expected Start to return error on invalid restate port, got nil")
	}
}

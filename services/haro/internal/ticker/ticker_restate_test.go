package ticker

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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
	"github.com/azylman/aerial/pkg/restatetest"
	"github.com/azylman/aerial/services/haro/internal/dispatch"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
)

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

// seedSchedule inserts a schedule of seedType ("cron", "oneshot", "heartbeat"; "" seeds nothing) due at runAt.
func seedSchedule(ctx context.Context, t *testing.T, db *sql.DB, seedType, scheduleID string, runAt time.Time, enabled, invalidCron, hasActiveHB bool) {
	t.Helper()
	cronExpr := "0 * * * *"
	if invalidCron {
		cronExpr = "not-a-cron"
	}
	switch seedType {
	case "cron":
		if err := store.InsertCronSchedule(ctx, db, model.CronSchedule{
			ID:        scheduleID,
			TargetID:  "100000000000000001",
			CronExpr:  cronExpr,
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
		if !invalidCron {
			cronExpr = "*/5 * * * *"
		}
		if err := store.InsertHeartbeatSchedule(ctx, db, model.HeartbeatSchedule{
			ID:        scheduleID,
			TargetID:  "100000000000000003",
			CronExpr:  cronExpr,
			Prompt:    "Heartbeat check",
			Timezone:  "UTC",
			NextRunAt: runAt,
			Enabled:   enabled,
		}); err != nil {
			t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
		}
		if hasActiveHB {
			if err := store.CreateScheduleRun(ctx, db, model.ScheduleRun{
				ID:           "run-active-" + scheduleID,
				ScheduleID:   scheduleID,
				ScheduleType: model.ScheduleTypeHeartbeat,
				TargetID:     "100000000000000003",
				ThreadID:     "thread-active",
				Status:       "running",
				StartedAt:    runAt.Add(-2 * time.Minute),
			}); err != nil {
				t.Fatalf("CreateScheduleRun failed: %v", err)
			}
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

			seedSchedule(ctx, t, db, tc.seedType, scheduleID, runAt, tc.enabled, false, false)

			h := restatetest.NewObjectHarness(t, scheduleID)

			dispatcher := &fakeDispatcher{}
			s := NewHaroSchedule(db, dispatcher)
			if !tc.defaultClock {
				s.nowFn = func() time.Time { return now }
			}

			_, err := s.Arm(h.ObjCtx, ArmRequest{ScheduleType: tc.scheduleType})
			if got := restatetest.TerminalCode(err); got != tc.wantCode || (err != nil && tc.wantCode == 0) {
				t.Fatalf("Arm err = %v, want terminal code %d", err, tc.wantCode)
			}

			wantKeys := 0
			if tc.wantArmed {
				wantKeys = 1
			}
			if got := fireKeys(h.Client); len(got) != wantKeys {
				t.Errorf("Fire sends = %v, want %d", got, wantKeys)
			}
			if tc.wantCode == http.StatusPreconditionFailed {
				h.Context.AssertCalled(t, "Set", "gen", int64(1))
			}
			if got := h.ClearedAll(); got != (tc.wantCode == http.StatusNotFound) {
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

	h := restatetest.NewObjectHarness(t, scheduleID)

	s := NewHaroSchedule(db, &fakeDispatcher{})
	s.nowFn = func() time.Time { return now }

	// Re-arming the same tick (e.g. after a lost Fire) sends Fire again under the
	// same key, so Restate dedupes a pending Fire and re-creates a lost one. The
	// generation is never bumped, so the pending Fire stays valid.
	for range 2 {
		if _, err := s.Arm(h.ObjCtx, ArmRequest{ScheduleType: model.ScheduleTypeCron}); err != nil {
			t.Fatalf("Arm err: %v", err)
		}
	}
	want := []string{"haro:fire:arm-idempotent:0:2026-11-22T11:00:00Z", "haro:fire:arm-idempotent:0:2026-11-22T11:00:00Z"}
	if got := fireKeys(h.Client); !reflect.DeepEqual(got, want) {
		t.Errorf("Fire keys = %v, want %v", got, want)
	}
	h.Context.AssertNotCalled(t, "Set", "gen", mock.Anything)
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
	h := restatetest.NewObjectHarness(t, "cancel-test-1", restatetest.WithInitialState("gen", int64(5)))
	restate.Set(h.ObjCtx, "armed_due_at", "2026-11-22T11:00:00Z")

	if _, err := NewHaroSchedule(nil, nil).Cancel(h.ObjCtx, struct{}{}); err != nil {
		t.Fatalf("Cancel unexpected err: %v", err)
	}
	gen, genErr := restate.Get[int64](h.ObjCtx, "gen")
	armed, armedErr := restate.Get[string](h.ObjCtx, "armed_due_at")
	if genErr != nil || armedErr != nil || gen != 0 || armed != "" || !h.ClearedAll() {
		t.Errorf("after Cancel gen=%d armed=%q cleared=%v (errs %v, %v); want all state cleared", gen, armed, h.ClearedAll(), genErr, armedErr)
	}
	if keys := fireKeys(h.Client); len(keys) != 0 {
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
			h := restatetest.NewObjectHarness(t, tc.id, restatetest.WithInitialState("gen", tc.initialGen))
			restate.Set(h.ObjCtx, "armed_due_at", tc.fireDue.Format(time.RFC3339Nano))

			disp := &fakeDispatcher{response: dispatch.PromptDispatchResponse{Status: "ok", ChannelID: "100000000000000001", MessageID: "msg-cancel"}}
			s := NewHaroSchedule(db, disp)
			s.nowFn = func() time.Time { return tc.fireDue }

			if _, err := s.Cancel(h.ObjCtx, struct{}{}); err != nil {
				t.Fatalf("Cancel err: %v", err)
			}
			if tc.rearm {
				if _, err := s.Arm(h.ObjCtx, ArmRequest{ScheduleType: model.ScheduleTypeCron}); err != nil {
					t.Fatalf("Arm err: %v", err)
				}
			}
			_, err := s.Fire(h.ObjCtx, FireRequest{Generation: tc.initialGen, ScheduleType: model.ScheduleTypeCron, DueAt: tc.fireDue})
			if got := restatetest.TerminalCode(err); got != tc.wantCode || (err != nil && tc.wantCode == 0) {
				t.Fatalf("Fire err = %v, want terminal code %d", err, tc.wantCode)
			}
			if len(disp.dispatched) != tc.wantDispatched {
				t.Errorf("dispatched %d prompts, want %d", len(disp.dispatched), tc.wantDispatched)
			}
			if got := fireKeys(h.Client); !reflect.DeepEqual(got, tc.wantFireKeys) {
				t.Errorf("Fire keys = %v, want %v", got, tc.wantFireKeys)
			}
		})
	}
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
			rc := restatetest.NewRunContext(context.Background())
			dispRes := DispatchStepResult{Status: tc.status, SkippedActive: tc.skipped, DispatchedAt: now, Error: "boom"}
			loaded := ScheduleRow{ScheduleType: model.ScheduleTypeCron, TargetID: "100000000000000001"}
			runID := deterministicScheduleRunID(model.ScheduleTypeCron, "rec-"+tc.name, now)
			err := s.recordRunStep(rc, loaded, model.ScheduleTypeCron, "rec-"+tc.name, runID, dispRes, now)
			if (err != nil) != tc.wantErr {
				t.Fatalf("recordRunStep err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := restatetest.TerminalCode(err); got != tc.wantCode {
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
			rc := restatetest.NewRunContext(context.Background())
			loaded := ScheduleRow{ScheduleType: tc.schedType, CronExpr: "0 * * * *", Timezone: "UTC"}
			res, err := s.finalizeStep(rc, loaded, tc.schedType, "missing-"+tc.name, time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC))
			if (err != nil) != tc.wantErr {
				t.Fatalf("finalizeStep err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := restatetest.TerminalCode(err); got != tc.wantCode {
				t.Errorf("terminal code = %d, want %d", got, tc.wantCode)
			}
			if res.IsOneShot != tc.wantOneShot {
				t.Errorf("IsOneShot = %v, want %v", res.IsOneShot, tc.wantOneShot)
			}
		})
	}
}

type fireSkipAndFailureCase struct {
	name           string
	scheduleType   string
	seedType       string
	gen            int64
	reqGen         int64
	disabled       bool
	stale          bool
	hasActiveHB    bool
	skippedActive  bool
	nilDispatcher  bool
	dispatchErr    error
	wantErr        bool
	wantDispatched bool
	wantCode       int
}

type fireSuccessCase struct {
	name         string
	scheduleType string
	seedType     string
	wantDeleted  bool
	superseded   bool
}

func verifyDispatches(t *testing.T, wantDispatched bool, disp dispatch.PromptDispatcher) {
	t.Helper()
	if disp == nil {
		return
	}
	fDisp, ok := disp.(*fakeDispatcher)
	if !ok {
		t.Fatal("expected *fakeDispatcher")
	}
	dispatchedCount := len(fDisp.dispatched) + len(fDisp.dispatchedRequests)
	if wantDispatched && dispatchedCount == 0 {
		t.Error("expected prompt dispatch, got none")
	}
	if !wantDispatched && dispatchedCount > 0 {
		t.Errorf("expected no prompt dispatch, got %d", dispatchedCount)
	}
}

func buildSkipAndFailureDispatcher(tc fireSkipAndFailureCase) dispatch.PromptDispatcher {
	if tc.nilDispatcher {
		return nil
	}
	hbStatus := model.StatusCompleted
	if tc.skippedActive {
		hbStatus = eriwire.StatusSkippedActive
	}
	return &fakeDispatcher{
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
}

func buildSuccessDispatcher(name string, runAt *time.Time) dispatch.PromptDispatcher {
	fDisp := &fakeDispatcher{
		response: dispatch.PromptDispatchResponse{
			Status:    model.StatusCompleted,
			ChannelID: "100000000000000001",
			MessageID: "msg-1",
		},
		dispatchResponse: eriwire.DispatchEnqueueResponse{
			Status:     model.StatusCompleted,
			DispatchID: "disp-hb-1",
		},
	}
	if name == "cron response with empty channel and message ID and zero dueAt" {
		fDisp.response.ChannelID = ""
		fDisp.response.MessageID = ""
		*runAt = time.Time{}
	}
	return fDisp
}

func TestScheduleObject_Fire_SkipsAndFailures(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)

	tests := []fireSkipAndFailureCase{
		{
			name:         "generation mismatch is no-op",
			scheduleType: model.ScheduleTypeCron,
			seedType:     "cron",
			gen:          2,
			reqGen:       1,
		},
		{
			name:         "schedule missing clears state and ends terminally with 404",
			scheduleType: model.ScheduleTypeCron,
			seedType:     "",
			wantErr:      true,
			wantCode:     http.StatusNotFound,
		},
		{
			name:         "schedule disabled is fenced and ends terminally with 412",
			scheduleType: model.ScheduleTypeCron,
			seedType:     "cron",
			disabled:     true,
			wantErr:      true,
			wantCode:     http.StatusPreconditionFailed,
		},
		{
			name:         "stale cron schedule >24h records skipped_stale and advances without dispatching",
			scheduleType: model.ScheduleTypeCron,
			seedType:     "cron",
			stale:        true,
		},
		{
			name:         "stale heartbeat schedule >24h records skipped_stale and advances without dispatching",
			scheduleType: model.ScheduleTypeHeartbeat,
			seedType:     "heartbeat",
			stale:        true,
		},
		{
			name:         "stale oneshot schedule >24h records skipped_stale and deletes",
			scheduleType: model.ScheduleTypeOneShot,
			seedType:     "oneshot",
			stale:        true,
		},
		{
			name:         "heartbeat with active run records skipped_active and advances without dispatching",
			scheduleType: model.ScheduleTypeHeartbeat,
			seedType:     "heartbeat",
			hasActiveHB:  true,
		},
		{
			name:           "heartbeat dispatch returns skipped_active status",
			scheduleType:   model.ScheduleTypeHeartbeat,
			seedType:       "heartbeat",
			skippedActive:  true,
			wantDispatched: true,
		},
		{
			name:          "cron with nil dispatcher returns error",
			scheduleType:  model.ScheduleTypeCron,
			seedType:      "cron",
			nilDispatcher: true,
			wantErr:       true,
		},
		{
			name:          "heartbeat with nil dispatcher returns error",
			scheduleType:  model.ScheduleTypeHeartbeat,
			seedType:      "heartbeat",
			nilDispatcher: true,
			wantErr:       true,
		},
		{
			name:           "dispatch error returns error to retry Step 2",
			scheduleType:   model.ScheduleTypeCron,
			seedType:       "cron",
			dispatchErr:    errors.New("eri connection failed"),
			wantErr:        true,
			wantDispatched: true,
		},
		{
			name:           "heartbeat dispatch error returns error",
			scheduleType:   model.ScheduleTypeHeartbeat,
			seedType:       "heartbeat",
			dispatchErr:    errors.New("heartbeat eri dispatch failed"),
			wantErr:        true,
			wantDispatched: true,
		},
		{
			name:           "cron eri 4xx records a failed run and arms the next tick",
			scheduleType:   model.ScheduleTypeCron,
			seedType:       "cron",
			dispatchErr:    restate.ToTerminalError(errors.New("restate prompt dispatch error (400): bad channel"), restate.WithErrorCode(http.StatusBadRequest)),
			wantDispatched: true,
		},
		{
			name:           "heartbeat eri 4xx records a failed run and arms the next tick",
			scheduleType:   model.ScheduleTypeHeartbeat,
			seedType:       "heartbeat",
			dispatchErr:    restate.ToTerminalError(errors.New("restate dispatch error (404): unknown agent"), restate.WithErrorCode(http.StatusNotFound)),
			wantDispatched: true,
		},
		{
			name:           "one-shot eri 4xx records a failed run and deletes the schedule",
			scheduleType:   model.ScheduleTypeOneShot,
			seedType:       "oneshot",
			dispatchErr:    restate.ToTerminalError(errors.New("restate prompt dispatch error (400): bad channel"), restate.WithErrorCode(http.StatusBadRequest)),
			wantDispatched: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTickerTestDB(t)
			ctx := context.Background()
			scheduleID := "fire-" + tc.name

			runAt := now.Add(-time.Minute)
			if tc.stale {
				runAt = now.Add(-25 * time.Hour)
			}

			seedSchedule(ctx, t, db, tc.seedType, scheduleID, runAt, !tc.disabled, false, tc.hasActiveHB)

			gen := tc.gen
			if gen == 0 {
				gen = 1
			}
			reqGen := tc.reqGen
			if reqGen == 0 {
				reqGen = 1
			}

			h := restatetest.NewObjectHarness(t, scheduleID, restatetest.WithInitialState("gen", gen))
			disp := buildSkipAndFailureDispatcher(tc)
			s := NewHaroSchedule(db, disp)
			s.nowFn = func() time.Time { return now }

			_, err := s.Fire(h.ObjCtx, FireRequest{
				Generation:   reqGen,
				ScheduleType: tc.scheduleType,
				DueAt:        runAt,
			})

			assertSkipAndFailureOutcome(ctx, t, tc, err, h, db, scheduleID, disp, gen)
		})
	}
}

func assertSkipAndFailureOutcome(ctx context.Context, t *testing.T, tc fireSkipAndFailureCase, err error, h *restatetest.ObjectHarness, db *sql.DB, scheduleID string, disp dispatch.PromptDispatcher, gen int64) {
	t.Helper()
	if tc.wantErr && err == nil {
		t.Fatal("expected error, got nil")
	}
	if !tc.wantErr && err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := restatetest.TerminalCode(err); got != tc.wantCode {
		t.Errorf("terminal code = %d, want %d (err %v)", got, tc.wantCode, err)
	}

	verifyDispatches(t, tc.wantDispatched, disp)

	if tc.disabled {
		h.Context.AssertCalled(t, "Set", "gen", gen+1)
	}
	if tc.wantCode == http.StatusNotFound && !h.ClearedAll() {
		t.Errorf("expected state cleared for 404, got cleared = false")
	}

	assertSkipAndFailureTerminalDispatch(ctx, t, tc, db, scheduleID, h)
	assertSkipAndFailureOneShot(ctx, t, tc, db, scheduleID, h)
}

func assertSkipAndFailureTerminalDispatch(ctx context.Context, t *testing.T, tc fireSkipAndFailureCase, db *sql.DB, scheduleID string, h *restatetest.ObjectHarness) {
	t.Helper()
	if tc.dispatchErr == nil || !restate.IsTerminalError(tc.dispatchErr) {
		return
	}
	if tc.scheduleType == model.ScheduleTypeCron || tc.scheduleType == model.ScheduleTypeOneShot {
		run, runErr := store.GetScheduleRunByDispatchID(ctx, db, deterministicScheduleRunID(tc.scheduleType, scheduleID, time.Date(2026, 11, 22, 9, 59, 0, 0, time.UTC)))
		if runErr != nil {
			t.Fatalf("expected recorded run: %v", runErr)
		}
		if run.Status != model.StatusFailed {
			t.Errorf("run status = %q, want %q", run.Status, model.StatusFailed)
		}
	}
	if tc.scheduleType == model.ScheduleTypeCron || tc.scheduleType == model.ScheduleTypeHeartbeat {
		if len(fireKeys(h.Client)) != 1 {
			t.Errorf("expected next Fire to be armed, got %v", fireKeys(h.Client))
		}
	}
}

func assertSkipAndFailureOneShot(ctx context.Context, t *testing.T, tc fireSkipAndFailureCase, db *sql.DB, scheduleID string, h *restatetest.ObjectHarness) {
	t.Helper()
	if tc.scheduleType != model.ScheduleTypeOneShot {
		return
	}
	if !tc.stale && (tc.dispatchErr == nil || !restate.IsTerminalError(tc.dispatchErr)) {
		return
	}
	if !h.ClearedAll() {
		t.Errorf("expected oneshot state cleared, got false")
	}
	if _, getErr := store.GetOneShotSchedule(ctx, db, scheduleID); getErr == nil {
		t.Error("expected one-shot schedule to be deleted, still found")
	}
}

func TestScheduleObject_Fire_Success(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)

	tests := []fireSuccessCase{
		{
			name:         "cron response with empty channel and message ID and zero dueAt",
			scheduleType: model.ScheduleTypeCron,
		},
		{
			name:         "cron with invalid cron expr in DB advances with 24h fallback",
			scheduleType: model.ScheduleTypeCron,
		},
		{
			name:         "heartbeat with invalid cron expr in DB advances with 24h fallback",
			scheduleType: model.ScheduleTypeHeartbeat,
		},
		{
			name:         "one-shot schedule success deletes schedule and clears state",
			scheduleType: model.ScheduleTypeOneShot,
			wantDeleted:  true,
		},
		{
			name:         "cron schedule success advances next_run_at and arms next Fire",
			scheduleType: model.ScheduleTypeCron,
		},
		{
			name:         "heartbeat schedule success advances next_run_at and arms next Fire",
			scheduleType: model.ScheduleTypeHeartbeat,
		},
		{
			name:         "fire for a superseded due tick is a no-op",
			scheduleType: model.ScheduleTypeCron,
			superseded:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			db := openTickerTestDB(t)
			ctx := context.Background()
			scheduleID := "fire-success-" + tc.name

			runAt := now.Add(-time.Minute)
			seedType := tc.seedType
			if seedType == "" {
				seedType = tc.scheduleType
				if seedType == model.ScheduleTypeOneShot {
					seedType = "oneshot"
				}
			}
			seedSchedule(ctx, t, db, seedType, scheduleID, runAt, true, strings.Contains(tc.name, "invalid cron expr"), false)

			h := restatetest.NewObjectHarness(t, scheduleID, restatetest.WithInitialState("gen", int64(1)))
			if tc.superseded {
				restate.Set(h.ObjCtx, "armed_due_at", runAt.Add(time.Hour).Format(time.RFC3339Nano))
			}

			disp := buildSuccessDispatcher(tc.name, &runAt)
			s := NewHaroSchedule(db, disp)
			s.nowFn = func() time.Time { return now }

			_, err := s.Fire(h.ObjCtx, FireRequest{
				Generation:   1,
				ScheduleType: tc.scheduleType,
				DueAt:        runAt,
			})
			assertSuccessOutcome(ctx, t, tc, err, h, db, scheduleID, disp)
		})
	}
}

func assertSuccessOutcome(ctx context.Context, t *testing.T, tc fireSuccessCase, err error, h *restatetest.ObjectHarness, db *sql.DB, scheduleID string, disp dispatch.PromptDispatcher) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	verifyDispatches(t, !tc.superseded, disp)

	if tc.wantDeleted && tc.scheduleType == model.ScheduleTypeOneShot {
		if !h.ClearedAll() {
			t.Errorf("state cleared = false, want true")
		}
		if _, getErr := store.GetOneShotSchedule(ctx, db, scheduleID); getErr == nil {
			t.Error("expected one-shot schedule to be deleted, still found")
		}
	}

	if !tc.superseded && (tc.scheduleType == model.ScheduleTypeCron || tc.scheduleType == model.ScheduleTypeHeartbeat) {
		if len(fireKeys(h.Client)) == 0 {
			t.Error("expected next Fire to be armed, got none")
		}
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

	// Post failure branch folded from TestBackfillActiveSchedules_PostErrors
	var failHits atomic.Int64
	failSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		failHits.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer failSrv.Close()

	failTk := NewTicker(db, nil)
	failTk.SetRestateConfig("9080", failSrv.URL, "", "")
	failTk.httpClient = failSrv.Client()
	if err := failTk.BackfillActiveSchedules(ctx); err == nil {
		t.Fatal("expected error on post failure, got nil")
	}
	if got := failHits.Load(); got != 3 {
		t.Errorf("expected 3 post attempts, got %d", got)
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
	h := restatetest.NewObjectHarness(t, scheduleID, restatetest.WithInitialState("gen", int64(1)))
	s.nowFn = func() time.Time { return now }
	if _, err := s.Fire(h.ObjCtx, FireRequest{Generation: 1, ScheduleType: model.ScheduleTypeHeartbeat, DueAt: dueAt}); err != nil {
		t.Fatalf("Fire(%s) error = %v", dueAt.Format(time.RFC3339), err)
	}
	// Every heartbeat tick, dispatched or skipped, re-arms the next Fire.
	if len(fireKeys(h.Client)) != 1 {
		t.Fatalf("Fire(%s) re-armed %v, want exactly one next Fire", dueAt.Format(time.RFC3339), fireKeys(h.Client))
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

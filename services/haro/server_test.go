package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
	"github.com/azylman/aerial/services/haro/internal/tools"
)

func TestWriteJSON_Table(t *testing.T) {
	tests := []struct {
		name            string
		initialHeader   string
		status          int
		data            any
		wantStatus      int
		wantContentType string
	}{
		{
			name:            "default content type",
			initialHeader:   "",
			status:          http.StatusOK,
			data:            map[string]string{"status": "ok"},
			wantStatus:      http.StatusOK,
			wantContentType: "application/json",
		},
		{
			name:            "preserved custom content type",
			initialHeader:   "application/problem+json",
			status:          http.StatusBadRequest,
			data:            map[string]string{"error": "bad request"},
			wantStatus:      http.StatusBadRequest,
			wantContentType: "application/problem+json",
		},
		{
			name:            "unsupported type encode error",
			initialHeader:   "",
			status:          http.StatusInternalServerError,
			data:            make(chan int),
			wantStatus:      http.StatusInternalServerError,
			wantContentType: "text/plain; charset=utf-8",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			if tc.initialHeader != "" {
				rec.Header().Set("Content-Type", tc.initialHeader)
			}
			httpkit.WriteJSON(rec, tc.status, tc.data)
			if rec.Code != tc.wantStatus {
				t.Errorf("got status %d, want %d", rec.Code, tc.wantStatus)
			}
			if got := rec.Header().Get("Content-Type"); got != tc.wantContentType {
				t.Errorf("got Content-Type %q, want %q", got, tc.wantContentType)
			}
		})
	}
}

type dbConfigTestCase struct {
	name    string
	cfg     *Config
	wantErr bool
}

type dbOpTestCase struct {
	name string
	ctx  context.Context
	db   *sql.DB
}

func testDBOpenFilesAndViews(t *testing.T, tmpDir string) {
	t.Helper()
	for _, p := range []string{filepath.Join(tmpDir, "fresh1.db"), filepath.Join(tmpDir, "fresh2.db") + "?cache=shared"} {
		fdb, err := openDB(t.Context(), p)
		if err != nil {
			t.Fatalf("openDB %s failed: %v", p, err)
		}
		_ = fdb.Close()
	}

	blockerFile := filepath.Join(tmpDir, "blocker.txt")
	_ = os.WriteFile(blockerFile, []byte("data"), 0o644)
	if _, err := openDB(t.Context(), filepath.Join(blockerFile, "sub", "db.sqlite")); err == nil {
		t.Error("expected directory blocker error")
	}

	viewDBPath := filepath.Join(tmpDir, "view_conflict.db")
	vdb, err := sql.Open("sqlite", viewDBPath)
	if err != nil {
		t.Fatalf("failed to open view db: %v", err)
	}
	_, _ = vdb.Exec("CREATE VIEW cron_schedules AS SELECT 1 AS id;")
	_ = vdb.Close()
	if _, err := openDB(t.Context(), viewDBPath); err == nil {
		t.Error("expected error creating schema on conflicting view database")
	}

	oldHook := sqliteTestInitHook
	sqliteTestInitHook = nil
	if _, err := openDB(t.Context(), ":memory:"); err == nil {
		t.Error("expected error for unhooked :memory:")
	}
	sqliteTestInitHook = oldHook
}

func testDBCRUDOperations(ctx context.Context, t *testing.T, db *sql.DB) {
	t.Helper()
	now := time.Now().UTC()
	c := CronSchedule{ID: "c1", TargetID: "t1", CronExpr: "0 0 * * *", Prompt: "p", NextRunAt: now, Enabled: true}
	s := OneShotSchedule{ID: "s1", ThreadID: "t1", Prompt: "p", RunAt: now}
	if err := store.InsertCronSchedule(ctx, db, c); err != nil {
		t.Fatalf("InsertCronSchedule failed: %v", err)
	}
	if err := store.InsertOneShotSchedule(ctx, db, s); err != nil {
		t.Fatalf("InsertOneShotSchedule failed: %v", err)
	}

	crons, _ := store.ListCronSchedules(ctx, db, "t1")
	ones, _ := store.ListOneShotSchedules(ctx, db, "t1")
	if len(crons) != 1 || len(ones) != 1 {
		t.Fatalf("expected 1 cron and 1 one-shot, got %d, %d", len(crons), len(ones))
	}

	eff := "high"
	if err := store.UpdateCronSchedule(ctx, db, "c1", &eff, nil, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateCronSchedule(ctx, db, "missing-id", &eff, nil, nil, nil, nil, nil); err == nil {
		t.Error("expected error")
	}
	if err := store.UpdateCronSchedule(ctx, db, "", nil, nil, nil, nil, nil, nil); err == nil {
		t.Error("expected error")
	}
	if err := store.UpdateCronSchedule(ctx, db, "c1", nil, nil, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	if delFalse, err := store.DeleteSchedule(ctx, db, "missing"); err != nil || delFalse {
		t.Fatal(err)
	}
	if delTrue, err := store.DeleteSchedule(ctx, db, "c1"); err != nil || !delTrue {
		t.Fatal(err)
	}

	_, _ = db.Exec("INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort) VALUES ('c-bad', 't1', '', '0 0 * * *', 'p', 'UTC', 'not-a-date', TRUE, CURRENT_TIMESTAMP, 'medium')")
	if _, err := store.ListCronSchedules(ctx, db, ""); err == nil {
		t.Error("expected scan error on invalid next_run_at")
	}
	_, _ = db.Exec("INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at, effort) VALUES ('o-bad', 't1', 'p', 'not-a-date', CURRENT_TIMESTAMP, 'medium')")
	if _, err := store.ListOneShotSchedules(ctx, db, ""); err == nil {
		t.Error("expected scan error on invalid run_at")
	}
}

func runDBOpCase(t *testing.T, tc dbOpTestCase, c CronSchedule, s OneShotSchedule, eff string) {
	t.Helper()
	if err := store.InsertCronSchedule(tc.ctx, tc.db, c); err == nil {
		t.Errorf("%s: expected err InsertCron", tc.name)
	}
	if err := store.InsertOneShotSchedule(tc.ctx, tc.db, s); err == nil {
		t.Errorf("%s: expected err InsertOneShot", tc.name)
	}
	if _, err := store.ListCronSchedules(tc.ctx, tc.db, ""); err == nil {
		t.Errorf("%s: expected err ListCron", tc.name)
	}
	if _, err := store.ListOneShotSchedules(tc.ctx, tc.db, ""); err == nil {
		t.Errorf("%s: expected err ListOneShot", tc.name)
	}
	if err := store.UpdateCronSchedule(tc.ctx, tc.db, "c1", &eff, nil, nil, nil, nil, nil); err == nil {
		t.Errorf("%s: expected err UpdateCron", tc.name)
	}
	if err := store.UpdateHeartbeatSchedule(tc.ctx, tc.db, HeartbeatSchedule{ID: "hb1"}); err == nil {
		t.Errorf("%s: expected err UpdateHeartbeat", tc.name)
	}
	if _, err := store.DeleteSchedule(tc.ctx, tc.db, "c1"); err == nil {
		t.Errorf("%s: expected err Delete", tc.name)
	}
	if err := store.InsertEventTrigger(tc.ctx, tc.db, EventTrigger{}); err == nil {
		t.Errorf("%s: expected err InsertEventTrigger", tc.name)
	}
	if _, err := store.ListEventTriggers(tc.ctx, tc.db, ""); err == nil {
		t.Errorf("%s: expected err ListEventTriggers", tc.name)
	}
	if _, err := store.DeleteEventTrigger(tc.ctx, tc.db, "e1"); err == nil {
		t.Errorf("%s: expected err DeleteEventTrigger", tc.name)
	}
	if err := store.ValidateLiveEventTarget(tc.ctx, tc.db, "vanadis_build", "b1"); err == nil {
		t.Errorf("%s: expected err ValidateLiveEventTarget", tc.name)
	}
	if _, err := store.ClaimEventTriggers(tc.ctx, tc.db, "flux", []string{"aerial"}, "sync_success"); err == nil {
		t.Errorf("%s: expected err ClaimEventTriggers", tc.name)
	}
	if _, err := tools.HandleListEventWatches(tc.ctx, tc.db, nil); err == nil {
		t.Errorf("%s: expected err HandleListEventWatches", tc.name)
	}
	if _, err := tools.HandleCancelEventWatch(tc.ctx, tc.db, map[string]any{"watch_id": "w1"}); err == nil {
		t.Errorf("%s: expected err HandleCancelEventWatch", tc.name)
	}
	if _, err := tools.HandleScheduleHeartbeat(tc.ctx, tc.db, "UTC", map[string]any{"channel_id": "123456789012345678", "cron_expr": "0 0 * * *", "prompt": "p"}); err == nil {
		t.Errorf("%s: expected err HandleScheduleHeartbeat", tc.name)
	}
	if _, err := tools.HandleWatchEvent(tc.ctx, tc.db, map[string]any{"channel_id": "123456789012345678", "source": "flux", "target": "aerial", "event_type": "sync_success", "prompt": "p"}); err == nil {
		t.Errorf("%s: expected err HandleWatchEvent", tc.name)
	}
}

func testDBStandaloneFunctions(ctx context.Context, t *testing.T, db, closedDB *sql.DB) {
	t.Helper()
	if model.FirstNonEmpty("", "   ") != "" {
		t.Error("expected empty string from firstNonEmpty")
	}
	if claimed, err := store.ClaimEventTriggers(ctx, db, "", nil, ""); err != nil || len(claimed) != 0 {
		t.Errorf("expected empty, nil for empty ClaimEventTriggers, got %v, %v", claimed, err)
	}
	if _, err := store.DeleteOneShotSchedule(ctx, db, ""); err == nil {
		t.Error("expected error for empty id in DeleteOneShotSchedule")
	}
	if err := store.AdvanceCronNextRun(ctx, db, "", time.Now()); err == nil {
		t.Error("expected error for empty id in AdvanceCronNextRun")
	}
	if err := store.AdvanceHeartbeatNextRun(ctx, db, "", time.Now()); err == nil {
		t.Error("expected error for empty id in AdvanceHeartbeatNextRun")
	}
	if _, err := store.HasRunningHeartbeatRun(ctx, closedDB, "sched-1", time.Now()); err == nil {
		t.Error("expected error for closed db in HasRunningHeartbeatRun")
	}
	if _, err := store.ClaimEventTriggers(ctx, closedDB, "camera", []string{"garage"}, "person_arrived"); err == nil {
		t.Error("expected error from closed db on claim path")
	}
	_ = NewToolHandler(&Config{VanadisURL: "http://vanadis.aerial", Timezone: "UTC"}, db)
}

func TestDB_OperationsAndErrors(t *testing.T) {
	cfgTests := []dbConfigTestCase{
		{"nil config NewDB", nil, true},
		{"empty dsn", &Config{DatabaseURL: ""}, true},
		{"unsupported scheme", &Config{DatabaseURL: "mysql://localhost/db"}, true},
	}
	for _, tc := range cfgTests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewDB(t.Context(), tc.cfg); (err != nil) != tc.wantErr {
				t.Fatalf("NewDB(t.Context(), ) err=%v", err)
			}
			if _, err := OpenDB(t.Context(), tc.cfg); (err != nil) != tc.wantErr {
				t.Fatalf("OpenDB(t.Context(), ) err=%v", err)
			}
		})
	}

	testDBOpenFilesAndViews(t, t.TempDir())

	db, err := OpenDB(t.Context(), &Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	testDBCRUDOperations(ctx, t, db)

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	closedDB, err := OpenDB(t.Context(), &Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB closed failed: %v", err)
	}
	_ = closedDB.Close()

	now := time.Now().UTC()
	c := CronSchedule{ID: "c1", TargetID: "t1", CronExpr: "0 0 * * *", Prompt: "p", NextRunAt: now, Enabled: true}
	s := OneShotSchedule{ID: "s1", ThreadID: "t1", Prompt: "p", RunAt: now}
	eff := "high"

	dbOpTests := []dbOpTestCase{
		{"nil db", ctx, nil},
		{"canceled ctx", canceledCtx, db},
		{"closed db", ctx, closedDB},
	}
	for _, tc := range dbOpTests {
		t.Run(tc.name, func(t *testing.T) {
			runDBOpCase(t, tc, c, s, eff)
		})
	}

	testDBStandaloneFunctions(ctx, t, db, closedDB)
}

func TestCoverageFlushEndpoint(t *testing.T) {
	server := NewServer(&ToolHandler{})
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	doFlush := func(wantStatus int) {
		t.Helper()
		resp, err := http.Get(ts.URL + "/debug/coverage/flush")
		if err != nil || resp.StatusCode != wantStatus {
			t.Fatalf("GET /debug/coverage/flush got resp=%v, want=%d, err=%v", resp, wantStatus, err)
		}
		_ = resp.Body.Close()
	}

	oldFn, oldMetaFn := httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn
	defer func() { httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn = oldFn, oldMetaFn }()

	called, metaCalled := false, false
	httpkit.WriteCountersDirFn = func(dir string) error {
		called = true
		return nil
	}
	httpkit.WriteMetaDirFn = func(dir string) error {
		metaCalled = true
		return nil
	}

	doFlush(http.StatusOK)
	if !called || !metaCalled {
		t.Fatalf("expected flush called, called=%v, meta=%v", called, metaCalled)
	}

	httpkit.WriteMetaDirFn = func(dir string) error { return errors.New("meta flush error") }
	doFlush(http.StatusOK)

	httpkit.WriteCountersDirFn = func(dir string) error { return errors.New("flush error") }
	doFlush(http.StatusInternalServerError)
}

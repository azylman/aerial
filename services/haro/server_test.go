package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
)

type JSONRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	ID      interface{} `json:"id,omitempty"`
	Result  interface{} `json:"result,omitempty"`
	Error   *struct {
		Code    int         `json:"code"`
		Message string      `json:"message"`
		Data    interface{} `json:"data,omitempty"`
	} `json:"error,omitempty"`
}

type mockErrReader struct{}

func (m *mockErrReader) Read([]byte) (int, error) { return 0, errors.New("simulated read error") }

type mockErrWriter struct {
	header http.Header
	err    error
}

func (m *mockErrWriter) Header() http.Header {
	if m.header == nil {
		m.header = make(http.Header)
	}
	return m.header
}
func (m *mockErrWriter) Write([]byte) (int, error) { return 0, m.err }
func (m *mockErrWriter) WriteHeader(int)           {}

type errReadCloser struct{}

func (e *errReadCloser) Read([]byte) (int, error) { return 0, io.EOF }
func (e *errReadCloser) Close() error             { return errors.New("simulated close error") }

func setupTestPostgresTables(t *testing.T, db *sql.DB) {
	t.Helper()
	schema := `
	CREATE TABLE IF NOT EXISTS cron_schedules (
		id TEXT PRIMARY KEY,
		target_id TEXT NOT NULL,
		title_prefix TEXT NOT NULL DEFAULT '',
		cron_expr TEXT NOT NULL,
		prompt TEXT NOT NULL,
		timezone TEXT NOT NULL DEFAULT 'America/Los_Angeles',
		next_run_at TIMESTAMPTZ NOT NULL,
		enabled BOOLEAN NOT NULL DEFAULT TRUE,
		effort TEXT NOT NULL DEFAULT 'low',
		created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_cron_schedules_next_run_at ON cron_schedules(enabled, next_run_at);

	CREATE TABLE IF NOT EXISTS one_shot_schedules (
		id TEXT PRIMARY KEY,
		thread_id TEXT NOT NULL,
		prompt TEXT NOT NULL,
		run_at TIMESTAMPTZ NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
		effort TEXT NOT NULL DEFAULT 'low'
	);
	CREATE INDEX IF NOT EXISTS idx_one_shot_schedules_run_at ON one_shot_schedules(run_at);

	CREATE TABLE IF NOT EXISTS heartbeat_schedules (
		id TEXT PRIMARY KEY,
		target_id TEXT NOT NULL,
		title_prefix TEXT NOT NULL DEFAULT '',
		cron_expr TEXT NOT NULL,
		prompt TEXT NOT NULL,
		timezone TEXT NOT NULL DEFAULT 'UTC',
		effort TEXT NOT NULL DEFAULT 'low',
		next_run_at TIMESTAMPTZ NOT NULL,
		enabled BOOLEAN NOT NULL DEFAULT TRUE,
		created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_heartbeat_schedules_next_run ON heartbeat_schedules(enabled, next_run_at);

	CREATE TABLE IF NOT EXISTS schedule_runs (
		id TEXT PRIMARY KEY,
		schedule_id TEXT NOT NULL,
		schedule_type TEXT NOT NULL,
		message_id TEXT NOT NULL DEFAULT '',
		target_id TEXT NOT NULL,
		thread_id TEXT NOT NULL,
		title TEXT NOT NULL DEFAULT '',
		prompt TEXT NOT NULL,
		status TEXT NOT NULL DEFAULT 'enqueued',
		started_at TIMESTAMPTZ NOT NULL,
		completed_at TIMESTAMPTZ,
		duration_ms INTEGER DEFAULT 0,
		effort TEXT NOT NULL DEFAULT 'low',
		model TEXT NOT NULL DEFAULT '',
		error TEXT NOT NULL DEFAULT ''
	);
	CREATE INDEX IF NOT EXISTS idx_schedule_runs_started_at ON schedule_runs(started_at DESC);

	CREATE TABLE IF NOT EXISTS event_triggers (
		id TEXT PRIMARY KEY,
		channel_id TEXT NOT NULL,
		source TEXT NOT NULL,
		target TEXT NOT NULL,
		event_type TEXT NOT NULL,
		prompt TEXT NOT NULL,
		effort TEXT NOT NULL DEFAULT 'medium',
		created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	CREATE INDEX IF NOT EXISTS idx_event_triggers_lookup ON event_triggers (source, target, event_type);
	CREATE INDEX IF NOT EXISTS idx_event_triggers_channel ON event_triggers (channel_id, created_at ASC);

	CREATE TABLE IF NOT EXISTS vanadis_builds (
		id TEXT PRIMARY KEY,
		status TEXT NOT NULL DEFAULT 'running'
	);

	CREATE TABLE IF NOT EXISTS vanadis_chunks (
		id TEXT PRIMARY KEY,
		build_id TEXT NOT NULL DEFAULT '',
		status TEXT NOT NULL DEFAULT 'pending'
	);
	`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("failed to setup test postgres tables: %v", err)
	}
}

func setupTestServer(t *testing.T) (*Server, *httptest.Server) {
	cfg := &Config{DatabaseURL: ":memory:", Timezone: DefaultTimezone, Port: "8080"}
	db, err := OpenDB(cfg)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	setupTestPostgresTables(t, db)
	server := NewServer(NewToolHandler(cfg, db))
	return server, httptest.NewServer(server.Routes())
}

func TestNewServer_NilHandlerPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected NewServer(nil) to panic")
		}
	}()
	NewServer(nil)
}

func TestServer_HTTPEndpointsAndMCPProtocol(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()

	recH, recC, recErr := httptest.NewRecorder(), httptest.NewRecorder(), httptest.NewRecorder()
	server.handleMCP(recH, httptest.NewRequest(http.MethodGet, "/health", nil))
	server.handleMCP(recC, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil))
	server.handleMCP(recErr, httptest.NewRequest(http.MethodPost, "/mcp", &mockErrReader{}))
	if recH.Code != http.StatusOK || (recC.Code != http.StatusOK && recC.Code != http.StatusInternalServerError) || recErr.Code != http.StatusBadRequest {
		t.Fatalf("direct handleMCP failed: H=%d, C=%d, Err=%d", recH.Code, recC.Code, recErr.Code)
	}

	httpTests := []struct {
		name       string
		method     string
		path       string
		body       io.Reader
		wantStatus int
	}{
		{"health endpoint", http.MethodGet, "/health", nil, http.StatusOK},
		{"mcp get probe", http.MethodGet, "/mcp", nil, http.StatusOK},
		{"root get probe", http.MethodGet, "/", nil, http.StatusOK},
		{"method not allowed", http.MethodDelete, "/mcp", nil, http.StatusMethodNotAllowed},
		{"empty body", http.MethodPost, "/mcp", strings.NewReader("   "), http.StatusBadRequest},
		{"malformed json", http.MethodPost, "/mcp", strings.NewReader("{bad json"), http.StatusBadRequest},
		{"notification 204", http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`), http.StatusNoContent},
		{"ping request", http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":100,"method":"ping"}`), http.StatusOK},
		{"initialize request", http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":101,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`), http.StatusOK},
		{"tools list request", http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":102,"method":"tools/list","params":{}}`), http.StatusOK},
		{"mcp post without content-type", http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":104,"method":"ping"}`), http.StatusOK},
		{"unknown method", http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":103,"method":"unknown/method"}`), http.StatusBadRequest},
	}
	for _, tc := range httpTests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, ts.URL+tc.path, tc.body)
			if err != nil {
				t.Fatalf("NewRequest error: %v", err)
			}
			if tc.method == http.MethodPost && tc.name != "mcp post without content-type" {
				req.Header.Set("Content-Type", "application/json")
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("request failed: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.wantStatus && (tc.name == "notification 204" && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted) {
				t.Fatalf("%s got status %d, want %d", tc.name, resp.StatusCode, tc.wantStatus)
			}
		})
	}

	rpcCtx := context.Background()
	_ = InsertCronSchedule(rpcCtx, server.handler.db, CronSchedule{
		ID:        "test-cron-id",
		TargetID:  "123",
		CronExpr:  "0 0 * * *",
		Prompt:    "test",
		NextRunAt: time.Now().UTC(),
		Enabled:   true,
	})
	_ = InsertHeartbeatSchedule(rpcCtx, server.handler.db, HeartbeatSchedule{
		ID:        "test-hb-id",
		TargetID:  "123",
		CronExpr:  "0 0 * * *",
		Prompt:    "test",
		NextRunAt: time.Now().UTC(),
		Enabled:   true,
	})
	_ = InsertOneShotSchedule(rpcCtx, server.handler.db, OneShotSchedule{
		ID:       "test-once-id",
		ThreadID: "123",
		Prompt:   "test",
		RunAt:    time.Now().UTC(),
	})

	rpcTests := []struct {
		name        string
		reqPayload  string
		wantToolErr bool
		wantRPCErr  bool
	}{
		{"schedule_recurring alias", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"scheduler_schedule_recurring","arguments":{"channel_id":"123","cron_expression":"0 20 * * 5","prompt":"plan","effort":"high"}}}`, false, false},
		{"update_cron_schedule", `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"update_cron_schedule","arguments":{"schedule_id":"missing-id","effort":"low"}}}`, true, false},
		{"update_cron_schedule success", `{"jsonrpc":"2.0","id":15,"method":"tools/call","params":{"name":"update_cron_schedule","arguments":{"schedule_id":"test-cron-id","effort":"medium"}}}`, false, false},
		{"schedule_once direct", `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"schedule_once","arguments":{"target_id":"456","run_at":"15m","prompt":"trash"}}}`, false, false},
		{"schedule_once alias validation", `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"scheduler_schedule_once","arguments":{"target_id":"","run_at":"15m","prompt":"trash"}}}`, true, false},
		{"list_schedules", `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"list_schedules","arguments":{}}}`, false, false},
		{"get_schedule success", `{"jsonrpc":"2.0","id":18,"method":"tools/call","params":{"name":"get_schedule","arguments":{"schedule_id":"test-cron-id"}}}`, false, false},
		{"get_schedule missing", `{"jsonrpc":"2.0","id":19,"method":"tools/call","params":{"name":"get_schedule","arguments":{"schedule_id":"missing-id"}}}`, true, false},
		{"schedule_heartbeat success", `{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"schedule_heartbeat","arguments":{"channel_id":"123456789012345678","cron_expr":"0 0 * * *","prompt":"monitor disk"}}}`, false, false},
		{"schedule_heartbeat invalid", `{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"schedule_heartbeat","arguments":{"channel_id":"invalid-snow","cron_expr":"0 0 * * *","prompt":"monitor disk"}}}`, true, false},
		{"update_heartbeat missing", `{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"update_heartbeat","arguments":{"id":"missing-hb","effort":"low"}}}`, true, false},
		{"update_heartbeat success", `{"jsonrpc":"2.0","id":16,"method":"tools/call","params":{"name":"update_heartbeat","arguments":{"id":"test-hb-id","effort":"medium"}}}`, false, false},
		{"cancel_schedule missing", `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"cancel_schedule","arguments":{"schedule_id":"none"}}}`, true, false},
		{"cancel_schedule success", `{"jsonrpc":"2.0","id":17,"method":"tools/call","params":{"name":"cancel_schedule","arguments":{"schedule_id":"test-once-id"}}}`, false, false},
		{"watch_event success", `{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"watch_event","arguments":{"channel_id":"123456789012345678","source":"hangar","target":"aerial","event_type":"sync_success","prompt":"deploy notification"}}}`, false, false},
		{"watch_event invalid", `{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"watch_event","arguments":{"channel_id":"bad-snowflake","source":"hangar","target":"aerial","event_type":"sync_success","prompt":"deploy notification"}}}`, true, false},
		{"list_event_watches", `{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"list_event_watches","arguments":{}}}`, false, false},
		{"cancel_event_watch missing", `{"jsonrpc":"2.0","id":14,"method":"tools/call","params":{"name":"cancel_event_watch","arguments":{"watch_id":"missing-watch"}}}`, true, false},
		{"unknown tool", `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"non_existent","arguments":{}}}`, false, true},
	}
	for _, tc := range rpcTests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Post(ts.URL+"/mcp", "application/json", strings.NewReader(tc.reqPayload))
			if err != nil {
				t.Fatalf("POST /mcp failed: %v", err)
			}
			defer resp.Body.Close()
			var rpcResp JSONRPCResponse
			if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
				t.Fatalf("decode failed: %v", err)
			}
			if tc.wantRPCErr && rpcResp.Error == nil {
				t.Fatalf("expected RPC error, got %+v", rpcResp)
			} else if !tc.wantRPCErr && rpcResp.Error != nil {
				t.Fatalf("unexpected RPC error: %+v", rpcResp.Error)
			}
			if tc.wantToolErr {
				resMap, _ := rpcResp.Result.(map[string]interface{})
				if isErr, _ := resMap["isError"].(bool); !isErr {
					t.Fatalf("expected isError=true, got %+v", resMap)
				}
			}
		})
	}
}

func TestServer_WriteResponseAndDisconnect(t *testing.T) {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	httpkit.WriteResponse(rec, req, "haro", []byte("ok"))
	if rec.Body.String() != "ok" {
		t.Fatalf("expected ok, got %s", rec.Body.String())
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	reqCanceled := httptest.NewRequest(http.MethodGet, "/health", nil).WithContext(ctx)

	errTests := []struct {
		name string
		req  *http.Request
		err  error
		want bool
	}{
		{"nil err", req, nil, false},
		{"canceled context", reqCanceled, errors.New("write err"), true},
		{"epipe", req, syscall.EPIPE, true},
		{"econnreset", req, syscall.ECONNRESET, true},
		{"net err closed", req, net.ErrClosed, true},
		{"generic error", req, errors.New("disk full"), false},
	}
	for _, tc := range errTests {
		t.Run(tc.name, func(t *testing.T) {
			if got := httpkit.IsClientDisconnect(tc.req, tc.err); got != tc.want {
				t.Errorf("isClientDisconnect() = %v, want %v", got, tc.want)
			}
		})
	}

	httpkit.WriteResponse(&mockErrWriter{err: errors.New("err")}, req, "haro", []byte("data"))
	for _, c := range []io.Closer{nil, io.NopCloser(strings.NewReader("")), &errReadCloser{}} {
		httpkit.CloseWarn(c, "test")
	}
}

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

func TestDB_OperationsAndErrors(t *testing.T) {
	cfgTests := []struct {
		name    string
		cfg     *Config
		wantErr bool
	}{
		{"nil config NewDB", nil, true},
		{"empty dsn", &Config{DatabaseURL: ""}, true},
		{"unsupported scheme", &Config{DatabaseURL: "mysql://localhost/db"}, true},
	}
	for _, tc := range cfgTests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewDB(tc.cfg); (err != nil) != tc.wantErr {
				t.Fatalf("NewDB() err=%v", err)
			}
			if _, err := OpenDB(tc.cfg); (err != nil) != tc.wantErr {
				t.Fatalf("OpenDB() err=%v", err)
			}
		})
	}

	tmpDir := t.TempDir()
	for _, p := range []string{filepath.Join(tmpDir, "fresh1.db"), filepath.Join(tmpDir, "fresh2.db") + "?cache=shared"} {
		fdb, err := openDB(p)
		if err != nil {
			t.Fatalf("openDB %s failed: %v", p, err)
		}
		_ = fdb.Close()
	}

	blockerFile := filepath.Join(tmpDir, "blocker.txt")
	_ = os.WriteFile(blockerFile, []byte("data"), 0o644)
	if _, err := openDB(filepath.Join(blockerFile, "sub", "db.sqlite")); err == nil {
		t.Error("expected directory blocker error")
	}

	viewDBPath := filepath.Join(tmpDir, "view_conflict.db")
	vdb, err := sql.Open("sqlite", viewDBPath)
	if err != nil {
		t.Fatalf("failed to open view db: %v", err)
	}
	_, _ = vdb.Exec("CREATE VIEW cron_schedules AS SELECT 1 AS id;")
	_ = vdb.Close()
	if _, err := openDB(viewDBPath); err == nil {
		t.Error("expected error creating schema on conflicting view database")
	}

	oldHook := sqliteTestInitHook
	sqliteTestInitHook = nil
	if _, err := openDB(":memory:"); err == nil {
		t.Error("expected error for unhooked :memory:")
	}
	sqliteTestInitHook = oldHook

	origMax, origBase := postgresMaxAttempts, postgresRetryBase
	postgresMaxAttempts, postgresRetryBase = 1, time.Millisecond
	_, errPg := openDB("postgres://invalid:invalid@127.0.0.1:59997/db?sslmode=disable")
	postgresMaxAttempts, postgresRetryBase = origMax, origBase
	if errPg == nil {
		t.Error("expected postgres connection failure")
	}

	db, err := OpenDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	now := time.Now().UTC()
	c := CronSchedule{ID: "c1", TargetID: "t1", CronExpr: "0 0 * * *", Prompt: "p", NextRunAt: now, Enabled: true}
	s := OneShotSchedule{ID: "s1", ThreadID: "t1", Prompt: "p", RunAt: now}
	if err := InsertCronSchedule(ctx, db, c); err != nil {
		t.Fatalf("InsertCronSchedule failed: %v", err)
	}
	if err := InsertOneShotSchedule(ctx, db, s); err != nil {
		t.Fatalf("InsertOneShotSchedule failed: %v", err)
	}

	crons, _ := ListCronSchedules(ctx, db, "t1")
	ones, _ := ListOneShotSchedules(ctx, db, "t1")
	if len(crons) != 1 || len(ones) != 1 {
		t.Fatalf("expected 1 cron and 1 one-shot, got %d, %d", len(crons), len(ones))
	}

	eff := "high"
	if err := UpdateCronSchedule(ctx, db, "c1", &eff, nil, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := UpdateCronSchedule(ctx, db, "missing-id", &eff, nil, nil, nil, nil, nil); err == nil {
		t.Error("expected error")
	}
	if err := UpdateCronSchedule(ctx, db, "", nil, nil, nil, nil, nil, nil); err == nil {
		t.Error("expected error")
	}
	if err := UpdateCronSchedule(ctx, db, "c1", nil, nil, nil, nil, nil, nil); err != nil {
		t.Fatal(err)
	}

	if delFalse, err := DeleteSchedule(ctx, db, "missing"); err != nil || delFalse {
		t.Fatal(err)
	}
	if delTrue, err := DeleteSchedule(ctx, db, "c1"); err != nil || !delTrue {
		t.Fatal(err)
	}

	_, _ = db.Exec("INSERT INTO cron_schedules (id, target_id, title_prefix, cron_expr, prompt, timezone, next_run_at, enabled, created_at, effort) VALUES ('c-bad', 't1', '', '0 0 * * *', 'p', 'UTC', 'not-a-date', TRUE, CURRENT_TIMESTAMP, 'medium')")
	if _, err := ListCronSchedules(ctx, db, ""); err == nil {
		t.Error("expected scan error on invalid next_run_at")
	}
	_, _ = db.Exec("INSERT INTO one_shot_schedules (id, thread_id, prompt, run_at, created_at, effort) VALUES ('o-bad', 't1', 'p', 'not-a-date', CURRENT_TIMESTAMP, 'medium')")
	if _, err := ListOneShotSchedules(ctx, db, ""); err == nil {
		t.Error("expected scan error on invalid run_at")
	}

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	closedDB, err := OpenDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB closed failed: %v", err)
	}
	_ = closedDB.Close()

	dbOpTests := []struct {
		name string
		ctx  context.Context
		db   *sql.DB
	}{
		{"nil db", ctx, nil},
		{"canceled ctx", canceledCtx, db},
		{"closed db", ctx, closedDB},
	}
	for _, tc := range dbOpTests {
		t.Run(tc.name, func(t *testing.T) {
			if err := InsertCronSchedule(tc.ctx, tc.db, c); err == nil {
				t.Errorf("%s: expected err InsertCron", tc.name)
			}
			if err := InsertOneShotSchedule(tc.ctx, tc.db, s); err == nil {
				t.Errorf("%s: expected err InsertOneShot", tc.name)
			}
			if _, err := ListCronSchedules(tc.ctx, tc.db, ""); err == nil {
				t.Errorf("%s: expected err ListCron", tc.name)
			}
			if _, err := ListOneShotSchedules(tc.ctx, tc.db, ""); err == nil {
				t.Errorf("%s: expected err ListOneShot", tc.name)
			}
			if err := UpdateCronSchedule(tc.ctx, tc.db, "c1", &eff, nil, nil, nil, nil, nil); err == nil {
				t.Errorf("%s: expected err UpdateCron", tc.name)
			}
			if _, err := DeleteSchedule(tc.ctx, tc.db, "c1"); err == nil {
				t.Errorf("%s: expected err Delete", tc.name)
			}
			if err := InsertEventTrigger(tc.ctx, tc.db, EventTrigger{}); err == nil {
				t.Errorf("%s: expected err InsertEventTrigger", tc.name)
			}
			if _, err := ListEventTriggers(tc.ctx, tc.db, ""); err == nil {
				t.Errorf("%s: expected err ListEventTriggers", tc.name)
			}
			if _, err := DeleteEventTrigger(tc.ctx, tc.db, "e1"); err == nil {
				t.Errorf("%s: expected err DeleteEventTrigger", tc.name)
			}
			if err := ValidateLiveEventTarget(tc.ctx, tc.db, "vanadis_build", "b1"); err == nil {
				t.Errorf("%s: expected err ValidateLiveEventTarget", tc.name)
			}
			if _, err := CheckEventTriggers(tc.ctx, tc.db, "hangar", []string{"aerial"}, "sync_success"); err == nil {
				t.Errorf("%s: expected err CheckEventTriggers", tc.name)
			}
			if _, err := ClaimEventTriggers(tc.ctx, tc.db, "hangar", []string{"aerial"}, "sync_success"); err == nil {
				t.Errorf("%s: expected err ClaimEventTriggers", tc.name)
			}
			if _, err := HandleListEventWatches(tc.ctx, tc.db, nil); err == nil {
				t.Errorf("%s: expected err HandleListEventWatches", tc.name)
			}
			if _, err := HandleCancelEventWatch(tc.ctx, tc.db, map[string]any{"watch_id": "w1"}); err == nil {
				t.Errorf("%s: expected err HandleCancelEventWatch", tc.name)
			}
			if _, err := HandleScheduleHeartbeat(tc.ctx, tc.db, "UTC", map[string]any{"channel_id": "123456789012345678", "cron_expr": "0 0 * * *", "prompt": "p"}); err == nil {
				t.Errorf("%s: expected err HandleScheduleHeartbeat", tc.name)
			}
			if _, err := HandleWatchEvent(tc.ctx, tc.db, map[string]any{"channel_id": "123456789012345678", "source": "hangar", "target": "aerial", "event_type": "sync_success", "prompt": "p"}); err == nil {
				t.Errorf("%s: expected err HandleWatchEvent", tc.name)
			}
		})
	}
	if firstNonEmpty("", "   ") != "" {
		t.Error("expected empty string from firstNonEmpty")
	}
	if matched, err := CheckEventTriggers(ctx, db, "", nil, ""); err != nil || matched {
		t.Errorf("expected false, nil for empty CheckEventTriggers, got %v, %v", matched, err)
	}
	if claimed, err := ClaimEventTriggers(ctx, db, "", nil, ""); err != nil || len(claimed) != 0 {
		t.Errorf("expected empty, nil for empty ClaimEventTriggers, got %v, %v", claimed, err)
	}
	_ = NewToolHandler(&Config{VanadisURL: "http://vanadis.aerial", Timezone: "UTC"}, db)
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

func TestOrdinal(t *testing.T) {
	tests := []struct {
		name string
		num  int
		want string
	}{
		{"first", 1, "1st"},
		{"second", 2, "2nd"},
		{"third", 3, "3rd"},
		{"fourth", 4, "4th"},
		{"eleventh", 11, "11th"},
		{"twelfth", 12, "12th"},
		{"thirteenth", 13, "13th"},
		{"twenty-first", 21, "21st"},
		{"twenty-second", 22, "22nd"},
		{"twenty-third", 23, "23rd"},
		{"negative number", -1, "-1st"},
		{"negative eleventh", -11, "-11th"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := Ordinal(tc.num)
			if got != tc.want {
				t.Errorf("Ordinal(%d) = %q, want %q", tc.num, got, tc.want)
			}
		})
	}
}

func TestFormatCronDescription(t *testing.T) {
	tests := []struct {
		name     string
		cronExpr string
		want     string
	}{
		{"empty", "", ""},
		{"yearly", "@yearly", "Every year on Jan 1st at 00:00"},
		{"annually", "@annually", "Every year on Jan 1st at 00:00"},
		{"monthly", "@monthly", "1st of every month at 00:00"},
		{"weekly", "@weekly", "Every week on Sunday at 00:00"},
		{"daily", "@daily", "Every day at 00:00"},
		{"midnight", "@midnight", "Every day at 00:00"},
		{"hourly", "@hourly", "Every hour"},
		{"invalid field count", "0 0 * *", "0 0 * *"},
		{"every minute", "* * * * *", "Every minute"},
		{"step minutes", "*/15 * * * *", "Every 15 minutes"},
		{"step hours", "0 */2 * * *", "Every 2 hours"},
		{"invalid min or hour", "xx yy * * *", "xx yy * * *"},
		{"every day at 09:30", "30 9 * * *", "Every day at 09:30"},
		{"weekdays numeric", "0 9 * * 1-5", "Weekdays (Mon–Fri) at 09:00"},
		{"weekdays text", "0 9 * * MON-FRI", "Weekdays (Mon–Fri) at 09:00"},
		{"weekends numeric 0-6", "0 10 * * 0,6", "Weekends (Sat–Sun) at 10:00"},
		{"weekends numeric 6-0", "0 10 * * 6,0", "Weekends (Sat–Sun) at 10:00"},
		{"weekends text", "0 10 * * SAT,SUN", "Weekends (Sat–Sun) at 10:00"},
		{"single day numeric", "0 12 * * 2", "Every Tuesday at 12:00"},
		{"multiple days list", "0 12 * * 1,3,5", "Mon, Wed, Fri at 12:00"},
		{"specific day of month", "15 14 1 * *", "1st of every month at 14:15"},
		{"day of month and month", "0 8 25 12 *", "Every year on Dec 25th at 08:00"},
		{"multiple days text", "0 12 * * MON,WED", "MON, WED at 12:00"},
		{"day of month non-numeric month", "0 8 25 FOO *", "At 08:00 (cron: 0 8 25 FOO *)"},
		{"non-numeric day of month with month", "0 8 1-5 12 *", "At 08:00 (cron: 0 8 1-5 12 *)"},
		{"other cron pattern", "0 9 * 5 1-3", "At 09:00 (cron: 0 9 * 5 1-3)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := FormatCronDescription(tc.cronExpr)
			if got != tc.want {
				t.Errorf("FormatCronDescription(%q) = %q, want %q", tc.cronExpr, got, tc.want)
			}
		})
	}
}

func TestSanitizeScheduleTextAndFormatters(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		wantText string
	}{
		{"empty string", "", ""},
		{"clean string", "hello world", "hello world"},
		{"github pat classic", "token ghp_1234567890abcdefghijklmnopqrstuvwxyz", "token [REDACTED]"},
		{"github pat fine grained", "my github_pat_11AAAAAAA_bbbbbbbbbbbbbb token", "my [REDACTED] token"},
		{"bearer token", "Authorization: Bearer mysecrettoken123456789", "Authorization: [REDACTED]"},
		{"key value secret assignment", "API_KEY: supersecretpassword", "API_KEY: [REDACTED]"},
		{"key value token assignment", "TOKEN=my_top_secret_token", "TOKEN=[REDACTED]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := SanitizeScheduleText(tc.input)
			if got != tc.wantText {
				t.Errorf("SanitizeScheduleText(%q) = %q, want %q", tc.input, got, tc.wantText)
			}
		})
	}
}

func TestFormatScheduleHelpers(t *testing.T) {
	tests := []struct {
		name          string
		cronCount     int
		hbCount       int
		oneShotCount  int
		wantChannelID string
	}{
		{"empty slices", 0, 0, 0, ""},
		{"populated slices", 1, 1, 1, "chan-123"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var crons []CronSchedule
			var hbs []HeartbeatSchedule
			var oneshots []OneShotSchedule
			if tc.cronCount > 0 {
				crons = append(crons, CronSchedule{
					ID:          "cron-1",
					TargetID:    tc.wantChannelID,
					TitlePrefix: "Report GITHUB_PAT=ghp_1234567890abcdef",
					CronExpr:    "0 9 * * 1-5",
					Prompt:      "Check ghp_secret",
					Timezone:    "America/Los_Angeles",
					NextRunAt:   time.Now().UTC(),
					Enabled:     true,
					Effort:      "low",
				})
			}
			if tc.hbCount > 0 {
				hbs = append(hbs, HeartbeatSchedule{
					ID:          "hb-1",
					TargetID:    tc.wantChannelID,
					TitlePrefix: "HB Alert",
					CronExpr:    "*/5 * * * *",
					Prompt:      "Heartbeat Bearer secrettoken123456",
					Timezone:    "UTC",
					Effort:      "high",
					NextRunAt:   time.Now().UTC(),
					Enabled:     true,
				})
			}
			if tc.oneShotCount > 0 {
				oneshots = append(oneshots, OneShotSchedule{
					ID:       "os-1",
					ThreadID: tc.wantChannelID,
					Prompt:   "Remind Bearer secrettoken123456",
					RunAt:    time.Now().UTC(),
					Effort:   "medium",
				})
			}

			formattedCrons := FormatCronSchedulesWithDesc(crons)
			formattedHBs := FormatHeartbeatSchedulesWithDesc(hbs)
			formattedOS := FormatOneShotSchedulesJSON(oneshots)

			if len(formattedCrons) != tc.cronCount {
				t.Fatalf("FormatCronSchedulesWithDesc got len=%d, want %d", len(formattedCrons), tc.cronCount)
			}
			if len(formattedHBs) != tc.hbCount {
				t.Fatalf("FormatHeartbeatSchedulesWithDesc got len=%d, want %d", len(formattedHBs), tc.hbCount)
			}
			if len(formattedOS) != tc.oneShotCount {
				t.Fatalf("FormatOneShotSchedulesJSON got len=%d, want %d", len(formattedOS), tc.oneShotCount)
			}

			if tc.cronCount > 0 {
				if formattedCrons[0].ChannelID != tc.wantChannelID || formattedCrons[0].TargetID != tc.wantChannelID {
					t.Errorf("channel/target id mismatch: %v", formattedCrons[0])
				}
				if strings.Contains(formattedCrons[0].TitlePrefix, "ghp_") || strings.Contains(formattedCrons[0].Prompt, "ghp_") {
					t.Errorf("cron text not sanitized: %v", formattedCrons[0])
				}
				if formattedCrons[0].CronDescription == "" {
					t.Errorf("cron description missing")
				}
			}
			if tc.hbCount > 0 {
				if strings.Contains(formattedHBs[0].Prompt, "secrettoken") {
					t.Errorf("heartbeat text not sanitized: %v", formattedHBs[0])
				}
			}
			if tc.oneShotCount > 0 {
				if strings.Contains(formattedOS[0].Prompt, "secrettoken") {
					t.Errorf("oneshot text not sanitized: %v", formattedOS[0])
				}
			}
		})
	}
}

func TestHandleSchedules_Table(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	db := server.handler.db

	// Seed one cron schedule with sensitive text
	err := InsertCronSchedule(ctx, db, CronSchedule{
		ID:          "cron-1",
		TargetID:    "123456789012345678",
		TitlePrefix: "Cron Token: ghp_12345678901234567890",
		CronExpr:    "0 9 * * 1-5",
		Prompt:      "Prompt with Bearer secrettoken123456 and TOKEN=secret",
		Timezone:    "America/Los_Angeles",
		NextRunAt:   time.Now().UTC().Add(1 * time.Hour),
		Enabled:     true,
		Effort:      "low",
		CreatedAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("failed to insert cron: %v", err)
	}

	// Seed one heartbeat schedule
	err = InsertHeartbeatSchedule(ctx, db, HeartbeatSchedule{
		ID:          "hb-1",
		TargetID:    "123456789012345678",
		TitlePrefix: "HB Alert",
		CronExpr:    "*/5 * * * *",
		Prompt:      "Monitor with github_pat_11AAA_bbb",
		Timezone:    "UTC",
		Effort:      "medium",
		NextRunAt:   time.Now().UTC().Add(30 * time.Minute),
		Enabled:     true,
		CreatedAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("failed to insert heartbeat: %v", err)
	}

	// Seed one one-shot schedule
	err = InsertOneShotSchedule(ctx, db, OneShotSchedule{
		ID:        "os-1",
		ThreadID:  "123456789012345678",
		Prompt:    "One shot prompt with API_KEY: secretpass",
		Effort:    "high",
		RunAt:     time.Now().UTC().Add(15 * time.Minute),
		CreatedAt: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("failed to insert oneshot: %v", err)
	}

	// Seed schedule runs (success and failure)
	_ = CreateScheduleRun(ctx, db, ScheduleRun{
		ID:           "run-1",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
		TargetID:     "123456789012345678",
		ThreadID:     "123456789012345678",
		Title:        "Run 1",
		Prompt:       "Run Prompt with ghp_12345678901234567890",
		Status:       "completed",
		StartedAt:    time.Now().UTC().Add(-1 * time.Hour),
		DurationMs:   100,
	})
	_ = CreateScheduleRun(ctx, db, ScheduleRun{
		ID:           "run-2",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
		TargetID:     "123456789012345678",
		ThreadID:     "123456789012345678",
		Title:        "Run 2",
		Prompt:       "Run Prompt 2",
		Status:       "failed",
		StartedAt:    time.Now().UTC().Add(-2 * time.Hour),
		DurationMs:   50,
		Error:        "Error with TOKEN=badtoken",
	})

	closedDB, err := OpenDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, closedDB))

	tests := []struct {
		name       string
		method     string
		useServer  *Server
		wantStatus int
		wantTotal  int
	}{
		{
			name:       "valid get schedules with seeded data",
			method:     http.MethodGet,
			useServer:  server,
			wantStatus: http.StatusOK,
			wantTotal:  3,
		},
		{
			name:       "cached get schedules hit",
			method:     http.MethodGet,
			useServer:  server,
			wantStatus: http.StatusOK,
			wantTotal:  3,
		},
		{
			name:       "method not allowed post",
			method:     http.MethodPost,
			useServer:  server,
			wantStatus: http.StatusMethodNotAllowed,
			wantTotal:  0,
		},
		{
			name:       "database error on closed db",
			method:     http.MethodGet,
			useServer:  serverClosed,
			wantStatus: http.StatusInternalServerError,
			wantTotal:  0,
		},
		{
			name:       "nil db server error",
			method:     http.MethodGet,
			useServer:  &Server{},
			wantStatus: http.StatusInternalServerError,
			wantTotal:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, "/schedules", nil)
			tc.useServer.handleSchedules(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("%s got status %d, want %d", tc.name, rec.Code, tc.wantStatus)
			}
			if tc.wantStatus == http.StatusOK {
				var resp SchedulesResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("failed to decode response: %v", err)
				}
				if resp.Status != "ok" {
					t.Errorf("expected status 'ok', got %q", resp.Status)
				}
				if resp.Summary.TotalActive != tc.wantTotal {
					t.Errorf("got TotalActive=%d, want %d", resp.Summary.TotalActive, tc.wantTotal)
				}
				if len(resp.Crons) > 0 && strings.Contains(resp.Crons[0].Prompt, "secrettoken") {
					t.Errorf("secret not redacted from cron prompt: %s", resp.Crons[0].Prompt)
				}
				if len(resp.Heartbeats) > 0 && strings.Contains(resp.Heartbeats[0].Prompt, "github_pat_") {
					t.Errorf("secret not redacted from heartbeat prompt: %s", resp.Heartbeats[0].Prompt)
				}
				if len(resp.OneShots) > 0 && strings.Contains(resp.OneShots[0].Prompt, "secretpass") {
					t.Errorf("secret not redacted from oneshot prompt: %s", resp.OneShots[0].Prompt)
				}
			}
		})
	}
}

func TestHandleScheduleRuns_Table(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()

	ctx := context.Background()
	db := server.handler.db

	for i := 1; i <= 5; i++ {
		status := "completed"
		if i%2 == 0 {
			status = "failed"
		}
		_ = CreateScheduleRun(ctx, db, ScheduleRun{
			ID:           fmt.Sprintf("run-%d", i),
			ScheduleID:   fmt.Sprintf("sched-%d", i%2+1),
			ScheduleType: "cron",
			TargetID:     "123456789012345678",
			ThreadID:     "123456789012345678",
			Title:        fmt.Sprintf("Run %d Title with ghp_12345678901234567890", i),
			Prompt:       fmt.Sprintf("Prompt %d with Bearer mysecrettoken123", i),
			Status:       status,
			StartedAt:    time.Now().UTC().Add(time.Duration(-i) * time.Hour),
			DurationMs:   int64(i * 10),
			Error:        "Error with TOKEN=supersecret",
		})
	}

	closedDB, err := OpenDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, closedDB))

	tests := []struct {
		name       string
		method     string
		url        string
		useServer  *Server
		wantStatus int
		wantCount  int
		wantTotal  int
	}{
		{
			name:       "default pagination",
			method:     http.MethodGet,
			url:        "/schedules/runs",
			useServer:  server,
			wantStatus: http.StatusOK,
			wantCount:  5,
			wantTotal:  5,
		},
		{
			name:       "custom limit and offset",
			method:     http.MethodGet,
			url:        "/schedules/runs?limit=2&offset=1",
			useServer:  server,
			wantStatus: http.StatusOK,
			wantCount:  2,
			wantTotal:  5,
		},
		{
			name:       "filter by schedule_id",
			method:     http.MethodGet,
			url:        "/schedules/runs?schedule_id=sched-2",
			useServer:  server,
			wantStatus: http.StatusOK,
			wantCount:  3,
			wantTotal:  3,
		},
		{
			name:       "filter by status",
			method:     http.MethodGet,
			url:        "/schedules/runs?status=failed",
			useServer:  server,
			wantStatus: http.StatusOK,
			wantCount:  2,
			wantTotal:  2,
		},
		{
			name:       "large limit clamped to 100",
			method:     http.MethodGet,
			url:        "/schedules/runs?limit=200",
			useServer:  server,
			wantStatus: http.StatusOK,
			wantCount:  5,
			wantTotal:  5,
		},
		{
			name:       "negative offset clamped to 0",
			method:     http.MethodGet,
			url:        "/schedules/runs?offset=-5",
			useServer:  server,
			wantStatus: http.StatusOK,
			wantCount:  5,
			wantTotal:  5,
		},
		{
			name:       "negative limit clamped to 50",
			method:     http.MethodGet,
			url:        "/schedules/runs?limit=-10",
			useServer:  server,
			wantStatus: http.StatusOK,
			wantCount:  5,
			wantTotal:  5,
		},
		{
			name:       "method not allowed",
			method:     http.MethodPost,
			url:        "/schedules/runs",
			useServer:  server,
			wantStatus: http.StatusMethodNotAllowed,
			wantCount:  0,
			wantTotal:  0,
		},
		{
			name:       "closed database error",
			method:     http.MethodGet,
			url:        "/schedules/runs",
			useServer:  serverClosed,
			wantStatus: http.StatusInternalServerError,
			wantCount:  0,
			wantTotal:  0,
		},
		{
			name:       "nil server db error",
			method:     http.MethodGet,
			url:        "/schedules/runs",
			useServer:  &Server{},
			wantStatus: http.StatusInternalServerError,
			wantCount:  0,
			wantTotal:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.url, nil)
			tc.useServer.handleScheduleRuns(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("%s got status %d, want %d", tc.name, rec.Code, tc.wantStatus)
			}
			if tc.wantStatus == http.StatusOK {
				var resp ScheduleRunsResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("failed to decode response: %v", err)
				}
				if resp.Status != "ok" {
					t.Errorf("expected status 'ok', got %q", resp.Status)
				}
				if len(resp.Runs) != tc.wantCount {
					t.Errorf("got %d runs, want %d", len(resp.Runs), tc.wantCount)
				}
				if resp.Total != tc.wantTotal {
					t.Errorf("got Total=%d, want %d", resp.Total, tc.wantTotal)
				}
				for _, r := range resp.Runs {
					if strings.Contains(r.Prompt, "mysecrettoken") {
						t.Errorf("prompt not sanitized: %s", r.Prompt)
					}
					if strings.Contains(r.Title, "ghp_") {
						t.Errorf("title not sanitized: %s", r.Title)
					}
					if strings.Contains(r.Error, "supersecret") {
						t.Errorf("error not sanitized: %s", r.Error)
					}
				}
			}
		})
	}
}

func TestHandleScheduleOneShot_Table(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()

	closedDB, err := OpenDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, closedDB))

	tests := []struct {
		name       string
		method     string
		body       string
		useServer  *Server
		wantStatus int
		wantID     string
	}{
		{
			name:       "valid with custom id",
			method:     http.MethodPost,
			body:       fmt.Sprintf(`{"id":"os-custom-1","thread_id":"123","prompt":"test prompt","run_at":%q,"effort":"high"}`, time.Now().UTC().Add(time.Hour).Format(time.RFC3339)),
			useServer:  server,
			wantStatus: http.StatusOK,
			wantID:     "os-custom-1",
		},
		{
			name:       "valid with auto-generated id",
			method:     http.MethodPost,
			body:       fmt.Sprintf(`{"thread_id":"123","prompt":"test prompt","run_at":%q}`, time.Now().UTC().Add(time.Hour).Format(time.RFC3339)),
			useServer:  server,
			wantStatus: http.StatusOK,
			wantID:     "",
		},
		{
			name:       "missing thread_id",
			method:     http.MethodPost,
			body:       fmt.Sprintf(`{"prompt":"test prompt","run_at":%q}`, time.Now().UTC().Add(time.Hour).Format(time.RFC3339)),
			useServer:  server,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing prompt",
			method:     http.MethodPost,
			body:       fmt.Sprintf(`{"thread_id":"123","run_at":%q}`, time.Now().UTC().Add(time.Hour).Format(time.RFC3339)),
			useServer:  server,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "zero run_at",
			method:     http.MethodPost,
			body:       `{"thread_id":"123","prompt":"test prompt"}`,
			useServer:  server,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "malformed json body",
			method:     http.MethodPost,
			body:       `{not valid json`,
			useServer:  server,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "method not allowed get",
			method:     http.MethodGet,
			body:       "",
			useServer:  server,
			wantStatus: http.StatusMethodNotAllowed,
		},
		{
			name:       "closed database error",
			method:     http.MethodPost,
			body:       fmt.Sprintf(`{"thread_id":"123","prompt":"test prompt","run_at":%q}`, time.Now().UTC().Add(time.Hour).Format(time.RFC3339)),
			useServer:  serverClosed,
			wantStatus: http.StatusInternalServerError,
		},
		{
			name:       "nil server db error",
			method:     http.MethodPost,
			body:       fmt.Sprintf(`{"thread_id":"123","prompt":"test prompt","run_at":%q}`, time.Now().UTC().Add(time.Hour).Format(time.RFC3339)),
			useServer:  &Server{},
			wantStatus: http.StatusInternalServerError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, "/schedules/oneshot", strings.NewReader(tc.body))
			tc.useServer.handleScheduleOneShot(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("%s got status %d, want %d, body: %s", tc.name, rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantStatus == http.StatusOK {
				var resp map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("failed to decode response: %v", err)
				}
				if resp["status"] != "ok" {
					t.Errorf("expected status 'ok', got %q", resp["status"])
				}
				if tc.wantID != "" && resp["id"] != tc.wantID {
					t.Errorf("expected id %q, got %q", tc.wantID, resp["id"])
				}
				if resp["id"] == "" {
					t.Errorf("expected non-empty id")
				}
			}
		})
	}
}

func TestDirectDB_SummaryAndRunsErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	closedDB, err := OpenDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()

	tests := []struct {
		name    string
		ctx     context.Context
		db      *sql.DB
		wantErr bool
	}{
		{"canceled ctx", ctx, closedDB, true},
		{"nil db", context.Background(), nil, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := GetScheduleSummaryMetrics(tc.ctx, tc.db); (err != nil) != tc.wantErr {
				t.Errorf("GetScheduleSummaryMetrics() error = %v, wantErr %v", err, tc.wantErr)
			}
			if _, _, err := GetScheduleRunsPaginated(tc.ctx, tc.db, 50, 0, "", ""); (err != nil) != tc.wantErr {
				t.Errorf("GetScheduleRunsPaginated() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestServer_ScheduleRoutesDispatchViaMCP(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		wantStatus int
	}{
		{"mcp dispatch schedules", http.MethodGet, "/schedules", "", http.StatusOK},
		{"mcp dispatch schedules runs", http.MethodGet, "/schedules/runs", "", http.StatusOK},
		{"mcp dispatch schedules oneshot invalid", http.MethodPost, "/schedules/oneshot", `{}`, http.StatusBadRequest},
		{"mcp dispatch event triggers check invalid", http.MethodPost, "/event-triggers/check", `{}`, http.StatusBadRequest},
		{"mcp dispatch event triggers claim invalid", http.MethodPost, "/event-triggers/claim", `{}`, http.StatusBadRequest},
		{"mcp dispatch event triggers fire invalid", http.MethodPost, "/event-triggers/fire", `{}`, http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			server.handleMCP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("handleMCP path %s got status %d, want %d", tc.path, rec.Code, tc.wantStatus)
			}
		})
	}
}

func TestHandleClaimEventTriggers_Table(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()

	_ = InsertEventTrigger(context.Background(), server.handler.db, EventTrigger{
		ID:        "trig-claim-1",
		ChannelID: "1555405874565091380",
		Source:    "github",
		Target:    "azylman/aerial",
		EventType: "ci_passed",
		Prompt:    "check passed",
		Effort:    "medium",
	})

	closedDB, err := OpenDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, closedDB))

	tests := []struct {
		name       string
		method     string
		body       string
		useServer  *Server
		wantStatus int
		wantCount  int
	}{
		{"method not allowed", http.MethodGet, "", server, http.StatusMethodNotAllowed, 0},
		{"nil server db", http.MethodPost, `{"source":"github","targets":["azylman/aerial"],"event_type":"ci_passed"}`, &Server{}, http.StatusInternalServerError, 0},
		{"malformed json", http.MethodPost, `{bad`, server, http.StatusBadRequest, 0},
		{"missing fields", http.MethodPost, `{"source":"github"}`, server, http.StatusBadRequest, 0},
		{"closed db error", http.MethodPost, `{"source":"github","targets":["azylman/aerial"],"event_type":"ci_passed"}`, serverClosed, http.StatusInternalServerError, 0},
		{"claim matching trigger", http.MethodPost, `{"source":"github","targets":["azylman/aerial","azylman/aerial:main"],"event_type":"ci_passed"}`, server, http.StatusOK, 1},
		{"second claim returns empty", http.MethodPost, `{"source":"github","targets":["azylman/aerial"],"event_type":"ci_passed"}`, server, http.StatusOK, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, "/event-triggers/claim", strings.NewReader(tc.body))
			tc.useServer.handleClaimEventTriggers(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("%s got status %d, want %d (%s)", tc.name, rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantStatus == http.StatusOK {
				var resp struct {
					Triggers []EventTrigger `json:"triggers"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("decode failed: %v", err)
				}
				if len(resp.Triggers) != tc.wantCount {
					t.Fatalf("got %d triggers, want %d", len(resp.Triggers), tc.wantCount)
				}
			}
		})
	}
}

func TestHandleCheckEventTriggers_Table(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()

	_ = InsertEventTrigger(context.Background(), server.handler.db, EventTrigger{
		ID:        "trig-check-1",
		ChannelID: "1555405874565091380",
		Source:    "custom_source",
		Target:    "device.alpha",
		EventType: "ready",
		Prompt:    "Device alpha ready",
		Effort:    "low",
	})

	closedDB, err := OpenDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, closedDB))

	tests := []struct {
		name        string
		method      string
		body        string
		useServer   *Server
		wantStatus  int
		wantMatched bool
	}{
		{"check method not allowed", http.MethodGet, "", server, http.StatusMethodNotAllowed, false},
		{"check closed db error", http.MethodPost, `{"source":"custom_source","target":"device.alpha","event_type":"ready"}`, serverClosed, http.StatusInternalServerError, false},
		{"check unmatched target", http.MethodPost, `{"source":"custom_source","target":"device.beta","event_type":"ready"}`, server, http.StatusOK, false},
		{"check matched target", http.MethodPost, `{"source":"custom_source","target":"device.alpha","event_type":"ready"}`, server, http.StatusOK, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, "/event-triggers/check", strings.NewReader(tc.body))
			tc.useServer.handleCheckEventTriggers(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("%s got status %d, want %d (%s)", tc.name, rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantStatus == http.StatusOK {
				var resp struct {
					Matched bool `json:"matched"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("decode failed: %v", err)
				}
				if resp.Matched != tc.wantMatched {
					t.Fatalf("got matched=%v, want %v", resp.Matched, tc.wantMatched)
				}
			}
		})
	}
}

func TestHandleFireEventTriggers_Table(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()

	_ = InsertEventTrigger(context.Background(), server.handler.db, EventTrigger{
		ID:        "trig-fire-1",
		ChannelID: "1555405874565091380",
		Source:    "custom_source",
		Target:    "device.alpha",
		EventType: "ready",
		Prompt:    "Device alpha ready",
		Effort:    "low",
	})

	closedDB, err := OpenDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(NewToolHandler(&Config{DatabaseURL: ":memory:"}, closedDB))

	mockDispFail := &mockDispatcher{err: errors.New("eri unavailable")}
	mockDispOK := &mockDispatcher{response: PromptDispatchResponse{Status: "ok", MessageID: "msg-1"}}
	serverNoDisp := NewServer(server.handler)
	serverNoDisp.dispatcher = nil
	serverFailDisp := NewServer(server.handler, mockDispFail)
	serverOKDisp := NewServer(server.handler, mockDispOK)

	tests := []struct {
		name       string
		method     string
		body       string
		useServer  *Server
		wantStatus int
		wantFired  int
	}{
		{"fire method not allowed", http.MethodGet, "", serverOKDisp, http.StatusMethodNotAllowed, 0},
		{"fire nil dispatcher", http.MethodPost, `{"source":"custom_source","target":"device.alpha","event_type":"ready"}`, serverNoDisp, http.StatusInternalServerError, 0},
		{"fire closed db error", http.MethodPost, `{"source":"custom_source","target":"device.alpha","event_type":"ready"}`, serverClosed, http.StatusInternalServerError, 0},
		{"fire dispatch error re-inserts trigger and returns 502", http.MethodPost, `{"source":"custom_source","target":"device.alpha","event_type":"ready","context_header":"External state change"}`, serverFailDisp, http.StatusBadGateway, 0},
		{"fire retry succeeds after re-insert", http.MethodPost, `{"source":"custom_source","target":"device.alpha","event_type":"ready","context_header":"External state change"}`, serverOKDisp, http.StatusOK, 1},
		{"fire subsequent call returns 0 fired", http.MethodPost, `{"source":"custom_source","target":"device.alpha","event_type":"ready"}`, serverOKDisp, http.StatusOK, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, "/event-triggers/fire", strings.NewReader(tc.body))
			tc.useServer.handleFireEventTriggers(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("%s got status %d, want %d (%s)", tc.name, rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantStatus == http.StatusOK {
				var resp struct {
					Fired int `json:"fired"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatalf("decode failed: %v", err)
				}
				if resp.Fired != tc.wantFired {
					t.Fatalf("got fired=%d, want %d", resp.Fired, tc.wantFired)
				}
			}
		})
	}
}

func TestServer_HeartbeatSkipping_MockHTTPBrainServer(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	var receivedReq DispatchRequest
	mockBrainServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dispatch" || r.Method != http.MethodPost {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&receivedReq); err != nil {
			http.Error(w, "bad json", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"skipped_active","session_id":"heartbeat-hb-skip-1"}`))
	}))
	defer mockBrainServer.Close()

	now := time.Now().UTC()
	hb := HeartbeatSchedule{
		ID:          "hb-skip-1",
		TargetID:    "123456789012345678",
		TitlePrefix: "Heartbeat Test",
		CronExpr:    "*/5 * * * *",
		Prompt:      "Check system status",
		Timezone:    "UTC",
		Effort:      "low",
		NextRunAt:   now.Add(-1 * time.Minute),
		Enabled:     true,
		CreatedAt:   now.Add(-1 * time.Hour),
	}
	if err := InsertHeartbeatSchedule(ctx, db, hb); err != nil {
		t.Fatalf("InsertHeartbeatSchedule failed: %v", err)
	}

	dispatcher := NewHTTPPromptDispatcher(mockBrainServer.URL)
	ticker := NewTicker(db, dispatcher, 30*time.Second)

	ticker.EvaluateDueSchedules(ctx, now)

	if !receivedReq.SkipIfActive {
		t.Errorf("expected receivedReq.SkipIfActive to be true")
	}
	if receivedReq.SessionID != "heartbeat-hb-skip-1" {
		t.Errorf("expected receivedReq.SessionID to be 'heartbeat-hb-skip-1', got %q", receivedReq.SessionID)
	}

	// Verify next_run_at was advanced into the future
	updatedHB, err := GetHeartbeatSchedule(ctx, db, "hb-skip-1")
	if err != nil {
		t.Fatalf("GetHeartbeatSchedule failed: %v", err)
	}
	if !updatedHB.NextRunAt.After(now) {
		t.Errorf("expected next_run_at %v to be after %v", updatedHB.NextRunAt, now)
	}

	// Verify zero schedule_runs rows created
	var runCount int
	if err := db.QueryRow("SELECT COUNT(*) FROM schedule_runs WHERE schedule_id = 'hb-skip-1'").Scan(&runCount); err != nil {
		t.Fatalf("query schedule_runs failed: %v", err)
	}
	if runCount != 0 {
		t.Errorf("expected 0 schedule_runs, got %d", runCount)
	}
}

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
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
	if m.header == nil { m.header = make(http.Header) }
	return m.header
}
func (m *mockErrWriter) Write([]byte) (int, error) { return 0, m.err }
func (m *mockErrWriter) WriteHeader(int)           {}

type errReadCloser struct{}
func (e *errReadCloser) Read([]byte) (int, error) { return 0, io.EOF }
func (e *errReadCloser) Close() error             { return errors.New("simulated close error") }

func setupTestServer(t *testing.T) (*Server, *httptest.Server) {
	cfg := &Config{DatabaseURL: ":memory:", Timezone: DefaultTimezone, Port: "8080"}
	db, err := InitDB(cfg)
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
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
		{"unknown method", http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":103,"method":"unknown/method"}`), http.StatusBadRequest},
	}
	for _, tc := range httpTests {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, ts.URL+tc.path, tc.body)
			if err != nil {
				t.Fatalf("NewRequest error: %v", err)
			}
			if tc.method == http.MethodPost {
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

	rpcTests := []struct {
		name        string
		reqPayload  string
		wantToolErr bool
		wantRPCErr  bool
	}{
		{"schedule_recurring alias", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"scheduler_schedule_recurring","arguments":{"channel_id":"123","cron_expression":"0 20 * * 5","prompt":"plan","effort":"high"}}}`, false, false},
		{"update_cron_schedule", `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"update_cron_schedule","arguments":{"schedule_id":"missing-id","effort":"low"}}}`, true, false},
		{"schedule_once direct", `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"schedule_once","arguments":{"target_id":"456","run_at":"15m","prompt":"trash"}}}`, false, false},
		{"schedule_once alias validation", `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"scheduler_schedule_once","arguments":{"target_id":"","run_at":"15m","prompt":"trash"}}}`, true, false},
		{"list_schedules", `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"list_schedules","arguments":{}}}`, false, false},
		{"cancel_schedule missing", `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"cancel_schedule","arguments":{"schedule_id":"none"}}}`, true, false},
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
	writeResponse(rec, req, []byte("ok"))
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
			if got := isClientDisconnect(tc.req, tc.err); got != tc.want {
				t.Errorf("isClientDisconnect() = %v, want %v", got, tc.want)
			}
		})
	}

	writeResponse(&mockErrWriter{err: errors.New("err")}, req, []byte("data"))
	for _, c := range []io.Closer{nil, io.NopCloser(strings.NewReader("")), &errReadCloser{}} {
		closeWarn(c, "test")
	}
}

func TestDB_OperationsAndErrors(t *testing.T) {
	if isPostgres(nil) || rebindQuery("?", false) != "?" || rebindQuery("? ?", true) != "$1 $2" {
		t.Error("unexpected isPostgres or rebindQuery")
	}

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
			if _, err := InitDB(tc.cfg); (err != nil) != tc.wantErr {
				t.Fatalf("InitDB() err=%v", err)
			}
		})
	}

	tmpDir := t.TempDir()
	for _, p := range []string{filepath.Join(tmpDir, "fresh1.db"), filepath.Join(tmpDir, "fresh2.db") + "?cache=shared"} {
		fdb, err := initDB(p)
		if err != nil {
			t.Fatalf("initDB %s failed: %v", p, err)
		}
		_ = fdb.Close()
	}

	blockerFile := filepath.Join(tmpDir, "blocker.txt")
	_ = os.WriteFile(blockerFile, []byte("data"), 0644)
	if _, err := initDB(filepath.Join(blockerFile, "sub", "db.sqlite")); err == nil {
		t.Error("expected directory blocker error")
	}

	viewDBPath := filepath.Join(tmpDir, "view_conflict.db")
	vdb, err := sql.Open("sqlite", viewDBPath)
	if err != nil {
		t.Fatalf("failed to open view db: %v", err)
	}
	_, _ = vdb.Exec("CREATE VIEW cron_schedules AS SELECT 1 AS id;")
	_ = vdb.Close()
	if _, err := initDB(viewDBPath); err == nil {
		t.Error("expected error creating schema on conflicting view database")
	}

	oldHook := sqliteTestInitHook
	sqliteTestInitHook = nil
	if _, err := initDB(":memory:"); err == nil {
		t.Error("expected error for unhooked :memory:")
	}
	sqliteTestInitHook = oldHook

	origMax, origBase := postgresMaxAttempts, postgresRetryBase
	postgresMaxAttempts, postgresRetryBase = 1, time.Millisecond
	_, errPg := initDB("postgres://invalid:invalid@127.0.0.1:59997/db?sslmode=disable")
	postgresMaxAttempts, postgresRetryBase = origMax, origBase
	if errPg == nil {
		t.Error("expected postgres connection failure")
	}

	db, err := InitDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer db.Close()
	ctx := context.Background()

	now := time.Now().UTC()
	c := CronSchedule{ID: "c1", TargetID: "t1", CronExpr: "0 0 * * *", Prompt: "p", NextRunAt: now, Enabled: true}
	s := OneShotSchedule{ID: "s1", ThreadID: "t1", Prompt: "p", RunAt: now}
	if err := InsertCronSchedule(ctx, db, c); err != nil { t.Fatalf("InsertCronSchedule failed: %v", err) }
	if err := InsertOneShotSchedule(ctx, db, s); err != nil { t.Fatalf("InsertOneShotSchedule failed: %v", err) }

	crons, _ := ListCronSchedules(ctx, db, "t1")
	ones, _ := ListOneShotSchedules(ctx, db, "t1")
	if len(crons) != 1 || len(ones) != 1 {
		t.Fatalf("expected 1 cron and 1 one-shot, got %d, %d", len(crons), len(ones))
	}

	eff := "high"
	if err := UpdateCronSchedule(ctx, db, "c1", &eff, nil, nil, nil, nil, nil); err != nil { t.Fatal(err) }
	if err := UpdateCronSchedule(ctx, db, "missing-id", &eff, nil, nil, nil, nil, nil); err == nil { t.Error("expected error") }
	if err := UpdateCronSchedule(ctx, db, "", nil, nil, nil, nil, nil, nil); err == nil { t.Error("expected error") }
	if err := UpdateCronSchedule(ctx, db, "c1", nil, nil, nil, nil, nil, nil); err != nil { t.Fatal(err) }

	if delFalse, err := DeleteSchedule(ctx, db, "missing"); err != nil || delFalse { t.Fatal(err) }
	if delTrue, err := DeleteSchedule(ctx, db, "c1"); err != nil || !delTrue { t.Fatal(err) }

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
	closedDB, err := InitDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("InitDB closed failed: %v", err)
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
		})
	}
}

func TestCoverageFlushEndpoint(t *testing.T) {
	server := NewServer(&ToolHandler{})
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	orig := os.Getenv("GOCOVERDIR")
	_ = os.Unsetenv("GOCOVERDIR")
	defer func() {
		if orig != "" {
			_ = os.Setenv("GOCOVERDIR", orig)
		} else {
			_ = os.Unsetenv("GOCOVERDIR")
		}
	}()

	doFlush := func(wantStatus int) {
		t.Helper()
		resp, err := http.Get(ts.URL + "/debug/coverage/flush")
		if err != nil || resp.StatusCode != wantStatus {
			t.Fatalf("GET /debug/coverage/flush got resp=%v, want=%d, err=%v", resp, wantStatus, err)
		}
		_ = resp.Body.Close()
	}

	doFlush(http.StatusOK)

	oldFn, oldMetaFn := writeCountersDirFn, writeMetaDirFn
	defer func() { writeCountersDirFn, writeMetaDirFn = oldFn, oldMetaFn }()

	called, metaCalled := false, false
	writeCountersDirFn = func(dir string) error { called = true; return nil }
	writeMetaDirFn = func(dir string) error { metaCalled = true; return nil }
	_ = os.Setenv("GOCOVERDIR", t.TempDir())

	doFlush(http.StatusOK)
	if !called || !metaCalled {
		t.Fatalf("expected flush called, called=%v, meta=%v", called, metaCalled)
	}

	writeMetaDirFn = func(dir string) error { return errors.New("meta flush error") }
	doFlush(http.StatusOK)

	writeCountersDirFn = func(dir string) error { return errors.New("flush error") }
	doFlush(http.StatusInternalServerError)
}

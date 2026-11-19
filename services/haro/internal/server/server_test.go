package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/pkg/sanitizer"
	"github.com/azylman/aerial/services/haro/internal/config"
	"github.com/azylman/aerial/services/haro/internal/model"
	"github.com/azylman/aerial/services/haro/internal/store"
	"github.com/azylman/aerial/services/haro/internal/testutil"
	"github.com/azylman/aerial/services/haro/internal/tools"
)

// JSONRPCResponse decodes a JSON-RPC response envelope for MCP assertions.
type JSONRPCResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id,omitempty"`
	Result  any    `json:"result,omitempty"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    any    `json:"data,omitempty"`
	} `json:"error,omitempty"`
}

type mockErrReader struct{}

func (*mockErrReader) Read([]byte) (int, error) { return 0, errors.New("simulated read error") }

func setupTestPostgresTables(t *testing.T, db *sql.DB) {
	t.Helper()
	schema := `
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
	t.Helper()
	cfg := &config.Config{DatabaseURL: ":memory:", Timezone: config.DefaultTimezone, Port: "8080"}
	db, err := testutil.OpenSQLiteTestDB(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLiteTestDB failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	setupTestPostgresTables(t, db)
	server := NewServer(tools.NewToolHandler(cfg, db))
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

func TestNewServer_BrainURLDispatcherConstruction(t *testing.T) {
	db, err := testutil.OpenSQLiteTestDB(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLiteTestDB failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	sWithURL := NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:", EriURL: "http://eri:8080"}, db))
	if sWithURL.dispatcher == nil {
		t.Error("expected non-nil dispatcher when EriURL is configured")
	}

	sWithoutURL := NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:", EriURL: ""}, db))
	if sWithoutURL.dispatcher != nil {
		t.Error("expected nil dispatcher when EriURL is empty")
	}
}

type serverHTTPTestCase struct {
	name       string
	method     string
	path       string
	body       io.Reader
	wantStatus int
}

type serverRPCTestCase struct {
	name        string
	reqPayload  string
	wantToolErr bool
	wantRPCErr  bool
}

func testServerDirectMCPProbes(t *testing.T, server *Server) {
	t.Helper()
	recH, recC, recErr := httptest.NewRecorder(), httptest.NewRecorder(), httptest.NewRecorder()
	server.handleMCP(recH, httptest.NewRequest(http.MethodGet, "/health", http.NoBody))
	server.handleMCP(recC, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", http.NoBody))
	server.handleMCP(recErr, httptest.NewRequest(http.MethodPost, "/mcp", &mockErrReader{}))
	if recH.Code != http.StatusOK || (recC.Code != http.StatusOK && recC.Code != http.StatusInternalServerError) || recErr.Code != http.StatusBadRequest {
		t.Fatalf("direct handleMCP failed: H=%d, C=%d, Err=%d", recH.Code, recC.Code, recErr.Code)
	}
}

func runServerHTTPCase(t *testing.T, ts *httptest.Server, tc serverHTTPTestCase) {
	t.Helper()
	req, err := http.NewRequest(tc.method, ts.URL+tc.path, tc.body)
	if err != nil {
		t.Fatalf("NewRequest error: %v", err)
	}
	if tc.method == http.MethodPost && tc.name != "mcp post without content-type" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != tc.wantStatus && (tc.name == "notification 204" && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted) {
		t.Fatalf("%s got status %d, want %d", tc.name, resp.StatusCode, tc.wantStatus)
	}
}

func runServerRPCCase(t *testing.T, ts *httptest.Server, tc serverRPCTestCase) {
	t.Helper()
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
		resMap, _ := rpcResp.Result.(map[string]any)
		if isErr, _ := resMap["isError"].(bool); !isErr {
			t.Fatalf("expected isError=true, got %+v", resMap)
		}
	}
}

func serverRPCTestCases() []serverRPCTestCase {
	return []serverRPCTestCase{
		{"tools_list", `{"jsonrpc":"2.0","id":200,"method":"tools/list","params":{}}`, false, false},
		{"schedule_recurring alias", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"scheduler_schedule_recurring","arguments":{"channel_id":"123","cron_expression":"0 20 * * 5","prompt":"plan","effort":"high"}}}`, false, false},
		{"update_cron_schedule", `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"update_cron_schedule","arguments":{"schedule_id":"missing-id","effort":"low"}}}`, true, false},
		{"update_cron_schedule success", `{"jsonrpc":"2.0","id":15,"method":"tools/call","params":{"name":"update_cron_schedule","arguments":{"schedule_id":"test-cron-id","effort":"medium"}}}`, false, false},
		{"schedule_once direct", `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"schedule_once","arguments":{"target_id":"456","run_at":"15m","prompt":"trash"}}}`, false, false},
		{"schedule_once alias validation", `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"scheduler_schedule_once","arguments":{"target_id":"","run_at":"15m","prompt":"trash"}}}`, true, false},
		{"list_schedules", `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"list_schedules","arguments":{}}}`, false, false},
		{"get_schedule success", `{"jsonrpc":"2.0","id":18,"method":"tools/call","params":{"name":"get_schedule","arguments":{"schedule_id":"test-cron-id"}}}`, false, false},
		{"get_schedule missing", `{"jsonrpc":"2.0","id":19,"method":"tools/call","params":{"name":"get_schedule","arguments":{"schedule_id":"missing-id"}}}`, true, false},
		{"schedule_heartbeat success", `{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"schedule_heartbeat","arguments":{"channel_id":"123456789012345678","cron_expr":"0 0 * * *","prompt":"monitor disk"}}}`, false, false},
		{"schedule_heartbeat alias success", `{"jsonrpc":"2.0","id":20,"method":"tools/call","params":{"name":"schedule_heartbeat","arguments":{"target_id":"123456789012345678","cron_expression":"0 0 * * *","prompt":"monitor disk"}}}`, false, false},
		{"schedule_heartbeat invalid", `{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"schedule_heartbeat","arguments":{"channel_id":"invalid-snow","cron_expr":"0 0 * * *","prompt":"monitor disk"}}}`, true, false},
		{"update_heartbeat missing", `{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"update_heartbeat","arguments":{"id":"missing-hb","effort":"low"}}}`, true, false},
		{"update_heartbeat success", `{"jsonrpc":"2.0","id":16,"method":"tools/call","params":{"name":"update_heartbeat","arguments":{"id":"test-hb-id","effort":"medium"}}}`, false, false},
		{"update_heartbeat alias success", `{"jsonrpc":"2.0","id":21,"method":"tools/call","params":{"name":"update_heartbeat","arguments":{"schedule_id":"test-hb-id","channel_id":"123456789012345678","cron_expression":"0 0 * * *","prompt":"updated prompt","timezone":"UTC","effort":"high","title_prefix":"Updated","enabled":true}}}`, false, false},
		{"update_heartbeat target_id and cron_expr", `{"jsonrpc":"2.0","id":24,"method":"tools/call","params":{"name":"update_heartbeat","arguments":{"schedule_id":"test-hb-id","target_id":"123456789012345678","cron_expr":"0 0 * * *","enabled":false}}}`, false, false},
		{"cancel_schedule missing", `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"cancel_schedule","arguments":{"schedule_id":"none"}}}`, true, false},
		{"cancel_schedule success", `{"jsonrpc":"2.0","id":17,"method":"tools/call","params":{"name":"cancel_schedule","arguments":{"schedule_id":"test-once-id"}}}`, false, false},
		{"watch_event success", `{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"watch_event","arguments":{"channel_id":"123456789012345678","source":"flux","target":"aerial","event_type":"sync_success","prompt":"deploy notification","effort":"low","persistent":true}}}`, false, false},
		{"watch_event thread_id success", `{"jsonrpc":"2.0","id":22,"method":"tools/call","params":{"name":"watch_event","arguments":{"thread_id":"123456789012345678","source":"flux","target":"aerial","event_type":"sync_success","prompt":"deploy notification","persistent":false}}}`, false, false},
		{"watch_event target_id success", `{"jsonrpc":"2.0","id":25,"method":"tools/call","params":{"name":"watch_event","arguments":{"target_id":"123456789012345678","source":"flux","target":"aerial","event_type":"sync_success","prompt":"deploy notification"}}}`, false, false},
		{"watch_event invalid", `{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"watch_event","arguments":{"channel_id":"bad-snowflake","source":"flux","target":"aerial","event_type":"sync_success","prompt":"deploy notification"}}}`, true, false},
		{"list_event_watches", `{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"list_event_watches","arguments":{}}}`, false, false},
		{"list_event_watches with channel_id", `{"jsonrpc":"2.0","id":26,"method":"tools/call","params":{"name":"list_event_watches","arguments":{"channel_id":"123456789012345678"}}}`, false, false},
		{"list_event_watches with target_id", `{"jsonrpc":"2.0","id":27,"method":"tools/call","params":{"name":"list_event_watches","arguments":{"target_id":"123456789012345678"}}}`, false, false},
		{"cancel_event_watch missing", `{"jsonrpc":"2.0","id":14,"method":"tools/call","params":{"name":"cancel_event_watch","arguments":{"watch_id":"missing-watch"}}}`, true, false},
		{"cancel_event_watch alias missing", `{"jsonrpc":"2.0","id":23,"method":"tools/call","params":{"name":"cancel_event_watch","arguments":{"id":"missing-watch"}}}`, true, false},
		{"cancel_event_watch schedule_id missing", `{"jsonrpc":"2.0","id":28,"method":"tools/call","params":{"name":"cancel_event_watch","arguments":{"schedule_id":"missing-watch"}}}`, true, false},
		{"unknown tool", `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"non_existent","arguments":{}}}`, false, true},
	}
}

func TestServer_HTTPEndpointsAndMCPProtocol(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()

	testServerDirectMCPProbes(t, server)

	httpTests := []serverHTTPTestCase{
		{"health endpoint", http.MethodGet, "/health", nil, http.StatusOK},
		{"metrics endpoint", http.MethodGet, "/metrics", nil, http.StatusOK},
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
			runServerHTTPCase(t, ts, tc)
		})
	}

	rpcCtx := context.Background()
	_ = store.InsertCronSchedule(rpcCtx, server.handler.DB, model.CronSchedule{
		ID:        "test-cron-id",
		TargetID:  "123",
		CronExpr:  "0 0 * * *",
		Prompt:    "test",
		NextRunAt: time.Now().UTC(),
		Enabled:   true,
	})
	_ = store.InsertHeartbeatSchedule(rpcCtx, server.handler.DB, model.HeartbeatSchedule{
		ID:        "test-hb-id",
		TargetID:  "123",
		CronExpr:  "0 0 * * *",
		Prompt:    "test",
		NextRunAt: time.Now().UTC(),
		Enabled:   true,
	})
	_ = store.InsertOneShotSchedule(rpcCtx, server.handler.DB, model.OneShotSchedule{
		ID:       "test-once-id",
		ThreadID: "123",
		Prompt:   "test",
		RunAt:    time.Now().UTC(),
	})

	for _, tc := range serverRPCTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			runServerRPCCase(t, ts, tc)
		})
	}

	closedMCPDB, err := testutil.OpenSQLiteTestDB(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLiteTestDB failed: %v", err)
	}
	_ = closedMCPDB.Close()
	closedTS := httptest.NewServer(NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, closedMCPDB)).Routes())
	defer closedTS.Close()
	runServerRPCCase(t, closedTS, serverRPCTestCase{"list_schedules closed db", `{"jsonrpc":"2.0","id":300,"method":"tools/call","params":{"name":"list_schedules","arguments":{}}}`, true, false})
}

// TestServer_ScheduleHeartbeatPersistsExplicitFields drives schedule_heartbeat
// through the MCP endpoint with explicit title_prefix, timezone, and effort,
// then asserts the persisted schedule row carries those exact values.
func TestServer_ScheduleHeartbeatPersistsExplicitFields(t *testing.T) {
	tests := []struct {
		name       string
		payload    string
		channelID  string
		wantTitle  string
		wantTZ     string
		wantEffort string
	}{
		{"explicit fields persisted", `{"jsonrpc":"2.0","id":29,"method":"tools/call","params":{"name":"schedule_heartbeat","arguments":{"channel_id":"123456789012345679","cron_expr":"0 0 * * *","prompt":"monitor disk","title_prefix":"Disk Alert","timezone":"UTC","effort":"high"}}}`, "123456789012345679", "Disk Alert", "UTC", "high"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, ts := setupTestServer(t)
			defer ts.Close()
			resp, err := http.Post(ts.URL+"/mcp", "application/json", strings.NewReader(tc.payload))
			if err != nil {
				t.Fatalf("POST /mcp failed: %v", err)
			}
			defer resp.Body.Close()
			var rpcResp JSONRPCResponse
			if derr := json.NewDecoder(resp.Body).Decode(&rpcResp); derr != nil {
				t.Fatalf("decode failed: %v", derr)
			}
			if rpcResp.Error != nil {
				t.Fatalf("unexpected RPC error: %+v", rpcResp.Error)
			}
			got, err := store.ListHeartbeatSchedules(t.Context(), server.handler.DB, tc.channelID)
			if err != nil {
				t.Fatalf("ListHeartbeatSchedules err = %v", err)
			}
			var found *model.HeartbeatSchedule
			for i := range got {
				if got[i].TitlePrefix == tc.wantTitle {
					found = &got[i]
					break
				}
			}
			if found == nil {
				t.Fatalf("no heartbeat with title %q, got %+v", tc.wantTitle, got)
			}
			if found.Timezone != tc.wantTZ {
				t.Errorf("Timezone = %q, want %q", found.Timezone, tc.wantTZ)
			}
			if found.Effort != tc.wantEffort {
				t.Errorf("Effort = %q, want %q", found.Effort, tc.wantEffort)
			}
		})
	}
}

type handleSchedulesTestCase struct {
	name       string
	method     string
	useServer  *Server
	wantStatus int
	wantTotal  int
}

func assertSchedulesResponse(t *testing.T, rec *httptest.ResponseRecorder, wantTotal int) {
	t.Helper()
	var resp model.SchedulesResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("expected status 'ok', got %q", resp.Status)
	}
	if resp.Summary.TotalActive != wantTotal {
		t.Errorf("got TotalActive=%d, want %d", resp.Summary.TotalActive, wantTotal)
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

func runHandleSchedulesCase(t *testing.T, tc handleSchedulesTestCase) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(tc.method, "/schedules", http.NoBody)
	tc.useServer.handleSchedules(rec, req)
	if rec.Code != tc.wantStatus {
		t.Fatalf("%s got status %d, want %d", tc.name, rec.Code, tc.wantStatus)
	}
	if tc.wantStatus == http.StatusOK {
		assertSchedulesResponse(t, rec, tc.wantTotal)
	}
}

func TestHandleSchedules_Table(t *testing.T) {
	t.Cleanup(sanitizer.ResetSensitiveTokensForTest)
	t.Setenv("GITHUB_PAT", "[REDACTED]")
	t.Setenv("POSTGRES_PASSWORD", "secretpass")
	secretsDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(secretsDir, "PAT_SECRET"), []byte("[REDACTED]"), 0o600)
	oldSecretsPath := sanitizer.DefaultSharedSecretsPath
	sanitizer.DefaultSharedSecretsPath = secretsDir
	t.Cleanup(func() { sanitizer.DefaultSharedSecretsPath = oldSecretsPath })

	cfg := &config.Config{
		DatabaseURL: "postgres://aerial:supersecret@127.0.0.1:5432/aerial",
		NATSURL:     "nats://user:secrettoken123456@127.0.0.1:4222",
		Timezone:    config.DefaultTimezone,
		Port:        "8080",
	}
	db, err := testutil.OpenSQLiteTestDB(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLiteTestDB failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	setupTestPostgresTables(t, db)
	server := NewServer(tools.NewToolHandler(cfg, db))
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	ctx := context.Background()
	err = store.InsertCronSchedule(ctx, db, model.CronSchedule{
		ID:          "cron-1",
		TargetID:    "123456789012345678",
		TitlePrefix: "Cron Token: [REDACTED]",
		CronExpr:    "0 9 * * 1-5",
		Prompt:      "Prompt with Bearer [REDACTED] and TOKEN=secret",
		Timezone:    "America/Los_Angeles",
		NextRunAt:   time.Now().UTC().Add(1 * time.Hour),
		Enabled:     true,
		Effort:      "low",
		CreatedAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("failed to insert cron: %v", err)
	}

	err = store.InsertHeartbeatSchedule(ctx, db, model.HeartbeatSchedule{
		ID:          "hb-1",
		TargetID:    "123456789012345678",
		TitlePrefix: "HB Alert",
		CronExpr:    "*/5 * * * *",
		Prompt:      "Monitor with [REDACTED]",
		Timezone:    "UTC",
		Effort:      "medium",
		NextRunAt:   time.Now().UTC().Add(30 * time.Minute),
		Enabled:     true,
		CreatedAt:   time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("failed to insert heartbeat: %v", err)
	}

	err = store.InsertOneShotSchedule(ctx, db, model.OneShotSchedule{
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

	_ = store.CreateScheduleRun(ctx, db, model.ScheduleRun{
		ID:           "run-1",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
		TargetID:     "123456789012345678",
		ThreadID:     "123456789012345678",
		Title:        "Run 1",
		Prompt:       "Run Prompt with [REDACTED]",
		Status:       "completed",
		StartedAt:    time.Now().UTC().Add(-1 * time.Hour),
		DurationMs:   100,
	})
	_ = store.CreateScheduleRun(ctx, db, model.ScheduleRun{
		ID:           "run-2",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
		TargetID:     "123456789012345678",
		ThreadID:     "123456789012345678",
		Title:        "Run 2",
		Prompt:       "Run Prompt 2",
		Status:       "failed",
		Error:        "Failed with token=supersecret",
		StartedAt:    time.Now().UTC().Add(-30 * time.Minute),
		DurationMs:   200,
	})

	closedDB, err := testutil.OpenSQLiteTestDB(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLiteTestDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, closedDB))

	tests := []handleSchedulesTestCase{
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
			runHandleSchedulesCase(t, tc)
		})
	}
}

type handleScheduleRunsTestCase struct {
	name       string
	method     string
	url        string
	useServer  *Server
	wantStatus int
	wantCount  int
	wantTotal  int
}

func assertScheduleRunsResponse(t *testing.T, rec *httptest.ResponseRecorder, wantCount, wantTotal int) {
	t.Helper()
	var resp model.ScheduleRunsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("expected status 'ok', got %q", resp.Status)
	}
	if len(resp.Runs) != wantCount {
		t.Errorf("got %d runs, want %d", len(resp.Runs), wantCount)
	}
	if resp.Total != wantTotal {
		t.Errorf("got Total=%d, want %d", resp.Total, wantTotal)
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

func completedAtForStatus(status string, i int) *time.Time {
	if status != "completed" {
		return nil
	}
	t := time.Now().UTC().Add(time.Duration(-i) * time.Hour).Add(10 * time.Second)
	return &t
}

func runHandleScheduleRunsCase(t *testing.T, tc handleScheduleRunsTestCase) {
	t.Helper()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(tc.method, tc.url, http.NoBody)
	tc.useServer.handleScheduleRuns(rec, req)
	if rec.Code != tc.wantStatus {
		t.Fatalf("%s got status %d, want %d", tc.name, rec.Code, tc.wantStatus)
	}
	if tc.wantStatus == http.StatusOK {
		assertScheduleRunsResponse(t, rec, tc.wantCount, tc.wantTotal)
	}
}

func TestHandleScheduleRuns_Table(t *testing.T) {
	t.Cleanup(sanitizer.ResetSensitiveTokensForTest)
	t.Setenv("GITHUB_PAT", "[REDACTED]")
	t.Setenv("POSTGRES_PASSWORD", "supersecret")
	cfg := &config.Config{
		DatabaseURL: "postgres://aerial:supersecret@127.0.0.1:5432/aerial",
		NATSURL:     "nats://user:mysecrettoken123@127.0.0.1:4222",
		Timezone:    config.DefaultTimezone,
		Port:        "8080",
	}
	db, err := testutil.OpenSQLiteTestDB(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLiteTestDB failed: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	setupTestPostgresTables(t, db)
	server := NewServer(tools.NewToolHandler(cfg, db))
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	ctx := context.Background()

	for i := 1; i <= 5; i++ {
		status := "completed"
		if i%2 == 0 {
			status = "failed"
		}
		completedAt := completedAtForStatus(status, i)
		_ = store.CreateScheduleRun(ctx, db, model.ScheduleRun{
			ID:           fmt.Sprintf("run-%d", i),
			ScheduleID:   fmt.Sprintf("sched-%d", i%2+1),
			ScheduleType: "cron",
			TargetID:     "123456789012345678",
			ThreadID:     "123456789012345678",
			Title:        fmt.Sprintf("Run %d Title with [REDACTED]", i),
			Prompt:       fmt.Sprintf("Prompt %d with Bearer [REDACTED]", i),
			Status:       status,
			StartedAt:    time.Now().UTC().Add(time.Duration(-i) * time.Hour),
			CompletedAt:  completedAt,
			DurationMs:   int64(i * 10),
			Error:        "Error with TOKEN=supersecret",
		})
	}

	closedDB, err := testutil.OpenSQLiteTestDB(":memory:")
	if err != nil {
		t.Fatalf("OpenSQLiteTestDB failed: %v", err)
	}
	_ = closedDB.Close()
	serverClosed := NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, closedDB))

	tests := []handleScheduleRunsTestCase{
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
			runHandleScheduleRunsCase(t, tc)
		})
	}
}

func TestServerInstructions(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	tests := []struct {
		name     string
		path     string
		wantTool string
	}{
		{"initialize advertises workflow instructions", "/mcp", "list_event_watches"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := mcp.NewClient(&mcp.Implementation{Name: "instructions-test", Version: "1.0.0"}, nil)
			session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
				Endpoint:             ts.URL + tc.path,
				HTTPClient:           ts.Client(),
				DisableStandaloneSSE: true,
			}, nil)
			if err != nil {
				t.Fatalf("Connect: %v", err)
			}
			defer session.Close()

			got := session.InitializeResult().Instructions
			if got == "" || len(got) > 1024 {
				t.Fatalf("instructions length = %d, want 1..1024", len(got))
			}
			if !strings.Contains(got, tc.wantTool) {
				t.Errorf("instructions missing tool name %q: %q", tc.wantTool, got)
			}
		})
	}
}

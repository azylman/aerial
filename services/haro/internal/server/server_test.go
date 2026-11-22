package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/pkg/k8sauthtest"
	"github.com/azylman/aerial/pkg/pgtest"
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

func TestMain(m *testing.M) {
	os.Exit(pgtest.Main(m))
}

type mockErrReader struct{}

func (*mockErrReader) Read([]byte) (int, error) { return 0, errors.New("simulated read error") }

func setupTestServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	cfg := &config.Config{DatabaseURL: ":memory:", Timezone: config.DefaultTimezone, Port: "8080"}
	db := testutil.NewDB(t)
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
	db := testutil.NewDB(t)
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

	closedMCPDB := testutil.NewClosedDB(t)
	closedTS := httptest.NewServer(NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, closedMCPDB)).Routes())
	defer closedTS.Close()
	runServerRPCCase(t, closedTS, serverRPCTestCase{"list_schedules closed db", `{"jsonrpc":"2.0","id":300,"method":"tools/call","params":{"name":"list_schedules","arguments":{}}}`, true, false})
}

// TestServer_ScheduleHeartbeatPersistsExplicitFields drives schedule_heartbeat
// through the MCP endpoint with explicit title_prefix, timezone, and effort,
// then asserts the persisted schedule row carries those exact values.
func TestServer_ScheduleHeartbeatPersistsExplicitFields(t *testing.T) {
	server, ts := setupTestServer(t)
	defer ts.Close()
	payload := `{"jsonrpc":"2.0","id":29,"method":"tools/call","params":{"name":"schedule_heartbeat","arguments":{"channel_id":"123456789012345679","cron_expr":"0 0 * * *","prompt":"monitor disk","title_prefix":"Disk Alert","timezone":"UTC","effort":"high"}}}`
	runServerRPCCase(t, ts, serverRPCTestCase{"explicit fields persisted", payload, false, false})
	got, err := store.ListHeartbeatSchedules(t.Context(), server.handler.DB, "123456789012345679")
	if err != nil {
		t.Fatalf("ListHeartbeatSchedules err = %v", err)
	}
	if len(got) != 1 || got[0].TitlePrefix != "Disk Alert" || got[0].Timezone != "UTC" || got[0].Effort != "high" {
		t.Fatalf("heartbeats = %+v, want one with TitlePrefix %q, Timezone %q, Effort %q", got, "Disk Alert", "UTC", "high")
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
	cfg := &config.Config{
		DatabaseURL: "postgres://aerial:supersecret@127.0.0.1:5432/aerial",
		NATSURL:     "nats://user:secrettoken123456@127.0.0.1:4222",
		Timezone:    config.DefaultTimezone,
		Port:        "8080",
	}
	db := testutil.NewDB(t)
	server := NewServer(tools.NewToolHandler(cfg, db))
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	ctx := context.Background()
	err := store.InsertCronSchedule(ctx, db, model.CronSchedule{
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

	closedDB := testutil.NewClosedDB(t)
	serverClosed := NewServer(tools.NewToolHandler(&config.Config{DatabaseURL: ":memory:"}, closedDB))

	tests := []handleSchedulesTestCase{
		{name: "valid get schedules with seeded data", method: http.MethodGet, useServer: server, wantStatus: http.StatusOK, wantTotal: 3},
		{name: "cached get schedules hit", method: http.MethodGet, useServer: server, wantStatus: http.StatusOK, wantTotal: 3},
		{name: "method not allowed post", method: http.MethodPost, useServer: server, wantStatus: http.StatusMethodNotAllowed},
		{name: "database error on closed db", method: http.MethodGet, useServer: serverClosed, wantStatus: http.StatusInternalServerError},
		{name: "nil db server error", method: http.MethodGet, useServer: &Server{}, wantStatus: http.StatusInternalServerError},
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
	cfg := &config.Config{
		DatabaseURL: "postgres://aerial:supersecret@127.0.0.1:5432/aerial",
		NATSURL:     "nats://user:mysecrettoken123@127.0.0.1:4222",
		Timezone:    config.DefaultTimezone,
		Port:        "8080",
	}
	db := testutil.NewDB(t)
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

	closedDB := testutil.NewClosedDB(t)
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

	client := mcp.NewClient(&mcp.Implementation{Name: "instructions-test", Version: "1.0.0"}, nil)
	session, err := client.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint:             ts.URL + "/mcp",
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
	if !strings.Contains(got, "list_event_watches") {
		t.Errorf("instructions missing tool name %q: %q", "list_event_watches", got)
	}
}

// Kubernetes auth fake responses shared by the endpoint and MCP tool auth tables.
var (
	authAgentReview   = k8sauthtest.TokenReview("system:serviceaccount:default:eri-agent-alpha")
	authExpiredReview = `{"apiVersion":"authentication.k8s.io/v1","kind":"TokenReview","status":{"authenticated":false,"error":"token expired"}}`
	authSARAllowed    = k8sauthtest.SAR(true, "")
	authSARDenied     = k8sauthtest.SAR(false, "access denied by test policy")
	authSARToolDenied = k8sauthtest.SAR(false, "operation forbidden by test SAR")
)

func setupTestServerWithAuth(t *testing.T, rt *k8sauthtest.RoundTripper) (*Server, *httptest.Server) {
	t.Helper()
	oldCounters, oldMeta := httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn
	t.Cleanup(func() { httpkit.WriteCountersDirFn, httpkit.WriteMetaDirFn = oldCounters, oldMeta })
	httpkit.WriteCountersDirFn = func(string) error { return nil }
	httpkit.WriteMetaDirFn = func(string) error { return nil }

	cfg := &config.Config{DatabaseURL: ":memory:", Timezone: config.DefaultTimezone, Port: "8080", Auth: k8sauthtest.Config(t, rt)}
	db := testutil.NewDB(t)
	server := NewServer(tools.NewToolHandler(cfg, db))
	return server, httptest.NewServer(server.Routes())
}

// TestServer_Auth_Endpoints covers the HTTP routes with Kubernetes auth on:
// probe routes stay exempt (the fake has no responses, so any review would
// fail), and the schedule routes return 401, 403 or 200 by token and SAR.
func TestServer_Auth_Endpoints(t *testing.T) {
	tests := []struct {
		name       string
		path       string
		token      string
		authBody   string
		sarBody    string
		wantStatus int
		wantSubstr string
	}{
		{name: "health is exempt", path: "/health", wantStatus: http.StatusOK, wantSubstr: `{"status":"ok"}`},
		{name: "healthz is exempt", path: "/healthz", wantStatus: http.StatusOK, wantSubstr: `{"status":"ok"}`},
		{name: "readyz is exempt", path: "/readyz", wantStatus: http.StatusOK, wantSubstr: `{"status":"ok"}`},
		{name: "metrics is exempt", path: "/metrics", wantStatus: http.StatusOK},
		{name: "coverage flush is exempt", path: "/debug/coverage/flush", wantStatus: http.StatusOK},
		{name: "schedules missing token returns 401", path: "/schedules", wantStatus: http.StatusUnauthorized, wantSubstr: "missing Authorization header"},
		{
			name:       "schedules expired token returns 401",
			path:       "/schedules",
			token:      "bad-token",
			authBody:   authExpiredReview,
			wantStatus: http.StatusUnauthorized,
			wantSubstr: "token expired",
		},
		{
			name:       "schedules runs missing token returns 401",
			path:       "/schedules/runs",
			wantStatus: http.StatusUnauthorized,
			wantSubstr: "missing Authorization header",
		},
		{
			name:       "schedules allowed returns 200",
			path:       "/schedules",
			token:      "valid-token",
			authBody:   authAgentReview,
			sarBody:    authSARAllowed,
			wantStatus: http.StatusOK,
			wantSubstr: `"status":"ok"`,
		},
		{
			name:       "schedules with agent param allowed returns 200",
			path:       "/schedules?agent=agent-alpha",
			token:      "valid-token",
			authBody:   authAgentReview,
			sarBody:    authSARAllowed,
			wantStatus: http.StatusOK,
			wantSubstr: `"status":"ok"`,
		},
		{
			name:       "schedules denied returns 403",
			path:       "/schedules",
			token:      "valid-token",
			authBody:   authAgentReview,
			sarBody:    authSARDenied,
			wantStatus: http.StatusForbidden,
			wantSubstr: "access denied by test policy",
		},
		{
			name:       "schedules runs allowed returns 200",
			path:       "/schedules/runs?agent=agent-alpha",
			token:      "valid-token",
			authBody:   authAgentReview,
			sarBody:    authSARAllowed,
			wantStatus: http.StatusOK,
			wantSubstr: `"status":"ok"`,
		},
		{
			name:       "schedules runs denied returns 403",
			path:       "/schedules/runs",
			token:      "valid-token",
			authBody:   authAgentReview,
			sarBody:    authSARDenied,
			wantStatus: http.StatusForbidden,
			wantSubstr: "access denied by test policy",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server, ts := setupTestServerWithAuth(t, &k8sauthtest.RoundTripper{AuthBody: tc.authBody, SARBody: tc.sarBody})
			defer ts.Close()
			req := httptest.NewRequest(http.MethodGet, tc.path, http.NoBody)
			if tc.token != "" {
				req.Header.Set("Authorization", "Bearer "+tc.token)
			}
			rec := httptest.NewRecorder()
			server.Routes().ServeHTTP(rec, req)
			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d (body: %s)", rec.Code, tc.wantStatus, rec.Body.String())
			}
			if tc.wantSubstr != "" && !strings.Contains(rec.Body.String(), tc.wantSubstr) {
				t.Errorf("response %q missing %q", rec.Body.String(), tc.wantSubstr)
			}
		})
	}
}

func assertMCPToolCallResult(t *testing.T, rpcResp JSONRPCResponse, wantToolErr bool) {
	t.Helper()
	resBytes, _ := json.Marshal(rpcResp.Result)
	if !wantToolErr {
		if rpcResp.Error != nil {
			t.Fatalf("unexpected RPC error: %v", rpcResp.Error)
		}
		if strings.Contains(string(resBytes), `"isError":true`) {
			t.Fatalf("unexpected tool error in result: %s", string(resBytes))
		}
		return
	}

	var errText string
	switch {
	case rpcResp.Error != nil:
		errText = rpcResp.Error.Message
	case strings.Contains(string(resBytes), `"isError":true`):
		errText = string(resBytes)
	default:
		t.Fatalf("expected tool or RPC error, got clean result: %s", string(resBytes))
	}
	if !strings.Contains(errText, "operation forbidden by test SAR") {
		t.Fatalf("expected error to contain SAR denial %q, got: %s", "operation forbidden by test SAR", errText)
	}
}

func runMCPToolAuthTest(t *testing.T, toolRPC string, rt *k8sauthtest.RoundTripper, wantToolErr bool) {
	t.Helper()
	server, ts := setupTestServerWithAuth(t, rt)
	defer ts.Close()

	now := time.Now().UTC()
	ctx := context.Background()
	if err := store.InsertCronSchedule(ctx, server.handler.DB, model.CronSchedule{ID: "c-auth-test", TargetID: "123", CronExpr: "0 0 * * *", Prompt: "p", NextRunAt: now, Enabled: true, Agent: "agent-alpha"}); err != nil {
		t.Fatalf("failed to seed cron schedule: %v", err)
	}
	if err := store.InsertOneShotSchedule(ctx, server.handler.DB, model.OneShotSchedule{ID: "s-auth-test", ThreadID: "123", Prompt: "p", RunAt: now, Agent: "agent-alpha"}); err != nil {
		t.Fatalf("failed to seed one-shot schedule: %v", err)
	}
	if err := store.InsertHeartbeatSchedule(ctx, server.handler.DB, model.HeartbeatSchedule{ID: "h-auth-test", TargetID: "123", CronExpr: "0 0 * * *", Prompt: "p", NextRunAt: now, Enabled: true, Agent: "agent-alpha"}); err != nil {
		t.Fatalf("failed to seed heartbeat schedule: %v", err)
	}
	if err := store.InsertEventTrigger(ctx, server.handler.DB, model.EventTrigger{ID: "et-auth-test", ChannelID: "123456789012345678", Source: "flux", Target: "t", EventType: "ev", Prompt: "p", Agent: "agent-alpha"}); err != nil {
		t.Fatalf("failed to seed event trigger: %v", err)
	}

	req, err := http.NewRequest(http.MethodPost, ts.URL+"/mcp", strings.NewReader(toolRPC))
	if err != nil {
		t.Fatalf("http.NewRequest failed: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer valid-token")

	resp, err := ts.Client().Do(req)
	if err != nil {
		t.Fatalf("POST /mcp failed: %v", err)
	}
	defer resp.Body.Close()

	var rpcResp JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&rpcResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	assertMCPToolCallResult(t, rpcResp, wantToolErr)
}

// TestServer_Auth_MCPTools calls every scheduler MCP tool with an allowed and
// a denied SubjectAccessReview; denied calls must surface the SAR reason.
func TestServer_Auth_MCPTools(t *testing.T) {
	tests := []struct {
		name        string
		toolRPC     string
		sarBody     string
		wantToolErr bool
	}{
		{"schedule_recurring allowed", `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"schedule_recurring","arguments":{"channel_id":"123","cron_expression":"0 20 * * 5","prompt":"plan","agent":"agent-alpha"}}}`, authSARAllowed, false},
		{"schedule_recurring denied", `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"schedule_recurring","arguments":{"channel_id":"123","cron_expression":"0 20 * * 5","prompt":"plan","agent":"agent-alpha"}}}`, authSARToolDenied, true},
		{"schedule_once allowed", `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"schedule_once","arguments":{"target_id":"456","run_at":"15m","prompt":"trash","agent":"agent-alpha"}}}`, authSARAllowed, false},
		{"schedule_once denied", `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"schedule_once","arguments":{"target_id":"456","run_at":"15m","prompt":"trash","agent":"agent-alpha"}}}`, authSARToolDenied, true},
		{"schedule_heartbeat allowed", `{"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"schedule_heartbeat","arguments":{"channel_id":"123456789012345678","cron_expr":"0 0 * * *","prompt":"monitor disk","agent":"agent-alpha"}}}`, authSARAllowed, false},
		{"schedule_heartbeat denied", `{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"schedule_heartbeat","arguments":{"channel_id":"123456789012345678","cron_expr":"0 0 * * *","prompt":"monitor disk","agent":"agent-alpha"}}}`, authSARToolDenied, true},
		{"watch_event allowed", `{"jsonrpc":"2.0","id":15,"method":"tools/call","params":{"name":"watch_event","arguments":{"channel_id":"123456789012345678","source":"flux","target":"aerial","event_type":"sync_success","prompt":"deploy notification","agent":"agent-alpha"}}}`, authSARAllowed, false},
		{"watch_event denied", `{"jsonrpc":"2.0","id":16,"method":"tools/call","params":{"name":"watch_event","arguments":{"channel_id":"123456789012345678","source":"flux","target":"aerial","event_type":"sync_success","prompt":"deploy notification","agent":"agent-alpha"}}}`, authSARToolDenied, true},
		{"list_schedules allowed", `{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"list_schedules","arguments":{"agent":"agent-alpha"}}}`, authSARAllowed, false},
		{"list_schedules denied", `{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"list_schedules","arguments":{"agent":"agent-alpha"}}}`, authSARToolDenied, true},
		{"list_event_watches allowed", `{"jsonrpc":"2.0","id":17,"method":"tools/call","params":{"name":"list_event_watches","arguments":{"agent":"agent-alpha"}}}`, authSARAllowed, false},
		{"list_event_watches denied", `{"jsonrpc":"2.0","id":18,"method":"tools/call","params":{"name":"list_event_watches","arguments":{"agent":"agent-alpha"}}}`, authSARToolDenied, true},
		{"update_cron_schedule allowed", `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"update_cron_schedule","arguments":{"schedule_id":"c-auth-test","effort":"medium"}}}`, authSARAllowed, false},
		{"update_cron_schedule denied", `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"update_cron_schedule","arguments":{"schedule_id":"c-auth-test","effort":"medium"}}}`, authSARToolDenied, true},
		{"update_heartbeat allowed", `{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"update_heartbeat","arguments":{"id":"h-auth-test","effort":"high"}}}`, authSARAllowed, false},
		{"update_heartbeat denied", `{"jsonrpc":"2.0","id":14,"method":"tools/call","params":{"name":"update_heartbeat","arguments":{"id":"h-auth-test","effort":"high"}}}`, authSARToolDenied, true},
		{"get_schedule allowed", `{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"get_schedule","arguments":{"schedule_id":"c-auth-test"}}}`, authSARAllowed, false},
		{"get_schedule denied", `{"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"get_schedule","arguments":{"schedule_id":"c-auth-test"}}}`, authSARToolDenied, true},
		{"cancel_schedule allowed", `{"jsonrpc":"2.0","id":19,"method":"tools/call","params":{"name":"cancel_schedule","arguments":{"schedule_id":"s-auth-test"}}}`, authSARAllowed, false},
		{"cancel_schedule denied", `{"jsonrpc":"2.0","id":20,"method":"tools/call","params":{"name":"cancel_schedule","arguments":{"schedule_id":"s-auth-test"}}}`, authSARToolDenied, true},
		{"cancel_event_watch allowed", `{"jsonrpc":"2.0","id":21,"method":"tools/call","params":{"name":"cancel_event_watch","arguments":{"watch_id":"et-auth-test"}}}`, authSARAllowed, false},
		{"cancel_event_watch denied", `{"jsonrpc":"2.0","id":22,"method":"tools/call","params":{"name":"cancel_event_watch","arguments":{"watch_id":"et-auth-test"}}}`, authSARToolDenied, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runMCPToolAuthTest(t, tc.toolRPC, &k8sauthtest.RoundTripper{AuthBody: authAgentReview, SARBody: tc.sarBody}, tc.wantToolErr)
		})
	}
}

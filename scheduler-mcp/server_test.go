package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"
)

func NewTestConfig() *Config {
	return &Config{
		DatabaseURL: ":memory:",
		Timezone:    DefaultTimezone,
		Port:        "8080",
	}
}

func setupTestServer(t *testing.T) (*Server, *httptest.Server) {
	cfg := NewTestConfig()
	db, err := InitDB(cfg)
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	handler := NewToolHandler(cfg, db)
	server := NewServer(handler)
	ts := httptest.NewServer(server.Routes())
	return server, ts
}

func TestHealthEndpoint(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/health")
	if err != nil {
		t.Fatalf("GET /health failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var body map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("Failed to decode health body: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("Expected status ok, got %v", body)
	}
}

func TestMCPInitialize(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	reqBody := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05"}}`
	resp, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(reqBody))
	if err != nil {
		t.Fatalf("POST /mcp initialize failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	var jsonResp JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&jsonResp); err != nil {
		t.Fatalf("Failed to decode JSON-RPC response: %v", err)
	}
	if jsonResp.Error != nil {
		t.Fatalf("Unexpected RPC error: %+v", jsonResp.Error)
	}

	resMap, ok := jsonResp.Result.(map[string]interface{})
	if !ok {
		t.Fatalf("Expected result map, got %+v", jsonResp.Result)
	}
	if resMap["protocolVersion"] != "2024-11-05" {
		t.Errorf("Expected protocolVersion 2024-11-05, got %v", resMap["protocolVersion"])
	}
}

func TestMCPToolsList(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	reqBody := `{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}`
	resp, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(reqBody))
	if err != nil {
		t.Fatalf("POST /mcp tools/list failed: %v", err)
	}
	defer resp.Body.Close()

	var jsonResp JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&jsonResp); err != nil {
		t.Fatalf("Failed to decode JSON-RPC response: %v", err)
	}

	resMap := jsonResp.Result.(map[string]interface{})
	tools := resMap["tools"].([]interface{})
	if len(tools) != 5 {
		t.Fatalf("Expected 5 tools, got %d", len(tools))
	}
}

func TestMCPToolsCallLifecycle(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	// 1. Call schedule_recurring via namespaced tool name
	recReq := `{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"scheduler_schedule_recurring","arguments":{"channel_id":"123","cron_expression":"0 20 * * 5","prompt":"weekly plan","effort":"high"}}}`
	resp, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(recReq))
	if err != nil {
		t.Fatalf("Call schedule_recurring failed: %v", err)
	}
	defer resp.Body.Close()

	var recResp JSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&recResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if recResp.Error != nil {
		t.Fatalf("Unexpected RPC error: %+v", recResp.Error)
	}
	resMap := recResp.Result.(map[string]interface{})
	content := resMap["content"].([]interface{})
	if len(content) == 0 {
		t.Fatalf("Expected content in response")
	}

	// Extract schedule_id
	firstContent := content[0].(map[string]interface{})
	var recData map[string]interface{}
	if err := json.Unmarshal([]byte(firstContent["text"].(string)), &recData); err != nil {
		t.Fatalf("Failed to unmarshal schedule_recurring response: %v", err)
	}
	schedID, ok := recData["schedule_id"].(string)
	if !ok || schedID == "" {
		t.Fatalf("Expected schedule_id in response: %+v", recData)
	}
	if recData["effort"] != "high" {
		t.Errorf("Expected effort 'high', got %v", recData["effort"])
	}

	// 1b. Call update_cron_schedule to switch effort to low
	updateReq := fmt.Sprintf(`{"jsonrpc":"2.0","id":31,"method":"tools/call","params":{"name":"update_cron_schedule","arguments":{"schedule_id":"%s","effort":"low"}}}`, schedID)
	respUp, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(updateReq))
	if err != nil {
		t.Fatalf("Call update_cron_schedule failed: %v", err)
	}
	defer respUp.Body.Close()

	var updateResp JSONRPCResponse
	if err := json.NewDecoder(respUp.Body).Decode(&updateResp); err != nil {
		t.Fatalf("Failed to decode update response: %v", err)
	}
	if updateResp.Error != nil {
		t.Fatalf("Unexpected RPC error on update: %+v", updateResp.Error)
	}
	upResMap := updateResp.Result.(map[string]interface{})
	upContent := upResMap["content"].([]interface{})
	var upData map[string]interface{}
	if err := json.Unmarshal([]byte(upContent[0].(map[string]interface{})["text"].(string)), &upData); err != nil {
		t.Fatalf("Failed to unmarshal update_cron_schedule response: %v", err)
	}
	if upData["effort"] != "low" {
		t.Errorf("Expected effort 'low' after update, got %v", upData["effort"])
	}

	// 2. Call schedule_once via direct tool name
	onceReq := `{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"schedule_once","arguments":{"target_id":"456","run_at":"15m","prompt":"take out trash"}}}`
	resp2, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(onceReq))
	if err != nil {
		t.Fatalf("Call schedule_once failed: %v", err)
	}
	defer resp2.Body.Close()

	var onceResp JSONRPCResponse
	if err := json.NewDecoder(resp2.Body).Decode(&onceResp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if onceResp.Error != nil {
		t.Fatalf("Unexpected RPC error: %+v", onceResp.Error)
	}

	// 3. Call list_schedules
	listReq := `{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"list_schedules","arguments":{}}}`
	resp3, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(listReq))
	if err != nil {
		t.Fatalf("Call list_schedules failed: %v", err)
	}
	defer resp3.Body.Close()

	var listResp JSONRPCResponse
	if err := json.NewDecoder(resp3.Body).Decode(&listResp); err != nil {
		t.Fatalf("Failed to decode list response: %v", err)
	}
	if listResp.Error != nil {
		t.Fatalf("Unexpected RPC error on list: %+v", listResp.Error)
	}

	// 4. Call unknown tool
	unknownReq := `{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"non_existent_tool","arguments":{}}}`
	resp4, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(unknownReq))
	if err != nil {
		t.Fatalf("Call unknown tool failed: %v", err)
	}
	defer resp4.Body.Close()

	var unknownResp JSONRPCResponse
	_ = json.NewDecoder(resp4.Body).Decode(&unknownResp)
	if unknownResp.Error == nil || unknownResp.Error.Code != -32601 {
		t.Errorf("Expected -32601 error for unknown tool, got %+v", unknownResp)
	}
}

func TestMCPUnknownMethodAndNotifications(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	// Notification (no id)
	notifReq := `{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}`
	resp, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(notifReq))
	if err != nil {
		t.Fatalf("Notification request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		t.Errorf("Expected 204 or 200 for notification, got %d", resp.StatusCode)
	}

	// Unknown method with id
	unknownReq := `{"jsonrpc":"2.0","id":99,"method":"random/unknown"}`
	resp2, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(unknownReq))
	if err != nil {
		t.Fatalf("Unknown method failed: %v", err)
	}
	defer resp2.Body.Close()

	var unknownResp JSONRPCResponse
	_ = json.NewDecoder(resp2.Body).Decode(&unknownResp)
	if unknownResp.Error == nil || unknownResp.Error.Code != -32601 {
		t.Errorf("Expected method not found error, got %+v", unknownResp)
	}
}

func TestMCP_AllHttpAndRPC_EdgeCases(t *testing.T) {
	_, ts := setupTestServer(t)
	defer ts.Close()

	// 1. GET / probe
	getResp, err := http.Get(ts.URL + "/")
	if err != nil || getResp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 for GET /, got %v, err: %v", getResp, err)
	}
	_ = getResp.Body.Close()

	// 2. PUT /mcp -> 405
	putReq, _ := http.NewRequest(http.MethodPut, ts.URL+"/mcp", bytes.NewBufferString("{}"))
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil || putResp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 Method Not Allowed, got %v, err: %v", putResp, err)
	}
	_ = putResp.Body.Close()

	// 3. POST /mcp empty body -> 400
	emptyResp, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString("  \n  "))
	if err != nil || emptyResp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for empty body, got %v, err: %v", emptyResp, err)
	}
	_ = emptyResp.Body.Close()

	// 4. Batch invalid JSON array -> parse error
	batchBadResp, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString("[{bad json"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer batchBadResp.Body.Close()
	var batchBadRPC JSONRPCResponse
	_ = json.NewDecoder(batchBadResp.Body).Decode(&batchBadRPC)
	if batchBadRPC.Error == nil || batchBadRPC.Error.Code != -32700 {
		t.Errorf("expected -32700 for malformed batch, got %+v", batchBadRPC)
	}

	// 5. Batch valid JSON array
	batchValidResp, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(`[{"jsonrpc":"2.0","id":10,"method":"ping"},{"jsonrpc":"2.0","id":11,"method":"tools/list"}]`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer batchValidResp.Body.Close()
	var batchResponses []JSONRPCResponse
	_ = json.NewDecoder(batchValidResp.Body).Decode(&batchResponses)
	if len(batchResponses) != 2 {
		t.Errorf("expected 2 responses in batch, got %d", len(batchResponses))
	}

	// 6. Single malformed JSON
	badResp, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString("{bad json"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer badResp.Body.Close()
	var badRPC JSONRPCResponse
	_ = json.NewDecoder(badResp.Body).Decode(&badRPC)
	if badRPC.Error == nil || badRPC.Error.Code != -32700 {
		t.Errorf("expected -32700 for malformed single req, got %+v", badRPC)
	}

	// 7. Ping single method
	pingResp, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(`{"jsonrpc":"2.0","id":12,"method":"ping"}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer pingResp.Body.Close()
	var pingRPC JSONRPCResponse
	_ = json.NewDecoder(pingResp.Body).Decode(&pingRPC)
	if pingRPC.Error != nil {
		t.Errorf("unexpected ping error: %+v", pingRPC.Error)
	}

	// 8. tools/call cancel_schedule
	cancelReq := `{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"cancel_schedule","arguments":{"schedule_id":"non-existent-id"}}}`
	cancelResp, err := http.Post(ts.URL+"/mcp", "application/json", bytes.NewBufferString(cancelReq))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cancelResp.Body.Close()
	var cancelRPC JSONRPCResponse
	_ = json.NewDecoder(cancelResp.Body).Decode(&cancelRPC)
	if cancelRPC.Result == nil {
		t.Errorf("expected result for cancel_schedule call, got %+v", cancelRPC)
	}
}

func TestMCP_DB_AllBranches(t *testing.T) {
	// 1. LoadConfigFromLookup combinations
	env1 := map[string]string{
		"PORT":             "9090",
		"DEFAULT_TIMEZONE": "America/New_York",
		"DATABASE_URL":     "postgres://db_url",
	}
	cfg1, err := LoadConfigFromLookup(func(k string) string { return env1[k] })
	if err != nil || cfg1.DatabaseURL != "postgres://db_url" || cfg1.Port != "9090" || cfg1.Timezone != "America/New_York" {
		t.Errorf("expected DATABASE_URL to be loaded, got %+v, err=%v", cfg1, err)
	}

	env2 := map[string]string{
		"DB_PATH": "/tmp/local.db",
	}
	cfg2, err := LoadConfigFromLookup(func(k string) string { return env2[k] })
	if err != nil || cfg2.DatabaseURL != "/tmp/local.db" || cfg2.Port != "8080" || cfg2.Timezone != DefaultTimezone {
		t.Errorf("expected DB_PATH to be loaded, got %+v, err=%v", cfg2, err)
	}

	env3 := map[string]string{
		"POSTGRES_USER":     "u",
		"POSTGRES_PASSWORD": "p",
		"POSTGRES_HOST":     "h",
		"POSTGRES_PORT":     "1234",
		"POSTGRES_DB":       "d",
	}
	cfg3, err := LoadConfigFromLookup(func(k string) string { return env3[k] })
	if err != nil || cfg3.DatabaseURL != "postgres://u:p@h:1234/d?sslmode=disable" {
		t.Errorf("expected generated postgres URL, got %+v, err=%v", cfg3, err)
	}

	// Unset all -> LoadConfigFromLookup must fail (no toxic postgres:5432 default!)
	emptyLookup := func(string) string { return "" }
	if _, err := LoadConfigFromLookup(emptyLookup); err == nil {
		t.Error("expected error from LoadConfigFromLookup when all DB env vars are unset")
	}

	// Nil lookup must fail explicitly
	if _, err := LoadConfigFromLookup(nil); err == nil {
		t.Error("expected error from LoadConfigFromLookup when lookup is nil")
	}

	// Smoke test production LoadConfig()
	_, _ = LoadConfig()

	// 2. isPostgres
	if isPostgres(nil) {
		t.Error("expected false for isPostgres(nil)")
	}

	// 3. rebindQuery
	qSQLite := rebindQuery("SELECT * FROM x WHERE a = ? AND b = ?", false)
	if qSQLite != "SELECT * FROM x WHERE a = ? AND b = ?" {
		t.Errorf("unexpected sqlite query: %s", qSQLite)
	}
	qPg := rebindQuery("SELECT * FROM x WHERE a = ? AND b = ?", true)
	if qPg != "SELECT * FROM x WHERE a = $1 AND b = $2" {
		t.Errorf("unexpected pg query: %s", qPg)
	}

	// 4. nil DB checks for db.go methods
	if err := InsertCronSchedule(nil, CronSchedule{}); err == nil {
		t.Error("expected error inserting cron with nil db")
	}
	if err := InsertOneShotSchedule(nil, OneShotSchedule{}); err == nil {
		t.Error("expected error inserting one shot with nil db")
	}
	if _, err := ListCronSchedules(nil, ""); err == nil {
		t.Error("expected error listing crons with nil db")
	}
	if _, err := ListOneShotSchedules(nil, ""); err == nil {
		t.Error("expected error listing one shots with nil db")
	}
	if _, err := DeleteSchedule(nil, "s1"); err == nil {
		t.Error("expected error deleting schedule with nil db")
	}
	if err := UpdateCronSchedule(nil, "s1", nil, nil, nil, nil, nil, nil); err == nil {
		t.Error("expected error updating cron with nil db")
	}
	if err := UpdateCronSchedule(nil, "", nil, nil, nil, nil, nil, nil); err == nil {
		t.Error("expected error updating cron with empty id")
	}

	// 5. Config validation for InitDB and NewDB
	if _, err := InitDB(nil); err == nil {
		t.Error("expected error for InitDB(nil)")
	}
	if _, err := InitDB(&Config{DatabaseURL: ""}); err == nil {
		t.Error("expected error for empty DatabaseURL")
	}
	if _, err := InitDB(&Config{DatabaseURL: "mysql://user:pass@host/db"}); err == nil {
		t.Error("expected error for unsupported scheme mysql://")
	}

	// 6. DB with valid targetID filters
	db, err := InitDB(&Config{DatabaseURL: ":memory:"})
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer db.Close()

	_ = InsertCronSchedule(db, CronSchedule{ID: "c1", TargetID: "t1", CronExpr: "0 0 * * *", Prompt: "P", NextRunAt: time.Now(), Enabled: true})
	_ = InsertOneShotSchedule(db, OneShotSchedule{ID: "o1", ThreadID: "t1", Prompt: "P", RunAt: time.Now()})

	cList, err := ListCronSchedules(db, "t1")
	if err != nil || len(cList) != 1 {
		t.Errorf("expected 1 cron schedule for t1, got %v, err: %v", cList, err)
	}
	oList, err := ListOneShotSchedules(db, "t1")
	if err != nil || len(oList) != 1 {
		t.Errorf("expected 1 one-shot schedule for t1, got %v, err: %v", oList, err)
	}

	// 7. Postgres connection failure
	origMax := postgresMaxAttempts
	origBase := postgresRetryBase
	postgresMaxAttempts = 2
	postgresRetryBase = 5 * time.Millisecond
	defer func() {
		postgresMaxAttempts = origMax
		postgresRetryBase = origBase
	}()

	_, errPg := InitDB(&Config{DatabaseURL: "postgres://invalid_user:invalid_pass@127.0.0.1:59997/db?sslmode=disable"})
	if errPg == nil {
		t.Error("expected error connecting to invalid postgres")
	}

	// 8. DeleteSchedule existing vs non-existing
	delFalse, err := DeleteSchedule(db, "non-existent-id")
	if err != nil || delFalse {
		t.Errorf("expected false, nil for non-existent schedule, got %v, %v", delFalse, err)
	}
	delTrue, err := DeleteSchedule(db, "c1")
	if err != nil || !delTrue {
		t.Errorf("expected true, nil for existing schedule, got %v, %v", delTrue, err)
	}

	// 9. RunApp with explicit config
	ctxShort, cancelShort := context.WithCancel(context.Background())
	cancelShort()
	_ = RunApp(ctxShort, &Config{DatabaseURL: ":memory:"})

	// 10. SQLite DSN with existing query parameters
	tempFile := filepath.Join(t.TempDir(), "test_params.db")
	dbParams, errParams := InitDB(&Config{DatabaseURL: tempFile + "?_pragma=foreign_keys(1)"})
	if errParams != nil {
		t.Fatalf("failed to init sqlite with query params: %v", errParams)
	}
	_ = dbParams.Close()
}


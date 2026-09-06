package main

import (
	"bytes"
	"context"
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
	"testing"
	"time"




	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/bwmarrin/discordgo"
)

type roundTripperFunc func(req *http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

func TestHandlePromptValidation(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
		DB: database,
	})
	pool.Start()
	defer pool.Stop()

	handler := handlePrompt(database, pool)

	// Test GET method not allowed
	req := httptest.NewRequest(http.MethodGet, "/prompt", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected status 405 MethodNotAllowed, got %d", w.Code)
	}

	// Test invalid empty prompt payload
	emptyPayload, _ := json.Marshal(map[string]string{"prompt": ""})
	req = httptest.NewRequest(http.MethodPost, "/prompt", bytes.NewReader(emptyPayload))
	w = httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 BadRequest for empty prompt, got %d", w.Code)
	}

	// Test invalid JSON payload
	req = httptest.NewRequest(http.MethodPost, "/prompt", bytes.NewReader([]byte("{invalid-json")))
	w = httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 BadRequest for invalid JSON, got %d", w.Code)
	}

	// Test valid prompt payload accepted
	validPayload, _ := json.Marshal(map[string]string{
		"prompt":          "Test valid prompt",
		"conversation_id": "test-conv-1",
		"message_id":      "msg-123",
	})
	req = httptest.NewRequest(http.MethodPost, "/prompt", bytes.NewReader(validPayload))
	w = httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("Expected status 202 StatusAccepted, got %d", w.Code)
	}

	// Verify message persisted to DB
	msg, err := db.GetMessage(database, "msg-123")
	if err != nil || msg == nil {
		t.Fatalf("Failed to retrieve persisted message: %v", err)
	}
	if msg.ThreadID != "test-conv-1" || msg.Content != "Test valid prompt" {
		t.Errorf("Unexpected message fields in DB: %+v", msg)
	}
}

func TestHandleTranscripts(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	handler := handleTranscripts(database)

	req := httptest.NewRequest(http.MethodGet, "/transcripts", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 OK, got %d", w.Code)
	}
}

func TestFormatCronDescription(t *testing.T) {
	tests := []struct {
		expr     string
		expected string
	}{
		{"0 9 * * 1-5", "Weekdays (Mon–Fri) at 09:00"},
		{"0 9 * * *", "Every day at 09:00"},
		{"*/15 * * * *", "Every 15 minutes"},
		{"0 */2 * * *", "Every 2 hours"},
		{"0 0 * * *", "Every day at 00:00"},
		{"30 8 * * 0", "Every Sunday at 08:30"},
		{"0 12 1 * *", "1st of every month at 12:00"},
		{"0 9 2 * *", "2nd of every month at 09:00"},
		{"0 9 3 * *", "3rd of every month at 09:00"},
		{"0 9 11 * *", "11th of every month at 09:00"},
		{"0 9 12 * *", "12th of every month at 09:00"},
		{"0 9 13 * *", "13th of every month at 09:00"},
		{"0 9 21 * *", "21st of every month at 09:00"},
		{"0 9 22 * *", "22nd of every month at 09:00"},
		{"0 9 23 * *", "23rd of every month at 09:00"},
		{"0 9 31 * *", "31st of every month at 09:00"},
		{"0 9 * * Mon,Wed,Fri", "Mon, Wed, Fri at 09:00"},
		{"0 9 1 1 1", "At 09:00 (cron: 0 9 1 1 1)"},
		{"foo bar * * *", "foo bar * * *"},
		{"* * * * *", "Every minute"},
		{"@daily", "Every day at 00:00"},
		{"@hourly", "Every hour"},
	}



	for _, tt := range tests {
		got := FormatCronDescription(tt.expr)
		if got != tt.expected {
			t.Errorf("FormatCronDescription(%q) = %q, want %q", tt.expr, got, tt.expected)
		}
	}
}

func TestSanitizeString(t *testing.T) {
	tests := []struct {
		input       string
		mustNotHave string
		mustHave    string
	}{
		{
			input:       "Execute prompt with token ghp_1234567890abcdefABCDEF123456",
			mustNotHave: "ghp_1234567890abcdefABCDEF123456",
			mustHave:    "[REDACTED]",
		},
		{
			input:       "Error: failed with github_pat_11ABCD1234_abcdef5678",
			mustNotHave: "github_pat_11ABCD1234_abcdef5678",
			mustHave:    "[REDACTED]",
		},
		{
			input:       "Clean prompt with no tokens to redact",
			mustNotHave: "[REDACTED]",
			mustHave:    "Clean prompt with no tokens to redact",
		},
	}

	for _, tt := range tests {
		got := SanitizeString(tt.input)
		if tt.mustNotHave != "" && strings.Contains(got, tt.mustNotHave) {
			t.Errorf("SanitizeString(%q) leaked token %q: %s", tt.input, tt.mustNotHave, got)
		}
		if tt.mustHave != "" && !strings.Contains(got, tt.mustHave) {
			t.Errorf("SanitizeString(%q) = %q, expected to contain %q", tt.input, got, tt.mustHave)
		}
	}
}

func TestSchedulesEndpoints(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()

	// 1. Seed cron schedule
	cron1 := db.CronSchedule{
		ID:          "cron-1",
		TargetID:    "chan-123",
		TitlePrefix: "Morning Brief",
		CronExpr:    "0 9 * * 1-5",
		Prompt:      "Check status with secret token ghp_secretToken1234567890",
		Timezone:    "America/Los_Angeles",
		NextRunAt:   now.Add(1 * time.Hour),
		Enabled:     true,
		CreatedAt:   now,
	}
	if err := db.CreateCronSchedule(database, cron1); err != nil {
		t.Fatalf("CreateCronSchedule failed: %v", err)
	}

	// 2. Seed one-shot schedule
	once1 := db.OneShotSchedule{
		ID:        "once-1",
		ThreadID:  "thread-456",
		Prompt:    "Reminder with secret key github_pat_1234567890abcdef",
		RunAt:     now.Add(30 * time.Minute),
		CreatedAt: now,
	}
	if err := db.CreateOneShotSchedule(database, once1); err != nil {
		t.Fatalf("CreateOneShotSchedule failed: %v", err)
	}

	// 3. Seed schedule runs
	runCompleted := db.ScheduleRun{
		ID:           "run-1",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
		MessageID:    "msg-1",
		TargetID:     "chan-123",
		ThreadID:     "th-1",
		Title:        "Morning Brief",
		Prompt:       "Check status with secret token ghp_secretToken1234567890",
		Status:       "completed",
		StartedAt:    now.Add(-2 * time.Hour),
		DurationMs:   4500,
	}
	if err := db.CreateScheduleRun(database, runCompleted); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}

	runFailed := db.ScheduleRun{
		ID:           "run-2",
		ScheduleID:   "cron-1",
		ScheduleType: "cron",
		MessageID:    "msg-2",
		TargetID:     "chan-123",
		ThreadID:     "th-2",
		Title:        "Morning Brief",
		Prompt:       "Run routine",
		Status:       "failed",
		StartedAt:    now.Add(-1 * time.Hour),
		DurationMs:   1200,
		Error:        "Failed auth with token ghp_errorToken99999999",
	}
	if err := db.CreateScheduleRun(database, runFailed); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}

	// Test GET /schedules
	schedulesHandler := handleSchedules(database)
	req := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	w := httptest.NewRecorder()
	schedulesHandler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK from /schedules, got %d: %s", w.Code, w.Body.String())
	}

	var schedResp SchedulesResponse
	if err := json.Unmarshal(w.Body.Bytes(), &schedResp); err != nil {
		t.Fatalf("Failed to parse /schedules response JSON: %v", err)
	}

	if schedResp.Status != "ok" {
		t.Errorf("Expected status ok, got %s", schedResp.Status)
	}
	if schedResp.Summary.TotalActive != 2 || schedResp.Summary.CronCount != 1 || schedResp.Summary.OneShotCount != 1 {
		t.Errorf("Unexpected summary metrics: %+v", schedResp.Summary)
	}
	if len(schedResp.Crons) != 1 {
		t.Fatalf("Expected 1 cron schedule, got %d", len(schedResp.Crons))
	}
	cronItem := schedResp.Crons[0]
	if cronItem.ID != "cron-1" || cronItem.ChannelID != "chan-123" {
		t.Errorf("Unexpected cron item fields: %+v", cronItem)
	}
	if cronItem.CronDescription != "Weekdays (Mon–Fri) at 09:00" {
		t.Errorf("Expected cron_description 'Weekdays (Mon–Fri) at 09:00', got %q", cronItem.CronDescription)
	}
	if strings.Contains(cronItem.Prompt, "ghp_secretToken1234567890") {
		t.Errorf("Prompt token not redacted in /schedules: %s", cronItem.Prompt)
	}
	if !strings.Contains(cronItem.Prompt, "[REDACTED]") {
		t.Errorf("Expected [REDACTED] in sanitized prompt, got %s", cronItem.Prompt)
	}

	if len(schedResp.OneShots) != 1 {
		t.Fatalf("Expected 1 one-shot schedule, got %d", len(schedResp.OneShots))
	}
	oneShotItem := schedResp.OneShots[0]
	if oneShotItem.ID != "once-1" || oneShotItem.ThreadID != "thread-456" {
		t.Errorf("Unexpected one-shot item fields: %+v", oneShotItem)
	}
	if strings.Contains(oneShotItem.Prompt, "github_pat_1234567890abcdef") {
		t.Errorf("Prompt token not redacted in one_shots: %s", oneShotItem.Prompt)
	}

	// Test Caching: Adding another schedule directly in DB should not change cached output immediately
	cron2 := db.CronSchedule{
		ID:          "cron-2",
		TargetID:    "chan-999",
		TitlePrefix: "Nightly",
		CronExpr:    "0 0 * * *",
		Prompt:      "Nightly check",
		NextRunAt:   now.Add(12 * time.Hour),
		Enabled:     true,
		CreatedAt:   now,
	}
	_ = db.CreateCronSchedule(database, cron2)

	wCached := httptest.NewRecorder()
	schedulesHandler(wCached, req)
	var cachedResp SchedulesResponse
	_ = json.Unmarshal(wCached.Body.Bytes(), &cachedResp)
	if len(cachedResp.Crons) != 1 {
		t.Errorf("Expected cached 1 cron schedule within 5s TTL, got %d", len(cachedResp.Crons))
	}

	// Test GET /schedules/runs
	runsHandler := handleScheduleRuns(database)
	reqRuns := httptest.NewRequest(http.MethodGet, "/schedules/runs", nil)
	wRuns := httptest.NewRecorder()
	runsHandler(wRuns, reqRuns)

	if wRuns.Code != http.StatusOK {
		t.Fatalf("Expected status 200 OK from /schedules/runs, got %d: %s", wRuns.Code, wRuns.Body.String())
	}

	var runsResp ScheduleRunsResponse
	if err := json.Unmarshal(wRuns.Body.Bytes(), &runsResp); err != nil {
		t.Fatalf("Failed to parse /schedules/runs response JSON: %v", err)
	}

	if runsResp.Status != "ok" || runsResp.Total != 2 || len(runsResp.Runs) != 2 {
		t.Errorf("Unexpected runs response: total=%d, len=%d", runsResp.Total, len(runsResp.Runs))
	}

	// Check token sanitization in error and prompt
	for _, r := range runsResp.Runs {
		if strings.Contains(r.Prompt, "ghp_secretToken1234567890") {
			t.Errorf("Run prompt token leaked: %s", r.Prompt)
		}
		if strings.Contains(r.Error, "ghp_errorToken99999999") {
			t.Errorf("Run error token leaked: %s", r.Error)
		}
	}

	// Test filtering /schedules/runs?status=failed
	reqFiltered := httptest.NewRequest(http.MethodGet, "/schedules/runs?status=failed", nil)
	wFiltered := httptest.NewRecorder()
	runsHandler(wFiltered, reqFiltered)

	var filteredResp ScheduleRunsResponse
	_ = json.Unmarshal(wFiltered.Body.Bytes(), &filteredResp)
	if filteredResp.Total != 1 || len(filteredResp.Runs) != 1 || filteredResp.Runs[0].Status != "failed" {
		t.Errorf("Unexpected filtered runs: %+v", filteredResp)
	}

	// Test pagination /schedules/runs?limit=1&offset=0
	reqPaginated := httptest.NewRequest(http.MethodGet, "/schedules/runs?limit=1&offset=0", nil)
	wPaginated := httptest.NewRecorder()
	runsHandler(wPaginated, reqPaginated)

	var paginatedResp ScheduleRunsResponse
	_ = json.Unmarshal(wPaginated.Body.Bytes(), &paginatedResp)
	if paginatedResp.Total != 2 || len(paginatedResp.Runs) != 1 || paginatedResp.Limit != 1 {
		t.Errorf("Unexpected paginated runs: %+v", paginatedResp)
	}

	// Test MethodNotAllowed for POST
	reqPost := httptest.NewRequest(http.MethodPost, "/schedules", nil)
	wPost := httptest.NewRecorder()
	schedulesHandler(wPost, reqPost)
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 MethodNotAllowed for POST /schedules, got %d", wPost.Code)
	}

	reqPostRuns := httptest.NewRequest(http.MethodPost, "/schedules/runs", nil)
	wPostRuns := httptest.NewRecorder()
	runsHandler(wPostRuns, reqPostRuns)
	if wPostRuns.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 MethodNotAllowed for POST /schedules/runs, got %d", wPostRuns.Code)
	}
}

func TestHandleTasks(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	now := time.Now().UTC()
	longPrompt := strings.Repeat("A", 600) + " ghp_123456789012345678901234567890123456"

	_ = db.InsertMessage(database, db.Message{
		ID:         "msg-test-task",
		ThreadID:   "thread-1",
		AuthorID:   "user-1",
		AuthorName: "Tester with token ghp_999999999999999999999999999999999999",
		Content:    "Secret token ghp_123456789012345678901234567890123456 in prompt " + longPrompt,
		Status:     db.StatusProcessing,
		CreatedAt:  now,
		UpdatedAt:  now,
	})

	handler := handleTasks(database)

	// 1. Test GET request returns 200 OK
	req := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", rr.Code, rr.Body.String())
	}

	var resp struct {
		Status string          `json:"status"`
		Total  int             `json:"total"`
		Tasks  []db.ActiveTask `json:"tasks"`
	}
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode json: %v", err)
	}

	if resp.Status != "ok" || resp.Total != 1 || len(resp.Tasks) != 1 {
		t.Fatalf("unexpected response payload: %+v", resp)
	}

	// 2. Verify token was redacted in prompt, summary, and author name
	if strings.Contains(resp.Tasks[0].Prompt, "ghp_123456") {
		t.Errorf("token was not redacted from prompt: %s", resp.Tasks[0].Prompt)
	}
	if strings.Contains(resp.Tasks[0].Summary, "ghp_123456") {
		t.Errorf("token was not redacted from summary: %s", resp.Tasks[0].Summary)
	}
	if resp.Tasks[0].Summary == "" {
		t.Errorf("expected summary to be populated, got empty")
	}
	if strings.Contains(resp.Tasks[0].AuthorName, "ghp_999999") {
		t.Errorf("token was not redacted from author name: %s", resp.Tasks[0].AuthorName)
	}

	// Verify truncation to <= 503 runes (500 + "...")
	promptRunes := []rune(resp.Tasks[0].Prompt)
	if len(promptRunes) > 503 || !strings.HasSuffix(resp.Tasks[0].Prompt, "...") {
		t.Errorf("prompt was not properly truncated to 500 chars with ellipsis: len=%d, text=%s", len(promptRunes), resp.Tasks[0].Prompt)
	}

	// 3. Test 1s TTL Caching: inserting another active message should not appear immediately
	_ = db.InsertMessage(database, db.Message{
		ID:         "msg-test-task-2",
		ThreadID:   "thread-2",
		AuthorID:   "user-2",
		AuthorName: "Tester 2",
		Content:    "Second prompt",
		Status:     db.StatusPending,
		CreatedAt:  now.Add(time.Second),
		UpdatedAt:  now.Add(time.Second),
	})

	rrCached := httptest.NewRecorder()
	handler.ServeHTTP(rrCached, req)
	if rrCached.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from cached call, got %d", rrCached.Code)
	}

	var cachedResp struct {
		Status string          `json:"status"`
		Total  int             `json:"total"`
		Tasks  []db.ActiveTask `json:"tasks"`
	}
	if err := json.NewDecoder(rrCached.Body).Decode(&cachedResp); err != nil {
		t.Fatalf("failed to decode cached json: %v", err)
	}
	if cachedResp.Total != 1 || len(cachedResp.Tasks) != 1 {
		t.Errorf("expected 1 cached task within 1s TTL, got %d", cachedResp.Total)
	}

	// 4. Test Method Not Allowed (e.g. POST)
	reqPost := httptest.NewRequest(http.MethodPost, "/tasks", nil)
	rrPost := httptest.NewRecorder()
	handler.ServeHTTP(rrPost, reqPost)
	if rrPost.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 Method Not Allowed, got %d", rrPost.Code)
	}
}

func TestMetricsEndpoint(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())

	// Record sample metrics to instantiate metric vectors
	metrics.RecordTurnCompleted("success", "discord", "Gemini 3.8 Flash (Low)", 2*time.Second)
	metrics.RecordClassifierRun("success", "Gemini 3.8 Flash (Low)", 500*time.Millisecond, 0.95, "wake")
	metrics.DiscordEventsTotal.WithLabelValues("ready").Inc()
	metrics.DiscordMessagesProcessedTotal.WithLabelValues("false", "enqueued").Inc()
	metrics.SchedulerExecutionsTotal.WithLabelValues("cron", "enqueued").Inc()

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200 OK from /metrics, got %d", rec.Code)
	}

	body := rec.Body.String()
	requiredSubstrings := []string{
		"aerial_brain_turns_total",
		"aerial_brain_turn_duration_seconds",
		"aerial_brain_active_workers",
		"aerial_brain_queue_depth",
		"aerial_brain_classifier_duration_seconds",
		"aerial_brain_classifier_decisions_total",
		"aerial_brain_classifier_confidence_score",
		"aerial_brain_discord_events_total",
		"aerial_brain_discord_messages_processed_total",
		"aerial_brain_scheduler_executions_total",
		"aerial_brain_build_info",
	}

	for _, sub := range requiredSubstrings {
		if !strings.Contains(body, sub) {
			t.Errorf("expected /metrics output to contain %q, body:\n%s", sub, body)
		}
	}
}



func TestNormalizeRoute(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"/prompt", "/prompt"},
		{"/transcripts", "/transcripts"},
		{"/transcripts/abc-123", "/transcripts"},
		{"/tasks", "/tasks"},
		{"/tasks/456", "/tasks"},
		{"/facts", "/facts"},
		{"/facts/789", "/facts"},
		{"/schedules", "/schedules"},
		{"/schedules/runs", "/schedules/runs"},
		{"/internal/reload", "/internal/reload"},
		{"/health", "/health"},
		{"/metrics", "/metrics"},
		{"/unknown/path", "unmatched"},
		{"/random", "unmatched"},
	}

	for _, tt := range tests {
		got := normalizeRoute(tt.input)
		if got != tt.expected {
			t.Errorf("normalizeRoute(%q) = %q; want %q", tt.input, got, tt.expected)
		}
	}
}

func TestMetricsMiddleware(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/unknown", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})

	handler := metricsMiddleware(mux)

	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	req404 := httptest.NewRequest(http.MethodGet, "/unknown", nil)
	rec404 := httptest.NewRecorder()
	handler.ServeHTTP(rec404, req404)

	if rec404.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec404.Code)
	}
}

type mockFlusherRecorder struct {
	*httptest.ResponseRecorder
	flushed bool
}

func (m *mockFlusherRecorder) Flush() {
	m.flushed = true
}

func TestStatusRecorder_Flush(t *testing.T) {
	// With flusher
	rec := &mockFlusherRecorder{ResponseRecorder: httptest.NewRecorder()}
	sr := &statusRecorder{ResponseWriter: rec, statusCode: http.StatusOK}
	sr.Flush()
	if !rec.flushed {
		t.Errorf("Expected Flush() to be forwarded to underlying flusher")
	}

	// Without flusher
	plainRec := httptest.NewRecorder()
	srPlain := &statusRecorder{ResponseWriter: struct{ http.ResponseWriter }{plainRec}, statusCode: http.StatusOK}
	srPlain.Flush() // should not panic
}

func TestHandleFacts_Comprehensive(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	// Seed some facts
	_, _ = db.InsertFact(database, "system", "Fact 1", 1.0, "t1", nil)
	_, _ = db.InsertFact(database, "user_preference", "Fact 2", 1.0, "t2", nil)

	handler := handleFacts(database)

	// 1. Method not allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/facts", nil)
	wPost := httptest.NewRecorder()
	handler(wPost, reqPost)
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 MethodNotAllowed, got %d", wPost.Code)
	}

	// 2. Valid GET without filters
	req := httptest.NewRequest(http.MethodGet, "/facts", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK from /facts, got %d: %s", w.Code, w.Body.String())
	}

	// 3. Valid GET with query params: category, limit, offset, q (including long string > 64 runes)
	longQuery := strings.Repeat("a", 100)
	reqFiltered := httptest.NewRequest(http.MethodGet, "/facts?category=system&limit=10&offset=0&q="+longQuery, nil)
	wFiltered := httptest.NewRecorder()
	handler(wFiltered, reqFiltered)
	if wFiltered.Code != http.StatusOK {
		t.Errorf("Expected 200 OK with filters, got %d: %s", wFiltered.Code, wFiltered.Body.String())
	}

	// 4. Closed DB error branch
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	handlerErr := handleFacts(closedDB)
	wErr := httptest.NewRecorder()
	handlerErr(wErr, req)
	if wErr.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 InternalServerError on closed DB, got %d", wErr.Code)
	}
}

func TestOrdinal_AllCases(t *testing.T) {
	cases := []struct {
		n    int
		want string
	}{
		{1, "1st"},
		{2, "2nd"},
		{3, "3rd"},
		{4, "4th"},
		{11, "11th"},
		{12, "12th"},
		{13, "13th"},
		{14, "14th"},
		{21, "21st"},
		{22, "22nd"},
		{23, "23rd"},
		{24, "24th"},
		{31, "31st"},
	}

	for _, tc := range cases {
		got := ordinal(tc.n)
		if got != tc.want {
			t.Errorf("ordinal(%d) = %q, want %q", tc.n, got, tc.want)
		}
	}
}

func TestFormatCronDescription_AllVariations(t *testing.T) {
	cases := []struct {
		expr string
		want string
	}{
		{"", ""},
		{"   ", ""},
		{"@yearly", "Every year on Jan 1st at 00:00"},
		{"@annually", "Every year on Jan 1st at 00:00"},
		{"@monthly", "1st of every month at 00:00"},
		{"@weekly", "Every week on Sunday at 00:00"},
		{"@daily", "Every day at 00:00"},
		{"@midnight", "Every day at 00:00"},
		{"@hourly", "Every hour"},
		{"invalid cron string", "invalid cron string"},
		{"* * * * *", "Every minute"},
		{"*/10 * * * *", "Every 10 minutes"},
		{"0 */4 * * *", "Every 4 hours"},
		{"0 9 * * *", "Every day at 09:00"},
		{"0 9 * * 1-5", "Weekdays (Mon–Fri) at 09:00"},
		{"0 9 * * MON-FRI", "Weekdays (Mon–Fri) at 09:00"},
		{"0 9 * * 0,6", "Weekends (Sat–Sun) at 09:00"},
		{"0 9 * * SAT,SUN", "Weekends (Sat–Sun) at 09:00"},
		{"0 9 * * 2", "Every Tuesday at 09:00"},
		{"0 9 * * 1,3,5", "Mon, Wed, Fri at 09:00"},
		{"0 12 1 * *", "1st of every month at 12:00"},
		{"0 12 22 * *", "22nd of every month at 12:00"},
		{"0 0 1 1 *", "Every year on Jan 1st at 00:00"},
		{"0 0 4 7 *", "Every year on Jul 4th at 00:00"},
		{"0 9 * 5 *", "At 09:00 (cron: 0 9 * 5 *)"},
	}

	for _, tc := range cases {
		got := FormatCronDescription(tc.expr)
		if got != tc.want {
			t.Errorf("FormatCronDescription(%q) = %q, want %q", tc.expr, got, tc.want)
		}
	}
}

func TestHandleSchedules_And_Runs_ErrorBranches(t *testing.T) {
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()

	// 1. handleSchedules with closed DB
	schedHandler := handleSchedules(closedDB)
	req := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	w := httptest.NewRecorder()
	schedHandler(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 on closed DB from handleSchedules, got %d", w.Code)
	}

	// 2. handleScheduleRuns with closed DB
	runsHandler := handleScheduleRuns(closedDB)
	reqRuns := httptest.NewRequest(http.MethodGet, "/schedules/runs", nil)
	wRuns := httptest.NewRecorder()
	runsHandler(wRuns, reqRuns)
	if wRuns.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 on closed DB from handleScheduleRuns, got %d", wRuns.Code)
	}

	// 3. handleTasks with closed DB
	tasksHandler := handleTasks(closedDB)
	reqTasks := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	wTasks := httptest.NewRecorder()
	tasksHandler(wTasks, reqTasks)
	if wTasks.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 on closed DB from handleTasks, got %d", wTasks.Code)
	}
}

func TestSetupBrainMux_And_Endpoints(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	reloaded := false
	reloadFn := func(source string) {
		reloaded = true
	}

	mux := SetupBrainMux(database, nil, reloadFn)

	// Test /health
	reqHealth := httptest.NewRequest(http.MethodGet, "/health", nil)
	wHealth := httptest.NewRecorder()
	mux.ServeHTTP(wHealth, reqHealth)
	if wHealth.Code != http.StatusOK {
		t.Errorf("Expected 200 from /health, got %d", wHealth.Code)
	}

	// Test /internal/reload (GET -> 405, POST -> 200)
	reqReloadGet := httptest.NewRequest(http.MethodGet, "/internal/reload", nil)
	wReloadGet := httptest.NewRecorder()
	mux.ServeHTTP(wReloadGet, reqReloadGet)
	if wReloadGet.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 from GET /internal/reload, got %d", wReloadGet.Code)
	}

	reqReloadPost := httptest.NewRequest(http.MethodPost, "/internal/reload", nil)
	wReloadPost := httptest.NewRecorder()
	mux.ServeHTTP(wReloadPost, reqReloadPost)
	if wReloadPost.Code != http.StatusOK || !reloaded {
		t.Errorf("Expected 200 and reloaded=true from POST /internal/reload, got %d, %t", wReloadPost.Code, reloaded)
	}
}

func TestInitializeBrainEnvironment_And_Config(t *testing.T) {
	cfg := config.Config{Model: "gemini-2.5-flash"}
	bCfg := NewBrainConfigFromEnv(cfg)
	if bCfg.Model != "gemini-2.5-flash" {
		t.Errorf("Unexpected model in BrainConfig: %s", bCfg.Model)
	}

	InitializeBrainEnvironment("", "gemini-2.5-flash", "test prompt")
	InitializeBrainEnvironment("test-api-key", "gemini-2.5-flash", "test prompt")

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{})
	reloadFn := CreateReloadConfigFunc(pool, "test-key", "test prompt", nil)
	reloadFn("UnitTest")
}

func TestRunBrainApp_Lifecycle(t *testing.T) {
	t.Setenv("AGY_BIN", "/bin/true")
	t.Setenv("PORT", "0")

	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test_brain.db")

	bCfg := BrainConfig{
		Port:         "0",
		AgyBin:       "/bin/true",
		Model:        "gemini-2.5-flash",
		APIKey:       "",
		SystemPrompt: "test",
		DBPath:       dbPath,
		DiscordToken: "",
	}

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)

	err := RunBrainApp(ctx, bCfg)
	if err != nil && err != http.ErrServerClosed {
		t.Errorf("Unexpected error running Brain app: %v", err)
	}

	// Test invalid DB path fails cleanly
	badCfg := BrainConfig{
		DBPath: "/nonexistent/invalid/dir/db.sqlite",
	}
	badCtx, badCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer badCancel()
	_ = RunBrainApp(badCtx, badCfg)
}

func TestHandleSchedules_Endpoints(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	_ = db.CreateCronSchedule(database, db.CronSchedule{
		ID:          "cron-1",
		TargetID:    "chan-1",
		TitlePrefix: "Daily news",
		CronExpr:    "0 8 * * *",
		Prompt:      "Morning briefing",
		Timezone:    "America/Los_Angeles",
		Enabled:     true,
	})
	_ = db.CreateOneShotSchedule(database, db.OneShotSchedule{
		ID:        "one-1",
		ThreadID:  "chan-1",
		Prompt:    "Remind me",
		RunAt:     time.Now().UTC().Add(1 * time.Hour),
		CreatedAt: time.Now().UTC(),
	})

	handler := handleSchedules(database)

	// 1. Method not allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/schedules", nil)
	wPost := httptest.NewRecorder()
	handler(wPost, reqPost)
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for POST /schedules, got %d", wPost.Code)
	}

	// 2. Successful GET
	reqGet := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	wGet := httptest.NewRecorder()
	handler(wGet, reqGet)
	if wGet.Code != http.StatusOK {
		t.Errorf("expected 200 for GET /schedules, got %d", wGet.Code)
	}

	// 3. Cached GET
	wGetCached := httptest.NewRecorder()
	handler(wGetCached, reqGet)
	if wGetCached.Code != http.StatusOK {
		t.Errorf("expected 200 for cached GET /schedules, got %d", wGetCached.Code)
	}

	// 4. Closed DB returns error
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	handlerClosed := handleSchedules(closedDB)
	wClosed := httptest.NewRecorder()
	handlerClosed(wClosed, reqGet)
	if wClosed.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for closed DB schedules, got %d", wClosed.Code)
	}
}

func TestHandleTasks_Endpoints(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	longPrompt := strings.Repeat("Task description that is very long. ", 30)
	_ = db.InsertMessage(database, db.Message{
		ID:         "task-msg-1",
		ThreadID:   "th-task-1",
		AuthorID:   "u1",
		AuthorName: "User",
		Content:    longPrompt,
		Status:     db.StatusProcessing,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	})

	handler := handleTasks(database)

	// 1. Method not allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/tasks", nil)
	wPost := httptest.NewRecorder()
	handler(wPost, reqPost)
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 for POST /tasks, got %d", wPost.Code)
	}

	// 2. Successful GET
	reqGet := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	wGet := httptest.NewRecorder()
	handler(wGet, reqGet)
	if wGet.Code != http.StatusOK {
		t.Errorf("expected 200 for GET /tasks, got %d", wGet.Code)
	}

	// 3. Cached GET
	wGetCached := httptest.NewRecorder()
	handler(wGetCached, reqGet)
	if wGetCached.Code != http.StatusOK {
		t.Errorf("expected 200 for cached GET /tasks, got %d", wGetCached.Code)
	}

	// 4. Closed DB error
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	handlerClosed := handleTasks(closedDB)
	wClosed := httptest.NewRecorder()
	handlerClosed(wClosed, reqGet)
	if wClosed.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for closed DB tasks, got %d", wClosed.Code)
	}
}

func TestHandleTranscripts_WithIncludeRaw(t *testing.T) {
	tmpHome := t.TempDir()
	_ = os.Setenv("HOME", tmpHome)

	logDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "brain", "conv-raw-test", ".system_generated", "logs")
	_ = os.MkdirAll(logDir, 0755)
	logContent := `{"step_index":0,"status":"RUNNING"}` + "\n" + `{"step_index":1,"status":"ERROR","error":"rate limit exceeded"}` + "\n"
	_ = os.WriteFile(filepath.Join(logDir, "transcript_full.jsonl"), []byte(logContent), 0644)

	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	_ = db.SaveSessionID(database, "th-ext-1", "conv-raw-test")

	handler := handleTranscripts(database)
	req := httptest.NewRequest(http.MethodGet, "/transcripts?include_raw=true", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for /transcripts?include_raw=true, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "conv-raw-test") {
		t.Errorf("expected transcript response to contain conv-raw-test, got: %s", w.Body.String())
	}
}

func TestHandlePrompt_AutoGeneratedIDsAndClosedDB(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})
	pool.Start()
	defer pool.Stop()

	handler := handlePrompt(database, pool)

	// 1. Auto-generate message_id and conversation_id
	payload, _ := json.Marshal(map[string]string{"prompt": "Auto ID prompt"})
	req := httptest.NewRequest(http.MethodPost, "/prompt", bytes.NewReader(payload))
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202 for prompt without IDs, got %d", w.Code)
	}

	// 2. Closed DB logging error
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	handlerClosed := handlePrompt(closedDB, nil)
	req2 := httptest.NewRequest(http.MethodPost, "/prompt", bytes.NewReader(payload))
	w2 := httptest.NewRecorder()
	handlerClosed(w2, req2)
	if w2.Code != http.StatusAccepted {
		t.Errorf("expected 202 even when DB insert fails, got %d", w2.Code)
	}
}

func TestCreateReloadConfigFunc_InvalidYAMLAndAlert(t *testing.T) {
	tmpDir := t.TempDir()
	badConfigPath := filepath.Join(tmpDir, "config.yaml")
	_ = os.WriteFile(badConfigPath, []byte("invalid: [yaml: content"), 0644)
	_ = os.Setenv("CONFIG_PATH", badConfigPath)
	defer func() { _ = os.Unsetenv("CONFIG_PATH") }()

	dg, _ := discordgo.New("Bot test-token")
	dg.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(bytes.NewReader([]byte("{}"))),
			Header:     make(http.Header),
		}, nil
	})

	database, _ := db.InitDB(":memory:")
	defer func() { _ = database.Close() }()
	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})

	reloadFn := CreateReloadConfigFunc(pool, "test-key", "test prompt", dg)
	reloadFn("TestReload")
}

func TestInitializeBrainEnvironment_AllBranches(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)
	_ = os.MkdirAll(filepath.Join(tmpDir, ".gemini", "config", "plugins", "superpowers", ".git"), 0755)

	// Test with non-empty apiKey, model, systemPrompt
	InitializeBrainEnvironment("test-key", "gemini-model-xyz", "Test system prompt")
}

func TestHandleScheduleRuns_Endpoints(t *testing.T) {

	database, _ := db.InitDB(":memory:")
	defer func() { _ = database.Close() }()

	handler := handleScheduleRuns(database)

	// 1. Method not allowed
	req := httptest.NewRequest(http.MethodPost, "/schedules/runs", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}

	// 2. Valid GET with query params
	req = httptest.NewRequest(http.MethodGet, "/schedules/runs?limit=10&schedule_id=sched-123", nil)
	w = httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	// 3. Closed DB error
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	handlerClosed := handleScheduleRuns(closedDB)
	req = httptest.NewRequest(http.MethodGet, "/schedules/runs", nil)
	w = httptest.NewRecorder()
	handlerClosed(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on closed DB, got %d", w.Code)
	}
}

func TestHandleSchedules_CacheAndErrors(t *testing.T) {
	database, _ := db.InitDB(":memory:")
	defer func() { _ = database.Close() }()

	handler := handleSchedules(database)

	// 1. First fetch (populates cache)
	req := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	// 2. Second fetch immediately (hits cache)
	req = httptest.NewRequest(http.MethodGet, "/schedules", nil)
	w = httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 from cache, got %d", w.Code)
	}

	// 3. Closed DB error
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	handlerClosed := handleSchedules(closedDB)
	req = httptest.NewRequest(http.MethodGet, "/schedules", nil)
	w = httptest.NewRecorder()
	handlerClosed(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on closed DB, got %d", w.Code)
	}
}

func TestHandleTasks_LongPromptAndCache(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	longPrompt := strings.Repeat("a", 600)
	_ = db.InsertMessage(database, db.Message{
		ID:        "msg-long-task",
		ThreadID:  "th-task-1",
		Content:   longPrompt,
		Status:    db.StatusProcessing,
		CreatedAt: time.Now().UTC(),
	})

	handler := handleTasks(database)

	// 1. Method not allowed
	req := httptest.NewRequest(http.MethodPost, "/tasks", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}

	// 2. First fetch (formats long prompt and populates cache)
	req = httptest.NewRequest(http.MethodGet, "/tasks", nil)
	w = httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	// 3. Cache hit fetch
	req = httptest.NewRequest(http.MethodGet, "/tasks", nil)
	w = httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200 from cache, got %d", w.Code)
	}

	// 4. Closed DB error
	closedDB, _ := db.InitDB(":memory:")
	_ = closedDB.Close()
	closedHandler := handleTasks(closedDB)
	req = httptest.NewRequest(http.MethodGet, "/tasks", nil)
	w = httptest.NewRecorder()
	closedHandler(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on closed DB, got %d", w.Code)
	}
}


func TestHandleSchedules_WithPopulatedRows(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	_ = db.CreateCronSchedule(database, db.CronSchedule{
		ID:          "cron-pop-1",
		TargetID:    "chan-pop-1",
		TitlePrefix: "Populated cron",
		CronExpr:    "0 9 * * 1-5",
		Prompt:      "Morning standup briefing",
		Timezone:    "America/Los_Angeles",
		Enabled:     true,
	})

	_ = db.CreateOneShotSchedule(database, db.OneShotSchedule{
		ID:       "oneshot-pop-1",
		ThreadID: "th-pop-1",
		Prompt:   "Remind me in 10 minutes",
		RunAt:    time.Now().UTC().Add(10 * time.Minute),
	})

	handler := handleSchedules(database)
	req := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}

	var resp SchedulesResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode schedules response: %v", err)
	}
	if len(resp.Crons) != 1 || len(resp.OneShots) != 1 {
		t.Errorf("expected 1 cron and 1 one-shot, got %d and %d", len(resp.Crons), len(resp.OneShots))
	}
}

func TestMetricsMiddleware_FlushAndNormalizeRoutes(t *testing.T) {
	routes := []string{
		"/prompt",
		"/transcripts",
		"/transcripts/raw",
		"/tasks",
		"/tasks/active",
		"/facts",
		"/facts/recent",
		"/schedules",
		"/schedules/runs",
		"/internal/reload",
		"/health",
		"/metrics",
		"/unknown/route",
	}

	for _, r := range routes {
		_ = normalizeRoute(r)
	}

	rec := &statusRecorder{
		ResponseWriter: httptest.NewRecorder(),
		statusCode:     200,
	}
	rec.WriteHeader(http.StatusAccepted)
	rec.Flush()
	if rec.statusCode != http.StatusAccepted {
		t.Errorf("expected 202, got %d", rec.statusCode)
	}
}

func TestFormatCronDescription_Fallback(t *testing.T) {

	invalidExpr := "invalid cron expression string"
	if desc := FormatCronDescription(invalidExpr); desc != invalidExpr {
		t.Errorf("expected %q, got %q", invalidExpr, desc)
	}
}

func TestCreateReloadConfigFunc_SuccessBranches(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)
	_ = os.MkdirAll(filepath.Join(tmpDir, ".gemini", "config", "plugins", "superpowers", ".git"), 0755)

	validConfig := `
model: "gemini-2.5-pro"
`
	validConfigPath := filepath.Join(tmpDir, "config.yaml")
	_ = os.WriteFile(validConfigPath, []byte(validConfig), 0644)
	_ = os.Setenv("CONFIG_PATH", validConfigPath)
	defer func() { _ = os.Unsetenv("CONFIG_PATH") }()

	dg, _ := discordgo.New("Bot test-token")
	database, _ := db.InitDB(":memory:")
	defer func() { _ = database.Close() }()
	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{DB: database})

	reloadFn := CreateReloadConfigFunc(pool, "test-api-key", "Test reload prompt", dg)
	reloadFn("TestSuccessReload")
}

func TestRunBrainApp_PortConflict(t *testing.T) {
	tmpDB := filepath.Join(t.TempDir(), "brain_port_test.db")
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on test port: %v", err)
	}
	defer func() { _ = ln.Close() }()

	_, portStr, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatalf("failed to split host port: %v", err)
	}

	bCfg := BrainConfig{
		Port:         portStr, // Conflicting port
		AgyBin:       "echo",
		Model:        "gemini-2.5-flash",
		DBPath:       tmpDB,
		DiscordToken: "",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = RunBrainApp(ctx, bCfg)
	if err == nil {
		t.Errorf("expected error running on conflicting port, got nil")
	}
}

func TestMetricsMiddleware_EmptyMethodAndNotFound(t *testing.T) {
	mux := http.NewServeMux()
	handler := metricsMiddleware(mux)

	req := httptest.NewRequest("", "/health", nil)
	req.Method = ""
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
}

func TestCreateReloadConfigFunc_EmptyAPIKeyAndNilPool(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)
	_ = os.MkdirAll(filepath.Join(tmpDir, ".gemini", "config", "plugins", "superpowers", ".git"), 0755)

	reloadFn := CreateReloadConfigFunc(nil, "", "Test prompt", nil)
	reloadFn("TestNilPoolEmptyAPIKey")
}

func TestConcurrentCache_SchedulesAndTasks(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	_ = db.InsertMessage(database, db.Message{
		ID:        "msg-concurrent-1",
		ThreadID:  "th-1",
		Content:   "task content",
		Status:    db.StatusProcessing,
		CreatedAt: time.Now().UTC(),
	})

	schedulesHandler := handleSchedules(database)
	tasksHandler := handleTasks(database)

	for i := 0; i < 5; i++ {
		go func() {
			req := httptest.NewRequest(http.MethodGet, "/schedules", nil)
			w := httptest.NewRecorder()
			schedulesHandler(w, req)
		}()
		go func() {
			req := httptest.NewRequest(http.MethodGet, "/tasks", nil)
			w := httptest.NewRecorder()
			tasksHandler(w, req)
		}()
	}
}

func TestMainFunction(t *testing.T) {
	orig := runBrainApp
	defer func() { runBrainApp = orig }()

	called := false
	runBrainApp = func(ctx context.Context, bCfg BrainConfig) error {
		called = true
		return nil
	}

	main()
	if !called {
		t.Errorf("expected runBrainApp to be called from main")
	}
}

func TestHandleFacts_AdvancedParams(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	_, _ = db.InsertFact(database, "general", "Test fact statement", 0.5, "test-conv-1", nil)

	handler := handleFacts(database)

	// 1. Method not allowed
	req := httptest.NewRequest(http.MethodPost, "/facts", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", w.Code)
	}

	// 2. Query with limit, offset, category, and long search term
	longQuery := "query-that-exceeds-64-characters-0123456789012345678901234567890123456789"
	req = httptest.NewRequest(http.MethodGet, fmt.Sprintf("/facts?limit=10&offset=0&category=general&q=%s", longQuery), nil)
	w = httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHandleTranscripts_ComprehensiveParsing(t *testing.T) {
	tmpHome := t.TempDir()
	_ = os.Setenv("HOME", tmpHome)

	transcriptDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "brain", "conv-test-1", ".system_generated", "logs")
	_ = os.MkdirAll(transcriptDir, 0755)

	transcriptContent := `{"status": "PROCESSING", "error": null}
{"status": "ERROR", "error": "test fatal error occurred"}
`
	_ = os.WriteFile(filepath.Join(transcriptDir, "transcript_full.jsonl"), []byte(transcriptContent), 0644)

	database, _ := db.InitDB(":memory:")
	defer func() { _ = database.Close() }()

	handler := handleTranscripts(database)

	// Request with include_raw=true
	req := httptest.NewRequest(http.MethodGet, "/transcripts?include_raw=true", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

type errTestReader struct{}

func (errTestReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("simulated body read failure")
}

func (errTestReader) Close() error {
	return nil
}

func TestHandlePrompt_ErrReaderAndPoolNil(t *testing.T) {
	database, _ := db.InitDB(":memory:")
	defer func() { _ = database.Close() }()

	handler := handlePrompt(database, nil)

	// 1. Error reading request body
	req := httptest.NewRequest(http.MethodPost, "/prompt", errTestReader{})
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 on errReader, got %d", w.Code)
	}

	// 2. Valid payload with pool == nil
	payload, _ := json.Marshal(map[string]string{"prompt": "Hello with nil pool"})
	req = httptest.NewRequest(http.MethodPost, "/prompt", bytes.NewReader(payload))
	w = httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202 on valid payload, got %d", w.Code)
	}
}

func TestHandleTranscripts_CompactLogs(t *testing.T) {
	tmpHome := t.TempDir()
	_ = os.Setenv("HOME", tmpHome)

	transcriptDir := filepath.Join(tmpHome, ".gemini", "antigravity-cli", "brain", "conv-compact-1", ".system_generated", "logs")
	_ = os.MkdirAll(transcriptDir, 0755)

	transcriptContent := `{"status": "DONE", "error": null}`
	_ = os.WriteFile(filepath.Join(transcriptDir, "transcript.jsonl"), []byte(transcriptContent), 0644)

	database, _ := db.InitDB(":memory:")
	defer func() { _ = database.Close() }()

	handler := handleTranscripts(database)

	req := httptest.NewRequest(http.MethodGet, "/transcripts", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}












package main

import (
	"bufio"
	"bytes"
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
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/classifier"
	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/env"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/scheduler"
	"github.com/azylman/aerial/brain/pkg/transcript"
	"github.com/bwmarrin/discordgo"
	_ "modernc.org/sqlite"
)

func TestHandlePromptValidation(t *testing.T) {
	store := db.NewFakeStore()

	pool := newTestWorkerPool(store)
	pool.Start()
	defer pool.Stop()

	handler := handlePrompt(store, pool)

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

	// Test missing channel_id payload rejected
	missingChPayload, _ := json.Marshal(map[string]string{
		"prompt": "Test prompt",
	})
	req = httptest.NewRequest(http.MethodPost, "/prompt", bytes.NewReader(missingChPayload))
	w = httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 BadRequest for missing channel_id, got %d", w.Code)
	}

	// Test invalid non-numeric snowflake channel_id rejected
	invalidChPayload, _ := json.Marshal(map[string]string{
		"prompt":     "Test prompt",
		"channel_id": "not-a-snowflake",
	})
	req = httptest.NewRequest(http.MethodPost, "/prompt", bytes.NewReader(invalidChPayload))
	w = httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400 BadRequest for non-snowflake channel_id, got %d", w.Code)
	}

	// Test valid prompt payload accepted
	validPayload, _ := json.Marshal(map[string]string{
		"prompt":     "Test valid prompt",
		"channel_id": "1542423172400291873",
		"message_id": "msg-123",
	})
	req = httptest.NewRequest(http.MethodPost, "/prompt", bytes.NewReader(validPayload))
	w = httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusAccepted {
		t.Errorf("Expected status 202 StatusAccepted, got %d", w.Code)
	}

	var respBody map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &respBody); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}
	if respBody["channel_id"] != "1542423172400291873" {
		t.Errorf("Expected channel_id in response, got %+v", respBody)
	}

	// Verify message persisted to DB
	msg, err := store.GetMessage(context.Background(), "msg-123")
	if err != nil || msg == nil {
		t.Fatalf("Failed to retrieve persisted message: %v", err)
	}
	if msg.ThreadID != "1542423172400291873" || msg.Content != "Test valid prompt" {
		t.Errorf("Unexpected message fields in DB: %+v", msg)
	}
}

func TestHandleDirectMessage(t *testing.T) {
	t.Run("method not allowed", func(t *testing.T) {
		handler := handleDirectMessage(&queue.WorkerPool{})
		req := httptest.NewRequest(http.MethodGet, "/discord/message", nil)
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("expected 405 MethodNotAllowed, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "Method not allowed") {
			t.Fatalf("expected 'Method not allowed' in body, got %q", w.Body.String())
		}
	})

	t.Run("nil pool returns 500", func(t *testing.T) {
		handler := handleDirectMessage(nil)
		payload := `{"channel_id":"1542423172400291873","content":"hello"}`
		req := httptest.NewRequest(http.MethodPost, "/discord/message", strings.NewReader(payload))
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusInternalServerError {
			t.Fatalf("expected 500 InternalServerError, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "Worker pool not available") {
			t.Fatalf("expected 'Worker pool not available' in body, got %q", w.Body.String())
		}
	})

	t.Run("invalid JSON returns 400", func(t *testing.T) {
		pool := &queue.WorkerPool{}
		handler := handleDirectMessage(pool)
		req := httptest.NewRequest(http.MethodPost, "/discord/message", strings.NewReader("{invalid-json"))
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 BadRequest, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "Invalid JSON payload") {
			t.Fatalf("expected 'Invalid JSON payload' in body, got %q", w.Body.String())
		}
	})

	t.Run("missing channel_id returns 400", func(t *testing.T) {
		pool := &queue.WorkerPool{}
		handler := handleDirectMessage(pool)
		req := httptest.NewRequest(http.MethodPost, "/discord/message", strings.NewReader(`{"content":"hello"}`))
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 BadRequest, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "Invalid payload: 'channel_id' field is required and cannot be empty") {
			t.Fatalf("expected channel_id required error, got %q", w.Body.String())
		}
	})

	t.Run("non-snowflake channel_id returns 400", func(t *testing.T) {
		pool := &queue.WorkerPool{}
		handler := handleDirectMessage(pool)
		req := httptest.NewRequest(http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"not-a-snowflake","content":"hello"}`))
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 BadRequest, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "Invalid payload: 'channel_id' must be a valid numeric Discord snowflake") {
			t.Fatalf("expected snowflake error, got %q", w.Body.String())
		}
	})

	t.Run("empty content returns 400", func(t *testing.T) {
		pool := &queue.WorkerPool{}
		handler := handleDirectMessage(pool)
		req := httptest.NewRequest(http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"  "}`))
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 BadRequest, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "Invalid payload: 'content' field is required and cannot be empty") {
			t.Fatalf("expected content required error, got %q", w.Body.String())
		}
	})

	t.Run("discord session not connected returns 503", func(t *testing.T) {
		pool := &queue.WorkerPool{}
		handler := handleDirectMessage(pool)
		req := httptest.NewRequest(http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`))
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 ServiceUnavailable, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "Discord session not connected") {
			t.Fatalf("expected 'Discord session not connected' in body, got %q", w.Body.String())
		}
	})

	t.Run("discord client error returns 403 for RESTError", func(t *testing.T) {
		pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
			DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
				return &discordgo.RESTError{
					Response: &http.Response{StatusCode: 403},
				}
			},
		})
		handler := handleDirectMessage(pool)
		req := httptest.NewRequest(http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`))
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusForbidden {
			t.Fatalf("expected 403 Forbidden, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "Discord client error") {
			t.Fatalf("expected 'Discord client error' in body, got %q", w.Body.String())
		}
	})

	t.Run("discord client error returns 400 for cannot send", func(t *testing.T) {
		pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
			DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
				return errors.New("cannot send messages to this thread")
			},
		})
		handler := handleDirectMessage(pool)
		req := httptest.NewRequest(http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`))
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("expected 400 BadRequest, got %d", w.Code)
		}
		if !strings.Contains(w.Body.String(), "Discord client error") {
			t.Fatalf("expected 'Discord client error' in body, got %q", w.Body.String())
		}
	})

	t.Run("success returns 200 with channel_id and delivers message", func(t *testing.T) {
		var capturedChannelID, capturedText string
		pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
			DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
				capturedChannelID = channelID
				capturedText = text
				return nil
			},
		})
		handler := handleDirectMessage(pool)

		// Test with channel_id and content
		req := httptest.NewRequest(http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"Deploy succeeded!"}`))
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d (body: %s)", w.Code, w.Body.String())
		}
		if capturedChannelID != "1542423172400291873" || capturedText != "Deploy succeeded!" {
			t.Fatalf("unexpected delivered values: channel=%q, text=%q", capturedChannelID, capturedText)
		}

		var resp map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response JSON: %v", err)
		}
		if resp["status"] != "sent" || resp["channel_id"] != "1542423172400291873" {
			t.Fatalf("unexpected response body: %+v", resp)
		}

		// Test fallback with thread_id and text
		req2 := httptest.NewRequest(http.MethodPost, "/discord/message", strings.NewReader(`{"thread_id":"1542423172400291874","text":"Deploy failed!"}`))
		w2 := httptest.NewRecorder()
		handler(w2, req2)

		if w2.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d (body: %s)", w2.Code, w2.Body.String())
		}
		if capturedChannelID != "1542423172400291874" || capturedText != "Deploy failed!" {
			t.Fatalf("unexpected delivered values: channel=%q, text=%q", capturedChannelID, capturedText)
		}
	})

	t.Run("mux route mounting for canonical /discord/message", func(t *testing.T) {
		pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
			DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
				return nil
			},
		})
		mux := SetupBrainMuxWithEmbedder(db.NewFakeStore(), pool, nil)

		// /discord/message should return 200 OK
		req := httptest.NewRequest(http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"test"}`))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200 for /discord/message, got %d (body: %s)", w.Code, w.Body.String())
		}

		// /internal/message should return 404 Not Found (dropped)
		reqInternal := httptest.NewRequest(http.MethodPost, "/internal/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"test"}`))
		wInternal := httptest.NewRecorder()
		mux.ServeHTTP(wInternal, reqInternal)

		if wInternal.Code != http.StatusNotFound {
			t.Errorf("expected 404 for dropped /internal/message, got %d", wInternal.Code)
		}
	})
}

func TestHandleTranscripts(t *testing.T) {
	store := db.NewFakeStore()

	tmpHome := t.TempDir()
	handler := handleTranscripts(store, tmpHome)

	req := httptest.NewRequest(http.MethodGet, "/transcripts", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 OK, got %d", w.Code)
	}
	if strings.TrimSpace(w.Body.String()) != "[]" {
		t.Errorf("Expected empty JSON array '[]', got %q", w.Body.String())
	}
}

func TestHandleTranscriptSearchAndStats(t *testing.T) {
	t.Run("Search_MethodNotAllowed", func(t *testing.T) {
		handler := handleTranscriptSearch(db.NewFakeStore(), nil)
		req := httptest.NewRequest(http.MethodPost, "/api/transcripts/search", nil)
		w := httptest.NewRecorder()
		handler(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})

	t.Run("Search_NilStore", func(t *testing.T) {
		handler := handleTranscriptSearch(nil, nil)
		req := httptest.NewRequest(http.MethodGet, "/api/transcripts/search?q=test", nil)
		w := httptest.NewRecorder()
		handler(w, req)
		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected 500, got %d", w.Code)
		}
	})

	t.Run("Search_Success", func(t *testing.T) {
		fake := db.NewFakeStore()
		_ = fake.UpsertSessionSummary(context.Background(), db.SessionSummary{
			SessionID: "sess-mux-1",
			Summary:   "Triaged container restart in aerial",
		})
		embedderCalled := false
		embedder := func(ctx context.Context, text string) ([]float32, error) {
			embedderCalled = true
			return make([]float32, 384), nil
		}
		handler := handleTranscriptSearch(fake, embedder)

		req := httptest.NewRequest(http.MethodGet, "/api/transcripts/search?q=container&mode=auto&limit=5&tool=run_command&session=sess-mux-1", nil)
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
		}
		if !embedderCalled {
			t.Error("expected embedder to be invoked")
		}
		var res transcript.SearchResult
		if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil {
			t.Fatalf("failed unmarshaling search response: %v", err)
		}
		if len(res.Sessions) != 1 || res.Sessions[0].SessionID != "sess-mux-1" {
			t.Errorf("unexpected search result: %+v", res)
		}
	})

	t.Run("Stats_MethodNotAllowed", func(t *testing.T) {
		handler := handleTranscriptStats(db.NewFakeStore())
		req := httptest.NewRequest(http.MethodPost, "/api/transcripts/stats", nil)
		w := httptest.NewRecorder()
		handler(w, req)
		if w.Code != http.StatusMethodNotAllowed {
			t.Errorf("expected 405, got %d", w.Code)
		}
	})

	t.Run("Stats_NilStore", func(t *testing.T) {
		handler := handleTranscriptStats(nil)
		req := httptest.NewRequest(http.MethodGet, "/api/transcripts/stats", nil)
		w := httptest.NewRecorder()
		handler(w, req)
		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected 500, got %d", w.Code)
		}
	})

	t.Run("Stats_Success", func(t *testing.T) {
		fake := db.NewFakeStore()
		_ = fake.UpsertSessionSummary(context.Background(), db.SessionSummary{
			SessionID: "sess-stat-1",
			Summary:   "Session 1",
		})
		handler := handleTranscriptStats(fake)

		req := httptest.NewRequest(http.MethodGet, "/api/transcripts/stats", nil)
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
		}
		var stats map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &stats); err != nil {
			t.Fatalf("failed unmarshaling stats response: %v", err)
		}
		if stats["total_sessions"] != float64(1) || stats["status"] != "ok" {
			t.Errorf("unexpected stats: %+v", stats)
		}
	})

	t.Run("Search_QueryParamAlias", func(t *testing.T) {
		fake := db.NewFakeStore()
		handler := handleTranscriptSearch(fake, nil)
		req := httptest.NewRequest(http.MethodGet, "/api/transcripts/search?query=fallback-query&limit=abc", nil)
		w := httptest.NewRecorder()
		handler(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200 OK, got %d", w.Code)
		}
	})

	t.Run("Search_Error", func(t *testing.T) {
		errStore := &errTranscriptStore{FakeStore: db.NewFakeStore()}
		handler := handleTranscriptSearch(errStore, nil)
		req := httptest.NewRequest(http.MethodGet, "/api/transcripts/search?q=test&mode=semantic", nil)
		w := httptest.NewRecorder()
		handler(w, req)
		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 on search store error, got %d", w.Code)
		}
	})

	t.Run("Stats_Error", func(t *testing.T) {
		errStore := &errTranscriptStore{FakeStore: db.NewFakeStore()}
		handler := handleTranscriptStats(errStore)
		req := httptest.NewRequest(http.MethodGet, "/api/transcripts/stats", nil)
		w := httptest.NewRecorder()
		handler(w, req)
		if w.Code != http.StatusInternalServerError {
			t.Errorf("expected 500 on store error, got %d", w.Code)
		}
	})

	t.Run("SetupBrainMuxWithEmbedder_Endpoints", func(t *testing.T) {
		fake := db.NewFakeStore()
		mux := SetupBrainMuxWithEmbedder(fake, nil, nil)

		// Test /api/transcripts/search route
		req := httptest.NewRequest(http.MethodGet, "/api/transcripts/search?q=test", nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200 from /api/transcripts/search route, got %d", w.Code)
		}

		// Test /api/transcripts/stats route
		req = httptest.NewRequest(http.MethodGet, "/api/transcripts/stats", nil)
		w = httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200 from /api/transcripts/stats route, got %d", w.Code)
		}
	})
}

type errTranscriptStore struct {
	*db.FakeStore
}

func (e *errTranscriptStore) GetSessionSyncStates(ctx context.Context) (map[string]db.SessionSyncState, error) {
	return nil, errors.New("simulated sync states db error")
}

func (e *errTranscriptStore) SearchTranscriptSteps(ctx context.Context, query, sessionFilter, toolFilter string, limit int) ([]db.TranscriptStep, error) {
	return nil, errors.New("simulated search transcript steps error")
}

func (e *errTranscriptStore) SearchSessionSummaries(ctx context.Context, queryEmbedding []float32, textQuery string, limit int, threshold float64) ([]db.SessionSummary, error) {
	return nil, errors.New("simulated search session summaries error")
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
	store := db.NewFakeStore()

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
	if err := store.CreateCronSchedule(context.Background(), cron1); err != nil {
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
	if err := store.CreateOneShotSchedule(context.Background(), once1); err != nil {
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
	if err := store.CreateScheduleRun(context.Background(), runCompleted); err != nil {
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
	if err := store.CreateScheduleRun(context.Background(), runFailed); err != nil {
		t.Fatalf("CreateScheduleRun failed: %v", err)
	}

	// Test GET /schedules
	schedulesHandler := handleSchedules(store)
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
	_ = store.CreateCronSchedule(context.Background(), cron2)

	wCached := httptest.NewRecorder()
	schedulesHandler(wCached, req)
	var cachedResp SchedulesResponse
	_ = json.Unmarshal(wCached.Body.Bytes(), &cachedResp)
	if len(cachedResp.Crons) != 1 {
		t.Errorf("Expected cached 1 cron schedule within 5s TTL, got %d", len(cachedResp.Crons))
	}

	// Test GET /schedules/runs
	runsHandler := handleScheduleRuns(store)
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
	store := db.NewFakeStore()

	now := time.Now().UTC()
	longPrompt := strings.Repeat("A", 600) + " ghp_123456789012345678901234567890123456"

	_ = store.InsertMessage(context.Background(), db.Message{
		ID:         "msg-test-task",
		ThreadID:   "thread-1",
		AuthorID:   "user-1",
		AuthorName: "Tester with token ghp_999999999999999999999999999999999999",
		Content:    "Secret token ghp_123456789012345678901234567890123456 in prompt " + longPrompt,
		Status:     db.StatusProcessing,
		CreatedAt:  now,
		UpdatedAt:  now,
	})

	handler := handleTasks(store)

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
	_ = store.InsertMessage(context.Background(), db.Message{
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
	metrics.RecordTurnCompleted("success", "discord", "gemini-3.8-flash-low", 2*time.Second)
	metrics.RecordClassifierRun("success", "gemini-3.8-flash-low", 500*time.Millisecond, 0.95, "wake")
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
	store := db.NewFakeStore()

	// Seed some facts
	_, _ = store.InsertFact(context.Background(), "system", "Fact 1", 1.0, nil)
	_, _ = store.InsertFact(context.Background(), "user_preference", "Fact 2", 1.0, nil)

	handler := handleFacts(store)

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
	closedStore := db.NewFakeStore()
	_ = closedStore.Close()
	handlerErr := handleFacts(closedStore)
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
		got := Ordinal(tc.n)
		if got != tc.want {
			t.Errorf("Ordinal(%d) = %q, want %q", tc.n, got, tc.want)
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
	closedStore := db.NewFakeStore()
	_ = closedStore.Close()

	// 1. handleSchedules with closed DB
	schedHandler := handleSchedules(closedStore)
	req := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	w := httptest.NewRecorder()
	schedHandler(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 on closed DB from handleSchedules, got %d", w.Code)
	}

	// 2. handleScheduleRuns with closed DB
	runsHandler := handleScheduleRuns(closedStore)
	reqRuns := httptest.NewRequest(http.MethodGet, "/schedules/runs", nil)
	wRuns := httptest.NewRecorder()
	runsHandler(wRuns, reqRuns)
	if wRuns.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 on closed DB from handleScheduleRuns, got %d", wRuns.Code)
	}

	// 3. handleTasks with closed DB
	tasksHandler := handleTasks(closedStore)
	reqTasks := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	wTasks := httptest.NewRecorder()
	tasksHandler(wTasks, reqTasks)
	if wTasks.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 on closed DB from handleTasks, got %d", wTasks.Code)
	}
}

func TestSetupBrainMux_And_Endpoints(t *testing.T) {
	store := db.NewFakeStore()

	tmpHome := t.TempDir()
	mux := SetupBrainMux(store, nil, tmpHome)

	// Test /health
	reqHealth := httptest.NewRequest(http.MethodGet, "/health", nil)
	wHealth := httptest.NewRecorder()
	mux.ServeHTTP(wHealth, reqHealth)
	if wHealth.Code != http.StatusOK {
		t.Errorf("Expected 200 from /health, got %d", wHealth.Code)
	}

	// Test /transcripts with injected homeDir
	reqTranscripts := httptest.NewRequest(http.MethodGet, "/transcripts", nil)
	wTranscripts := httptest.NewRecorder()
	mux.ServeHTTP(wTranscripts, reqTranscripts)
	if wTranscripts.Code != http.StatusOK {
		t.Errorf("Expected 200 from /transcripts, got %d", wTranscripts.Code)
	}
}

func TestInitializeBrainEnvironment_And_Config(t *testing.T) {
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.Model = "gemini-2.5-flash"
		d.GeminiHomeDir = tmpHome
		d.DataDir = tmpData
	})
	if cfg.Current().Model != "gemini-2.5-flash" {
		t.Errorf("Unexpected model in config: %s", cfg.Current().Model)
	}

	ctx := context.Background()
	_ = InitializeBrainEnvironment(ctx, cfg)

	reloadFn := CreateReloadConfigFunc(cfg, WithSkipEnvironmentSync())
	reloadFn("UnitTest")
}

func TestRunBrainApp_Lifecycle(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.Port = "0"
		d.AgyBin = "/bin/true"
		d.Model = "gemini-2.5-flash"
		d.SystemPrompt = "test"
		d.GeminiHomeDir = tmpDir
		d.DataDir = filepath.Join(tmpDir, "data")
	})

	var readyOnce sync.Once
	ready := make(chan struct{})
	oldReady := onServerReady
	onServerReady = func(addr string) {
		readyOnce.Do(func() {
			close(ready)
		})
	}
	defer func() { onServerReady = oldReady }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-ready
		cancel()
	}()

	mockStore := db.NewFakeStore()
	err := RunBrainApp(ctx, cfg, WithStore(mockStore))
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("Unexpected error running Brain app: %v", err)
	}
	onServerReady = oldReady

	// Test invalid DB path fails cleanly
	badCfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DatabaseURL = "/nonexistent/invalid/dir/db.sqlite"
		d.GeminiHomeDir = tmpDir
		d.DataDir = filepath.Join(tmpDir, "data")
	})
	badCtx, badCancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer badCancel()
	_ = RunBrainApp(badCtx, badCfg)
}

type errReader struct{}

func (errReader) Read(p []byte) (n int, err error) {
	return 0, errors.New("simulated network read error")
}

func TestHandlePrompt_ErrorBranches(t *testing.T) {
	store := db.NewFakeStore()

	pool := newTestWorkerPool(store)
	// pool is not started so it won't trigger real background worker executions

	handler := handlePrompt(store, pool)

	// 1. Read body error
	reqErr := httptest.NewRequest(http.MethodPost, "/prompt", errReader{})
	wErr := httptest.NewRecorder()
	handler(wErr, reqErr)
	if wErr.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for read error, got %d", wErr.Code)
	}

	// 2. Whitespace only prompt
	reqWs := httptest.NewRequest(http.MethodPost, "/prompt", strings.NewReader(`{"prompt":"   "}`))
	wWs := httptest.NewRecorder()
	handler(wWs, reqWs)
	if wWs.Code != http.StatusBadRequest {
		t.Errorf("Expected 400 for whitespace prompt, got %d", wWs.Code)
	}

	// 3. Auto-generated UUIDs when omitted
	reqAuto := httptest.NewRequest(http.MethodPost, "/prompt", strings.NewReader(`{"prompt":"hello world","channel_id":"1542423172400291873"}`))
	wAuto := httptest.NewRecorder()
	handler(wAuto, reqAuto)
	if wAuto.Code != http.StatusAccepted {
		t.Errorf("Expected 202 for auto prompt, got %d", wAuto.Code)
	}

	// 4. Closed DB returns 500
	closedStore := db.NewFakeStore()
	_ = closedStore.Close()
	handlerClosed := handlePrompt(closedStore, pool)
	reqClosed := httptest.NewRequest(http.MethodPost, "/prompt", strings.NewReader(`{"prompt":"hello fallback","channel_id":"1542423172400291873"}`))
	wClosed := httptest.NewRecorder()
	handlerClosed(wClosed, reqClosed)
	if wClosed.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 for closed DB insert failure, got %d", wClosed.Code)
	}

	// 5. Nil store returns 500
	handlerNilStore := handlePrompt(nil, pool)
	reqNilStore := httptest.NewRequest(http.MethodPost, "/prompt", strings.NewReader(`{"prompt":"hello nil store","channel_id":"1542423172400291873"}`))
	wNilStore := httptest.NewRecorder()
	handlerNilStore(wNilStore, reqNilStore)
	if wNilStore.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 for nil store, got %d", wNilStore.Code)
	}

	// 6. Nil pool handling (store succeeds, pool nil returns 202)
	handlerNilPool := handlePrompt(store, nil)
	reqNilPool := httptest.NewRequest(http.MethodPost, "/prompt", strings.NewReader(`{"prompt":"hello nil pool","channel_id":"1542423172400291873"}`))
	wNilPool := httptest.NewRecorder()
	handlerNilPool(wNilPool, reqNilPool)
	if wNilPool.Code != http.StatusAccepted {
		t.Errorf("Expected 202 for nil pool when store succeeds, got %d", wNilPool.Code)
	}
}

func TestHandleTranscripts_ErrorAndRawBranches(t *testing.T) {
	tmpHome := t.TempDir()

	store := db.NewFakeStore()

	// Seed an external conversation ID mapping
	_ = store.SaveConversationMapping(context.Background(), "thread-discord-100", "conv-alpha-100")

	// Create transcript folders
	brainDir := filepath.Join(tmpHome, "brain")
	convDir := filepath.Join(brainDir, "conv-alpha-100", ".system_generated", "logs")
	if err := os.MkdirAll(convDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}

	transcriptContent := `{"step_index": 1, "status": "DONE", "content": "hello"}
{"step_index": 2, "status": "ERROR", "error": "something failed"}
{"step_index": 3, "status": "RUNNING", "error": {"code": 500, "message": "object error"}}
not a valid json line
`
	if err := os.WriteFile(filepath.Join(convDir, "transcript.jsonl"), []byte(transcriptContent), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	handler := handleTranscripts(store, brainDir)

	// GET transcripts with include_raw=true
	reqGet := httptest.NewRequest(http.MethodGet, "/transcripts?include_raw=true", nil)
	wGet := httptest.NewRecorder()
	handler(wGet, reqGet)
	if wGet.Code != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", wGet.Code)
	}
	if !strings.Contains(wGet.Body.String(), "conv-alpha-100") {
		t.Errorf("Expected conv-alpha-100 in response: %s", wGet.Body.String())
	}
}

func TestHandleFacts_TruncationAndErrors(t *testing.T) {
	store := db.NewFakeStore()

	handler := handleFacts(store)

	// 1. Method Not Allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/facts", nil)
	wPost := httptest.NewRecorder()
	handler(wPost, reqPost)
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 MethodNotAllowed, got %d", wPost.Code)
	}

	// 2. Long query truncation (>64 runes)
	longQuery := strings.Repeat("a", 100)
	reqLong := httptest.NewRequest(http.MethodGet, "/facts?q="+longQuery, nil)
	wLong := httptest.NewRecorder()
	handler(wLong, reqLong)
	if wLong.Code != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", wLong.Code)
	}

	// 3. Closed DB error -> 500
	closedStore := db.NewFakeStore()
	_ = closedStore.Close()
	handlerClosed := handleFacts(closedStore)
	reqClosed := httptest.NewRequest(http.MethodGet, "/facts?q=test", nil)
	wClosed := httptest.NewRecorder()
	handlerClosed(wClosed, reqClosed)
	if wClosed.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 InternalServerError for closed DB, got %d", wClosed.Code)
	}
}

func TestHandleSchedules_And_Runs_GranularErrors(t *testing.T) {
	store := db.NewFakeStore()

	handlerSched := handleSchedules(store)

	// 1. handleSchedules: Method Not Allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/schedules", nil)
	wPost := httptest.NewRecorder()
	handlerSched(wPost, reqPost)
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 MethodNotAllowed, got %d", wPost.Code)
	}

	// 2. handleSchedules: Closed DB error on summary metrics -> 500
	closedStore := db.NewFakeStore()
	_ = closedStore.Close()
	handlerSchedClosed := handleSchedules(closedStore)
	reqClosed := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	wClosed := httptest.NewRecorder()
	handlerSchedClosed(wClosed, reqClosed)
	if wClosed.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 for closed DB, got %d", wClosed.Code)
	}

	// 3. handleScheduleRuns: Method Not Allowed
	handlerRuns := handleScheduleRuns(store)
	reqRunsPost := httptest.NewRequest(http.MethodPost, "/schedules/runs", nil)
	wRunsPost := httptest.NewRecorder()
	handlerRuns(wRunsPost, reqRunsPost)
	if wRunsPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 for runs POST, got %d", wRunsPost.Code)
	}

	// 4. handleScheduleRuns: query parameters & filters
	reqRunsQuery := httptest.NewRequest(http.MethodGet, "/schedules/runs?limit=100&offset=5&schedule_id=sched-1&status=completed", nil)
	wRunsQuery := httptest.NewRecorder()
	handlerRuns(wRunsQuery, reqRunsQuery)
	if wRunsQuery.Code != http.StatusOK {
		t.Errorf("Expected 200 for runs query, got %d", wRunsQuery.Code)
	}

	// 5. handleScheduleRuns: closed DB error -> 500
	handlerRunsClosed := handleScheduleRuns(closedStore)
	reqRunsClosed := httptest.NewRequest(http.MethodGet, "/schedules/runs", nil)
	wRunsClosed := httptest.NewRecorder()
	handlerRunsClosed(wRunsClosed, reqRunsClosed)
	if wRunsClosed.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 for closed DB on runs, got %d", wRunsClosed.Code)
	}
}

func TestHandleTasks_GranularErrors(t *testing.T) {
	store := db.NewFakeStore()

	handler := handleTasks(store)

	// 1. Method Not Allowed
	reqPost := httptest.NewRequest(http.MethodPost, "/tasks", nil)
	wPost := httptest.NewRecorder()
	handler(wPost, reqPost)
	if wPost.Code != http.StatusMethodNotAllowed {
		t.Errorf("Expected 405 MethodNotAllowed, got %d", wPost.Code)
	}

	// 2. Closed DB error -> 500
	closedStore := db.NewFakeStore()
	_ = closedStore.Close()
	handlerClosed := handleTasks(closedStore)
	reqClosed := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	wClosed := httptest.NewRecorder()
	handlerClosed(wClosed, reqClosed)
	if wClosed.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 for closed DB on tasks, got %d", wClosed.Code)
	}
}

func TestInitializeBrainEnvironment_DataSymlink(t *testing.T) {
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.GeminiHomeDir = tmpHome
		d.DataDir = tmpData
	})

	_ = InitializeBrainEnvironment(context.Background(), cfg)
}

func TestCreateReloadConfigFunc_InvalidYAMLAndAlert(t *testing.T) {
	cfg := config.NewTestConfig()
	s, _ := discordgo.New("Bot mock-token")

	reloadFn := CreateReloadConfigFunc(cfg,
		WithDiscordSession(s),
		WithReloadSupplier(func(active *config.Config) error {
			return errors.New("simulated invalid YAML error")
		}),
		WithSkipEnvironmentSync(),
	)
	reloadFn("TestReload")
}

func TestRunBrainApp_ServerReadinessAndShutdown(t *testing.T) {
	tmpDir := t.TempDir()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.Port = "0"
		d.AgyBin = "/bin/true"
		d.Model = "gemini-2.5-flash"
		d.SystemPrompt = "test"
		d.GeminiHomeDir = tmpDir
		d.DataDir = filepath.Join(tmpDir, "data")
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ready := make(chan struct{})
	oldReady := onServerReady
	onServerReady = func(addr string) {
		close(ready)
	}
	defer func() { onServerReady = oldReady }()

	errChan := make(chan error, 1)
	mockStore := db.NewFakeStore()
	go func() {
		errChan <- RunBrainApp(ctx, cfg, WithStore(mockStore))
	}()

	<-ready
	cancel()

	select {
	case err := <-errChan:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("RunBrainApp returned unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Errorf("RunBrainApp did not shut down within 5 seconds")
	}
}

func TestHandleSchedules_DetailedCoverage(t *testing.T) {
	store := db.NewFakeStore()

	// Seed cron and one shot schedules
	_ = store.CreateCronSchedule(context.Background(), db.CronSchedule{
		ID:          "cron-1",
		TargetID:    "target-1",
		TitlePrefix: "Daily Backup",
		CronExpr:    "0 0 * * *",
		Prompt:      "Run backup",
		Timezone:    "America/Los_Angeles",
		Enabled:     true,
		CreatedAt:   time.Now().UTC(),
	})
	_ = store.CreateOneShotSchedule(context.Background(), db.OneShotSchedule{
		ID:        "oneshot-1",
		ThreadID:  "target-1",
		Prompt:    "Run once",
		RunAt:     time.Now().UTC().Add(1 * time.Hour),
		CreatedAt: time.Now().UTC(),
	})

	handler := handleSchedules(store)

	// 1. Initial request (cache miss)
	req1 := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	w1 := httptest.NewRecorder()
	handler(w1, req1)
	if w1.Code != http.StatusOK {
		t.Errorf("Expected 200 on initial fetch, got %d", w1.Code)
	}
	if !strings.Contains(w1.Body.String(), "Daily Backup") {
		t.Errorf("Expected Daily Backup in response: %s", w1.Body.String())
	}

	// 2. Immediate request (cache hit under RLock)
	req2 := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	w2 := httptest.NewRecorder()
	handler(w2, req2)
	if w2.Code != http.StatusOK {
		t.Errorf("Expected 200 on cached fetch, got %d", w2.Code)
	}

	// 3. FailNext GetAllCronSchedules to trigger GetAllCronSchedules error
	storeCronErr := db.NewFakeStore()
	storeCronErr.FailNext("GetAllCronSchedules", errors.New("cron query forced error"))
	handlerCronErr := handleSchedules(storeCronErr)
	reqCronErr := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	wCronErr := httptest.NewRecorder()
	handlerCronErr(wCronErr, reqCronErr)
	if wCronErr.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 for cron query error, got %d", wCronErr.Code)
	}

	// 4. FailNext GetAllOneShotSchedules to trigger GetAllOneShotSchedules error
	storeOneShotErr := db.NewFakeStore()
	storeOneShotErr.FailNext("GetAllOneShotSchedules", errors.New("oneshot query forced error"))
	handlerOneShotErr := handleSchedules(storeOneShotErr)
	reqOneShotErr := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	wOneShotErr := httptest.NewRecorder()
	handlerOneShotErr(wOneShotErr, reqOneShotErr)
	if wOneShotErr.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 for one shot query error, got %d", wOneShotErr.Code)
	}
}

func TestHandleTasks_DetailedCoverage(t *testing.T) {
	store := db.NewFakeStore()

	longPrompt := strings.Repeat("a", 600)
	_ = store.InsertMessage(context.Background(), db.Message{
		ID:         "task-msg-1",
		ThreadID:   "thread-1",
		AuthorName: "Alice",
		Content:    longPrompt,
		Status:     db.StatusProcessing,
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	})

	handler := handleTasks(store)

	// 1. Initial request (cache miss)
	req1 := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	w1 := httptest.NewRecorder()
	handler(w1, req1)
	if w1.Code != http.StatusOK {
		t.Errorf("Expected 200 on initial tasks fetch, got %d", w1.Code)
	}
	if !strings.Contains(w1.Body.String(), "...") {
		t.Errorf("Expected prompt truncation in response: %s", w1.Body.String())
	}

	// 2. Immediate request (cache hit under RLock)
	req2 := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	w2 := httptest.NewRecorder()
	handler(w2, req2)
	if w2.Code != http.StatusOK {
		t.Errorf("Expected 200 on cached tasks fetch, got %d", w2.Code)
	}
}

func TestCreateReloadConfigFunc_SuccessCoverage(t *testing.T) {
	cfg := config.NewTestConfig()
	s, _ := discordgo.New("Bot mock-token")

	// 1. Failure supplier
	reloadFail := CreateReloadConfigFunc(cfg,
		WithDiscordSession(s),
		WithReloadSupplier(func(active *config.Config) error {
			return errors.New("simulated reload failure")
		}),
		WithSkipEnvironmentSync(),
	)
	reloadFail("TestReloadFailure")

	// 2. Success supplier
	reloadSuccess := CreateReloadConfigFunc(cfg,
		WithDiscordSession(s),
		WithReloadSupplier(func(active *config.Config) error {
			active.Update(&config.ConfigData{Model: "gemini-2.5-flash"})
			return nil
		}),
		WithSkipEnvironmentSync(),
	)
	reloadSuccess("TestReloadSuccess")
}

func TestRunBrainApp_DetailedOptions(t *testing.T) {
	t.Run("with discord token", func(t *testing.T) {
		tmpDir := t.TempDir()
		cfg := config.NewTestConfig(func(d *config.ConfigData) {
			d.Port = "0"
			d.AgyBin = "/bin/true"
			d.Model = "gemini-2.5-flash"
			d.APIKey = "test-key"
			d.SystemPrompt = "test"
			d.DiscordToken = "mock-token"
			d.GeminiHomeDir = tmpDir
			d.DataDir = filepath.Join(tmpDir, "data")
			d.LowEffortModel = "custom-flash-low"
		})

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ready := make(chan struct{})
		oldReady := onServerReady
		onServerReady = func(addr string) {
			close(ready)
		}
		defer func() { onServerReady = oldReady }()

		errChan := make(chan error, 1)
		mockStore := db.NewFakeStore()
		go func() {
			errChan <- RunBrainApp(ctx, cfg, WithStore(mockStore))
		}()

		<-ready
		cancel()

		select {
		case err := <-errChan:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Errorf("RunBrainApp returned unexpected error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("RunBrainApp did not shut down within 5 seconds")
		}
	})

	t.Run("port listen error", func(t *testing.T) {
		tmpDir := t.TempDir()
		badPortCfg := config.NewTestConfig(func(d *config.ConfigData) {
			d.Port = "-1"
			d.AgyBin = "/bin/true"
			d.Model = "gemini-2.5-flash"
			d.SystemPrompt = "test"
			d.GeminiHomeDir = tmpDir
			d.DataDir = filepath.Join(tmpDir, "data")
		})
		ctxBad, cancelBad := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancelBad()
		mockStore := db.NewFakeStore()
		_ = RunBrainApp(ctxBad, badPortCfg, WithStore(mockStore))
	})
}

func TestInitializeBrainEnvironment_Complete(t *testing.T) {
	// 1. Success with temporary HOME directory
	tmpDir := t.TempDir()
	tmpData := t.TempDir()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.GeminiHomeDir = tmpDir
		d.DataDir = tmpData
		d.APIKey = "test-api-key"
		d.Model = "gemini-2.5-flash"
		d.SystemPrompt = "test prompt"
	})

	_ = InitializeBrainEnvironment(context.Background(), cfg)

	// 2. Call again when directory already exists
	_ = InitializeBrainEnvironment(context.Background(), cfg)

	// 3. Error branches with uncreatable path
	badCfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.GeminiHomeDir = filepath.Join(tmpDir, "nonexistent", "sub", "dir", "invalid")
		d.DataDir = tmpData
		d.APIKey = "test-api-key"
		d.Model = "gemini-2.5-flash"
		d.SystemPrompt = "test prompt"
	})
	_ = InitializeBrainEnvironment(context.Background(), badCfg)
}

func TestCreateReloadConfigFunc_Complete(t *testing.T) {
	tmpDir := t.TempDir()
	tmpData := t.TempDir()

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.GeminiHomeDir = tmpDir
		d.DataDir = tmpData
		d.Model = "gemini-2.5-flash"
		d.APIKey = "test-api-key"
		d.SystemPrompt = "test system prompt"
	})

	reloadFn := CreateReloadConfigFunc(cfg,
		WithReloadSupplier(func(active *config.Config) error {
			return nil
		}),
	)
	reloadFn("TestSource")

	// Trigger error branches with invalid home
	blockerHome := filepath.Join(tmpDir, "blocked_home")
	_ = os.WriteFile(blockerHome, []byte("file"), 0644)
	cfg.Update(&config.ConfigData{
		GeminiHomeDir: filepath.Join(blockerHome, "sub"),
		DataDir:       tmpData,
	})
	reloadFn("TestErrorSource")
}


func TestRunBrainApp_ErrorBranches(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Nil config returns error
	ctx0, cancel0 := context.WithCancel(context.Background())
	defer cancel0()
	if err := RunBrainApp(ctx0, nil); err == nil {
		t.Errorf("Expected error for nil config, got nil")
	}

	// 2. Unsupported database scheme returns error
	cfgInvalidDB := config.NewTestConfig(func(d *config.ConfigData) {
		d.DatabaseURL = "mysql://user:pass@localhost/db"
		d.GeminiHomeDir = tmpDir
		d.DataDir = filepath.Join(tmpDir, "data")
	})
	ctx1, cancel1 := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel1()
	err := RunBrainApp(ctx1, cfgInvalidDB)
	if err == nil {
		t.Errorf("Expected error from invalid DBPath, got nil")
	}

	// 4. Empty DatabaseURL returns error
	cfgEmptyDB := config.NewTestConfig(func(d *config.ConfigData) {
		d.DatabaseURL = ""
		d.GeminiHomeDir = tmpDir
		d.DataDir = filepath.Join(tmpDir, "data")
	})
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	errEmpty := RunBrainApp(ctx2, cfgEmptyDB)
	if errEmpty == nil {
		t.Errorf("Expected error for empty DatabaseURL, got nil")
	}

	// 5. Pre-canceled context returns nil immediately
	ctxCanceled, cancelEarly := context.WithCancel(context.Background())
	cancelEarly()
	if err := RunBrainApp(ctxCanceled, cfgInvalidDB); err != nil {
		t.Errorf("Expected nil error for pre-canceled context, got %v", err)
	}
}

func TestHandleSchedules_ConcurrentDoubleCheck(t *testing.T) {
	store := db.NewFakeStore()

	handler := handleSchedules(store)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/schedules", nil)
			w := httptest.NewRecorder()
			handler(w, req)
		}()
	}
	wg.Wait()
}

func TestHandleTasks_ConcurrentDoubleCheck(t *testing.T) {
	store := db.NewFakeStore()

	handler := handleTasks(store)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/tasks", nil)
			w := httptest.NewRecorder()
			handler(w, req)
		}()
	}
	wg.Wait()
}

func TestHandleTranscripts_NonDirectoryAndBadHome(t *testing.T) {
	store := db.NewFakeStore()

	tmpDir := t.TempDir()

	brainDir := filepath.Join(tmpDir, "brain")
	_ = os.MkdirAll(brainDir, 0755)
	_ = os.WriteFile(filepath.Join(brainDir, "regular_file.txt"), []byte("not a dir"), 0644)

	handler := handleTranscripts(store, brainDir, "", "   ", "/nonexistent/dir")
	req := httptest.NewRequest(http.MethodGet, "/transcripts", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", w.Code)
	}
}

func TestFormatCronDescription_AdditionalPatterns(t *testing.T) {
	cases := []struct {
		expr string
		want string
	}{
		{"0 9 * * Mon,Wed,Fri", "Mon, Wed, Fri at 09:00"},
		{"0 9 * * 1,3,5", "Mon, Wed, Fri at 09:00"},
		{"0/15 * * * *", "0/15 * * * *"},
	}
	for _, tc := range cases {
		got := FormatCronDescription(tc.expr)
		if got != tc.want {
			t.Errorf("FormatCronDescription(%q) = %q, want %q", tc.expr, got, tc.want)
		}
	}
}

func TestMetricsMiddleware_Implicit200(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/implicit", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})
	handler := metricsMiddleware(mux)
	req := httptest.NewRequest(http.MethodGet, "/implicit", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("Expected 200, got %d", rec.Code)
	}
}

func TestHandleTranscripts_DBErrorBranch(t *testing.T) {
	tmpDir := t.TempDir()

	convID := "test-conv-err-1"
	brainDir := filepath.Join(tmpDir, "brain")
	tDir := filepath.Join(brainDir, convID, ".system_generated", "logs")
	_ = os.MkdirAll(tDir, 0755)
	_ = os.WriteFile(filepath.Join(tDir, "transcript.jsonl"), []byte(`{"status":"DONE"}`+"\n"), 0644)

	closedStore := db.NewFakeStore()
	_ = closedStore.Close()

	handler := handleTranscripts(closedStore, brainDir)
	req := httptest.NewRequest(http.MethodGet, "/transcripts", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", w.Code)
	}
}

func TestRunBrainApp_PureConfig(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := config.NewFromData(&config.ConfigData{
		GeminiHomeDir: tmpDir,
		DataDir:       filepath.Join(tmpDir, "data"),
		Port:          "0",
		Model:         "test-model",
		Timezone:      "UTC",
		SystemPrompt:  "test prompt",
		Channels: map[string]config.ChannelPolicy{
			"default": {Mode: "threads"},
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Immediate cancellation to test lifecycle shutdown

	mockStore := db.NewFakeStore()
	err := RunBrainApp(ctx, cfg, WithStore(mockStore))
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("expected clean shutdown, got %v", err)
	}
}

func TestDefaultTranscriptRoots(t *testing.T) {
	// 1. Nil config
	nilRoots := DefaultTranscriptRoots(nil)
	if len(nilRoots) != 1 || nilRoots[0] != "/data/brain" {
		t.Errorf("DefaultTranscriptRoots(nil) = %v, want [/data/brain]", nilRoots)
	}

	// 2. Custom config with specific DataDir and GeminiHomeDir
	tmpDir := t.TempDir()
	dataDir := filepath.Join(tmpDir, "custom_data")
	homeDir := filepath.Join(tmpDir, "custom_home")

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = dataDir
		d.GeminiHomeDir = homeDir
	})

	roots := DefaultTranscriptRoots(cfg)
	expected := []string{
		filepath.Join(dataDir, "brain"),
		filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain"),
		filepath.Join(homeDir, ".gemini", "antigravity", "brain"),
	}

	if len(roots) != len(expected) {
		t.Fatalf("DefaultTranscriptRoots returned %d roots, want %d", len(roots), len(expected))
	}
	for i, want := range expected {
		if roots[i] != want {
			t.Errorf("roots[%d] = %q, want %q", i, roots[i], want)
		}
	}

	// 3. Config with empty/whitespace DataDir defaults to filepath.Join("/data", "brain")
	emptyDataCfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.DataDir = "   "
		d.GeminiHomeDir = homeDir
	})
	emptyRoots := DefaultTranscriptRoots(emptyDataCfg)
	expectedEmpty := []string{
		filepath.Join("/data", "brain"),
		filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain"),
		filepath.Join(homeDir, ".gemini", "antigravity", "brain"),
	}
	if len(emptyRoots) != len(expectedEmpty) || emptyRoots[0] != expectedEmpty[0] {
		t.Errorf("DefaultTranscriptRoots with whitespace DataDir = %v, want %v", emptyRoots, expectedEmpty)
	}
}

func TestHandleTranscripts_Deduplication(t *testing.T) {
	store := db.NewFakeStore()

	tmpDir := t.TempDir()
	root1 := filepath.Join(tmpDir, "root1")
	root2 := filepath.Join(tmpDir, "root2")

	convID := "shared-conv-1"
	dir1 := filepath.Join(root1, convID, ".system_generated", "logs")
	dir2 := filepath.Join(root2, convID, ".system_generated", "logs")
	_ = os.MkdirAll(dir1, 0755)
	_ = os.MkdirAll(dir2, 0755)
	_ = os.WriteFile(filepath.Join(dir1, "transcript.jsonl"), []byte(`{"step_index":1,"status":"DONE"}`+"\n"), 0644)
	_ = os.WriteFile(filepath.Join(dir2, "transcript.jsonl"), []byte(`{"step_index":1,"status":"DONE"}`+"\n"), 0644)

	handler := handleTranscripts(store, root1, root2)
	req := httptest.NewRequest(http.MethodGet, "/transcripts", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected 200 OK, got %d", w.Code)
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &results); err != nil {
		t.Fatalf("Failed to parse json: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected exactly 1 deduplicated result, got %d", len(results))
	} else {
		expectedPath := filepath.Join(dir1, "transcript.jsonl")
		if gotPath, ok := results[0]["path"].(string); !ok || gotPath != expectedPath {
			t.Errorf("Expected first root to win with path %q, got %q", expectedPath, gotPath)
		}
	}
}

func TestDefaultGeminiHomeDir_Extended(t *testing.T) {
	// 1. With HOME set
	t.Setenv("HOME", "/custom/home")
	if got := DefaultGeminiHomeDir(); got != "/custom/home" {
		t.Errorf("expected /custom/home, got %s", got)
	}

	// 2. With HOME empty
	t.Setenv("HOME", "")
	gotEmpty := DefaultGeminiHomeDir()
	if gotEmpty == "" {
		t.Errorf("expected non-empty default gemini home dir")
	}

	// 3. Direct helper tests for defaultGeminiHomeDirWithLookup
	if got := defaultGeminiHomeDirWithLookup("/custom/home", false, nil); got != "/custom/home" {
		t.Errorf("expected /custom/home, got %s", got)
	}
	if got := defaultGeminiHomeDirWithLookup("", true, nil); !strings.Contains(got, "aerial-test-gemini") {
		t.Errorf("expected temp aerial-test-gemini, got %s", got)
	}
	if got := defaultGeminiHomeDirWithLookup("", false, func() (string, error) { return "/home/testuser", nil }); got != "/home/testuser" {
		t.Errorf("expected /home/testuser, got %s", got)
	}
	if got := defaultGeminiHomeDirWithLookup("", false, func() (string, error) { return "  ", nil }); got != "/root" {
		t.Errorf("expected /root for whitespace user home dir, got %s", got)
	}
	if got := defaultGeminiHomeDirWithLookup("", false, func() (string, error) { return "", errors.New("err") }); got != "/root" {
		t.Errorf("expected /root when lookup fails, got %s", got)
	}
	if got := defaultGeminiHomeDirWithLookup("", false, nil); got != "/root" {
		t.Errorf("expected /root when lookup fn is nil, got %s", got)
	}
	if got := defaultDataDir(false); got != "/data" {
		t.Errorf("expected /data from defaultDataDir(false), got %s", got)
	}
}

func TestMetricsMiddleware_EmptyMethod(t *testing.T) {
	handler := metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest("", "/metrics", nil)
	req.Method = ""
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rr.Code)
	}
}

func TestInitializeBrainEnvironment_Errors(t *testing.T) {
	// 1. nil config
	if err := InitializeBrainEnvironment(context.Background(), nil); err != nil {
		t.Errorf("expected nil error for nil config, got %v", err)
	}

	// 2. DataDir exists but MkdirAll for brainDir fails because a regular file is in the way
	tmpDir := t.TempDir()
	conflictFile := filepath.Join(tmpDir, "brain")
	_ = os.WriteFile(conflictFile, []byte("file-not-dir"), 0644)

	cfg := config.NewTestConfig()
	cur := cfg.Current()
	cur.DataDir = tmpDir
	cur.GeminiHomeDir = tmpDir
	cfg.Update(cur)

	_ = InitializeBrainEnvironment(context.Background(), cfg)

	// 3. Parent of cliBrainDir is a file
	tmpDir2 := t.TempDir()
	cliParentConflict := filepath.Join(tmpDir2, ".gemini", "antigravity-cli")
	_ = os.MkdirAll(filepath.Dir(cliParentConflict), 0755)
	_ = os.WriteFile(cliParentConflict, []byte("file-not-dir"), 0644)

	cur2 := cfg.Current()
	cur2.DataDir = tmpDir2
	cur2.GeminiHomeDir = tmpDir2
	cfg.Update(cur2)
	_ = InitializeBrainEnvironment(context.Background(), cfg)
}

func TestRunBrainApp_EarlyReturns(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. nil config
	if err := RunBrainApp(context.Background(), nil); err == nil {
		t.Errorf("expected error for nil config")
	}

	// 2. Cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.GeminiHomeDir = tmpDir
		d.DataDir = filepath.Join(tmpDir, "data")
	})
	if err := RunBrainApp(ctx, cfg); err != nil {
		t.Errorf("expected nil for cancelled context, got %v", err)
	}

	// 2b. Cancelled context with provisioner sync error in InitializeBrainEnvironment
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	blockingDir := filepath.Join(t.TempDir(), "block")
	_ = os.WriteFile(blockingDir, []byte("file"), 0644)
	cfgInitErr := config.NewTestConfig(func(d *config.ConfigData) {
		d.GeminiHomeDir = blockingDir
		d.DataDir = filepath.Join(tmpDir, "data")
	})
	if err := RunBrainApp(ctx2, cfgInitErr); err != nil {
		t.Errorf("expected nil for cancelled context with init error, got %v", err)
	}

	// 2c. Live context with init error in InitializeBrainEnvironment
	_ = RunBrainApp(context.Background(), cfgInitErr)

	// 3. Missing DatabaseURL
	ctxLive := context.Background()
	cfg2 := config.NewTestConfig(func(d *config.ConfigData) {
		d.GeminiHomeDir = tmpDir
		d.DataDir = filepath.Join(tmpDir, "data")
		d.DatabaseURL = ""
	})
	errMissingDB := RunBrainApp(ctxLive, cfg2)
	if errMissingDB == nil || !strings.Contains(errMissingDB.Error(), "database URL or path is required") {
		t.Errorf("expected database URL error, got %v", errMissingDB)
	}

	// 4. Missing GeminiHomeDir
	cfgNoHome := config.NewTestConfig(func(d *config.ConfigData) {
		d.GeminiHomeDir = ""
		d.DataDir = filepath.Join(tmpDir, "data")
		d.DatabaseURL = ":memory:"
	})
	errNoHome := RunBrainApp(ctxLive, cfgNoHome)
	if errNoHome == nil || !strings.Contains(errNoHome.Error(), "gemini home directory is required") {
		t.Errorf("expected gemini home directory error, got %v", errNoHome)
	}

	// 5. Missing DataDir
	cfgNoData := config.NewTestConfig(func(d *config.ConfigData) {
		d.GeminiHomeDir = tmpDir
		d.DataDir = ""
		d.DatabaseURL = ":memory:"
	})
	errNoData := RunBrainApp(ctxLive, cfgNoData)
	if errNoData == nil || !strings.Contains(errNoData.Error(), "data directory is required") {
		t.Errorf("expected data directory error, got %v", errNoData)
	}

	// 6. Missing Port
	cfgNoPort := config.NewTestConfig(func(d *config.ConfigData) {
		d.GeminiHomeDir = tmpDir
		d.DataDir = filepath.Join(tmpDir, "data")
		d.DatabaseURL = ":memory:"
		d.Port = ""
	})
	errNoPort := RunBrainApp(ctxLive, cfgNoPort)
	if errNoPort == nil || !strings.Contains(errNoPort.Error(), "port is required") {
		t.Errorf("expected port error, got %v", errNoPort)
	}

	// 7. Invalid DatabaseURL
	cur2 := cfg2.Current()
	cur2.DatabaseURL = "invalid-protocol://host:port/dbname"
	cur2.GeminiHomeDir = tmpDir
	cur2.DataDir = filepath.Join(tmpDir, "data")
	cur2.Port = "0"
	cfg2.Update(cur2)
	errBadDB := RunBrainApp(ctxLive, cfg2)
	if errBadDB == nil || !strings.Contains(errBadDB.Error(), "failed to initialize database") {
		t.Errorf("expected db init error, got %v", errBadDB)
	}
}

func TestCreateReloadConfigFunc_ProvisionerError(t *testing.T) {
	cfg := config.NewTestConfig()
	cur := cfg.Current()
	tmpFile := filepath.Join(t.TempDir(), "blocking_file")
	_ = os.WriteFile(tmpFile, []byte("file"), 0644)
	cur.GeminiHomeDir = tmpFile
	cfg.Update(cur)

	prov := env.NewFromConfig(cfg)
	reloadFn := CreateReloadConfigFunc(cfg, WithProvisioner(prov))
	reloadFn("test-provisioner-error")
}

func TestHandleSessions_UnreadableTranscript(t *testing.T) {
	store := db.NewFakeStore()

	tmpDir := t.TempDir()
	convDir := filepath.Join(tmpDir, "conv-no-transcript", ".system_generated", "logs")
	_ = os.MkdirAll(convDir, 0755)

	handler := handleTranscripts(store, tmpDir)
	req := httptest.NewRequest(http.MethodGet, "/transcripts", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rr.Code)
	}
}

func TestHandleSchedules_CacheDoubleCheck(t *testing.T) {
	store := db.NewFakeStore()

	handler := handleSchedules(store)

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/schedules", nil)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
		}()
	}
	wg.Wait()
}

func TestHandleTasks_CacheDoubleCheck(t *testing.T) {
	store := db.NewFakeStore()

	handler := handleTasks(store)

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := httptest.NewRequest(http.MethodGet, "/tasks", nil)
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
		}()
	}
	wg.Wait()
}

func TestRunBrainApp_FullLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(tmpDir, ".gemini", "config", "skills"), 0755)

	cfg := config.NewTestConfig()
	cur := cfg.Current()
	cur.Port = "0"
	cur.AgyBin = "/bin/true"
	cur.LowEffortModel = ""
	cur.Voice.AmbientContext = &config.AmbientContextConfig{
		Template: "Time: {{.now}}",
		CacheTTL: "5s",
	}
	if cur.Channels == nil {
		cur.Channels = make(map[string]config.ChannelPolicy)
	}
	cur.Channels["default"] = config.ChannelPolicy{
		Mode: "thread",
		AmbientContext: &config.AmbientContextConfig{
			Template: "Time: {{.now}}",
			CacheTTL: "5s",
		},
	}
	cfg.Update(cur)

	var serverAddr string
	ready := make(chan struct{})
	oldReady := onServerReady
	onServerReady = func(addr string) {
		serverAddr = addr
		close(ready)
	}
	defer func() { onServerReady = oldReady }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-ready
		reqBody := `{"prompt": "Hello test prompt", "channel_id": "1542423172400291873", "message_id": "msg-1"}`
		resp, err := http.Post(fmt.Sprintf("http://%s/prompt", serverAddr), "application/json", strings.NewReader(reqBody))
		if err == nil {
			_ = resp.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	mockStore := db.NewFakeStore()
	err := RunBrainApp(ctx, cfg, WithStore(mockStore), WithProcessSpawner(runner.NewMockDaemonSpawner()))
	if err != nil {
		t.Fatalf("RunBrainApp failed: %v", err)
	}
}

func TestRunBrainApp_SIGHUP(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(tmpDir, ".gemini", "config", "skills"), 0755)

	cfg := config.NewTestConfig()
	cur := cfg.Current()
	cur.GeminiHomeDir = tmpDir
	cur.DataDir = filepath.Join(tmpDir, "data")
	cur.Port = "0"
	cur.Model = "gemini-2.5-flash"
	cur.DatabaseURL = ":memory:"
	if cur.AgyBin == "" {
		cur.AgyBin = "/bin/true"
	}
	cfg.Update(cur)

	ready := make(chan struct{})
	oldReady := onServerReady
	onServerReady = func(addr string) {
		close(ready)
	}
	defer func() { onServerReady = oldReady }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	hupChan := make(chan os.Signal, 1)
	reloadReceived := make(chan string, 1)

	go func() {
		<-ready
		// Send mock SIGHUP via injected channel (zero raw POSIX signal broadcasts)
		hupChan <- syscall.SIGHUP
		select {
		case src := <-reloadReceived:
			if src != "SIGHUP" {
				t.Errorf("expected reload source SIGHUP, got %q", src)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("timeout waiting for SIGHUP reload callback")
		}
		cancel()
	}()

	mockStore := db.NewFakeStore()
	err := RunBrainApp(ctx, cfg,
		WithStore(mockStore),
		WithProcessSpawner(runner.NewMockDaemonSpawner()),
		WithHupChannel(hupChan),
		WithOnReload(func(src string) {
			select {
			case reloadReceived <- src:
			default:
			}
		}),
	)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatalf("RunBrainApp failed: %v", err)
	}
}

func TestBrainAppOptions_Coverage(t *testing.T) {
	var opts brainAppOptions
	hupChan := make(chan os.Signal, 1)
	WithHupChannel(hupChan)(&opts)
	if opts.hupChan != hupChan {
		t.Errorf("expected hupChan to be set")
	}

	called := false
	WithOnReload(func(src string) {
		called = true
	})(&opts)
	if opts.onReload == nil {
		t.Fatalf("expected onReload to be set")
	}
	opts.onReload("test")
	if !called {
		t.Errorf("expected onReload to be invoked")
	}
}

func TestRunBrainApp_DiscordLowEffortPool_EphemeralHome(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(tmpDir, ".gemini", "config", "skills"), 0755)

	cfg := config.NewTestConfig()
	cur := cfg.Current()
	cur.DataDir = tmpDir
	cur.GeminiHomeDir = tmpDir
	cur.DiscordToken = "mock-discord-token"
	cur.Port = "0"
	cur.AgyBin = "/bin/true"
	cur.LowEffortModel = "gemini-test-low"
	cfg.Update(cur)

	expectedEphemeralHome := filepath.Join(tmpDir, "runtimes", "ephemeral")
	expectedDiscordHome := filepath.Join(tmpDir, "runtimes", "discord")

	var spawnedConfigsMu sync.Mutex
	var spawnedConfigs []runner.DaemonConfig

	baseMock := runner.NewMockDaemonSpawner()
	allSpawned := make(chan struct{})
	var spawnedOnce sync.Once
	trackingSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, dCfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			spawnedConfigsMu.Lock()
			spawnedConfigs = append(spawnedConfigs, dCfg)
			hasEph0, hasEph1, hasDisc0, hasDisc1 := false, false, false, false
			for _, sc := range spawnedConfigs {
				if sc.ThreadID == "ephemeral:worker-0" {
					hasEph0 = true
				}
				if sc.ThreadID == "ephemeral:worker-1" {
					hasEph1 = true
				}
				if sc.ThreadID == "discord:worker-0" {
					hasDisc0 = true
				}
				if sc.ThreadID == "discord:worker-1" {
					hasDisc1 = true
				}
			}
			if hasEph0 && hasEph1 && hasDisc0 && hasDisc1 {
				spawnedOnce.Do(func() { close(allSpawned) })
			}
			spawnedConfigsMu.Unlock()
			return baseMock.Spawn(ctx, dCfg)
		},
	}

	ready := make(chan struct{})
	oldReady := onServerReady
	onServerReady = func(addr string) {
		close(ready)
	}
	defer func() { onServerReady = oldReady }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-ready
		select {
		case <-allSpawned:
		case <-time.After(2 * time.Second):
		}
		cancel()
	}()

	mockStore := db.NewFakeStore()
	err := RunBrainApp(ctx, cfg, WithStore(mockStore), WithProcessSpawner(trackingSpawner))
	if err != nil {
		t.Fatalf("RunBrainApp failed: %v", err)
	}

	spawnedConfigsMu.Lock()
	defer spawnedConfigsMu.Unlock()

	foundEph0 := false
	foundEph1 := false
	foundDisc0 := false
	foundDisc1 := false
	for _, sc := range spawnedConfigs {
		if sc.ThreadID == "ephemeral:worker-0" {
			foundEph0 = true
			foundHome := false
			for _, e := range sc.Env {
				if e == "HOME="+expectedEphemeralHome {
					foundHome = true
					break
				}
			}
			if !foundHome {
				t.Errorf("ephemeral:worker-0 expected HOME=%q in Env, got: %v", expectedEphemeralHome, sc.Env)
			}
		}
		if sc.ThreadID == "ephemeral:worker-1" {
			foundEph1 = true
			foundHome := false
			for _, e := range sc.Env {
				if e == "HOME="+expectedEphemeralHome {
					foundHome = true
					break
				}
			}
			if !foundHome {
				t.Errorf("ephemeral:worker-1 expected HOME=%q in Env, got: %v", expectedEphemeralHome, sc.Env)
			}
		}
		if sc.ThreadID == "discord:worker-0" {
			foundDisc0 = true
			foundHome := false
			for _, e := range sc.Env {
				if e == "HOME="+expectedDiscordHome {
					foundHome = true
					break
				}
			}
			if !foundHome {
				t.Errorf("discord:worker-0 expected HOME=%q in Env, got: %v", expectedDiscordHome, sc.Env)
			}
		}
		if sc.ThreadID == "discord:worker-1" {
			foundDisc1 = true
			foundHome := false
			for _, e := range sc.Env {
				if e == "HOME="+expectedDiscordHome {
					foundHome = true
					break
				}
			}
			if !foundHome {
				t.Errorf("discord:worker-1 expected HOME=%q in Env, got: %v", expectedDiscordHome, sc.Env)
			}
		}
	}

	if !foundEph0 {
		t.Errorf("expected pre-warmed daemon for ephemeral:worker-0")
	}
	if !foundEph1 {
		t.Errorf("expected pre-warmed daemon for ephemeral:worker-1")
	}
	if !foundDisc0 {
		t.Errorf("expected pre-warmed daemon for discord:worker-0")
	}
	if !foundDisc1 {
		t.Errorf("expected pre-warmed daemon for discord:worker-1")
	}
}

func TestRunBrainApp_ProcessPoolInitErrors(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(tmpDir, ".gemini", "config", "skills"), 0755)

	cfg := config.NewTestConfig()
	cur := cfg.Current()
	cur.DataDir = tmpDir
	cur.GeminiHomeDir = tmpDir
	cur.Port = "0"
	cur.AgyBin = "/bin/true"
	cfg.Update(cur)

	failingSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, dCfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			return nil, nil, nil, nil, errors.New("simulated spawn failure")
		},
	}

	ready := make(chan struct{})
	oldReady := onServerReady
	onServerReady = func(addr string) {
		close(ready)
	}
	defer func() { onServerReady = oldReady }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-ready
		cancel()
	}()

	mockStore := db.NewFakeStore()
	if err := RunBrainApp(ctx, cfg, WithStore(mockStore), WithProcessSpawner(failingSpawner)); err != nil {
		t.Fatalf("unexpected error from RunBrainApp: %v", err)
	}
}

type mockErrProcessHandle struct{}

func (m *mockErrProcessHandle) Pid() int    { return 99999 }
func (m *mockErrProcessHandle) Kill() error { return errors.New("simulated kill error") }
func (m *mockErrProcessHandle) Wait() error { return nil }

func TestRunBrainApp_DaemonCloseErrors(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(tmpDir, ".gemini", "config", "skills"), 0755)

	cfg := config.NewTestConfig()
	cur := cfg.Current()
	cur.DataDir = tmpDir
	cur.GeminiHomeDir = tmpDir
	cur.Port = "0"
	cur.AgyBin = "/bin/true"
	cfg.Update(cur)

	spawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, dCfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			_ = inR
			_ = errW
			go func() {
				_, _ = outW.Write([]byte("ready\n"))
			}()
			return inW, outR, errR, &mockErrProcessHandle{}, nil
		},
	}

	ready := make(chan struct{})
	oldReady := onServerReady
	onServerReady = func(addr string) {
		close(ready)
	}
	defer func() { onServerReady = oldReady }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-ready
		cancel()
	}()

	mockStore := db.NewFakeStore()
	if err := RunBrainApp(ctx, cfg, WithStore(mockStore), WithProcessSpawner(spawner)); err != nil {
		t.Fatalf("unexpected error from RunBrainApp: %v", err)
	}
}

func TestHandlers_NilStore(t *testing.T) {
	// 1. handleFacts with nil store
	reqFacts := httptest.NewRequest(http.MethodGet, "/facts", nil)
	recFacts := httptest.NewRecorder()
	handleFacts(nil)(recFacts, reqFacts)
	if recFacts.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for handleFacts(nil), got %d", recFacts.Code)
	}

	// 2. handleSchedules with nil store
	reqSched := httptest.NewRequest(http.MethodGet, "/schedules", nil)
	recSched := httptest.NewRecorder()
	handleSchedules(nil)(recSched, reqSched)
	if recSched.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for handleSchedules(nil), got %d", recSched.Code)
	}

	// 3. handleScheduleRuns with nil store
	reqRuns := httptest.NewRequest(http.MethodGet, "/schedule-runs", nil)
	recRuns := httptest.NewRecorder()
	handleScheduleRuns(nil)(recRuns, reqRuns)
	if recRuns.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for handleScheduleRuns(nil), got %d", recRuns.Code)
	}

	// 4. handleTasks with nil store
	reqTasks := httptest.NewRequest(http.MethodGet, "/tasks", nil)
	recTasks := httptest.NewRecorder()
	handleTasks(nil)(recTasks, reqTasks)
	if recTasks.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for handleTasks(nil), got %d", recTasks.Code)
	}
}

func TestDefaultPaths(t *testing.T) {
	// 1. DefaultGeminiHomeDir with explicit HOME
	t.Setenv("HOME", "/custom/gemini/home")
	if dir := DefaultGeminiHomeDir(); dir != "/custom/gemini/home" {
		t.Errorf("expected /custom/gemini/home, got %s", dir)
	}

	// 2. DefaultGeminiHomeDir with empty HOME in testing
	t.Setenv("HOME", "")
	if dir := DefaultGeminiHomeDir(); dir == "" {
		t.Errorf("expected non-empty fallback directory, got empty")
	}

	// 3. DefaultDataDir in testing
	if dir := DefaultDataDir(); dir == "" {
		t.Errorf("expected non-empty data dir, got empty")
	}
}

type errorResponseWriter struct {
	http.ResponseWriter
}

func (e *errorResponseWriter) Header() http.Header {
	return e.ResponseWriter.Header()
}

func (e *errorResponseWriter) WriteHeader(statusCode int) {
	e.ResponseWriter.WriteHeader(statusCode)
}

func (e *errorResponseWriter) Write([]byte) (int, error) {
	return 0, errors.New("write error")
}

type errCloserReader struct {
	io.Reader
}

func (r *errCloserReader) Close() error {
	return errors.New("close error")
}

func TestWriteJSON_Error(t *testing.T) {
	w := httptest.NewRecorder()
	// Channels cannot be JSON-encoded
	writeJSON(w, http.StatusOK, make(chan int))
}

func TestHandlePrompt_BodyCloseError(t *testing.T) {
	store := db.NewFakeStore()
	pool := newTestWorkerPool(store)
	handler := handlePrompt(store, pool)

	validPayload, _ := json.Marshal(map[string]string{
		"prompt":     "Test prompt",
		"channel_id": "1542423172400291873",
	})
	req := httptest.NewRequest(http.MethodPost, "/prompt", &errCloserReader{Reader: bytes.NewReader(validPayload)})
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusAccepted {
		t.Errorf("expected 202, got %d", w.Code)
	}
}

func TestSetupBrainMux_WriteErrors(t *testing.T) {
	store := db.NewFakeStore()
	pool := newTestWorkerPool(store)
	mux := SetupBrainMux(store, pool)

	// /health write error
	reqHealth := httptest.NewRequest(http.MethodGet, "/health", nil)
	recHealth := httptest.NewRecorder()
	errWHealth := &errorResponseWriter{ResponseWriter: recHealth}
	mux.ServeHTTP(errWHealth, reqHealth)
}

func TestRunBrainApp_DefaultStoreError(t *testing.T) {
	tmpDir := t.TempDir()
	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.Port = "0"
		d.AgyBin = "/bin/true"
		d.Model = "gemini-2.5-flash"
		d.SystemPrompt = "test"
		d.GeminiHomeDir = tmpDir
		d.DataDir = filepath.Join(tmpDir, "data")
		d.DatabaseURL = "invalid://schema"
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := RunBrainApp(ctx, cfg)
	if err == nil || !strings.Contains(err.Error(), "failed to initialize database") {
		t.Errorf("expected failed to initialize database error, got: %v", err)
	}
}

func TestRunBrainApp_DefaultStoreSuccess(t *testing.T) {
	db.RegisterSQLiteTestHook(func(dsn string) (*sql.DB, error) {
		return sql.Open("sqlite", ":memory:")
	})
	defer db.RegisterSQLiteTestHook(nil)

	tmpDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(tmpDir, "data", ".gemini", "config", "skills"), 0755)
	_ = os.MkdirAll(filepath.Join(tmpDir, ".gemini", "config", "skills"), 0755)

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.Port = "0"
		d.AgyBin = "/bin/true"
		d.Model = "gemini-2.5-flash"
		d.SystemPrompt = "test"
		d.GeminiHomeDir = tmpDir
		d.DataDir = filepath.Join(tmpDir, "data")
		d.DatabaseURL = ":memory:"
	})

	ready := make(chan struct{})
	oldReady := onServerReady
	onServerReady = func(addr string) {
		close(ready)
	}
	defer func() { onServerReady = oldReady }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-ready
		cancel()
	}()

	if err := RunBrainApp(ctx, cfg, WithProcessSpawner(runner.NewMockDaemonSpawner())); err != nil {
		t.Fatalf("unexpected error from RunBrainApp: %v", err)
	}
}

func TestHandleVoiceAsk_Validation(t *testing.T) {
	store := db.NewFakeStore()
	pool := newTestWorkerPool(store)
	pool.Start()
	defer pool.Stop()

	handler := handleVoiceAsk(pool)

	// 1. GET not allowed
	req := httptest.NewRequest(http.MethodGet, "/voice/ask", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405 MethodNotAllowed, got %d", w.Code)
	}
	if w.Header().Get("Allow") != "POST" {
		t.Errorf("expected Allow: POST header, got %q", w.Header().Get("Allow"))
	}

	// 2. Empty prompt
	req = httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":""}`))
	w = httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 BadRequest for empty prompt, got %d", w.Code)
	}

	// 3. Malformed JSON
	req = httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{invalid-json`))
	w = httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 BadRequest for malformed JSON, got %d", w.Code)
	}

	// 4. Request body read error
	reqErrBody := httptest.NewRequest(http.MethodPost, "/voice/ask", &errTestReader{})
	wErrBody := httptest.NewRecorder()
	handler(wErrBody, reqErrBody)
	if wErrBody.Code != http.StatusBadRequest {
		t.Errorf("expected 400 BadRequest for body read error, got %d", wErrBody.Code)
	}

	// 5. Non-flusher response writer for SSE
	reqSSE := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`))
	reqSSE.Header.Set("Accept", "text/event-stream")
	nonFlusher := &nonFlusherTestWriter{}
	handler(nonFlusher, reqSSE)
	if nonFlusher.code != http.StatusInternalServerError {
		t.Errorf("expected 500 for non-flusher SSE, got %d", nonFlusher.code)
	}

	// 6. conversation_id fallback when session_id is empty
	reqConvID := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"conversation_id":"conv-123","prompt":"hello"}`))
	wConvID := httptest.NewRecorder()
	handler(wConvID, reqConvID)
	if wConvID.Code != http.StatusOK {
		t.Errorf("expected 200 OK for conversation_id fallback, got %d", wConvID.Code)
	}

	// 7. Missing session_id, node_id, and conversation_id returns 400 Bad Request
	reqNoID := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello"}`))
	wNoID := httptest.NewRecorder()
	handler(wNoID, reqNoID)
	if wNoID.Code != http.StatusBadRequest {
		t.Errorf("expected 400 BadRequest for missing session_id, got %d", wNoID.Code)
	}
	if !strings.Contains(wNoID.Body.String(), "session_id") {
		t.Errorf("expected session_id required error message, got %s", wNoID.Body.String())
	}

	// 8. node_id provided successfully
	reqNodeID := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","node_id":"touch-kiosk-kitchen"}`))
	wNodeID := httptest.NewRecorder()
	handler(wNodeID, reqNodeID)
	if wNodeID.Code != http.StatusOK {
		t.Errorf("expected 200 OK for node_id routing, got %d: %s", wNodeID.Code, wNodeID.Body.String())
	}

	// 9. node_id priority over session_id and conversation_id
	reqPriority := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","node_id":"touch-kiosk-kitchen","session_id":"turn-123","conversation_id":"conv-456"}`))
	wPriority := httptest.NewRecorder()
	handler(wPriority, reqPriority)
	if wPriority.Code != http.StatusOK {
		t.Errorf("expected 200 OK for node_id priority, got %d", wPriority.Code)
	}

	// 10. Device identifier length exceeds 128 characters returns 400 Bad Request
	tooLongID := strings.Repeat("a", 129)
	reqTooLong := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(fmt.Sprintf(`{"prompt":"hello","node_id":%q}`, tooLongID)))
	wTooLong := httptest.NewRecorder()
	handler(wTooLong, reqTooLong)
	if wTooLong.Code != http.StatusBadRequest {
		t.Errorf("expected 400 BadRequest for oversized device ID, got %d", wTooLong.Code)
	}
	if !strings.Contains(wTooLong.Body.String(), "exceeds 128 characters") {
		t.Errorf("expected exceeds 128 characters error message, got %s", wTooLong.Body.String())
	}

	// 11. Device identifier containing control characters returns 400 Bad Request
	reqInvalidChars := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","node_id":"dock\ninvalid"}`))
	wInvalidChars := httptest.NewRecorder()
	handler(wInvalidChars, reqInvalidChars)
	if wInvalidChars.Code != http.StatusBadRequest {
		t.Errorf("expected 400 BadRequest for control characters in device ID, got %d", wInvalidChars.Code)
	}
}

type errTestReader struct{}

func (e *errTestReader) Read(p []byte) (int, error) {
	return 0, errors.New("simulated body read error")
}

type nonFlusherTestWriter struct {
	header http.Header
	code   int
	body   bytes.Buffer
}

func (n *nonFlusherTestWriter) Header() http.Header {
	if n.header == nil {
		n.header = make(http.Header)
	}
	return n.header
}
func (n *nonFlusherTestWriter) Write(b []byte) (int, error) {
	return n.body.Write(b)
}
func (n *nonFlusherTestWriter) WriteHeader(statusCode int) {
	n.code = statusCode
}

func TestHandleVoiceAsk_SSE_Success(t *testing.T) {
	store := db.NewFakeStore()
	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
		Store: store,
		VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			if onStatus != nil {
				onStatus("⚡ Checking smart lights...")
			}
			return "Lights have been turned off.", "sess-test-456", nil
		},
	})
	pool.Start()
	defer pool.Stop()

	handler := handleVoiceAsk(pool)

	payload := `{"prompt":"turn off the lights","session_id":"sess-test-456"}`
	req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(payload))
	req.Header.Set("Accept", "text/event-stream")

	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}
	if !strings.Contains(w.Header().Get("Content-Type"), "text/event-stream") {
		t.Errorf("expected Content-Type text/event-stream, got %q", w.Header().Get("Content-Type"))
	}
	if w.Header().Get("X-Accel-Buffering") != "no" {
		t.Errorf("expected X-Accel-Buffering: no, got %q", w.Header().Get("X-Accel-Buffering"))
	}

	body := w.Body.String()
	if !strings.Contains(body, "event: status\ndata: {\"status\":\"⚡ Checking smart lights...\"}\n\n") {
		t.Errorf("expected status event in SSE stream, got:\n%s", body)
	}
	if !strings.Contains(body, "event: reply\ndata: {\"conversation_id\":\"sess-test-456\",\"reply\":\"Lights have been turned off.\"}\n\n") {
		t.Errorf("expected reply event with conversation_id in SSE stream, got:\n%s", body)
	}
	if !strings.Contains(body, "event: done\ndata: {\"conversation_id\":\"sess-test-456\"}\n\n") {
		t.Errorf("expected done event in SSE stream, got:\n%s", body)
	}

	mRec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(mRec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(mRec.Body.String(), `aerial_brain_voice_ttfr_duration_seconds_bucket{mode="sse",status="success"`) {
		t.Errorf("expected voice TTFR metric for sse/success, got:\n%s", mRec.Body.String())
	}
}

func TestHandleVoiceAsk_SSE_SentenceStreaming(t *testing.T) {
	store := db.NewFakeStore()
	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
		Store: store,
		VoiceStreamRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string), onSentence func(string)) (string, string, error) {
			if onStatus != nil {
				onStatus("⚡ Checking status...")
			}
			if onSentence != nil {
				onSentence("Here is sentence one.")
				onSentence("And here is sentence two.")
			}
			return "Here is sentence one. And here is sentence two.", "sess-stream-789", nil
		},
	})
	pool.Start()
	defer pool.Stop()

	handler := handleVoiceAsk(pool)

	payload := `{"prompt":"tell me two things","session_id":"sess-stream-789"}`
	req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(payload))
	req.Header.Set("Accept", "text/event-stream")

	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}

	body := w.Body.String()
	if !strings.Contains(body, "event: status\ndata: {\"status\":\"⚡ Checking status...\"}\n\n") {
		t.Errorf("expected status event in SSE stream, got:\n%s", body)
	}
	if !strings.Contains(body, "event: sentence\ndata: {\"text\":\"Here is sentence one.\"}\n\n") {
		t.Errorf("expected sentence event 1 in SSE stream, got:\n%s", body)
	}
	if !strings.Contains(body, "event: sentence\ndata: {\"text\":\"And here is sentence two.\"}\n\n") {
		t.Errorf("expected sentence event 2 in SSE stream, got:\n%s", body)
	}
	if !strings.Contains(body, "event: reply\ndata: {\"conversation_id\":\"sess-stream-789\",\"reply\":\"Here is sentence one. And here is sentence two.\"}\n\n") {
		t.Errorf("expected reply event with conversation_id in SSE stream, got:\n%s", body)
	}
	if !strings.Contains(body, "event: done\ndata: {\"conversation_id\":\"sess-stream-789\"}\n\n") {
		t.Errorf("expected done event in SSE stream, got:\n%s", body)
	}

	// Verify ordering: sentence events must appear before reply event
	idxSent1 := strings.Index(body, "event: sentence\ndata: {\"text\":\"Here is sentence one.\"}")
	idxSent2 := strings.Index(body, "event: sentence\ndata: {\"text\":\"And here is sentence two.\"}")
	idxReply := strings.Index(body, "event: reply\ndata: {\"conversation_id\":\"sess-stream-789\"")
	if idxSent1 == -1 || idxSent2 == -1 || idxReply == -1 || idxSent1 > idxSent2 || idxSent2 > idxReply {
		t.Errorf("invalid event ordering: idxSent1=%d, idxSent2=%d, idxReply=%d", idxSent1, idxSent2, idxReply)
	}
}

func TestHandleVoiceAsk_JSON_Success(t *testing.T) {
	store := db.NewFakeStore()
	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
		Store: store,
		VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			return "JSON voice reply", "sess-json-123", nil
		},
	})
	pool.Start()
	defer pool.Stop()

	handler := handleVoiceAsk(pool)

	payload := `{"prompt":"what is the weather?","session_id":"sess-json-123"}`
	req := httptest.NewRequest(http.MethodPost, "/api/voice/ask", strings.NewReader(payload))
	req.Header.Set("Accept", "application/json")

	w := httptest.NewRecorder()
	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w.Code)
	}

	var resp VoiceAskResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if resp.Reply != "JSON voice reply" || resp.ConversationID != "sess-json-123" || resp.NodeID != "" {
		t.Errorf("unexpected response when node_id omitted: %+v", resp)
	}

	// Request with explicit node_id echoes node_id
	payloadWithNode := `{"prompt":"what is the weather?","node_id":"touch-kiosk-kitchen"}`
	reqWithNode := httptest.NewRequest(http.MethodPost, "/api/voice/ask", strings.NewReader(payloadWithNode))
	reqWithNode.Header.Set("Accept", "application/json")
	wWithNode := httptest.NewRecorder()
	handler(wWithNode, reqWithNode)
	if wWithNode.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for node_id request, got %d", wWithNode.Code)
	}
	var respWithNode VoiceAskResponse
	if err := json.Unmarshal(wWithNode.Body.Bytes(), &respWithNode); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if respWithNode.NodeID != "touch-kiosk-kitchen" {
		t.Errorf("expected echoed node_id 'touch-kiosk-kitchen', got %q", respWithNode.NodeID)
	}

	mRec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(mRec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(mRec.Body.String(), `aerial_brain_voice_ttfr_duration_seconds_bucket{mode="json",status="success"`) {
		t.Errorf("expected voice TTFR metric for json/success, got:\n%s", mRec.Body.String())
	}
}

func TestHandleVoiceAsk_Errors(t *testing.T) {
	store := db.NewFakeStore()

	// 1. SSE mode with error
	poolErr := queue.NewWorkerPool(queue.WorkerPoolConfig{
		Store: store,
		VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			return "", "", errors.New("deliberation timed out")
		},
	})
	poolErr.Start()
	defer poolErr.Stop()

	hErr := handleVoiceAsk(poolErr)
	req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"fail","session_id":"sess-err-1"}`))
	req.Header.Set("Accept", "text/event-stream")
	w := httptest.NewRecorder()
	hErr(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 OK for committed SSE stream, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "event: error\ndata: {\"error\":\"deliberation timed out\"}\n\n") {
		t.Errorf("expected error event, got:\n%s", body)
	}
	if !strings.Contains(body, "event: done\ndata: {}\n\n") {
		t.Errorf("expected done event after error, got:\n%s", body)
	}

	// 2. JSON mode with error
	reqJSON := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"fail","session_id":"sess-err-2"}`))
	reqJSON.Header.Set("Accept", "application/json")
	wJSON := httptest.NewRecorder()
	hErr(wJSON, reqJSON)

	if wJSON.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 InternalServerError, got %d", wJSON.Code)
	}
	if !strings.Contains(wJSON.Body.String(), "deliberation timed out") {
		t.Errorf("expected error detail in response, got:\n%s", wJSON.Body.String())
	}

	// 3. Nil pool handling
	hNil := handleVoiceAsk(nil)
	reqNil := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"sess-nil"}`))
	wNil := httptest.NewRecorder()
	hNil(wNil, reqNil)
	if wNil.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 for nil pool, got %d", wNil.Code)
	}

	// Nil pool SSE mode
	reqNilSSE := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"sess-nil-sse"}`))
	reqNilSSE.Header.Set("Accept", "text/event-stream")
	wNilSSE := httptest.NewRecorder()
	hNil(wNilSSE, reqNilSSE)
	if wNilSSE.Code != http.StatusOK {
		t.Errorf("expected 200 for SSE with nil pool, got %d", wNilSSE.Code)
	}
	if !strings.Contains(wNilSSE.Body.String(), "event: error") {
		t.Errorf("expected error event for nil pool SSE, got:\n%s", wNilSSE.Body.String())
	}

	mRec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(mRec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if !strings.Contains(mRec.Body.String(), `aerial_brain_voice_ttfr_duration_seconds_bucket{mode="sse",status="error"`) {
		t.Errorf("expected voice TTFR metric for sse/error, got:\n%s", mRec.Body.String())
	}
	if !strings.Contains(mRec.Body.String(), `aerial_brain_voice_ttfr_duration_seconds_bucket{mode="json",status="error"`) {
		t.Errorf("expected voice TTFR metric for json/error, got:\n%s", mRec.Body.String())
	}
}

func TestNormalizeRoute_Voice(t *testing.T) {
	if got := normalizeRoute("/voice/ask"); got != "/voice/ask" {
		t.Errorf("normalizeRoute(/voice/ask) = %q, want /voice/ask", got)
	}
	if got := normalizeRoute("/api/voice/ask"); got != "/voice/ask" {
		t.Errorf("normalizeRoute(/api/voice/ask) = %q, want /voice/ask", got)
	}
}

func TestCreateReloadConfigFunc_WithWorkerPool(t *testing.T) {
	cfg := config.NewTestConfig()
	pool := queue.New(cfg, queue.WorkerPoolConfig{})
	reloadFn := CreateReloadConfigFunc(cfg,
		WithSkipEnvironmentSync(),
		WithReloadSupplier(func(active *config.Config) error {
			return nil
		}),
		WithWorkerPool(pool),
	)
	reloadFn("TestWithWorkerPool")
}

type nonFlusherResponseWriter struct {
	header http.Header
	code   int
	body   bytes.Buffer
}

func (w *nonFlusherResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *nonFlusherResponseWriter) Write(b []byte) (int, error) {
	return w.body.Write(b)
}

func (w *nonFlusherResponseWriter) WriteHeader(statusCode int) {
	w.code = statusCode
}

func TestHandleVoiceAsk_NonFlusherStreamingError(t *testing.T) {
	handler := handleVoiceAsk(nil)
	req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`))
	req.Header.Set("Accept", "text/event-stream")

	w := &nonFlusherResponseWriter{}
	handler(w, req)
	if w.code != http.StatusInternalServerError {
		t.Errorf("expected 500 for non-flusher streaming, got %d", w.code)
	}
	if !strings.Contains(w.body.String(), "Streaming not supported") {
		t.Errorf("expected streaming not supported error, got: %s", w.body.String())
	}
}

func TestHandleVoiceAsk_InvalidPayload(t *testing.T) {
	handler := handleVoiceAsk(nil)
	reqEmpty := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"   "}`))
	wEmpty := httptest.NewRecorder()
	handler(wEmpty, reqEmpty)
	if wEmpty.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for empty prompt, got %d", wEmpty.Code)
	}

	reqBadJSON := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{invalid-json`))
	wBadJSON := httptest.NewRecorder()
	handler(wBadJSON, reqBadJSON)
	if wBadJSON.Code != http.StatusBadRequest {
		t.Errorf("expected 400 for malformed json, got %d", wBadJSON.Code)
	}
}

func TestHandleTranscripts_NonStringError(t *testing.T) {
	store := db.NewFakeStore()
	tmpDir := t.TempDir()
	convDir := filepath.Join(tmpDir, "conv-err", ".system_generated", "logs")
	_ = os.MkdirAll(convDir, 0755)
	_ = os.WriteFile(filepath.Join(convDir, "transcript.jsonl"), []byte(`{"step_index":1,"error":99999}`+"\n"), 0644)

	handler := handleTranscripts(store, tmpDir)
	req := httptest.NewRequest(http.MethodGet, "/transcripts", nil)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "99999") {
		t.Errorf("expected 99999 in response, got %s", rr.Body.String())
	}
}

func TestInitializeBrainEnvironment_SymlinkParentBlocker(t *testing.T) {
	tmpDir := t.TempDir()
	geminiDir := filepath.Join(tmpDir, "home", ".gemini")
	_ = os.MkdirAll(geminiDir, 0755)
	_ = os.WriteFile(filepath.Join(geminiDir, "antigravity-cli"), []byte("blocking-file"), 0644)

	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.GeminiHomeDir = filepath.Join(tmpDir, "home")
		d.DataDir = filepath.Join(tmpDir, "data")
	})
	_ = os.MkdirAll(filepath.Join(tmpDir, "data"), 0755)

	_ = InitializeBrainEnvironment(context.Background(), cfg)
}

type errCloseReader struct {
	io.Reader
}

func (e *errCloseReader) Close() error {
	return errors.New("simulated close error")
}

func TestHandleVoiceAsk_BodyCloseError(t *testing.T) {
	store := db.NewFakeStore()
	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
		Store: store,
		VoiceRunnerFunc: func(c context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			return "done", "sess-1", nil
		},
	})
	pool.Start()
	defer pool.Stop()

	handler := handleVoiceAsk(pool)
	req := httptest.NewRequest(http.MethodPost, "/voice/ask", &errCloseReader{Reader: strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`)})
	w := httptest.NewRecorder()
	handler(w, req)
}

func TestHandleVoiceAsk_SSE_CancelledContext(t *testing.T) {
	store := db.NewFakeStore()
	ctx, cancel := context.WithCancel(context.Background())
	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
		Store: store,
		VoiceRunnerFunc: func(c context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			cancel()
			if onStatus != nil {
				onStatus("status after cancel")
			}
			return "done", "sess-1", nil
		},
	})
	pool.Start()
	defer pool.Stop()

	handler := handleVoiceAsk(pool)
	req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`)).WithContext(ctx)
	req.Header.Set("Accept", "text/event-stream")
	w := httptest.NewRecorder()
	handler(w, req)
}

func TestHandleVoiceAsk_SSE_VoiceRunnerError(t *testing.T) {
	store := db.NewFakeStore()
	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
		Store: store,
		VoiceRunnerFunc: func(c context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			if onStatus != nil {
				onStatus("working")
			}
			return "", "", errors.New("synthetic voice failure")
		},
	})
	pool.Start()
	defer pool.Stop()

	handler := handleVoiceAsk(pool)
	req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`))
	req.Header.Set("Accept", "text/event-stream")
	w := httptest.NewRecorder()
	handler(w, req)

	if !strings.Contains(w.Body.String(), "event: error") {
		t.Errorf("expected SSE error event, got %q", w.Body.String())
	}
}

type errFlusherWriter struct {
	header http.Header
}

func (e *errFlusherWriter) Header() http.Header {
	if e.header == nil {
		e.header = make(http.Header)
	}
	return e.header
}
func (e *errFlusherWriter) Write(p []byte) (int, error) {
	return 0, errors.New("simulated write error")
}
func (e *errFlusherWriter) WriteHeader(statusCode int) {}
func (e *errFlusherWriter) Flush()                    {}

func TestHandleVoiceAsk_SSE_WriteError(t *testing.T) {
	store := db.NewFakeStore()
	pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
		Store: store,
		VoiceRunnerFunc: func(c context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
			return "done", "sess-1", nil
		},
	})
	pool.Start()
	defer pool.Stop()

	handler := handleVoiceAsk(pool)
	req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`))
	req.Header.Set("Accept", "text/event-stream")
	w := &errFlusherWriter{}
	handler(w, req)
}

func TestInitializeBrainEnvironment_SymlinkError(t *testing.T) {
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	cfg := config.NewTestConfig(func(d *config.ConfigData) {
		d.GeminiHomeDir = tmpHome
		d.DataDir = tmpData
	})

	parent := filepath.Join(tmpHome, ".gemini", "antigravity-cli")
	_ = os.MkdirAll(parent, 0755)
	_ = os.Chmod(parent, 0555)
	defer func() { _ = os.Chmod(parent, 0755) }()

	_ = InitializeBrainEnvironment(context.Background(), cfg)
}

func TestUnifiedPool_EphemeralLLMFuncIntegration(t *testing.T) {
	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":\"00000000-0000-0000-0000-000000000001\"}\n")
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					line := scanner.Text()
					var response string
					if strings.Contains(line, "summarize") || strings.Contains(line, "title") {
						response = "title summary ok"
					} else {
						response = `{"confidence":0.9,"reasoning":"test"}`
					}
					_, _ = fmt.Fprintf(outW, "{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":%q}}\n", response)
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}

	pool := runner.NewUnifiedProcessPool(runner.PoolConfig{
		DefaultModel: "gemini-2.5-flash",
		TargetModels: map[string]string{
			"ephemeral:classifier": "gemini-3.8-flash-low",
			"ephemeral:summarizer": "gemini-3.8-flash-low",
		},
	}, mockSpawner)
	defer pool.Close()

	ctx := context.Background()

	// 1. EphemeralLLMFunc for classifier
	classifierLLM := pool.EphemeralLLMFunc("ephemeral:classifier")
	clsRes, err := classifierLLM(ctx, "", "classify this prompt")
	if err != nil {
		t.Fatalf("unexpected error from classifierLLM: %v", err)
	}
	if clsRes != `{"confidence":0.9,"reasoning":"test"}` {
		t.Fatalf("unexpected classifier result: %q", clsRes)
	}

	// 2. EphemeralLLMFunc for summarizer
	summarizerLLM := pool.EphemeralLLMFunc("ephemeral:summarizer")
	sumRes, err := summarizerLLM(ctx, "", "please summarize title")
	if err != nil {
		t.Fatalf("unexpected error from summarizerLLM: %v", err)
	}
	if sumRes != "title summary ok" {
		t.Fatalf("unexpected summarizer result: %q", sumRes)
	}

	// 3. Classifier integration with WithProcessPool
	cfg := config.NewTestConfig()
	cur := cfg.Current()
	cur.Ollama.BaseURL = "" // Ensure primary LLMFunc is used
	cfg.Update(cur)
	cls := classifier.New(cfg, nil, classifier.WithProcessPool(pool))
	cRes, err := cls.LLMFunc(ctx, "", "classify ambient message")
	if err != nil {
		t.Fatalf("unexpected error from cls.LLMFunc: %v", err)
	}
	if cRes != `{"confidence":0.9,"reasoning":"test"}` {
		t.Fatalf("unexpected cls.LLMFunc result: %q", cRes)
	}

	// 4. Scheduler integration with WithLLMFunc
	store := db.NewFakeStore()
	wPool := newTestWorkerPool(store)
	sched, err := scheduler.New(cfg, store, wPool, nil, scheduler.WithLLMFunc(summarizerLLM))
	if err != nil {
		t.Fatalf("failed to create scheduler: %v", err)
	}
	if sched == nil {
		t.Fatal("expected non-nil scheduler")
	}
}

func TestUnifiedPool_EphemeralLLMFuncErrors(t *testing.T) {
	mockSpawner := &runner.MockDaemonSpawner{
		SpawnFn: func(ctx context.Context, cfg runner.DaemonConfig) (io.WriteCloser, io.ReadCloser, io.ReadCloser, runner.ProcessHandle, error) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			errR, errW := io.Pipe()
			defer errW.Close()
			go func() {
				_, _ = fmt.Fprintf(outW, "{\"event\":\"init\",\"conversation_id\":\"00000000-0000-0000-0000-000000000002\"}\n")
				scanner := bufio.NewScanner(inR)
				for scanner.Scan() {
					_, _ = fmt.Fprintf(outW, "{\"event\":\"result\",\"result\":{\"status\":\"ERROR\",\"error\":\"daemon turn crashed\"}}\n")
				}
			}()
			return inW, outR, errR, runner.NewMockProcessHandle(12345), nil
		},
	}

	pool := runner.NewUnifiedProcessPool(runner.PoolConfig{
		DefaultModel: "gemini-2.5-flash",
	}, mockSpawner)
	defer pool.Close()

	llmFn := pool.EphemeralLLMFunc("ephemeral:classifier")
	_, err := llmFn(context.Background(), "", "classify this task")
	if err == nil || !strings.Contains(err.Error(), "daemon turn crashed") {
		t.Fatalf("expected 'daemon turn crashed' error, got: %v", err)
	}

	// Closed pool error
	closedPool := runner.NewUnifiedProcessPool(runner.PoolConfig{DefaultModel: "gemini-2.5-flash"}, mockSpawner)
	_ = closedPool.Close()
	closedLLM := closedPool.EphemeralLLMFunc("ephemeral:classifier")
	_, errClosed := closedLLM(context.Background(), "", "classify prompt")
	if errClosed == nil {
		t.Fatal("expected error from closed pool")
	}
}

func TestDualProcessPool_InitializationAndWiring(t *testing.T) {
	mockSpawner := runner.NewMockDaemonSpawner()
	tmpDir := t.TempDir()

	discordPrimaryPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
		GeminiHomeDir: filepath.Join(tmpDir, "runtimes", "discord"),
		Model:         "gemini-3.7-pro",
	}, mockSpawner)
	defer discordPrimaryPool.Close()

	discordLowEffortPool := runner.NewUnifiedProcessPool(runner.PoolConfig{
		GeminiHomeDir:    filepath.Join(tmpDir, "runtimes", "discord"),
		PrewarmedTargets: []string{"ephemeral:classifier", "ephemeral:summarizer"},
		Model:            "gemini-3.8-flash-low",
	}, mockSpawner)
	defer discordLowEffortPool.Close()

	voicePool := runner.NewUnifiedProcessPool(runner.PoolConfig{
		GeminiHomeDir:    filepath.Join(tmpDir, "runtimes", "voice"),
		PrewarmedTargets: []string{"kiosk"},
		Model:            "gemini-3.8-flash-low",
	}, mockSpawner)
	defer voicePool.Close()

	initCtx, initCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer initCancel()
	if err := discordPrimaryPool.Initialize(initCtx); err != nil {
		t.Fatalf("failed initializing discord primary pool: %v", err)
	}
	if err := discordLowEffortPool.Initialize(initCtx); err != nil {
		t.Fatalf("failed initializing discord low effort pool: %v", err)
	}
	if err := voicePool.Initialize(initCtx); err != nil {
		t.Fatalf("failed initializing voice pool: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if discordLowEffortPool.HasDaemon("ephemeral:classifier") &&
			discordLowEffortPool.HasDaemon("ephemeral:summarizer") &&
			voicePool.HasDaemon("kiosk") {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !discordLowEffortPool.HasDaemon("ephemeral:classifier") {
		t.Errorf("expected discordLowEffortPool to have ephemeral:classifier pre-warmed")
	}
	if !discordLowEffortPool.HasDaemon("ephemeral:summarizer") {
		t.Errorf("expected discordLowEffortPool to have ephemeral:summarizer pre-warmed")
	}
	if !voicePool.HasDaemon("kiosk") {
		t.Errorf("expected voicePool to have kiosk pre-warmed")
	}

	store := db.NewFakeStore()
	pool := queue.New(nil, queue.WorkerPoolConfig{
		Store:                store,
		ProcessPool:          discordPrimaryPool,
		LowEffortProcessPool: discordLowEffortPool,
		VoiceProcessPool:     voicePool,
	})
	pool.Start()

	if pool.ProcessPool() != discordPrimaryPool {
		t.Errorf("expected pool.ProcessPool() to be discordPrimaryPool")
	}
	if pool.LowEffortProcessPool() != discordLowEffortPool {
		t.Errorf("expected pool.LowEffortProcessPool() to be discordLowEffortPool")
	}
	if pool.VoiceProcessPool() != voicePool {
		t.Errorf("expected pool.VoiceProcessPool() to be voicePool")
	}

	pool.Stop()
}

func TestMain_DualProcessPoolWiring(t *testing.T) {
	primarySpawner := runner.NewMockSpawner()
	lowEffortSpawner := runner.NewMockSpawner()

	p1 := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.7-pro"}, primarySpawner)
	defer p1.Close()
	p2 := runner.NewUnifiedProcessPool(runner.PoolConfig{Model: "gemini-3.8-flash-low"}, lowEffortSpawner)
	defer p2.Close()

	if p1.Model() != "gemini-3.7-pro" || p2.Model() != "gemini-3.8-flash-low" {
		t.Fatalf("expected distinct models in dual pools")
	}
}

func TestVoicePool_FactoryWiring(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	voiceHome := filepath.Join(tmpDir, "voice")
	spawner := runner.NewMockSpawner()

	t.Run("GeminiAPI_WithHarnessAPIKey", func(t *testing.T) {
		t.Parallel()
		data := &config.ConfigData{
			HarnessAPIKey: "test-harness-key",
			Voice: config.VoiceConfig{
				Engine:           "gemini_api",
				Model:            "gemini-2.5-flash",
				PrewarmedTargets: []string{"kiosk"},
			},
			SystemPrompt: "Test System Prompt",
		}
		cfg := config.NewFromData(data)

		pool := createVoiceProcessPool(cfg, voiceHome, "fallback-model", spawner)
		defer pool.Close()

		dynPool, ok := pool.(*runner.DynamicVoicePool)
		if !ok {
			t.Fatalf("expected *runner.DynamicVoicePool, got %T", pool)
		}
		geminiPool, ok := dynPool.CurrentPool().(*runner.GeminiAPIPool)
		if !ok {
			t.Fatalf("expected *runner.GeminiAPIPool, got %T", dynPool.CurrentPool())
		}
		if dynPool.Model() != "gemini-2.5-flash" {
			t.Errorf("expected model 'gemini-2.5-flash', got %q", dynPool.Model())
		}
		if geminiPool.APIKey() != "test-harness-key" {
			t.Errorf("expected APIKey 'test-harness-key', got %q", geminiPool.APIKey())
		}
	})

	t.Run("GeminiAPI_WithAmbientContext", func(t *testing.T) {
		t.Parallel()
		data := &config.ConfigData{
			APIKey: "test-ambient-key",
			Voice: config.VoiceConfig{
				Engine: "gemini_api",
				AmbientContext: &config.AmbientContextConfig{
					CacheTTL: "30s",
					Template: "Status: {{.ha.state}}",
					Tools: []config.AmbientToolConfig{
						{Name: "ha", Tool: "get_state"},
					},
				},
			},
		}
		cfg := config.NewFromData(data)

		pool := createVoiceProcessPool(cfg, voiceHome, "fallback-model", spawner)
		defer pool.Close()

		dynPool, ok := pool.(*runner.DynamicVoicePool)
		if !ok {
			t.Fatalf("expected *runner.DynamicVoicePool, got %T", pool)
		}
		geminiPool, ok := dynPool.CurrentPool().(*runner.GeminiAPIPool)
		if !ok {
			t.Fatalf("expected *runner.GeminiAPIPool, got %T", dynPool.CurrentPool())
		}
		if geminiPool == nil {
			t.Fatalf("expected non-nil geminiPool")
		}
	})

	t.Run("GeminiAPI_WithAmbientContext_InvalidTemplate", func(t *testing.T) {
		t.Parallel()
		data := &config.ConfigData{
			APIKey: "test-ambient-key",
			Voice: config.VoiceConfig{
				Engine: "gemini_api",
				AmbientContext: &config.AmbientContextConfig{
					CacheTTL: "30s",
					Template: "{{.invalid template syntax",
				},
			},
		}
		cfg := config.NewFromData(data)
		pool := createVoiceProcessPool(cfg, voiceHome, "fallback-model", spawner)
		defer pool.Close()
	})

	t.Run("GeminiAPI_WithAPIKeyFallback", func(t *testing.T) {
		t.Parallel()
		data := &config.ConfigData{
			APIKey: "test-standard-key",
			Voice: config.VoiceConfig{
				Engine: "gemini_api",
			},
		}
		cfg := config.NewFromData(data)

		pool := createVoiceProcessPool(cfg, voiceHome, "fallback-model", spawner)
		defer pool.Close()

		dynPool, ok := pool.(*runner.DynamicVoicePool)
		if !ok {
			t.Fatalf("expected *runner.DynamicVoicePool, got %T", pool)
		}
		geminiPool, ok := dynPool.CurrentPool().(*runner.GeminiAPIPool)
		if !ok {
			t.Fatalf("expected *runner.GeminiAPIPool, got %T", dynPool.CurrentPool())
		}
		if dynPool.Model() != "fallback-model" {
			t.Errorf("expected fallback model 'fallback-model', got %q", dynPool.Model())
		}
		if geminiPool.APIKey() != "test-standard-key" {
			t.Errorf("expected APIKey 'test-standard-key', got %q", geminiPool.APIKey())
		}
	})

	t.Run("GeminiAPI_EmptyKeyFallbackToAgy", func(t *testing.T) {
		t.Parallel()
		data := &config.ConfigData{
			Voice: config.VoiceConfig{
				Engine: "gemini_api",
			},
		}
		cfg := config.NewFromData(data)

		pool := createVoiceProcessPool(cfg, voiceHome, "fallback-model", spawner)
		defer pool.Close()

		dynPool, ok := pool.(*runner.DynamicVoicePool)
		if !ok {
			t.Fatalf("expected *runner.DynamicVoicePool when API keys are empty, got %T", pool)
		}
		unifiedPool, ok := dynPool.CurrentPool().(*runner.UnifiedProcessPool)
		if !ok {
			t.Fatalf("expected *runner.UnifiedProcessPool when API keys are empty, got %T", dynPool.CurrentPool())
		}
		if dynPool.Model() != "fallback-model" {
			t.Errorf("expected model 'fallback-model', got %q", dynPool.Model())
		}
		if unifiedPool.Model() != "fallback-model" {
			t.Errorf("expected model 'fallback-model', got %q", unifiedPool.Model())
		}
	})

	t.Run("AgyEngine", func(t *testing.T) {
		t.Parallel()
		data := &config.ConfigData{
			HarnessAPIKey: "some-key",
			Voice: config.VoiceConfig{
				Engine: "agy",
			},
		}
		cfg := config.NewFromData(data)

		pool := createVoiceProcessPool(cfg, voiceHome, "fallback-model", spawner)
		defer pool.Close()

		dynPool, ok := pool.(*runner.DynamicVoicePool)
		if !ok {
			t.Fatalf("expected *runner.DynamicVoicePool for engine=agy, got %T", pool)
		}
		unifiedPool, ok := dynPool.CurrentPool().(*runner.UnifiedProcessPool)
		if !ok {
			t.Fatalf("expected *runner.UnifiedProcessPool for engine=agy, got %T", dynPool.CurrentPool())
		}
		if dynPool.Model() != "fallback-model" {
			t.Errorf("expected model 'fallback-model', got %q", dynPool.Model())
		}
		if unifiedPool.Model() != "fallback-model" {
			t.Errorf("expected model 'fallback-model', got %q", unifiedPool.Model())
		}
	})

	t.Run("Alias_createVoicePool", func(t *testing.T) {
		t.Parallel()
		data := &config.ConfigData{
			HarnessAPIKey: "key-123",
			Voice: config.VoiceConfig{
				Engine: "gemini_api",
			},
		}
		cfg := config.NewFromData(data)

		pool := createVoicePool(cfg, voiceHome, "model-xyz", spawner)
		defer pool.Close()

		dynPool, ok := pool.(*runner.DynamicVoicePool)
		if !ok {
			t.Fatalf("expected *runner.DynamicVoicePool from createVoicePool alias, got %T", pool)
		}
		if _, ok := dynPool.CurrentPool().(*runner.GeminiAPIPool); !ok {
			t.Fatalf("expected *runner.GeminiAPIPool from createVoicePool alias, got %T", dynPool.CurrentPool())
		}
	})

	t.Run("NilConfig", func(t *testing.T) {
		t.Parallel()
		pool := createVoiceProcessPool(nil, voiceHome, "fallback-model", spawner)
		defer pool.Close()

		dynPool, ok := pool.(*runner.DynamicVoicePool)
		if !ok {
			t.Fatalf("expected *runner.DynamicVoicePool for nil config, got %T", pool)
		}
		unifiedPool, ok := dynPool.CurrentPool().(*runner.UnifiedProcessPool)
		if !ok {
			t.Fatalf("expected *runner.UnifiedProcessPool for nil config, got %T", dynPool.CurrentPool())
		}
		if dynPool.Model() != "fallback-model" {
			t.Errorf("expected model 'fallback-model', got %q", dynPool.Model())
		}
		if unifiedPool.Model() != "fallback-model" {
			t.Errorf("expected model 'fallback-model', got %q", unifiedPool.Model())
		}
	})

	t.Run("DynamicHotReload_EngineSwitch", func(t *testing.T) {
		t.Parallel()
		data := &config.ConfigData{
			HarnessAPIKey: "harness-key",
			Voice: config.VoiceConfig{
				Engine: "agy",
				Model:  "agy-model",
			},
		}
		cfg := config.NewFromData(data)

		pool := createVoiceProcessPool(cfg, voiceHome, "fallback-model", spawner)
		defer pool.Close()

		dynPool, ok := pool.(*runner.DynamicVoicePool)
		if !ok {
			t.Fatalf("expected *runner.DynamicVoicePool, got %T", pool)
		}
		if _, ok := dynPool.CurrentPool().(*runner.UnifiedProcessPool); !ok {
			t.Fatalf("expected initial pool to be UnifiedProcessPool, got %T", dynPool.CurrentPool())
		}

		// Update config to gemini_api
		fresh := cfg.Current().Clone()
		fresh.Voice.Engine = "gemini_api"
		fresh.Voice.Model = "gemini-2.5-pro"
		cfg.Update(fresh)

		// Trigger MarkDirty on pool
		dynPool.MarkDirty()

		// Verify pool swapped to GeminiAPIPool with updated model
		geminiPool, ok := dynPool.CurrentPool().(*runner.GeminiAPIPool)
		if !ok {
			t.Fatalf("expected swapped pool to be GeminiAPIPool, got %T", dynPool.CurrentPool())
		}
		if geminiPool.Model() != "gemini-2.5-pro" {
			t.Errorf("expected model 'gemini-2.5-pro', got %q", geminiPool.Model())
		}
	})
}

func TestRunBrainApp_VoiceEngineGeminiAPI(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.MkdirAll(filepath.Join(tmpDir, ".gemini", "config", "skills"), 0755)

	cfg := config.NewTestConfig()
	cur := cfg.Current()
	cur.DataDir = tmpDir
	cur.GeminiHomeDir = tmpDir
	cur.DiscordToken = "mock-discord-token"
	cur.Port = "0"
	cur.AgyBin = "/bin/true"
	cur.HarnessAPIKey = "gemini-test-key"
	cur.Voice = config.VoiceConfig{
		Engine:           "gemini_api",
		Model:            "gemini-2.5-flash",
		PrewarmedTargets: []string{"kiosk"},
	}
	cfg.Update(cur)

	ready := make(chan struct{})
	oldReady := onServerReady
	onServerReady = func(addr string) {
		close(ready)
	}
	defer func() { onServerReady = oldReady }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-ready
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()

	mockStore := db.NewFakeStore()
	err := RunBrainApp(ctx, cfg, WithStore(mockStore), WithProcessSpawner(runner.NewMockDaemonSpawner()))
	if err != nil {
		t.Fatalf("RunBrainApp with gemini_api voice engine failed: %v", err)
	}
}

func TestExtractVoiceMCPServers(t *testing.T) {
	t.Parallel()

	// 1. Nil config
	nilResult := extractVoiceMCPServers(nil)
	if len(nilResult) != 1 || nilResult[0].Name != "scheduler" {
		t.Fatalf("expected scheduler default for nil config, got: %+v", nilResult)
	}

	// 2. Populated config with Common and Voice servers
	cur := &config.ConfigData{
		McpServers: config.TargetMcpConfig{
			Common: map[string]json.RawMessage{
				"ha-mcp": json.RawMessage(`{"serverUrl":"http://homeassistant:8123/api/webhook/test","headers":{"Authorization":"Bearer secret"}}`),
			},
			Voice: map[string]json.RawMessage{
				"kiosk-mcp": json.RawMessage(`{"serverUrl":"http://kiosk:4000/mcp"}`),
			},
			Discord: map[string]json.RawMessage{
				"discord-only": json.RawMessage(`{"serverUrl":"http://discord:4000/mcp"}`),
			},
		},
	}

	servers := extractVoiceMCPServers(cur)
	serverMap := make(map[string]runner.MCPServerConfig)
	for _, s := range servers {
		serverMap[s.Name] = s
	}

	if len(serverMap) != 3 {
		t.Fatalf("expected 3 voice servers (scheduler, ha-mcp, kiosk-mcp), got %d: %+v", len(serverMap), servers)
	}

	if _, ok := serverMap["scheduler"]; !ok {
		t.Errorf("missing scheduler in voice servers")
	}
	if ha, ok := serverMap["ha-mcp"]; !ok {
		t.Errorf("missing ha-mcp in voice servers")
	} else {
		if ha.ServerURL != "http://homeassistant:8123/api/webhook/test" {
			t.Errorf("unexpected ServerURL for ha-mcp: %q", ha.ServerURL)
		}
		if ha.Headers["Authorization"] != "Bearer secret" {
			t.Errorf("unexpected headers for ha-mcp: %+v", ha.Headers)
		}
	}
	if kiosk, ok := serverMap["kiosk-mcp"]; !ok {
		t.Errorf("missing kiosk-mcp in voice servers")
	} else if kiosk.ServerURL != "http://kiosk:4000/mcp" {
		t.Errorf("unexpected ServerURL for kiosk-mcp: %q", kiosk.ServerURL)
	}
	if _, ok := serverMap["discord-only"]; ok {
		t.Errorf("discord-only server should not be in voice servers")
	}
}

func TestExtractAllMCPServers(t *testing.T) {
	t.Parallel()

	// 1. Nil config
	nilResult := extractAllMCPServers(nil)
	if len(nilResult) != 1 || nilResult[0].Name != "scheduler" {
		t.Fatalf("expected scheduler default for nil config, got: %+v", nilResult)
	}

	// 2. Populated config with Common, Discord, and Voice servers
	cur := &config.ConfigData{
		McpServers: config.TargetMcpConfig{
			Common: map[string]json.RawMessage{
				"ha-mcp": json.RawMessage(`{"serverUrl":"http://homeassistant:8123/api/webhook/test","headers":{"Authorization":"Bearer secret"}}`),
			},
			Voice: map[string]json.RawMessage{
				"kiosk-mcp": json.RawMessage(`{"serverUrl":"http://kiosk:4000/mcp"}`),
			},
			Discord: map[string]json.RawMessage{
				"discord-only": json.RawMessage(`{"serverUrl":"http://discord:4000/mcp"}`),
			},
		},
	}

	servers := extractAllMCPServers(cur)
	serverMap := make(map[string]runner.MCPServerConfig)
	for _, s := range servers {
		serverMap[s.Name] = s
	}

	if len(serverMap) != 4 {
		t.Fatalf("expected 4 servers (scheduler, ha-mcp, kiosk-mcp, discord-only), got %d: %+v", len(serverMap), servers)
	}
	if _, ok := serverMap["scheduler"]; !ok {
		t.Errorf("missing scheduler")
	}
	if _, ok := serverMap["ha-mcp"]; !ok {
		t.Errorf("missing ha-mcp")
	}
	if _, ok := serverMap["kiosk-mcp"]; !ok {
		t.Errorf("missing kiosk-mcp")
	}
	if _, ok := serverMap["discord-only"]; !ok {
		t.Errorf("missing discord-only")
	}
}

func TestBuildPoolEnv(t *testing.T) {
	t.Parallel()

	t.Run("EmptyRuntimeBaseReturnsBaseEnv", func(t *testing.T) {
		t.Parallel()
		base := []string{"FOO=bar", "BAZ=qux"}
		res := buildPoolEnv(base, "")
		if len(res) != 2 || res[0] != "FOO=bar" || res[1] != "BAZ=qux" {
			t.Errorf("expected unmodified env, got %v", res)
		}
	})

	t.Run("InjectsUnifiedCacheDirectories", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		base := []string{"EXISTING=1"}
		res := buildPoolEnv(base, tmpDir)

		envMap := make(map[string]string)
		for _, e := range res {
			if k, v, ok := strings.Cut(e, "="); ok {
				envMap[k] = v
			}
		}

		expectedGoCache := filepath.Join(tmpDir, "cache", "go-build")
		expectedGoPath := filepath.Join(tmpDir, "cache", "go")
		expectedGoModCache := filepath.Join(tmpDir, "cache", "go", "pkg", "mod")
		expectedLintCache := filepath.Join(tmpDir, "cache", "golangci-lint")

		if envMap["GOCACHE"] != expectedGoCache {
			t.Errorf("expected GOCACHE=%q, got %q", expectedGoCache, envMap["GOCACHE"])
		}
		if envMap["GOPATH"] != expectedGoPath {
			t.Errorf("expected GOPATH=%q, got %q", expectedGoPath, envMap["GOPATH"])
		}
		if envMap["GOMODCACHE"] != expectedGoModCache {
			t.Errorf("expected GOMODCACHE=%q, got %q", expectedGoModCache, envMap["GOMODCACHE"])
		}
		if envMap["GOLANGCI_LINT_CACHE"] != expectedLintCache {
			t.Errorf("expected GOLANGCI_LINT_CACHE=%q, got %q", expectedLintCache, envMap["GOLANGCI_LINT_CACHE"])
		}
		if envMap["EXISTING"] != "1" {
			t.Errorf("expected EXISTING=1 to be preserved, got %q", envMap["EXISTING"])
		}

		// Verify directories were created
		for _, d := range []string{expectedGoCache, expectedGoModCache, expectedLintCache} {
			if fi, err := os.Stat(d); err != nil || !fi.IsDir() {
				t.Errorf("expected directory %q to exist, err: %v", d, err)
			}
		}
	})

	t.Run("PreservesExplicitCacheEnv", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		base := []string{
			"GOCACHE=/custom/gocache",
			"GOPATH=/custom/gopath",
			"GOMODCACHE=/custom/modcache",
			"GOLANGCI_LINT_CACHE=/custom/lintcache",
		}
		res := buildPoolEnv(base, tmpDir)

		envMap := make(map[string]string)
		for _, e := range res {
			if k, v, ok := strings.Cut(e, "="); ok {
				envMap[k] = v
			}
		}

		if envMap["GOCACHE"] != "/custom/gocache" {
			t.Errorf("expected custom GOCACHE, got %q", envMap["GOCACHE"])
		}
		if envMap["GOPATH"] != "/custom/gopath" {
			t.Errorf("expected custom GOPATH, got %q", envMap["GOPATH"])
		}
		if envMap["GOMODCACHE"] != "/custom/modcache" {
			t.Errorf("expected custom GOMODCACHE, got %q", envMap["GOMODCACHE"])
		}
		if envMap["GOLANGCI_LINT_CACHE"] != "/custom/lintcache" {
			t.Errorf("expected custom GOLANGCI_LINT_CACHE, got %q", envMap["GOLANGCI_LINT_CACHE"])
		}
	})

	t.Run("InjectsConfiguredTimezone", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		base := []string{"EXISTING=1"}
		res := buildPoolEnv(base, tmpDir, "America/Los_Angeles")

		envMap := make(map[string]string)
		for _, e := range res {
			if k, v, ok := strings.Cut(e, "="); ok {
			envMap[k] = v
			}
		}

		if envMap["TZ"] != "America/Los_Angeles" {
			t.Errorf("expected TZ=America/Los_Angeles, got %q", envMap["TZ"])
		}
	})

	t.Run("PreservesExplicitTZ", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		base := []string{"TZ=UTC"}
		res := buildPoolEnv(base, tmpDir, "America/Los_Angeles")

		envMap := make(map[string]string)
		for _, e := range res {
			if k, v, ok := strings.Cut(e, "="); ok {
			envMap[k] = v
			}
		}

		if envMap["TZ"] != "UTC" {
			t.Errorf("expected explicit TZ=UTC to be preserved, got %q", envMap["TZ"])
		}
	})
}

type mockSequentialPool struct {
	name       string
	initCalled bool
	onInit     func(name string)
}

func (m *mockSequentialPool) GetOrCreateSession(ctx context.Context, targetKey string, sessionID string) (runner.AgentSession, error) {
	return nil, nil
}

func (m *mockSequentialPool) Initialize(ctx context.Context) error {
	m.initCalled = true
	if m.onInit != nil {
		m.onInit(m.name)
	}
	return nil
}

func (m *mockSequentialPool) Close() error {
	return nil
}

func TestPrewarmProcessPoolsSequentially(t *testing.T) {
	var mu sync.Mutex
	var order []string

	p1 := &mockSequentialPool{
		name: "pool1",
		onInit: func(name string) {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		},
	}
	p2 := &mockSequentialPool{
		name: "pool2",
		onInit: func(name string) {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		},
	}
	p3 := &mockSequentialPool{
		name: "pool3",
		onInit: func(name string) {
			mu.Lock()
			order = append(order, name)
			mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	PrewarmProcessPoolsSequentially(ctx, p1, nil, p2, p3)

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		count := len(order)
		mu.Unlock()
		if count == 3 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()

	if len(order) != 3 {
		t.Fatalf("expected 3 pools initialized, got %d (%v)", len(order), order)
	}
	if order[0] != "pool1" || order[1] != "pool2" || order[2] != "pool3" {
		t.Errorf("expected sequential order [pool1, pool2, pool3], got %v", order)
	}
}

func TestPrewarmProcessPoolsSequentially_CancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled upfront

	p1 := &mockSequentialPool{name: "p1"}
	PrewarmProcessPoolsSequentially(ctx, p1)

	time.Sleep(15 * time.Millisecond)
}

func TestHandlePRRegister(t *testing.T) {
	store := db.NewFakeStore()
	handler := handlePRRegister(store)

	// 1. Method Not Allowed
	{
		req := httptest.NewRequest(http.MethodGet, "/internal/pr/register", nil)
		rec := httptest.NewRecorder()
		handler(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Errorf("Expected 405 Method Not Allowed, got %d", rec.Code)
		}
	}

	// 2. Nil store returns 500
	{
		nilHandler := handlePRRegister(nil)
		req := httptest.NewRequest(http.MethodPost, "/internal/pr/register", strings.NewReader(`{}`))
		rec := httptest.NewRecorder()
		nilHandler(rec, req)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("Expected 500 when store is nil, got %d", rec.Code)
		}
	}

	// 3. Invalid JSON
	{
		req := httptest.NewRequest(http.MethodPost, "/internal/pr/register", strings.NewReader(`{invalid-json`))
		rec := httptest.NewRecorder()
		handler(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("Expected 400 Bad Request on invalid JSON, got %d", rec.Code)
		}
	}

	// 4. Validation errors
	testCases := []struct {
		name    string
		payload string
	}{
		{"empty repo", `{"repo":"","pr_number":1,"branch":"feat","head_sha":"sha","target_id":"1555405874565091380"}`},
		{"zero pr_number", `{"repo":"aerial","pr_number":0,"branch":"feat","head_sha":"sha","target_id":"1555405874565091380"}`},
		{"empty branch", `{"repo":"aerial","pr_number":1,"branch":"","head_sha":"sha","target_id":"1555405874565091380"}`},
		{"empty head_sha", `{"repo":"aerial","pr_number":1,"branch":"feat","head_sha":"","target_id":"1555405874565091380"}`},
		{"empty target_id", `{"repo":"aerial","pr_number":1,"branch":"feat","head_sha":"sha","target_id":""}`},
		{"too short target_id", `{"repo":"aerial","pr_number":1,"branch":"feat","head_sha":"sha","target_id":"12345"}`},
		{"too long target_id", `{"repo":"aerial","pr_number":1,"branch":"feat","head_sha":"sha","target_id":"123456789012345678901"}`},
		{"non-numeric target_id", `{"repo":"aerial","pr_number":1,"branch":"feat","head_sha":"sha","target_id":"155540587456509138A"}`},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/internal/pr/register", strings.NewReader(tc.payload))
			rec := httptest.NewRecorder()
			handler(rec, req)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("[%s] Expected 400 Bad Request, got %d (%s)", tc.name, rec.Code, rec.Body.String())
			}
		})
	}

	// 5. Successful Registration
	{
		payload := `{"repo":"aerial","pr_number":508,"branch":"feat/push-pipeline","head_sha":"head1234","target_id":"1555405874565091380","title":"feat: push pipeline"}`
		req := httptest.NewRequest(http.MethodPost, "/internal/pr/register", strings.NewReader(payload))
		rec := httptest.NewRecorder()
		handler(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("Expected 200 OK, got %d (%s)", rec.Code, rec.Body.String())
		}

		var resp map[string]interface{}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("Failed to decode response JSON: %v", err)
		}
		if resp["status"] != "registered" || resp["repo"] != "aerial" || resp["target_id"] != "1555405874565091380" {
			t.Errorf("Unexpected response payload: %+v", resp)
		}

		// Verify persisted in store
		recInDB, err := store.GetPRByNumber(context.Background(), "aerial", 508)
		if err != nil {
			t.Fatalf("Failed to retrieve registered PR from store: %v", err)
		}
		if recInDB.Branch != "feat/push-pipeline" || recInDB.HeadSHA != "head1234" || recInDB.Title != "feat: push pipeline" {
			t.Errorf("Stored PR mismatch: %+v", recInDB)
		}
	}

	// 6. DB failure returns 500
	{
		store.FailNext("UpsertPR", errors.New("simulated DB failure"))
		payload := `{"repo":"aerial","pr_number":509,"branch":"feat/push-pipeline","head_sha":"head5678","target_id":"1555405874565091380"}`
		req := httptest.NewRequest(http.MethodPost, "/internal/pr/register", strings.NewReader(payload))
		rec := httptest.NewRecorder()
		handler(rec, req)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("Expected 500 on store failure, got %d", rec.Code)
		}
	}
}

func TestCoverageFlushEndpoint(t *testing.T) {
	mux := SetupBrainMuxWithEmbedder(nil, nil, nil)
	ts := httptest.NewServer(mux)
	defer ts.Close()

	// 1. Without GOCOVERDIR -> 200 OK
	orig := os.Getenv("GOCOVERDIR")
	_ = os.Unsetenv("GOCOVERDIR")
	defer func() {
		if orig != "" {
			_ = os.Setenv("GOCOVERDIR", orig)
		} else {
			_ = os.Unsetenv("GOCOVERDIR")
		}
	}()

	resp, err := http.Get(ts.URL + "/debug/coverage/flush")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK without GOCOVERDIR, got: %d", resp.StatusCode)
	}

	// 2. With GOCOVERDIR + mock success -> 200 OK
	oldFn := writeCountersDirFn
	defer func() { writeCountersDirFn = oldFn }()

	called := false
	writeCountersDirFn = func(dir string) error {
		called = true
		return nil
	}
	_ = os.Setenv("GOCOVERDIR", t.TempDir())

	resp2, err2 := http.Get(ts.URL + "/debug/coverage/flush")
	if err2 != nil {
		t.Fatalf("unexpected error: %v", err2)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK with GOCOVERDIR, got: %d", resp2.StatusCode)
	}
	if !called {
		t.Errorf("expected writeCountersDirFn to be called")
	}

	// 3. With GOCOVERDIR + mock error -> 500 Internal Server Error
	writeCountersDirFn = func(dir string) error {
		return errors.New("simulated flush error")
	}

	resp3, err3 := http.Get(ts.URL + "/debug/coverage/flush")
	if err3 != nil {
		t.Fatalf("unexpected error: %v", err3)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusInternalServerError {
		t.Errorf("expected 500 on error, got: %d", resp3.StatusCode)
	}
}





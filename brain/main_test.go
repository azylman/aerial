package main

import (
	"bytes"
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
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/azylman/aerial/brain/pkg/config"
	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/discord"
	"github.com/azylman/aerial/brain/pkg/env"
	"github.com/azylman/aerial/brain/pkg/metrics"
	"github.com/azylman/aerial/brain/pkg/queue"
	"github.com/azylman/aerial/brain/pkg/runner"
	"github.com/azylman/aerial/brain/pkg/transcript"
	"github.com/bwmarrin/discordgo"
	_ "modernc.org/sqlite"
)

func TestHandlePromptValidation(t *testing.T) {
	store := db.NewFakeStore()
	pool := newTestWorkerPool(store)
	pool.Start()
	defer pool.Stop()

	closedStore := db.NewFakeStore()
	_ = closedStore.Close()

	for _, tc := range []struct {
		name       string
		method     string
		store      db.Store
		pool       *queue.WorkerPool
		body       io.Reader
		wantStatus int
		wantSubstr string
	}{
		{"method not allowed", http.MethodGet, store, pool, nil, http.StatusMethodNotAllowed, "Method not allowed"},
		{"read error", http.MethodPost, store, pool, &errReadCloser{readErr: errors.New("simulated network read error")}, http.StatusBadRequest, "Failed to read request body"},
		{"invalid JSON", http.MethodPost, store, pool, strings.NewReader("{invalid-json"), http.StatusBadRequest, "Invalid payload: 'prompt' field is required and cannot be empty"},
		{"empty prompt", http.MethodPost, store, pool, strings.NewReader(`{"prompt":""}`), http.StatusBadRequest, "Invalid payload: 'prompt' field is required and cannot be empty"},
		{"whitespace prompt", http.MethodPost, store, pool, strings.NewReader(`{"prompt":"   "}`), http.StatusBadRequest, "Invalid payload: 'prompt' field is required and cannot be empty"},
		{"missing channel_id", http.MethodPost, store, pool, strings.NewReader(`{"prompt":"hello"}`), http.StatusBadRequest, "Invalid payload: 'channel_id' field is required and cannot be empty"},
		{"non-snowflake channel_id", http.MethodPost, store, pool, strings.NewReader(`{"prompt":"hello","channel_id":"not-a-snowflake"}`), http.StatusBadRequest, "Invalid payload: 'channel_id' must be a valid numeric Discord snowflake"},
		{"nil store", http.MethodPost, nil, pool, strings.NewReader(`{"prompt":"hello","channel_id":"1542423172400291873"}`), http.StatusInternalServerError, "Database store not configured"},
		{"closed store", http.MethodPost, closedStore, pool, strings.NewReader(`{"prompt":"hello","channel_id":"1542423172400291873"}`), http.StatusInternalServerError, "Failed to persist prompt message"},
		{"body close error", http.MethodPost, store, pool, &errReadCloser{reader: strings.NewReader(`{"prompt":"hello","channel_id":"1542423172400291873"}`), closeErr: errors.New("close error")}, http.StatusAccepted, "channel_id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := handlePrompt(tc.store, tc.pool)
			req := httptest.NewRequest(tc.method, "/prompt", tc.body)
			w := httptest.NewRecorder()
			handler(w, req)
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
		})
	}

	for _, tc := range []struct {
		name        string
		pool        *queue.WorkerPool
		payload     string
		msgID       string
		wantEffort  string
		wantModel   string
		wantSilent  bool
		wantChunkID string
		wantContent string
	}{
		{
			name:        "default effort",
			pool:        pool,
			payload:     `{"prompt":"Test valid prompt","channel_id":"1542423172400291873","message_id":"msg-123"}`,
			msgID:       "msg-123",
			wantContent: "Test valid prompt",
		},
		{
			name:       "gemini-4-flash silent",
			pool:       pool,
			payload:    `{"prompt":"Execute chunk 1","channel_id":"1542423172400291873","message_id":"msg-silent-1","model":"gemini-4-flash","silent":true}`,
			msgID:      "msg-silent-1",
			wantEffort: "low",
			wantModel:  "gemini-4-flash",
			wantSilent: true,
		},
		{
			name:       "gemini-3.8-flash-medium",
			pool:       pool,
			payload:    `{"prompt":"Execute medium chunk","channel_id":"1542423172400291873","message_id":"msg-med-1","model":"gemini-3.8-flash-medium","silent":true}`,
			msgID:      "msg-med-1",
			wantEffort: "medium",
			wantModel:  "gemini-3.8-flash-medium",
			wantSilent: true,
		},
		{
			name:        "vanadis_chunk_id nil pool",
			pool:        nil,
			payload:     `{"prompt":"hello nil pool","channel_id":"1542423172400291873","message_id":"msg-vanadis-chunk","vanadis_chunk_id":"chunk-abc-99"}`,
			msgID:       "msg-vanadis-chunk",
			wantChunkID: "chunk-abc-99",
		},
		{
			name:        "auto-generated uuid when message_id omitted",
			pool:        pool,
			payload:     `{"prompt":"auto uuid prompt","channel_id":"1542423172400291873"}`,
			wantContent: "auto uuid prompt",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := handlePrompt(store, tc.pool)
			req := httptest.NewRequest(http.MethodPost, "/prompt", strings.NewReader(tc.payload))
			w := httptest.NewRecorder()
			handler(w, req)
			if w.Code != http.StatusAccepted {
				t.Fatalf("got status=%d body=%q, want 202", w.Code, w.Body.String())
			}
			if tc.msgID != "" {
				msg, err := store.GetMessage(context.Background(), tc.msgID)
				if err != nil || msg == nil {
					t.Fatalf("failed to retrieve persisted message %s: %v", tc.msgID, err)
				}
				if tc.wantContent != "" && msg.Content != tc.wantContent {
					t.Errorf("content=%q, want %q", msg.Content, tc.wantContent)
				}
				if tc.wantEffort != "" && msg.Effort != tc.wantEffort {
					t.Errorf("effort=%q, want %q", msg.Effort, tc.wantEffort)
				}
				if tc.wantModel != "" && msg.Metadata.Model != tc.wantModel {
					t.Errorf("model=%q, want %q", msg.Metadata.Model, tc.wantModel)
				}
				if tc.wantSilent && !msg.Metadata.Silent {
					t.Errorf("expected silent=true, got %v", msg.Metadata.Silent)
				}
				if tc.wantChunkID != "" && msg.Metadata.VanadisChunkID != tc.wantChunkID {
					t.Errorf("chunkID=%q, want %q", msg.Metadata.VanadisChunkID, tc.wantChunkID)
				}
			}
		})
	}
}

func TestHandleDirectMessage(t *testing.T) {
	for _, tc := range []struct {
		name       string
		method     string
		pool       *queue.WorkerPool
		body       io.Reader
		wantStatus int
		wantSubstr string
	}{
		{"method not allowed", http.MethodGet, &queue.WorkerPool{}, nil, http.StatusMethodNotAllowed, "Method not allowed"},
		{"nil pool", http.MethodPost, nil, strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`), http.StatusInternalServerError, "Worker pool not available"},
		{"read and close error", http.MethodPost, &queue.WorkerPool{}, &errReadCloser{readErr: errors.New("read err"), closeErr: errors.New("close err")}, http.StatusBadRequest, "Failed to read request body"},
		{"invalid JSON", http.MethodPost, &queue.WorkerPool{}, strings.NewReader("{invalid-json"), http.StatusBadRequest, "Invalid JSON payload"},
		{"missing channel_id", http.MethodPost, &queue.WorkerPool{}, strings.NewReader(`{"content":"hello"}`), http.StatusBadRequest, "Invalid payload: 'channel_id' field is required and cannot be empty"},
		{"non-snowflake channel_id", http.MethodPost, &queue.WorkerPool{}, strings.NewReader(`{"channel_id":"not-a-snowflake","content":"hello"}`), http.StatusBadRequest, "Invalid payload: 'channel_id' must be a valid numeric Discord snowflake"},
		{"empty content", http.MethodPost, &queue.WorkerPool{}, strings.NewReader(`{"channel_id":"1542423172400291873","content":"  "}`), http.StatusBadRequest, "Invalid payload: 'content' field is required and cannot be empty"},
		{"discord session not connected", http.MethodPost, &queue.WorkerPool{}, strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`), http.StatusServiceUnavailable, "Discord session not connected"},
		{"discord client RESTError", http.MethodPost, queue.NewWorkerPool(queue.WorkerPoolConfig{
			DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
				return &discordgo.RESTError{Response: &http.Response{StatusCode: 403}}
			},
		}), strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`), http.StatusForbidden, "Discord client error"},
		{"discord client cannot send", http.MethodPost, queue.NewWorkerPool(queue.WorkerPoolConfig{
			DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
				return errors.New("cannot send messages to this thread")
			},
		}), strings.NewReader(`{"channel_id":"1542423172400291873","content":"hello"}`), http.StatusBadRequest, "Discord client error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := handleDirectMessage(tc.pool)
			req := httptest.NewRequest(tc.method, "/discord/message", tc.body)
			w := httptest.NewRecorder()
			handler(w, req)
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
		})
	}

	t.Run("delivery success and fallback", func(t *testing.T) {
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

	t.Run("mux route mounting for /discord/message", func(t *testing.T) {
		pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
			DeliveryFunc: func(s *discordgo.Session, channelID, text string) error {
				return nil
			},
		})
		mux := SetupBrainMuxWithEmbedder(db.NewFakeStore(), pool, nil)

		req := httptest.NewRequest(http.MethodPost, "/discord/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"test"}`))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Errorf("expected 200 for /discord/message, got %d (body: %s)", w.Code, w.Body.String())
		}

		reqInternal := httptest.NewRequest(http.MethodPost, "/internal/message", strings.NewReader(`{"channel_id":"1542423172400291873","content":"test"}`))
		wInternal := httptest.NewRecorder()
		mux.ServeHTTP(wInternal, reqInternal)
		if wInternal.Code != http.StatusNotFound {
			t.Errorf("expected 404 for dropped /internal/message, got %d", wInternal.Code)
		}
	})
}

func TestHandleStatusWidget(t *testing.T) {
	for _, tc := range []struct {
		name       string
		method     string
		pool       *queue.WorkerPool
		body       string
		wantStatus int
		wantSubstr string
	}{
		{"method not allowed", http.MethodGet, &queue.WorkerPool{}, "", http.StatusMethodNotAllowed, "Method not allowed"},
		{"nil pool", http.MethodPost, nil, `{"channel_id":"1542423172400291873","chunks":[{"title":"task"}]}`, http.StatusInternalServerError, "Worker pool not available"},
		{"invalid JSON", http.MethodPost, &queue.WorkerPool{}, "{invalid-json", http.StatusBadRequest, "Invalid JSON payload"},
		{"missing channel_id", http.MethodPost, &queue.WorkerPool{}, `{"chunks":[{"title":"task"}]}`, http.StatusBadRequest, "Invalid payload: 'channel_id' field is required and cannot be empty"},
		{"non-snowflake channel_id", http.MethodPost, &queue.WorkerPool{}, `{"channel_id":"invalid-snow","chunks":[{"title":"task"}]}`, http.StatusBadRequest, "Invalid payload: 'channel_id' must be a valid numeric Discord snowflake"},
		{"empty chunks", http.MethodPost, &queue.WorkerPool{}, `{"channel_id":"1542423172400291873","chunks":[]}`, http.StatusBadRequest, "Invalid payload: 'chunks' field is required and cannot be empty"},
		{"discord session not connected", http.MethodPost, &queue.WorkerPool{}, `{"channel_id":"1542423172400291873","chunks":[{"title":"task"}]}`, http.StatusServiceUnavailable, "Discord session not connected"},
		{"discord client RESTError", http.MethodPost, queue.NewWorkerPool(queue.WorkerPoolConfig{
			WidgetDeliveryFunc: func(req discord.StatusWidgetRequest) (string, error) {
				return "", &discordgo.RESTError{Response: &http.Response{StatusCode: 403}}
			},
		}), `{"channel_id":"1542423172400291873","chunks":[{"title":"task"}]}`, http.StatusForbidden, "Discord client error"},
		{"discord client cannot send", http.MethodPost, queue.NewWorkerPool(queue.WorkerPoolConfig{
			WidgetDeliveryFunc: func(req discord.StatusWidgetRequest) (string, error) {
				return "", errors.New("cannot send messages to this thread")
			},
		}), `{"channel_id":"1542423172400291873","chunks":[{"title":"task"}]}`, http.StatusBadRequest, "Discord client error"},
		{"generic delivery failure", http.MethodPost, queue.NewWorkerPool(queue.WorkerPoolConfig{
			WidgetDeliveryFunc: func(req discord.StatusWidgetRequest) (string, error) {
				return "", errors.New("database connection reset")
			},
		}), `{"channel_id":"1542423172400291873","chunks":[{"title":"task"}]}`, http.StatusInternalServerError, "Failed to deliver widget"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := handleStatusWidget(tc.pool)
			var r io.Reader
			if tc.body != "" {
				r = strings.NewReader(tc.body)
			}
			req := httptest.NewRequest(tc.method, "/discord/widget", r)
			w := httptest.NewRecorder()
			handler(w, req)
			if w.Code != tc.wantStatus || !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Fatalf("got status=%d body=%q, want status=%d substr=%q", w.Code, w.Body.String(), tc.wantStatus, tc.wantSubstr)
			}
		})
	}

	t.Run("success returns 200 with message_id and delivers widget", func(t *testing.T) {
		var capturedReq discord.StatusWidgetRequest
		pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
			WidgetDeliveryFunc: func(req discord.StatusWidgetRequest) (string, error) {
				capturedReq = req
				return "created-widget-msg-789", nil
			},
		})
		handler := handleStatusWidget(pool)

		payload := `{
			"channel_id": "1542423172400291873",
			"id": "build-123",
			"title": "Build Title",
			"status": "running",
			"chunks": [
				{"index": 1, "repo": "aerial", "title": "Chunk 1", "status": "completed"}
			]
		}`
		req := httptest.NewRequest(http.MethodPost, "/discord/widget", strings.NewReader(payload))
		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d (body: %s)", w.Code, w.Body.String())
		}

		var resp map[string]interface{}
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response JSON: %v", err)
		}
		if resp["status"] != "ok" || resp["message_id"] != "created-widget-msg-789" {
			t.Fatalf("unexpected response body: %+v", resp)
		}
		if capturedReq.ChannelID != "1542423172400291873" || capturedReq.ID != "build-123" {
			t.Fatalf("unexpected captured request: %+v", capturedReq)
		}
	})

	t.Run("mux route mounting for /discord/widget", func(t *testing.T) {
		pool := queue.NewWorkerPool(queue.WorkerPoolConfig{
			WidgetDeliveryFunc: func(req discord.StatusWidgetRequest) (string, error) {
				return "widget-99", nil
			},
		})
		mux := SetupBrainMuxWithEmbedder(db.NewFakeStore(), pool, nil)

		req := httptest.NewRequest(http.MethodPost, "/discord/widget", strings.NewReader(`{"channel_id":"1542423172400291873","chunks":[{"title":"t"}]}`))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Errorf("expected 200 for /discord/widget, got %d", w.Code)
		}
	})
}

func TestHandleTranscripts(t *testing.T) {
	store := db.NewFakeStore()
	tmpHome := t.TempDir()

	// 1. Empty dir returns []
	handler := handleTranscripts(store, tmpHome)
	req := httptest.NewRequest(http.MethodGet, "/transcripts", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != "[]" {
		t.Fatalf("Expected 200 with empty array, got %d: %q", w.Code, w.Body.String())
	}

	// 2. Transcript file with various errors and include_raw=true
	_ = store.SaveConversationMapping(context.Background(), "thread-discord-100", "conv-alpha-100")
	brainDir := filepath.Join(tmpHome, "brain")
	convDir := filepath.Join(brainDir, "conv-alpha-100", ".system_generated", "logs")
	if err := os.MkdirAll(convDir, 0755); err != nil {
		t.Fatalf("MkdirAll failed: %v", err)
	}
	transcriptContent := "{\"step_index\": 1, \"status\": \"DONE\", \"content\": \"hello\"}\n" +
		"{\"step_index\": 2, \"status\": \"ERROR\", \"error\": \"something failed\"}\n" +
		"{\"step_index\": 3, \"status\": \"RUNNING\", \"error\": {\"code\": 500, \"message\": \"object error\"}}\n" +
		"{\"step_index\": 4, \"status\": \"RUNNING\", \"error\": 99999}\n" +
		"not a valid json line\n"
	if err := os.WriteFile(filepath.Join(convDir, "transcript.jsonl"), []byte(transcriptContent), 0644); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	// Include unreadable folder (missing transcript.jsonl) and non-directory file
	_ = os.MkdirAll(filepath.Join(brainDir, "conv-no-transcript", ".system_generated", "logs"), 0755)
	_ = os.WriteFile(filepath.Join(brainDir, "regular_file.txt"), []byte("not a dir"), 0644)

	handler2 := handleTranscripts(store, brainDir, "", "   ", "/nonexistent/dir")
	reqGet := httptest.NewRequest(http.MethodGet, "/transcripts?include_raw=true", nil)
	wGet := httptest.NewRecorder()
	handler2(wGet, reqGet)
	if wGet.Code != http.StatusOK {
		t.Fatalf("Expected 200 OK, got %d", wGet.Code)
	}
	bodyStr := wGet.Body.String()
	if !strings.Contains(bodyStr, "conv-alpha-100") || !strings.Contains(bodyStr, "99999") {
		t.Errorf("Expected conv-alpha-100 and 99999 in response: %s", bodyStr)
	}

	// 3. Closed DB error branch (GetExternalConversationID warning branch)
	closedStore := db.NewFakeStore()
	_ = closedStore.Close()
	handlerClosed := handleTranscripts(closedStore, brainDir)
	wClosed := httptest.NewRecorder()
	handlerClosed(wClosed, req)
	if wClosed.Code != http.StatusOK {
		t.Errorf("Expected 200 OK on closed DB warning branch, got %d", wClosed.Code)
	}

	// 4. Multi-root deduplication
	root1 := filepath.Join(tmpHome, "root1")
	root2 := filepath.Join(tmpHome, "root2")
	dir1 := filepath.Join(root1, "shared-conv-1", ".system_generated", "logs")
	dir2 := filepath.Join(root2, "shared-conv-1", ".system_generated", "logs")
	_ = os.MkdirAll(dir1, 0755)
	_ = os.MkdirAll(dir2, 0755)
	_ = os.WriteFile(filepath.Join(dir1, "transcript.jsonl"), []byte(`{"step_index":1,"status":"DONE"}`+"\n"), 0644)
	_ = os.WriteFile(filepath.Join(dir2, "transcript.jsonl"), []byte(`{"step_index":1,"status":"DONE"}`+"\n"), 0644)

	handlerDedup := handleTranscripts(store, root1, root2)
	wDedup := httptest.NewRecorder()
	handlerDedup(wDedup, req)
	if wDedup.Code != http.StatusOK {
		t.Errorf("Expected 200 OK from dedup, got %d", wDedup.Code)
	}
}

func TestHandleTranscriptSearchAndStats(t *testing.T) {
	fake := db.NewFakeStore()
	_ = fake.UpsertSessionSummary(context.Background(), db.SessionSummary{
		SessionID: "sess-mux-1",
		Summary:   "Triaged container restart in aerial",
	})
	errStore := &errTranscriptStore{FakeStore: db.NewFakeStore()}
	embedderCalled := false
	embedder := func(ctx context.Context, text string) ([]float32, error) {
		embedderCalled = true
		return make([]float32, 384), nil
	}

	for _, tc := range []struct {
		name       string
		handler    http.HandlerFunc
		method     string
		target     string
		wantStatus int
	}{
		{"search method not allowed", handleTranscriptSearch(fake, nil), http.MethodPost, "/api/transcripts/search", http.StatusMethodNotAllowed},
		{"search nil store", handleTranscriptSearch(nil, nil), http.MethodGet, "/api/transcripts/search?q=test", http.StatusInternalServerError},
		{"search store error", handleTranscriptSearch(errStore, nil), http.MethodGet, "/api/transcripts/search?q=test&mode=semantic", http.StatusInternalServerError},
		{"stats method not allowed", handleTranscriptStats(fake), http.MethodPost, "/api/transcripts/stats", http.StatusMethodNotAllowed},
		{"stats nil store", handleTranscriptStats(nil), http.MethodGet, "/api/transcripts/stats", http.StatusInternalServerError},
		{"stats store error", handleTranscriptStats(errStore), http.MethodGet, "/api/transcripts/stats", http.StatusInternalServerError},
		{"search query param alias", handleTranscriptSearch(fake, nil), http.MethodGet, "/api/transcripts/search?query=fallback-query&limit=abc", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.target, nil)
			w := httptest.NewRecorder()
			tc.handler(w, req)
			if w.Code != tc.wantStatus {
				t.Fatalf("got status=%d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
		})
	}

	t.Run("search success and embedder", func(t *testing.T) {
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
		if err := json.Unmarshal(w.Body.Bytes(), &res); err != nil || len(res.Sessions) != 1 || res.Sessions[0].SessionID != "sess-mux-1" {
			t.Fatalf("unexpected search result: %+v, err: %v", res, err)
		}
	})

	t.Run("stats success", func(t *testing.T) {
		handler := handleTranscriptStats(fake)
		req := httptest.NewRequest(http.MethodGet, "/api/transcripts/stats", nil)
		w := httptest.NewRecorder()
		handler(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
		}
		var stats map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &stats); err != nil || stats["total_sessions"] != float64(1) || stats["status"] != "ok" {
			t.Fatalf("unexpected stats: %+v, err: %v", stats, err)
		}
	})

	t.Run("mux endpoints", func(t *testing.T) {
		mux := SetupBrainMuxWithEmbedder(fake, nil, nil)
		for _, path := range []string{"/api/transcripts/search?q=test", "/api/transcripts/stats"} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Errorf("expected 200 for %s, got %d", path, w.Code)
			}
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

	// Error branches & Nil store
	closedStore := db.NewFakeStore()
	_ = closedStore.Close()

	storeCronErr := db.NewFakeStore()
	storeCronErr.FailNext("GetAllCronSchedules", errors.New("cron query forced error"))

	storeOneShotErr := db.NewFakeStore()
	storeOneShotErr.FailNext("GetAllOneShotSchedules", errors.New("oneshot query forced error"))

	for _, tc := range []struct {
		name       string
		handler    http.HandlerFunc
		target     string
		wantStatus int
	}{
		{"schedules closed DB", handleSchedules(closedStore), "/schedules", http.StatusInternalServerError},
		{"schedules nil store", handleSchedules(nil), "/schedules", http.StatusInternalServerError},
		{"schedules cron err", handleSchedules(storeCronErr), "/schedules", http.StatusInternalServerError},
		{"schedules oneshot err", handleSchedules(storeOneShotErr), "/schedules", http.StatusInternalServerError},
		{"runs closed DB", handleScheduleRuns(closedStore), "/schedules/runs", http.StatusInternalServerError},
		{"runs nil store", handleScheduleRuns(nil), "/schedules/runs", http.StatusInternalServerError},
		{"runs query filter", handleScheduleRuns(store), "/schedules/runs?limit=100&offset=5&schedule_id=cron-1&status=completed", http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, tc.target, nil)
			w := httptest.NewRecorder()
			tc.handler(w, req)
			if w.Code != tc.wantStatus {
				t.Fatalf("got status=%d, want %d", w.Code, tc.wantStatus)
			}
		})
	}

	// Concurrent double check
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := httptest.NewRequest(http.MethodGet, "/schedules", nil)
			rw := httptest.NewRecorder()
			schedulesHandler(rw, r)
		}()
	}
	wg.Wait()
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

	// 5. Error branches & Nil store
	closedStore := db.NewFakeStore()
	_ = closedStore.Close()
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"nil store", handleTasks(nil)},
		{"closed store", handleTasks(closedStore)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/tasks", nil)
			rw := httptest.NewRecorder()
			tc.handler(rw, r)
			if rw.Code != http.StatusInternalServerError {
				t.Fatalf("got status=%d, want 500", rw.Code)
			}
		})
	}

	// 6. Concurrent double check
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := httptest.NewRequest(http.MethodGet, "/tasks", nil)
			rw := httptest.NewRecorder()
			handler(rw, r)
		}()
	}
	wg.Wait()
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
	t.Parallel()

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
		{"/voice/ask", "/voice/ask"},
		{"/api/voice/ask", "/voice/ask"},
		{"/unknown/path", "unmatched"},
		{"/random", "unmatched"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			got := normalizeRoute(tt.input)
			if got != tt.expected {
				t.Errorf("normalizeRoute(%q) = %q; want %q", tt.input, got, tt.expected)
			}
		})
	}
}

func TestMetricsMiddleware(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/unknown", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/implicit", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})

	handler := metricsMiddleware(mux)

	// Normal 200
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	// 404
	req404 := httptest.NewRequest(http.MethodGet, "/unknown", nil)
	rec404 := httptest.NewRecorder()
	handler.ServeHTTP(rec404, req404)
	if rec404.Code != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", rec404.Code)
	}

	// Implicit 200
	reqImp := httptest.NewRequest(http.MethodGet, "/implicit", nil)
	recImp := httptest.NewRecorder()
	handler.ServeHTTP(recImp, reqImp)
	if recImp.Code != http.StatusOK {
		t.Errorf("expected 200 for implicit write, got %d", recImp.Code)
	}

	// Empty method
	emptyMethodHandler := metricsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	reqEmpty := httptest.NewRequest("", "/metrics", nil)
	reqEmpty.Method = ""
	recEmpty := httptest.NewRecorder()
	emptyMethodHandler.ServeHTTP(recEmpty, reqEmpty)
	if recEmpty.Code != http.StatusOK {
		t.Errorf("expected 200 OK for empty method, got %d", recEmpty.Code)
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
	t.Parallel()

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

	// 5. Nil store returns 500
	handlerNil := handleFacts(nil)
	wNil := httptest.NewRecorder()
	handlerNil(wNil, req)
	if wNil.Code != http.StatusInternalServerError {
		t.Errorf("Expected 500 InternalServerError on nil store, got %d", wNil.Code)
	}
}

func TestSetupBrainMux_And_Endpoints(t *testing.T) {
	store := db.NewFakeStore()
	pool := newTestWorkerPool(store)
	tmpHome := t.TempDir()
	mux := SetupBrainMuxWithEmbedder(store, pool, nil, tmpHome)

	// Test /health
	reqHealth := httptest.NewRequest(http.MethodGet, "/health", nil)
	wHealth := httptest.NewRecorder()
	mux.ServeHTTP(wHealth, reqHealth)
	if wHealth.Code != http.StatusOK {
		t.Errorf("Expected 200 from /health, got %d", wHealth.Code)
	}

	// Test /health write error
	mux.ServeHTTP(&flusherErrWriter{}, httptest.NewRequest(http.MethodGet, "/health", nil))

	// Test /transcripts with injected homeDir
	reqTranscripts := httptest.NewRequest(http.MethodGet, "/transcripts", nil)
	wTranscripts := httptest.NewRecorder()
	mux.ServeHTTP(wTranscripts, reqTranscripts)
	if wTranscripts.Code != http.StatusOK {
		t.Errorf("Expected 200 from /transcripts, got %d", wTranscripts.Code)
	}

	// Test writeJSON error
	writeJSON(httptest.NewRecorder(), http.StatusOK, make(chan int))

	// Test /debug/coverage/flush without GOCOVERDIR
	t.Setenv("GOCOVERDIR", "")
	wFlush := httptest.NewRecorder()
	mux.ServeHTTP(wFlush, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil))
	if wFlush.Code != http.StatusOK {
		t.Errorf("expected 200 OK without GOCOVERDIR, got %d", wFlush.Code)
	}

	// Test /debug/coverage/flush with GOCOVERDIR
	oldFn := writeCountersDirFn
	defer func() { writeCountersDirFn = oldFn }()
	oldMetaFn := writeMetaDirFn
	defer func() { writeMetaDirFn = oldMetaFn }()

	t.Setenv("GOCOVERDIR", t.TempDir())
	called := false
	writeCountersDirFn = func(dir string) error {
		called = true
		return nil
	}
	metaCalled := false
	writeMetaDirFn = func(dir string) error {
		metaCalled = true
		return nil
	}

	wFlush = httptest.NewRecorder()
	mux.ServeHTTP(wFlush, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil))
	if wFlush.Code != http.StatusOK || !called || !metaCalled {
		t.Errorf("expected 200 OK with GOCOVERDIR and mocks called, got %d", wFlush.Code)
	}

	// With GOCOVERDIR + writeMetaDir error -> still proceeds to write counters (200 OK)
	writeMetaDirFn = func(dir string) error {
		return errors.New("simulated meta flush error")
	}
	wFlush = httptest.NewRecorder()
	mux.ServeHTTP(wFlush, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil))
	if wFlush.Code != http.StatusOK {
		t.Errorf("expected 200 OK when meta flush errors, got %d", wFlush.Code)
	}

	// With GOCOVERDIR + mock error -> 500
	writeCountersDirFn = func(dir string) error {
		return errors.New("simulated flush error")
	}
	wFlush = httptest.NewRecorder()
	mux.ServeHTTP(wFlush, httptest.NewRequest(http.MethodGet, "/debug/coverage/flush", nil))
	if wFlush.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 on flush error, got %d", wFlush.Code)
	}
}

func TestInitializeBrainEnvironment(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T) *config.Config
		wantErr bool
	}{
		{
			name:    "nil config",
			setup:   func(t *testing.T) *config.Config { return nil },
			wantErr: false,
		},
		{
			name: "success fresh and idempotent",
			setup: func(t *testing.T) *config.Config {
				return config.NewTestConfig(func(d *config.ConfigData) {
					d.Model = "gemini-2.5-flash"
					d.GeminiHomeDir = t.TempDir()
					d.DataDir = t.TempDir()
					d.APIKey = "test-api-key"
					d.SystemPrompt = "test prompt"
				})
			},
			wantErr: false,
		},
		{
			name: "uncreatable gemini home",
			setup: func(t *testing.T) *config.Config {
				tmpDir := t.TempDir()
				return config.NewTestConfig(func(d *config.ConfigData) {
					d.GeminiHomeDir = filepath.Join(tmpDir, "nonexistent", "sub", "dir", "invalid")
					d.DataDir = t.TempDir()
				})
			},
			wantErr: false,
		},
		{
			name: "conflict file in data dir",
			setup: func(t *testing.T) *config.Config {
				tmpDir := t.TempDir()
				_ = os.WriteFile(filepath.Join(tmpDir, "brain"), []byte("file-not-dir"), 0644)
				return config.NewTestConfig(func(d *config.ConfigData) {
					d.DataDir = tmpDir
					d.GeminiHomeDir = tmpDir
				})
			},
			wantErr: false,
		},
		{
			name: "conflict file in cli parent",
			setup: func(t *testing.T) *config.Config {
				tmpDir := t.TempDir()
				conflict := filepath.Join(tmpDir, ".gemini", "antigravity-cli")
				_ = os.MkdirAll(filepath.Dir(conflict), 0755)
				_ = os.WriteFile(conflict, []byte("file-not-dir"), 0644)
				return config.NewTestConfig(func(d *config.ConfigData) {
					d.DataDir = tmpDir
					d.GeminiHomeDir = tmpDir
				})
			},
			wantErr: true,
		},
		{
			name: "symlink parent blocker",
			setup: func(t *testing.T) *config.Config {
				tmpDir := t.TempDir()
				geminiDir := filepath.Join(tmpDir, "home", ".gemini")
				_ = os.MkdirAll(geminiDir, 0755)
				_ = os.WriteFile(filepath.Join(geminiDir, "antigravity-cli"), []byte("blocking-file"), 0644)
				_ = os.MkdirAll(filepath.Join(tmpDir, "data"), 0755)
				return config.NewTestConfig(func(d *config.ConfigData) {
					d.GeminiHomeDir = filepath.Join(tmpDir, "home")
					d.DataDir = filepath.Join(tmpDir, "data")
				})
			},
			wantErr: true,
		},
		{
			name: "symlink chmod 0555 error",
			setup: func(t *testing.T) *config.Config {
				tmpHome := t.TempDir()
				parent := filepath.Join(tmpHome, ".gemini", "antigravity-cli")
				_ = os.MkdirAll(parent, 0755)
				_ = os.Chmod(parent, 0555)
				t.Cleanup(func() { _ = os.Chmod(parent, 0755) })
				return config.NewTestConfig(func(d *config.ConfigData) {
					d.GeminiHomeDir = tmpHome
					d.DataDir = t.TempDir()
				})
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.setup(t)
			err := InitializeBrainEnvironment(context.Background(), cfg)
			if (err != nil) != tc.wantErr {
				t.Errorf("wantErr=%v, got err=%v", tc.wantErr, err)
			}
			if tc.name == "success fresh and idempotent" {
				if err := InitializeBrainEnvironment(context.Background(), cfg); err != nil {
					t.Fatalf("unexpected error on idempotent second call: %v", err)
				}
			}
		})
	}
}

func TestCreateReloadConfigFunc(t *testing.T) {
	t.Run("invalid YAML with alert", func(t *testing.T) {
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
	})

	t.Run("supplier success updating model", func(t *testing.T) {
		cfg := config.NewTestConfig()
		s, _ := discordgo.New("Bot mock-token")

		reloadSuccess := CreateReloadConfigFunc(cfg,
			WithDiscordSession(s),
			WithReloadSupplier(func(active *config.Config) error {
				active.Update(&config.ConfigData{Model: "gemini-2.5-flash"})
				return nil
			}),
			WithSkipEnvironmentSync(),
		)
		reloadSuccess("TestReloadSuccess")
	})

	t.Run("complete success with sync and blocker home error", func(t *testing.T) {
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

		blockerHome := filepath.Join(tmpDir, "blocked_home")
		if err := os.WriteFile(blockerHome, []byte("file"), 0644); err != nil {
			t.Fatalf("failed to write blocker file: %v", err)
		}
		cfg.Update(&config.ConfigData{
			GeminiHomeDir: filepath.Join(blockerHome, "sub"),
			DataDir:       tmpData,
		})
		reloadFn("TestErrorSource")
	})

	t.Run("provisioner error", func(t *testing.T) {
		cfg := config.NewTestConfig()
		cur := cfg.Current()
		tmpFile := filepath.Join(t.TempDir(), "blocking_file")
		if err := os.WriteFile(tmpFile, []byte("file"), 0644); err != nil {
			t.Fatalf("failed to write blocker file: %v", err)
		}
		cur.GeminiHomeDir = tmpFile
		cfg.Update(cur)

		prov := env.NewFromConfig(cfg)
		reloadFn := CreateReloadConfigFunc(cfg, WithProvisioner(prov))
		reloadFn("test-provisioner-error")
	})

	t.Run("with worker pool", func(t *testing.T) {
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
	})
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

func TestDefaultPaths(t *testing.T) {
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

	// 4. DefaultDataDir tests
	if got := defaultDataDir(false); got != "/data" {
		t.Errorf("expected /data from defaultDataDir(false), got %s", got)
	}
	if dir := DefaultDataDir(); dir == "" {
		t.Errorf("expected non-empty data dir, got empty")
	}
}

func runBrainAppUntilReady(t *testing.T, cfg *config.Config, opts ...BrainAppOption) (string, context.CancelFunc, func() error) {
	t.Helper()
	var readyOnce sync.Once
	ready := make(chan struct{})
	var boundAddr string
	oldReady := onServerReady
	onServerReady = func(addr string) {
		readyOnce.Do(func() {
			boundAddr = addr
			close(ready)
		})
	}
	t.Cleanup(func() {
		onServerReady = oldReady
	})

	ctx, cancel := context.WithCancel(context.Background())

	var checkOpts brainAppOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&checkOpts)
		}
	}
	var effectiveOpts []BrainAppOption
	if checkOpts.processSpawner == nil {
		effectiveOpts = append(effectiveOpts, WithProcessSpawner(runner.NewMockDaemonSpawner()))
	}
	if checkOpts.store == nil {
		effectiveOpts = append(effectiveOpts, WithStore(db.NewFakeStore()))
	}
	effectiveOpts = append(effectiveOpts, opts...)

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunBrainApp(ctx, cfg, effectiveOpts...)
	}()

	select {
	case <-ready:
	case err := <-errCh:
		cancel()
		t.Fatalf("RunBrainApp exited prematurely before ready: %v", err)
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatalf("RunBrainApp timed out waiting for server ready")
	}

	var waitCalled bool
	wait := func() error {
		waitCalled = true
		select {
		case err := <-errCh:
			return err
		case <-time.After(15 * time.Second):
			t.Errorf("RunBrainApp did not shut down within 15s")
			return errors.New("shutdown timeout")
		}
	}

	t.Cleanup(func() {
		if !waitCalled {
			cancel()
			select {
			case <-errCh:
			case <-time.After(5 * time.Second):
			}
		}
	})

	return boundAddr, cancel, wait
}

func TestRunBrainApp_ValidationAndErrors(t *testing.T) {
	tmpDir := t.TempDir()

	tests := []struct {
		name       string
		cfg        func() *config.Config
		ctx        func() (context.Context, context.CancelFunc)
		opts       []BrainAppOption
		wantErrStr string
		wantNil    bool
	}{
		{
			name: "nil config",
			cfg:  func() *config.Config { return nil },
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErrStr: "config cannot be nil",
		},
		{
			name: "canceled context",
			cfg: func() *config.Config {
				return config.NewTestConfig(func(d *config.ConfigData) {
					d.GeminiHomeDir = tmpDir
					d.DataDir = filepath.Join(tmpDir, "data")
				})
			},
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			wantNil: true,
		},
		{
			name: "canceled context with init error",
			cfg: func() *config.Config {
				blockDir := filepath.Join(t.TempDir(), "block")
				_ = os.WriteFile(blockDir, []byte("file"), 0644)
				return config.NewTestConfig(func(d *config.ConfigData) {
					d.GeminiHomeDir = blockDir
					d.DataDir = filepath.Join(tmpDir, "data")
				})
			},
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx, cancel
			},
			wantNil: true,
		},
		{
			name: "missing database URL without store",
			cfg: func() *config.Config {
				return config.NewTestConfig(func(d *config.ConfigData) {
					d.GeminiHomeDir = tmpDir
					d.DataDir = filepath.Join(tmpDir, "data")
					d.DatabaseURL = ""
				})
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErrStr: "database URL or path is required",
		},
		{
			name: "missing gemini home dir",
			cfg: func() *config.Config {
				return config.NewTestConfig(func(d *config.ConfigData) {
					d.GeminiHomeDir = ""
					d.DataDir = filepath.Join(tmpDir, "data")
					d.DatabaseURL = ":memory:"
				})
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErrStr: "gemini home directory is required",
		},
		{
			name: "missing data dir",
			cfg: func() *config.Config {
				return config.NewTestConfig(func(d *config.ConfigData) {
					d.GeminiHomeDir = tmpDir
					d.DataDir = ""
					d.DatabaseURL = ":memory:"
				})
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErrStr: "data directory is required",
		},
		{
			name: "missing port",
			cfg: func() *config.Config {
				return config.NewTestConfig(func(d *config.ConfigData) {
					d.GeminiHomeDir = tmpDir
					d.DataDir = filepath.Join(tmpDir, "data")
					d.DatabaseURL = ":memory:"
					d.Port = ""
				})
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			wantErrStr: "port is required",
		},
		{
			name: "unsupported database scheme",
			cfg: func() *config.Config {
				return config.NewTestConfig(func(d *config.ConfigData) {
					d.DatabaseURL = "mysql://user:pass@localhost/db"
					d.GeminiHomeDir = tmpDir
					d.DataDir = filepath.Join(tmpDir, "data")
					d.Port = "0"
				})
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 1*time.Second)
			},
			wantErrStr: "failed to initialize database",
		},
		{
			name: "invalid listen port",
			cfg: func() *config.Config {
				return config.NewTestConfig(func(d *config.ConfigData) {
					d.Port = "-1"
					d.AgyBin = "/bin/true"
					d.Model = "gemini-2.5-flash"
					d.SystemPrompt = "test"
					d.GeminiHomeDir = tmpDir
					d.DataDir = filepath.Join(tmpDir, "data")
				})
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 1*time.Second)
			},
			opts:       []BrainAppOption{WithStore(db.NewFakeStore())},
			wantErrStr: "listen",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := tt.ctx()
			defer cancel()

			cfg := tt.cfg()
			err := RunBrainApp(ctx, cfg, tt.opts...)
			if tt.wantNil {
				if err != nil {
					t.Errorf("expected nil error, got: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErrStr)
			}
			if !strings.Contains(err.Error(), tt.wantErrStr) {
				t.Errorf("expected error containing %q, got: %v", tt.wantErrStr, err)
			}
		})
	}

	t.Run("port conflict", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed to bind listener: %v", err)
		}
		defer ln.Close()

		_, portStr, err := net.SplitHostPort(ln.Addr().String())
		if err != nil {
			t.Fatalf("failed to split host port: %v", err)
		}

		conflictCfg := config.NewTestConfig(func(d *config.ConfigData) {
			d.Port = portStr
			d.AgyBin = "/bin/true"
			d.Model = "gemini-2.5-flash"
			d.SystemPrompt = "test"
			d.GeminiHomeDir = tmpDir
			d.DataDir = filepath.Join(tmpDir, "data")
		})
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err = RunBrainApp(ctx, conflictCfg, WithStore(db.NewFakeStore()))
		if err == nil {
			t.Fatalf("expected error from port conflict, got nil")
		}
	})
}

type mockErrProcessHandle struct{}

func (m *mockErrProcessHandle) Pid() int    { return 99999 }
func (m *mockErrProcessHandle) Kill() error { return errors.New("simulated kill error") }
func (m *mockErrProcessHandle) Wait() error { return nil }

func TestRunBrainApp_FullLifecycleAndPools(t *testing.T) {
	t.Run("PrewarmedWorkers", func(t *testing.T) {
		tmpDir := t.TempDir()
		_ = os.MkdirAll(filepath.Join(tmpDir, ".gemini", "config", "skills"), 0755)

		cfg := config.NewTestConfig()
		cur := cfg.Current()
		cur.Port = "0"
		cur.AgyBin = "/bin/true"
		cur.DiscordToken = "mock-discord-token"
		cur.HarnessAPIKey = "gemini-test-key"
		cur.DataDir = tmpDir
		cur.GeminiHomeDir = tmpDir
		cur.LowEffortModel = "gemini-test-low"
		cur.MediumEffortModel = "gemini-test-medium"
		cur.Ollama.BaseURL = "http://127.0.0.1:65534"
		cur.Voice = config.VoiceConfig{
			Engine:           "gemini_api",
			Model:            "gemini-2.5-flash",
			PrewarmedTargets: []string{"kiosk"},
			AmbientContext: &config.AmbientContextConfig{
				Template: "Time: {{.now}}",
				CacheTTL: "5s",
			},
		}
		if cur.Channels == nil {
			cur.Channels = make(map[string]config.ChannelPolicy)
		}
		cur.Channels["default"] = config.ChannelPolicy{
			Mode: "threads",
			AmbientContext: &config.AmbientContextConfig{
				Template: "Time: {{.now}}",
				CacheTTL: "5s",
			},
		}
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
				hasEph0, hasEph1, hasDisc0, hasDisc1, hasMed0, hasMed1 := false, false, false, false, false, false
				for _, sc := range spawnedConfigs {
					switch sc.ThreadID {
					case "ephemeral:worker-0":
						hasEph0 = true
					case "ephemeral:worker-1":
						hasEph1 = true
					case "discord:worker-0":
						hasDisc0 = true
					case "discord:worker-1":
						hasDisc1 = true
					case "discord:medium-worker-0":
						hasMed0 = true
					case "discord:medium-worker-1":
						hasMed1 = true
					}
				}
				if hasEph0 && hasEph1 && hasDisc0 && hasDisc1 && hasMed0 && hasMed1 {
					spawnedOnce.Do(func() { close(allSpawned) })
				}
				spawnedConfigsMu.Unlock()
				return baseMock.Spawn(ctx, dCfg)
			},
		}

		boundAddr, cancel, wait := runBrainAppUntilReady(t, cfg,
			WithProcessSpawner(trackingSpawner),
		)

		reqBody := `{"prompt": "Hello test prompt", "channel_id": "1542423172400291873", "message_id": "msg-1"}`
		resp, err := http.Post(fmt.Sprintf("http://%s/prompt", boundAddr), "application/json", strings.NewReader(reqBody))
		if err == nil {
			_ = resp.Body.Close()
		}

		select {
		case <-allSpawned:
		case <-time.After(2 * time.Second):
		}

		cancel()
		if err := wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("unexpected error waiting for server shutdown: %v", err)
		}

		spawnedConfigsMu.Lock()
		defer spawnedConfigsMu.Unlock()

		foundEph0, foundEph1, foundDisc0, foundDisc1, foundMed0, foundMed1 := false, false, false, false, false, false
		for _, sc := range spawnedConfigs {
			switch sc.ThreadID {
			case "ephemeral:worker-0":
				foundEph0 = true
				if !hasEnv(sc.Env, "HOME="+expectedEphemeralHome) {
					t.Errorf("ephemeral:worker-0 expected HOME=%q, got %v", expectedEphemeralHome, sc.Env)
				}
			case "ephemeral:worker-1":
				foundEph1 = true
				if !hasEnv(sc.Env, "HOME="+expectedEphemeralHome) {
					t.Errorf("ephemeral:worker-1 expected HOME=%q, got %v", expectedEphemeralHome, sc.Env)
				}
			case "discord:worker-0":
				foundDisc0 = true
				if !hasEnv(sc.Env, "HOME="+expectedDiscordHome) {
					t.Errorf("discord:worker-0 expected HOME=%q, got %v", expectedDiscordHome, sc.Env)
				}
			case "discord:worker-1":
				foundDisc1 = true
				if !hasEnv(sc.Env, "HOME="+expectedDiscordHome) {
					t.Errorf("discord:worker-1 expected HOME=%q, got %v", expectedDiscordHome, sc.Env)
				}
			case "discord:medium-worker-0":
				foundMed0 = true
			case "discord:medium-worker-1":
				foundMed1 = true
			}
		}

		if !foundEph0 || !foundEph1 || !foundDisc0 || !foundDisc1 || !foundMed0 || !foundMed1 {
			t.Errorf("expected all prewarmed workers spawned: eph0=%v eph1=%v disc0=%v disc1=%v med0=%v med1=%v",
				foundEph0, foundEph1, foundDisc0, foundDisc1, foundMed0, foundMed1)
		}
	})

	t.Run("SIGHUP", func(t *testing.T) {
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

		hupChan := make(chan os.Signal, 1)
		reloadReceived := make(chan string, 1)

		_, cancel, wait := runBrainAppUntilReady(t, cfg,
			WithHupChannel(hupChan),
			WithOnReload(func(src string) {
				select {
				case reloadReceived <- src:
				default:
				}
			}),
		)

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
		if err := wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("unexpected error waiting for shutdown: %v", err)
		}
	})

	t.Run("DefaultStoreSuccess", func(t *testing.T) {
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

		_, cancel, wait := runBrainAppUntilReady(t, cfg, WithStore(nil))
		cancel()
		if err := wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("unexpected error from RunBrainApp: %v", err)
		}
	})

	t.Run("ProcessPoolInitErrors", func(t *testing.T) {
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

		_, cancel, wait := runBrainAppUntilReady(t, cfg, WithProcessSpawner(failingSpawner))
		cancel()
		if err := wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("unexpected error from RunBrainApp: %v", err)
		}
	})

	t.Run("DaemonCloseErrors", func(t *testing.T) {
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

		_, cancel, wait := runBrainAppUntilReady(t, cfg, WithProcessSpawner(spawner))
		cancel()
		if err := wait(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatalf("unexpected error from RunBrainApp: %v", err)
		}
	})
}

func hasEnv(envs []string, target string) bool {
	for _, e := range envs {
		if e == target {
			return true
		}
	}
	return false
}

func TestBrainAppOptions_Coverage(t *testing.T) {
	var opts brainAppOptions
	store := db.NewFakeStore()
	WithStore(store)(&opts)
	if opts.store != store {
		t.Errorf("expected store to be set")
	}

	spawner := runner.NewMockDaemonSpawner()
	WithProcessSpawner(spawner)(&opts)
	if opts.processSpawner != spawner {
		t.Errorf("expected processSpawner to be set")
	}

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

type errReadCloser struct {
	reader   io.Reader
	readErr  error
	closeErr error
}

func (e *errReadCloser) Read(p []byte) (int, error) {
	if e.readErr != nil {
		return 0, e.readErr
	}
	if e.reader != nil {
		return e.reader.Read(p)
	}
	return 0, errors.New("simulated network read error")
}

func (e *errReadCloser) Close() error {
	if e.closeErr != nil {
		return e.closeErr
	}
	return nil
}

type mockHTTPResponseWriter struct {
	header http.Header
	code   int
	body   bytes.Buffer
}

func (w *mockHTTPResponseWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}

func (w *mockHTTPResponseWriter) Write(b []byte) (int, error) {
	return w.body.Write(b)
}

func (w *mockHTTPResponseWriter) WriteHeader(statusCode int) {
	w.code = statusCode
}

type flusherErrWriter struct {
	header http.Header
	code   int
}

func (e *flusherErrWriter) Header() http.Header {
	if e.header == nil {
		e.header = make(http.Header)
	}
	return e.header
}

func (e *flusherErrWriter) Write(p []byte) (int, error) {
	return 0, errors.New("simulated write error")
}

func (e *flusherErrWriter) WriteHeader(statusCode int) {
	e.code = statusCode
}

func (e *flusherErrWriter) Flush() {}

func TestHandleVoiceAsk_Validation(t *testing.T) {
	store := db.NewFakeStore()
	pool := newTestWorkerPool(store)
	pool.Start()
	defer pool.Stop()

	handler := handleVoiceAsk(pool)

	tests := []struct {
		name       string
		method     string
		body       io.Reader
		accept     string
		wantStatus int
		wantSubstr string
	}{
		{"GET not allowed", http.MethodGet, nil, "", http.StatusMethodNotAllowed, "Method not allowed"},
		{"empty prompt", http.MethodPost, strings.NewReader(`{"prompt":""}`), "", http.StatusBadRequest, "'prompt' field is required"},
		{"malformed JSON", http.MethodPost, strings.NewReader(`{invalid-json`), "", http.StatusBadRequest, "'prompt' field is required"},
		{"body read error", http.MethodPost, &errReadCloser{readErr: errors.New("simulated body read error")}, "", http.StatusBadRequest, "Failed to read request body"},
		{"conversation_id fallback", http.MethodPost, strings.NewReader(`{"conversation_id":"conv-123","prompt":"hello"}`), "", http.StatusOK, ""},
		{"missing session_id", http.MethodPost, strings.NewReader(`{"prompt":"hello"}`), "", http.StatusBadRequest, "session_id"},
		{"node_id provided", http.MethodPost, strings.NewReader(`{"prompt":"hello","node_id":"touch-kiosk-kitchen"}`), "", http.StatusOK, ""},
		{"node_id priority", http.MethodPost, strings.NewReader(`{"prompt":"hello","node_id":"touch-kiosk-kitchen","session_id":"turn-123","conversation_id":"conv-456"}`), "", http.StatusOK, ""},
		{"oversized device ID", http.MethodPost, strings.NewReader(fmt.Sprintf(`{"prompt":"hello","node_id":%q}`, strings.Repeat("a", 129))), "", http.StatusBadRequest, "exceeds 128 characters"},
		{"control characters in device ID", http.MethodPost, strings.NewReader(`{"prompt":"hello","node_id":"dock\ninvalid"}`), "", http.StatusBadRequest, "contains invalid characters"},
		{"body close error handled", http.MethodPost, &errReadCloser{reader: strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`), closeErr: errors.New("simulated close error")}, "", http.StatusOK, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "/voice/ask", tc.body)
			if tc.accept != "" {
				req.Header.Set("Accept", tc.accept)
			}
			w := httptest.NewRecorder()
			handler(w, req)
			if w.Code != tc.wantStatus {
				t.Fatalf("got status %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
			if tc.wantSubstr != "" && !strings.Contains(w.Body.String(), tc.wantSubstr) {
				t.Errorf("expected substring %q in body, got: %s", tc.wantSubstr, w.Body.String())
			}
			if tc.method != http.MethodPost && w.Header().Get("Allow") != "POST" {
				t.Errorf("expected Allow: POST header, got %q", w.Header().Get("Allow"))
			}
		})
	}

	t.Run("non-flusher response writer for SSE", func(t *testing.T) {
		reqSSE := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`))
		reqSSE.Header.Set("Accept", "text/event-stream")
		nonFlusher := &mockHTTPResponseWriter{}
		handler(nonFlusher, reqSSE)
		if nonFlusher.code != http.StatusInternalServerError {
			t.Errorf("expected 500 for non-flusher SSE, got %d", nonFlusher.code)
		}
	})
}

func newVoiceHandler(cfg queue.WorkerPoolConfig) (http.HandlerFunc, func()) {
	if cfg.Store == nil {
		cfg.Store = db.NewFakeStore()
	}
	pool := queue.NewWorkerPool(cfg)
	pool.Start()
	return handleVoiceAsk(pool), pool.Stop
}

func TestHandleVoiceAsk_SSE(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				if onStatus != nil {
					onStatus("⚡ Checking smart lights...")
				}
				return "Lights have been turned off.", "sess-test-456", nil
			},
		})
		defer stop()

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
			t.Errorf("expected done event with conversation_id in SSE stream, got:\n%s", body)
		}
	})

	t.Run("SentenceStreaming", func(t *testing.T) {
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
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
		defer stop()

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

		idxSent1 := strings.Index(body, "event: sentence\ndata: {\"text\":\"Here is sentence one.\"}")
		idxSent2 := strings.Index(body, "event: sentence\ndata: {\"text\":\"And here is sentence two.\"}")
		idxReply := strings.Index(body, "event: reply\ndata: {\"conversation_id\":\"sess-stream-789\"")
		if idxSent1 == -1 || idxSent2 == -1 || idxReply == -1 || idxSent1 > idxSent2 || idxSent2 > idxReply {
			t.Errorf("invalid event ordering: idxSent1=%d, idxSent2=%d, idxReply=%d", idxSent1, idxSent2, idxReply)
		}
	})

	t.Run("CancelledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(c context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				cancel()
				if onStatus != nil {
					onStatus("status after cancel")
				}
				return "done", "sess-1", nil
			},
		})
		defer stop()

		req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`)).WithContext(ctx)
		req.Header.Set("Accept", "text/event-stream")
		w := httptest.NewRecorder()
		handler(w, req)
	})

	t.Run("VoiceRunnerError", func(t *testing.T) {
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(c context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				if onStatus != nil {
					onStatus("working")
				}
				return "", "", errors.New("synthetic voice failure")
			},
		})
		defer stop()

		req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`))
		req.Header.Set("Accept", "text/event-stream")
		w := httptest.NewRecorder()
		handler(w, req)

		if !strings.Contains(w.Body.String(), "event: error") {
			t.Errorf("expected SSE error event, got %q", w.Body.String())
		}
	})

	t.Run("WriteError", func(t *testing.T) {
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(c context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				return "done", "sess-1", nil
			},
		})
		defer stop()

		req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(`{"prompt":"hello","session_id":"kiosk"}`))
		req.Header.Set("Accept", "text/event-stream")
		w := &flusherErrWriter{}
		handler(w, req)
	})

	t.Run("JSON_Success", func(t *testing.T) {
		handler, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				return "The temperature is 72 degrees.", "sess-json-789", nil
			},
		})
		defer stop()

		payload := `{"prompt":"what is the temperature?","session_id":"sess-json-789"}`
		req := httptest.NewRequest(http.MethodPost, "/voice/ask", strings.NewReader(payload))
		req.Header.Set("Accept", "application/json")

		w := httptest.NewRecorder()
		handler(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("expected 200 OK, got %d", w.Code)
		}
		var resp VoiceAskResponse
		if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to decode JSON response: %v", err)
		}
		if resp.Reply != "The temperature is 72 degrees." {
			t.Errorf("expected reply 'The temperature is 72 degrees.', got %q", resp.Reply)
		}
		if resp.SessionID != "sess-json-789" {
			t.Errorf("expected session_id 'sess-json-789', got %q", resp.SessionID)
		}
	})

	t.Run("Errors", func(t *testing.T) {
		hErr, stop := newVoiceHandler(queue.WorkerPoolConfig{
			VoiceRunnerFunc: func(ctx context.Context, prompt, sessionID string, onStatus func(string)) (string, string, error) {
				return "", "", errors.New("deliberation timed out")
			},
		})
		defer stop()

		// JSON mode with error
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

		// Nil pool handling
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
	})
}

func TestVoicePool_FactoryWiring(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	voiceHome := filepath.Join(tmpDir, "voice")
	spawner := runner.NewMockSpawner()

	tests := []struct {
		name          string
		cfg           *config.Config
		wantGeminiAPI bool
		wantModel     string
		wantAPIKey    string
	}{
		{
			name: "GeminiAPI_WithHarnessAPIKey",
			cfg: config.NewFromData(&config.ConfigData{
				HarnessAPIKey: "test-harness-key",
				Voice: config.VoiceConfig{
					Engine:           "gemini_api",
					Model:            "gemini-2.5-flash",
					PrewarmedTargets: []string{"kiosk"},
				},
				SystemPrompt: "Test System Prompt",
			}),
			wantGeminiAPI: true,
			wantModel:     "gemini-2.5-flash",
			wantAPIKey:    "test-harness-key",
		},
		{
			name: "GeminiAPI_WithAmbientContext",
			cfg: config.NewFromData(&config.ConfigData{
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
			}),
			wantGeminiAPI: true,
			wantModel:     "fallback-model",
			wantAPIKey:    "test-ambient-key",
		},
		{
			name: "GeminiAPI_WithAmbientContext_InvalidTemplate",
			cfg: config.NewFromData(&config.ConfigData{
				APIKey: "test-ambient-key",
				Voice: config.VoiceConfig{
					Engine: "gemini_api",
					AmbientContext: &config.AmbientContextConfig{
						CacheTTL: "30s",
						Template: "{{.invalid template syntax",
					},
				},
			}),
			wantGeminiAPI: true,
			wantModel:     "fallback-model",
			wantAPIKey:    "test-ambient-key",
		},
		{
			name: "GeminiAPI_WithAPIKeyFallback",
			cfg: config.NewFromData(&config.ConfigData{
				APIKey: "test-standard-key",
				Voice: config.VoiceConfig{
					Engine: "gemini_api",
				},
			}),
			wantGeminiAPI: true,
			wantModel:     "fallback-model",
			wantAPIKey:    "test-standard-key",
		},
		{
			name: "GeminiAPI_EmptyKeyFallbackToAgy",
			cfg: config.NewFromData(&config.ConfigData{
				Voice: config.VoiceConfig{
					Engine: "gemini_api",
				},
			}),
			wantGeminiAPI: false,
			wantModel:     "fallback-model",
		},
		{
			name: "AgyEngine",
			cfg: config.NewFromData(&config.ConfigData{
				HarnessAPIKey: "some-key",
				Voice: config.VoiceConfig{
					Engine: "agy",
				},
			}),
			wantGeminiAPI: false,
			wantModel:     "fallback-model",
		},
		{
			name:          "NilConfig",
			cfg:           nil,
			wantGeminiAPI: false,
			wantModel:     "fallback-model",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pool := createVoiceProcessPool(tc.cfg, voiceHome, "fallback-model", spawner)
			defer pool.Close()

			dynPool, ok := pool.(*runner.DynamicVoicePool)
			if !ok {
				t.Fatalf("expected *runner.DynamicVoicePool, got %T", pool)
			}
			if dynPool.Model() != tc.wantModel {
				t.Errorf("expected model %q, got %q", tc.wantModel, dynPool.Model())
			}
			if tc.wantGeminiAPI {
				geminiPool, ok := dynPool.CurrentPool().(*runner.GeminiAPIPool)
				if !ok {
					t.Fatalf("expected *runner.GeminiAPIPool, got %T", dynPool.CurrentPool())
				}
				if tc.wantAPIKey != "" && geminiPool.APIKey() != tc.wantAPIKey {
					t.Errorf("expected APIKey %q, got %q", tc.wantAPIKey, geminiPool.APIKey())
				}
			} else {
				unifiedPool, ok := dynPool.CurrentPool().(*runner.UnifiedProcessPool)
				if !ok {
					t.Fatalf("expected *runner.UnifiedProcessPool, got %T", dynPool.CurrentPool())
				}
				if unifiedPool.Model() != tc.wantModel {
					t.Errorf("expected model %q, got %q", tc.wantModel, unifiedPool.Model())
				}
			}
		})
	}

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

func TestExtractMCPServers(t *testing.T) {
	t.Parallel()

	// 1. Nil config
	nilVoice := extractMCPServers(nil, false)
	if len(nilVoice) != 1 || nilVoice[0].Name != "scheduler" {
		t.Fatalf("expected scheduler default for nil voice config, got: %+v", nilVoice)
	}
	nilAll := extractMCPServers(nil, true)
	if len(nilAll) != 1 || nilAll[0].Name != "scheduler" {
		t.Fatalf("expected scheduler default for nil all config, got: %+v", nilAll)
	}

	// 2. Populated config with Common, Voice, Discord
	rawValid := json.RawMessage(`{"serverUrl":"http://alpha:8080/mcp","headers":{"Auth":"Bearer tok"}}`)
	rawDiscord := json.RawMessage(`{"serverUrl":"http://discord:8080/mcp"}`)
	rawVoice := json.RawMessage(`{"serverUrl":"http://voice:8080/mcp"}`)
	data := &config.ConfigData{
		McpServers: config.TargetMcpConfig{
			Common: map[string]json.RawMessage{
				"alpha": rawValid,
			},
			Discord: map[string]json.RawMessage{
				"beta": rawDiscord,
			},
			Voice: map[string]json.RawMessage{
				"gamma": rawVoice,
			},
		},
	}

	voiceServers := extractMCPServers(data, false)
	if len(voiceServers) != 3 {
		t.Fatalf("expected 3 servers for voice (scheduler, alpha, gamma), got %d: %+v", len(voiceServers), voiceServers)
	}
	allServers := extractMCPServers(data, true)
	if len(allServers) != 4 {
		t.Fatalf("expected 4 servers for all (scheduler, alpha, beta, gamma), got %d: %+v", len(allServers), allServers)
	}
}

func envSliceToMap(env []string) map[string]string {
	m := make(map[string]string, len(env))
	for _, e := range env {
		if k, v, ok := strings.Cut(e, "="); ok {
			m[k] = v
		}
	}
	return m
}

func TestBuildPoolEnv(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		base      []string
		useTmpDir bool
		tz        []string
		wantExact []string
		wantEnv   func(tmpDir string) map[string]string
		checkDirs bool
	}{
		{
			name:      "EmptyRuntimeBaseReturnsBaseEnv",
			base:      []string{"FOO=bar", "BAZ=qux"},
			wantExact: []string{"FOO=bar", "BAZ=qux"},
		},
		{
			name:      "InjectsUnifiedCacheDirectories",
			base:      []string{"EXISTING=1"},
			useTmpDir: true,
			wantEnv: func(tmpDir string) map[string]string {
				return map[string]string{
					"GOCACHE":             filepath.Join(tmpDir, "cache", "go-build"),
					"GOPATH":              filepath.Join(tmpDir, "cache", "go"),
					"GOMODCACHE":          filepath.Join(tmpDir, "cache", "go", "pkg", "mod"),
					"GOLANGCI_LINT_CACHE": filepath.Join(tmpDir, "cache", "golangci-lint"),
					"EXISTING":            "1",
				}
			},
			checkDirs: true,
		},
		{
			name: "PreservesExplicitCacheEnv",
			base: []string{
				"GOCACHE=/custom/gocache",
				"GOPATH=/custom/gopath",
				"GOMODCACHE=/custom/modcache",
				"GOLANGCI_LINT_CACHE=/custom/lintcache",
			},
			useTmpDir: true,
			wantEnv: func(tmpDir string) map[string]string {
				return map[string]string{
					"GOCACHE":             "/custom/gocache",
					"GOPATH":              "/custom/gopath",
					"GOMODCACHE":          "/custom/modcache",
					"GOLANGCI_LINT_CACHE": "/custom/lintcache",
				}
			},
		},
		{
			name:      "InjectsConfiguredTimezone",
			base:      []string{"EXISTING=1"},
			useTmpDir: true,
			tz:        []string{"America/Los_Angeles"},
			wantEnv: func(tmpDir string) map[string]string {
				return map[string]string{"TZ": "America/Los_Angeles"}
			},
		},
		{
			name:      "PreservesExplicitTZ",
			base:      []string{"TZ=UTC"},
			useTmpDir: true,
			tz:        []string{"America/Los_Angeles"},
			wantEnv: func(tmpDir string) map[string]string {
				return map[string]string{"TZ": "UTC"}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runtimeBase := ""
			var tmpDir string
			if tc.useTmpDir {
				tmpDir = t.TempDir()
				runtimeBase = tmpDir
			}
			res := buildPoolEnv(tc.base, runtimeBase, tc.tz...)
			if tc.wantExact != nil {
				if len(res) != len(tc.wantExact) || res[0] != tc.wantExact[0] || res[1] != tc.wantExact[1] {
					t.Errorf("expected exact %v, got %v", tc.wantExact, res)
				}
				return
			}
			expectedEnv := tc.wantEnv(tmpDir)
			envMap := envSliceToMap(res)
			for k, v := range expectedEnv {
				if envMap[k] != v {
					t.Errorf("expected %s=%q, got %q", k, v, envMap[k])
				}
			}
			if tc.checkDirs {
				for _, d := range []string{
					filepath.Join(tmpDir, "cache", "go-build"),
					filepath.Join(tmpDir, "cache", "go", "pkg", "mod"),
					filepath.Join(tmpDir, "cache", "golangci-lint"),
				} {
					if fi, err := os.Stat(d); err != nil || !fi.IsDir() {
						t.Errorf("expected directory %q to exist, err: %v", d, err)
					}
				}
			}
		})
	}
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
	initCh := make(chan string, 3)

	p1 := &mockSequentialPool{
		name:   "pool1",
		onInit: func(name string) { initCh <- name },
	}
	p2 := &mockSequentialPool{
		name:   "pool2",
		onInit: func(name string) { initCh <- name },
	}
	p3 := &mockSequentialPool{
		name:   "pool3",
		onInit: func(name string) { initCh <- name },
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	PrewarmProcessPoolsSequentially(ctx, p1, nil, p2, p3)

	var order []string
	for i := 0; i < 3; i++ {
		select {
		case name := <-initCh:
			order = append(order, name)
		case <-time.After(2 * time.Second):
			t.Fatalf("timeout waiting for pool %d to initialize", i+1)
		}
	}

	if order[0] != "pool1" || order[1] != "pool2" || order[2] != "pool3" {
		t.Errorf("expected sequential order [pool1, pool2, pool3], got %v", order)
	}

	t.Run("CancelledContext", func(t *testing.T) {
		cancelCtx, cancelFn := context.WithCancel(context.Background())
		cancelFn() // cancelled upfront

		done := make(chan struct{})
		pCancelled := &mockSequentialPool{
			name: "pCancelled",
			onInit: func(string) {
				close(done)
			},
		}
		PrewarmProcessPoolsSequentially(cancelCtx, pCancelled)
		select {
		case <-done:
		case <-time.After(50 * time.Millisecond):
		}
	})
}

func TestHandlePRRegister(t *testing.T) {
	store := db.NewFakeStore()
	handler := handlePRRegister(store)

	postPR := func(h http.HandlerFunc, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/internal/pr/register", strings.NewReader(body))
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec
	}

	errorCases := []struct {
		name       string
		method     string
		h          http.HandlerFunc
		body       string
		wantStatus int
	}{
		{"method not allowed", http.MethodGet, handler, "", http.StatusMethodNotAllowed},
		{"nil store", http.MethodPost, handlePRRegister(nil), "{}", http.StatusInternalServerError},
		{"invalid JSON", http.MethodPost, handler, "{invalid-json", http.StatusBadRequest},
		{"empty repo", http.MethodPost, handler, `{"repo":"","pr_number":1,"branch":"feat","head_sha":"sha","target_id":"1555405874565091380"}`, http.StatusBadRequest},
		{"zero pr_number", http.MethodPost, handler, `{"repo":"aerial","pr_number":0,"branch":"feat","head_sha":"sha","target_id":"1555405874565091380"}`, http.StatusBadRequest},
		{"empty branch", http.MethodPost, handler, `{"repo":"aerial","pr_number":1,"branch":"","head_sha":"sha","target_id":"1555405874565091380"}`, http.StatusBadRequest},
		{"empty head_sha", http.MethodPost, handler, `{"repo":"aerial","pr_number":1,"branch":"feat","head_sha":"","target_id":"1555405874565091380"}`, http.StatusBadRequest},
		{"empty target_id", http.MethodPost, handler, `{"repo":"aerial","pr_number":1,"branch":"feat","head_sha":"sha","target_id":""}`, http.StatusBadRequest},
		{"too short target_id", http.MethodPost, handler, `{"repo":"aerial","pr_number":1,"branch":"feat","head_sha":"sha","target_id":"12345"}`, http.StatusBadRequest},
		{"too long target_id", http.MethodPost, handler, `{"repo":"aerial","pr_number":1,"branch":"feat","head_sha":"sha","target_id":"123456789012345678901"}`, http.StatusBadRequest},
		{"non-numeric target_id", http.MethodPost, handler, `{"repo":"aerial","pr_number":1,"branch":"feat","head_sha":"sha","target_id":"155540587456509138A"}`, http.StatusBadRequest},
	}
	for _, tc := range errorCases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, "/internal/pr/register", strings.NewReader(tc.body))
			rec := httptest.NewRecorder()
			tc.h(rec, req)
			if rec.Code != tc.wantStatus {
				t.Errorf("[%s] Expected %d, got %d (%s)", tc.name, tc.wantStatus, rec.Code, rec.Body.String())
			}
		})
	}

	// Successful Registration
	{
		payload := `{"repo":"aerial","pr_number":508,"branch":"feat/push-pipeline","head_sha":"head1234","target_id":"1555405874565091380","title":"feat: push pipeline"}`
		rec := postPR(handler, payload)
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

		recInDB, err := store.GetPRByNumber(context.Background(), "aerial", 508)
		if err != nil {
			t.Fatalf("Failed to retrieve registered PR from store: %v", err)
		}
		if recInDB.Branch != "feat/push-pipeline" || recInDB.HeadSHA != "head1234" || recInDB.Title != "feat: push pipeline" {
			t.Errorf("Stored PR mismatch: %+v", recInDB)
		}
	}

	// DB failure returns 500
	{
		store.FailNext("UpsertPR", errors.New("simulated DB failure"))
		payload := `{"repo":"aerial","pr_number":509,"branch":"feat/push-pipeline","head_sha":"head5678","target_id":"1555405874565091380"}`
		rec := postPR(handler, payload)
		if rec.Code != http.StatusInternalServerError {
			t.Errorf("Expected 500 on DB failure, got %d (%s)", rec.Code, rec.Body.String())
		}
	}

	// Success with valid empty target_id fallback to target channel
	{
		payload := `{"repo":"aerial","pr_number":510,"branch":"feat/push-pipeline","head_sha":"head9999","target_id":"1555405874565091380"}`
		rec := postPR(handler, payload)
		if rec.Code != http.StatusOK {
			t.Errorf("Expected 200 OK, got %d (%s)", rec.Code, rec.Body.String())
		}
	}

	// Idempotent re-registration updates branch and head_sha
	{
		payload := `{"repo":"aerial","pr_number":508,"branch":"feat/push-pipeline-v2","head_sha":"headUpdated","target_id":"1555405874565091380"}`
		rec := postPR(handler, payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("Expected 200 OK on re-registration, got %d (%s)", rec.Code, rec.Body.String())
		}

		recInDB, err := store.GetPRByNumber(context.Background(), "aerial", 508)
		if err != nil {
			t.Fatalf("Failed to retrieve re-registered PR: %v", err)
		}
		if recInDB.Branch != "feat/push-pipeline-v2" || recInDB.HeadSHA != "headUpdated" {
			t.Errorf("Re-registration did not update fields: %+v", recInDB)
		}
	}

	// Registration with pool delivers standalone PR widget and updates status_message_id
	{
		var deliveredReq discord.StatusWidgetRequest
		testPool := queue.NewWorkerPool(queue.WorkerPoolConfig{
			WidgetDeliveryFunc: func(r discord.StatusWidgetRequest) (string, error) {
				deliveredReq = r
				return "msg-widget-standalone-1", nil
			},
		})
		poolHandler := handlePRRegister(store, testPool)

		payload := `{"repo":"aerial","pr_number":601,"branch":"feat/widget","head_sha":"sha601","target_id":"1555405874565091380","title":"feat: widget title"}`
		rec := postPR(poolHandler, payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("Expected 200 OK with pool, got %d (%s)", rec.Code, rec.Body.String())
		}
		if deliveredReq.ID != "pr-aerial-601" || deliveredReq.ChannelID != "1555405874565091380" || deliveredReq.Title != "feat: widget title" {
			t.Errorf("Unexpected delivered widget request: %+v", deliveredReq)
		}
		prInStore, err := store.GetPRByNumber(context.Background(), "aerial", 601)
		if err != nil || prInStore.StatusMessageID != "msg-widget-standalone-1" {
			t.Errorf("Expected status_message_id msg-widget-standalone-1 in store, got %+v (err: %v)", prInStore, err)
		}
	}

	// Registration with pool and Vanadis chunk delivers Vanadis widget and updates build status_message_id
	{
		err := store.CreateVanadisBuild(context.Background(), db.VanadisBuild{
			ID:        "build-reg-widget",
			ThreadID:  "1555405874565091380",
			Title:     "Update Database Schema And Widget Card",
			Objective: "Build widget delivery test",
			Status:    "running",
			Tier:      2,
		}, []db.VanadisChunk{
			{
				ID:           "chunk-reg-widget-0",
				ChunkIndex:   0,
				Title:        "Chunk Zero",
				Instructions: "Do zero",
			},
		})
		if err != nil {
			t.Fatalf("CreateVanadisBuild failed: %v", err)
		}

		var deliveredVanadisReq discord.StatusWidgetRequest
		testPool := queue.NewWorkerPool(queue.WorkerPoolConfig{
			WidgetDeliveryFunc: func(r discord.StatusWidgetRequest) (string, error) {
				deliveredVanadisReq = r
				return "msg-build-widget-1", nil
			},
		})
		poolHandler := handlePRRegister(store, testPool)

		payload := `{"repo":"aerial","pr_number":602,"branch":"feat/vanadis-widget","head_sha":"sha602","target_id":"1555405874565091380","title":"feat: vanadis widget","vanadis_chunk_id":"chunk-reg-widget-0"}`
		rec := postPR(poolHandler, payload)
		if rec.Code != http.StatusOK {
			t.Fatalf("Expected 200 OK with pool and vanadis, got %d (%s)", rec.Code, rec.Body.String())
		}
		if deliveredVanadisReq.ID != "build-reg-widget" || deliveredVanadisReq.ChannelID != "1555405874565091380" || deliveredVanadisReq.Title != "Update Database Schema And Widget Card" {
			t.Errorf("Unexpected delivered vanadis widget request: %+v", deliveredVanadisReq)
		}
		buildInStore, _, err := store.GetVanadisBuild(context.Background(), "build-reg-widget")
		if err != nil || buildInStore.StatusMessageID != "msg-build-widget-1" {
			t.Errorf("Expected status_message_id msg-build-widget-1 in store, got %+v (err: %v)", buildInStore, err)
		}
	}

	// Registration with pool covering warning/fallback branches (empty title, store/delivery errors)
	{
		errPool := queue.NewWorkerPool(queue.WorkerPoolConfig{
			WidgetDeliveryFunc: func(r discord.StatusWidgetRequest) (string, error) {
				return "", errors.New("widget delivery failed")
			},
		})
		errPoolHandler := handlePRRegister(store, errPool)

		// Standalone PR with empty title + widget delivery error
		payloadEmptyTitle := `{"repo":"aerial","pr_number":603,"branch":"feat/empty-title","head_sha":"sha603","target_id":"1555405874565091380"}`
		rec := postPR(errPoolHandler, payloadEmptyTitle)
		if rec.Code != http.StatusOK {
			t.Fatalf("Expected 200 OK even when widget delivery fails, got %d", rec.Code)
		}

		// Standalone PR with GetPRByNumber error + UpdatePRStatusMessageID error
		okPool := queue.NewWorkerPool(queue.WorkerPoolConfig{
			WidgetDeliveryFunc: func(r discord.StatusWidgetRequest) (string, error) {
				return "msg-new-id", nil
			},
		})
		okPoolHandler := handlePRRegister(store, okPool)
		store.FailNext("GetPRByNumber", errors.New("get pr failed"))
		store.FailNext("UpdatePRStatusMessageID", errors.New("update msg id failed"))
		rec = postPR(okPoolHandler, payloadEmptyTitle)
		if rec.Code != http.StatusOK {
			t.Fatalf("Expected 200 OK when GetPRByNumber/UpdatePRStatusMessageID fail, got %d", rec.Code)
		}

		// Vanadis chunk widget with errors
		vanadisPayload := `{"repo":"aerial","pr_number":604,"branch":"feat/v-err","head_sha":"sha604","target_id":"1555405874565091380","vanadis_chunk_id":"chunk-reg-widget-0"}`
		store.FailNext("GetVanadisChunk", errors.New("get chunk failed"))
		rec = postPR(okPoolHandler, vanadisPayload)
		if rec.Code != http.StatusOK {
			t.Fatalf("Expected 200 OK when GetVanadisChunk fails, got %d", rec.Code)
		}

		store.FailNext("GetVanadisBuild", errors.New("get build failed"))
		rec = postPR(okPoolHandler, vanadisPayload)
		if rec.Code != http.StatusOK {
			t.Fatalf("Expected 200 OK when GetVanadisBuild fails, got %d", rec.Code)
		}

		rec = postPR(errPoolHandler, vanadisPayload)
		if rec.Code != http.StatusOK {
			t.Fatalf("Expected 200 OK when Vanadis DeliverStatusWidget fails, got %d", rec.Code)
		}

		store.FailNext("UpdateVanadisBuildStatusMessageID", errors.New("update build msg id failed"))
		rec = postPR(okPoolHandler, vanadisPayload)
		if rec.Code != http.StatusOK {
			t.Fatalf("Expected 200 OK when UpdateVanadisBuildStatusMessageID fails, got %d", rec.Code)
		}
	}
}

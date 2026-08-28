package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/azylman/aerial/brain/pkg/db"
	"github.com/azylman/aerial/brain/pkg/queue"
)

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

	// Verify message persisted to SQLite
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

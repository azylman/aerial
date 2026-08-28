package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/azylman/aerial/brain/pkg/db"
)

func TestHandlePromptValidation(t *testing.T) {
	database, err := db.InitDB(":memory:")
	if err != nil {
		t.Fatalf("InitDB failed: %v", err)
	}
	defer func() { _ = database.Close() }()

	handler := handlePrompt(database, "echo", "", "", "", 1, nil)

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

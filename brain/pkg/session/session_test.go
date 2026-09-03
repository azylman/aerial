package session

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFindLatestSessionDirAndExtract(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)

	convID := "test-conv-123"
	logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("Failed to create temp logs dir: %v", err)
	}

	transcriptData := `{"type":"PLANNER_RESPONSE","status":"DONE","content":"Hello world!"}
{"type":"PLANNER_RESPONSE","status":"ERROR","error":"something went wrong"}`

	tPath := filepath.Join(logsDir, "transcript.jsonl")
	if err := os.WriteFile(tPath, []byte(transcriptData), 0644); err != nil {
		t.Fatalf("Failed to write transcript.jsonl: %v", err)
	}

	latest := FindLatestSessionDir(time.Now().Add(-1 * time.Hour))
	if latest != convID {
		t.Errorf("Expected latest session dir %s, got: %s", convID, latest)
	}

	resp, errStr := ExtractResponseAndError(convID)
	if resp != "Hello world!" {
		t.Errorf("Expected response 'Hello world!', got: '%s'", resp)
	}
	if errStr != `"something went wrong"` {
		t.Errorf("Expected error '\"something went wrong\"', got: '%s'", errStr)
	}

	diag := DumpSessionDiagnosticLogs(convID)
	if diag == "" {
		t.Error("Expected non-empty diagnostic logs")
	}
}

func TestMultiTurnTurnScoping(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)

	convID := "test-multi-turn-456"
	logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("Failed to create temp logs dir: %v", err)
	}

	// Turn 1 succeeded, Turn 2 crashed with no response
	transcriptData := `{"step_index":0,"type":"USER_INPUT","content":"Turn 1"}
{"step_index":1,"type":"PLANNER_RESPONSE","status":"DONE","content":"Turn 1 Response"}
{"step_index":2,"type":"USER_INPUT","content":"Turn 2"}
{"step_index":3,"type":"SYSTEM_MESSAGE","content":"Server restarted"}
{"step_index":4,"type":"PLANNER_RESPONSE","status":"DONE","content":""}`

	tPath := filepath.Join(logsDir, "transcript.jsonl")
	if err := os.WriteFile(tPath, []byte(transcriptData), 0644); err != nil {
		t.Fatalf("Failed to write transcript.jsonl: %v", err)
	}

	resp, errStr := ExtractResponseAndError(convID)
	if resp != "" {
		t.Errorf("Expected empty response for Turn 2, but got Turn 1 response '%s'", resp)
	}
	if errStr != "" {
		t.Errorf("Expected empty error, got '%s'", errStr)
	}

	hasTool := HasSuccessfulToolCall(convID)
	if hasTool {
		t.Error("Expected HasSuccessfulToolCall = false for failed Turn 2")
	}
}

func TestSessionErrorCases(t *testing.T) {
	// Test non-existent conversation ID
	resp, errStr := ExtractResponseAndError("nonexistent-conv-id-999")
	if resp != "" || errStr != "" {
		t.Errorf("Expected empty response/error for nonexistent conv, got resp='%s', err='%s'", resp, errStr)
	}

	// Test invalid HOME directory or inaccessible path
	_ = os.Setenv("HOME", "/proc/unwritable_dir")
	latest := FindLatestSessionDir(time.Now())
	if latest != "" {
		t.Errorf("Expected empty session dir for invalid path, got: %s", latest)
	}

	diag := DumpSessionDiagnosticLogs("nonexistent-conv-id-999")
	if diag != "" {
		t.Errorf("Expected empty diag log for nonexistent conv, got: %s", diag)
	}
}

func TestAppendAmbientTurn_NoopWhenMissing(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)

	// Case 1: Empty session ID
	err := AppendAmbientTurn("", "lounge", "Alice", "Hello", time.Now())
	if err != nil {
		t.Fatalf("expected nil for empty sessionID, got %v", err)
	}

	// Case 2: Non-existent session ID
	err = AppendAmbientTurn("non-existent-session-id", "lounge", "Alice", "Hello", time.Now())
	if err != nil {
		t.Fatalf("expected nil for non-existent sessionID, got %v", err)
	}

	// Verify no directory or file was created in tmpDir
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to read tmpDir: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("expected no entries in tmpDir, got %d", len(entries))
	}
}

func TestAppendAmbientTurn_EmptyFiles(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)

	sessionID := "session-empty-files"
	sessDir, err := EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}

	fixedTime := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	err = AppendAmbientTurn(sessionID, "#lounge", "Alice", "Hello ambient world", fixedTime)
	if err != nil {
		t.Fatalf("AppendAmbientTurn failed: %v", err)
	}

	tPath := filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl")
	data, err := os.ReadFile(tPath)
	if err != nil {
		t.Fatalf("failed to read transcript.jsonl: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 1 {
		t.Fatalf("expected 1 line, got %d: %q", len(lines), string(data))
	}

	var step TranscriptStep
	if err := json.Unmarshal([]byte(lines[0]), &step); err != nil {
		t.Fatalf("failed to unmarshal step: %v", err)
	}

	if step.StepIndex != 0 {
		t.Errorf("expected step_index 0, got %d", step.StepIndex)
	}
	if step.Source != "USER_EXPLICIT" {
		t.Errorf("expected source USER_EXPLICIT, got %s", step.Source)
	}
	if step.Type != "USER_INPUT" {
		t.Errorf("expected type USER_INPUT, got %s", step.Type)
	}
	if step.Status != "DONE" {
		t.Errorf("expected status DONE, got %s", step.Status)
	}
	expectedContent := "[Chat #lounge] @Alice (2026-09-02T12:00:00Z): Hello ambient world"
	if step.Content != expectedContent {
		t.Errorf("expected content %q, got %q", expectedContent, step.Content)
	}
	if step.CreatedAt != "2026-09-02T12:00:00Z" {
		t.Errorf("expected created_at 2026-09-02T12:00:00Z, got %s", step.CreatedAt)
	}
}

func TestAppendAmbientTurn_Monotonic(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)

	sessionID := "session-monotonic"
	sessDir, err := EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}

	logsDir := filepath.Join(sessDir, ".system_generated", "logs")
	seedData := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-09-02T10:00:00Z","content":"Seed 0"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-09-02T10:00:01Z","content":"Seed 1 response"}
`
	for _, name := range []string{"transcript.jsonl", "transcript_full.jsonl"} {
		if err := os.WriteFile(filepath.Join(logsDir, name), []byte(seedData), 0644); err != nil {
			t.Fatalf("failed to seed %s: %v", name, err)
		}
	}

	t1 := time.Date(2026, 9, 2, 10, 5, 0, 0, time.UTC)
	t2 := time.Date(2026, 9, 2, 10, 6, 0, 0, time.UTC)

	if err := AppendAmbientTurn(sessionID, "dev", "Bob", "Turn 2 text", t1); err != nil {
		t.Fatalf("failed to append turn 2: %v", err)
	}
	if err := AppendAmbientTurn(sessionID, "dev", "Charlie", "Turn 3 text", t2); err != nil {
		t.Fatalf("failed to append turn 3: %v", err)
	}

	tPath := filepath.Join(logsDir, "transcript.jsonl")
	data, err := os.ReadFile(tPath)
	if err != nil {
		t.Fatalf("failed to read transcript.jsonl: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 4 {
		t.Fatalf("expected 4 lines, got %d", len(lines))
	}

	for i, line := range lines {
		var step TranscriptStep
		if err := json.Unmarshal([]byte(line), &step); err != nil {
			t.Fatalf("line %d failed to unmarshal: %v", i, err)
		}
		if step.StepIndex != i {
			t.Errorf("expected line %d to have step_index %d, got %d", i, i, step.StepIndex)
		}
	}
}

func TestAppendAmbientTurn_DualSync(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)

	sessionID := "session-dual-sync"
	sessDir, err := EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}

	now := time.Now().UTC()
	turns := []struct {
		channel string
		author  string
		text    string
		t       time.Time
	}{
		{"#lounge", "Alice", "Message 1", now},
		{"random", "Bob", "Message 2", now.Add(1 * time.Minute)},
		{"###general", "Charlie", "Message 3", now.Add(2 * time.Minute)},
	}

	for _, turn := range turns {
		if err := AppendAmbientTurn(sessionID, turn.channel, turn.author, turn.text, turn.t); err != nil {
			t.Fatalf("AppendAmbientTurn failed: %v", err)
		}
	}

	logsDir := filepath.Join(sessDir, ".system_generated", "logs")
	dataTranscript, err := os.ReadFile(filepath.Join(logsDir, "transcript.jsonl"))
	if err != nil {
		t.Fatalf("failed to read transcript.jsonl: %v", err)
	}
	dataFull, err := os.ReadFile(filepath.Join(logsDir, "transcript_full.jsonl"))
	if err != nil {
		t.Fatalf("failed to read transcript_full.jsonl: %v", err)
	}

	if !bytes.Equal(dataTranscript, dataFull) {
		t.Errorf("transcript.jsonl and transcript_full.jsonl are not byte-for-byte identical:\nTranscript:\n%s\nFull:\n%s", string(dataTranscript), string(dataFull))
	}

	// Verify channel cleaning
	if !strings.Contains(string(dataTranscript), "[Chat #lounge]") {
		t.Error("expected cleaned channel #lounge")
	}
	if !strings.Contains(string(dataTranscript), "[Chat #random]") {
		t.Error("expected cleaned channel #random")
	}
	if !strings.Contains(string(dataTranscript), "[Chat #general]") {
		t.Error("expected cleaned channel #general from ###general")
	}
}

func TestAppendAmbientTurn_MissingTrailingNewline(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)

	sessionID := "session-missing-newline"
	sessDir, err := EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}

	logsDir := filepath.Join(sessDir, ".system_generated", "logs")
	rawLine := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-09-02T12:00:00Z","content":"Line without newline"}`
	// Note: explicitly NO trailing newline!
	for _, name := range []string{"transcript.jsonl", "transcript_full.jsonl"} {
		if err := os.WriteFile(filepath.Join(logsDir, name), []byte(rawLine), 0644); err != nil {
			t.Fatalf("failed to write %s: %v", name, err)
		}
	}

	now := time.Date(2026, 9, 2, 12, 1, 0, 0, time.UTC)
	if err := AppendAmbientTurn(sessionID, "lounge", "Bob", "Second line", now); err != nil {
		t.Fatalf("AppendAmbientTurn failed: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(logsDir, "transcript.jsonl"))
	if err != nil {
		t.Fatalf("failed to read transcript.jsonl: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 lines, got %d: %q", len(lines), string(data))
	}

	var step0, step1 TranscriptStep
	if err := json.Unmarshal([]byte(lines[0]), &step0); err != nil {
		t.Fatalf("line 0 unmarshal failed: %v", err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &step1); err != nil {
		t.Fatalf("line 1 unmarshal failed: %v", err)
	}

	if step0.StepIndex != 0 {
		t.Errorf("expected step0 step_index 0, got %d", step0.StepIndex)
	}
	if step1.StepIndex != 1 {
		t.Errorf("expected step1 step_index 1, got %d", step1.StepIndex)
	}
}

func TestEnsureSessionDir(t *testing.T) {
	tmpDir := t.TempDir()
	_ = os.Setenv("HOME", tmpDir)

	// Error case: empty sessionID
	_, err := EnsureSessionDir("")
	if err == nil {
		t.Error("expected error for empty sessionID, got nil")
	}

	sessionID := "session-bootstrap-check"
	sessDir, err := EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}

	if !strings.Contains(sessDir, sessionID) {
		t.Errorf("expected sessDir to contain %s, got %s", sessionID, sessDir)
	}

	logsDir := filepath.Join(sessDir, ".system_generated", "logs")
	fi, err := os.Stat(logsDir)
	if err != nil {
		t.Fatalf("expected logsDir to exist: %v", err)
	}
	if !fi.IsDir() {
		t.Fatalf("expected logsDir to be a directory")
	}

	for _, name := range []string{"transcript.jsonl", "transcript_full.jsonl"} {
		p := filepath.Join(logsDir, name)
		finfo, err := os.Stat(p)
		if err != nil {
			t.Fatalf("expected %s to exist: %v", name, err)
		}
		if finfo.Size() != 0 {
			t.Errorf("expected empty file %s, got size %d", name, finfo.Size())
		}
	}

	// Verify idempotency: call EnsureSessionDir again, should not truncate existing data
	testData := []byte(`{"step_index":0}`)
	_ = os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), testData, 0644)

	sessDir2, err := EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("second EnsureSessionDir call failed: %v", err)
	}
	if sessDir2 != sessDir {
		t.Errorf("expected identical sessDir %s, got %s", sessDir, sessDir2)
	}

	readBack, _ := os.ReadFile(filepath.Join(logsDir, "transcript.jsonl"))
	if string(readBack) != string(testData) {
		t.Errorf("EnsureSessionDir wiped existing transcript data: %s", string(readBack))
	}
}



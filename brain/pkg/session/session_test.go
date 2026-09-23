package session

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func setupTestManager(t *testing.T) (*Manager, string) {
	t.Helper()
	tmpDir := t.TempDir()
	mgr := New(tmpDir, "")
	return mgr, tmpDir
}

func TestFindLatestSessionDirAndExtract(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)

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

	latest := mgr.FindLatestSessionDir(time.Now().Add(-1 * time.Hour))
	if latest != convID {
		t.Errorf("Expected latest session dir %s, got: %s", convID, latest)
	}

	resp, errStr := mgr.ExtractResponseAndError(convID)
	if resp != "Hello world!" {
		t.Errorf("Expected response 'Hello world!', got: '%s'", resp)
	}
	if errStr != "something went wrong" {
		t.Errorf("Expected error 'something went wrong', got: '%s'", errStr)
	}

	diag := mgr.DumpSessionDiagnosticLogs(convID)
	if diag == "" {
		t.Error("Expected non-empty diagnostic logs")
	}
}

func TestMultiTurnTurnScoping(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)

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

	resp, errStr := mgr.ExtractResponseAndError(convID)
	if resp != "" {
		t.Errorf("Expected empty response for Turn 2, but got Turn 1 response '%s'", resp)
	}
	if errStr != "" {
		t.Errorf("Expected empty error, got '%s'", errStr)
	}

	hasTool := mgr.HasSuccessfulToolCall(convID)
	if hasTool {
		t.Error("Expected HasSuccessfulToolCall = false for failed Turn 2")
	}
}

func TestSessionErrorCases(t *testing.T) {
	// Test non-existent conversation ID on empty manager
	mEmpty := New("", "")
	resp, errStr := mEmpty.ExtractResponseAndError("nonexistent-conv-id-999")
	if resp != "" || errStr != "" {
		t.Errorf("Expected empty response/error for nonexistent conv, got resp='%s', err='%s'", resp, errStr)
	}

	// Test invalid HOME directory or inaccessible path with injected path
	mInvalid := New("/proc/unwritable_dir", "")
	latest := mInvalid.FindLatestSessionDir(time.Now())
	if latest != "" {
		t.Errorf("Expected empty session dir for invalid path, got: %s", latest)
	}

	diag := mInvalid.DumpSessionDiagnosticLogs("nonexistent-conv-id-999")
	if diag != "" {
		t.Errorf("Expected empty diag log for nonexistent conv, got: %s", diag)
	}

	// Test nil manager safety
	var nilMgr *Manager
	if nilLatest := nilMgr.FindLatestSessionDir(time.Now()); nilLatest != "" {
		t.Errorf("Expected empty string from nilMgr.FindLatestSessionDir, got: %s", nilLatest)
	}
	if nilDiag := nilMgr.DumpSessionDiagnosticLogs("conv-123"); nilDiag != "" {
		t.Errorf("Expected empty string from nilMgr.DumpSessionDiagnosticLogs, got: %s", nilDiag)
	}
	if nilResp, nilErr := nilMgr.ExtractResponseAndError("conv-123"); nilResp != "" || nilErr != "" {
		t.Errorf("Expected empty response/err from nilMgr.ExtractResponseAndError, got resp=%q, err=%q", nilResp, nilErr)
	}
	if nilHasTool := nilMgr.HasSuccessfulToolCall("conv-123"); nilHasTool {
		t.Errorf("Expected false from nilMgr.HasSuccessfulToolCall")
	}
	if nilExists := nilMgr.SessionExistsOnDisk("conv-123"); nilExists {
		t.Errorf("Expected false from nilMgr.SessionExistsOnDisk")
	}
	if _, nilEnsureErr := nilMgr.EnsureSessionDir("conv-123"); nilEnsureErr == nil {
		t.Errorf("Expected error from nilMgr.EnsureSessionDir, got nil")
	}
	if nilAppendErr := nilMgr.AppendAmbientTurn("conv-123", "chan", "auth", "text", time.Now()); nilAppendErr != nil {
		t.Errorf("Expected nil error from nilMgr.AppendAmbientTurn, got %v", nilAppendErr)
	}
}

func TestAppendAmbientTurn_NoopWhenMissing(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)

	// Case 1: Empty session ID
	err := mgr.AppendAmbientTurn("", "lounge", "Alice", "Hello", time.Now())
	if err != nil {
		t.Fatalf("expected nil for empty sessionID, got %v", err)
	}

	// Case 2: Non-existent session ID
	err = mgr.AppendAmbientTurn("non-existent-session-id", "lounge", "Alice", "Hello", time.Now())
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
	mgr, _ := setupTestManager(t)

	sessionID := uuid.New().String()
	sessDir, err := mgr.EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sessDir) })

	fixedTime := time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)
	err = mgr.AppendAmbientTurn(sessionID, "#lounge", "Alice", "Hello ambient world", fixedTime)
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
	if step.Source != SourceAmbient {
		t.Errorf("expected source %s, got %s", SourceAmbient, step.Source)
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
	mgr, _ := setupTestManager(t)

	sessionID := uuid.New().String()
	sessDir, err := mgr.EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sessDir) })

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

	if err := mgr.AppendAmbientTurn(sessionID, "dev", "Bob", "Turn 2 text", t1); err != nil {
		t.Fatalf("failed to append turn 2: %v", err)
	}
	if err := mgr.AppendAmbientTurn(sessionID, "dev", "Charlie", "Turn 3 text", t2); err != nil {
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
	mgr, _ := setupTestManager(t)

	sessionID := uuid.New().String()
	sessDir, err := mgr.EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sessDir) })

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
		if err := mgr.AppendAmbientTurn(sessionID, turn.channel, turn.author, turn.text, turn.t); err != nil {
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
	mgr, _ := setupTestManager(t)

	sessionID := uuid.New().String()
	sessDir, err := mgr.EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sessDir) })

	logsDir := filepath.Join(sessDir, ".system_generated", "logs")
	rawLine := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","status":"DONE","created_at":"2026-09-02T12:00:00Z","content":"Line without newline"}`
	// Note: explicitly NO trailing newline!
	for _, name := range []string{"transcript.jsonl", "transcript_full.jsonl"} {
		if err := os.WriteFile(filepath.Join(logsDir, name), []byte(rawLine), 0644); err != nil {
			t.Fatalf("failed to write %s: %v", name, err)
		}
	}

	now := time.Date(2026, 9, 2, 12, 1, 0, 0, time.UTC)
	if err := mgr.AppendAmbientTurn(sessionID, "lounge", "Bob", "Second line", now); err != nil {
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
	mgr, _ := setupTestManager(t)

	// Error case: empty sessionID
	_, err := mgr.EnsureSessionDir("")
	if err == nil {
		t.Error("expected error for empty sessionID, got nil")
	}

	// Error case: empty roots on manager
	mNoRoots := New("", "")
	_, err = mNoRoots.EnsureSessionDir("sess-1")
	if err == nil {
		t.Error("expected error for EnsureSessionDir with no roots configured, got nil")
	}

	sessionID := uuid.New().String()
	sessDir, err := mgr.EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sessDir) })

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

	sessDir2, err := mgr.EnsureSessionDir(sessionID)
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

func TestAppendAmbientTurn_SanitizationAndNormalization(t *testing.T) {
	mgr, _ := setupTestManager(t)

	sessionID := uuid.New().String()
	sessDir, err := mgr.EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sessDir) })

	// Non-UTC timezone (e.g. UTC-7)
	loc := time.FixedZone("PDT", -7*60*60)
	localTime := time.Date(2026, 9, 2, 10, 30, 0, 0, loc)

	// Channel with extra spaces and #, author with @ and spaces
	err = mgr.AppendAmbientTurn(sessionID, "  ###special-room  ", "  @BobTheBuilder  ", "Sanitized content", localTime)
	if err != nil {
		t.Fatalf("AppendAmbientTurn failed: %v", err)
	}

	tPath := filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl")
	data, err := os.ReadFile(tPath)
	if err != nil {
		t.Fatalf("failed to read transcript.jsonl: %v", err)
	}

	var step TranscriptStep
	if err := json.Unmarshal(bytes.TrimSpace(data), &step); err != nil {
		t.Fatalf("failed to unmarshal step: %v", err)
	}

	// Timestamp must be normalized to UTC (10:30 PDT = 17:30 UTC)
	if step.CreatedAt != "2026-09-02T17:30:00Z" {
		t.Errorf("expected UTC timestamp 2026-09-02T17:30:00Z, got %s", step.CreatedAt)
	}

	// Content must have clean channel and clean author without double @
	expectedContent := "[Chat #special-room] @BobTheBuilder (2026-09-02T17:30:00Z): Sanitized content"
	if step.Content != expectedContent {
		t.Errorf("expected content %q, got %q", expectedContent, step.Content)
	}
}

func TestAppendAmbientTurn_LargeLineSeek(t *testing.T) {
	mgr, _ := setupTestManager(t)

	sessionID := uuid.New().String()
	sessDir, err := mgr.EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sessDir) })

	logsDir := filepath.Join(sessDir, ".system_generated", "logs")

	// Create a huge line (>70KB) that has step_index: 42
	hugePadding := strings.Repeat("x", 80000)
	hugeStep := fmt.Sprintf(`{"step_index":42,"source":"MODEL","type":"PLANNER_RESPONSE","status":"DONE","created_at":"2026-09-02T12:00:00Z","content":%q}`+"\n", hugePadding)

	for _, name := range []string{"transcript.jsonl", "transcript_full.jsonl"} {
		if err := os.WriteFile(filepath.Join(logsDir, name), []byte(hugeStep), 0644); err != nil {
			t.Fatalf("failed to write %s: %v", name, err)
		}
	}

	// Now append an ambient turn: it should scan past the 80KB line and find step_index: 42, making next index 43!
	err = mgr.AppendAmbientTurn(sessionID, "general", "Alice", "After large step", time.Now())
	if err != nil {
		t.Fatalf("AppendAmbientTurn failed: %v", err)
	}

	lastIdx, err := getLastStepIndex(filepath.Join(logsDir, "transcript.jsonl"))
	if err != nil {
		t.Fatalf("getLastStepIndex failed: %v", err)
	}
	if lastIdx != 43 {
		t.Errorf("expected step_index 43, got %d", lastIdx)
	}
}

func TestAppendAmbientTurn_Concurrent(t *testing.T) {
	mgr, _ := setupTestManager(t)

	sessionID := uuid.New().String()
	sessDir, err := mgr.EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sessDir) })

	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(idx int) {
			defer wg.Done()
			_ = mgr.AppendAmbientTurn(sessionID, "lounge", fmt.Sprintf("User%d", idx), fmt.Sprintf("Msg %d", idx), time.Now())
		}(i)
	}
	wg.Wait()

	tPath := filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl")
	data, err := os.ReadFile(tPath)
	if err != nil {
		t.Fatalf("failed to read transcript.jsonl: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != n {
		t.Fatalf("expected %d lines, got %d", n, len(lines))
	}

	// Verify all step indexes from 0 to n-1 are present without duplicates or gaps
	seen := make(map[int]bool)
	for _, line := range lines {
		var step TranscriptStep
		if err := json.Unmarshal([]byte(line), &step); err != nil {
			t.Fatalf("failed to unmarshal line: %v", err)
		}
		seen[step.StepIndex] = true
	}

	for i := 0; i < n; i++ {
		if !seen[i] {
			t.Errorf("expected step_index %d to be present", i)
		}
	}
}

func TestSessionExistsOnDisk(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)

	// 1. Empty or whitespace session ID -> returns false
	emptyCases := []string{"", "   ", "\t", "\n", " \t \n "}
	for _, id := range emptyCases {
		if mgr.SessionExistsOnDisk(id) {
			t.Errorf("expected SessionExistsOnDisk(%q) to be false", id)
		}
	}

	// 2. Path traversal attempts -> returns false
	traversalCases := []string{
		"../../etc/passwd",
		"foo/bar",
		`foo\\bar`,
		"../foo",
		"foo/../bar",
		`..\\foo`,
		"/etc/passwd",
		`C:\\Windows`,
	}
	for _, id := range traversalCases {
		if mgr.SessionExistsOnDisk(id) {
			t.Errorf("expected SessionExistsOnDisk(%q) to be false", id)
		}
	}

	// 3. Non-existent session ID -> returns false
	if mgr.SessionExistsOnDisk("non-existent-session-id-999") {
		t.Errorf("expected SessionExistsOnDisk for non-existent session to be false")
	}

	// 4. Directory with 0-byte transcript.jsonl -> returns false
	zeroByteID := "session-zero-byte"
	zeroDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", zeroByteID, ".system_generated", "logs")
	if err := os.MkdirAll(zeroDir, 0755); err != nil {
		t.Fatalf("failed to create zeroDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(zeroDir, "transcript.jsonl"), []byte{}, 0644); err != nil {
		t.Fatalf("failed to write 0-byte transcript: %v", err)
	}
	if mgr.SessionExistsOnDisk(zeroByteID) {
		t.Errorf("expected SessionExistsOnDisk(%q) with 0-byte transcript to be false", zeroByteID)
	}

	// 5. Directory with valid non-empty transcript.jsonl -> returns true
	validTranscriptID := "session-valid-transcript"
	validDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", validTranscriptID, ".system_generated", "logs")
	if err := os.MkdirAll(validDir, 0755); err != nil {
		t.Fatalf("failed to create validDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(validDir, "transcript.jsonl"), []byte(`{"step_index":0}`+"\n"), 0644); err != nil {
		t.Fatalf("failed to write valid transcript: %v", err)
	}
	if !mgr.SessionExistsOnDisk(validTranscriptID) {
		t.Errorf("expected SessionExistsOnDisk(%q) with valid transcript to be true", validTranscriptID)
	}

	// 6. Existing non-empty .pb file in ~/.gemini/antigravity-cli/conversations/<id>.pb -> returns true
	pbCliID := "session-valid-cli-pb"
	cliPbDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "conversations")
	if err := os.MkdirAll(cliPbDir, 0755); err != nil {
		t.Fatalf("failed to create cliPbDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(cliPbDir, pbCliID+".pb"), []byte("mock-protobuf-cli"), 0644); err != nil {
		t.Fatalf("failed to write cli .pb: %v", err)
	}
	if !mgr.SessionExistsOnDisk(pbCliID) {
		t.Errorf("expected SessionExistsOnDisk(%q) with cli .pb to be true", pbCliID)
	}

	// 7. Existing non-empty .pb file in ~/.gemini/antigravity/conversations/<id>.pb -> returns true
	pbAgyID := "session-valid-agy-pb"
	agyPbDir := filepath.Join(tmpDir, ".gemini", "antigravity", "conversations")
	if err := os.MkdirAll(agyPbDir, 0755); err != nil {
		t.Fatalf("failed to create agyPbDir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(agyPbDir, pbAgyID+".pb"), []byte("mock-protobuf-agy"), 0644); err != nil {
		t.Fatalf("failed to write agy .pb: %v", err)
	}
	if !mgr.SessionExistsOnDisk(pbAgyID) {
		t.Errorf("expected SessionExistsOnDisk(%q) with agy .pb to be true", pbAgyID)
	}

	// 8. 0-byte .pb file -> returns false
	pbZeroID := "session-zero-pb"
	if err := os.WriteFile(filepath.Join(cliPbDir, pbZeroID+".pb"), []byte{}, 0644); err != nil {
		t.Fatalf("failed to write zero-byte cli .pb: %v", err)
	}
	if mgr.SessionExistsOnDisk(pbZeroID) {
		t.Errorf("expected SessionExistsOnDisk(%q) with 0-byte .pb to be false", pbZeroID)
	}
}

func TestSessionPackage_TargetDirsAndBaseDir(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)

	// 1. Create brain roots with session directories
	cliBrainRoot := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain")
	agyBrainRoot := filepath.Join(tmpDir, ".gemini", "antigravity", "brain")
	_ = os.MkdirAll(cliBrainRoot, 0755)
	_ = os.MkdirAll(agyBrainRoot, 0755)

	sess1 := filepath.Join(cliBrainRoot, "sess-1")
	sess2 := filepath.Join(agyBrainRoot, "sess-2")
	_ = os.MkdirAll(sess1, 0755)
	_ = os.MkdirAll(sess2, 0755)

	// Test getTargetDirs with empty convID (searches brain roots)
	dirs := mgr.getTargetDirs("")
	if len(dirs) < 2 {
		t.Errorf("expected at least 2 target dirs for empty convID, got %v", dirs)
	}

	// Test resolveBaseDir returns valid non-empty directory
	baseDir := mgr.resolveBaseDir("sess-2")
	if baseDir == "" {
		t.Errorf("expected non-empty baseDir for sess-2")
	}

	baseDirSess2 := mgr.resolveBaseDir("sess-2")
	if baseDirSess2 != agyBrainRoot {
		t.Errorf("expected baseDir %s, got %s", agyBrainRoot, baseDirSess2)
	}

	baseDirNew := mgr.resolveBaseDir("new-session-xyz")
	if baseDirNew != cliBrainRoot {
		t.Errorf("expected baseDir %s, got %s", cliBrainRoot, baseDirNew)
	}
}

func TestHasSuccessfulToolCall_ToolExecuted(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)

	convID := "tool-test-123"
	logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	_ = os.MkdirAll(logsDir, 0755)

	transcript := `{"step_index":0,"type":"USER_INPUT","content":"Run tool"}
{"step_index":1,"type":"MCP_TOOL","status":"DONE","content":"Tool result"}
{"step_index":2,"type":"PLANNER_RESPONSE","status":"DONE","content":"Tool finished"}
`
	_ = os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(transcript), 0644)

	if !mgr.HasSuccessfulToolCall(convID) {
		t.Errorf("expected HasSuccessfulToolCall = true when MCP_TOOL status DONE is present")
	}
}

func TestGetLastStepIndex_EdgeCases(t *testing.T) {
	tmpDir := t.TempDir()

	// 1. Non-existent file
	idx, err := getLastStepIndex(filepath.Join(tmpDir, "non_existent.jsonl"))
	if err != nil || idx != -1 {
		t.Errorf("expected -1, nil for non-existent file, got %d, %v", idx, err)
	}

	// 2. Empty file
	emptyFile := filepath.Join(tmpDir, "empty.jsonl")
	_ = os.WriteFile(emptyFile, []byte(""), 0644)
	idx, err = getLastStepIndex(emptyFile)
	if err != nil || idx != -1 {
		t.Errorf("expected -1, nil for empty file, got %d, %v", idx, err)
	}

	// 3. File with invalid JSON lines
	badFile := filepath.Join(tmpDir, "bad.jsonl")
	_ = os.WriteFile(badFile, []byte("invalid json line\n"), 0644)
	idx, err = getLastStepIndex(badFile)
	if err != nil || idx != -1 {
		t.Errorf("expected -1, nil for invalid json file, got %d, %v", idx, err)
	}
}

func TestDumpSessionDiagnosticLogs_LongLogFile(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)

	convID := "diag-long-123"
	logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	_ = os.MkdirAll(logsDir, 0755)

	var sb strings.Builder
	for i := 0; i < 75; i++ {
		sb.WriteString(fmt.Sprintf("log line %d\n", i))
	}
	_ = os.WriteFile(filepath.Join(logsDir, "output.log"), []byte(sb.String()), 0644)

	diag := mgr.DumpSessionDiagnosticLogs(convID)
	if !strings.Contains(diag, "[...showing last 50 lines...]") {
		t.Errorf("expected truncation notice in diagnostic output, got: %s", diag)
	}
}

func TestFindLatestSessionDir_WithRegularFiles(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)

	brainDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain")
	_ = os.MkdirAll(brainDir, 0755)

	// Create a regular file in brain directory
	_ = os.WriteFile(filepath.Join(brainDir, "notes.txt"), []byte("not a session"), 0644)

	// Create a session dir
	sessDir := filepath.Join(brainDir, "sess-real")
	_ = os.MkdirAll(sessDir, 0755)

	latest := mgr.FindLatestSessionDir(time.Now().Add(-1 * time.Hour))
	if latest != "sess-real" {
		t.Errorf("expected latest session 'sess-real', got %q", latest)
	}
}

func TestAppendTranscriptStep_Errors(t *testing.T) {
	err := appendTranscriptStep("/dev/null/impossible/transcript.jsonl", []byte("data"))
	if err == nil {
		t.Error("expected error for impossible transcript path")
	}
}

func TestAppendAmbientTurn_ZeroTimestamp(t *testing.T) {
	mgr, _ := setupTestManager(t)

	sessionID := uuid.New().String()
	sessDir, err := mgr.EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sessDir) })

	// Call with zero time
	err = mgr.AppendAmbientTurn(sessionID, "general", "Alice", "Zero time msg", time.Time{})
	if err != nil {
		t.Fatalf("AppendAmbientTurn with zero time failed: %v", err)
	}
}

func TestExtractResponseAndError_LegacyAndModernAmbientTurns(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)

	convID := "test-conv-ambient-filter"
	logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("Failed to create temp logs dir: %v", err)
	}

	// 1. Test Modern ambient turn with Source == SourceAmbient
	modernTranscript := `{"step_index":0,"type":"USER_INPUT","source":"USER_EXPLICIT","content":"Turn 1 user request"}
{"step_index":1,"type":"PLANNER_RESPONSE","status":"DONE","content":"Modern Turn 1 Response"}
{"step_index":2,"type":"USER_INPUT","source":"AMBIENT","content":"[Chat #general] @Bob (2026-09-06T12:00:00Z): modern ambient msg"}
`
	tPath := filepath.Join(logsDir, "transcript.jsonl")
	if err := os.WriteFile(tPath, []byte(modernTranscript), 0644); err != nil {
		t.Fatalf("Failed to write transcript.jsonl: %v", err)
	}

	resp, errStr := mgr.ExtractResponseAndError(convID)
	if resp != "Modern Turn 1 Response" {
		t.Errorf("Expected response 'Modern Turn 1 Response', got: %q (err: %s)", resp, errStr)
	}

	// 2. Test Legacy ambient turn with Source == USER_EXPLICIT but content starting with "[Chat #"
	legacyTranscript := `{"step_index":0,"type":"USER_INPUT","source":"USER_EXPLICIT","content":"Turn 1 user request"}
{"step_index":1,"type":"PLANNER_RESPONSE","status":"DONE","content":"Legacy Turn 1 Response"}
{"step_index":2,"type":"USER_INPUT","source":"USER_EXPLICIT","content":"[Chat #lounge] @Charlie (2026-09-06T12:00:00Z): legacy ambient msg"}
`
	if err := os.WriteFile(tPath, []byte(legacyTranscript), 0644); err != nil {
		t.Fatalf("Failed to write transcript.jsonl: %v", err)
	}

	resp, errStr = mgr.ExtractResponseAndError(convID)
	if resp != "Legacy Turn 1 Response" {
		t.Errorf("Expected response 'Legacy Turn 1 Response', got: %q (err: %s)", resp, errStr)
	}

	// 3. Test genuine new user turn (non-ambient) - must correctly scope and return empty response
	newTurnTranscript := legacyTranscript + `{"step_index":3,"type":"USER_INPUT","source":"USER_EXPLICIT","content":"Turn 2 genuine request"}
`
	if err := os.WriteFile(tPath, []byte(newTurnTranscript), 0644); err != nil {
		t.Fatalf("Failed to write transcript.jsonl: %v", err)
	}

	resp, errStr = mgr.ExtractResponseAndError(convID)
	if resp != "" {
		t.Errorf("Expected empty response for incomplete Turn 2, got: %q", resp)
	}
}

func TestGetSessionLastActivity(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)

	// 1. Empty or whitespace session ID -> zero time, nil error
	for _, emptyID := range []string{"", "   ", "	", "\n"} {
		tAct, err := mgr.GetSessionLastActivity(emptyID)
		if err != nil || !tAct.IsZero() {
			t.Errorf("expected zero time, nil err for %q, got %v, %v", emptyID, tAct, err)
		}
	}

	// 2. Path traversal attempts -> zero time, nil error
	for _, traversalID := range []string{"../../etc/passwd", "foo/bar", `foo\\bar`, "../foo", "foo/../bar"} {
		tAct, err := mgr.GetSessionLastActivity(traversalID)
		if err != nil || !tAct.IsZero() {
			t.Errorf("expected zero time, nil err for traversal %q, got %v, %v", traversalID, tAct, err)
		}
	}

	// 3. Non-existent session -> zero time, nil error
	tAct, err := mgr.GetSessionLastActivity("non-existent-session-xyz")
	if err != nil || !tAct.IsZero() {
		t.Errorf("expected zero time, nil err for non-existent session, got %v, %v", tAct, err)
	}

	// 4. Session with only transcripts
	sessionID := "test-session-activity-1"
	sessRoot := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessionID)
	logsDir := filepath.Join(sessRoot, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("failed to create logsDir: %v", err)
	}

	t1 := time.Now().Add(-10 * time.Minute).Truncate(time.Second)
	tPath := filepath.Join(logsDir, "transcript.jsonl")
	if err := os.WriteFile(tPath, []byte(`{"step": 1}`), 0644); err != nil {
		t.Fatalf("failed to write transcript: %v", err)
	}
	_ = os.Chtimes(tPath, t1, t1)

	actTime, err := mgr.GetSessionLastActivity(sessionID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !actTime.Equal(t1) {
		t.Errorf("expected activity time %v, got %v", t1, actTime)
	}

	// 5. Session with task logs
	tasksDir := filepath.Join(sessRoot, ".system_generated", "tasks")
	if err := os.MkdirAll(tasksDir, 0755); err != nil {
		t.Fatalf("failed to create tasksDir: %v", err)
	}

	t2 := time.Now().Add(-3 * time.Minute).Truncate(time.Second)
	taskLog1 := filepath.Join(tasksDir, "task-1.log")
	if err := os.WriteFile(taskLog1, []byte("build started\n"), 0644); err != nil {
		t.Fatalf("failed to write taskLog1: %v", err)
	}
	_ = os.Chtimes(taskLog1, t2, t2)

	// Non-log files and subdirectories in tasks/ should be ignored
	_ = os.WriteFile(filepath.Join(tasksDir, "notes.txt"), []byte("ignored"), 0644)
	_ = os.MkdirAll(filepath.Join(tasksDir, "subfolder.log"), 0755)

	actTime, err = mgr.GetSessionLastActivity(sessionID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !actTime.Equal(t2) {
		t.Errorf("expected activity time to advance to task-1 %v, got %v", t2, actTime)
	}

	// 6. Update task log with newer timestamp
	t3 := time.Now().Add(-30 * time.Second).Truncate(time.Second)
	taskLog2 := filepath.Join(tasksDir, "task-2.log")
	if err := os.WriteFile(taskLog2, []byte("tests running\n"), 0644); err != nil {
		t.Fatalf("failed to write taskLog2: %v", err)
	}
	_ = os.Chtimes(taskLog2, t3, t3)

	actTime, err = mgr.GetSessionLastActivity(sessionID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !actTime.Equal(t3) {
		t.Errorf("expected activity time to advance to task-2 %v, got %v", t3, actTime)
	}

	// 7. Custom roots passed variadically
	customRoot := t.TempDir()
	customSessID := "custom-sess-root-123"
	customLogs := filepath.Join(customRoot, customSessID, ".system_generated", "logs")
	_ = os.MkdirAll(customLogs, 0755)
	tCustom := time.Now().Add(-5 * time.Second).Truncate(time.Second)
	cPath := filepath.Join(customLogs, "transcript.jsonl")
	_ = os.WriteFile(cPath, []byte("custom root"), 0644)
	_ = os.Chtimes(cPath, tCustom, tCustom)

	// Normal lookup won't find custom root
	normTime, _ := mgr.GetSessionLastActivity(customSessID)
	if !normTime.IsZero() {
		t.Errorf("expected zero time in standard roots for custom sess, got %v", normTime)
	}

	// Variadic custom roots find it directly
	customAct, err := mgr.GetSessionLastActivity(customSessID, customRoot)
	if err != nil {
		t.Fatalf("unexpected error with custom root: %v", err)
	}
	if !customAct.Equal(tCustom) {
		t.Errorf("expected %v with custom root, got %v", tCustom, customAct)
	}

	// Direct call to pure LastActivityFromRoots
	pureAct, err := LastActivityFromRoots(customSessID, []string{customRoot})
	if err != nil {
		t.Fatalf("unexpected error from LastActivityFromRoots: %v", err)
	}
	if !pureAct.Equal(tCustom) {
		t.Errorf("expected %v from LastActivityFromRoots, got %v", tCustom, pureAct)
	}
}

func TestManager_Roots(t *testing.T) {
	// 1. Manager with empty home/data has nil/empty roots
	mEmpty := New("", "")
	if len(mEmpty.Roots()) != 0 {
		t.Errorf("expected 0 roots for empty manager, got %v", mEmpty.Roots())
	}

	// 2. Manager with explicit paths
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	m := New(tmpHome, tmpData)
	roots := m.Roots()
	if len(roots) != 3 {
		t.Fatalf("expected 3 roots, got %d (%v)", len(roots), roots)
	}
	expectedDataRoot := filepath.Join(tmpData, "brain")
	if roots[0] != expectedDataRoot {
		t.Errorf("expected roots[0] == %s, got %s", expectedDataRoot, roots[0])
	}

	// 3. Defensive copy: modifying returned slice must not affect m.roots
	roots[0] = "/mutated"
	if m.Roots()[0] == "/mutated" {
		t.Errorf("Roots() returned a mutable reference to internal slice")
	}

	// 4. Nil receiver safety
	var nilMgr *Manager
	if nilRoots := nilMgr.Roots(); nilRoots != nil {
		t.Errorf("expected nil roots for nil manager, got %v", nilRoots)
	}
}

func TestManager_HomeDir_DataDir(t *testing.T) {
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	m := New(tmpHome, tmpData)
	if m.HomeDir() != tmpHome {
		t.Errorf("expected %s, got %s", tmpHome, m.HomeDir())
	}
	if m.DataDir() != tmpData {
		t.Errorf("expected %s, got %s", tmpData, m.DataDir())
	}

	var nilMgr *Manager
	if nilMgr.HomeDir() != "" {
		t.Errorf("expected empty string for nil HomeDir")
	}
	if nilMgr.DataDir() != "" {
		t.Errorf("expected empty string for nil DataDir")
	}
	if act, err := nilMgr.GetSessionLastActivity("sess"); err != nil || !act.IsZero() {
		t.Errorf("expected zero time for nil manager, got %v, %v", act, err)
	}
}

func TestManager_DumpSessionDiagnosticLogs_DataDirCoverage(t *testing.T) {
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	m := New(tmpHome, tmpData)

	convID := "test-diag-conv"
	dataLogDir := filepath.Join(tmpData, "brain", convID, ".system_generated", "logs")
	_ = os.MkdirAll(dataLogDir, 0755)

	// Add sub-directory inside dataLogDir so entry.IsDir() executes continue
	_ = os.MkdirAll(filepath.Join(dataLogDir, "subdir"), 0755)

	// Add an empty file
	_ = os.WriteFile(filepath.Join(dataLogDir, "empty.log"), []byte(""), 0644)

	// Add a file with content
	_ = os.WriteFile(filepath.Join(dataLogDir, "file1.log"), []byte("log line 1\nlog line 2\n"), 0644)

	diag := m.DumpSessionDiagnosticLogs(convID)
	if !strings.Contains(diag, "log line 1") {
		t.Errorf("expected diag logs to contain log line 1, got %q", diag)
	}
}

func TestAppendAmbientTurn_EdgeCases(t *testing.T) {
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	m := New(tmpHome, tmpData)

	sessID := "sess-edge-ambient"
	// Create session directory under tmpData/brain/sessID/.system_generated/logs
	logsDir := filepath.Join(tmpData, "brain", sessID, ".system_generated", "logs")
	_ = os.MkdirAll(logsDir, 0755)

	// Write transcript without trailing newline
	transcriptPath := filepath.Join(logsDir, "transcript.jsonl")
	_ = os.WriteFile(transcriptPath, []byte(`{"step_index":0,"content":"hello"}`), 0644)

	// Append turn with zero timestamp
	err := m.AppendAmbientTurn(sessID, "general", "alice", "hey there", time.Time{})
	if err != nil {
		t.Fatalf("AppendAmbientTurn failed: %v", err)
	}

	data, err := os.ReadFile(transcriptPath)
	if err != nil {
		t.Fatalf("failed to read transcript: %v", err)
	}
	if !strings.Contains(string(data), "[Chat #general] @alice") {
		t.Errorf("expected ambient turn content, got %s", string(data))
	}
}

func TestGetLastStepIndex_Errors(t *testing.T) {
	tmpDir := t.TempDir()
	emptyFile := filepath.Join(tmpDir, "empty.jsonl")
	_ = os.WriteFile(emptyFile, []byte(""), 0644)
	idx, err := getLastStepIndex(emptyFile)
	if idx != -1 || err != nil {
		t.Errorf("expected -1, nil for empty file, got %d, %v", idx, err)
	}

	nonExistent := filepath.Join(tmpDir, "does_not_exist.jsonl")
	idx, err = getLastStepIndex(nonExistent)
	if idx != -1 || err != nil {
		t.Errorf("expected -1, nil for non-existent file, got %d, %v", idx, err)
	}

	trailingNewlines := filepath.Join(tmpDir, "trailing.jsonl")
	_ = os.WriteFile(trailingNewlines, []byte("{\"step_index\":5}\n\n\n"), 0644)
	idx, err = getLastStepIndex(trailingNewlines)
	if idx != 5 || err != nil {
		t.Errorf("expected 5, nil for trailing newlines, got %d, %v", idx, err)
	}
}

func TestEnsureSessionDir_Coverage(t *testing.T) {
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	m := New(tmpHome, tmpData)

	dir, err := m.EnsureSessionDir("sess-ensure-1")
	if err != nil {
		t.Fatalf("EnsureSessionDir failed: %v", err)
	}
	if !strings.Contains(dir, "sess-ensure-1") {
		t.Errorf("expected session dir to contain sess-ensure-1, got %s", dir)
	}

	// 1. Nil manager
	var nilM *Manager
	if _, err := nilM.EnsureSessionDir("sess"); err == nil {
		t.Error("expected error for nil manager")
	}

	// 2. Empty sessionID
	if _, err := m.EnsureSessionDir(""); err == nil {
		t.Error("expected error for empty sessionID")
	}

	// 3. Manager without roots
	mEmpty := New("", "")
	if _, err := mEmpty.EnsureSessionDir("sess"); err == nil {
		t.Error("expected error for manager without roots")
	}

	// 4. MkdirAll error: blocking file
	blockingFile := filepath.Join(tmpData, "brain", "sess-blocking")
	_ = os.WriteFile(blockingFile, []byte("blocking"), 0644)
	if _, err := m.EnsureSessionDir("sess-blocking"); err == nil {
		t.Error("expected error when file blocks session directory creation")
	}
}

func TestAppendAmbientTurn_NilAndEmpty(t *testing.T) {
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	m := New(tmpHome, tmpData)

	var nilM *Manager
	if err := nilM.AppendAmbientTurn("s", "c", "a", "t", time.Time{}); err != nil {
		t.Errorf("expected nil error for nil manager, got %v", err)
	}
	if err := m.AppendAmbientTurn("", "c", "a", "t", time.Time{}); err != nil {
		t.Errorf("expected nil error for empty sessionID, got %v", err)
	}
	if err := m.AppendAmbientTurn("s", "c", "a", "", time.Time{}); err != nil {
		t.Errorf("expected nil error for empty text, got %v", err)
	}
	// Session where logsDir does not exist
	if err := m.AppendAmbientTurn("non-existent-session-xyz", "c", "a", "t", time.Time{}); err != nil {
		t.Errorf("expected nil error for non-existent session, got %v", err)
	}
}

func TestLastActivityFromRoots_PatternExpansions(t *testing.T) {
	// Empty sessionID or roots
	if act, err := LastActivityFromRoots("", []string{"/tmp"}); err != nil || !act.IsZero() {
		t.Errorf("expected zero time for empty sessionID, got %v, %v", act, err)
	}
	if act, err := LastActivityFromRoots("sess", nil); err != nil || !act.IsZero() {
		t.Errorf("expected zero time for nil roots, got %v, %v", act, err)
	}

	tmpDir := t.TempDir()
	sessID := "pattern-sess-123"
	sessDir := filepath.Join(tmpDir, sessID)
	_ = os.MkdirAll(sessDir, 0755)

	// Roots with %s and {session} and .system_generated/logs suffix
	logsSuffixDir := filepath.Join(sessDir, ".system_generated", "logs")
	_ = os.MkdirAll(logsSuffixDir, 0755)

	roots := []string{
		"", // empty root to hit strings.TrimSpace(root) == "" continue
		filepath.Join(tmpDir, "%s"),
		filepath.Join(tmpDir, "{session}"),
		sessDir,       // cleanExpanded == trimmed
		logsSuffixDir, // strings.HasSuffix .system_generated/logs
	}
	act, err := LastActivityFromRoots(sessID, roots)
	if err != nil {
		t.Fatalf("LastActivityFromRoots failed: %v", err)
	}
	_ = act
}

func TestEnsureSessionDir_OpenFileError(t *testing.T) {
	tmpHome := t.TempDir()
	tmpData := t.TempDir()
	m := New(tmpHome, tmpData)

	sessID := "sess-open-err"
	logsDir := filepath.Join(tmpData, "brain", sessID, ".system_generated", "logs")
	_ = os.MkdirAll(logsDir, 0755)
	// Create transcript.jsonl as a directory so os.OpenFile fails
	_ = os.MkdirAll(filepath.Join(logsDir, "transcript.jsonl"), 0755)

	if _, err := m.EnsureSessionDir(sessID); err == nil {
		t.Error("expected error when transcript.jsonl is a directory")
	}
}

func TestAppendAmbientTurn_C2LogsDir(t *testing.T) {
	tmpData := t.TempDir()

	sessID := "sess-c2-test"
	// c2 is dir/sessionID/.system_generated/logs
	// If we configure root as tmpData (not tmpData/brain):
	mRoot := New("", "")
	mRoot.roots = []string{tmpData} // dir = tmpData, so c1 is tmpData/.system_generated/logs (missing), c2 is tmpData/sessID/.system_generated/logs (exists)
	c2LogsDir := filepath.Join(tmpData, sessID, ".system_generated", "logs")
	_ = os.MkdirAll(c2LogsDir, 0755)
	_ = os.WriteFile(filepath.Join(c2LogsDir, "transcript.jsonl"), []byte("{\"step_index\":1}\n"), 0644)

	err := mRoot.AppendAmbientTurn(sessID, "chat", "bob", "c2 message", time.Now())
	if err != nil {
		t.Fatalf("AppendAmbientTurn failed for c2 dir: %v", err)
	}
}

func TestCleanupEphemeralSession(t *testing.T) {
	// Empty convID is safe no-op
	CleanupEphemeralSession("")
	CleanupEphemeralSession("   ")

	// Path traversal protection
	CleanupEphemeralSession("../evil", t.TempDir())
	CleanupEphemeralSession("foo/bar", t.TempDir())

	// Cleans directory under search root
	tmpDir := t.TempDir()
	convID := "test-conv-12345"
	targetDir := filepath.Join(tmpDir, convID)
	if err := os.MkdirAll(targetDir, 0755); err != nil {
		t.Fatalf("failed to create target dir: %v", err)
	}

	CleanupEphemeralSession(convID, "", "   ", tmpDir)

	if _, err := os.Stat(targetDir); !os.IsNotExist(err) {
		t.Errorf("expected target dir to be removed, but it still exists")
	}
}

func TestDefaultMaxSessionTurnsConstant(t *testing.T) {
	if DefaultMaxSessionTurns != 10 {
		t.Errorf("expected DefaultMaxSessionTurns to be 10, got %d", DefaultMaxSessionTurns)
	}
}

func TestExtractFinalSubstantiveResponse_MultiTurnExcludesChatter(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)
	convID := "multi-turn-chatter-123"
	logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("Failed to create temp logs dir: %v", err)
	}

	transcript := `{"type":"USER_INPUT","source":"USER_EXPLICIT","content":"revert the orrery"}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"Reverting the orrery in PR #225.","tool_calls":[{"name":"schedule","args":{}}]}
{"type":"GENERIC","status":"DONE","content":"tool result 1"}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"Waiting for unit test suite completion on PR #225.","tool_calls":[{"name":"run_command","args":{}}]}
{"type":"GENERIC","status":"DONE","content":"tool result 2"}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"Waiting on Docker image builds for dashboard on PR #225.","tool_calls":[{"name":"schedule","args":{}}]}
{"type":"GENERIC","status":"DONE","content":"tool result 3"}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"Handled, boss—PR #225 is merged and synced. 🍵"}
`
	if err := os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(transcript), 0644); err != nil {
		t.Fatalf("Failed to write transcript: %v", err)
	}

	finalText, isSilent, err := mgr.ExtractFinalSubstantiveResponse(nil, convID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if isSilent {
		t.Errorf("Expected isSilent to be false, got true")
	}
	expected := "Handled, boss—PR #225 is merged and synced. 🍵"
	if finalText != expected {
		t.Errorf("Expected %q, got %q", expected, finalText)
	}
}

func TestExtractFinalSubstantiveResponse_WaitNoticesDelivered(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)
	convID := "wait-notice-delivered-123"
	logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("Failed to create temp logs dir: %v", err)
	}

	transcript := `{"type":"USER_INPUT","source":"USER_EXPLICIT","content":"run long task"}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"Running step 1...","tool_calls":[{"name":"run_command","args":{}}]}
{"type":"GENERIC","status":"DONE","content":"task scheduled"}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"wait for background task 123 to complete"}
`
	if err := os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(transcript), 0644); err != nil {
		t.Fatalf("Failed to write transcript: %v", err)
	}

	finalText, isSilent, err := mgr.ExtractFinalSubstantiveResponse(nil, convID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if isSilent {
		t.Errorf("Expected isSilent to be false, got true")
	}
	expected := "wait for background task 123 to complete"
	if finalText != expected {
		t.Errorf("Expected %q, got %q", expected, finalText)
	}
}

func TestExtractFinalSubstantiveResponse_EmptyTerminalResponse(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)
	convID := "empty-terminal-123"
	logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("Failed to create temp logs dir: %v", err)
	}

	transcript := `{"type":"USER_INPUT","source":"USER_EXPLICIT","content":"run command"}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"Starting command...","tool_calls":[{"name":"run_command","args":{}}]}
{"type":"GENERIC","status":"DONE","content":"success"}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"   "}
`
	if err := os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(transcript), 0644); err != nil {
		t.Fatalf("Failed to write transcript: %v", err)
	}

	finalText, isSilent, err := mgr.ExtractFinalSubstantiveResponse(nil, convID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if isSilent {
		t.Errorf("Expected isSilent to be false for empty terminal response, got true")
	}
	if finalText != "" {
		t.Errorf("Expected empty finalText, got %q", finalText)
	}
}

func TestExtractFinalSubstantiveResponse_TerminalToolStubDoesNotResurrectChatter(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)
	convID := "terminal-stub-123"
	logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("Failed to create temp logs dir: %v", err)
	}

	transcript := `{"type":"USER_INPUT","source":"USER_EXPLICIT","content":"deploy"}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"Deploying now...","tool_calls":[{"name":"run_command","args":{}}]}
{"type":"GENERIC","status":"DONE","content":"tool running"}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"","tool_calls":[{"name":"schedule","args":{}}]}
`
	if err := os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(transcript), 0644); err != nil {
		t.Fatalf("Failed to write transcript: %v", err)
	}

	finalText, isSilent, err := mgr.ExtractFinalSubstantiveResponse(nil, convID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if isSilent {
		t.Errorf("Expected isSilent to be false, got true")
	}
	if finalText != "" {
		t.Errorf("Expected empty finalText so caller falls back safely, got %q", finalText)
	}
}

func TestExtractFinalSubstantiveResponse_TrailingEmptyStep_FindsSubstantiveStep(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)
	convID := "trailing-empty-123"
	logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("Failed to create temp logs dir: %v", err)
	}

	transcript := `{"type":"USER_INPUT","source":"USER_EXPLICIT","content":"deploy"}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"Deploying now...","tool_calls":[{"name":"run_command","args":{}}]}
{"type":"GENERIC","status":"DONE","content":"tool running"}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"Deployment completed successfully!"}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"   "}
`
	if err := os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(transcript), 0644); err != nil {
		t.Fatalf("Failed to write transcript: %v", err)
	}

	finalText, isSilent, err := mgr.ExtractFinalSubstantiveResponse(nil, convID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if isSilent {
		t.Errorf("Expected isSilent to be false, got true")
	}
	if finalText != "Deployment completed successfully!" {
		t.Errorf("Expected 'Deployment completed successfully!', got %q", finalText)
	}
}


func TestExtractFinalSubstantiveResponse_AmbientInterleaving(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)
	convID := "ambient-interleave-123"
	logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("Failed to create temp logs dir: %v", err)
	}

	transcript := `{"type":"USER_INPUT","source":"USER_EXPLICIT","content":"check status"}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"Checking system...","tool_calls":[{"name":"run_command","args":{}}]}
{"type":"USER_INPUT","source":"AMBIENT","content":"[Chat #lounge] @ryan: hello world"}
{"type":"GENERIC","status":"DONE","content":"all green"}
{"type":"PLANNER_RESPONSE","status":"DONE","content":"All systems green and operational. ⚡"}
`
	if err := os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(transcript), 0644); err != nil {
		t.Fatalf("Failed to write transcript: %v", err)
	}

	finalText, isSilent, err := mgr.ExtractFinalSubstantiveResponse(nil, convID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if isSilent {
		t.Errorf("Expected isSilent to be false, got true")
	}
	expected := "All systems green and operational. ⚡"
	if finalText != expected {
		t.Errorf("Expected %q, got %q", expected, finalText)
	}
}

func TestExtractFinalSubstantiveResponse_SecurityAndErrors(t *testing.T) {
	mgr, _ := setupTestManager(t)

	// Path traversal protection
	text, silent, err := mgr.ExtractFinalSubstantiveResponse(nil, "../evil/path")
	if err != nil || text != "" || silent {
		t.Errorf("Expected empty result for traversal path, got (%q, %v, %v)", text, silent, err)
	}

	// Empty conversation ID
	text, silent, err = mgr.ExtractFinalSubstantiveResponse(nil, "   ")
	if err != nil || text != "" || silent {
		t.Errorf("Expected empty result for empty ID, got (%q, %v, %v)", text, silent, err)
	}

	// Nil manager
	var nilMgr *Manager
	text, silent, err = nilMgr.ExtractFinalSubstantiveResponse(nil, "some-id")
	if err != nil || text != "" || silent {
		t.Errorf("Expected empty result for nil manager, got (%q, %v, %v)", text, silent, err)
	}
}

func TestExtractFinalSubstantiveResponse_CoverageBoost(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)

	// 1. Cancelled context at start
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := mgr.ExtractFinalSubstantiveResponse(ctxCancel, "some-conv")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	// 2. Empty transcript file (size 0)
	convEmpty := "empty-conv-123"
	emptyLogs := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convEmpty, ".system_generated", "logs")
	_ = os.MkdirAll(emptyLogs, 0755)
	_ = os.WriteFile(filepath.Join(emptyLogs, "transcript.jsonl"), []byte(""), 0644)
	text, silent, err := mgr.ExtractFinalSubstantiveResponse(context.Background(), convEmpty)
	if err != nil || text != "" || silent {
		t.Errorf("expected empty result for empty file, got (%q, %v, %v)", text, silent, err)
	}

	// 3. Large transcript file (> 1MB) with offset > 0 and user input in tail chunk
	convLarge1 := "large-conv-tail"
	largeLogs1 := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convLarge1, ".system_generated", "logs")
	_ = os.MkdirAll(largeLogs1, 0755)

	padding := strings.Repeat(`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"padding"}
`, 15000) // ~1.2MB of padding
	tailWithInput := `{"step_index":20000,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"run command"}
{"step_index":20001,"source":"MODEL","type":"PLANNER_RESPONSE","content":"Done with large run!"}
`
	_ = os.WriteFile(filepath.Join(largeLogs1, "transcript.jsonl"), []byte(padding+tailWithInput), 0644)
	text, silent, err = mgr.ExtractFinalSubstantiveResponse(context.Background(), convLarge1)
	if err != nil || text != "Done with large run!" || silent {
		t.Errorf("expected 'Done with large run!', got (%q, %v, %v)", text, silent, err)
	}

	// 4. Large transcript file (> 1MB) where user input is at beginning (not in tail chunk, lastUserInputIdx == -1 with offset > 0)
	convLarge2 := "large-conv-head"
	largeLogs2 := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convLarge2, ".system_generated", "logs")
	_ = os.MkdirAll(largeLogs2, 0755)

	headWithInput := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"start long process"}
`
	tailOnlyPlanner := `{"step_index":20001,"source":"MODEL","type":"PLANNER_RESPONSE","content":"Final response from head input!"}
`
	_ = os.WriteFile(filepath.Join(largeLogs2, "transcript.jsonl"), []byte(headWithInput+padding+tailOnlyPlanner), 0644)
	text, silent, err = mgr.ExtractFinalSubstantiveResponse(context.Background(), convLarge2)
	if err != nil || text != "Final response from head input!" || silent {
		t.Errorf("expected 'Final response from head input!', got (%q, %v, %v)", text, silent, err)
	}

	// 5. Corrupted JSON and non-PLANNER_RESPONSE in tail steps
	convCorrupt := "corrupt-conv-steps"
	corruptLogs := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convCorrupt, ".system_generated", "logs")
	_ = os.MkdirAll(corruptLogs, 0755)

	corruptContent := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"Valid earlier response"}
{bad json line that fails unmarshal}
{"step_index":2,"source":"SYSTEM","type":"USER_INPUT","content":"system event"}
{"step_index":3,"source":"MODEL","type":"PLANNER_RESPONSE","content":"Recovered successfully"}
`
	_ = os.WriteFile(filepath.Join(corruptLogs, "transcript.jsonl"), []byte(corruptContent), 0644)
	text, silent, err = mgr.ExtractFinalSubstantiveResponse(context.Background(), convCorrupt)
	if err != nil || text != "Recovered successfully" || silent {
		t.Errorf("expected 'Recovered successfully', got (%q, %v, %v)", text, silent, err)
	}
}

func TestSession_RemediatedBranchesAndEdges(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)

	// 1. ExtractResponseAndError with empty content and ToolCalls
	convToolOnly := "conv-tool-only"
	logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convToolOnly, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("failed to create logs dir: %v", err)
	}
	toolOnlyContent := `{"step_index":0,"source":"MODEL","type":"PLANNER_RESPONSE","content":"","tool_calls":[{"name":"bash"}]}`
	if err := os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(toolOnlyContent), 0644); err != nil {
		t.Fatalf("failed to write transcript: %v", err)
	}
	resp, _ := mgr.ExtractResponseAndError(convToolOnly)
	if !strings.HasPrefix(resp, "[Tool Call Requested]:") {
		t.Errorf("expected '[Tool Call Requested]:', got %q", resp)
	}

	// 2. ExtractFinalSubstantiveResponse with loop-time context cancellation
	ctxCancel, cancel := context.WithCancel(context.Background())
	convCancel := "conv-cancel-loop"
	logsDirCancel := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convCancel, ".system_generated", "logs")
	_ = os.MkdirAll(logsDirCancel, 0755)
	_ = os.WriteFile(filepath.Join(logsDirCancel, "transcript_full.jsonl"), []byte(`{"step_index":0,"type":"USER_INPUT"}`), 0644)
	_ = os.WriteFile(filepath.Join(logsDirCancel, "transcript.jsonl"), []byte(`{"step_index":0,"type":"USER_INPUT"}`), 0644)
	cancel()
	_, _, err := mgr.ExtractFinalSubstantiveResponse(ctxCancel, convCancel)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}

	// 3. ExtractFinalSubstantiveResponse with non-PLANNER_RESPONSE and unmarshal error traversed in reverse
	convReverse := "conv-reverse-edge"
	logsDirReverse := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convReverse, ".system_generated", "logs")
	_ = os.MkdirAll(logsDirReverse, 0755)
	revContent := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"Valid response"}
{malformed json line}
{"step_index":2,"source":"SYSTEM","type":"SYSTEM_NOTIFICATION","content":"info"}
`
	_ = os.WriteFile(filepath.Join(logsDirReverse, "transcript.jsonl"), []byte(revContent), 0644)
	text, silent, err := mgr.ExtractFinalSubstantiveResponse(context.Background(), convReverse)
	if err != nil || text != "Valid response" || silent {
		t.Errorf("expected 'Valid response', got (%q, %v, %v)", text, silent, err)
	}

	// 4. HasSuccessfulToolCall with empty lines between steps
	convEmptyLine := "conv-empty-line"
	logsDirEmpty := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convEmptyLine, ".system_generated", "logs")
	_ = os.MkdirAll(logsDirEmpty, 0755)
	emptyLineContent := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}

{"step_index":1,"source":"MODEL","type":"RUN_COMMAND","status":"DONE"}
`
	_ = os.WriteFile(filepath.Join(logsDirEmpty, "transcript.jsonl"), []byte(emptyLineContent), 0644)
	hasTool := mgr.HasSuccessfulToolCall(convEmptyLine)
	if !hasTool {
		t.Errorf("expected HasSuccessfulToolCall to be true with empty line in transcript")
	}

	// 5. AppendAmbientTurn using candidate 2 directory path
	convCand2 := "conv-cand-2"
	cand2Logs := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convCand2, convCand2, ".system_generated", "logs")
	_ = os.MkdirAll(cand2Logs, 0755)
	_ = os.WriteFile(filepath.Join(cand2Logs, "transcript.jsonl"), []byte(`{"step_index":0,"source":"USER_INPUT","content":"hello"}`+"\n"), 0644)
	err = mgr.AppendAmbientTurn(convCand2, "general", "alice", "hello from cand2", time.Now())
	if err != nil {
		t.Errorf("expected nil error appending to cand2 logs, got %v", err)
	}

	// 6. Direct test of getLastStepIndex error path on a directory
	_, err = getLastStepIndex(tmpDir)
	if err == nil {
		t.Errorf("expected error from getLastStepIndex on directory, got nil")
	}
}

func TestSessionRotationConstants(t *testing.T) {
	if DefaultMaxSessionTurns != 10 {
		t.Errorf("expected DefaultMaxSessionTurns=10, got %d", DefaultMaxSessionTurns)
	}
	if DefaultMaxSessionSteps != 350 {
		t.Errorf("expected DefaultMaxSessionSteps=350, got %d", DefaultMaxSessionSteps)
	}
	if DefaultMaxTranscriptBytes != 1024*1024 {
		t.Errorf("expected DefaultMaxTranscriptBytes=1048576, got %d", DefaultMaxTranscriptBytes)
	}
}

func TestManager_GetTranscriptSize(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := New(tmpDir, tmpDir)

	// 1. Nil manager
	var nilMgr *Manager
	if sz := nilMgr.GetTranscriptSize("sess-1"); sz != 0 {
		t.Errorf("expected 0 from nil manager, got %d", sz)
	}

	// 2. Empty / whitespace session ID
	if sz := mgr.GetTranscriptSize(""); sz != 0 {
		t.Errorf("expected 0 for empty session ID, got %d", sz)
	}
	if sz := mgr.GetTranscriptSize("   "); sz != 0 {
		t.Errorf("expected 0 for whitespace session ID, got %d", sz)
	}

	// 3. Security: path traversal
	if sz := mgr.GetTranscriptSize("../../etc/passwd"); sz != 0 {
		t.Errorf("expected 0 for path traversal, got %d", sz)
	}
	if sz := mgr.GetTranscriptSize("a/b"); sz != 0 {
		t.Errorf("expected 0 for slash in session ID, got %d", sz)
	}

	// 4. Non-existent session
	if sz := mgr.GetTranscriptSize("non-existent-sess"); sz != 0 {
		t.Errorf("expected 0 for non-existent session, got %d", sz)
	}

	// 5. Valid session with transcript.jsonl
	sessID := "sess-size-test"
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessID, ".system_generated", "logs")
	if err := os.MkdirAll(sessDir, 0755); err != nil {
		t.Fatalf("failed to create logs dir: %v", err)
	}
	tPath := filepath.Join(sessDir, "transcript.jsonl")
	content := []byte(`{"step_index": 0, "type": "USER_INPUT"}` + "\n")
	if err := os.WriteFile(tPath, content, 0644); err != nil {
		t.Fatalf("failed to write transcript: %v", err)
	}

	expectedSize := int64(len(content))
	if sz := mgr.GetTranscriptSize(sessID); sz != expectedSize {
		t.Errorf("expected size %d, got %d", expectedSize, sz)
	}

	// 6. Dual-file: transcript_full.jsonl is larger
	fullPath := filepath.Join(sessDir, "transcript_full.jsonl")
	fullContent := append(content, []byte(`{"step_index": 1, "type": "PLANNER_RESPONSE", "extra": "large payload data here"}` + "\n")...)
	if err := os.WriteFile(fullPath, fullContent, 0644); err != nil {
		t.Fatalf("failed to write transcript_full: %v", err)
	}
	expectedFullSize := int64(len(fullContent))
	if sz := mgr.GetTranscriptSize(sessID); sz != expectedFullSize {
		t.Errorf("expected max size %d, got %d", expectedFullSize, sz)
	}
}

func TestManager_CountTranscriptSteps(t *testing.T) {
	tmpDir := t.TempDir()
	mgr := New(tmpDir, tmpDir)

	// 1. Nil manager
	var nilMgr *Manager
	if steps := nilMgr.CountTranscriptSteps("sess-1"); steps != 0 {
		t.Errorf("expected 0 steps from nil manager, got %d", steps)
	}

	// 2. Empty / whitespace session ID
	if steps := mgr.CountTranscriptSteps(""); steps != 0 {
		t.Errorf("expected 0 steps for empty session ID, got %d", steps)
	}
	if steps := mgr.CountTranscriptSteps("   "); steps != 0 {
		t.Errorf("expected 0 steps for whitespace session ID, got %d", steps)
	}

	// 3. Security: path traversal
	if steps := mgr.CountTranscriptSteps("../../../root"); steps != 0 {
		t.Errorf("expected 0 steps for path traversal, got %d", steps)
	}

	// 4. Non-existent session
	if steps := mgr.CountTranscriptSteps("sess-ghost"); steps != 0 {
		t.Errorf("expected 0 steps for ghost session, got %d", steps)
	}

	// 5. Valid session with empty file -> 0 steps
	sessID := "sess-step-test"
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessID, ".system_generated", "logs")
	if err := os.MkdirAll(sessDir, 0755); err != nil {
		t.Fatalf("failed to create logs dir: %v", err)
	}
	tPath := filepath.Join(sessDir, "transcript.jsonl")
	if err := os.WriteFile(tPath, []byte(""), 0644); err != nil {
		t.Fatalf("failed to create empty file: %v", err)
	}
	if steps := mgr.CountTranscriptSteps(sessID); steps != 0 {
		t.Errorf("expected 0 steps for empty file, got %d", steps)
	}

	// 6. Transcript with monotonic steps: last step_index is 41 -> 42 total steps
	lines := `{"step_index": 0, "type": "USER_INPUT"}
{"step_index": 1, "type": "PLANNER_RESPONSE"}
{"step_index": 41, "type": "PLANNER_RESPONSE"}
`
	if err := os.WriteFile(tPath, []byte(lines), 0644); err != nil {
		t.Fatalf("failed to write lines: %v", err)
	}
	if steps := mgr.CountTranscriptSteps(sessID); steps != 42 {
		t.Errorf("expected 42 steps, got %d", steps)
	}

	// 7. transcript_full.jsonl has higher step index 99 -> 100 total steps
	fullPath := filepath.Join(sessDir, "transcript_full.jsonl")
	fullLines := lines + `{"step_index": 99, "type": "PLANNER_RESPONSE"}` + "\n"
	if err := os.WriteFile(fullPath, []byte(fullLines), 0644); err != nil {
		t.Fatalf("failed to write full lines: %v", err)
	}
	if steps := mgr.CountTranscriptSteps(sessID); steps != 100 {
		t.Errorf("expected 100 steps from larger full transcript, got %d", steps)
	}
}

func TestHasUnfinishedBackgroundTask(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	mgr := New("", tmpDir)

	// 1. Nil manager or invalid ID
	var nilMgr *Manager
	if has, _, _ := nilMgr.HasUnfinishedBackgroundTask("test"); has {
		t.Error("expected false for nil manager")
	}
	if has, _, _ := mgr.HasUnfinishedBackgroundTask(""); has {
		t.Error("expected false for empty convID")
	}
	if has, _, _ := mgr.HasUnfinishedBackgroundTask("../bad"); has {
		t.Error("expected false for path traversal")
	}

	// 2. Non-existent session
	if has, _, _ := mgr.HasUnfinishedBackgroundTask("non-existent-sess"); has {
		t.Error("expected false for non-existent session")
	}

	// 3. Completed background task in latest turn
	sessCompleted := "sess-completed-task"
	logsDir1 := filepath.Join(tmpDir, "brain", sessCompleted, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir1, 0755); err != nil {
		t.Fatal(err)
	}
	linesCompleted := `{"step_index": 0, "source": "USER_EXPLICIT", "type": "USER_INPUT", "content": "run task"}
{"step_index": 1, "source": "MODEL", "type": "GENERIC", "status": "RUNNING", "content": "Tool is running as a background task with task id: sess-completed-task/task-1\nTask Description: test"}
{"step_index": 2, "source": "SYSTEM", "type": "SYSTEM_MESSAGE", "status": "DONE", "content": "[Message] sender=sess-completed-task/task-1 content=Task id \"sess-completed-task/task-1\" finished with result:\nSuccess"}
{"step_index": 3, "source": "MODEL", "type": "PLANNER_RESPONSE", "status": "DONE", "content": "Finished successfully."}
`
	if err := os.WriteFile(filepath.Join(logsDir1, "transcript.jsonl"), []byte(linesCompleted), 0644); err != nil {
		t.Fatal(err)
	}
	has, taskID, err := mgr.HasUnfinishedBackgroundTask(sessCompleted)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if has {
		t.Errorf("expected has=false for completed task, got true with taskID=%s", taskID)
	}

	// 3b. Completed background task via finished result text only (no sender)
	sessCompletedResultOnly := "sess-completed-result-only"
	logsDirResultOnly := filepath.Join(tmpDir, "brain", sessCompletedResultOnly, ".system_generated", "logs")
	if err := os.MkdirAll(logsDirResultOnly, 0755); err != nil {
		t.Fatal(err)
	}
	linesResultOnly := `{"step_index": 0, "source": "USER_EXPLICIT", "type": "USER_INPUT", "content": "run task"}
{"step_index": 1, "source": "MODEL", "type": "GENERIC", "status": "RUNNING", "content": "Tool is running as a background task with task id: sess-completed-result-only/task-2"}
{"step_index": 2, "source": "SYSTEM", "type": "SYSTEM_MESSAGE", "status": "DONE", "content": "Task id \"sess-completed-result-only/task-2\" finished with result:\nSuccess"}
{"step_index": 3, "source": "MODEL", "type": "PLANNER_RESPONSE", "status": "DONE", "content": "Finished successfully."}
`
	if err := os.WriteFile(filepath.Join(logsDirResultOnly, "transcript.jsonl"), []byte(linesResultOnly), 0644); err != nil {
		t.Fatal(err)
	}
	has, taskID, err = mgr.HasUnfinishedBackgroundTask(sessCompletedResultOnly)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if has {
		t.Errorf("expected has=false for task completed via result text, got true with taskID=%s", taskID)
	}

	// 4. Unfinished background task in latest turn
	sessUnfinished := "sess-unfinished-task"
	logsDir2 := filepath.Join(tmpDir, "brain", sessUnfinished, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir2, 0755); err != nil {
		t.Fatal(err)
	}
	linesUnfinished := `{"step_index": 0, "source": "USER_EXPLICIT", "type": "USER_INPUT", "content": "submit PR"}
{"step_index": 1, "source": "MODEL", "type": "GENERIC", "status": "RUNNING", "content": "Tool is running as a background task with task id: sess-unfinished-task/task-444\nTask Description: aerial-pr.sh submit"}
{"step_index": 2, "source": "MODEL", "type": "PLANNER_RESPONSE", "status": "DONE", "content": "Waiting on background task task-444 to complete."}
`
	if err := os.WriteFile(filepath.Join(logsDir2, "transcript.jsonl"), []byte(linesUnfinished), 0644); err != nil {
		t.Fatal(err)
	}
	has, taskID, err = mgr.HasUnfinishedBackgroundTask(sessUnfinished)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !has {
		t.Error("expected has=true for unfinished task, got false")
	}
	if taskID != "sess-unfinished-task/task-444" {
		t.Errorf("expected taskID=sess-unfinished-task/task-444, got %s", taskID)
	}

	// 5. Multi-turn: task completed in Turn 1, Turn 2 has a new unfinished task
	sessMultiTurn := "sess-multiturn-tasks"
	logsDir3 := filepath.Join(tmpDir, "brain", sessMultiTurn, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir3, 0755); err != nil {
		t.Fatal(err)
	}
	linesMultiTurn := `{"step_index": 0, "source": "USER_EXPLICIT", "type": "USER_INPUT", "content": "first turn"}
{"step_index": 1, "source": "MODEL", "type": "GENERIC", "status": "RUNNING", "content": "Tool is running as a background task with task id: sess-multiturn-tasks/task-1"}
{"step_index": 2, "source": "SYSTEM", "type": "SYSTEM_MESSAGE", "status": "DONE", "content": "Task id \"sess-multiturn-tasks/task-1\" finished with result:\nDone"}
{"step_index": 3, "source": "MODEL", "type": "PLANNER_RESPONSE", "status": "DONE", "content": "Turn 1 done"}
{"step_index": 4, "source": "USER_EXPLICIT", "type": "USER_INPUT", "content": "second turn"}
{"step_index": 5, "source": "MODEL", "type": "GENERIC", "status": "RUNNING", "content": "Tool is running as a background task with task id: sess-multiturn-tasks/task-99"}
{"step_index": 6, "source": "MODEL", "type": "PLANNER_RESPONSE", "status": "DONE", "content": "Waiting on background task task-99"}
`
	if err := os.WriteFile(filepath.Join(logsDir3, "transcript.jsonl"), []byte(linesMultiTurn), 0644); err != nil {
		t.Fatal(err)
	}
	has, taskID, err = mgr.HasUnfinishedBackgroundTask(sessMultiTurn)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !has || taskID != "sess-multiturn-tasks/task-99" {
		t.Errorf("expected has=true with taskID=sess-multiturn-tasks/task-99, got (%v, %s)", has, taskID)
	}
}

func TestSession_ActiveTaskPersistence(t *testing.T) {
	tempHome := t.TempDir()
	tempData := t.TempDir()
	mgr := New(tempHome, tempData)

	sessDir := filepath.Join(tempData, "brain", "sess-active")
	if err := os.MkdirAll(sessDir, 0755); err != nil {
		t.Fatalf("failed to create session dir: %v", err)
	}

	tasks := []TaskMetadata{
		{TaskID: "task-persist-1", ToolName: "run_command", CommandLine: "sleep 30", StartedAt: time.Now()},
	}

	if err := mgr.SaveActiveTasks("sess-active", tasks); err != nil {
		t.Fatalf("SaveActiveTasks failed: %v", err)
	}

	loaded, err := mgr.GetActiveTasks("sess-active")
	if err != nil {
		t.Fatalf("GetActiveTasks failed: %v", err)
	}
	if len(loaded) != 1 || loaded[0].TaskID != "task-persist-1" {
		t.Errorf("expected loaded task-persist-1, got %v", loaded)
	}

	// Non-existent session
	loadedNone, err := mgr.GetActiveTasks("non-existent-sess")
	if err != nil || loadedNone != nil {
		t.Errorf("expected nil, nil for non-existent session, got %v, %v", loadedNone, err)
	}

	// Corrupted active_tasks.json
	corruptFile := filepath.Join(sessDir, ".system_generated", "active_tasks.json")
	if err := os.WriteFile(corruptFile, []byte("{invalid json"), 0644); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}
	if _, err := mgr.GetActiveTasks("sess-active"); err == nil {
		t.Errorf("expected unmarshal error on corrupt active_tasks.json")
	}

	// Invalid session IDs
	if _, err := mgr.GetSessionDir(""); err == nil {
		t.Errorf("expected error on empty session ID")
	}
	if _, err := mgr.GetSessionDir("../escape"); err == nil {
		t.Errorf("expected error on path traversal session ID")
	}
	var nilMgr *Manager
	if _, err := nilMgr.GetSessionDir("sess-1"); err == nil {
		t.Errorf("expected error on nil manager")
	}
	mgrNoRoots := New("", "")
	if _, err := mgrNoRoots.GetSessionDir("sess-1"); err == nil {
		t.Errorf("expected error on manager with no roots")
	}
	if err := mgr.SaveActiveTasks("", tasks); err == nil {
		t.Errorf("expected error on SaveActiveTasks with empty sessionID")
	}
	if _, err := mgr.GetActiveTasks(""); err == nil {
		t.Errorf("expected error on GetActiveTasks with empty sessionID")
	}
}

func TestFormatStepError_Coverage(t *testing.T) {
	if s := formatStepError(nil); s != "" {
		t.Errorf("expected empty string, got %q", s)
	}
	if s := formatStepError("error string"); s != "error string" {
		t.Errorf("expected 'error string', got %q", s)
	}
	if s := formatStepError(map[string]any{"code": 500, "msg": "server error"}); !strings.Contains(s, "server error") {
		t.Errorf("expected json containing server error, got %q", s)
	}
	if s := formatStepError(12345); s != "12345" {
		t.Errorf("expected '12345', got %q", s)
	}
}

func TestExtractLastTurnError(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)

	convID := "test-turn-error-123"
	logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("Failed to create temp logs dir: %v", err)
	}

	t0 := time.Now().Add(-10 * time.Minute)
	t1 := time.Now().Add(-5 * time.Minute)
	t2 := time.Now().Add(-1 * time.Minute)

	// Turn 1 had an old error. Turn 2 had a quota exhaustion error.
	transcript := fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"Turn 1 request","created_at":%q}
{"step_index":1,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"Old error from turn 1","created_at":%q}
{"step_index":2,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"Turn 2 request","created_at":%q}
{"step_index":3,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"RESOURCE_EXHAUSTED: Google Gemini API quota reached. resets in 4h51m59s.","created_at":%q}
`, t0.Format(time.RFC3339), t0.Add(time.Second).Format(time.RFC3339), t1.Format(time.RFC3339), t2.Format(time.RFC3339))

	tPath := filepath.Join(logsDir, "transcript.jsonl")
	if err := os.WriteFile(tPath, []byte(transcript), 0644); err != nil {
		t.Fatalf("Failed to write transcript.jsonl: %v", err)
	}

	// 1. Inspecting with since = t1 (turn 2 start) should find the quota error
	errStr, err := mgr.ExtractLastTurnError(context.Background(), convID, t1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(errStr, "RESOURCE_EXHAUSTED") {
		t.Errorf("expected quota error, got %q", errStr)
	}

	// 2. Inspecting with since = now (after turn 2) should NOT find the error
	errAfter, err := mgr.ExtractLastTurnError(context.Background(), convID, time.Now())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if errAfter != "" {
		t.Errorf("expected no error after current turn, got %q", errAfter)
	}

	// 3. Inspecting with unquoted error field
	convID2 := "test-turn-error-unquoted"
	logsDir2 := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convID2, ".system_generated", "logs")
	_ = os.MkdirAll(logsDir2, 0755)
	transcript2 := fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"Req","created_at":%q}
{"step_index":1,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","error":"quoted JSON error message","created_at":%q}
`, t1.Format(time.RFC3339), t2.Format(time.RFC3339))
	_ = os.WriteFile(filepath.Join(logsDir2, "transcript.jsonl"), []byte(transcript2), 0644)

	errStr2, err := mgr.ExtractLastTurnError(context.Background(), convID2, t1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if errStr2 != "quoted JSON error message" {
		t.Errorf("expected unquoted error message, got %q", errStr2)
	}

	// 4. Edge cases: nil manager, empty ID, path traversal, cancelled context
	var nilMgr *Manager
	if e, _ := nilMgr.ExtractLastTurnError(context.Background(), "any", time.Time{}); e != "" {
		t.Errorf("expected empty from nil manager, got %q", e)
	}
	if e, _ := mgr.ExtractLastTurnError(context.Background(), "", time.Time{}); e != "" {
		t.Errorf("expected empty from empty convID, got %q", e)
	}
	if e, _ := mgr.ExtractLastTurnError(context.Background(), "../escape", time.Time{}); e != "" {
		t.Errorf("expected empty from traversal, got %q", e)
	}
	ctxCancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := mgr.ExtractLastTurnError(ctxCancelled, convID, time.Time{}); err == nil {
		t.Errorf("expected context cancellation error")
	}
}

func TestExtractLastTurnError_CoverageBoost(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)

	// 1. Empty transcript file (size 0)
	convEmpty := "empty-err-conv"
	emptyLogs := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convEmpty, ".system_generated", "logs")
	_ = os.MkdirAll(emptyLogs, 0755)
	_ = os.WriteFile(filepath.Join(emptyLogs, "transcript.jsonl"), []byte(""), 0644)
	errStr, err := mgr.ExtractLastTurnError(context.Background(), convEmpty, time.Time{})
	if err != nil || errStr != "" {
		t.Errorf("expected empty error for empty file, got (%q, %v)", errStr, err)
	}

	// 2. Large transcript file (> 1MB) with user input in tail chunk and error at end
	convLarge1 := "large-err-tail"
	largeLogs1 := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convLarge1, ".system_generated", "logs")
	_ = os.MkdirAll(largeLogs1, 0755)
	padding := strings.Repeat(`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"padding"}
`, 15000)
	tailWithError := `{"step_index":20000,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"run command"}
{"step_index":20001,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"fatal: tail error detected"}
`
	_ = os.WriteFile(filepath.Join(largeLogs1, "transcript.jsonl"), []byte(padding+tailWithError), 0644)
	errStr, err = mgr.ExtractLastTurnError(context.Background(), convLarge1, time.Time{})
	if err != nil || errStr != "fatal: tail error detected" {
		t.Errorf("expected 'fatal: tail error detected', got (%q, %v)", errStr, err)
	}

	// 3. Large transcript file (> 1MB) where user input is at beginning (head)
	convLarge2 := "large-err-head"
	largeLogs2 := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convLarge2, ".system_generated", "logs")
	_ = os.MkdirAll(largeLogs2, 0755)
	headWithInput := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"start long process"}
`
	tailOnlyError := `{"step_index":20001,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"head error recovered"}
`
	_ = os.WriteFile(filepath.Join(largeLogs2, "transcript.jsonl"), []byte(headWithInput+padding+tailOnlyError), 0644)
	errStr, err = mgr.ExtractLastTurnError(context.Background(), convLarge2, time.Time{})
	if err != nil || errStr != "head error recovered" {
		t.Errorf("expected 'head error recovered', got (%q, %v)", errStr, err)
	}

	// 4. Corrupted JSON and fallback to content when step.Error is null
	convCorrupt := "corrupt-err-conv"
	corruptLogs := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convCorrupt, ".system_generated", "logs")
	_ = os.MkdirAll(corruptLogs, 0755)
	corruptContent := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"test"}
{bad json line}
{"step_index":1,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","error":null,"content":"fallback content error"}
`
	_ = os.WriteFile(filepath.Join(corruptLogs, "transcript.jsonl"), []byte(corruptContent), 0644)
	errStr, err = mgr.ExtractLastTurnError(context.Background(), convCorrupt, time.Time{})
	if err != nil || errStr != "fallback content error" {
		t.Errorf("expected 'fallback content error', got (%q, %v)", errStr, err)
	}

	// 5. Context cancellation in middle of reverse scan loop
	ctxCancel, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = mgr.ExtractLastTurnError(ctxCancel, convLarge1, time.Time{})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestFormatStepError_DetailedCoverage(t *testing.T) {
	var p *int = nil
	if s := formatStepError(p); s != "" {
		t.Errorf("expected empty string for typed nil, got %q", s)
	}
	unmarshalable := map[string]any{"bad": func() {}}
	if s := formatStepError(unmarshalable); s == "" {
		t.Errorf("expected string representation for unmarshalable map, got empty string")
	}
}

func TestManager_TasksCoverage(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)
	sessID := "task-cov-sess"
	sessDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", sessID)
	_ = os.MkdirAll(sessDir, 0755)

	tasks := []TaskMetadata{
		{TaskID: "task-1", ToolName: "run_command", CommandLine: "echo test", StartedAt: time.Now()},
	}
	if err := mgr.SaveActiveTasks(sessID, tasks); err != nil {
		t.Fatalf("SaveActiveTasks failed: %v", err)
	}

	loaded, err := mgr.GetActiveTasks(sessID)
	if err != nil {
		t.Fatalf("GetActiveTasks failed: %v", err)
	}
	if len(loaded) != 1 || loaded[0].TaskID != "task-1" {
		t.Errorf("unexpected loaded tasks: %+v", loaded)
	}

	// Corrupt active_tasks.json
	taskPath := filepath.Join(sessDir, ".system_generated", "active_tasks.json")
	_ = os.WriteFile(taskPath, []byte("invalid-json"), 0644)
	if _, err := mgr.GetActiveTasks(sessID); err == nil {
		t.Errorf("expected error reading corrupted active_tasks.json")
	}
}

func TestExtractLastTurnError_MoreEdgeCases(t *testing.T) {
	mgr, tmpDir := setupTestManager(t)
	convID := "edge-turn-error-conv"
	logsDir := filepath.Join(tmpDir, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	_ = os.MkdirAll(logsDir, 0755)

	// Step with quoted error string in json.RawMessage
	stepQuoted := `{"step_index":0,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","error":"quoted error string"}
`
	_ = os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(stepQuoted), 0644)
	errStr, err := mgr.ExtractLastTurnError(context.Background(), convID, time.Time{})
	if err != nil || errStr != "quoted error string" {
		t.Errorf("expected 'quoted error string', got (%q, %v)", errStr, err)
	}

	// Step with unquoted/raw numeric error
	stepNumeric := `{"step_index":0,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","error":502}
`
	_ = os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(stepNumeric), 0644)
	errStr, err = mgr.ExtractLastTurnError(context.Background(), convID, time.Time{})
	if err != nil || errStr != "502" {
		t.Errorf("expected '502', got (%q, %v)", errStr, err)
	}

	// Step before 'since' with RFC3339 timestamp
	oldTime := time.Now().Add(-2 * time.Hour)
	stepOld := fmt.Sprintf(`{"step_index":0,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"ancient error","created_at":%q}
`, oldTime.Format(time.RFC3339))
	_ = os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(stepOld), 0644)
	errStr, err = mgr.ExtractLastTurnError(context.Background(), convID, time.Now())
	if err != nil || errStr != "" {
		t.Errorf("expected empty string for ancient error, got (%q, %v)", errStr, err)
	}
}

func TestGetLastStepIndex_Coverage(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Non-existent file
	idx, err := getLastStepIndex(filepath.Join(tempDir, "nonexistent.jsonl"))
	if err != nil || idx != -1 {
		t.Errorf("expected (-1, nil) for nonexistent file, got (%d, %v)", idx, err)
	}

	// 2. Directory instead of file
	dirPath := filepath.Join(tempDir, "subfolder")
	_ = os.MkdirAll(dirPath, 0755)
	if _, err := getLastStepIndex(dirPath); err == nil {
		t.Errorf("expected error when checking directory, got nil")
	}

	// 3. Empty file
	emptyFile := filepath.Join(tempDir, "empty.jsonl")
	_ = os.WriteFile(emptyFile, []byte(""), 0644)
	idx, err = getLastStepIndex(emptyFile)
	if err != nil || idx != -1 {
		t.Errorf("expected (-1, nil) for empty file, got (%d, %v)", idx, err)
	}

	// 4. File with lines missing step_index or negative
	badSteps := filepath.Join(tempDir, "badsteps.jsonl")
	_ = os.WriteFile(badSteps, []byte("{\"other\":\"field\"}\n{\"step_index\":-1}\n"), 0644)
	idx, err = getLastStepIndex(badSteps)
	if err != nil || idx != -1 {
		t.Errorf("expected (-1, nil) for file without valid step_index, got (%d, %v)", idx, err)
	}
}

func TestAppendTranscriptStep_NoTrailingNewline(t *testing.T) {
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "no_newline.jsonl")
	// Write file without trailing newline
	_ = os.WriteFile(filePath, []byte("{\"step_index\":0}"), 0644)

	err := appendTranscriptStep(filePath, []byte("{\"step_index\":1}"))
	if err != nil {
		t.Fatalf("appendTranscriptStep failed: %v", err)
	}

	data, _ := os.ReadFile(filePath)
	expected := "{\"step_index\":0}\n{\"step_index\":1}\n"
	if string(data) != expected {
		t.Errorf("expected %q, got %q", expected, string(data))
	}
}

func TestDumpSessionDiagnosticLogs_Coverage(t *testing.T) {
	tempHome := t.TempDir()
	tempData := t.TempDir()
	mgr := New(tempHome, tempData)

	convID := "diag-conv-1"
	logsDir := filepath.Join(tempHome, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	_ = os.MkdirAll(logsDir, 0755)

	// Write log file with > 50 lines
	var lines []string
	for i := 0; i < 60; i++ {
		lines = append(lines, fmt.Sprintf("Log line %d in diagnosis", i))
	}
	_ = os.WriteFile(filepath.Join(logsDir, "test.log"), []byte(strings.Join(lines, "\n")), 0644)

	diagOutput := mgr.DumpSessionDiagnosticLogs(convID)
	if !strings.Contains(diagOutput, "[...showing last 50 lines...]") {
		t.Errorf("expected diagnostic output to include truncated marker, got %s", diagOutput)
	}

	var nilMgr *Manager
	if s := nilMgr.DumpSessionDiagnosticLogs(convID); s != "" {
		t.Errorf("expected empty string for nil manager, got %q", s)
	}
}

func TestManager_GetTargetDirs_Empty(t *testing.T) {
	tempHome := t.TempDir()
	mgr := New(tempHome, "")

	// Create subfolder in root
	sub := filepath.Join(tempHome, ".gemini", "antigravity-cli", "brain", "sub-1")
	_ = os.MkdirAll(sub, 0755)

	dirs := mgr.getTargetDirs("")
	if len(dirs) == 0 {
		t.Errorf("expected target dirs when convID is empty")
	}

	var nilMgr *Manager
	if d := nilMgr.getTargetDirs(""); d != nil {
		t.Errorf("expected nil for nil manager")
	}
}

func TestExtractLastTurnError_LargeFileAndOffset(t *testing.T) {
	tempHome := t.TempDir()
	tempData := t.TempDir()
	mgr := New(tempHome, tempData)

	convID := "large-conv-1"
	logsDir := filepath.Join(tempHome, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("failed to create logsDir: %v", err)
	}

	// 1. Write an empty transcript file (fi.Size() == 0 branch)
	emptyPath := filepath.Join(logsDir, "transcript_full.jsonl")
	if err := os.WriteFile(emptyPath, []byte(""), 0644); err != nil {
		t.Fatalf("failed to write empty transcript: %v", err)
	}

	// 2. Write a large transcript.jsonl (> 1MB) where user input is at the beginning
	// and last 1MB chunk has no user input, forcing fallback to os.ReadFile(tPath).
	tPath := filepath.Join(logsDir, "transcript.jsonl")
	f, err := os.Create(tPath)
	if err != nil {
		t.Fatalf("failed to create large transcript: %v", err)
	}

	// First line: USER_INPUT
	userStep := `{"source":"USER_EXPLICIT","type":"USER_INPUT","content":"Run long process"}` + "\n"
	if _, err := f.WriteString(userStep); err != nil {
		_ = f.Close()
		t.Fatalf("write failed: %v", err)
	}

	// Middle lines: fill > 1.1 MB with MODEL steps
	filler := `{"source":"MODEL","type":"PLANNER_RESPONSE","content":"working..."}` + "\n"
	targetBytes := 1100000
	written := len(userStep)
	for written < targetBytes {
		n, err := f.WriteString(filler)
		if err != nil {
			_ = f.Close()
			t.Fatalf("write filler failed: %v", err)
		}
		written += n
	}

	// Last line: ERROR_MESSAGE
	errStep := `{"source":"SYSTEM","type":"ERROR_MESSAGE","content":"Resource exhausted: quota limit reached","created_at":"2026-09-22T20:00:00Z"}` + "\n"
	if _, err := f.WriteString(errStep); err != nil {
		_ = f.Close()
		t.Fatalf("write err step failed: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close failed: %v", err)
	}

	// Test ExtractLastTurnError on large file
	extracted, err := mgr.ExtractLastTurnError(context.Background(), convID, time.Time{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.Contains(extracted, "Resource exhausted") {
		t.Errorf("expected Resource exhausted in extracted error, got %q", extracted)
	}

	// Test ExtractLastTurnError with canceled context
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := mgr.ExtractLastTurnError(canceledCtx, convID, time.Now()); err == nil {
		t.Errorf("expected context canceled error")
	}
}

func TestSaveAndGetActiveTasks_EdgeCases(t *testing.T) {
	tempHome := t.TempDir()
	mgr := New(tempHome, "")

	convID := "tasks-conv-1"

	// 1. GetActiveTasks on non-existent session returns nil, nil
	tasks, err := mgr.GetActiveTasks("non-existent-conv")
	if err != nil || tasks != nil {
		t.Errorf("expected nil, nil for non-existent session, got tasks=%v, err=%v", tasks, err)
	}

	// 2. SaveActiveTasks with invalid convID returns error
	if err := mgr.SaveActiveTasks("../escape", nil); err == nil {
		t.Errorf("expected error saving tasks to invalid session ID")
	}

	// 3. SaveActiveTasks successfully
	sampleTasks := []TaskMetadata{
		{TaskID: "task-101", ToolName: "bash", CommandLine: "ls -la"},
	}
	if err := mgr.SaveActiveTasks(convID, sampleTasks); err != nil {
		t.Fatalf("SaveActiveTasks failed: %v", err)
	}

	// 4. GetActiveTasks successfully retrieves saved tasks
	gotTasks, err := mgr.GetActiveTasks(convID)
	if err != nil || len(gotTasks) != 1 || gotTasks[0].TaskID != "task-101" {
		t.Fatalf("GetActiveTasks mismatch: got %v, err %v", gotTasks, err)
	}

	// 5. GetActiveTasks with corrupted JSON file
	sessDir, err := mgr.GetSessionDir(convID)
	if err != nil {
		t.Fatalf("GetSessionDir failed: %v", err)
	}
	tasksFile := filepath.Join(sessDir, ".system_generated", "active_tasks.json")
	if err := os.WriteFile(tasksFile, []byte("{invalid-json"), 0644); err != nil {
		t.Fatalf("failed to write corrupted json: %v", err)
	}
	if _, err := mgr.GetActiveTasks(convID); err == nil {
		t.Errorf("expected unmarshal error on corrupt active_tasks.json")
	}

	// 6. GetActiveTasks when path is a directory (os.ReadFile returns non-NotExist error)
	_ = os.Remove(tasksFile)
	if err := os.MkdirAll(tasksFile, 0755); err != nil {
		t.Fatalf("failed to create directory in place of file: %v", err)
	}
	if _, err := mgr.GetActiveTasks(convID); err == nil {
		t.Errorf("expected error reading directory as file in GetActiveTasks")
	}

	// 7. GetActiveTasks with invalid session ID returns error
	if _, err := mgr.GetActiveTasks("../../bad-id"); err == nil {
		t.Errorf("expected error from GetActiveTasks with invalid session ID")
	}
}

func TestGetLastStepIndex_DirectoryAndEmpty(t *testing.T) {
	tempDir := t.TempDir()

	// 1. Directory path returns error
	if idx, err := getLastStepIndex(tempDir); err == nil || idx != -1 {
		t.Errorf("expected error for directory path in getLastStepIndex, got idx=%d, err=%v", idx, err)
	}

	// 2. Empty file returns -1, nil
	emptyFile := filepath.Join(tempDir, "empty.jsonl")
	if err := os.WriteFile(emptyFile, []byte{}, 0644); err != nil {
		t.Fatalf("failed to create empty file: %v", err)
	}
	if idx, err := getLastStepIndex(emptyFile); err != nil || idx != -1 {
		t.Errorf("expected -1, nil for empty file, got idx=%d, err=%v", idx, err)
	}
}

func TestExtractResponseAndError_ErrorMessageOnly(t *testing.T) {
	tempHome := t.TempDir()
	mgr := New(tempHome, "")

	convID := "err-msg-conv"
	logsDir := filepath.Join(tempHome, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	// Write ERROR_MESSAGE turn without step.Error field, but with step.Content
	line := `{"source":"SYSTEM","type":"ERROR_MESSAGE","content":"Fatal quota exhausted message"}` + "\n"
	if err := os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte(line), 0644); err != nil {
		t.Fatalf("write failed: %v", err)
	}

	_, lastErr := mgr.ExtractResponseAndError(convID)
	if lastErr != "Fatal quota exhausted message" {
		t.Errorf("expected 'Fatal quota exhausted message', got %q", lastErr)
	}
}

func TestExtractFinalSubstantiveResponse_LargeFileAndEmpty(t *testing.T) {
	tempHome := t.TempDir()
	mgr := New(tempHome, "")

	convID := "final-subst-large"
	logsDir := filepath.Join(tempHome, ".gemini", "antigravity-cli", "brain", convID, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0755); err != nil {
		t.Fatalf("mkdir failed: %v", err)
	}

	// 1. Write empty transcript_full.jsonl
	if err := os.WriteFile(filepath.Join(logsDir, "transcript_full.jsonl"), []byte(""), 0644); err != nil {
		t.Fatalf("write empty failed: %v", err)
	}

	// 2. Write large transcript.jsonl (> 1MB)
	tPath := filepath.Join(logsDir, "transcript.jsonl")
	f, err := os.Create(tPath)
	if err != nil {
		t.Fatalf("create failed: %v", err)
	}

	// First line: user input
	userStep := `{"source":"USER_EXPLICIT","type":"USER_INPUT","content":"Start"}` + "\n"
	_, _ = f.WriteString(userStep)

	filler := `{"source":"MODEL","type":"PLANNER_RESPONSE","content":"working..."}` + "\n"
	for i := 0; i < 18000; i++ {
		_, _ = f.WriteString(filler)
	}

	finalStep := `{"source":"MODEL","type":"PLANNER_RESPONSE","content":"Final substantive message"}` + "\n"
	_, _ = f.WriteString(finalStep)
	_ = f.Close()

	resp, isSilent, err := mgr.ExtractFinalSubstantiveResponse(context.Background(), convID)
	if err != nil || isSilent || !strings.Contains(resp, "Final substantive message") {
		t.Errorf("unexpected: resp=%q, isSilent=%v, err=%v", resp, isSilent, err)
	}

	// Canceled context inside loop
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := mgr.ExtractFinalSubstantiveResponse(canceledCtx, convID); err == nil {
		t.Errorf("expected context canceled error")
	}
}







package session

import (
	"bytes"
	"encoding/json"
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
	if errStr != `"something went wrong"` {
		t.Errorf("Expected error '\"something went wrong\"', got: '%s'", errStr)
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


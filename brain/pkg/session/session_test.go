package session

import (
	"os"
	"path/filepath"
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

package runner

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseAgyOutput(t *testing.T) {
	tests := []struct {
		name        string
		stdout      string
		wantErr     bool
		wantConvID  string
		wantStatus  string
		wantResp    string
		wantTokens  int
	}{
		{
			name:        "Valid Success Response",
			stdout:      `{"conversation_id":"11111111-2222-3333-4444-555555555555","status":"SUCCESS","response":"Hello world!","duration_seconds":1.25,"num_turns":1,"usage":{"total_tokens":42}}`,
			wantErr:     false,
			wantConvID:  "11111111-2222-3333-4444-555555555555",
			wantStatus:  "SUCCESS",
			wantResp:    "Hello world!",
			wantTokens:  42,
		},
		{
			name:        "Valid Error Response",
			stdout:      `{"conversation_id":"11111111-2222-3333-4444-555555555555","status":"ERROR","error":"context window exceeded","duration_seconds":0.5}`,
			wantErr:     false,
			wantConvID:  "11111111-2222-3333-4444-555555555555",
			wantStatus:  "ERROR",
			wantResp:    "",
		},
		{
			name:        "Empty Stdout",
			stdout:      "",
			wantErr:     true,
		},
		{
			name:        "Invalid JSON",
			stdout:      "plain text without json formatting",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resp, err := ParseAgyOutput(tt.stdout)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ParseAgyOutput() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				if resp.ConversationID != tt.wantConvID {
					t.Errorf("ConversationID = %q, want %q", resp.ConversationID, tt.wantConvID)
				}
				if resp.Status != tt.wantStatus {
					t.Errorf("Status = %q, want %q", resp.Status, tt.wantStatus)
				}
				if resp.Response != tt.wantResp {
					t.Errorf("Response = %q, want %q", resp.Response, tt.wantResp)
				}
				if tt.wantTokens > 0 && resp.Usage.TotalTokens != tt.wantTokens {
					t.Errorf("TotalTokens = %d, want %d", resp.Usage.TotalTokens, tt.wantTokens)
				}
			}
		})
	}
}

func TestIsSilentSentinel(t *testing.T) {
	tests := []struct {
		name     string
		stdout   string
		expected bool
	}{
		{name: "Empty string", stdout: "", expected: true},
		{name: "Whitespace only", stdout: "   \n\t\r  ", expected: true},
		{name: "Visible conversational text", stdout: "Hello world!", expected: false},
		{name: "Visible conversational text", stdout: "Here is your requested answer.", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsSilentSentinel(tt.stdout)
			if got != tt.expected {
				t.Errorf("IsSilentSentinel(%q) = %v, want %v", tt.stdout, got, tt.expected)
			}
		})
	}
}

func TestClassifyError(t *testing.T) {
	tests := []struct {
		name                 string
		exitCode             int
		stdout               string
		stderr               string
		wantFailure          bool
		wantTransient        bool
		wantCorrupt          bool
		errDetailMustContain string
	}{
		{
			name:          "Clean Success With JSON Envelope",
			exitCode:      0,
			stdout:        `{"conversation_id":"abc","status":"SUCCESS","response":"Hello there!"}`,
			stderr:        "",
			wantFailure:   false,
			wantTransient: false,
			wantCorrupt:   false,
		},
		{
			name:          "Clean Success With Empty Response In JSON Envelope",
			exitCode:      0,
			stdout:        `{"conversation_id":"abc","status":"SUCCESS","response":""}`,
			stderr:        "",
			wantFailure:   false,
			wantTransient: false,
			wantCorrupt:   false,
		},
		{
			name:          "Clean Success: Conversational response discussing maximum context length and 503 errors",
			exitCode:      0,
			stdout:        `{"conversation_id":"abc","status":"SUCCESS","response":"The model's maximum context length is 1M tokens. When context window exceeded occurs, handle error 503."}`,
			stderr:        "",
			wantFailure:   false,
			wantTransient: false,
			wantCorrupt:   false,
		},
		{
			name:          "Clean Success: Benign debug stderr with timeout and rate limit logs",
			exitCode:      0,
			stdout:        `{"conversation_id":"abc","status":"SUCCESS","response":"Processed your request cleanly."}`,
			stderr:        "[DEBUG] rate limit check passed, timeout set to 30s",
			wantFailure:   false,
			wantTransient: false,
			wantCorrupt:   false,
		},
		{
			name:          "Self-Healing: Exit 0 with conversation not found warning on stderr",
			exitCode:      0,
			stdout:        `{"conversation_id":"new-uuid","status":"SUCCESS","response":"Hello fresh session!"}`,
			stderr:        `warning: conversation "stale-uuid" not found`,
			wantFailure:   false,
			wantTransient: false,
			wantCorrupt:   false,
		},
		{
			name:                 "Exit Code 0 With Empty Stdout Is Flagged As Failure",
			exitCode:             0,
			stdout:               "",
			stderr:               "",
			wantFailure:          true,
			wantTransient:        false,
			wantCorrupt:          false,
			errDetailMustContain: "process produced empty stdout",
		},
		{
			name:                 "Exit Code 0 With Non-JSON Stdout",
			exitCode:             0,
			stdout:               "not a valid json output",
			stderr:               "",
			wantFailure:          true,
			wantTransient:        false,
			wantCorrupt:          false,
			errDetailMustContain: "invalid json response",
		},
		{
			name:                 "Exit Code 0 With Context Window Exceeded In JSON",
			exitCode:             0,
			stdout:               `{"conversation_id":"abc","status":"ERROR","error":"context length exceeded: max context length is 1000000"}`,
			stderr:               "",
			wantFailure:          true,
			wantTransient:        false,
			wantCorrupt:          true,
			errDetailMustContain: "context length exceeded",
		},
		{
			name:                 "Transient 503 Unavailable",
			exitCode:             1,
			stdout:               "",
			stderr:               "Error: status: UNAVAILABLE: Model is overloaded (Error 503)",
			wantFailure:          true,
			wantTransient:        true,
			wantCorrupt:          false,
			errDetailMustContain: "503",
		},
		{
			name:                 "Transient 429 Rate Limit",
			exitCode:             1,
			stdout:               "",
			stderr:               "RESOURCE_EXHAUSTED: Rate limit exceeded (429)",
			wantFailure:          true,
			wantTransient:        true,
			wantCorrupt:          false,
			errDetailMustContain: "Rate limit",
		},
		{
			name:                 "Transient Context Deadline Exceeded",
			exitCode:             -1,
			stdout:               "",
			stderr:               "context deadline exceeded",
			wantFailure:          true,
			wantTransient:        true,
			wantCorrupt:          false,
			errDetailMustContain: "context deadline exceeded",
		},
		{
			name:                 "Session Corruption - Corrupted Session",
			exitCode:             1,
			stdout:               "",
			stderr:               "Error: failed to load conversation: session corrupted",
			wantFailure:          true,
			wantTransient:        false,
			wantCorrupt:          true,
			errDetailMustContain: "failed to load",
		},
		{
			name:                 "Database Locked - Not Session Corruption",
			exitCode:             1,
			stdout:               "",
			stderr:               "sqlite error: database is locked",
			wantFailure:          true,
			wantTransient:        false,
			wantCorrupt:          false,
			errDetailMustContain: "database is locked",
		},
		{
			name:                 "General Process Failure",
			exitCode:             127,
			stdout:               "",
			stderr:               "command not found: agy",
			wantFailure:          true,
			wantTransient:        false,
			wantCorrupt:          false,
			errDetailMustContain: "command not found",
		},
		{
			name:                 "Exit Code 0 With Partial Stdout And 503 In Stderr (Truncated Stream)",
			exitCode:             0,
			stdout:               `{"conversation_id":"abc","status":"ERROR","error":"503 service unavailable"}`,
			stderr:               "Agent execution terminated due to error. agent executor error: Error 503, Message: This model is currently experiencing high demand., Status: UNAVAILABLE",
			wantFailure:          true,
			wantTransient:        true,
			wantCorrupt:          false,
			errDetailMustContain: "503",
		},
		{
			name:                 "Exit Code 0 With Partial Stdout And Generic Error In Stderr",
			exitCode:             0,
			stdout:               `{"conversation_id":"abc","status":"SUCCESS","response":"partial"}`,
			stderr:               "Agent execution terminated due to error: panic: runtime error",
			wantFailure:          true,
			wantTransient:        false,
			wantCorrupt:          false,
			errDetailMustContain: "terminated",
		},
		{
			name:                 "Empty Stdout with Panic in Stderr and Exit Code 0",
			exitCode:             0,
			stdout:               "",
			stderr:               "panic: internal null pointer",
			wantFailure:          true,
			wantTransient:        false,
			wantCorrupt:          false,
			errDetailMustContain: "panic:",
		},
		{
			name:                 "MCP SSE Handshake Drop Does Not Corrupt Session (Exit Code 0 with Error)",
			exitCode:             0,
			stdout:               `{"conversation_id":"abc","status":"ERROR","error":"server name github failed to load: calling \"initialize\": sending \"initialize\": failed to connect (session ID: ): session not found"}`,
			stderr:               "",
			wantFailure:          true,
			wantTransient:        false,
			wantCorrupt:          false,
			errDetailMustContain: "session not found",
		},
		{
			name:                 "MCP SSE Handshake Drop in Non-Zero Exit Does Not Corrupt Session",
			exitCode:             1,
			stdout:               "",
			stderr:               "server name docker failed to load: calling \"initialize\": sending \"initialize\": failed to connect (session ID: ): session not found",
			wantFailure:          true,
			wantTransient:        false,
			wantCorrupt:          false,
			errDetailMustContain: "session not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isFailure, isTransient, isCorrupt, errDetail := ClassifyError(tt.exitCode, tt.stdout, tt.stderr)
			if isFailure != tt.wantFailure {
				t.Errorf("isFailure = %v, want %v", isFailure, tt.wantFailure)
			}
			if isTransient != tt.wantTransient {
				t.Errorf("isTransient = %v, want %v", isTransient, tt.wantTransient)
			}
			if isCorrupt != tt.wantCorrupt {
				t.Errorf("isCorrupt = %v, want %v", isCorrupt, tt.wantCorrupt)
			}
			if tt.errDetailMustContain != "" && !strings.Contains(errDetail, tt.errDetailMustContain) {
				t.Errorf("errDetail %q does not contain %q", errDetail, tt.errDetailMustContain)
			}
		})
	}
}

func TestExtractSessionID(t *testing.T) {
	stderr1 := "2026-08-28T10:00:00Z Starting conversation update stream for 12345678-abcd-1234-abcd-1234567890ab\nConnecting..."
	id1 := ExtractSessionID(stderr1, time.Now())
	if id1 != "12345678-abcd-1234-abcd-1234567890ab" {
		t.Errorf("Expected extracted ID 12345678-abcd-1234-abcd-1234567890ab, got %s", id1)
	}

	stderr2 := "Initialized session_id: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee successfully"
	id2 := ExtractSessionID(stderr2, time.Now())
	if id2 != "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee" {
		t.Errorf("Expected extracted ID aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee, got %s", id2)
	}
}

func TestRunAgyWithEcho(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// In linux container, echo is available
	stdout, stderr, exitCode, err := RunAgy(ctx, "echo", "Hello aerial", "", "", "", 1)
	if err != nil {
		t.Fatalf("RunAgy failed: %v", err)
	}
	if exitCode != 0 {
		t.Errorf("Expected exitCode 0, got %d", exitCode)
	}
	if !strings.Contains(stdout, "Hello aerial") {
		t.Errorf("Expected stdout to contain 'Hello aerial', got: %q (stderr: %q)", stdout, stderr)
	}
}

func TestRunAgy_OptionsAndFailures(t *testing.T) {
	ctx := context.Background()

	// 1. Run with full options (model, sessionID, apiKey, timeout)
	stdout, _, exitCode, err := RunAgy(ctx, "echo", "test prompt", "conv-123", "api-key-xyz", "gemini-pro", 5)
	if err != nil || exitCode != 0 {
		t.Errorf("RunAgy with options failed: %v, exitCode: %d", err, exitCode)
	}
	if !strings.Contains(stdout, "test prompt") {
		t.Errorf("expected stdout to contain prompt")
	}

	// 2. Run with default agyBin ("") and non-existent binary to test error handling
	_, _, exitCode, err = RunAgy(ctx, "/path/to/definitely/non_existent_binary", "prompt", "", "", "", 0)
	if err == nil || exitCode != -1 {
		t.Errorf("expected error and exitCode -1 for non-existent binary, got err=%v, code=%d", err, exitCode)
	}
}

func TestExtractSessionID_EdgeCases(t *testing.T) {
	if id := ExtractSessionID("", time.Now()); id != "" {
		t.Errorf("expected empty string for empty stderr, got %q", id)
	}
	if id := ExtractSessionID("random unformatted stderr", time.Now()); id != "" {
		t.Errorf("expected empty string for unformatted stderr, got %q", id)
	}
}

func TestClassifyError_AdditionalBranches(t *testing.T) {
	// 1. Exit code 0, invalid JSON, stderr has context window exceeded
	isFail, isTrans, isCorrupt, detail := ClassifyError(0, "invalid-json", "context window exceeded: 1000 tokens")
	if !isFail || !isCorrupt || isTrans || detail != "context window exceeded" {
		t.Errorf("unexpected classification for context window exceeded: %v, %v, %v, %s", isFail, isTrans, isCorrupt, detail)
	}

	// 2. Exit code 0, invalid JSON, stderr has session corrupt
	isFail, isTrans, isCorrupt, _ = ClassifyError(0, "invalid-json", "session corrupt: parse failure")
	if !isFail || !isCorrupt || isTrans {
		t.Errorf("unexpected classification for session corrupt: %v, %v, %v", isFail, isTrans, isCorrupt)
	}

	// 3. Exit code 0, invalid JSON, stderr has 503
	isFail, isTrans, isCorrupt, _ = ClassifyError(0, "invalid-json", "error 503: unavailable")
	if !isFail || isCorrupt || !isTrans {
		t.Errorf("unexpected classification for 503: %v, %v, %v", isFail, isTrans, isCorrupt)
	}

	// 4. Long error message line (> 200 characters)
	longLine := strings.Repeat("A", 250)
	_, _, _, detailLong := ClassifyError(1, "", longLine)
	if len(detailLong) > 200 || !strings.HasSuffix(detailLong, "...") {
		t.Errorf("expected long line to be truncated to 200 chars with '...', got len=%d: %s", len(detailLong), detailLong)
	}

	// 5. Non-zero exit code with empty stderr but non-empty stdout
	isFail, _, _, detailStdout := ClassifyError(1, "error reported in stdout", "")
	if !isFail || detailStdout != "error reported in stdout" {
		t.Errorf("expected detail from stdout, got %q", detailStdout)
	}

	// 6. Non-zero exit code with empty stderr and empty stdout
	_, _, _, detailEmpty := ClassifyError(42, "", "")
	if detailEmpty != "execution failed with exit code 42" {
		t.Errorf("expected fallback error detail, got %q", detailEmpty)
	}

	// 7. Non-zero exit code with conversation not found
	isFail, _, isCorrupt, _ = ClassifyError(1, "", "conversation not found")
	if !isFail || !isCorrupt {
		t.Errorf("expected isCorrupt=true for conversation not found")
	}

	// 8. Exit 0 with non-success status and empty error
	isFail, _, _, detailStatus := ClassifyError(0, `{"status":"FAILED"}`, "")
	if !isFail || detailStatus != "runner status: FAILED" {
		t.Errorf("expected runner status detail, got %q", detailStatus)
	}

	// 9. Exit 0 with success status but non-empty error field
	isFail, _, _, detailRespErr := ClassifyError(0, `{"status":"SUCCESS","error":"subtask timed out"}`, "")
	if !isFail || detailRespErr != "subtask timed out" {
		t.Errorf("expected error detail 'subtask timed out', got %q", detailRespErr)
	}

	// 10. Exit 0 with invalid JSON and fatal error in stderr
	isFail, _, _, detailFatal := ClassifyError(0, "not-json", "panic: nil pointer dereference")
	if !isFail || !strings.Contains(detailFatal, "panic") {
		t.Errorf("expected fatal error classification, got %q", detailFatal)
	}

	// 11. Stderr with only debug/info lines falls through in extractErrorDetail
	_, _, _, detailOnlyLogs := ClassifyError(1, "", "DEBUG loading models\nINFO connecting\nStarting conversation update stream for xyz\n\n")
	if detailOnlyLogs != "execution failed with exit code 1" {
		t.Errorf("expected fallback error detail for log-only stderr, got %q", detailOnlyLogs)
	}
}

func TestRunAgy_EmptyAgyBinFallback(t *testing.T) {
	tmpDir := t.TempDir()
	mockAgy := filepath.Join(tmpDir, "agy")
	_ = os.WriteFile(mockAgy, []byte("#!/bin/sh\necho '{\"status\":\"SUCCESS\"}'\n"), 0755)
	t.Setenv("PATH", tmpDir+":"+os.Getenv("PATH"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// agyBin is "" so it defaults to "agy" from PATH
	stdout, _, exitCode, err := RunAgy(ctx, "", "hello", "", "", "", 1)
	if err != nil || exitCode != 0 {
		t.Errorf("RunAgy with empty agyBin failed: %v, exitCode: %d", err, exitCode)
	}
	if !strings.Contains(stdout, "SUCCESS") {
		t.Errorf("unexpected stdout: %q", stdout)
	}
}

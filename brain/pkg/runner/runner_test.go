package runner

import (
	"context"
	"errors"
	"fmt"
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
			name:          "Clean Success: Conversational response discussing watchdog and inactivity timeout exceeded",
			exitCode:      0,
			stdout:        `{"conversation_id":"abc","status":"SUCCESS","response":"Here is how [watchdog] inactivity timeout exceeded works when monitoring stderr."}`,
			stderr:        "",
			wantFailure:   false,
			wantTransient: false,
			wantCorrupt:   false,
		},
		{
			name:          "Clean Success: Conversational response discussing watchdog max duration exceeded",
			exitCode:      0,
			stdout:        `{"conversation_id":"abc","status":"SUCCESS","response":"The [watchdog] max duration exceeded cap is set to 60m."}`,
			stderr:        "",
			wantFailure:   false,
			wantTransient: false,
			wantCorrupt:   false,
		},
		{
			name:                 "Transient Quota Exceeded In Stdout On Exit 1",
			exitCode:             1,
			stdout:               `{"status":"ERROR","error":"RESOURCE_EXHAUSTED: You have exceeded your current quota"}`,
			stderr:               "",
			wantFailure:          true,
			wantTransient:        true,
			wantCorrupt:          false,
			errDetailMustContain: "quota",
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
			wantTransient:        true,
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
		{
			name:                 "Transient ModelProvider Missing API Key Mismatch",
			exitCode:             1,
			stdout:               "",
			stderr:               `modelProvider is set to "gemini" in settings.json, but the GEMINI_API_KEY environment variable is not set. Set GEMINI_API_KEY to your Gemini API key, or remove "modelProvider" from settings.json to use the default backend.`,
			wantFailure:          true,
			wantTransient:        true,
			wantCorrupt:          false,
			errDetailMustContain: "modelProvider is set to \"gemini\"",
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


func TestActivityWriter_ThreadSafetyAndSessionDiscovery(t *testing.T) {
	w := NewActivityWriter("")
	if w.SessionID() != "" {
		t.Errorf("expected empty initial session ID, got %q", w.SessionID())
	}
	startNano := w.LastActivity().UnixNano()

	// Write session start log chunk
	chunk := []byte("INFO: Starting conversation update stream for 12345-abcd-6789\n")
	n, err := w.Write(chunk)
	if err != nil || n != len(chunk) {
		t.Fatalf("Write failed: n=%d, err=%v", n, err)
	}

	if w.SessionID() != "12345-abcd-6789" {
		t.Errorf("expected session ID 12345-abcd-6789, got %q", w.SessionID())
	}
	if w.LastActivity().UnixNano() < startNano {
		t.Errorf("expected lastActivity to advance")
	}
	if w.String() != string(chunk) {
		t.Errorf("expected buffered output %q, got %q", string(chunk), w.String())
	}

	// Test empty write does nothing
	nZero, errZero := w.Write([]byte{})
	if nZero != 0 || errZero != nil {
		t.Errorf("expected 0, nil from empty write, got %d, %v", nZero, errZero)
	}

	// Test initialized session ID retains value
	wPre := NewActivityWriter("pre-existing-uuid")
	if wPre.SessionID() != "pre-existing-uuid" {
		t.Errorf("expected pre-existing-uuid, got %q", wPre.SessionID())
	}
	_, _ = wPre.Write([]byte("INFO: Starting conversation update stream for different-uuid\n"))
	if wPre.SessionID() != "pre-existing-uuid" {
		t.Errorf("expected pre-existing session ID to not be overwritten, got %q", wPre.SessionID())
	}

	// Test general session regex discovery
	wGen := NewActivityWriter("")
	_, _ = wGen.Write([]byte("Initialized conversation_id: general-session-uuid-999\n"))
	if wGen.SessionID() != "general-session-uuid-999" {
		t.Errorf("expected general-session-uuid-999, got %q", wGen.SessionID())
	}

	// Concurrent writes and reads
	wConc := NewActivityWriter("")
	done := make(chan struct{})
	for i := 0; i < 10; i++ {
		go func(id int) {
			for j := 0; j < 50; j++ {
				_, _ = wConc.Write([]byte(fmt.Sprintf("log line from worker %d turn %d\n", id, j)))
				_ = wConc.LastActivity()
				_ = wConc.SessionID()
				_ = wConc.String()
			}
			done <- struct{}{}
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	if wConc.LastActivity().IsZero() {
		t.Errorf("expected non-zero LastActivity")
	}
	if len(wConc.String()) == 0 {
		t.Errorf("expected non-empty String buffer")
	}
}

func TestRunAgyWithWatchdog_InactivityTimeout(t *testing.T) {
	ctx := context.Background()
	opts := WatchdogOptions{
		InactivityTimeout: 50 * time.Millisecond,
		MaxDuration:       2 * time.Second,
		PollInterval:      10 * time.Millisecond,
	}

	mockAgy := filepath.Join(t.TempDir(), "mock-agy")
	script := "#!/bin/sh\nsleep 1\n"
	if err := os.WriteFile(mockAgy, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	stdout, stderr, exitCode, err := RunAgyWithWatchdog(
		ctx,
		mockAgy,
		"prompt",
		"",
		"",
		"",
		opts,
	)

	if exitCode == 0 {
		t.Errorf("expected non-zero exit code on inactivity timeout, got 0")
	}
	if !errors.Is(err, ErrInactivityTimeout) {
		t.Errorf("expected ErrInactivityTimeout, got: %v", err)
	}
	if !strings.Contains(stderr, "[watchdog]") || !strings.Contains(stderr, "inactivity timeout exceeded") {
		t.Errorf("expected [watchdog] inactivity timeout diagnosis in stderr, got: %s", stderr)
	}
	_ = stdout
}

func TestRunAgyWithWatchdog_ActiveStderrHeartbeat(t *testing.T) {
	ctx := context.Background()
	opts := WatchdogOptions{
		InactivityTimeout: 100 * time.Millisecond,
		MaxDuration:       2 * time.Second,
		PollInterval:      15 * time.Millisecond,
	}

	mockAgy := filepath.Join(t.TempDir(), "mock-agy")
	// 5 pulses 30ms apart = ~150ms total execution time, beating the 100ms inactivity timeout
	script := "#!/bin/sh\nfor i in 1 2 3 4 5; do\n  echo \"pulse $i\" >&2\n  sleep 0.03\ndone\necho '{\"status\":\"SUCCESS\",\"response\":\"done\"}'\n"
	if err := os.WriteFile(mockAgy, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	stdout, stderr, exitCode, err := RunAgyWithWatchdog(
		ctx,
		mockAgy,
		"prompt",
		"",
		"",
		"",
		opts,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exitCode != 0 {
		t.Errorf("expected exit code 0, got %d (stderr: %s)", exitCode, stderr)
	}
	if strings.Contains(stderr, "[watchdog]") {
		t.Errorf("unexpected watchdog intervention in stderr: %s", stderr)
	}
	if !strings.Contains(stdout, `"SUCCESS"`) {
		t.Errorf("expected stdout to contain SUCCESS, got %q", stdout)
	}
}

func TestRunAgyWithWatchdog_ActiveStdoutHeartbeat(t *testing.T) {
	ctx := context.Background()
	opts := WatchdogOptions{
		InactivityTimeout: 100 * time.Millisecond,
		MaxDuration:       2 * time.Second,
		PollInterval:      15 * time.Millisecond,
	}

	mockAgy := filepath.Join(t.TempDir(), "mock-agy")
	// 5 stdout pulses 30ms apart = ~150ms total execution time, beating the 100ms inactivity timeout
	script := "#!/bin/sh\nfor i in 1 2 3 4 5; do\n  echo \"stdout pulse $i\"\n  sleep 0.03\ndone\necho '{\"status\":\"SUCCESS\",\"response\":\"done\"}'\n"
	if err := os.WriteFile(mockAgy, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	stdout, stderr, exitCode, err := RunAgyWithWatchdog(
		ctx,
		mockAgy,
		"prompt",
		"",
		"",
		"",
		opts,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exitCode != 0 {
		t.Errorf("expected exit code 0, got %d (stderr: %s)", exitCode, stderr)
	}
	if strings.Contains(stderr, "[watchdog]") {
		t.Errorf("unexpected watchdog intervention in stderr: %s", stderr)
	}
	if !strings.Contains(stdout, "stdout pulse 5") {
		t.Errorf("expected stdout to contain stdout pulses, got: %q", stdout)
	}
}

func TestRunAgyWithWatchdog_CustomTranscriptDirs(t *testing.T) {
	ctx := context.Background()
	tempDir := t.TempDir()
	testSessionID := "custom-sess-9988"
	customLogDir := filepath.Join(tempDir, "custom-logs", testSessionID, ".system_generated", "logs")
	if err := os.MkdirAll(customLogDir, 0755); err != nil {
		t.Fatalf("failed to create custom log dir: %v", err)
	}
	transcriptFile := filepath.Join(customLogDir, "transcript.jsonl")

	opts := WatchdogOptions{
		InactivityTimeout: 100 * time.Millisecond,
		MaxDuration:       2 * time.Second,
		PollInterval:      15 * time.Millisecond,
		TranscriptDirs:    []string{filepath.Join(tempDir, "custom-logs")},
	}

	mockAgy := filepath.Join(t.TempDir(), "mock-agy")
	script := fmt.Sprintf("#!/bin/sh\nfor i in 1 2 3 4 5; do\n  echo \"{\\\"step\\\": $i}\" >> %s\n  sleep 0.03\ndone\necho '{\"status\":\"SUCCESS\",\"response\":\"done\"}'\n", transcriptFile)
	if err := os.WriteFile(mockAgy, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	stdout, stderr, exitCode, err := RunAgyWithWatchdog(
		ctx,
		mockAgy,
		"prompt",
		testSessionID,
		"",
		"",
		opts,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exitCode != 0 {
		t.Errorf("expected exit code 0, got %d (stderr: %s)", exitCode, stderr)
	}
	if strings.Contains(stderr, "[watchdog]") {
		t.Errorf("unexpected watchdog intervention in stderr: %s", stderr)
	}
	if !strings.Contains(stdout, `"SUCCESS"`) {
		t.Errorf("expected stdout to contain SUCCESS, got %q", stdout)
	}
}

func TestRunAgyWithWatchdog_TranscriptHeartbeat(t *testing.T) {
	ctx := context.Background()
	testSessionID := "test-transcript-session-12345"
	homeDir, err := os.UserHomeDir()
	if err != nil {
		homeDir = "/root"
	}
	logDir := filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain", testSessionID, ".system_generated", "logs")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		t.Fatalf("failed to create mock log dir: %v", err)
	}
	defer os.RemoveAll(filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain", testSessionID))

	transcriptFile := filepath.Join(logDir, "transcript.jsonl")

	opts := WatchdogOptions{
		InactivityTimeout: 100 * time.Millisecond,
		MaxDuration:       2 * time.Second,
		PollInterval:      15 * time.Millisecond,
	}

	mockAgy := filepath.Join(t.TempDir(), "mock-agy")
	// Writes to transcript.jsonl every 30ms for 5 pulses (~150ms > 100ms inactivity timeout)
	script := fmt.Sprintf("#!/bin/sh\nfor i in 1 2 3 4 5; do\n  echo \"{\\\"step\\\": $i}\" >> %s\n  sleep 0.03\ndone\necho '{\"status\":\"SUCCESS\",\"response\":\"done\"}'\n", transcriptFile)
	if err := os.WriteFile(mockAgy, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	stdout, stderr, exitCode, err := RunAgyWithWatchdog(
		ctx,
		mockAgy,
		"prompt",
		testSessionID,
		"",
		"",
		opts,
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if exitCode != 0 {
		t.Errorf("expected exit code 0, got %d (stderr: %s)", exitCode, stderr)
	}
	if strings.Contains(stderr, "[watchdog]") {
		t.Errorf("unexpected watchdog intervention in stderr: %s", stderr)
	}
	if !strings.Contains(stdout, `"SUCCESS"`) {
		t.Errorf("expected stdout to contain SUCCESS, got %q", stdout)
	}
}

func TestRunAgyWithWatchdog_MaxDuration(t *testing.T) {
	ctx := context.Background()
	opts := WatchdogOptions{
		InactivityTimeout: 500 * time.Millisecond,
		MaxDuration:       50 * time.Millisecond,
		PollInterval:      10 * time.Millisecond,
	}

	mockAgy := filepath.Join(t.TempDir(), "mock-agy")
	script := "#!/bin/sh\nsleep 1\n"
	if err := os.WriteFile(mockAgy, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	stdout, stderr, exitCode, err := RunAgyWithWatchdog(
		ctx,
		mockAgy,
		"prompt",
		"",
		"",
		"",
		opts,
	)

	if exitCode == 0 {
		t.Errorf("expected non-zero exit code on max duration exceeded, got 0")
	}
	if !errors.Is(err, ErrMaxDuration) {
		t.Errorf("expected ErrMaxDuration, got: %v", err)
	}
	if !strings.Contains(stderr, "[watchdog]") || !strings.Contains(stderr, "max duration exceeded") {
		t.Errorf("expected [watchdog] max duration exceeded in stderr, got: %s", stderr)
	}
	_ = stdout
}

func TestClassifyError_WatchdogInactivityNotTransient(t *testing.T) {
	stderr := "Starting conversation update stream for uuid-123\n[watchdog] inactivity timeout exceeded (5m without output or transcript update)"
	isFailure, isTransient, isSessionCorruption, errDetail := ClassifyError(-1, "", stderr)

	if !isFailure {
		t.Errorf("expected isFailure=true")
	}
	if isTransient {
		t.Errorf("expected isTransient=false for inactivity watchdog stall")
	}
	if isSessionCorruption {
		t.Errorf("expected isSessionCorruption=false")
	}
	if !strings.Contains(errDetail, "inactivity timeout exceeded") {
		t.Errorf("expected errDetail to contain inactivity timeout, got %q", errDetail)
	}
}

func TestClassifyError_WatchdogMaxDurationNotTransient(t *testing.T) {
	stderr := "Starting conversation update stream for uuid-123\n[watchdog] max duration exceeded (60m total duration cap)"
	isFailure, isTransient, isSessionCorruption, errDetail := ClassifyError(-1, "", stderr)

	if !isFailure {
		t.Errorf("expected isFailure=true")
	}
	if isTransient {
		t.Errorf("expected isTransient=false for max duration watchdog kill")
	}
	if isSessionCorruption {
		t.Errorf("expected isSessionCorruption=false")
	}
	if !strings.Contains(errDetail, "max duration exceeded") {
		t.Errorf("expected errDetail to contain max duration exceeded, got %q", errDetail)
	}
}

func TestIsInactivityTimeout(t *testing.T) {
	tests := []struct {
		name      string
		errDetail string
		stderr    string
		want      bool
	}{
		{
			name:      "Inactivity in errDetail",
			errDetail: "[watchdog] inactivity timeout exceeded (5m without output)",
			stderr:    "",
			want:      true,
		},
		{
			name:      "Inactivity in stderr",
			errDetail: "some other detail",
			stderr:    "logs...\n[watchdog] inactivity timeout exceeded (5m0s without output or transcript update)",
			want:      true,
		},
		{
			name:      "Case insensitivity",
			errDetail: "[WATCHDOG] Inactivity Timeout Exceeded",
			stderr:    "",
			want:      true,
		},
		{
			name:      "Max duration is not inactivity timeout",
			errDetail: "[watchdog] max duration exceeded (60m0s total duration cap)",
			stderr:    "",
			want:      false,
		},
		{
			name:      "Transient context deadline exceeded is not watchdog inactivity",
			errDetail: "context deadline exceeded",
			stderr:    "",
			want:      false,
		},
		{
			name:      "Generic timeout is not watchdog inactivity",
			errDetail: "",
			stderr:    "request timed out",
			want:      false,
		},
		{
			name:      "Empty strings",
			errDetail: "",
			stderr:    "",
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsInactivityTimeout(tt.errDetail, tt.stderr)
			if got != tt.want {
				t.Errorf("IsInactivityTimeout(%q, %q) = %v, want %v", tt.errDetail, tt.stderr, got, tt.want)
			}
		})
	}
}

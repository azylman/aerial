package runner

import (
	"bytes"
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
		{
			name: "Valid stream-json Success Stream",
			stdout: `{"event":"init","conversation_id":"22222222-3333-4444-5555-666666666666"}
{"event":"step_update","type":"tool_call","name":"view_file"}
{"event":"result","result":{"conversation_id":"22222222-3333-4444-5555-666666666666","status":"SUCCESS","response":"Stream finished!","duration_seconds":3.14,"num_turns":2,"usage":{"total_tokens":88}}}`,
			wantErr:    false,
			wantConvID: "22222222-3333-4444-5555-666666666666",
			wantStatus: "SUCCESS",
			wantResp:   "Stream finished!",
			wantTokens: 88,
		},
		{
			name: "Stream-json with Init Propagation to Result",
			stdout: `{"event":"init","conversation_id":"33333333-4444-5555-6666-777777777777"}
{"event":"step_update","type":"thinking"}
{"event":"result","result":{"status":"SUCCESS","response":"Propagated UUID!","duration_seconds":1.0,"num_turns":1}}`,
			wantErr:    false,
			wantConvID: "33333333-4444-5555-6666-777777777777",
			wantStatus: "SUCCESS",
			wantResp:   "Propagated UUID!",
		},
		{
			name: "Stream-json Error Stream",
			stdout: `{"event":"init","conversation_id":"44444444-5555-6666-7777-888888888888"}
{"event":"result","result":{"status":"ERROR","error":"model timeout"}}`,
			wantErr:    false,
			wantConvID: "44444444-5555-6666-7777-888888888888",
			wantStatus: "ERROR",
			wantResp:   "",
		},
		{
			name: "Truncated Stream-json Missing Result Event",
			stdout: `{"event":"init","conversation_id":"55555555-6666-7777-8888-999999999999"}
{"event":"step_update","type":"run_command"}`,
			wantErr: true,
		},
		{
			name: "Large Line (>100KB) Stream-json Without Scanner Panic",
			stdout: `{"event":"init","conversation_id":"66666666-7777-8888-9999-000000000000"}
{"event":"step_update","huge_payload":"` + strings.Repeat("a", 120*1024) + `"}
{"event":"result","result":{"status":"SUCCESS","response":"Handled large line","duration_seconds":2.0,"num_turns":1}}`,
			wantErr:    false,
			wantConvID: "66666666-7777-8888-9999-000000000000",
			wantStatus: "SUCCESS",
			wantResp:   "Handled large line",
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

func TestActivityTap_ChunkSplittingAndFiltering(t *testing.T) {
	var outBuf bytes.Buffer
	actWriter := NewActivityWriter("")
	tap := newActivityTap(&outBuf, actWriter, true)

	// Chunk 1 ends midway through the init event
	chunk1 := []byte("{\"event\":\"init\",\"conversa")
	n1, err1 := tap.Write(chunk1)
	if err1 != nil || n1 != len(chunk1) {
		t.Fatalf("Write(chunk1) = (%d, %v), want (%d, nil)", n1, err1, len(chunk1))
	}
	if actWriter.SessionID() != "" {
		t.Errorf("Expected empty sessionID before complete line, got %q", actWriter.SessionID())
	}

	// Chunk 2 completes the init event and has a step_update
	targetUUID := "12345678-abcd-ef01-2345-6789abcdef01"
	chunk2 := []byte(fmt.Sprintf("tion_id\":%q}\n{\"event\":\"step_update\",\"data\":\"lots of bloat\"}\n", targetUUID))
	n2, err2 := tap.Write(chunk2)
	if err2 != nil || n2 != len(chunk2) {
		t.Fatalf("Write(chunk2) = (%d, %v), want (%d, nil)", n2, err2, len(chunk2))
	}

	if actWriter.SessionID() != targetUUID {
		t.Errorf("Expected latched sessionID = %q, got %q", targetUUID, actWriter.SessionID())
	}

	// Chunk 3: result event
	chunk3 := []byte("{\"event\":\"result\",\"result\":{\"status\":\"SUCCESS\",\"response\":\"done\"}}\n")
	_, _ = tap.Write(chunk3)
	tap.Flush()

	out := outBuf.String()
	if !strings.Contains(out, targetUUID) {
		t.Errorf("Expected outBuf to contain init event with %s, got: %s", targetUUID, out)
	}
	if !strings.Contains(out, "SUCCESS") {
		t.Errorf("Expected outBuf to contain result event, got: %s", out)
	}
	if strings.Contains(out, "lots of bloat") {
		t.Errorf("Expected outBuf to filter out step_update events, but found 'lots of bloat' in: %s", out)
	}
}

func TestExtractSessionID_NDJSONInit(t *testing.T) {
	targetUUID := "88888888-9999-aaaa-bbbb-cccccccccccc"
	ndjsonOutput := fmt.Sprintf("{\"event\":\"init\",\"conversation_id\":%q}\n{\"event\":\"step_update\"}\n", targetUUID)
	extracted := ExtractSessionID(ndjsonOutput, time.Now())
	if extracted != targetUUID {
		t.Errorf("ExtractSessionID(ndjsonOutput) = %q, want %q", extracted, targetUUID)
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
			name:          "Empty Response With Status SUCCESS On Exit 0 (Silent Sentinel)",
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
	chunk := []byte("INFO: Starting conversation update stream for 12345678-abcd-ef01-2345-6789abcdef01\n")
	n, err := w.Write(chunk)
	if err != nil || n != len(chunk) {
		t.Fatalf("Write failed: n=%d, err=%v", n, err)
	}

	if w.SessionID() != "12345678-abcd-ef01-2345-6789abcdef01" {
		t.Errorf("expected session ID 12345678-abcd-ef01-2345-6789abcdef01, got %q", w.SessionID())
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
	wPre := NewActivityWriter("11111111-2222-3333-4444-555555555555")
	if wPre.SessionID() != "11111111-2222-3333-4444-555555555555" {
		t.Errorf("expected 11111111-2222-3333-4444-555555555555, got %q", wPre.SessionID())
	}
	_, _ = wPre.Write([]byte("INFO: Starting conversation update stream for 99999999-8888-7777-6666-555555555555\n"))
	if wPre.SessionID() != "11111111-2222-3333-4444-555555555555" {
		t.Errorf("expected pre-existing session ID to not be overwritten, got %q", wPre.SessionID())
	}

	// Test general session regex discovery
	wGen := NewActivityWriter("")
	_, _ = wGen.Write([]byte("Initialized conversation_id: 99999999-8888-7777-6666-555555555555\n"))
	if wGen.SessionID() != "99999999-8888-7777-6666-555555555555" {
		t.Errorf("expected 99999999-8888-7777-6666-555555555555, got %q", wGen.SessionID())
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

func TestClassifyError_Exit0_ResponseDiscussingContextLimit_NotCorrupt(t *testing.T) {
	stdout := `{"conversation_id":"11111111-2222-3333-4444-555555555555","status":"SUCCESS","response":"The maximum context length in tokens is 1 million. When context window exceeded occurs, the system rotates sessions."}`
	isFailure, isTransient, isCorrupt, errDetail := ClassifyError(0, stdout, "")
	if isFailure || isTransient || isCorrupt {
		t.Fatalf("Expected clean success, got failure=%v, transient=%v, corrupt=%v, errDetail=%q", isFailure, isTransient, isCorrupt, errDetail)
	}
}

func TestExtractSessionID_StrictUUID_IgnoresCommonWords(t *testing.T) {
	tests := []struct {
		name     string
		stderr   string
		wantUUID string
	}{
		{
			name:     "Session started words",
			stderr:   "Session started\nStarting session now",
			wantUUID: "",
		},
		{
			name:     "Conversation initialization-failed words",
			stderr:   "Conversation initialization-failed: could not load",
			wantUUID: "",
		},
		{
			name:     "Valid RFC4122 UUID in update stream",
			stderr:   "Starting conversation update stream for 12345678-abcd-ef01-2345-6789abcdef01",
			wantUUID: "12345678-abcd-ef01-2345-6789abcdef01",
		},
		{
			name:     "Valid RFC4122 UUID in session line",
			stderr:   "conversation_id: aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
			wantUUID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
		},
		{
			name:     "Invalid partial or hex length",
			stderr:   "conversation: 1234-5678",
			wantUUID: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractSessionID(tt.stderr, time.Now())
			if got != tt.wantUUID {
				t.Errorf("ExtractSessionID(%q) = %q, want %q", tt.stderr, got, tt.wantUUID)
			}
		})
	}
}

func TestActivityWriter_ChunkBoundaryUUID(t *testing.T) {
	w := NewActivityWriter("")
	chunk1 := []byte("Some logs... Starting conversation update stream for 12345678-abcd-")
	chunk2 := []byte("ef01-2345-6789abcdef01 and more logs...")

	_, _ = w.Write(chunk1)
	if sess := w.SessionID(); sess != "" {
		t.Errorf("Expected empty session after chunk1, got %q", sess)
	}

	_, _ = w.Write(chunk2)
	wantUUID := "12345678-abcd-ef01-2345-6789abcdef01"
	if sess := w.SessionID(); sess != wantUUID {
		t.Errorf("Expected extracted session %q after chunk2, got %q", wantUUID, sess)
	}
}

func TestIsQuotaPause(t *testing.T) {
	tests := []struct {
		name      string
		errDetail string
		stderr    string
		want      bool
	}{
		{
			name:      "Google subscription quota reached",
			errDetail: "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 16m58s.",
			stderr:    "",
			want:      true,
		},
		{
			name:      "Subscription quota in stderr",
			errDetail: "execution failed with exit code 1",
			stderr:    "[ERROR] Individual quota reached. Resets in 5m.",
			want:      true,
		},
		{
			name:      "Resource has been exhausted with reset timer",
			errDetail: "Resource has been exhausted (e.g. check quota). Resets in 45s.",
			stderr:    "",
			want:      true,
		},
		{
			name:      "Resource exhausted with quota in stderr",
			errDetail: "429 Too Many Requests",
			stderr:    "RESOURCE_EXHAUSTED: check quota limits. Resets in 30s.",
			want:      true,
		},
		{
			name:      "Generic 503 unavailable (transient, not quota pause)",
			errDetail: "503 Service Unavailable",
			stderr:    "The service is experiencing high demand",
			want:      false,
		},
		{
			name:      "Process syntax error",
			errDetail: "syntax error near token",
			stderr:    "",
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsQuotaPause(tt.errDetail, tt.stderr)
			if got != tt.want {
				t.Errorf("IsQuotaPause(%q, %q) = %v, want %v", tt.errDetail, tt.stderr, got, tt.want)
			}
		})
	}
}

func TestExtractQuotaResetDuration(t *testing.T) {
	tests := []struct {
		name         string
		errDetail    string
		stderr       string
		wantDur      time.Duration
		wantExact    bool
	}{
		{
			name:      "Standard Google minutes and seconds",
			errDetail: "Individual quota reached. Please upgrade your subscription to increase your limits. Resets in 16m58s.",
			stderr:    "",
			wantDur:   16*time.Minute + 58*time.Second,
			wantExact: true,
		},
		{
			name:      "Minutes and seconds with space",
			errDetail: "Resets in 16m 58s.",
			stderr:    "",
			wantDur:   16*time.Minute + 58*time.Second,
			wantExact: true,
		},
		{
			name:      "Spelled out unit minutes",
			errDetail: "Resets in 15 minutes.",
			stderr:    "",
			wantDur:   15 * time.Minute,
			wantExact: true,
		},
		{
			name:      "Short seconds",
			errDetail: "Resource has been exhausted. Resets in 45s.",
			stderr:    "",
			wantDur:   45 * time.Second,
			wantExact: true,
		},
		{
			name:      "Hours and minutes",
			errDetail: "Resets in 1h 15m.",
			stderr:    "",
			wantDur:   1*time.Hour + 15*time.Minute,
			wantExact: true,
		},
		{
			name:      "No reset time specified - fallback 20m",
			errDetail: "Individual quota reached. Please upgrade your subscription.",
			stderr:    "",
			wantDur:   20 * time.Minute,
			wantExact: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotDur, gotExact := ExtractQuotaResetDuration(tt.errDetail, tt.stderr)
			if gotDur != tt.wantDur || gotExact != tt.wantExact {
				t.Errorf("ExtractQuotaResetDuration(%q, %q) = (%v, %v), want (%v, %v)",
					tt.errDetail, tt.stderr, gotDur, gotExact, tt.wantDur, tt.wantExact)
			}
		})
	}
}

func TestActivityWriter_RawUUIDWithoutPrefix(t *testing.T) {
	w := NewActivityWriter("")
	// Invalid UUID in text
	_, _ = w.Write([]byte("some random text with 12345678-abcd-ef01-2345-6789abcdef0z invalid uuid"))
	if w.SessionID() != "" {
		t.Errorf("expected empty session ID for invalid uuid, got %q", w.SessionID())
	}

	// Valid UUID in text without stream or session keyword
	_, _ = w.Write([]byte("some random text with 11111111-2222-3333-4444-555555555555 embedded inside"))
	if w.SessionID() != "11111111-2222-3333-4444-555555555555" {
		t.Errorf("expected extracted raw UUID 11111111-2222-3333-4444-555555555555, got %q", w.SessionID())
	}
}

func TestRunAgyWithWatchdog_OptionDefaultsAndEdgeCases(t *testing.T) {
	ctx := context.Background()

	// 1. Test invalid / non-existent binary path triggers cmd.Start() error
	_, _, exitCode, err := RunAgyWithWatchdog(ctx, "/nonexistent/path/to/agy-bin", "prompt", "", "", "", WatchdogOptions{})
	if exitCode != -1 || err == nil {
		t.Errorf("expected exitCode -1 and error for nonexistent binary, got %d, %v", exitCode, err)
	}

	// 2. Test MaxDuration formatting with seconds (< 1 minute)
	mockAgy := filepath.Join(t.TempDir(), "mock-agy-sec")
	script := "#!/bin/sh\necho '{\"status\":\"SUCCESS\",\"response\":\"ok\"}'\n"
	if err := os.WriteFile(mockAgy, []byte(script), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	stdout, stderr, exitCode, err := RunAgyWithWatchdog(ctx, mockAgy, "test prompt", "sess-123", "secret-key", "gemini-pro", WatchdogOptions{
		InactivityTimeout: 2 * time.Second,
		MaxDuration:       30 * time.Second,
		PollInterval:      10 * time.Millisecond,
		TranscriptDirs: []string{
			filepath.Join(t.TempDir(), "%s"),
			filepath.Join(t.TempDir(), "{session}"),
			filepath.Join(t.TempDir(), "plain"),
		},
	})
	if err != nil || exitCode != 0 {
		t.Fatalf("RunAgyWithWatchdog failed: %v, exitCode=%d, stderr=%s", err, exitCode, stderr)
	}
	if !strings.Contains(stdout, "SUCCESS") {
		t.Errorf("expected SUCCESS in stdout, got %q", stdout)
	}

	// 3. Test modelProvider is set to \"gemini\" in stderr with non-zero exit code and apiKey == \"\"
	mockAgyModelErr := filepath.Join(t.TempDir(), "mock-agy-model-err")
	scriptErr := "#!/bin/sh\necho 'modelprovider is set to \"gemini\"' >&2\nexit 1\n"
	if err := os.WriteFile(mockAgyModelErr, []byte(scriptErr), 0755); err != nil {
		t.Fatalf("failed to write mock script: %v", err)
	}

	_, stderrErr, exitCodeErr, _ := RunAgyWithWatchdog(ctx, mockAgyModelErr, "prompt", "", "", "gemini-flash", WatchdogOptions{
		InactivityTimeout: 1 * time.Second,
		MaxDuration:       2 * time.Second,
		PollInterval:      10 * time.Millisecond,
	})
	if exitCodeErr != 1 {
		t.Errorf("expected exitCode 1, got %d", exitCodeErr)
	}
	if !strings.Contains(stderrErr, "modelprovider") {
		t.Errorf("expected stderr to contain modelprovider error, got %q", stderrErr)
	}

	// 4. Test RunAgy wrapper function
	stdoutWrap, stderrWrap, exitCodeWrap, errWrap := RunAgy(ctx, mockAgy, "wrap prompt", "", "", "", 1)
	if errWrap != nil || exitCodeWrap != 0 {
		t.Errorf("RunAgy wrapper failed: %v, code=%d, stderr=%s", errWrap, exitCodeWrap, stderrWrap)
	}
	if !strings.Contains(stdoutWrap, "SUCCESS") {
		t.Errorf("expected SUCCESS from RunAgy, got %q", stdoutWrap)
	}
}

func TestExtractSessionID_AdditionalBranches(t *testing.T) {
	// Empty stderr
	if id := ExtractSessionID("", time.Now()); id != "" {
		t.Errorf("expected empty string for empty stderr, got %q", id)
	}

	// Invalid candidate in reUpdateStream
	stderrInvalidStream := "Starting conversation update stream for not-a-uuid\n"
	if id := ExtractSessionID(stderrInvalidStream, time.Now()); id != "" {
		t.Errorf("expected empty string for invalid update stream uuid, got %q", id)
	}

	// Invalid candidate in reGeneralSession
	stderrInvalidGen := "conversation_id: not-a-valid-uuid\n"
	if id := ExtractSessionID(stderrInvalidGen, time.Now()); id != "" {
		t.Errorf("expected empty string for invalid general session uuid, got %q", id)
	}

	// Raw UUID in text (fallback reUUIDInText)
	stderrRaw := "Some unstructured error log mentioning 12345678-abcd-ef01-2345-6789abcdef01 in the trace"
	if id := ExtractSessionID(stderrRaw, time.Now()); id != "12345678-abcd-ef01-2345-6789abcdef01" {
		t.Errorf("expected raw UUID extracted, got %q", id)
	}
}

func TestIsInactivityTimeout_AdditionalWatchdogBranches(t *testing.T) {
	if !IsInactivityTimeout("[watchdog]", "inactivity detected") {
		t.Errorf("expected true when [watchdog] in errDetail and inactivity in stderr")
	}
	if !isWatchdogInactivity("[WATCHDOG] inactivity happened") {
		t.Errorf("expected true for [watchdog] inactivity")
	}
	if !isWatchdogMaxDuration("[WATCHDOG] max duration reached") {
		t.Errorf("expected true for [watchdog] max duration")
	}
	if isWatchdogInactivity("all good") {
		t.Errorf("expected false for regular text")
	}
	if isWatchdogMaxDuration("all good") {
		t.Errorf("expected false for regular text")
	}
}

func TestExtractWatchdogDetail_EdgeCases(t *testing.T) {
	// Line > 200 chars truncation
	longLine := "[watchdog] " + strings.Repeat("x", 250)
	detail := extractWatchdogDetail(longLine, "fallback")
	if len(detail) != 200 || !strings.HasSuffix(detail, "...") {
		t.Errorf("expected 200-char truncated detail with ..., got len %d: %q", len(detail), detail)
	}

	// Fallback keyword when no lines match [watchdog] or keyword
	fallback := extractWatchdogDetail("regular log line 1\nregular log line 2", "my-fallback-keyword")
	if fallback != "my-fallback-keyword" {
		t.Errorf("expected fallback keyword, got %q", fallback)
	}
}

func TestClassifyError_AdditionalScenarios(t *testing.T) {
	// 1. exitCode 0 with empty stdout and empty stderr -> process produced empty stdout
	isFail, isTrans, isCorrupt, errDetail := ClassifyError(0, "", "")
	if !isFail || !isTrans || isCorrupt || errDetail != "process produced empty stdout" {
		t.Errorf("unexpected empty stdout/stderr result: fail=%v trans=%v corrupt=%v detail=%q", isFail, isTrans, isCorrupt, errDetail)
	}

	// 2. exitCode 0 with empty stdout and fatal stderr -> isTransient=false
	isFail, isTrans, isCorrupt, errDetail = ClassifyError(0, "", "fatal error: runtime panic")
	if !isFail || isTrans || isCorrupt {
		t.Errorf("unexpected fatal stderr result: fail=%v trans=%v corrupt=%v detail=%q", isFail, isTrans, isCorrupt, errDetail)
	}

	// 3. exitCode 0 with unparseable stdout and context window keyword
	isFail, isTrans, isCorrupt, errDetail = ClassifyError(0, "not json maximum context length exceeded", "")
	if !isFail || isTrans || !isCorrupt || errDetail != "context window exceeded" {
		t.Errorf("unexpected context window parse error: fail=%v trans=%v corrupt=%v detail=%q", isFail, isTrans, isCorrupt, errDetail)
	}

	// 4. exitCode 0 with unparseable stdout and corruption keyword
	isFail, isTrans, isCorrupt, _ = ClassifyError(0, "not json corrupted session state", "failed to load conversation")
	if !isFail || isTrans || !isCorrupt {
		t.Errorf("unexpected corruption parse error: fail=%v trans=%v corrupt=%v", isFail, isTrans, isCorrupt)
	}

	// 5. exitCode 0 with unparseable stdout and transient keyword
	isFail, isTrans, isCorrupt, _ = ClassifyError(0, "not json 503 service unavailable", "high demand")
	if !isFail || !isTrans || isCorrupt {
		t.Errorf("unexpected transient parse error: fail=%v trans=%v corrupt=%v", isFail, isTrans, isCorrupt)
	}

	// 6. exitCode 0 with unparseable stdout and fatal stderr
	isFail, isTrans, isCorrupt, _ = ClassifyError(0, "not json output", "panic: nil pointer dereference")
	if !isFail || isTrans || isCorrupt {
		t.Errorf("unexpected fatal parse error: fail=%v trans=%v corrupt=%v", isFail, isTrans, isCorrupt)
	}

	// 7. exitCode 0 with unparseable stdout and no matching keyword
	isFail, isTrans, isCorrupt, errDetail = ClassifyError(0, "not json output", "regular stderr")
	if !isFail || isTrans || isCorrupt || !strings.Contains(errDetail, "invalid json response") {
		t.Errorf("unexpected generic invalid json result: fail=%v trans=%v corrupt=%v detail=%q", isFail, isTrans, isCorrupt, errDetail)
	}

	// 8. exitCode 0 with resp.Status != "SUCCESS" and resp.Error == ""
	stdoutStatusErr := `{"status":"FAILED"}`
	isFail, _, _, errDetail = ClassifyError(0, stdoutStatusErr, "")
	if !isFail || errDetail != "runner status: FAILED" {
		t.Errorf("unexpected status failure: fail=%v detail=%q", isFail, errDetail)
	}

	// 9. exitCode 0 with resp.Status == "SUCCESS" and resp.Error != ""
	stdoutRespErr := `{"status":"SUCCESS","error":"quota exceeded"}`
	isFail, isTrans, _, errDetail = ClassifyError(0, stdoutRespErr, "")
	if !isFail || !isTrans || errDetail != "quota exceeded" {
		t.Errorf("unexpected resp.Error failure: fail=%v trans=%v detail=%q", isFail, isTrans, errDetail)
	}

	// 10. exitCode 0 with resp.Status == "SUCCESS" and fatal stderr
	stdoutSuccess := `{"status":"SUCCESS","response":"all good"}`
	isFail, isTrans, isCorrupt, errDetail = ClassifyError(0, stdoutSuccess, "fatal: unrecoverable crash")
	if !isFail || isTrans || isCorrupt || !strings.Contains(errDetail, "fatal") {
		t.Errorf("unexpected fatal stderr with success json: fail=%v trans=%v corrupt=%v detail=%q", isFail, isTrans, isCorrupt, errDetail)
	}

	// 11. exitCode != 0 with conversation not found (corruption)
	isFail, _, isCorrupt, _ = ClassifyError(1, "", "conversation not found in registry")
	if !isFail || !isCorrupt {
		t.Errorf("expected session corruption for conversation not found, got corrupt=%v", isCorrupt)
	}

	// 12. exitCode != 0 with empty stderr and short stdout (<300 chars)
	isFail, _, _, errDetail = ClassifyError(2, "short failure message", "")
	if !isFail || errDetail != "short failure message" {
		t.Errorf("expected short stdout as errDetail, got %q", errDetail)
	}

	// 13. exitCode != 0 with empty stderr and JSON error in stdout
	isFail, _, _, errDetail = ClassifyError(2, `{"error":"inner json error"}`, "")
	if !isFail || errDetail != "inner json error" {
		t.Errorf("expected inner json error as errDetail, got %q", errDetail)
	}

	// 14. exitCode != 0 with empty stderr and long stdout (>300 chars)
	longStdout := strings.Repeat("a", 400)
	isFail, _, _, errDetail = ClassifyError(2, longStdout, "")
	if !isFail || errDetail != "execution failed with exit code 2" {
		t.Errorf("expected fallback exit code message, got %q", errDetail)
	}
}

func TestExtractErrorDetail_FilterNoiseAndTruncation(t *testing.T) {
	// Filter noise lines: Starting conversation update stream, DEBUG, INFO, blanks
	stderrWithNoise := `
Starting conversation update stream for session-123
DEBUG: initializing subsystem
INFO: connecting to gateway

Actual error occurred here
`
	detail := extractErrorDetail(stderrWithNoise, 1)
	if detail != "Actual error occurred here" {
		t.Errorf("expected 'Actual error occurred here', got %q", detail)
	}

	// Long error line truncation (>200 chars)
	longError := strings.Repeat("e", 250)
	detailLong := extractErrorDetail(longError, 1)
	if len(detailLong) != 200 || !strings.HasSuffix(detailLong, "...") {
		t.Errorf("expected 200-char truncated detail, got len %d: %q", len(detailLong), detailLong)
	}

	// Only noise lines in stderr -> fallback
	onlyNoise := "DEBUG: step 1\nINFO: step 2\nStarting conversation update stream for test\n"
	detailNoise := extractErrorDetail(onlyNoise, 42)
	if detailNoise != "execution failed with exit code 42" {
		t.Errorf("expected fallback for only-noise stderr, got %q", detailNoise)
	}
}

func TestExtractQuotaResetDuration_ClampingAndEdgeCases(t *testing.T) {
	// Clamped < 10s -> 30s
	durShort, exactShort := ExtractQuotaResetDuration("Resets in 5s.", "")
	if !exactShort || durShort != 30*time.Second {
		t.Errorf("expected 30s clamping for 5s, got %v, %v", durShort, exactShort)
	}

	// Clamped > 24h -> 24h
	durLong, exactLong := ExtractQuotaResetDuration("Resets in 48h.", "")
	if !exactLong || durLong != 24*time.Hour {
		t.Errorf("expected 24h clamping for 48h, got %v, %v", durLong, exactLong)
	}

	// Unparseable duration -> 20m fallback
	durBad, exactBad := ExtractQuotaResetDuration("Resets in abcdefg.", "")
	if exactBad || durBad != 20*time.Minute {
		t.Errorf("expected (20m, false) fallback for bad duration, got %v, %v", durBad, exactBad)
	}
}




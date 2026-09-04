package runner

import (
	"context"
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


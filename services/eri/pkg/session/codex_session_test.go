package session

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCodexInternal_ErrorLineExtraction(t *testing.T) {
	errorLineTests := []struct {
		name      string
		line      string
		wantError string
	}{
		{name: "invalid json", line: "not-json", wantError: ""},
		{name: "invalid payload", line: `{"type":"event_msg","payload":"not-obj"}`, wantError: ""},
		{name: "event_msg error string payload.error", line: `{"type":"event_msg","payload":{"type":"error","error":"raw error"}}`, wantError: "raw error"},
		{name: "event_msg error text", line: `{"type":"event_msg","payload":{"type":"error","text":"some text error"}}`, wantError: "some text error"},
		{name: "event_msg error empty fallback", line: `{"type":"event_msg","payload":{"type":"error"}}`, wantError: "error event"},
		{name: "event_msg turn_aborted with message", line: `{"type":"event_msg","payload":{"type":"turn_aborted","message":"aborted msg"}}`, wantError: "aborted msg"},
		{name: "event_msg turn_aborted empty fallback", line: `{"type":"event_msg","payload":{"type":"turn_aborted"}}`, wantError: "turn aborted"},
		{name: "response_item error with message", line: `{"type":"response_item","payload":{"type":"error","message":"resp msg err"}}`, wantError: "resp msg err"},
		{name: "response_item error with text blocks", line: `{"type":"response_item","payload":{"status":"error","content":[{"type":"text","text":"resp text err"}]}}`, wantError: "resp text err"},
		{name: "response_item error with raw json error", line: `{"type":"response_item","payload":{"status":"error","error":{"code":123}}}`, wantError: `{"code":123}`},
		{name: "event_msg error with message", line: `{"type":"event_msg","payload":{"type":"error","message":"raw msg error"}}`, wantError: "raw msg error"},
		{name: "event_msg turn_aborted with reason", line: `{"type":"event_msg","payload":{"type":"turn_aborted","reason":"user aborted"}}`, wantError: "user aborted"},
		{name: "event_msg unknown type", line: `{"type":"event_msg","payload":{"type":"other"}}`, wantError: ""},
		{name: "response_item status failed", line: `{"type":"response_item","payload":{"status":"failed","message":"step failed"}}`, wantError: "step failed"},
		{name: "response_item not error", line: `{"type":"response_item","payload":{"type":"message","role":"assistant","text":"ok"}}`, wantError: ""},
		{name: "unknown entry type", line: `{"type":"unknown","payload":{"type":"error"}}`, wantError: ""},
	}
	for _, tc := range errorLineTests {
		t.Run(tc.name, func(t *testing.T) {
			got, beforeCutoff := extractCodexErrorFromLine(tc.line, time.Time{})
			if beforeCutoff {
				t.Fatalf("expected beforeCutoff to be false with zero time cutoff")
			}
			if got != tc.wantError {
				t.Fatalf("extractCodexErrorFromLine = %q, want %q", got, tc.wantError)
			}
		})
	}
}

func TestCodexInternal_ResponseEdgeCases(t *testing.T) {
	t.Parallel()
	responseEdgeTests := []struct {
		name     string
		lines    []string
		wantResp string
	}{
		{name: "empty lines", lines: []string{"", "   "}, wantResp: ""},
		{name: "tool call requested prefix", lines: []string{`{"type":"response_item","payload":{"type":"message","role":"assistant","text":"[Tool Call Requested]: foo"}}`}, wantResp: ""},
		{name: "invalid json", lines: []string{"not-json"}, wantResp: ""},
		{name: "invalid payload", lines: []string{`{"type":"response_item","payload":"bad"}`}, wantResp: ""},
		{name: "assistant commentary skipped", lines: []string{`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","text":"commentary text"}}`}, wantResp: ""},
		{name: "assistant commentary skipped and earlier final answer rescued", lines: []string{
			`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","text":"final answer text"}}`,
			`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"commentary","text":"commentary text"}}`,
		}, wantResp: "final answer text"},
		{name: "assistant final_answer rescued", lines: []string{`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","text":"final answer text"}}`}, wantResp: "final answer text"},
		{name: "pre-tool assistant message followed by function_call returns empty", lines: []string{
			`{"type":"response_item","payload":{"type":"message","role":"assistant","text":"I will run a tool"}}`,
			`{"type":"response_item","payload":{"type":"function_call","name":"bash"}}`,
		}, wantResp: ""},
		{name: "pre-tool assistant message followed by local_shell_call returns empty", lines: []string{
			`{"type":"response_item","payload":{"type":"message","role":"assistant","text":"I will run shell"}}`,
			`{"type":"response_item","payload":{"type":"local_shell_call","command":"ls"}}`,
		}, wantResp: ""},
		{name: "local_shell_call followed by final answer rescues final answer", lines: []string{
			`{"type":"response_item","payload":{"type":"local_shell_call","command":"ls"}}`,
			`{"type":"response_item","payload":{"type":"message","role":"assistant","phase":"final_answer","text":"done running shell"}}`,
		}, wantResp: "done running shell"},
		{name: "pre-tool assistant message followed by function_call_output returns empty", lines: []string{
			`{"type":"response_item","payload":{"type":"message","role":"assistant","text":"I will run a tool"}}`,
			`{"type":"response_item","payload":{"type":"function_call_output","output":"success"}}`,
		}, wantResp: ""},
		{name: "pre-tool assistant message followed by custom_tool_call returns empty", lines: []string{
			`{"type":"response_item","payload":{"type":"message","role":"assistant","text":"I will run a tool"}}`,
			`{"type":"response_item","payload":{"type":"custom_tool_call","name":"tool"}}`,
		}, wantResp: ""},
		{name: "prior-turn assistant message followed by non-ambient user message returns empty", lines: []string{
			`{"type":"response_item","payload":{"type":"message","role":"assistant","text":"prior turn answer"}}`,
			`{"type":"response_item","payload":{"type":"message","role":"user","text":"new user turn"}}`,
		}, wantResp: ""},
		{name: "assistant message followed by ambient user message is still rescued", lines: []string{
			`{"type":"response_item","payload":{"type":"message","role":"assistant","text":"prior answer"}}`,
			`{"type":"response_item","payload":{"type":"message","role":"user","text":"[Chat ambient message] ping"}}`,
		}, wantResp: "prior answer"},
	}
	for _, tc := range responseEdgeTests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := scanCodexLinesForResponse(context.Background(), tc.lines, time.Time{})
			if got != tc.wantResp {
				t.Fatalf("scanCodexLinesForResponse = %q, want %q", got, tc.wantResp)
			}
		})
	}
}

func TestCodexInternal_UserLineClassification(t *testing.T) {
	userLineTests := []struct {
		name string
		line string
		want bool
	}{
		{name: "empty", line: "", want: false},
		{name: "invalid json", line: "not-json", want: false},
		{name: "invalid payload", line: `{"type":"response_item","payload":"bad"}`, want: false},
		{name: "non response_item", line: `{"type":"event_msg"}`, want: false},
		{name: "non message type", line: `{"type":"response_item","payload":{"type":"function_call"}}`, want: false},
		{name: "role not user", line: `{"type":"response_item","payload":{"type":"message","role":"assistant"}}`, want: false},
		{name: "ambient prefix", line: `{"type":"response_item","payload":{"type":"message","role":"user","text":"[Chat ambient message] hi"}}`, want: false},
		{name: "valid user prompt", line: `{"type":"response_item","payload":{"type":"message","role":"user","text":"real user prompt"}}`, want: true},
	}
	for _, tc := range userLineTests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isNonAmbientCodexUserLine(tc.line); got != tc.want {
				t.Fatalf("isNonAmbientCodexUserLine(%q) = %v, want %v", tc.line, got, tc.want)
			}
		})
	}
}

func TestCodexInternal_StepLineClassification(t *testing.T) {
	stepLineTests := []struct {
		name string
		line string
		want bool
	}{
		{name: "empty", line: "", want: false},
		{name: "invalid json", line: "not-json", want: false},
		{name: "invalid payload", line: `{"type":"response_item","payload":"bad"}`, want: false},
		{name: "non response_item", line: `{"type":"event_msg"}`, want: false},
		{name: "user message is not step", line: `{"type":"response_item","payload":{"type":"message","role":"user"}}`, want: false},
		{name: "assistant message is step", line: `{"type":"response_item","payload":{"type":"message","role":"assistant"}}`, want: true},
		{name: "function call is step", line: `{"type":"response_item","payload":{"type":"function_call"}}`, want: true},
	}
	for _, tc := range stepLineTests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isCodexStepLine(tc.line); got != tc.want {
				t.Fatalf("isCodexStepLine(%q) = %v, want %v", tc.line, got, tc.want)
			}
		})
	}
}

func TestCodexInternal_ExtractResponseFromCodexTranscriptFile(t *testing.T) {
	tmpDir := t.TempDir()
	emptyPath := filepath.Join(tmpDir, "empty.jsonl")
	if err := os.WriteFile(emptyPath, nil, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	validPath := filepath.Join(tmpDir, "valid.jsonl")
	validContent := `{"type":"response_item","timestamp":"2026-11-07T10:00:00Z","payload":{"type":"message","role":"user","text":"hello"}}` + "\n" +
		`{"type":"response_item","timestamp":"2026-11-07T10:00:02Z","payload":{"type":"message","role":"assistant","text":"hello from assistant"}}` + "\n"
	if err := os.WriteFile(validPath, []byte(validContent), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	tests := []struct {
		name     string
		path     string
		wantResp string
	}{
		{name: "empty file", path: emptyPath, wantResp: ""},
		{name: "valid file", path: validPath, wantResp: "hello from assistant"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractResponseFromCodexTranscriptFile(context.Background(), tc.path, time.Time{})
			if got != tc.wantResp {
				t.Fatalf("extractResponseFromCodexTranscriptFile = %q, want %q", got, tc.wantResp)
			}
		})
	}
}

func TestManager_Codex_ExtractLastTurnError(t *testing.T) {
	tmp := t.TempDir()
	homeDir := filepath.Join(tmp, "home")
	dataDir := filepath.Join(tmp, "data")
	mgr := New(homeDir, dataDir)

	sessDir := filepath.Join(dataDir, "codex", "sessions")
	if err := os.MkdirAll(sessDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	sessPath := filepath.Join(sessDir, "rollout-codex-err.jsonl")
	content := strings.Join([]string{
		`{"type":"response_item","timestamp":"2026-11-07T10:00:00Z","payload":{"type":"message","role":"user","text":"run command"}}`,
		`{"type":"event_msg","timestamp":"2026-11-07T10:00:05Z","payload":{"type":"error","text":"tool execution failed"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(sessPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	tests := []struct {
		name      string
		sessionID string
		wantError string
	}{
		{name: "valid codex error extracted", sessionID: "codex-err", wantError: "tool execution failed"},
		{name: "unknown session", sessionID: "unknown-sess", wantError: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mgr.ExtractLastTurnError(context.Background(), tc.sessionID, time.Time{})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.wantError {
				t.Fatalf("ExtractLastTurnError = %q, want %q", got, tc.wantError)
			}
		})
	}
}

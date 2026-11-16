package transcript

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestExtractCodexSessionID_TableDriven(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()

	fileWithID := filepath.Join(tmpDir, "meta_id.jsonl")
	_ = os.WriteFile(fileWithID, []byte("{\"type\":\"session_meta\",\"payload\":{\"id\":\"sess_from_id\"}}\n"), 0o644)

	fileWithSessionID := filepath.Join(tmpDir, "meta_sess_id.jsonl")
	_ = os.WriteFile(fileWithSessionID, []byte("\n{\"type\":\"session_meta\",\"payload\":{\"session_id\":\"sess_from_session_id\"}}\n"), 0o644)

	fileOther := filepath.Join(tmpDir, "meta_other.jsonl")
	_ = os.WriteFile(fileOther, []byte("{\"type\":\"response_item\"}\n"), 0o644)

	fileEmpty := filepath.Join(tmpDir, "empty.jsonl")
	_ = os.WriteFile(fileEmpty, []byte("\n\n"), 0o644)

	fileLargeMeta := filepath.Join(tmpDir, "large_meta.jsonl")
	largeLine := fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"id\":\"sess_large_meta_id\"},\"instructions\":%q}\n", strings.Repeat("x", 80*1024))
	_ = os.WriteFile(fileLargeMeta, []byte(largeLine), 0o644)

	tests := []struct {
		name     string
		path     string
		filename string
		want     string
	}{
		{name: "FromPayloadID", path: fileWithID, filename: "rollout-meta_id", want: "sess_from_id"},
		{name: "FromPayloadSessionID", path: fileWithSessionID, filename: "rollout-meta_sess_id", want: "sess_from_session_id"},
		{name: "FromPayloadLargeMetaOver64KB", path: fileLargeMeta, filename: "rollout-large-meta", want: "sess_large_meta_id"},
		{name: "FallbackRolloutTestPrefix", path: fileOther, filename: "rollout-test-abc-123", want: "abc-123"},
		{name: "FallbackRolloutPrefix", path: fileOther, filename: "rollout-def-456", want: "def-456"},
		{name: "TimestampedRolloutUUID", path: fileOther, filename: "rollout-2026-11-15T18-30-00-550e8400-e29b-41d4-a716-446655440000", want: "550e8400-e29b-41d4-a716-446655440000"},
		{name: "TimestampedRolloutMissingFileUUID", path: filepath.Join(tmpDir, "missing.jsonl"), filename: "rollout-1731720000-019335ef-0123-7abc-8def-0123456789ab", want: "019335ef-0123-7abc-8def-0123456789ab"},
		{name: "FallbackPlainName", path: fileEmpty, filename: "simple-session", want: "simple-session"},
		{name: "NonexistentFileRollout", path: filepath.Join(tmpDir, "missing.jsonl"), filename: "rollout-test-xyz", want: "xyz"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractCodexSessionID(tc.path, tc.filename)
			if got != tc.want {
				t.Errorf("extractCodexSessionID(%q, %q) = %q, want %q", tc.path, tc.filename, got, tc.want)
			}
		})
	}
}

type parseCodexEntryTestCase struct {
	name        string
	env         codexLineEnvelope
	wantHandled bool
	wantType    string
	wantStatus  string
	wantTool    string
	wantCallID  string
	wantContent string
	wantThink   string
}

func parseCodexEntryTestCases() []parseCodexEntryTestCase {
	return []parseCodexEntryTestCase{
		{
			name:        "meta envelopes ignored but handled",
			env:         codexLineEnvelope{Type: "session_meta"},
			wantHandled: true,
		},
		{
			name:        "turn context envelope ignored but handled",
			env:         codexLineEnvelope{Type: "turn_context"},
			wantHandled: true,
		},
		{
			name:        "event msg envelope ignored but handled",
			env:         codexLineEnvelope{Type: "event_msg"},
			wantHandled: true,
		},
		{
			name: "developer message ignored but handled",
			env: codexLineEnvelope{
				Type:    "response_item",
				Payload: json.RawMessage(`{"type":"message","role":"developer","content":"system instructions"}`),
			},
			wantHandled: true,
		},
		{
			name: "system message ignored but handled",
			env: codexLineEnvelope{
				Type:    "response_item",
				Payload: json.RawMessage(`{"type":"message","role":"system","content":"system prompt"}`),
			},
			wantHandled: true,
		},
		{
			name:        "unknown envelope type rejected",
			env:         codexLineEnvelope{Type: "unknown_type"},
			wantHandled: false,
		},
		{
			name:        "empty payload rejected",
			env:         codexLineEnvelope{Type: "response_item"},
			wantHandled: false,
		},
		{
			name:        "invalid JSON payload rejected",
			env:         codexLineEnvelope{Type: "response_item", Payload: json.RawMessage(`{not-json`)},
			wantHandled: false,
		},
		{
			name: "reasoning with summary fallback",
			env: codexLineEnvelope{
				Type:    "response_item",
				Payload: json.RawMessage(`{"type":"reasoning","summary":"summary thinking text"}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusDone,
			wantThink:   "summary thinking text",
		},
		{
			name: "local_shell_call with Cmd and ID",
			env: codexLineEnvelope{
				Type:    "response_item",
				Payload: json.RawMessage(`{"type":"local_shell_call","id":"sh_1","cmd":"echo hello"}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusDone,
			wantTool:    "local_shell",
			wantCallID:  "sh_1",
		},
		{
			name: "local_shell_call with Arguments and Input",
			env: codexLineEnvelope{
				Type:    "response_item",
				Payload: json.RawMessage(`{"type":"local_shell_call","call_id":"sh_2","arguments":"{\"command\":\"ls\"}"}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusDone,
			wantTool:    "local_shell",
			wantCallID:  "sh_2",
		},
		{
			name: "web_search_call with Query",
			env: codexLineEnvelope{
				Type:    "response_item",
				Payload: json.RawMessage(`{"type":"web_search_call","id":"ws_1","query":"golang testing"}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusDone,
			wantTool:    "web_search",
			wantCallID:  "ws_1",
		},
		{
			name: "web_search_call with Input",
			env: codexLineEnvelope{
				Type:    "response_item",
				Payload: json.RawMessage(`{"type":"web_search_call","call_id":"ws_2","input":{"query":"subagents"}}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusDone,
			wantTool:    "web_search",
			wantCallID:  "ws_2",
		},
		{
			name: "function_call_output with error status and fallback text",
			env: codexLineEnvelope{
				Type:    "response_item",
				Payload: json.RawMessage(`{"type":"function_call_output","id":"call_err","status":"error","text":"failed to exec"}`),
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  "ERROR",
			wantCallID:  "call_err",
			wantContent: "failed to exec",
		},
		{
			name: "function_call_output with failed status and is_error",
			env: codexLineEnvelope{
				Type:    "response_item",
				Payload: json.RawMessage(`{"type":"function_call_output","call_id":"call_fail","is_error":true,"output":"raw error message"}`),
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  "ERROR",
			wantCallID:  "call_fail",
			wantContent: "raw error message",
		},
		{
			name: "unknown payload type rejected",
			env: codexLineEnvelope{
				Type:    "response_item",
				Payload: json.RawMessage(`{"type":"unknown_payload"}`),
			},
			wantHandled: false,
		},
	}
}

func TestParseCodexEntry_TableDriven(t *testing.T) {
	t.Parallel()

	for _, tc := range parseCodexEntryTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &transcriptScanState{firstParsedStep: -1}
			handled := st.parseCodexEntry(tc.env)
			if handled != tc.wantHandled {
				t.Fatalf("parseCodexEntry() handled = %v, want %v", handled, tc.wantHandled)
			}
			if tc.wantType == "" && len(st.parsedSteps) != 0 {
				t.Fatalf("expected 0 parsed steps, got %d", len(st.parsedSteps))
			}
			if handled && len(st.parsedSteps) > 0 {
				assertParsedCodexStep(t, st.parsedSteps[0], tc.wantType, tc.wantStatus, tc.wantTool, tc.wantCallID, tc.wantContent, tc.wantThink)
			}
		})
	}
}

func assertParsedCodexStep(t *testing.T, step rawTranscriptStep, wantType, wantStatus, wantTool, wantCallID, wantContent, wantThink string) {
	t.Helper()
	if wantType != "" && step.Type != wantType {
		t.Errorf("step.Type = %q, want %q", step.Type, wantType)
	}
	if wantStatus != "" && step.Status != wantStatus {
		t.Errorf("step.Status = %q, want %q", step.Status, wantStatus)
	}
	if wantTool != "" && step.ToolName != wantTool {
		t.Errorf("step.ToolName = %q, want %q", step.ToolName, wantTool)
	}
	if wantCallID != "" && step.ToolCallID != wantCallID && (len(step.ToolCalls) == 0 || step.ToolCalls[0].ID != wantCallID) {
		t.Errorf("step callID = %q (toolCalls: %+v), want %q", step.ToolCallID, step.ToolCalls, wantCallID)
	}
	if wantContent != "" && step.Content != wantContent {
		t.Errorf("step.Content = %q, want %q", step.Content, wantContent)
	}
	if wantThink != "" && step.Thinking != wantThink {
		t.Errorf("step.Thinking = %q, want %q", step.Thinking, wantThink)
	}
}

func populateSyncTranscriptsEngineFixtures(t *testing.T, tmp, brainDir, projDir string, tc syncTranscriptsEngineCase) {
	t.Helper()
	for _, f := range tc.brainFiles {
		writeTranscriptFile(t, brainDir, f.sessionID, f.content, time.Now().Add(-f.age))
	}
	writeDatedSessionFiles(t, projDir, tc.claudeFiles)
	codexDir := filepath.Join(tmp, "codex", "sessions")
	_ = os.MkdirAll(codexDir, 0o755)
	writeDatedSessionFiles(t, codexDir, tc.codexFiles)
}

func writeDatedSessionFiles(t *testing.T, dir string, files []sessionFileFixture) {
	t.Helper()
	for _, f := range files {
		p := filepath.Join(dir, f.sessionID+".jsonl")
		if err := os.WriteFile(p, []byte(f.content), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		mtime := time.Now().Add(-f.age).Truncate(time.Second)
		if err := os.Chtimes(p, mtime, mtime); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
	}
}

func TestIsUUID_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{name: "ValidLowercase", in: "550e8400-e29b-41d4-a716-446655440000", want: true},
		{name: "ValidUppercase", in: "550E8400-E29B-41D4-A716-446655440000", want: true},
		{name: "TooShort", in: "550e8400-e29b-41d4-a716-44665544000", want: false},
		{name: "TooLong", in: "550e8400-e29b-41d4-a716-4466554400000", want: false},
		{name: "BadHyphenPos8", in: "550e8400xe29b-41d4-a716-446655440000", want: false},
		{name: "BadHyphenPos13", in: "550e8400-e29bx41d4-a716-446655440000", want: false},
		{name: "BadHyphenPos18", in: "550e8400-e29b-41d4x716-446655440000", want: false},
		{name: "BadHyphenPos23", in: "550e8400-e29b-41d4-a716x446655440000", want: false},
		{name: "NonHexDigit", in: "z50e8400-e29b-41d4-a716-446655440000", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isUUID(tc.in); got != tc.want {
				t.Errorf("isUUID(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestExtractOutputText_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		raw      json.RawMessage
		fallback string
		want     string
	}{
		{name: "FallbackNonEmpty", raw: json.RawMessage(`"hello"`), fallback: "fallback", want: "fallback"},
		{name: "RawEmpty", raw: nil, fallback: "", want: ""},
		{name: "RawJSONString", raw: json.RawMessage(`"unmarshaled text"`), fallback: "", want: "unmarshaled text"},
		{name: "RawJSONObject", raw: json.RawMessage(`{"key":"val"}`), fallback: "", want: `{"key":"val"}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := extractOutputText(tc.raw, tc.fallback); got != tc.want {
				t.Errorf("extractOutputText(%s, %q) = %q, want %q", string(tc.raw), tc.fallback, got, tc.want)
			}
		})
	}
}

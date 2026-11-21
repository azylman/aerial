package transcript

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type parseMuseEntryTestCase struct {
	name        string
	env         museLineEnvelope
	wantHandled bool
	wantType    string
	wantStatus  string
	wantTool    string
	wantCallID  string
	wantContent string
	wantArgs    string
}

func parseMuseEntryEventCases() []parseMuseEntryTestCase {
	return []parseMuseEntryTestCase{
		{
			name:        "unknown envelope type rejected",
			env:         museLineEnvelope{PayloadType: "unknown.type"},
			wantHandled: false,
		},
		{
			name:        "non-run kind ignored",
			env:         museLineEnvelope{PayloadType: "runtime.session", Payload: musePayload{Kind: "init"}},
			wantHandled: true,
		},
		{
			name:        "retained frame session_permission_transaction ignored",
			env:         museLineEnvelope{RetainedFrame: "session_permission_transaction"},
			wantHandled: true,
		},
		{
			name:        "runtime.session.metadata payload type ignored",
			env:         museLineEnvelope{PayloadType: "runtime.session.metadata"},
			wantHandled: true,
		},
		{
			name:        "session.started payload type ignored",
			env:         museLineEnvelope{PayloadType: "session.started"},
			wantHandled: true,
		},
		{
			name:        "run.model.configured payload type ignored",
			env:         museLineEnvelope{PayloadType: "run.model.configured"},
			wantHandled: true,
		},
		{
			name: "started user input event",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510000000000,
				Payload: musePayload{
					Kind:  "run",
					Event: museEvent{Kind: "started", Prompt: "Please refactor this service"},
				},
			},
			wantHandled: true,
			wantType:    stepTypeUserInput,
			wantStatus:  stepStatusDone,
			wantContent: "Please refactor this service",
		},
		{
			name: "started ambient input ignored",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510000000000,
				Payload: musePayload{
					Kind:  "run",
					Event: museEvent{Kind: "started", Prompt: "[Chat Alex]: hello"},
				},
			},
			wantHandled: true,
		},
		{
			name: "started empty prompt ignored",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510000000000,
				Payload: musePayload{
					Kind:  "run",
					Event: museEvent{Kind: "started", Prompt: ""},
				},
			},
			wantHandled: true,
		},
		{
			name: "assistant_message_committed event",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510001000000,
				Payload: musePayload{
					Kind:  "run",
					Event: museEvent{Kind: "assistant_message_committed", Text: "I will look into that."},
				},
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusDone,
			wantContent: "I will look into that.",
		},
		{
			name: "assistant_message_committed empty text ignored",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510001000000,
				Payload: musePayload{
					Kind:  "run",
					Event: museEvent{Kind: "assistant_message_committed", Text: ""},
				},
			},
			wantHandled: true,
		},
		{
			name: "terminal failed event with reason",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510006000000,
				Payload: musePayload{
					Kind:  "run",
					Event: museEvent{Kind: "terminal", Terminal: "failed", Reason: "Out of memory"},
				},
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusError,
			wantContent: "Out of memory",
		},
		{
			name: "terminal failed default reason",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510006000000,
				Payload: musePayload{
					Kind:  "run",
					Event: museEvent{Kind: "terminal", Terminal: "failed"},
				},
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusError,
			wantContent: "terminal failed",
		},
		{
			name: "terminal succeeded ignored",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510006000000,
				Payload: musePayload{
					Kind:  "run",
					Event: museEvent{Kind: "terminal", Terminal: "succeeded"},
				},
			},
			wantHandled: true,
		},
	}
}

func parseMuseEntryToolCallCases() []parseMuseEntryTestCase {
	return []parseMuseEntryTestCase{
		{
			name: "assistant_tool_calls_committed event",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510002000000,
				Payload: musePayload{
					Kind: "run",
					Event: museEvent{
						Kind: "assistant_tool_calls_committed",
						ToolCalls: []museToolCall{
							{Name: "bash", CallID: "call_123", Args: json.RawMessage(`{"command":"ls -la"}`)},
						},
					},
				},
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusDone,
			wantTool:    "bash",
			wantCallID:  "call_123",
			wantArgs:    "ls -la",
		},
		{
			name: "assistant_tool_calls_committed empty tool_calls ignored",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510002000000,
				Payload: musePayload{
					Kind:  "run",
					Event: museEvent{Kind: "assistant_tool_calls_committed"},
				},
			},
			wantHandled: true,
		},
		{
			name: "assistant_tool_calls_committed with ID fallback",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510002000000,
				Payload: musePayload{
					Kind: "run",
					Event: museEvent{
						Kind: "assistant_tool_calls_committed",
						ToolCalls: []museToolCall{
							{Name: "glob", ID: "call_id_only", Args: json.RawMessage(`{"pattern":"*.go"}`)},
						},
					},
				},
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusDone,
			wantTool:    "glob",
			wantCallID:  "call_id_only",
			wantArgs:    "*.go",
		},
	}
}

func parseMuseEntryToolResultCases() []parseMuseEntryTestCase {
	return []parseMuseEntryTestCase{
		{
			name: "tool_result_batch_committed event success",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510003000000,
				Payload: musePayload{
					Kind: "run",
					Event: museEvent{
						Kind: "tool_result_batch_committed",
						Results: []museToolResult{
							{CallID: "call_123", Output: "total 0"},
						},
					},
				},
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  stepStatusDone,
			wantCallID:  "call_123",
			wantContent: "total 0",
		},
		{
			name: "tool_result_batch_committed event error",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510004000000,
				Payload: musePayload{
					Kind: "run",
					Event: museEvent{
						Kind: "tool_result_batch_committed",
						Results: []museToolResult{
							{CallID: "call_123", Output: "exit code 1", IsError: true},
						},
					},
				},
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  stepStatusError,
			wantCallID:  "call_123",
			wantContent: "exit code 1",
		},
		{
			name: "tool_result_batch_committed fallback text",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510004000000,
				Payload: musePayload{
					Kind: "run",
					Event: museEvent{
						Kind: "tool_result_batch_committed",
						Results: []museToolResult{
							{CallID: "call_123", Text: "stderr output", IsError: true},
						},
					},
				},
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  stepStatusError,
			wantCallID:  "call_123",
			wantContent: "stderr output",
		},
		{
			name: "tool_result_batch_committed default error fallback",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510004000000,
				Payload: musePayload{
					Kind: "run",
					Event: museEvent{
						Kind: "tool_result_batch_committed",
						Results: []museToolResult{
							{CallID: "call_123", IsError: true},
						},
					},
				},
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  stepStatusError,
			wantCallID:  "call_123",
			wantContent: "tool failed",
		},
		{
			name: "tool_result_batch_committed with ToolCallID fallback",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  1762510003000000,
				Payload: musePayload{
					Kind: "run",
					Event: museEvent{
						Kind: "tool_result_batch_committed",
						Results: []museToolResult{
							{ToolCallID: "call_fallback", Output: "content output"},
						},
					},
				},
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  stepStatusDone,
			wantCallID:  "call_fallback",
			wantContent: "content output",
		},
	}
}

func parseMuseEntryTestCases() []parseMuseEntryTestCase {
	cases := append(parseMuseEntryEventCases(), parseMuseEntryToolCallCases()...)
	return append(cases, parseMuseEntryToolResultCases()...)
}

func assertParsedMuseStep(t *testing.T, st *transcriptScanState, tc parseMuseEntryTestCase) {
	t.Helper()
	if tc.wantType == "" {
		if len(st.parsedSteps) != 0 {
			t.Fatalf("[%s] expected 0 recorded steps, got %d", tc.name, len(st.parsedSteps))
		}
		return
	}
	if len(st.parsedSteps) != 1 {
		t.Fatalf("[%s] expected 1 recorded step, got %d", tc.name, len(st.parsedSteps))
	}
	step := st.parsedSteps[0]
	if step.Type != tc.wantType {
		t.Errorf("[%s] step.Type = %q, want %q", tc.name, step.Type, tc.wantType)
	}
	if step.Status != tc.wantStatus {
		t.Errorf("[%s] step.Status = %q, want %q", tc.name, step.Status, tc.wantStatus)
	}
	if tc.wantTool != "" && step.ToolName != tc.wantTool {
		t.Errorf("[%s] step.ToolName = %q, want %q", tc.name, step.ToolName, tc.wantTool)
	}
	if tc.wantCallID != "" && step.ToolCallID != tc.wantCallID {
		if len(step.ToolCalls) == 0 || step.ToolCalls[0].ID != tc.wantCallID {
			t.Errorf("[%s] step.ToolCallID = %q, want %q", tc.name, step.ToolCallID, tc.wantCallID)
		}
	}
	if tc.wantContent != "" && step.Content != tc.wantContent {
		t.Errorf("[%s] step.Content = %q, want %q", tc.name, step.Content, tc.wantContent)
	}
	if tc.wantArgs != "" {
		if len(step.ToolCalls) == 0 || !strings.Contains(string(step.ToolCalls[0].Args), tc.wantArgs) {
			t.Errorf("[%s] step.ToolCalls[0].Args = %s, want to contain %q", tc.name, string(step.ToolCalls[0].Args), tc.wantArgs)
		}
	}
}

func TestParseMuseEntry(t *testing.T) {
	t.Parallel()

	for _, tc := range parseMuseEntryTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &transcriptScanState{firstParsedStep: -1, maxStepIndex: -1}
			handled := st.parseMuseEntry(tc.env)
			if handled != tc.wantHandled {
				t.Fatalf("parseMuseEntry handled = %v, want %v", handled, tc.wantHandled)
			}
			if !tc.wantHandled {
				return
			}
			assertParsedMuseStep(t, st, tc)
		})
	}
}

func TestDiscoverMuseSessions(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()

	museDir1 := filepath.Join(tmp, "muse", "sessions")
	_ = os.MkdirAll(filepath.Join(museDir1, "sess-1"), 0o755)
	_ = os.WriteFile(filepath.Join(museDir1, "sess-1", "session.jsonl"), []byte(`{"payload_type":"runtime.session"}`+"\n"), 0o644)

	_ = os.WriteFile(filepath.Join(museDir1, "sess-2.jsonl"), []byte(`{"payload_type":"runtime.session"}`+"\n"), 0o644)

	// Hidden file should be skipped
	_ = os.WriteFile(filepath.Join(museDir1, ".hidden.jsonl"), []byte(`{"payload_type":"runtime.session"}`+"\n"), 0o644)

	discovered := make(map[string]discoveredSession)
	discoverMuseSessions([]string{museDir1}, discovered)

	if len(discovered) != 2 {
		t.Fatalf("discoverMuseSessions found %d sessions, want 2", len(discovered))
	}
	if s, ok := discovered["sess-1"]; !ok || s.engine != EngineMuse {
		t.Errorf("sess-1 missing or wrong engine: %+v", s)
	}
	if s, ok := discovered["sess-2"]; !ok || s.engine != EngineMuse {
		t.Errorf("sess-2 missing or wrong engine: %+v", s)
	}
}

func TestIsSkippedRuntimeDir(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		dir  string
		want bool
	}{
		{name: "empty string", dir: "", want: true},
		{name: "ephemeral runtime", dir: "ephemeral", want: true},
		{name: "dot prefixed hidden runtime", dir: ".hidden", want: true},
		{name: "discord runtime", dir: "discord", want: false},
		{name: "cli runtime", dir: "cli", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isSkippedRuntimeDir(tc.dir); got != tc.want {
				t.Errorf("isSkippedRuntimeDir(%q) = %v, want %v", tc.dir, got, tc.want)
			}
		})
	}
}

func TestFindTranscriptFile_Muse(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()

	tests := []struct {
		name      string
		relPath   string
		sessionID string
		wantFound bool
	}{
		{name: "muse direct", relPath: filepath.Join("muse", "sessions", "s-muse1.jsonl"), sessionID: "s-muse1", wantFound: true},
		{name: "muse session.jsonl", relPath: filepath.Join("muse", "sessions", "s-muse2", "session.jsonl"), sessionID: "s-muse2", wantFound: true},
		{name: "muse nested session.jsonl", relPath: filepath.Join("muse", "sessions", "2026", "11", "15", "s-muse3", "session.jsonl"), sessionID: "s-muse3", wantFound: true},
		{name: "muse nested direct jsonl", relPath: filepath.Join("muse", "sessions", "2026", "11", "15", "s-muse3b.jsonl"), sessionID: "s-muse3b", wantFound: true},
		{name: "muse dot local share", relPath: filepath.Join(".local", "share", "muse", "sessions", "s-muse4.jsonl"), sessionID: "s-muse4", wantFound: true},
		{name: "muse runtime discord share", relPath: filepath.Join("runtimes", "discord", ".local", "share", "muse", "sessions", "s-muse5.jsonl"), sessionID: "s-muse5", wantFound: true},
		{name: "muse runtime ephemeral share ignored", relPath: filepath.Join("runtimes", "ephemeral", ".local", "share", "muse", "sessions", "s-muse-ephem.jsonl"), sessionID: "s-muse-ephem", wantFound: false},
		{name: "muse runtime dot-prefixed share ignored", relPath: filepath.Join("runtimes", ".hidden", ".local", "share", "muse", "sessions", "s-muse-hidden.jsonl"), sessionID: "s-muse-hidden", wantFound: false},
	}

	brainDir := filepath.Join(tmp, "brain")
	_ = os.MkdirAll(brainDir, 0o755)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fullPath := filepath.Join(tmp, tc.relPath)
			_ = os.MkdirAll(filepath.Dir(fullPath), 0o755)
			_ = os.WriteFile(fullPath, []byte(`{"payload_type":"runtime.session"}`+"\n"), 0o644)

			gotPath := findTranscriptFile(brainDir, tc.sessionID)
			if tc.wantFound {
				if gotPath != fullPath {
					t.Fatalf("findTranscriptFile(%q) = %q, want %q", tc.sessionID, gotPath, fullPath)
				}
			} else {
				if gotPath != "" {
					t.Fatalf("findTranscriptFile(%q) = %q, want empty", tc.sessionID, gotPath)
				}
			}
		})
	}
}

func TestEngineForPath_Muse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "muse sessions dir", path: "/data/muse/sessions/2026/11/15/sess-1/session.jsonl", want: "muse"},
		{name: "muse dot local share sessions dir", path: "/data/runtimes/discord/.local/share/muse/sessions/sess-1.jsonl", want: "muse"},
		{name: "session.jsonl suffix anywhere", path: "/some/path/sess-1/session.jsonl", want: "muse"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := engineForPath("/data/brain", tc.path)
			if got != tc.want {
				t.Fatalf("engineForPath(%q) = %q, want %q", tc.path, got, tc.want)
			}
		})
	}
}

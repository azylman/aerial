package transcript

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
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
			name: "non-run kind ignored",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				Payload:     musePayload{Kind: "init"},
			},
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

func TestSyncSessionTranscript_Muse(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	store := dbtest.NewFakeStore()

	museDir := filepath.Join(tmp, "muse", "sessions")
	sessionID := "sess-muse-sync-test"
	sessDir := filepath.Join(museDir, sessionID)
	_ = os.MkdirAll(sessDir, 0o755)

	content := strings.Join([]string{
		`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"Create a new Go test"}}}`,
		`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"assistant_tool_calls_committed","tool_calls":[{"name":"bash","call_id":"call_1","args":{"command":"go test ./..."}}]}}}`,
		`{"payload_type":"runtime.session","recorded_at":1762510002000000,"payload":{"kind":"run","event":{"kind":"tool_result_batch_committed","results":[{"call_id":"call_1","output":"PASS"}]}}}`,
		`{"payload_type":"runtime.session","recorded_at":1762510003000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"Tests pass cleanly."}}}`,
	}, "\n") + "\n"

	transcriptFile := filepath.Join(sessDir, "session.jsonl")
	_ = os.WriteFile(transcriptFile, []byte(content), 0o644)

	opts := SyncOptions{
		BrainDir:        filepath.Join(tmp, "brain"),
		MuseSessionsDir: museDir,
		BatchLimit:      10,
		IdleThreshold:   10 * time.Minute,
		HeadLimit:       1000,
		TailLimit:       1000,
	}

	stats, err := SyncSessionTranscript(context.Background(), store, filepath.Join(tmp, "brain"), sessionID, opts)
	if err != nil {
		t.Fatalf("SyncSessionTranscript error: %v", err)
	}
	if stats.Synced != 1 {
		t.Fatalf("stats.Synced = %d, want 1", stats.Synced)
	}
	if stats.StepsInserted != 4 {
		t.Fatalf("stats.StepsInserted = %d, want 4", stats.StepsInserted)
	}

	states, err := store.GetSessionSyncStates(context.Background())
	if err != nil {
		t.Fatalf("GetSessionSyncStates error: %v", err)
	}
	if state, ok := states[sessionID]; !ok || state.Engine != EngineMuse {
		t.Errorf("session state = %+v, want engine %q", state, EngineMuse)
	}

	steps, err := store.GetTranscriptStepRange(context.Background(), "", sessionID, 0, 100)
	if err != nil {
		t.Fatalf("GetTranscriptStepRange error: %v", err)
	}
	if len(steps) != 4 {
		t.Fatalf("found %d steps, want 4", len(steps))
	}
	// Check tool step normalized kind
	toolStep := steps[1]
	if toolStep.ToolName != "bash" || toolStep.ToolKind != ToolKindShell {
		t.Errorf("toolStep tool=%q, kind=%q, want bash/shell", toolStep.ToolName, toolStep.ToolKind)
	}
}

func TestSyncSessionTranscript_Muse_ConfiguredDir(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()
	store := dbtest.NewFakeStore()

	// Isolated directory completely outside brainDir
	customDir := filepath.Join(tmp, "custom_isolated_muse_sessions")
	sessionSingle := "sess-single-configured"
	sessionBulk := "sess-bulk-configured"

	_ = os.MkdirAll(filepath.Join(customDir, sessionSingle), 0o755)
	_ = os.MkdirAll(filepath.Join(customDir, sessionBulk), 0o755)

	contentSingle := `{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"Single prompt"}}}` + "\n"
	contentBulk := `{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"Bulk prompt"}}}` + "\n"

	_ = os.WriteFile(filepath.Join(customDir, sessionSingle, "session.jsonl"), []byte(contentSingle), 0o644)
	_ = os.WriteFile(filepath.Join(customDir, sessionBulk, "session.jsonl"), []byte(contentBulk), 0o644)

	brainDir := filepath.Join(tmp, "brain")
	_ = os.MkdirAll(brainDir, 0o755)

	opts := SyncOptions{
		BrainDir:        brainDir,
		MuseSessionsDir: customDir,
		BatchLimit:      10,
		IdleThreshold:   10 * time.Minute,
	}

	// 1. Single-session sync
	statsSingle, err := SyncSessionTranscript(context.Background(), store, brainDir, sessionSingle, opts)
	if err != nil {
		t.Fatalf("SyncSessionTranscript error: %v", err)
	}
	if statsSingle.Synced != 1 {
		t.Fatalf("statsSingle.Synced = %d, want 1", statsSingle.Synced)
	}

	// 2. Bulk sync
	statsBulk, err := SyncTranscripts(context.Background(), store, brainDir, nil, nil, opts)
	if err != nil {
		t.Fatalf("SyncTranscripts error: %v", err)
	}
	if statsBulk.Synced < 1 {
		t.Fatalf("statsBulk.Synced = %d, want >= 1", statsBulk.Synced)
	}

	states, err := store.GetSessionSyncStates(context.Background())
	if err != nil {
		t.Fatalf("GetSessionSyncStates error: %v", err)
	}
	if state, ok := states[sessionSingle]; !ok || state.Engine != EngineMuse {
		t.Errorf("sessionSingle state = %+v, want engine %q", state, EngineMuse)
	}
	if state, ok := states[sessionBulk]; !ok || state.Engine != EngineMuse {
		t.Errorf("sessionBulk state = %+v, want engine %q", state, EngineMuse)
	}
}

func TestFindTranscriptFile_Muse(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()

	tests := []struct {
		name      string
		relPath   string
		sessionID string
	}{
		{name: "muse direct", relPath: filepath.Join("muse", "sessions", "s-muse1.jsonl"), sessionID: "s-muse1"},
		{name: "muse session.jsonl", relPath: filepath.Join("muse", "sessions", "s-muse2", "session.jsonl"), sessionID: "s-muse2"},
		{name: "muse nested session.jsonl", relPath: filepath.Join("muse", "sessions", "2026", "11", "15", "s-muse3", "session.jsonl"), sessionID: "s-muse3"},
		{name: "muse nested direct jsonl", relPath: filepath.Join("muse", "sessions", "2026", "11", "15", "s-muse3b.jsonl"), sessionID: "s-muse3b"},
		{name: "muse dot local share", relPath: filepath.Join(".local", "share", "muse", "sessions", "s-muse4.jsonl"), sessionID: "s-muse4"},
		{name: "muse runtime discord share", relPath: filepath.Join("runtimes", "discord", ".local", "share", "muse", "sessions", "s-muse5.jsonl"), sessionID: "s-muse5"},
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
			if gotPath != fullPath {
				t.Fatalf("findTranscriptFile(%q) = %q, want %q", tc.sessionID, gotPath, fullPath)
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

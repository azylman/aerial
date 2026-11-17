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

func parseMuseEntryEventCasesPart1() []parseMuseEntryTestCase {
	return []parseMuseEntryTestCase{
		{
			name:        "unknown envelope type rejected",
			env:         museLineEnvelope{PayloadType: "unknown.type"},
			wantHandled: false,
		},
		{
			name:        "empty payload rejected",
			env:         museLineEnvelope{PayloadType: "runtime.session"},
			wantHandled: false,
		},
		{
			name: "started user input event",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510000000000),
				Payload:     json.RawMessage(`{"event":{"kind":"started","prompt":"Please refactor this service"}}`),
			},
			wantHandled: true,
			wantType:    stepTypeUserInput,
			wantStatus:  stepStatusDone,
			wantContent: "Please refactor this service",
		},
		{
			name: "record payload user input",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510000000000),
				Payload:     json.RawMessage(`{"record":{"kind":"started","prompt":"Prompt from record"}}`),
			},
			wantHandled: true,
			wantType:    stepTypeUserInput,
			wantStatus:  stepStatusDone,
			wantContent: "Prompt from record",
		},
		{
			name: "flat payload user input",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510000000000),
				Payload:     json.RawMessage(`{"kind":"started","prompt":"Prompt from flat"}`),
			},
			wantHandled: true,
			wantType:    stepTypeUserInput,
			wantStatus:  stepStatusDone,
			wantContent: "Prompt from flat",
		},
		{
			name: "started ambient input ignored",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510000000000),
				Payload:     json.RawMessage(`{"event":{"kind":"started","prompt":"[Chat Alex]: hello"}}`),
			},
			wantHandled: true,
			wantType:    "",
		},
		{
			name: "assistant_message_committed event",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  "2026-11-07T10:00:01Z",
				Payload:     json.RawMessage(`{"event":{"kind":"assistant_message_committed","text":"I will look into that."}}`),
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
				RecordedAt:  "2026-11-07T10:00:01Z",
				Payload:     json.RawMessage(`{"event":{"kind":"assistant_message_committed","text":""}}`),
			},
			wantHandled: true,
			wantType:    "",
		},
		{
			name: "tool_call event",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510002000000),
				Payload:     json.RawMessage(`{"event":{"kind":"tool_call","name":"bash","id":"call_123","arguments":{"command":"ls -la"}}}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusDone,
			wantTool:    "bash",
			wantCallID:  "call_123",
		},
		{
			name: "tool_completed event success",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510003000000),
				Payload:     json.RawMessage(`{"event":{"kind":"tool_completed","id":"call_123","output":"total 0"}}`),
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  stepStatusDone,
			wantCallID:  "call_123",
			wantContent: "total 0",
		},
		{
			name: "tool_completed event error",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510004000000),
				Payload:     json.RawMessage(`{"event":{"kind":"tool_completed","id":"call_123","status":"failed","output":"exit code 1"}}`),
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  stepStatusError,
			wantCallID:  "call_123",
			wantContent: "exit code 1",
		},
		{
			name: "model_completed event",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510005000000),
				Payload:     json.RawMessage(`{"event":{"kind":"model_completed","text":"Finished task successfully."}}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusDone,
			wantContent: "Finished task successfully.",
		},
	}
}

func parseMuseEntryEventCasesPart2() []parseMuseEntryTestCase {
	return []parseMuseEntryTestCase{
		{
			name: "terminal failed event",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510006000000),
				Payload:     json.RawMessage(`{"event":{"kind":"terminal","terminal":"failed","reason":"Out of memory"}}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusError,
			wantContent: "Out of memory",
		},
		{
			name: "terminal failed with error bytes",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510006000000),
				Payload:     json.RawMessage(`{"event":{"kind":"terminal","terminal":"failed","error":{"message":"quota exceeded"}}}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusError,
			wantContent: "quota exceeded",
		},
		{
			name: "terminal failed with text",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510006000000),
				Payload:     json.RawMessage(`{"event":{"kind":"terminal","terminal":"failed","text":"Process killed"}}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusError,
			wantContent: "Process killed",
		},
		{
			name: "terminal failed default text",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510006000000),
				Payload:     json.RawMessage(`{"event":{"kind":"terminal","terminal":"failed"}}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusError,
			wantContent: "terminal failed",
		},
		{
			name: "terminal non-failed ignored",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510006000000),
				Payload:     json.RawMessage(`{"event":{"kind":"terminal","terminal":"succeeded"}}`),
			},
			wantHandled: true,
			wantType:    "",
		},
		{
			name: "terminal completed with null error ignored",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510006000000),
				Payload:     json.RawMessage(`{"event":{"kind":"terminal","terminal":"completed","error":null}}`),
			},
			wantHandled: true,
			wantType:    "",
		},
	}
}

func parseMuseEntryEventCases() []parseMuseEntryTestCase {
	return append(parseMuseEntryEventCasesPart1(), parseMuseEntryEventCasesPart2()...)
}

func parseMuseEntryMessageCases() []parseMuseEntryTestCase {
	return []parseMuseEntryTestCase{
		{
			name: "message role user event",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510007000000),
				Payload:     json.RawMessage(`{"type":"message","role":"user","content":"User follow up"}`),
			},
			wantHandled: true,
			wantType:    stepTypeUserInput,
			wantStatus:  stepStatusDone,
			wantContent: "User follow up",
		},
		{
			name: "message role user with null error",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510007000000),
				Payload:     json.RawMessage(`{"type":"message","role":"user","content":"User follow up","error":null}`),
			},
			wantHandled: true,
			wantType:    stepTypeUserInput,
			wantStatus:  stepStatusDone,
			wantContent: "User follow up",
		},
		{
			name: "message role user ambient ignored",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510007000000),
				Payload:     json.RawMessage(`{"type":"message","role":"user","content":"[Chat Alex]: hello"}`),
			},
			wantHandled: true,
			wantType:    "",
		},
		{
			name: "message role assistant event",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510007000000),
				Payload:     json.RawMessage(`{"type":"message","role":"assistant","content":"Direct message text"}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusDone,
			wantContent: "Direct message text",
		},
		{
			name: "message role assistant with null error",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510007000000),
				Payload:     json.RawMessage(`{"type":"message","role":"assistant","content":"Direct message text","error":null}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusDone,
			wantContent: "Direct message text",
		},
		{
			name: "message role assistant empty ignored",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510007000000),
				Payload:     json.RawMessage(`{"type":"message","role":"assistant","content":""}`),
			},
			wantHandled: true,
			wantType:    "",
		},
		{
			name: "payload without event or message ignored",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510007000000),
				Payload:     json.RawMessage(`{"type":"other"}`),
			},
			wantHandled: true,
			wantType:    "",
		},
	}
}

func parseMuseEntryToolCallCases() []parseMuseEntryTestCase {
	return []parseMuseEntryTestCase{
		{
			name: "tool_call event with tool and call_id",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510002000000),
				Payload:     json.RawMessage(`{"event":{"kind":"tool_call","tool":"read_file","call_id":"call_456","args":"{\"path\":\"foo.txt\"}"}}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusDone,
			wantTool:    "read_file",
			wantCallID:  "call_456",
		},
		{
			name: "tool_call event with input",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510002000000),
				Payload:     json.RawMessage(`{"event":{"kind":"tool_call","name":"glob","id":"call_input","input":{"pattern":"*.go"}}}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusDone,
			wantTool:    "glob",
			wantCallID:  "call_input",
			wantArgs:    "*.go",
		},
		{
			name: "tool_completed event with content",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510003000000),
				Payload:     json.RawMessage(`{"event":{"kind":"tool_completed","id":"call_content","content":"file content output"}}`),
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  stepStatusDone,
			wantCallID:  "call_content",
			wantContent: "file content output",
		},
		{
			name: "tool_completed event with result and error status",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510004000000),
				Payload:     json.RawMessage(`{"event":{"kind":"tool_completed","call_id":"call_456","status":"error","result":"file not found"}}`),
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  stepStatusError,
			wantCallID:  "call_456",
			wantContent: "file not found",
		},
		{
			name: "tool_completed event failed with reason",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510004000000),
				Payload:     json.RawMessage(`{"event":{"kind":"tool_completed","call_id":"call_reason","status":"failed","reason":"permission denied"}}`),
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  stepStatusError,
			wantCallID:  "call_reason",
			wantContent: "permission denied",
		},
		{
			name: "tool_completed event failed with error object",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510004000000),
				Payload:     json.RawMessage(`{"event":{"kind":"tool_completed","call_id":"call_err_obj","status":"failed","error":{"message":"command timed out"}}}`),
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  stepStatusError,
			wantCallID:  "call_err_obj",
			wantContent: "command timed out",
		},
		{
			name: "tool_completed event failed default fallback",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510004000000),
				Payload:     json.RawMessage(`{"event":{"kind":"tool_completed","call_id":"call_fallback","status":"failed"}}`),
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  stepStatusError,
			wantCallID:  "call_fallback",
			wantContent: "tool failed",
		},
	}
}

func parseMuseEntryFlatCases() []parseMuseEntryTestCase {
	return []parseMuseEntryTestCase{
		{
			name: "flat payload tool_call event",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510002000000),
				Payload:     json.RawMessage(`{"kind":"tool_call","name":"bash","id":"call_flat_1","arguments":{"command":"ls"}}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusDone,
			wantTool:    "bash",
			wantCallID:  "call_flat_1",
		},
		{
			name: "flat payload tool_completed event",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510003000000),
				Payload:     json.RawMessage(`{"kind":"tool_completed","id":"call_flat_1","name":"bash","output":"file.txt"}`),
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  stepStatusDone,
			wantTool:    "bash",
			wantCallID:  "call_flat_1",
			wantContent: "file.txt",
		},
		{
			name: "flat payload tool_completed error event",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510004000000),
				Payload:     json.RawMessage(`{"kind":"tool_completed","id":"call_flat_1","name":"bash","output":"failed","is_error":true}`),
			},
			wantHandled: true,
			wantType:    stepTypeTool,
			wantStatus:  stepStatusError,
			wantTool:    "bash",
			wantCallID:  "call_flat_1",
			wantContent: "failed",
		},
		{
			name: "flat payload terminal failed event",
			env: museLineEnvelope{
				PayloadType: "runtime.session",
				RecordedAt:  int64(1762510006000000),
				Payload:     json.RawMessage(`{"kind":"terminal","terminal":"failed","reason":"flat fatal error"}`),
			},
			wantHandled: true,
			wantType:    stepTypePlannerResponse,
			wantStatus:  stepStatusError,
			wantContent: "flat fatal error",
		},
	}
}

func parseMuseEntryTestCases() []parseMuseEntryTestCase {
	cases := append(parseMuseEntryEventCases(), parseMuseEntryToolCallCases()...)
	cases = append(cases, parseMuseEntryMessageCases()...)
	return append(cases, parseMuseEntryFlatCases()...)
}

func assertParsedMuseStep(t *testing.T, st *transcriptScanState, tc parseMuseEntryTestCase) {
	t.Helper()
	if tc.wantType == "" {
		if len(st.parsedSteps) != 0 {
			t.Fatalf("expected 0 recorded steps, got %d", len(st.parsedSteps))
		}
		return
	}
	if len(st.parsedSteps) != 1 {
		t.Fatalf("expected 1 recorded step, got %d", len(st.parsedSteps))
	}
	step := st.parsedSteps[0]
	if step.Type != tc.wantType {
		t.Errorf("step.Type = %q, want %q", step.Type, tc.wantType)
	}
	if step.Status != tc.wantStatus {
		t.Errorf("step.Status = %q, want %q", step.Status, tc.wantStatus)
	}
	if tc.wantTool != "" && step.ToolName != tc.wantTool {
		t.Errorf("step.ToolName = %q, want %q", step.ToolName, tc.wantTool)
	}
	if tc.wantCallID != "" && step.ToolCallID != tc.wantCallID {
		if len(step.ToolCalls) == 0 || step.ToolCalls[0].ID != tc.wantCallID {
			t.Errorf("step.ToolCallID = %q, want %q", step.ToolCallID, tc.wantCallID)
		}
	}
	if tc.wantContent != "" && step.Content != tc.wantContent {
		t.Errorf("step.Content = %q, want %q", step.Content, tc.wantContent)
	}
	if tc.wantArgs != "" {
		if len(step.ToolCalls) == 0 || !strings.Contains(string(step.ToolCalls[0].Args), tc.wantArgs) {
			t.Errorf("step.ToolCalls[0].Args = %s, want to contain %q", string(step.ToolCalls[0].Args), tc.wantArgs)
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
		`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Create a new Go test"}}}`,
		`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"tool_call","name":"bash","id":"call_1","arguments":{"command":"go test ./..."}}}}`,
		`{"payload_type":"runtime.session","recorded_at":1762510002000000,"payload":{"event":{"kind":"tool_completed","id":"call_1","name":"bash","output":"PASS"}}}`,
		`{"payload_type":"runtime.session","recorded_at":1762510003000000,"payload":{"event":{"kind":"assistant_message_committed","text":"Tests pass cleanly."}}}`,
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

	contentSingle := `{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Single prompt"}}}` + "\n"
	contentBulk := `{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Bulk prompt"}}}` + "\n"

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

func TestParseMuseTimestamp_Worker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		raw       any
		wantZero  bool
		wantEpoch int64
	}{
		{name: "nil", raw: nil, wantZero: true},
		{name: "empty", raw: "", wantZero: true},
		{name: "invalid", raw: "invalid", wantZero: true},
		{name: "rfc3339nano", raw: "2026-11-07T10:00:00.123456789Z", wantEpoch: 1794045600},
		{name: "rfc3339", raw: "2026-11-07T10:00:00Z", wantEpoch: 1794045600},
		{name: "string micro", raw: "1762510000000000", wantEpoch: 1762510000},
		{name: "string nano", raw: "1762510000000000000", wantEpoch: 1762510000},
		{name: "string seconds", raw: "1762510000", wantEpoch: 1762510000},
		{name: "string negative", raw: "-10", wantZero: true},
		{name: "string float", raw: "1762510000.5", wantEpoch: 1762510000},
		{name: "float64 nano", raw: float64(1762510000000000000), wantEpoch: 1762510000},
		{name: "float64 micro", raw: float64(1762510000000000), wantEpoch: 1762510000},
		{name: "float64 micro round up", raw: 1762510000999999.99, wantEpoch: 1762510001},
		{name: "float64 subsecond fraction", raw: float64(1762510000.75), wantEpoch: 1762510000},
		{name: "float64 seconds round up", raw: 1762510000.999999999, wantEpoch: 1762510001},
		{name: "float64 zero", raw: float64(0), wantZero: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := parseMuseTimestamp(tc.raw)
			if tc.wantZero && !got.IsZero() {
				t.Fatalf("parseMuseTimestamp(%v) = %v, want zero", tc.raw, got)
			}
			if !tc.wantZero && got.Unix() != tc.wantEpoch {
				t.Fatalf("parseMuseTimestamp(%v) = %v (unix %d), want %d", tc.raw, got, got.Unix(), tc.wantEpoch)
			}
		})
	}

	t.Run("extractMuseTimestamp", func(t *testing.T) {
		t.Parallel()
		if extractMuseTimestamp(museLineEnvelope{RecordedAt: "2026-11-07T10:00:00Z"}).IsZero() {
			t.Errorf("RecordedAt was zero")
		}
		if extractMuseTimestamp(museLineEnvelope{Timestamp: "2026-11-07T10:00:00Z"}).IsZero() {
			t.Errorf("Timestamp was zero")
		}
		if extractMuseTimestamp(museLineEnvelope{CreatedAt: "2026-11-07T10:00:00Z"}).IsZero() {
			t.Errorf("CreatedAt was zero")
		}
		if !extractMuseTimestamp(museLineEnvelope{}).IsZero() {
			t.Errorf("empty envelope gave non-zero timestamp")
		}
	})
}

func TestExtractMuseErrorBytes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  json.RawMessage
		want string
	}{
		{name: "empty", raw: nil, want: ""},
		{name: "null error", raw: json.RawMessage("null"), want: ""},
		{name: "string json", raw: json.RawMessage(`"permission denied"`), want: "permission denied"},
		{name: "object with message", raw: json.RawMessage(`{"message":"rate limit exceeded"}`), want: "rate limit exceeded"},
		{name: "raw fallback", raw: json.RawMessage(`{"code":500}`), want: `{"code":500}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := extractMuseErrorBytes(tc.raw)
			if got != tc.want {
				t.Fatalf("extractMuseErrorBytes() = %q, want %q", got, tc.want)
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

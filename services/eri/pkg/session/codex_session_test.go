package session

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type codexSessionLifecycleCase struct {
	name            string
	isNilMgr        bool
	sessionID       string
	location        string
	content         string
	zeroByte        bool
	age             time.Duration
	wantExists      bool
	wantPaths       int
	wantSize        int64
	wantSteps       int
	wantTurns       int
	wantNonZeroTime bool
}

func resolveCodexWriteDir(location, homeDir, dataDir string) (string, string) {
	switch location {
	case "data_codex":
		return filepath.Join(dataDir, "codex", "sessions", "2026", "11", "15"), "rollout-test-"
	case "data_runtime":
		return filepath.Join(dataDir, "runtimes", "discord", ".codex", "sessions"), ""
	case "home_codex":
		return filepath.Join(homeDir, ".codex", "sessions"), "rollout-"
	case "direct_codex":
		return filepath.Join(dataDir, "codex", "sessions"), ""
	default:
		return "", ""
	}
}

func setupCodexSessionFile(tc codexSessionLifecycleCase, homeDir, dataDir string, t0 time.Time) {
	writeDir, prefix := resolveCodexWriteDir(tc.location, homeDir, dataDir)
	if writeDir == "" {
		return
	}
	_ = os.MkdirAll(writeDir, 0o755)
	filePath := filepath.Join(writeDir, prefix+tc.sessionID+".jsonl")
	if tc.zeroByte {
		_ = os.WriteFile(filePath, nil, 0o644)
		return
	}
	if tc.content == "" {
		return
	}
	_ = os.WriteFile(filePath, []byte(tc.content), 0o644)
	mtime := t0
	if tc.age > 0 {
		mtime = time.Now().Add(-tc.age)
	}
	_ = os.Chtimes(filePath, mtime, mtime)
}

func assertCodexSessionMetrics(t *testing.T, mgr *Manager, tc codexSessionLifecycleCase, dataDir string) {
	t.Helper()
	actualPaths := mgr.codexTranscriptPaths(tc.sessionID)
	diskExists := mgr.CodexSessionExistsOnDisk(tc.sessionID)
	genericExists := mgr.SessionExistsOnDisk(tc.sessionID)
	measuredSize := mgr.GetTranscriptSize(tc.sessionID)
	stepTotal := mgr.CountTranscriptSteps(tc.sessionID)
	turnTotal := mgr.CountTranscriptTurns(tc.sessionID)

	if len(actualPaths) != tc.wantPaths || diskExists != tc.wantExists || genericExists != tc.wantExists {
		t.Errorf("[%s] state mismatch: paths=%d/%d disk=%v/%v gen=%v/%v",
			tc.name, len(actualPaths), tc.wantPaths, diskExists, tc.wantExists, genericExists, tc.wantExists)
	}

	if (tc.wantSize > 0 && measuredSize != tc.wantSize) || (tc.content != "" && measuredSize == 0) {
		t.Errorf("[%s] size mismatch: got=%d want=%d (hasContent=%v)", tc.name, measuredSize, tc.wantSize, tc.content != "")
	}

	if stepTotal != tc.wantSteps || turnTotal != tc.wantTurns {
		t.Errorf("[%s] count mismatch: steps=%d/%d turns=%d/%d", tc.name, stepTotal, tc.wantSteps, turnTotal, tc.wantTurns)
	}

	targetRoot := ""
	if tc.name == "custom roots override in GetSessionLastActivity" {
		targetRoot = filepath.Join(dataDir, "codex", "sessions")
	}
	lastAct, err := evaluateCodexLastActivity(mgr, tc.sessionID, targetRoot)
	if err != nil {
		t.Fatalf("[%s] GetSessionLastActivity error: %v", tc.name, err)
	}
	if (tc.wantNonZeroTime && lastAct.IsZero()) || (!tc.wantNonZeroTime && !lastAct.IsZero()) {
		t.Errorf("[%s] lastActivity time unexpected: zero=%v wantNonZero=%v", tc.name, lastAct.IsZero(), tc.wantNonZeroTime)
	}
}

func evaluateCodexLastActivity(mgr *Manager, sessionID, customRoot string) (time.Time, error) {
	if customRoot != "" {
		return mgr.GetSessionLastActivity(sessionID, customRoot)
	}
	return mgr.GetSessionLastActivity(sessionID)
}

func runCodexSessionLifecycleCase(t *testing.T, tc codexSessionLifecycleCase, t0 time.Time) {
	t.Helper()
	tmp := t.TempDir()
	var mgr *Manager
	var dataDir string
	if !tc.isNilMgr {
		homeDir := filepath.Join(tmp, "home")
		dataDir = filepath.Join(tmp, "data")
		mgr = New(homeDir, dataDir)
		setupCodexSessionFile(tc, homeDir, dataDir, t0)
	}
	assertCodexSessionMetrics(t, mgr, tc, dataDir)
}

func TestCodexSessionLifecycle(t *testing.T) {
	t0 := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)

	tests := []codexSessionLifecycleCase{
		{
			name:      "nil manager",
			isNilMgr:  true,
			sessionID: "sess-codex-nil",
		},
		{
			name:      "empty session ID",
			sessionID: "",
		},
		{
			name:      "path traversal session ID",
			sessionID: "../bad-id",
		},
		{
			name:      "data codex nested rollout session",
			sessionID: "sess-codex-data",
			location:  "data_codex",
			content: strings.Join([]string{
				`{"type":"response_item","timestamp":"2026-11-07T10:00:00Z","payload":{"type":"message","role":"user","content":[{"type":"text","text":"Initial prompt"}]}}`,
				`{"type":"response_item","timestamp":"2026-11-07T10:00:02Z","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"Done"}]}}`,
			}, "\n") + "\n",
			age:             10 * time.Minute,
			wantExists:      true,
			wantPaths:       1,
			wantSteps:       1,
			wantTurns:       1,
			wantNonZeroTime: true,
		},
		{
			name:      "data runtime codex session with tool calls and ambient",
			sessionID: "sess-codex-runtime",
			location:  "data_runtime",
			content: strings.Join([]string{
				`{"type":"response_item","timestamp":"2026-11-07T10:00:00Z","payload":{"type":"message","role":"user","content":[{"type":"text","text":"Prompt 1"}]}}`,
				`{"type":"response_item","timestamp":"2026-11-07T10:00:02Z","payload":{"type":"function_call","name":"Read","arguments":"{}"}}`,
				`{"type":"response_item","timestamp":"2026-11-07T10:00:04Z","payload":{"type":"function_call_output","output":"file contents"}}`,
				`{"type":"response_item","timestamp":"2026-11-07T10:00:05Z","payload":{"type":"message","role":"user","content":[{"type":"text","text":"[Chat ambient message]"}]}}`,
				`{"type":"response_item","timestamp":"2026-11-07T10:00:06Z","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"Final answer"}]}}`,
			}, "\n") + "\n",
			wantExists:      true,
			wantPaths:       1,
			wantSteps:       3,
			wantTurns:       1,
			wantNonZeroTime: true,
		},
		{
			name:      "home codex rollout session",
			sessionID: "sess-codex-home",
			location:  "home_codex",
			content: strings.Join([]string{
				`{"type":"response_item","timestamp":"2026-11-07T10:00:00Z","payload":{"type":"message","role":"user","content":[{"type":"text","text":"Home prompt"}]}}`,
			}, "\n") + "\n",
			wantExists:      true,
			wantPaths:       1,
			wantSteps:       0,
			wantTurns:       1,
			wantNonZeroTime: true,
		},
		{
			name:      "direct in codex base directory",
			sessionID: "sess-codex-direct",
			location:  "direct_codex",
			content: strings.Join([]string{
				`{"type":"response_item","timestamp":"2026-11-07T10:00:00Z","payload":{"type":"message","role":"user","content":[{"type":"text","text":"Direct prompt"}]}}`,
			}, "\n") + "\n",
			wantExists:      true,
			wantPaths:       1,
			wantTurns:       1,
			wantNonZeroTime: true,
		},
		{
			name:       "zero byte file does not exist",
			sessionID:  "sess-codex-zero",
			location:   "data_codex",
			zeroByte:   true,
			wantExists: false,
			wantPaths:  1,
			wantSize:   0,
		},
		{
			name:            "custom roots override in GetSessionLastActivity",
			sessionID:       "sess-codex-custom-root",
			location:        "data_codex",
			content:         `{"type":"response_item","timestamp":"2026-11-07T10:00:00Z","payload":{"type":"message","role":"user","content":[{"type":"text","text":"Hello"}]}}` + "\n",
			wantExists:      true,
			wantPaths:       1,
			wantTurns:       1,
			wantNonZeroTime: true,
		},
		{
			name:      "codex transcript with empty lines and invalid lines",
			sessionID: "sess-codex-empty-lines",
			location:  "data_codex",
			content: strings.Join([]string{
				"",
				"invalid-json",
				`{"type":"response_item","timestamp":"2026-11-07T10:00:00Z","payload":{"type":"message","role":"user","content":[{"type":"text","text":"Valid prompt"}]}}`,
				"",
				`{"type":"response_item","timestamp":"2026-11-07T10:00:02Z","payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"Valid reply"}]}}`,
				"",
			}, "\n") + "\n",
			wantExists:      true,
			wantPaths:       1,
			wantSteps:       1,
			wantTurns:       1,
			wantNonZeroTime: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runCodexSessionLifecycleCase(t, tc, t0)
		})
	}
}

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
			got, _ := extractCodexErrorFromLine(tc.line, time.Time{})
			if got != tc.wantError {
				t.Fatalf("extractCodexErrorFromLine = %q, want %q", got, tc.wantError)
			}
		})
	}
}

func TestCodexInternal_ExtractCodexText(t *testing.T) {
	textTests := []struct {
		name     string
		content  string
		text     string
		wantText string
	}{
		{name: "raw string content", content: `"raw content"`, wantText: "raw content"},
		{name: "text blocks", content: `[{"type":"text","text":"part 1"},{"type":"text","text":"part 2"}]`, wantText: "part 1\npart 2"},
		{name: "input_text and output_text blocks", content: `[{"type":"input_text","text":"prompt"},{"type":"output_text","text":"reply"}]`, wantText: "prompt\nreply"},
		{name: "fallback text", content: `{"invalid":1}`, text: "fallback text", wantText: "fallback text"},
		{name: "empty", content: ``, text: "", wantText: ""},
	}
	for _, tc := range textTests {
		t.Run(tc.name, func(t *testing.T) {
			got := extractCodexText(json.RawMessage(tc.content), tc.text)
			if got != tc.wantText {
				t.Fatalf("extractCodexText = %q, want %q", got, tc.wantText)
			}
		})
	}
}

func TestCodexInternal_ResponseEdgeCases(t *testing.T) {
	responseEdgeTests := []struct {
		name     string
		lines    []string
		wantResp string
	}{
		{name: "empty lines", lines: []string{"", "   "}, wantResp: ""},
		{name: "tool call requested prefix", lines: []string{`{"type":"response_item","payload":{"type":"message","role":"assistant","text":"[Tool Call Requested]: foo"}}`}, wantResp: ""},
		{name: "invalid json", lines: []string{"not-json"}, wantResp: ""},
		{name: "invalid payload", lines: []string{`{"type":"response_item","payload":"bad"}`}, wantResp: ""},
	}
	for _, tc := range responseEdgeTests {
		t.Run(tc.name, func(t *testing.T) {
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
	_ = os.WriteFile(emptyPath, nil, 0o644)

	validPath := filepath.Join(tmpDir, "valid.jsonl")
	validContent := `{"type":"response_item","timestamp":"2026-11-07T10:00:00Z","payload":{"type":"message","role":"user","text":"hello"}}` + "\n" +
		`{"type":"response_item","timestamp":"2026-11-07T10:00:02Z","payload":{"type":"message","role":"assistant","text":"hello from assistant"}}` + "\n"
	_ = os.WriteFile(validPath, []byte(validContent), 0o644)

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
	_ = os.MkdirAll(sessDir, 0o755)
	sessPath := filepath.Join(sessDir, "rollout-codex-err.jsonl")
	content := strings.Join([]string{
		`{"type":"response_item","timestamp":"2026-11-07T10:00:00Z","payload":{"type":"message","role":"user","text":"run command"}}`,
		`{"type":"event_msg","timestamp":"2026-11-07T10:00:05Z","payload":{"type":"error","text":"tool execution failed"}}`,
	}, "\n") + "\n"
	_ = os.WriteFile(sessPath, []byte(content), 0o644)

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

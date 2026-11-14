package session

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// EnsureSessionDir bootstraps the session directory and transcript files if they do not exist.
func (m *Manager) EnsureSessionDir(sessionID string) (string, error) {
	cleanID, ok := m.validSessionID(sessionID)
	if !ok {
		return "", fmt.Errorf("sessionID %q is invalid", sessionID)
	}

	baseDir := m.resolveBaseDir(cleanID)
	if baseDir == "" {
		return "", fmt.Errorf("no session roots configured")
	}
	sessionDir := filepath.Join(baseDir, cleanID)
	logsDir := filepath.Join(sessionDir, ".system_generated", "logs")

	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		return "", fmt.Errorf("failed to create session logs directory: %w", err)
	}

	for _, name := range []string{"transcript.jsonl", "transcript_full.jsonl"} {
		filePath := filepath.Join(logsDir, name)
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return "", fmt.Errorf("failed to ensure %s: %w", name, err)
		}
		if closeErr := f.Close(); closeErr != nil {
			log.Printf("[Session] Warning closing newly created transcript %s: %v", filePath, closeErr)
		}
	}

	return sessionDir, nil
}

// resolveBaseDir locates the appropriate brain base directory for sessions.
func (m *Manager) resolveBaseDir(sessionID string) string {
	if m == nil || len(m.roots) == 0 {
		return ""
	}
	for _, targetDir := range m.getTargetDirs(sessionID) {
		if fi, err := os.Stat(targetDir); err == nil && fi.IsDir() {
			return filepath.Dir(targetDir)
		}
	}
	for _, root := range m.roots {
		if fi, err := os.Stat(root); err == nil && fi.IsDir() {
			return root
		}
	}
	return m.roots[0]
}

// 1-line positional fixture helpers.
func writeTranscript(t *testing.T, mgr *Manager, sessionID, filename, content string) string {
	t.Helper()
	sessDir, err := mgr.EnsureSessionDir(sessionID)
	if err != nil {
		t.Fatalf("EnsureSessionDir(%q): %v", sessionID, err)
	}
	logsDir := filepath.Join(sessDir, ".system_generated", "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll logsDir: %v", err)
	}
	p := filepath.Join(logsDir, filename)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile %s: %v", filename, err)
	}
	return p
}

func writeConversationFile(t *testing.T, dir, sessionID, ext string, size int) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("MkdirAll %s: %v", dir, err)
	}
	p := filepath.Join(dir, sessionID+ext)
	if err := os.WriteFile(p, make([]byte, size), 0o644); err != nil {
		t.Fatalf("WriteFile %s: %v", p, err)
	}
	return p
}

type managerInitActivityCase struct {
	name              string
	isNilMgr          bool
	sessionID         string
	roots             []string
	setupLogs         bool
	setupTasks        bool
	setupClaudeDirect bool
	wantErr           bool
	wantZero          bool
}

func setupManagerInitActivityFixtures(tc managerInitActivityCase, tmp string) {
	if tc.setupClaudeDirect && len(tc.roots) > 0 {
		_ = os.MkdirAll(tc.roots[0], 0o755)
		_ = os.WriteFile(filepath.Join(tc.roots[0], tc.sessionID+".jsonl"), []byte(`{"type":"user"}`+"\n"), 0o644)
	}
	if tc.setupLogs {
		sessDir := filepath.Join(tmp, "home", ".gemini", "antigravity-cli", "brain", tc.sessionID)
		logsDir := filepath.Join(sessDir, ".system_generated", "logs")
		_ = os.MkdirAll(logsDir, 0o755)
		_ = os.WriteFile(filepath.Join(logsDir, "transcript.jsonl"), []byte("line1\n"), 0o644)
	}
	if tc.setupTasks {
		sessDir := filepath.Join(tmp, "home", ".gemini", "antigravity-cli", "brain", tc.sessionID)
		tasksDir := filepath.Join(sessDir, ".system_generated", "tasks")
		_ = os.MkdirAll(tasksDir, 0o755)
		_ = os.WriteFile(filepath.Join(tasksDir, "task-1.log"), []byte("log\n"), 0o644)
		_ = os.MkdirAll(filepath.Join(tasksDir, "nested-dir"), 0o755)
		_ = os.WriteFile(filepath.Join(tasksDir, "ignore.txt"), []byte("txt"), 0o644)
	}
}

func runManagerInitAndActivityCase(t *testing.T, m *Manager, tmp string, tc managerInitActivityCase) {
	t.Helper()
	mgr := m
	if tc.isNilMgr {
		mgr = nil
	}
	roots := tc.roots
	if roots == nil && mgr != nil {
		roots = mgr.Roots()
	}
	setupManagerInitActivityFixtures(tc, tmp)
	ts, err := mgr.GetSessionLastActivity(tc.sessionID, roots...)
	if (err != nil) != tc.wantErr {
		t.Fatalf("GetSessionLastActivity error=%v, wantErr=%v", err, tc.wantErr)
	}
	if tc.wantZero && !ts.IsZero() {
		t.Fatalf("expected zero timestamp, got %v", ts)
	} else if !tc.wantZero && ts.IsZero() {
		t.Fatalf("expected non-zero timestamp")
	}

	tsDirect, errDirect := LastActivityFromRoots(tc.sessionID, roots)
	if (errDirect != nil) != tc.wantErr {
		t.Fatalf("LastActivityFromRoots error=%v", errDirect)
	}
	if tc.wantZero && !tsDirect.IsZero() {
		t.Fatalf("LastActivityFromRoots expected zero, got %v", tsDirect)
	}
}

func TestManager_InitAndActivity(t *testing.T) {
	tmp := t.TempDir()
	m := New(filepath.Join(tmp, "home"), filepath.Join(tmp, "data"))
	if m == nil || m.homeDir == "" || m.dataDir == "" || len(m.Roots()) == 0 {
		t.Fatalf("unexpected Manager initialization: %+v", m)
	}
	var nilMgr *Manager
	if len(nilMgr.Roots()) != 0 {
		t.Fatalf("nil Manager should return nil roots")
	}

	tests := []managerInitActivityCase{
		{name: "nil manager", isNilMgr: true, sessionID: "sess-1", wantZero: true},
		{name: "empty session ID", sessionID: "", wantZero: true},
		{name: "path traversal ID", sessionID: "../bad", wantZero: true},
		{name: "empty roots", sessionID: "sess-1", roots: []string{}, wantZero: true},
		{name: "empty string root", sessionID: "sess-1", roots: []string{""}, wantZero: true},
		{name: "nonexistent roots", sessionID: "sess-1", roots: []string{filepath.Join(tmp, "missing")}, wantZero: true},
		{name: "percent s placeholder", sessionID: "sess-logs", roots: []string{filepath.Join(tmp, "home", ".gemini", "antigravity-cli", "brain", "%s")}, setupLogs: true},
		{name: "session placeholder", sessionID: "sess-logs", roots: []string{filepath.Join(tmp, "home", ".gemini", "antigravity-cli", "brain", "{session}")}, setupLogs: true},
		{name: "direct session root", sessionID: "sess-logs", roots: []string{filepath.Join(tmp, "home", ".gemini", "antigravity-cli", "brain", "sess-logs")}, setupLogs: true},
		{name: "direct logs root", sessionID: "sess-logs", roots: []string{filepath.Join(tmp, "home", ".gemini", "antigravity-cli", "brain", "sess-logs", ".system_generated", "logs")}, setupLogs: true},
		{name: "logs activity timestamp", sessionID: "sess-logs", setupLogs: true},
		{name: "task activity timestamp", sessionID: "sess-tasks", setupTasks: true},
		{name: "direct claude jsonl root", sessionID: "sess-claude-root", roots: []string{filepath.Join(tmp, "claude-direct")}, setupClaudeDirect: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runManagerInitAndActivityCase(t, m, tmp, tc)
		})
	}
}

type ensureSessionDirCase struct {
	name       string
	isNilMgr   bool
	emptyRoots bool
	sessionID  string
	preExist   bool
	wantErr    bool
}

func runEnsureSessionDirCase(t *testing.T, tc ensureSessionDirCase) {
	t.Helper()
	tmp := t.TempDir()
	var mgr *Manager
	if tc.emptyRoots {
		mgr = &Manager{}
	} else if !tc.isNilMgr {
		mgr = New(filepath.Join(tmp, "home"), filepath.Join(tmp, "data"))
		if tc.preExist {
			dir, _ := mgr.EnsureSessionDir(tc.sessionID)
			_ = os.MkdirAll(dir, 0o755)
		}
	}
	dir, err := mgr.EnsureSessionDir(tc.sessionID)
	if (err != nil) != tc.wantErr {
		t.Fatalf("EnsureSessionDir error=%v, wantErr=%v", err, tc.wantErr)
	}
	if !tc.wantErr && dir == "" {
		t.Fatalf("expected non-empty dir")
	}
}

func testResolveBaseDirBranches(t *testing.T) {
	t.Helper()
	tmp := t.TempDir()
	mgr := New(filepath.Join(tmp, "nonexistent-root-1"), filepath.Join(tmp, "nonexistent-root-2"))
	if base := mgr.resolveBaseDir("s1"); base == "" {
		t.Fatalf("expected fallback root")
	}
	var nilMgr *Manager
	if nilMgr.resolveBaseDir("s1") != "" {
		t.Fatalf("expected empty for nil manager")
	}
	existingRoot := filepath.Join(tmp, "home", ".gemini", "antigravity-cli", "brain")
	_ = os.MkdirAll(existingRoot, 0o755)
	mgrWithExisting := New(filepath.Join(tmp, "home"), "")
	preDir := filepath.Join(existingRoot, "sess-pre")
	_ = os.MkdirAll(preDir, 0o755)
	if base := mgrWithExisting.resolveBaseDir("sess-pre"); base != existingRoot {
		t.Fatalf("expected existing parent %q, got %q", existingRoot, base)
	}
	if base := mgrWithExisting.resolveBaseDir("s1"); base != existingRoot {
		t.Fatalf("expected existing root %q, got %q", existingRoot, base)
	}
	blockMgr := New(filepath.Join(tmp, "home"), filepath.Join(tmp, "data"))
	sessDir := filepath.Join(existingRoot, "sess-block")
	_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), 0o755)
	if _, err := blockMgr.EnsureSessionDir("sess-block"); err == nil {
		t.Fatalf("expected OpenFile error when transcript.jsonl is a directory")
	}
	mkdirBlockDir := filepath.Join(existingRoot, "sess-mkdir-block")
	_ = os.MkdirAll(mkdirBlockDir, 0o755)
	_ = os.WriteFile(filepath.Join(mkdirBlockDir, ".system_generated"), []byte("file"), 0o644)
	if _, err := blockMgr.EnsureSessionDir("sess-mkdir-block"); err == nil {
		t.Fatalf("expected MkdirAll error when .system_generated is a file")
	}
}

func TestEnsureSessionDirAndBaseDir(t *testing.T) {
	tests := []ensureSessionDirCase{
		{name: "nil manager", isNilMgr: true, sessionID: "sess-1", wantErr: true},
		{name: "empty session ID", sessionID: "", wantErr: true},
		{name: "path traversal", sessionID: "../bad", wantErr: true},
		{name: "empty roots manager", emptyRoots: true, sessionID: "sess-no-roots", wantErr: true},
		{name: "valid new session", sessionID: "sess-valid", wantErr: false},
		{name: "preexisting session", sessionID: "sess-pre", preExist: true, wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runEnsureSessionDirCase(t, tc)
		})
	}

	t.Run("getTargetDirs branches", func(t *testing.T) {
		tmp := t.TempDir()
		mgr := New(filepath.Join(tmp, "home"), filepath.Join(tmp, "data"))
		_ = os.MkdirAll(filepath.Join(tmp, "home", ".gemini", "antigravity-cli", "brain", "session-latest"), 0o755)
		dirsWithID := mgr.getTargetDirs("test-conv")
		if len(dirsWithID) == 0 {
			t.Fatalf("expected target dirs for test-conv")
		}
		dirsEmpty := mgr.getTargetDirs("")
		if len(dirsEmpty) == 0 {
			t.Fatalf("expected target dirs for empty convID")
		}
		var nilMgr *Manager
		if len(nilMgr.getTargetDirs("test")) != 0 {
			t.Fatalf("expected nil target dirs for nil manager")
		}
	})

	t.Run("resolveBaseDir and EnsureSessionDir branches", func(t *testing.T) {
		testResolveBaseDirBranches(t)
	})
}

func TestGetLastStepIndex(t *testing.T) {
	tests := []struct {
		name         string
		isMissing    bool
		isDir        bool
		isParentFile bool
		content      string
		wantIdx      int
		wantErr      bool
	}{
		{name: "nonexistent file", isMissing: true, wantIdx: -1, wantErr: false},
		{name: "parent is regular file", isParentFile: true, wantIdx: -1, wantErr: true},
		{name: "empty file", content: "", wantIdx: -1, wantErr: false},
		{name: "directory path", isDir: true, wantIdx: -1, wantErr: true},
		{name: "invalid json", content: "invalid json line\n", wantIdx: -1, wantErr: false},
		{name: "trailing newlines", content: "{\"step_index\":5}\n\n\n", wantIdx: 5, wantErr: false},
		{name: "empty line in seek", content: "{\"step_index\":1}\n\n{\"step_index\":9}\n", wantIdx: 9, wantErr: false},
		{name: "large chunk seek", content: fmt.Sprintf(`{"step_index":42,"content":%q}`+"\n", strings.Repeat("x", 80000)), wantIdx: 42, wantErr: false},
		{name: "large file no step index", content: strings.Repeat("not a valid step line\n", 4000), wantIdx: -1, wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			p := filepath.Join(tmp, "transcript.jsonl")
			switch {
			case tc.isDir:
				p = tmp
			case tc.isParentFile:
				parentFile := filepath.Join(tmp, "not-a-dir")
				_ = os.WriteFile(parentFile, []byte("file"), 0o644)
				p = filepath.Join(parentFile, "transcript.jsonl")
			case !tc.isMissing:
				_ = os.WriteFile(p, []byte(tc.content), 0o644)
			}
			idx, err := getLastStepIndex(p)
			if (err != nil) != tc.wantErr {
				t.Fatalf("getLastStepIndex error=%v, wantErr=%v", err, tc.wantErr)
			}
			if idx != tc.wantIdx {
				t.Fatalf("getLastStepIndex=%d, want %d", idx, tc.wantIdx)
			}
		})
	}
}

type extractLastTurnErrorCase struct {
	name             string
	isNilMgr         bool
	convID           string
	transcript       string
	claudeTranscript string
	writeFilename    string
	since            time.Time
	cancelCtx        bool
	wantError        string
	wantErr          bool
}

func standardExtractTurnCases(t0, t1 time.Time) []extractLastTurnErrorCase {
	return []extractLastTurnErrorCase{
		{name: "nil manager", isNilMgr: true, convID: "s1"},
		{name: "empty convID", convID: ""},
		{name: "path traversal", convID: "../bad"},
		{name: "canceled context", convID: "s-canc", cancelCtx: true, wantErr: true},
		{
			name:   "extracts error after user input",
			convID: "s-turn-err",
			transcript: fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","created_at":%q}`+"\n"+
				`{"step_index":1,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","error":"disk full","created_at":%q}`+"\n",
				t0.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)),
			since:     t0,
			wantError: "disk full",
		},
		{
			name:          "transcript file fallback when full is empty",
			convID:        "s-transcript-fallback",
			writeFilename: "transcript.jsonl",
			transcript: fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","created_at":%q}`+"\n"+
				`{"step_index":1,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","error":"fallback err","created_at":%q}`+"\n",
				t0.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)),
			since:     t0,
			wantError: "fallback err",
		},
		{
			name:   "unquotes json string error in last turn",
			convID: "s-unquoted-err",
			transcript: fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","created_at":%q}`+"\n\n"+
				`{"step_index":1,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","error":%q,"created_at":%q}`+"\n",
				t0.Format(time.RFC3339Nano), "quoted error string", t1.Format(time.RFC3339Nano)),
			since:     t0,
			wantError: "quoted error string",
		},
		{
			name:   "step created before since stops scan",
			convID: "s-before-since",
			transcript: fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","created_at":%q}`+"\n"+
				`{"step_index":1,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","error":"old err","created_at":%q}`+"\n",
				t0.Format(time.RFC3339Nano), t0.Add(-time.Hour).Format(time.RFC3339Nano)),
			since:     t0,
			wantError: "",
		},
		{
			name:   "skips generic and extracts fallback content error",
			convID: "s-fallback-content-err",
			transcript: fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","created_at":%q}`+"\n"+
				`{"step_index":1,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"content err","created_at":%q}`+"\n"+
				`{"step_index":2,"source":"MODEL","type":"GENERIC","status":"ERROR","content":"skip generic","created_at":%q}`+"\n",
				t0.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)),
			since:     t0,
			wantError: "content err",
		},
		{
			name:   "raw json object error with intermediate user input",
			convID: "s-raw-obj-err",
			transcript: fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","created_at":%q}`+"\n"+
				`{"step_index":1,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"skip me","created_at":%q}`+"\n"+
				`{"step_index":2,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","error":{"code":500},"created_at":%q}`+"\n",
				t0.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)),
			since:     t0,
			wantError: `{"code":500}`,
		},
		{
			name:   "no error in turn",
			convID: "s-no-err",
			transcript: fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","created_at":%q}`+"\n"+
				`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"all clear","created_at":%q}`+"\n",
				t0.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)),
			since:     t0,
			wantError: "",
		},
		{
			name:       "large file seek with fallback read",
			convID:     "s-large-err",
			transcript: buildLargeTranscriptFile(t0, t1),
			since:      t0,
			wantError:  "large chunk fail",
		},
	}
}

func claudeExtractTurnCases(t0, t1 time.Time) []extractLastTurnErrorCase {
	return []extractLastTurnErrorCase{
		{
			name:   "claude transcript extracts error event after user input",
			convID: "s-claude-err-event",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Do work"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"error","timestamp":%q,"error":"claude connection reset"}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:     t0,
			wantError: "claude connection reset",
		},
		{
			name:   "claude transcript extracts error object message",
			convID: "s-claude-err-obj",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Check quota"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"error","timestamp":%q,"error":{"message":"rate limit exceeded"}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:     t0,
			wantError: "rate limit exceeded",
		},
		{
			name:   "claude transcript extracts error from tool_result is_error",
			convID: "s-claude-tool-err",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Run script"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":[{"type":"tool_result","is_error":true,"content":"exit status 1"}]}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:     t0,
			wantError: "exit status 1",
		},
		{
			name:   "claude transcript extracts assistant error with status error",
			convID: "s-claude-asst-status-err",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Start job"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"assistant","status":"error","timestamp":%q,"message":{"role":"assistant","content":[{"type":"text","text":"Internal assistant crash"}]}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:     t0,
			wantError: "Internal assistant crash",
		},
		{
			name:   "claude error before cutoff is ignored",
			convID: "s-claude-err-before-cutoff",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Query"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"error","timestamp":%q,"error":"old error"}`, t0.Add(-time.Hour).Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:     t0,
			wantError: "",
		},
		{
			name:   "claude top level raw json error code",
			convID: "s-claude-raw-json-code",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Run query"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"error","timestamp":%q,"error":{"code":500}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:     t0,
			wantError: `{"code":500}`,
		},
		{
			name:   "claude type error with message content",
			convID: "s-claude-msg-content-err",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Invoke tool"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"error","timestamp":%q,"message":{"content":"message content error"}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:     t0,
			wantError: "message content error",
		},
		{
			name:   "claude tool result with text block content",
			convID: "s-claude-tool-text-blocks",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Read log"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":[{"type":"tool_result","is_error":true,"content":[{"type":"text","text":"block text error"}]}]}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:     t0,
			wantError: "block text error",
		},
		{
			name:   "claude tool result with raw json object content",
			convID: "s-claude-tool-raw-obj",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Execute API"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":[{"type":"tool_result","is_error":true,"content":{"raw":"error"}}]}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:     t0,
			wantError: `{"raw":"error"}`,
		},
		{
			name:   "claude turn with no error",
			convID: "s-claude-no-err",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Run tests"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"assistant","timestamp":%q,"message":{"role":"assistant","content":[{"type":"text","text":"Tests passed"}]}}`, t1.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":[{"type":"tool_result","is_error":false,"content":"ok"}]}}`, t1.Add(time.Second).Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:     t0,
			wantError: "",
		},
	}
}

func extractLastTurnErrorCases(t0, t1 time.Time) []extractLastTurnErrorCase {
	return append(standardExtractTurnCases(t0, t1), claudeExtractTurnCases(t0, t1)...)
}

func buildLargeTranscriptFile(t0, t1 time.Time) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","created_at":%q}`+"\n", t0.Format(time.RFC3339Nano))
	padding := strings.Repeat("A", 1000)
	for i := 1; i <= 1050; i++ {
		fmt.Fprintf(&sb, `{"step_index":%d,"source":"MODEL","type":"PLANNER_RESPONSE","content":%q}`+"\n", i, padding)
	}
	fmt.Fprintf(&sb, `{"step_index":1051,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","error":"large chunk fail","created_at":%q}`+"\n", t1.Format(time.RFC3339Nano))
	return sb.String()
}

func runExtractLastTurnErrorCase(t *testing.T, tc extractLastTurnErrorCase) {
	t.Helper()
	tmp := t.TempDir()
	var mgr *Manager
	if !tc.isNilMgr {
		mgr = New(filepath.Join(tmp, "home"), filepath.Join(tmp, "data"))
		if tc.transcript != "" {
			fn := "transcript_full.jsonl"
			if tc.writeFilename != "" {
				fn = tc.writeFilename
			}
			writeTranscript(t, mgr, tc.convID, fn, tc.transcript)
		}
		if tc.claudeTranscript != "" {
			projDir := filepath.Join(tmp, "data", "claude", "projects", "-test-proj")
			_ = os.MkdirAll(projDir, 0o755)
			_ = os.WriteFile(filepath.Join(projDir, tc.convID+".jsonl"), []byte(tc.claudeTranscript), 0o644)
		}
	}
	ctx := context.Background()
	if tc.cancelCtx {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		cancel()
	}
	errStr, err := mgr.ExtractLastTurnError(ctx, tc.convID, tc.since)
	if (err != nil) != tc.wantErr {
		t.Fatalf("ExtractLastTurnError error=%v, wantErr=%v", err, tc.wantErr)
	}
	if errStr != tc.wantError {
		t.Fatalf("ExtractLastTurnError=%q, want %q", errStr, tc.wantError)
	}
}

func TestExtractLastTurnError(t *testing.T) {
	t0 := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)
	tests := extractLastTurnErrorCases(t0, t1)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runExtractLastTurnErrorCase(t, tc)
		})
	}
}

func TestExtractResponseSince(t *testing.T) {
	t0 := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)

	t.Run("IsStepBefore variants", func(t *testing.T) {
		now := time.Now()
		if isStepBefore("", now) {
			t.Fatalf("expected false for empty createdAt")
		}
		if isStepBefore("2026-10-19T12:00:00Z", time.Time{}) {
			t.Fatalf("expected false for zero since")
		}
		if !isStepBefore("2026-10-19T12:00:00.123456789Z", now) {
			t.Fatalf("expected true for RFC3339Nano")
		}
		if isStepBefore("not-a-valid-ts", now) {
			t.Fatalf("expected false for invalid timestamp")
		}
	})

	tests := []struct {
		name             string
		isNilMgr         bool
		convID           string
		transcript       string
		claudeTranscript string
		since            time.Time
		cancelCtx        bool
		useCtx           bool
		wantResp         string
	}{
		{name: "nil manager", isNilMgr: true, convID: "s1", since: t0},
		{name: "empty convID", convID: "", since: t0},
		{name: "path traversal", convID: "../bad", since: t0},
		{name: "canceled context", convID: "s1", since: t0, cancelCtx: true, useCtx: true},
		{name: "rescues response created after since", convID: "s-after", transcript: fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"Turn 1\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"substantive answer\",\"created_at\":%q}\ninvalid-json-line\n", t0.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)), since: t0, wantResp: "substantive answer"},
		{name: "rfc3339 without nanos format", convID: "s-rfc3339", transcript: fmt.Sprintf("{\"type\":\"PLANNER_RESPONSE\",\"content\":\"rfc3339 answer\",\"created_at\":%q}\n", t1.Format(time.RFC3339)), since: t0, wantResp: "rfc3339 answer"},
		{name: "response created before since", convID: "s-before", transcript: fmt.Sprintf("{\"type\":\"PLANNER_RESPONSE\",\"content\":\"stale answer\",\"created_at\":%q}\n", t0.Format(time.RFC3339Nano)), since: t1, wantResp: ""},
		{name: "invalid timestamp string", convID: "s-invalid-ts", transcript: "{\"type\":\"PLANNER_RESPONSE\",\"content\":\"bad ts answer\",\"created_at\":\"not-a-timestamp\"}\n", since: t0, wantResp: "bad ts answer"},
		{name: "skips tool call request and generic types and empty planner content", convID: "s-tool-generic", transcript: fmt.Sprintf("{\"type\":\"GENERIC\",\"content\":\"search results\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"[Tool Call Requested]: grep\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"   \",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"actual answer\",\"created_at\":%q}\n", t0.Format(time.RFC3339Nano), t0.Format(time.RFC3339Nano), t0.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)), since: t0, wantResp: "actual answer"},
		{name: "tail has tool call and empty content with older step before since", convID: "s-tail-empty", transcript: fmt.Sprintf("{\"type\":\"USER_INPUT\",\"created_at\":%q}\n{\"type\":\"GENERIC\",\"content\":\"skip me\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"[Tool Call Requested]: run\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"\",\"tool_calls\":[{\"name\":\"call\"}],\"created_at\":%q}\n", t0.Add(-time.Hour).Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)), since: t0, wantResp: ""},
		{
			name:   "claude transcript extracts latest substantive assistant response",
			convID: "s-claude-resp",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Run build"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"assistant","timestamp":%q,"message":{"role":"assistant","content":[{"type":"thinking","thinking":"Analyzing"},{"type":"text","text":"Build finished successfully"}]}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:    t0,
			wantResp: "Build finished successfully",
		},
		{
			name:   "claude transcript ignores tool_use only blocks",
			convID: "s-claude-tool-only",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Run read"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"assistant","timestamp":%q,"message":{"role":"assistant","content":[{"type":"tool_use","name":"Read","input":{}}]}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:    t0,
			wantResp: "",
		},
		{
			name:   "claude transcript response before since returns empty",
			convID: "s-claude-stale",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"assistant","timestamp":%q,"message":{"role":"assistant","content":[{"type":"text","text":"Stale message"}]}}`, t0.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:    t1,
			wantResp: "",
		},
		{
			name:   "claude transcript assistant with raw string content",
			convID: "s-claude-raw-str",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"assistant","timestamp":%q,"message":{"role":"assistant","content":"Raw string response"}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:    t0,
			wantResp: "Raw string response",
		},
		{
			name:             "claude transcript canceled context returns empty",
			convID:           "s-claude-canc",
			claudeTranscript: `{"type":"assistant","timestamp":"2026-10-19T12:01:00Z","message":{"role":"assistant","content":"reply"}}` + "\n",
			since:            t0,
			cancelCtx:        true,
			useCtx:           true,
			wantResp:         "",
		},
		{
			name:             "claude transcript empty file returns empty",
			convID:           "s-claude-empty-file",
			claudeTranscript: "",
			since:            t0,
			wantResp:         "",
		},
		{
			name:   "claude transcript tool call requested prefix ignored",
			convID: "s-claude-tool-req-prefix",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"assistant","timestamp":%q,"message":{"role":"assistant","content":"[Tool Call Requested]: Read"}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:    t0,
			wantResp: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			var mgr *Manager
			if !tc.isNilMgr {
				mgr = New(filepath.Join(tmp, "home"), filepath.Join(tmp, "data"))
				if tc.transcript != "" {
					writeTranscript(t, mgr, tc.convID, "transcript_full.jsonl", tc.transcript)
				}
				if tc.claudeTranscript != "" {
					projDir := filepath.Join(tmp, "data", "claude", "projects", "-test-proj")
					_ = os.MkdirAll(projDir, 0o755)
					_ = os.WriteFile(filepath.Join(projDir, tc.convID+".jsonl"), []byte(tc.claudeTranscript), 0o644)
				}
			}
			ctx := context.Background()
			if tc.cancelCtx {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			var got string
			if tc.useCtx {
				got = mgr.ExtractResponseSinceContext(ctx, tc.convID, tc.since)
			} else {
				got = mgr.ExtractResponseSince(tc.convID, tc.since)
			}
			if got != tc.wantResp {
				t.Fatalf("ExtractResponseSince=%q, want %q", got, tc.wantResp)
			}
		})
	}
}

type sessionStorageMetricsCase struct {
	name        string
	isNilMgr    bool
	sessionID   string
	dbSize      int
	pbSize      int
	transSize   int
	fullSize    int
	stepCount   int
	turnCount   int
	rtDir       bool
	statErrFile bool
	wantDBSize  int64
	wantTrSize  int64
	wantSteps   int
	wantTurns   int
	wantExists  bool
}

func setupSessionStorageMetrics(t *testing.T, mgr *Manager, tc sessionStorageMetricsCase, tmp string) {
	t.Helper()
	convDir := filepath.Join(tmp, "conversations")
	if tc.statErrFile {
		_ = os.WriteFile(convDir, []byte("file"), 0o644)
		sessDir := filepath.Join(tmp, "brain", tc.sessionID)
		_ = os.MkdirAll(sessDir, 0o755)
		_ = os.WriteFile(filepath.Join(sessDir, ".system_generated"), []byte("file"), 0o644)
	}
	if tc.rtDir {
		rtDir := filepath.Join(tmp, "runtimes", "discord", ".gemini", "antigravity-cli", "conversations")
		writeConversationFile(t, rtDir, tc.sessionID, ".db", 200)
	}
	if tc.dbSize > 0 || (tc.name == "0-byte files do not exist") {
		writeConversationFile(t, convDir, tc.sessionID, ".db", tc.dbSize)
	}
	if tc.pbSize > 0 || (tc.name == "0-byte files do not exist") {
		writeConversationFile(t, convDir, tc.sessionID, ".pb", tc.pbSize)
	}
	setupSessionStorageTranscript(t, mgr, tc)
}

func setupSessionStorageTranscript(t *testing.T, mgr *Manager, tc sessionStorageMetricsCase) {
	t.Helper()
	if tc.transSize > 0 || (tc.name == "0-byte files do not exist") {
		sessDir, _ := mgr.EnsureSessionDir(tc.sessionID)
		var content string
		if tc.stepCount > 0 {
			var sb strings.Builder
			for i := range tc.stepCount {
				typ := "PLANNER_RESPONSE"
				source := "MODEL"
				if i < tc.turnCount {
					typ = "USER_INPUT"
					source = "USER_EXPLICIT"
				}
				fmt.Fprintf(&sb, `{"step_index":%d,"source":%q,"type":%q,"content":"msg"}`+"\n", i, source, typ)
			}
			content = sb.String()
		} else if tc.transSize > 0 {
			content = strings.Repeat("x", tc.transSize)
		}
		_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), []byte(content), 0o644)
	}
	if tc.fullSize > 0 {
		sessDir, _ := mgr.EnsureSessionDir(tc.sessionID)
		_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript_full.jsonl"), []byte(strings.Repeat("y", tc.fullSize)), 0o644)
	}
}

func assertSessionStorageMetrics(t *testing.T, mgr *Manager, tc sessionStorageMetricsCase) {
	t.Helper()
	if got := mgr.SessionExistsOnDisk(tc.sessionID); got != tc.wantExists {
		t.Fatalf("SessionExistsOnDisk=%v, want %v", got, tc.wantExists)
	}
	if got := mgr.GetSessionDBSize(tc.sessionID); got != tc.wantDBSize {
		t.Fatalf("GetSessionDBSize=%d, want %d", got, tc.wantDBSize)
	}
	if got := mgr.GetTranscriptSize(tc.sessionID); got != tc.wantTrSize {
		t.Fatalf("GetTranscriptSize=%d, want %d", got, tc.wantTrSize)
	}
	if got := mgr.CountTranscriptSteps(tc.sessionID); got != tc.wantSteps {
		t.Fatalf("CountTranscriptSteps=%d, want %d", got, tc.wantSteps)
	}
	if got := mgr.CountTranscriptTurns(tc.sessionID); got != tc.wantTurns {
		t.Fatalf("CountTranscriptTurns=%d, want %d", got, tc.wantTurns)
	}
}

func runSessionStorageMetricsCase(t *testing.T, tc sessionStorageMetricsCase) {
	t.Helper()
	tmp := t.TempDir()
	var mgr *Manager
	if !tc.isNilMgr {
		mgr = New(tmp, tmp)
		setupSessionStorageMetrics(t, mgr, tc, tmp)
	}
	assertSessionStorageMetrics(t, mgr, tc)
}

func TestSessionStorageAndRotationMetrics(t *testing.T) {
	tests := []sessionStorageMetricsCase{
		{name: "nil manager", isNilMgr: true, sessionID: "s1", wantExists: false},
		{name: "empty session ID", sessionID: "", wantExists: false},
		{name: "path traversal", sessionID: "../bad", wantExists: false},
		{
			name:       "db and pb and transcript metrics",
			sessionID:  "s-metrics",
			dbSize:     500,
			pbSize:     300,
			transSize:  150,
			fullSize:   450,
			stepCount:  5,
			turnCount:  2,
			wantDBSize: 500,
			wantTrSize: 450,
			wantSteps:  5,
			wantTurns:  2,
			wantExists: true,
		},
		{
			name:       "pb only exists",
			sessionID:  "s-pb-only",
			pbSize:     350,
			wantDBSize: 350,
			wantExists: true,
		},
		{
			name:       "transcript only exists",
			sessionID:  "s-tr-only",
			transSize:  200,
			wantTrSize: 200,
			wantExists: true,
		},
		{
			name:       "runtime candidate directory match",
			sessionID:  "s-runtime",
			rtDir:      true,
			wantDBSize: 200,
			wantExists: true,
		},
		{
			name:       "0-byte files do not exist",
			sessionID:  "s-zero-byte",
			dbSize:     0,
			pbSize:     0,
			transSize:  0,
			wantExists: false,
		},
		{
			name:        "stat non-not-exist error when candidate dir is a file",
			sessionID:   "s-stat-err",
			statErrFile: true,
			wantExists:  false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runSessionStorageMetricsCase(t, tc)
		})
	}

	candidateTests := []struct {
		name     string
		isNilMgr bool
	}{
		{"nil manager candidate dirs", true},
	}
	for _, tc := range candidateTests {
		t.Run(tc.name, func(t *testing.T) {
			var mgr *Manager
			if len(mgr.conversationCandidateDirs()) != 0 {
				t.Fatalf("expected nil candidate dirs for nil manager")
			}
		})
	}

	if DefaultMinSessionTurns != 3 || DefaultEmergencyRotationMultiplier != 3 ||
		DefaultMaxSessionTurns != 8 || DefaultMaxSessionSteps != 180 ||
		DefaultMaxTranscriptBytes != 1024*1024 || DefaultMaxSessionTranscriptBytes != 2*1024*1024 ||
		DefaultMaxSessionDBBytes != 4*1024*1024 || DefaultMaxQuotaPauseDBBytes != 3*1024*1024 {
		t.Fatalf("unexpected session rotation constant values")
	}
}

func TestSanitizeClaudeProjectPath(t *testing.T) {
	tests := []struct {
		name string
		cwd  string
		want string
	}{
		{name: "root directory", cwd: "/", want: "-"},
		{name: "empty cwd", cwd: "", want: ""},
		{name: "standard unix path", cwd: "/data/runtimes/discord", want: "-data-runtimes-discord"},
		{name: "path with dots", cwd: "/share/aerial-config/..", want: "-share-aerial-config---"},
		{name: "relative path with dots", cwd: "foo.bar/baz.qux", want: "foo-bar-baz-qux"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := SanitizeClaudeProjectPath(tc.cwd); got != tc.want {
				t.Fatalf("SanitizeClaudeProjectPath(%q) = %q, want %q", tc.cwd, got, tc.want)
			}
		})
	}
}

type claudeSessionLifecycleCase struct {
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

func resolveClaudeWriteDir(location, homeDir, dataDir string) string {
	switch location {
	case "data_claude":
		return filepath.Join(dataDir, "claude", "projects", "-data-proj")
	case "data_runtime":
		return filepath.Join(dataDir, "runtimes", "discord", ".claude", "projects", "-runtime-proj")
	case "home_claude":
		return filepath.Join(homeDir, ".claude", "projects", "-home-proj")
	case "direct_claude":
		return filepath.Join(dataDir, "claude", "projects")
	default:
		return ""
	}
}

func setupClaudeSessionFile(tc claudeSessionLifecycleCase, homeDir, dataDir string, t0 time.Time) {
	writeDir := resolveClaudeWriteDir(tc.location, homeDir, dataDir)
	if writeDir == "" {
		return
	}
	_ = os.MkdirAll(writeDir, 0o755)
	filePath := filepath.Join(writeDir, tc.sessionID+".jsonl")
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

func assertClaudeSessionMetrics(t *testing.T, mgr *Manager, tc claudeSessionLifecycleCase, dataDir string) {
	t.Helper()
	paths := mgr.claudeTranscriptPaths(tc.sessionID)
	if len(paths) != tc.wantPaths {
		t.Errorf("claudeTranscriptPaths count=%d, want %d", len(paths), tc.wantPaths)
	}

	exists := mgr.ClaudeSessionExistsOnDisk(tc.sessionID)
	if exists != tc.wantExists {
		t.Errorf("ClaudeSessionExistsOnDisk=%v, want %v", exists, tc.wantExists)
	}

	sessExists := mgr.SessionExistsOnDisk(tc.sessionID)
	if sessExists != tc.wantExists {
		t.Errorf("SessionExistsOnDisk=%v, want %v", sessExists, tc.wantExists)
	}

	size := mgr.GetTranscriptSize(tc.sessionID)
	if tc.wantSize > 0 && size != tc.wantSize {
		t.Errorf("GetTranscriptSize=%d, want %d", size, tc.wantSize)
	} else if tc.content != "" && size == 0 {
		t.Errorf("GetTranscriptSize expected non-zero, got 0")
	}

	steps := mgr.CountTranscriptSteps(tc.sessionID)
	if steps != tc.wantSteps {
		t.Errorf("CountTranscriptSteps=%d, want %d", steps, tc.wantSteps)
	}

	turns := mgr.CountTranscriptTurns(tc.sessionID)
	if turns != tc.wantTurns {
		t.Errorf("CountTranscriptTurns=%d, want %d", turns, tc.wantTurns)
	}

	var lastAct time.Time
	var err error
	if tc.name == "custom roots override in GetSessionLastActivity" {
		lastAct, err = mgr.GetSessionLastActivity(tc.sessionID, filepath.Join(dataDir, "claude", "projects"))
	} else {
		lastAct, err = mgr.GetSessionLastActivity(tc.sessionID)
	}
	if err != nil {
		t.Fatalf("GetSessionLastActivity error: %v", err)
	}
	if tc.wantNonZeroTime && lastAct.IsZero() {
		t.Errorf("GetSessionLastActivity expected non-zero, got zero")
	} else if !tc.wantNonZeroTime && !lastAct.IsZero() {
		t.Errorf("GetSessionLastActivity expected zero, got %v", lastAct)
	}
}

func runClaudeSessionLifecycleCase(t *testing.T, tc claudeSessionLifecycleCase, t0 time.Time) {
	t.Helper()
	tmp := t.TempDir()
	var mgr *Manager
	var dataDir string
	if !tc.isNilMgr {
		homeDir := filepath.Join(tmp, "home")
		dataDir = filepath.Join(tmp, "data")
		mgr = New(homeDir, dataDir)
		setupClaudeSessionFile(tc, homeDir, dataDir, t0)
	}
	assertClaudeSessionMetrics(t, mgr, tc, dataDir)
}

func TestClaudeSessionLifecycle(t *testing.T) {
	t0 := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)

	tests := []claudeSessionLifecycleCase{
		{
			name:      "nil manager",
			isNilMgr:  true,
			sessionID: "sess-claude-nil",
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
			name:      "data claude projects standard session",
			sessionID: "sess-claude-data",
			location:  "data_claude",
			content: strings.Join([]string{
				`{"type":"user","timestamp":"2026-11-07T10:00:00Z","message":{"role":"user","content":"Initial prompt"}}`,
				`{"type":"assistant","timestamp":"2026-11-07T10:00:02Z","message":{"role":"assistant","content":[{"type":"text","text":"Done"}]}}`,
			}, "\n") + "\n",
			age:             10 * time.Minute,
			wantExists:      true,
			wantPaths:       1,
			wantSteps:       1,
			wantTurns:       1,
			wantNonZeroTime: true,
		},
		{
			name:      "data runtime claude projects session with tool results and ambient",
			sessionID: "sess-claude-runtime",
			location:  "data_runtime",
			content: strings.Join([]string{
				`{"type":"user","timestamp":"2026-11-07T10:00:00Z","message":{"role":"user","content":"Prompt 1"}}`,
				`{"type":"assistant","timestamp":"2026-11-07T10:00:02Z","message":{"role":"assistant","content":[{"type":"tool_use","name":"Read","input":{}}]}}`,
				`{"type":"user","timestamp":"2026-11-07T10:00:04Z","message":{"role":"user","content":[{"type":"tool_result","content":"file contents"}]}}`,
				`{"type":"user","timestamp":"2026-11-07T10:00:05Z","message":{"role":"user","content":"[Chat ambient message]"}}`,
				`{"type":"assistant","timestamp":"2026-11-07T10:00:06Z","message":{"role":"assistant","content":[{"type":"text","text":"Final answer"}]}}`,
				`{"type":"tool_result","timestamp":"2026-11-07T10:00:07Z","content":"extra tool"}`,
			}, "\n") + "\n",
			wantExists:      true,
			wantPaths:       1,
			wantSteps:       4,
			wantTurns:       1,
			wantNonZeroTime: true,
		},
		{
			name:      "home claude projects session",
			sessionID: "sess-claude-home",
			location:  "home_claude",
			content: strings.Join([]string{
				`{"type":"user","timestamp":"2026-11-07T10:00:00Z","message":{"role":"user","content":"Home prompt"}}`,
			}, "\n") + "\n",
			wantExists:      true,
			wantPaths:       1,
			wantSteps:       0,
			wantTurns:       1,
			wantNonZeroTime: true,
		},
		{
			name:      "direct in claude projects base directory",
			sessionID: "sess-claude-direct",
			location:  "direct_claude",
			content: strings.Join([]string{
				`{"type":"user","timestamp":"2026-11-07T10:00:00Z","message":{"role":"user","content":"Direct prompt"}}`,
			}, "\n") + "\n",
			wantExists:      true,
			wantPaths:       1,
			wantTurns:       1,
			wantNonZeroTime: true,
		},
		{
			name:       "zero byte file does not exist",
			sessionID:  "sess-claude-zero",
			location:   "data_claude",
			zeroByte:   true,
			wantExists: false,
			wantPaths:  1,
			wantSize:   0,
		},
		{
			name:            "custom roots override in GetSessionLastActivity",
			sessionID:       "sess-claude-custom-root",
			location:        "data_claude",
			content:         `{"type":"user","timestamp":"2026-11-07T10:00:00Z","message":{"role":"user","content":"Hello"}}` + "\n",
			wantExists:      true,
			wantPaths:       1,
			wantTurns:       1,
			wantNonZeroTime: true,
		},
		{
			name:      "claude transcript with empty lines and invalid lines",
			sessionID: "sess-claude-empty-lines",
			location:  "data_claude",
			content: strings.Join([]string{
				"",
				"invalid-json",
				`{"type":"user","timestamp":"2026-11-07T10:00:00Z","message":{"role":"user","content":"Valid prompt"}}`,
				"",
				`{"type":"assistant","timestamp":"2026-11-07T10:00:02Z","message":{"role":"assistant","content":[{"type":"text","text":"Valid reply"}]}}`,
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
			runClaudeSessionLifecycleCase(t, tc, t0)
		})
	}
}

func testClaudeToolResultContent(t *testing.T) {
	t.Helper()
	tests := []struct {
		name       string
		content    string
		wantResult bool
	}{
		{name: "empty content", content: "", wantResult: false},
		{name: "invalid json content", content: "not-json", wantResult: false},
		{name: "empty json array", content: "[]", wantResult: false},
		{name: "non-tool-result array", content: `[{"type":"text","text":"hello"}]`, wantResult: false},
		{name: "tool-result array", content: `[{"type":"tool_result","content":"output"}]`, wantResult: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := isClaudeToolResultContent(json.RawMessage(tc.content))
			if got != tc.wantResult {
				t.Fatalf("isClaudeToolResultContent(%q) = %v, want %v", tc.content, got, tc.wantResult)
			}
		})
	}
}

func testClaudeMissingFiles(t *testing.T) {
	t.Helper()
	missingFileTests := []struct {
		name      string
		filePath  string
		wantSteps int
		wantTurns int
		wantResp  string
	}{
		{name: "nonexistent file", filePath: "/nonexistent/path/file.jsonl", wantSteps: 0, wantTurns: 0, wantResp: ""},
	}
	for _, tc := range missingFileTests {
		t.Run(tc.name, func(t *testing.T) {
			if steps := countStepsInClaudeTranscript(tc.filePath); steps != tc.wantSteps {
				t.Fatalf("countStepsInClaudeTranscript = %d, want %d", steps, tc.wantSteps)
			}
			if turns := countTurnsInClaudeTranscript(tc.filePath); turns != tc.wantTurns {
				t.Fatalf("countTurnsInClaudeTranscript = %d, want %d", turns, tc.wantTurns)
			}
			if resp := extractResponseFromClaudeTranscriptFile(context.Background(), tc.filePath, time.Now()); resp != tc.wantResp {
				t.Fatalf("extractResponseFromClaudeTranscriptFile = %q, want %q", resp, tc.wantResp)
			}
		})
	}
}

func testClaudeTargetProjects(t *testing.T) {
	t.Helper()
	targetTests := []struct {
		name       string
		cwd        string
		createFile bool
		prePopSeen bool
		wantFound  bool
	}{
		{name: "empty cwd", cwd: "", createFile: true, prePopSeen: false, wantFound: false},
		{name: "missing file", cwd: "/missing/cwd", createFile: false, prePopSeen: false, wantFound: false},
		{name: "file found and not seen", cwd: "/found/cwd", createFile: true, prePopSeen: false, wantFound: true},
		{name: "file already seen", cwd: "/seen/cwd", createFile: true, prePopSeen: true, wantFound: false},
	}
	for _, tc := range targetTests {
		t.Run("checkClaudeTargetProject_"+tc.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			cleanID := "sess-target"
			cand := filepath.Join(tmpDir, SanitizeClaudeProjectPath(tc.cwd), cleanID+".jsonl")
			if tc.createFile && tc.cwd != "" {
				_ = os.MkdirAll(filepath.Dir(cand), 0o755)
				_ = os.WriteFile(cand, []byte("data\n"), 0o644)
			}
			seen := make(map[string]bool)
			if tc.prePopSeen {
				seen[filepath.Clean(cand)] = true
			}
			gotPath, ok := checkClaudeTargetProject(tmpDir, tc.cwd, cleanID, seen)
			if ok != tc.wantFound {
				t.Fatalf("checkClaudeTargetProject ok=%v, want %v", ok, tc.wantFound)
			}
			if tc.wantFound && gotPath != cand {
				t.Fatalf("checkClaudeTargetProject path=%q, want %q", gotPath, cand)
			}
		})
	}
}

func testClaudeAssistantAndErrors(t *testing.T) {
	t.Helper()
	assistantTests := []struct {
		name     string
		content  string
		wantText string
	}{
		{name: "invalid json object", content: `{"invalid":true}`, wantText: ""},
		{name: "string content", content: `"hello world"`, wantText: "hello world"},
		{name: "blocks content", content: `[{"type":"text","text":"block text"}]`, wantText: "block text"},
	}
	for _, tc := range assistantTests {
		t.Run("extractAssistantText_"+tc.name, func(t *testing.T) {
			got := extractAssistantText(json.RawMessage(tc.content))
			if got != tc.wantText {
				t.Fatalf("extractAssistantText = %q, want %q", got, tc.wantText)
			}
		})
	}

	errorLineTests := []struct {
		name      string
		line      string
		wantError string
	}{
		{name: "direct error type", line: `{"type":"error","message":{"role":"assistant","content":"direct error"}}`, wantError: "direct error"},
		{name: "status error text", line: `{"status":"error","message":{"role":"assistant","content":"status err"}}`, wantError: "status err"},
	}
	for _, tc := range errorLineTests {
		t.Run("extractClaudeErrorFromLine_"+tc.name, func(t *testing.T) {
			got, _ := extractClaudeErrorFromLine(tc.line, time.Time{})
			if got != tc.wantError {
				t.Fatalf("extractClaudeErrorFromLine = %q, want %q", got, tc.wantError)
			}
		})
	}
}

func TestClaudeInternalHelpers(t *testing.T) {
	testClaudeToolResultContent(t)
	testClaudeMissingFiles(t)
	testClaudeTargetProjects(t)
	testClaudeAssistantAndErrors(t)
}

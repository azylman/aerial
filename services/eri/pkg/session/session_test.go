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

func TestManager_Init(t *testing.T) {
	tmp := t.TempDir()
	m := New(filepath.Join(tmp, "home"), filepath.Join(tmp, "data"))
	if m == nil || m.homeDir == "" || m.dataDir == "" || len(m.Roots()) == 0 {
		t.Fatalf("unexpected Manager initialization: %+v", m)
	}
	var nilMgr *Manager
	if len(nilMgr.Roots()) != 0 {
		t.Fatalf("nil Manager should return nil roots")
	}
}

// writeEnsureSessionFixtures creates dirs and files relative to root.
func writeEnsureSessionFixtures(t *testing.T, root string, dirs []string, files map[string]string) {
	t.Helper()
	for _, rel := range dirs {
		if err := os.MkdirAll(filepath.Join(root, rel), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	for rel, content := range files {
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, rel)), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, rel), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestEnsureSessionDir(t *testing.T) {
	t.Parallel()
	const brain = "home/.gemini/antigravity-cli/brain"
	tests := []struct {
		name       string
		isNilMgr   bool
		emptyRoots bool
		sessionID  string
		preExist   bool
		mkdirs     []string          // created under the temp root before the call
		files      map[string]string // written under the temp root before the call
		wantErr    bool
	}{
		{name: "nil manager", isNilMgr: true, sessionID: "sess-1", wantErr: true},
		{name: "empty session ID", sessionID: "", wantErr: true},
		{name: "path traversal", sessionID: "../bad", wantErr: true},
		{name: "empty roots manager", emptyRoots: true, sessionID: "sess-no-roots", wantErr: true},
		{name: "valid new session", sessionID: "sess-valid"},
		{name: "preexisting session", sessionID: "sess-pre", preExist: true},
		{
			name: "transcript.jsonl is a directory fails OpenFile", sessionID: "sess-block",
			mkdirs: []string{brain + "/sess-block/.system_generated/logs/transcript.jsonl"}, wantErr: true,
		},
		{
			name: ".system_generated is a file fails MkdirAll", sessionID: "sess-mkdir-block",
			files: map[string]string{brain + "/sess-mkdir-block/.system_generated": "file"}, wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmp := t.TempDir()
			writeEnsureSessionFixtures(t, tmp, tc.mkdirs, tc.files)
			var mgr *Manager
			if tc.emptyRoots {
				mgr = &Manager{}
			} else if !tc.isNilMgr {
				mgr = New(filepath.Join(tmp, "home"), filepath.Join(tmp, "data"))
			}
			if tc.preExist {
				if _, err := mgr.EnsureSessionDir(tc.sessionID); err != nil {
					t.Fatalf("pre-create EnsureSessionDir: %v", err)
				}
			}
			dir, err := mgr.EnsureSessionDir(tc.sessionID)
			if (err != nil) != tc.wantErr {
				t.Fatalf("EnsureSessionDir error=%v, wantErr=%v", err, tc.wantErr)
			}
			if !tc.wantErr && dir == "" {
				t.Fatalf("expected non-empty dir")
			}
		})
	}
}

func TestResolveBaseDir(t *testing.T) {
	t.Parallel()
	const brain = "home/.gemini/antigravity-cli/brain"
	tests := []struct {
		name      string
		isNilMgr  bool
		home      string // relative to the temp root
		data      string // relative to the temp root; "" = no data dir
		mkdirs    []string
		sessionID string
		want      string // relative to the temp root; "" = empty result
	}{
		{name: "nil manager", isNilMgr: true, sessionID: "s1"},
		{
			name: "no existing root falls back to the first root", home: "nonexistent-root-1", data: "nonexistent-root-2", sessionID: "s1",
			want: "nonexistent-root-2/brain",
		},
		{
			name: "existing session dir returns its parent root", home: "home", mkdirs: []string{brain + "/sess-pre"}, sessionID: "sess-pre",
			want: brain,
		},
		{name: "first existing root wins", home: "home", mkdirs: []string{brain}, sessionID: "s1", want: brain},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmp := t.TempDir()
			writeEnsureSessionFixtures(t, tmp, tc.mkdirs, nil)
			var mgr *Manager
			if !tc.isNilMgr {
				data := ""
				if tc.data != "" {
					data = filepath.Join(tmp, tc.data)
				}
				mgr = New(filepath.Join(tmp, tc.home), data)
			}
			want := ""
			if tc.want != "" {
				want = filepath.Join(tmp, tc.want)
			}
			if got := mgr.resolveBaseDir(tc.sessionID); got != want {
				t.Fatalf("resolveBaseDir(%q) = %q, want %q", tc.sessionID, got, want)
			}
		})
	}
}

func TestGetTargetDirs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		isNilMgr     bool
		convID       string
		wantNonEmpty bool
	}{
		{"conversation id lists per-root dirs", false, "test-conv", true},
		{"empty conversation id finds the latest dir", false, "", true},
		{"nil manager", true, "test", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmp := t.TempDir()
			if err := os.MkdirAll(filepath.Join(tmp, "home", ".gemini", "antigravity-cli", "brain", "session-latest"), 0o755); err != nil {
				t.Fatal(err)
			}
			var mgr *Manager
			if !tc.isNilMgr {
				mgr = New(filepath.Join(tmp, "home"), filepath.Join(tmp, "data"))
			}
			if got := mgr.getTargetDirs(tc.convID); (len(got) > 0) != tc.wantNonEmpty {
				t.Fatalf("getTargetDirs(%q) = %v, want non-empty %v", tc.convID, got, tc.wantNonEmpty)
			}
		})
	}
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
	museTranscript   string
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
	}
}

func claudeExtractTurnCasesExtra(t0, t1 time.Time) []extractLastTurnErrorCase {
	return []extractLastTurnErrorCase{
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
		{
			name:   "claude transcript extracts synthetic rate limit reply",
			convID: "s-claude-synth-rate-limit",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Check status"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"assistant","timestamp":%q,"isApiErrorMessage":true,"error":"rate_limit","message":{"model":"<synthetic>","role":"assistant","content":[{"type":"text","text":"You've hit your 5-hour limit · resets 12:10pm (America/Los_Angeles)"}]}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:     t0,
			wantError: "claude synthetic reply (error=rate_limit): You've hit your 5-hour limit · resets 12:10pm (America/Los_Angeles)",
		},
		{
			name:   "claude transcript extracts top level error with content text",
			convID: "s-claude-err-with-content",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Run query"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"assistant","timestamp":%q,"error":"api_req_failed","message":{"model":"claude-3-5-sonnet","role":"assistant","content":"Upstream cloudflare 502"}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:     t0,
			wantError: "api_req_failed: Upstream cloudflare 502",
		},
		{
			name:   "claude transcript extracts top level error when content contains it",
			convID: "s-claude-err-contained",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Run query"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"assistant","timestamp":%q,"error":"overloaded","message":{"model":"claude-3-5-sonnet","role":"assistant","content":"The server is overloaded right now"}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:     t0,
			wantError: "overloaded",
		},
		{
			name:   "claude transcript extracts synthetic error without content",
			convID: "s-claude-synth-empty-content",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Run query"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"assistant","timestamp":%q,"error":"rate_limit","message":{"model":"<synthetic>","role":"assistant","content":""}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:     t0,
			wantError: "rate_limit",
		},
	}
}

func museExtractTurnCases(t0, t1 time.Time) []extractLastTurnErrorCase {
	return []extractLastTurnErrorCase{
		{
			name:   "muse transcript extracts terminal error",
			convID: "s-muse-err",
			museTranscript: strings.Join([]string{
				fmt.Sprintf(`{"payload_type":"runtime.session","recorded_at":%d,"payload":{"kind":"run","event":{"kind":"started","prompt":"Run tool"}}}`, t0.UnixMicro()),
				fmt.Sprintf(`{"payload_type":"runtime.session","recorded_at":%d,"payload":{"kind":"run","event":{"kind":"terminal","terminal":"failed","reason":"Connection refused"}}}`, t1.UnixMicro()),
			}, "\n") + "\n",
			since:     t0,
			wantError: "Connection refused",
		},
	}
}

func extractLastTurnErrorCases(t0, t1 time.Time) []extractLastTurnErrorCase {
	cases := append(standardExtractTurnCases(t0, t1), claudeExtractTurnCases(t0, t1)...)
	cases = append(cases, claudeExtractTurnCasesExtra(t0, t1)...)
	return append(cases, museExtractTurnCases(t0, t1)...)
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

func setupExtractErrorFixtures(t *testing.T, mgr *Manager, tc extractLastTurnErrorCase, tmp string) {
	t.Helper()
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
	if tc.museTranscript != "" {
		museDir := filepath.Join(tmp, "data", "muse", "sessions")
		_ = os.MkdirAll(museDir, 0o755)
		_ = os.WriteFile(filepath.Join(museDir, tc.convID+".jsonl"), []byte(tc.museTranscript), 0o644)
	}
}

func runExtractLastTurnErrorCase(t *testing.T, tc extractLastTurnErrorCase) {
	t.Helper()
	tmp := t.TempDir()
	var mgr *Manager
	if !tc.isNilMgr {
		mgr = New(filepath.Join(tmp, "home"), filepath.Join(tmp, "data"))
		setupExtractErrorFixtures(t, mgr, tc, tmp)
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

type extractResponseSinceCase struct {
	name             string
	isNilMgr         bool
	convID           string
	transcript       string
	claudeTranscript string
	museTranscript   string
	codexTranscript  string
	since            time.Time
	cancelCtx        bool
	wantResp         string
}

func setupExtractResponseFixtures(t *testing.T, mgr *Manager, tc extractResponseSinceCase, tmp string) {
	t.Helper()
	if tc.transcript != "" {
		writeTranscript(t, mgr, tc.convID, "transcript_full.jsonl", tc.transcript)
	}
	if tc.claudeTranscript != "" {
		projDir := filepath.Join(tmp, "data", "claude", "projects", "-test-proj")
		_ = os.MkdirAll(projDir, 0o755)
		_ = os.WriteFile(filepath.Join(projDir, tc.convID+".jsonl"), []byte(tc.claudeTranscript), 0o644)
	}
	if tc.museTranscript != "" {
		museDir := filepath.Join(tmp, "data", "muse", "sessions")
		_ = os.MkdirAll(museDir, 0o755)
		_ = os.WriteFile(filepath.Join(museDir, tc.convID+".jsonl"), []byte(tc.museTranscript), 0o644)
	}
	if tc.codexTranscript != "" {
		codexDir := filepath.Join(tmp, "data", "codex", "sessions", "2026", "10", "19")
		_ = os.MkdirAll(codexDir, 0o755)
		_ = os.WriteFile(filepath.Join(codexDir, "rollout-"+tc.convID+".jsonl"), []byte(tc.codexTranscript), 0o644)
	}
}

func runExtractResponseSinceCase(t *testing.T, tc extractResponseSinceCase) {
	t.Helper()
	tmp := t.TempDir()
	var mgr *Manager
	if !tc.isNilMgr {
		mgr = New(filepath.Join(tmp, "home"), filepath.Join(tmp, "data"))
		setupExtractResponseFixtures(t, mgr, tc, tmp)
	}
	ctx := context.Background()
	if tc.cancelCtx {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(ctx)
		cancel()
	}
	if got := mgr.ExtractResponseSinceContext(ctx, tc.convID, tc.since); got != tc.wantResp {
		t.Fatalf("ExtractResponseSinceContext=%q, want %q", got, tc.wantResp)
	}
}

func TestIsStepBefore(t *testing.T) {
	t.Parallel()
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
}

func TestExtractResponseSince(t *testing.T) {
	t0 := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)

	tests := []extractResponseSinceCase{
		{name: "nil manager", isNilMgr: true, convID: "s1", since: t0},
		{name: "empty convID", convID: "", since: t0},
		{name: "path traversal", convID: "../bad", since: t0},
		{name: "canceled context", convID: "s1", since: t0, cancelCtx: true},
		{name: "rescues response created after since", convID: "s-after", transcript: fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"Turn 1\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"substantive answer\",\"created_at\":%q}\ninvalid-json-line\n", t0.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)), since: t0, wantResp: "substantive answer"},
		{name: "rfc3339 without nanos format", convID: "s-rfc3339", transcript: fmt.Sprintf("{\"type\":\"PLANNER_RESPONSE\",\"content\":\"rfc3339 answer\",\"created_at\":%q}\n", t1.Format(time.RFC3339)), since: t0, wantResp: "rfc3339 answer"},
		{name: "response created before since", convID: "s-before", transcript: fmt.Sprintf("{\"type\":\"PLANNER_RESPONSE\",\"content\":\"stale answer\",\"created_at\":%q}\n", t0.Format(time.RFC3339Nano)), since: t1, wantResp: ""},
		{name: "invalid timestamp string", convID: "s-invalid-ts", transcript: "{\"type\":\"PLANNER_RESPONSE\",\"content\":\"bad ts answer\",\"created_at\":\"not-a-timestamp\"}\n", since: t0, wantResp: "bad ts answer"},
		{name: "skips tool call request and generic types and empty planner content", convID: "s-tool-generic", transcript: fmt.Sprintf("{\"type\":\"GENERIC\",\"content\":\"search results\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"[Tool Call Requested]: grep\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"   \",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"actual answer\",\"created_at\":%q}\n", t0.Format(time.RFC3339Nano), t0.Format(time.RFC3339Nano), t0.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)), since: t0, wantResp: "actual answer"},
		{name: "tail has tool call and empty content with older step before since", convID: "s-tail-empty", transcript: fmt.Sprintf("{\"type\":\"USER_INPUT\",\"created_at\":%q}\n{\"type\":\"GENERIC\",\"content\":\"skip me\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"[Tool Call Requested]: run\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"\",\"tool_calls\":[{\"name\":\"call\"}],\"created_at\":%q}\n", t0.Add(-time.Hour).Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)), since: t0, wantResp: ""},
		{name: "skips agy background abort notice and does not rescue pre-tool chatter", convID: "s-agy-abort-pre-tool", transcript: fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"run task\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"Starting task now...\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"Turn is aborted because model did not make any tool calls or return any response text\",\"created_at\":%q}\n", t0.Format(time.RFC3339Nano), t0.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)), since: t0, wantResp: ""},
		{name: "rescues valid response written after agy background abort notice", convID: "s-agy-abort-then-valid", transcript: fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"run task\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"Starting task now...\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"Turn is aborted because model did not make any tool calls or return any response text\",\"created_at\":%q}\n{\"type\":\"SYSTEM_MESSAGE\",\"content\":\"Task completed\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"Task completed successfully\",\"created_at\":%q}\n", t0.Format(time.RFC3339Nano), t0.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano), t1.Add(time.Second).Format(time.RFC3339Nano), t1.Add(2*time.Second).Format(time.RFC3339Nano)), since: t0, wantResp: "Task completed successfully"},
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
		{name: "claude transcript synthetic placeholder only returns empty", convID: "s-claude-synthetic-placeholder", claudeTranscript: fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Hi"}}`, t0.Format(time.RFC3339Nano)) + "\n" + fmt.Sprintf(`{"type":"assistant","timestamp":%q,"isApiErrorMessage":false,"error":null,"message":{"model":"<synthetic>","role":"assistant","stop_reason":"stop_sequence","usage":{"output_tokens":0},"content":[{"type":"text","text":"No response requested."}]}}`, t1.Format(time.RFC3339Nano)) + "\n", since: t0, wantResp: ""},
		{name: "claude transcript synthetic rate limit only returns empty", convID: "s-claude-synthetic-rate-limit", claudeTranscript: fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Hi"}}`, t0.Format(time.RFC3339Nano)) + "\n" + fmt.Sprintf(`{"type":"assistant","timestamp":%q,"isApiErrorMessage":true,"error":"rate_limit","message":{"model":"<synthetic>","role":"assistant","stop_reason":"stop_sequence","usage":{"output_tokens":0},"content":[{"type":"text","text":"You've hit your 5-hour limit · resets 12:10pm (America/Los_Angeles)"}]}}`, t1.Format(time.RFC3339Nano)) + "\n", since: t0, wantResp: ""},
		{
			name:   "claude transcript intermediate assistant text before synthetic rate limit returns empty",
			convID: "s-claude-intermediate-then-rate-limit",
			claudeTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Hi"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"assistant","timestamp":%q,"message":{"model":"claude-3-5-sonnet","role":"assistant","content":[{"type":"text","text":"Working on step 1..."}]}}`, t1.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"assistant","timestamp":%q,"isApiErrorMessage":true,"error":"rate_limit","message":{"model":"<synthetic>","role":"assistant","stop_reason":"stop_sequence","usage":{"output_tokens":0},"content":[{"type":"text","text":"You've hit your 5-hour limit · resets 12:10pm (America/Los_Angeles)"}]}}`, t1.Add(time.Second).Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:    t0,
			wantResp: "",
		},
		{name: "claude transcript real reply before synthetic placeholder returns real reply", convID: "s-claude-real-then-synthetic", claudeTranscript: fmt.Sprintf(`{"type":"user","timestamp":%q,"message":{"role":"user","content":"Hi"}}`, t0.Format(time.RFC3339Nano)) + "\n" + fmt.Sprintf(`{"type":"assistant","timestamp":%q,"message":{"model":"claude-opus-5-5","role":"assistant","content":[{"type":"text","text":"Deploy is green"}]}}`, t1.Format(time.RFC3339Nano)) + "\n" + fmt.Sprintf(`{"type":"assistant","timestamp":%q,"isApiErrorMessage":false,"error":null,"message":{"model":"<synthetic>","role":"assistant","stop_reason":"stop_sequence","usage":{"output_tokens":0},"content":[{"type":"text","text":"No response requested."}]}}`, t1.Add(time.Second).Format(time.RFC3339Nano)) + "\n", since: t0, wantResp: "Deploy is green"},
		{
			name:             "claude transcript canceled context returns empty",
			convID:           "s-claude-canc",
			claudeTranscript: `{"type":"assistant","timestamp":"2026-10-19T12:01:00Z","message":{"role":"assistant","content":"reply"}}` + "\n",
			since:            t0,
			cancelCtx:        true,
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
		{
			name:   "muse transcript extracts latest assistant response",
			convID: "s-muse-resp",
			museTranscript: strings.Join([]string{
				fmt.Sprintf(`{"payload_type":"runtime.session","recorded_at":%d,"payload":{"kind":"run","event":{"kind":"started","prompt":"Run build"}}}`, t0.UnixMicro()),
				fmt.Sprintf(`{"payload_type":"runtime.session","recorded_at":%d,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"Build finished successfully"}}}`, t1.UnixMicro()),
			}, "\n") + "\n",
			since:    t0,
			wantResp: "Build finished successfully",
		},
		{
			name:   "muse transcript stale response before since returns empty",
			convID: "s-muse-stale",
			museTranscript: strings.Join([]string{
				fmt.Sprintf(`{"payload_type":"runtime.session","recorded_at":%d,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"Stale"}}}`, t0.UnixMicro()),
			}, "\n") + "\n",
			since:    t1,
			wantResp: "",
		},
		{
			name:   "codex transcript extracts latest assistant response",
			convID: "s-codex-resp",
			codexTranscript: strings.Join([]string{
				fmt.Sprintf(`{"type":"session_meta","timestamp":%q,"payload":{"id":"s-codex-resp"}}`, t0.Format(time.RFC3339Nano)),
				fmt.Sprintf(`{"type":"response_item","timestamp":%q,"payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Codex finished"}]}}`, t1.Format(time.RFC3339Nano)),
			}, "\n") + "\n",
			since:    t0,
			wantResp: "Codex finished",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runExtractResponseSinceCase(t, tc)
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
	// zeroByteFiles writes empty .db, .pb and transcript files.
	zeroByteFiles bool
	wantDBSize    int64
	wantTrSize    int64
	wantSteps     int
	wantTurns     int
	wantExists    bool
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
	if tc.dbSize > 0 || tc.zeroByteFiles {
		writeConversationFile(t, convDir, tc.sessionID, ".db", tc.dbSize)
	}
	if tc.pbSize > 0 || tc.zeroByteFiles {
		writeConversationFile(t, convDir, tc.sessionID, ".pb", tc.pbSize)
	}
	setupSessionStorageTranscript(t, mgr, tc)
}

func setupSessionStorageTranscript(t *testing.T, mgr *Manager, tc sessionStorageMetricsCase) {
	t.Helper()
	if tc.transSize > 0 || tc.zeroByteFiles {
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
		{name: "pb only exists", sessionID: "s-pb-only", pbSize: 350, wantDBSize: 350, wantExists: true},
		{name: "transcript only exists", sessionID: "s-tr-only", transSize: 200, wantTrSize: 200, wantExists: true},
		{name: "runtime candidate directory match", sessionID: "s-runtime", rtDir: true, wantDBSize: 200, wantExists: true},
		{name: "0-byte files do not exist", sessionID: "s-zero-byte", zeroByteFiles: true, wantExists: false},
		{name: "stat non-not-exist error when candidate dir is a file", sessionID: "s-stat-err", statErrFile: true, wantExists: false},
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

type sessionLifecycleCase struct {
	name       string
	engine     string
	isNilMgr   bool
	sessionID  string
	location   string
	content    string
	zeroByte   bool
	age        time.Duration
	wantExists bool
	wantPaths  int
	wantSteps  int
	wantTurns  int
}

// sessionEngineProbe holds the engine-specific transcript lookups exercised by TestSessionLifecycle.
type sessionEngineProbe struct {
	paths  func(*Manager, string) []string
	exists func(*Manager, string) bool
}

var sessionEngineProbes = map[string]sessionEngineProbe{
	"claude": {(*Manager).claudeTranscriptPaths, (*Manager).ClaudeSessionExistsOnDisk},
	"codex":  {(*Manager).codexTranscriptPaths, (*Manager).CodexSessionExistsOnDisk},
	"muse":   {(*Manager).museTranscriptPaths, (*Manager).MuseSessionExistsOnDisk},
}

// sessionLayout returns the transcript path an engine writes for a session at a storage location ("" = none).
func sessionLayout(engine, location, homeDir, dataDir, sessionID string) string {
	switch engine + "/" + location {
	case "claude/data":
		return filepath.Join(dataDir, "claude", "projects", "-data-proj", sessionID+".jsonl")
	case "claude/runtime":
		return filepath.Join(dataDir, "runtimes", "discord", ".claude", "projects", "-runtime-proj", sessionID+".jsonl")
	case "claude/home":
		return filepath.Join(homeDir, ".claude", "projects", "-home-proj", sessionID+".jsonl")
	case "claude/direct":
		return filepath.Join(dataDir, "claude", "projects", sessionID+".jsonl")
	case "codex/data":
		return filepath.Join(dataDir, "codex", "sessions", "2026", "11", "15", "rollout-test-"+sessionID+".jsonl")
	case "codex/runtime":
		return filepath.Join(dataDir, "runtimes", "discord", ".codex", "sessions", sessionID+".jsonl")
	case "codex/home":
		return filepath.Join(homeDir, ".codex", "sessions", "rollout-"+sessionID+".jsonl")
	case "codex/direct":
		return filepath.Join(dataDir, "codex", "sessions", sessionID+".jsonl")
	case "codex/direct_dated":
		return filepath.Join(dataDir, "codex", "sessions", "rollout-2026-11-07T10-00-00-"+sessionID+".jsonl")
	case "muse/data":
		return filepath.Join(dataDir, "muse", "sessions", "2026", "11", "15", sessionID, "session.jsonl")
	case "muse/runtime":
		return filepath.Join(dataDir, "runtimes", "discord", ".local", "share", "muse", "sessions", sessionID+".jsonl")
	case "muse/home":
		return filepath.Join(homeDir, ".local", "share", "muse", "sessions", sessionID, "session.jsonl")
	case "muse/direct":
		return filepath.Join(dataDir, "muse", "sessions", sessionID+".jsonl")
	default:
		return ""
	}
}

func writeLifecycleSessionFile(t *testing.T, tc sessionLifecycleCase, homeDir, dataDir string, t0 time.Time) {
	t.Helper()
	filePath := sessionLayout(tc.engine, tc.location, homeDir, dataDir, tc.sessionID)
	if filePath == "" || (!tc.zeroByte && tc.content == "") {
		return
	}
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filePath, []byte(tc.content), 0o644); err != nil {
		t.Fatal(err)
	}
	if tc.zeroByte {
		return
	}
	mtime := t0
	if tc.age > 0 {
		mtime = time.Now().Add(-tc.age)
	}
	if err := os.Chtimes(filePath, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

// jsonl joins transcript lines with a trailing newline.
func jsonl(lines ...string) string { return strings.Join(lines, "\n") + "\n" }

func lifecycleClaudeUser(ts, text string) string {
	return `{"type":"user","timestamp":"2026-11-07T10:00:` + ts + `Z","message":{"role":"user","content":"` + text + `"}}`
}

func lifecycleClaudeText(ts, text string) string {
	return `{"type":"assistant","timestamp":"2026-11-07T10:00:` + ts + `Z","message":{"role":"assistant","content":[{"type":"text","text":"` + text + `"}]}}`
}

func lifecycleCodexMsg(ts, role, text string) string {
	return `{"type":"response_item","timestamp":"2026-11-07T10:00:` + ts +
		`Z","payload":{"type":"message","role":"` + role + `","content":[{"type":"text","text":"` + text + `"}]}}`
}

func lifecycleMuseRun(sec, event string) string {
	return `{"payload_type":"runtime.session","recorded_at":176251000` + sec + `000000,"payload":{"kind":"run","event":` + event + `}}`
}

func TestSessionLifecycle(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)

	tests := []sessionLifecycleCase{
		{name: "claude nil manager", engine: "claude", isNilMgr: true, sessionID: "sess-claude-nil"},
		{name: "claude empty session ID", engine: "claude"},
		{name: "claude path traversal session ID", engine: "claude", sessionID: "../bad-id"},
		{
			name: "claude data projects standard session", engine: "claude", sessionID: "sess-claude-data", location: "data",
			content: jsonl(lifecycleClaudeUser("00", "Initial prompt"), lifecycleClaudeText("02", "Done")),
			age:     10 * time.Minute, wantExists: true, wantPaths: 1, wantSteps: 1, wantTurns: 1,
		},
		{
			name: "claude data runtime session with tool results and ambient", engine: "claude", sessionID: "sess-claude-runtime", location: "runtime",
			content: jsonl(
				lifecycleClaudeUser("00", "Prompt 1"),
				`{"type":"assistant","timestamp":"2026-11-07T10:00:02Z","message":{"role":"assistant","content":[{"type":"tool_use","name":"Read","input":{}}]}}`,
				`{"type":"user","timestamp":"2026-11-07T10:00:04Z","message":{"role":"user","content":[{"type":"tool_result","content":"file contents"}]}}`,
				lifecycleClaudeUser("05", "[Chat ambient message]"),
				lifecycleClaudeText("06", "Final answer"),
				`{"type":"tool_result","timestamp":"2026-11-07T10:00:07Z","content":"extra tool"}`,
			),
			wantExists: true, wantPaths: 1, wantSteps: 4, wantTurns: 1,
		},
		{
			name: "claude home projects session", engine: "claude", sessionID: "sess-claude-home", location: "home",
			content: jsonl(lifecycleClaudeUser("00", "Home prompt")), wantExists: true, wantPaths: 1, wantTurns: 1,
		},
		{
			name: "claude direct in projects base directory", engine: "claude", sessionID: "sess-claude-direct", location: "direct",
			content: jsonl(lifecycleClaudeUser("00", "Direct prompt")), wantExists: true, wantPaths: 1, wantTurns: 1,
		},
		{
			name: "claude zero byte file does not exist", engine: "claude", sessionID: "sess-claude-zero", location: "data", zeroByte: true,
			wantPaths: 1,
		},
		{
			name: "claude transcript with empty lines and invalid lines", engine: "claude", sessionID: "sess-claude-empty-lines", location: "data",
			content:    jsonl("", "invalid-json", lifecycleClaudeUser("00", "Valid prompt"), "", lifecycleClaudeText("02", "Valid reply"), ""),
			wantExists: true, wantPaths: 1, wantSteps: 1, wantTurns: 1,
		},
		{name: "codex nil manager", engine: "codex", isNilMgr: true, sessionID: "sess-codex-nil"},
		{name: "codex empty session ID", engine: "codex"},
		{name: "codex path traversal session ID", engine: "codex", sessionID: "../bad-id"},
		{
			name: "codex direct timestamped rollout session", engine: "codex", sessionID: "sess-codex-dated", location: "direct_dated",
			content:    jsonl(lifecycleCodexMsg("00", "user", "Initial prompt"), lifecycleCodexMsg("02", "assistant", "Done")),
			wantExists: true, wantPaths: 1, wantSteps: 1, wantTurns: 1,
		},
		{
			name: "codex data nested rollout session", engine: "codex", sessionID: "sess-codex-data", location: "data",
			content: jsonl(lifecycleCodexMsg("00", "user", "Initial prompt"), lifecycleCodexMsg("02", "assistant", "Done")),
			age:     10 * time.Minute, wantExists: true, wantPaths: 1, wantSteps: 1, wantTurns: 1,
		},
		{
			name: "codex data runtime session with tool calls and ambient", engine: "codex", sessionID: "sess-codex-runtime", location: "runtime",
			content: jsonl(
				lifecycleCodexMsg("00", "user", "Prompt 1"),
				`{"type":"response_item","timestamp":"2026-11-07T10:00:02Z","payload":{"type":"function_call","name":"Read","arguments":"{}"}}`,
				`{"type":"response_item","timestamp":"2026-11-07T10:00:04Z","payload":{"type":"function_call_output","output":"file contents"}}`,
				lifecycleCodexMsg("05", "user", "[Chat ambient message]"),
				lifecycleCodexMsg("06", "assistant", "Final answer"),
			),
			wantExists: true, wantPaths: 1, wantSteps: 3, wantTurns: 1,
		},
		{
			name: "codex home rollout session", engine: "codex", sessionID: "sess-codex-home", location: "home",
			content: jsonl(lifecycleCodexMsg("00", "user", "Home prompt")), wantExists: true, wantPaths: 1, wantTurns: 1,
		},
		{
			name: "codex direct in base directory", engine: "codex", sessionID: "sess-codex-direct", location: "direct",
			content: jsonl(lifecycleCodexMsg("00", "user", "Direct prompt")), wantExists: true, wantPaths: 1, wantTurns: 1,
		},
		{name: "codex zero byte file does not exist", engine: "codex", sessionID: "sess-codex-zero", location: "data", zeroByte: true, wantPaths: 1},
		{
			name: "codex transcript with empty lines and invalid lines", engine: "codex", sessionID: "sess-codex-empty-lines", location: "data",
			content:    jsonl("", "invalid-json", lifecycleCodexMsg("00", "user", "Valid prompt"), "", lifecycleCodexMsg("02", "assistant", "Valid reply"), ""),
			wantExists: true, wantPaths: 1, wantSteps: 1, wantTurns: 1,
		},
		{name: "muse nil manager", engine: "muse", isNilMgr: true, sessionID: "sess-muse-nil"},
		{name: "muse empty session ID", engine: "muse"},
		{name: "muse path traversal session ID", engine: "muse", sessionID: "../bad-id"},
		{
			name: "muse data nested session.jsonl", engine: "muse", sessionID: "sess-muse-data", location: "data",
			content: jsonl(
				lifecycleMuseRun("0", `{"kind":"started","prompt":"Initial prompt"}`),
				lifecycleMuseRun("2", `{"kind":"assistant_message_committed","text":"Done"}`),
			),
			age: 10 * time.Minute, wantExists: true, wantPaths: 1, wantSteps: 1, wantTurns: 1,
		},
		{
			name: "muse data runtime discord session.jsonl direct", engine: "muse", sessionID: "sess-muse-runtime", location: "runtime",
			content: jsonl(
				lifecycleMuseRun("0", `{"kind":"started","prompt":"User msg"}`),
				lifecycleMuseRun("1", `{"kind":"assistant_message_committed","text":"Assistant msg"}`),
				lifecycleMuseRun("2", `{"kind":"assistant_tool_calls_committed","tool_calls":[{"name":"bash","call_id":"call_1"}]}`),
				lifecycleMuseRun("3", `{"kind":"tool_result_batch_committed","results":[{"call_id":"call_1","output":"ok"}]}`),
			),
			wantExists: true, wantPaths: 1, wantSteps: 3, wantTurns: 1,
		},
		{
			name: "muse home session.jsonl", engine: "muse", sessionID: "sess-muse-home", location: "home",
			content: jsonl(
				lifecycleMuseRun("0", `{"kind":"started","prompt":"[Chat alpha]: ambient message"}`),
				lifecycleMuseRun("1", `{"kind":"assistant_message_committed","text":"model output"}`),
			),
			wantExists: true, wantPaths: 1, wantSteps: 1,
		},
		{name: "muse zero byte session", engine: "muse", sessionID: "sess-muse-zero", location: "direct", zeroByte: true, wantPaths: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			probe := sessionEngineProbes[tc.engine]
			var mgr *Manager
			if !tc.isNilMgr {
				tmp := t.TempDir()
				homeDir, dataDir := filepath.Join(tmp, "home"), filepath.Join(tmp, "data")
				mgr = New(homeDir, dataDir)
				writeLifecycleSessionFile(t, tc, homeDir, dataDir, t0)
			}
			if got := len(probe.paths(mgr, tc.sessionID)); got != tc.wantPaths {
				t.Errorf("%s transcript paths count = %d, want %d", tc.engine, got, tc.wantPaths)
			}
			if got := probe.exists(mgr, tc.sessionID); got != tc.wantExists {
				t.Errorf("%s SessionExistsOnDisk = %v, want %v", tc.engine, got, tc.wantExists)
			}
			if got := mgr.SessionExistsOnDisk(tc.sessionID); got != tc.wantExists {
				t.Errorf("SessionExistsOnDisk = %v, want %v", got, tc.wantExists)
			}
			if size := mgr.GetTranscriptSize(tc.sessionID); (size > 0) != (tc.content != "") {
				t.Errorf("GetTranscriptSize = %d, want non-zero only with content (hasContent=%v)", size, tc.content != "")
			}
			if got := mgr.CountTranscriptSteps(tc.sessionID); got != tc.wantSteps {
				t.Errorf("CountTranscriptSteps = %d, want %d", got, tc.wantSteps)
			}
			if got := mgr.CountTranscriptTurns(tc.sessionID); got != tc.wantTurns {
				t.Errorf("CountTranscriptTurns = %d, want %d", got, tc.wantTurns)
			}
		})
	}
}

func TestIsClaudeToolResultContent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		content    string
		wantResult bool
	}{
		{"empty content", "", false},
		{"invalid json content", "not-json", false},
		{"empty json array", "[]", false},
		{"non-tool-result array", `[{"type":"text","text":"hello"}]`, false},
		{"tool-result array", `[{"type":"tool_result","content":"output"}]`, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isClaudeToolResultContent(json.RawMessage(tc.content)); got != tc.wantResult {
				t.Fatalf("isClaudeToolResultContent(%q) = %v, want %v", tc.content, got, tc.wantResult)
			}
		})
	}
}

func TestClaudeTranscriptFileReaders(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		filePath  string
		wantSteps int
		wantTurns int
		wantResp  string
	}{
		{name: "nonexistent file", filePath: "/nonexistent/path/file.jsonl"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
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

func TestCheckClaudeTargetProject(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		cwd        string
		createFile bool
		prePopSeen bool
		wantFound  bool
	}{
		{name: "empty cwd", cwd: "", createFile: true},
		{name: "missing file", cwd: "/missing/cwd"},
		{name: "file found and not seen", cwd: "/found/cwd", createFile: true, wantFound: true},
		{name: "file already seen", cwd: "/seen/cwd", createFile: true, prePopSeen: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tmpDir := t.TempDir()
			cleanID := "sess-target"
			cand := filepath.Join(tmpDir, SanitizeClaudeProjectPath(tc.cwd), cleanID+".jsonl")
			if tc.createFile && tc.cwd != "" {
				if err := os.MkdirAll(filepath.Dir(cand), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(cand, []byte("data\n"), 0o644); err != nil {
					t.Fatal(err)
				}
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

func TestExtractAssistantText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		content  string
		wantText string
	}{
		{"invalid json object", `{"invalid":true}`, ""},
		{"string content", `"hello world"`, "hello world"},
		{"blocks content", `[{"type":"text","text":"block text"}]`, "block text"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := extractAssistantText(json.RawMessage(tc.content)); got != tc.wantText {
				t.Fatalf("extractAssistantText = %q, want %q", got, tc.wantText)
			}
		})
	}
}

func TestExtractClaudeErrorFromLine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		line      string
		wantError string
	}{
		{"direct error type", `{"type":"error","message":{"role":"assistant","content":"direct error"}}`, "direct error"},
		{"status error text", `{"status":"error","message":{"role":"assistant","content":"status err"}}`, "status err"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got, _ := extractClaudeErrorFromLine(tc.line, time.Time{}); got != tc.wantError {
				t.Fatalf("extractClaudeErrorFromLine = %q, want %q", got, tc.wantError)
			}
		})
	}
}

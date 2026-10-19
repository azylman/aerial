package session

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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

func TestManager_InitAndActivity(t *testing.T) {
	tmp := t.TempDir()
	m := New(filepath.Join(tmp, "home"), filepath.Join(tmp, "data"))
	if m == nil || m.HomeDir() == "" || m.DataDir() == "" || len(m.Roots()) == 0 {
		t.Fatalf("unexpected Manager initialization: %+v", m)
	}
	var nilMgr *Manager
	if nilMgr.HomeDir() != "" || nilMgr.DataDir() != "" || len(nilMgr.Roots()) != 0 {
		t.Fatalf("nil Manager should return empty strings and nil roots")
	}

	tests := []struct {
		name       string
		isNilMgr   bool
		sessionID  string
		roots      []string
		setupLogs  bool
		setupTasks bool
		wantErr    bool
		wantZero   bool
	}{
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
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mgr := m
			if tc.isNilMgr {
				mgr = nil
			}
			roots := tc.roots
			if roots == nil && mgr != nil {
				roots = mgr.Roots()
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
		})
	}
}

func TestEnsureSessionDirAndBaseDir(t *testing.T) {
	tests := []struct {
		name       string
		isNilMgr   bool
		emptyRoots bool
		sessionID  string
		preExist   bool
		wantErr    bool
	}{
		{name: "nil manager", isNilMgr: true, sessionID: "sess-1", wantErr: true},
		{name: "empty session ID", sessionID: "", wantErr: true},
		{name: "path traversal", sessionID: "../bad", wantErr: true},
		{name: "empty roots manager", emptyRoots: true, sessionID: "sess-no-roots", wantErr: true},
		{name: "valid new session", sessionID: "sess-valid", wantErr: false},
		{name: "preexisting session", sessionID: "sess-pre", preExist: true, wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
		})
	}

	t.Run("getTargetDirs branches", func(t *testing.T) {
		tmp := t.TempDir()
		mgr := New(filepath.Join(tmp, "home"), filepath.Join(tmp, "data"))
		_ = os.MkdirAll(filepath.Join(tmp, "home", ".gemini", "antigravity-cli", "brain", "session-latest"), 0o755)
		dirsWithID := mgr.ExportGetTargetDirs("test-conv")
		if len(dirsWithID) == 0 {
			t.Fatalf("expected target dirs for test-conv")
		}
		dirsEmpty := mgr.ExportGetTargetDirs("")
		if len(dirsEmpty) == 0 {
			t.Fatalf("expected target dirs for empty convID")
		}
		var nilMgr *Manager
		if len(nilMgr.ExportGetTargetDirs("test")) != 0 {
			t.Fatalf("expected nil target dirs for nil manager")
		}
	})

	t.Run("resolveBaseDir and GetSessionDir branches", func(t *testing.T) {
		tmp := t.TempDir()
		mgr := New(filepath.Join(tmp, "nonexistent-root-1"), filepath.Join(tmp, "nonexistent-root-2"))
		if base := mgr.resolveBaseDir("s1"); base == "" {
			t.Fatalf("expected fallback root")
		}
		var nilMgr *Manager
		if nilMgr.resolveBaseDir("s1") != "" {
			t.Fatalf("expected empty for nil manager")
		}
		if _, err := nilMgr.GetSessionDir("s1"); err == nil {
			t.Fatalf("expected error for nil manager GetSessionDir")
		}
		if _, err := (&Manager{}).GetSessionDir("s1"); err == nil {
			t.Fatalf("expected error for empty roots GetSessionDir")
		}
		existingRoot := filepath.Join(tmp, "home", ".gemini", "antigravity-cli", "brain")
		_ = os.MkdirAll(existingRoot, 0o755)
		mgrWithExisting := New(filepath.Join(tmp, "home"), "")
		if base := mgrWithExisting.resolveBaseDir("s1"); base != existingRoot {
			t.Fatalf("expected existing root %q, got %q", existingRoot, base)
		}
		blockMgr := New(filepath.Join(tmp, "home"), filepath.Join(tmp, "data"))
		sessDir, _ := blockMgr.GetSessionDir("sess-block")
		_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"), 0o755)
		if _, err := blockMgr.EnsureSessionDir("sess-block"); err == nil {
			t.Fatalf("expected OpenFile error when transcript.jsonl is a directory")
		}
		mkdirBlockDir, _ := blockMgr.GetSessionDir("sess-mkdir-block")
		_ = os.MkdirAll(mkdirBlockDir, 0o755)
		_ = os.WriteFile(filepath.Join(mkdirBlockDir, ".system_generated"), []byte("file"), 0o644)
		if _, err := blockMgr.EnsureSessionDir("sess-mkdir-block"); err == nil {
			t.Fatalf("expected MkdirAll error when .system_generated is a file")
		}
	})
}

func TestAppendAmbientTurn(t *testing.T) {
	t0 := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name       string
		isNilMgr   bool
		sessionID  string
		channel    string
		author     string
		content    string
		noNewline  bool
		isC2       bool
		useZeroTS  bool
		wantSubstr string
		wantIndex  int
		wantErr    bool
	}{
		{name: "nil manager", isNilMgr: true, sessionID: "s1", channel: "c", author: "a", content: "m", wantErr: false},
		{name: "empty session", sessionID: "", channel: "c", author: "a", content: "m", wantErr: false},
		{name: "path traversal", sessionID: "../bad", channel: "c", author: "a", content: "m", wantErr: false},
		{name: "empty content", sessionID: "s-empty-turn", channel: "c", author: "a", content: "   ", wantErr: false},
		{name: "valid append to clean transcript", sessionID: "s-clean", channel: "#dev", author: "alex", content: "hello aerial", useZeroTS: true, wantSubstr: "hello aerial"},
		{name: "append to file without trailing newline", sessionID: "s-nonewline", channel: "#dev", author: "alex", content: "appended line", noNewline: true, wantSubstr: "appended line", wantIndex: 1},
		{name: "append to c2 nested session transcript", sessionID: "s-c2", channel: "#dev", author: "alex", content: "c2 message", isC2: true, wantSubstr: "c2 message"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			var mgr *Manager
			if !tc.isNilMgr {
				mgr = New(tmp, tmp)
				if tc.isC2 {
					c2Dir := filepath.Join(tmp, "brain", tc.sessionID, tc.sessionID, ".system_generated", "logs")
					_ = os.MkdirAll(c2Dir, 0o755)
					_ = os.WriteFile(filepath.Join(c2Dir, "transcript.jsonl"), []byte(""), 0o644)
				} else if tc.sessionID != "" && !strings.Contains(tc.sessionID, "..") {
					sessDir, _ := mgr.EnsureSessionDir(tc.sessionID)
					p := filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl")
					if tc.noNewline {
						_ = os.WriteFile(p, []byte(`{"step_index":0,"type":"USER_INPUT","content":"pre"}`), 0o644)
					}
				}
			}
			ts := t0
			if tc.useZeroTS {
				ts = time.Time{}
			}
			err := mgr.AppendAmbientTurn(tc.sessionID, tc.channel, tc.author, tc.content, ts)
			if (err != nil) != tc.wantErr {
				t.Fatalf("AppendAmbientTurn error=%v, wantErr=%v", err, tc.wantErr)
			}
			if tc.wantSubstr != "" {
				sessDir, _ := mgr.GetSessionDir(tc.sessionID)
				data, _ := os.ReadFile(filepath.Join(sessDir, ".system_generated", "logs", "transcript.jsonl"))
				if tc.isC2 {
					data, _ = os.ReadFile(filepath.Join(tmp, "brain", tc.sessionID, tc.sessionID, ".system_generated", "logs", "transcript.jsonl"))
				}
				if !strings.Contains(string(data), tc.wantSubstr) {
					t.Fatalf("missing substring %q:\n%s", tc.wantSubstr, string(data))
				}
			}
		})
	}

	t.Run("appendTranscriptStep error branch", func(t *testing.T) {
		tmp := t.TempDir()
		badPath := filepath.Join(tmp, "missing-dir", "sub", "test.jsonl")
		if err := ExportAppendTranscriptStep(badPath, []byte("data")); err == nil {
			t.Fatalf("expected error appending to non-existent dir")
		}
	})
}

func TestGetLastStepIndex(t *testing.T) {
	tests := []struct {
		name      string
		isMissing bool
		isDir     bool
		content   string
		wantIdx   int
		wantErr   bool
	}{
		{name: "nonexistent file", isMissing: true, wantIdx: -1, wantErr: false},
		{name: "empty file", content: "", wantIdx: -1, wantErr: false},
		{name: "directory path", isDir: true, wantIdx: -1, wantErr: true},
		{name: "invalid json", content: "invalid json line\n", wantIdx: -1, wantErr: false},
		{name: "trailing newlines", content: "{\"step_index\":5}\n\n\n", wantIdx: 5, wantErr: false},
		{name: "large chunk seek", content: fmt.Sprintf(`{"step_index":42,"content":%q}`+"\n", strings.Repeat("x", 80000)), wantIdx: 42, wantErr: false},
		{name: "large file no step index", content: strings.Repeat("not a valid step line\n", 4000), wantIdx: -1, wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			p := filepath.Join(tmp, "transcript.jsonl")
			if tc.isDir {
				p = tmp
			} else if !tc.isMissing {
				_ = os.WriteFile(p, []byte(tc.content), 0o644)
			}
			idx, err := ExportGetLastStepIndex(p)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ExportGetLastStepIndex error=%v, wantErr=%v", err, tc.wantErr)
			}
			if idx != tc.wantIdx {
				t.Fatalf("ExportGetLastStepIndex=%d, want %d", idx, tc.wantIdx)
			}
		})
	}
}

func TestFindLatestSessionDir(t *testing.T) {
	tests := []struct {
		name     string
		isNilMgr bool
		populate bool
		wantSub  string
	}{
		{name: "nil manager", isNilMgr: true, wantSub: ""},
		{name: "empty roots", isNilMgr: false, populate: false, wantSub: ""},
		{name: "finds newest session", isNilMgr: false, populate: true, wantSub: "sess-2"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			var mgr *Manager
			if !tc.isNilMgr {
				mgr = New(tmp, tmp)
				if tc.populate {
					d1 := filepath.Join(tmp, ".gemini", "antigravity-cli", "brain", "sess-1")
					d2 := filepath.Join(tmp, ".gemini", "antigravity-cli", "brain", "sess-2")
					dIgnored := filepath.Join(tmp, ".gemini", "antigravity-cli", "brain", "ambient-eval-skip")
					fIgnored := filepath.Join(tmp, ".gemini", "antigravity-cli", "brain", "a-file.txt")
					_ = os.MkdirAll(d1, 0o755)
					_ = os.MkdirAll(d2, 0o755)
					_ = os.MkdirAll(dIgnored, 0o755)
					_ = os.WriteFile(fIgnored, []byte("file"), 0o644)
					tPast := time.Now().Add(-10 * time.Minute)
					tNew := time.Now()
					_ = os.Chtimes(d1, tPast, tPast)
					_ = os.Chtimes(d2, tNew, tNew)
					_ = os.Chtimes(dIgnored, tNew.Add(time.Minute), tNew.Add(time.Minute))
				}
			}
			got := mgr.FindLatestSessionDir(time.Time{})
			if tc.wantSub == "" && got != "" {
				t.Fatalf("expected empty, got %q", got)
			} else if tc.wantSub != "" && !strings.Contains(got, tc.wantSub) {
				t.Fatalf("got %q, want substring %q", got, tc.wantSub)
			}
		})
	}
}

func TestDumpSessionDiagnosticLogs(t *testing.T) {
	tests := []struct {
		name       string
		isNilMgr   bool
		sessionID  string
		setupLogs  bool
		wantSubstr string
	}{
		{name: "nil manager", isNilMgr: true, sessionID: "s1", wantSubstr: ""},
		{name: "empty ID", sessionID: "", wantSubstr: ""},
		{name: "path traversal", sessionID: "../bad", wantSubstr: ""},
		{
			name:       "dumps transcript and task logs",
			sessionID:  "s-diag",
			setupLogs:  true,
			wantSubstr: "transcript.jsonl",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			var mgr *Manager
			if !tc.isNilMgr {
				mgr = New(tmp, tmp)
				if tc.setupLogs {
					writeTranscript(t, mgr, tc.sessionID, "transcript.jsonl", "{\"step_index\":0}\n")
					sessDir, _ := mgr.GetSessionDir(tc.sessionID)
					taskLog := filepath.Join(sessDir, ".system_generated", "tasks", "task-1.log")
					_ = os.MkdirAll(filepath.Dir(taskLog), 0o755)
					_ = os.WriteFile(taskLog, []byte("task trace line"), 0o644)
				}
			}
			dump := mgr.DumpSessionDiagnosticLogs(tc.sessionID)
			if tc.wantSubstr == "" && dump != "" {
				t.Fatalf("expected empty, got %q", dump)
			} else if tc.wantSubstr != "" && !strings.Contains(dump, tc.wantSubstr) {
				t.Fatalf("dump missing substring %q:\n%s", tc.wantSubstr, dump)
			}
		})
	}
}

func TestExtractResponseAndError(t *testing.T) {
	t.Run("FormatStepError variants", func(t *testing.T) {
		var typedNil *int
		formatTests := []struct {
			name     string
			val      any
			expected string
		}{
			{"nil", nil, ""},
			{"string", "some error", "some error"},
			{"map", map[string]interface{}{"msg": "fail"}, `{"msg":"fail"}`},
			{"typed nil", typedNil, ""},
			{"int", 500, "500"},
		}
		for _, ft := range formatTests {
			t.Run(ft.name, func(t *testing.T) {
				got := ExportFormatStepError(ft.val)
				if got != ft.expected {
					t.Fatalf("ExportFormatStepError(%v)=%q, want %q", ft.val, got, ft.expected)
				}
			})
		}
		// Unmarshalable map branch
		gotUnm := ExportFormatStepError(map[string]interface{}{"bad": make(chan int)})
		if !strings.HasPrefix(gotUnm, "map[bad:") {
			t.Fatalf("expected map[bad prefix, got %q", gotUnm)
		}
	})

	tests := []struct {
		name         string
		isNilMgr     bool
		convID       string
		transcript   string
		wantResponse string
		wantError    string
	}{
		{name: "nil manager", isNilMgr: true, convID: "s1"},
		{name: "empty session", convID: ""},
		{name: "path traversal", convID: "../bad"},
		{name: "clean response without error", convID: "s-clean", transcript: `{"step_index":0,"source":"MODEL","type":"PLANNER_RESPONSE","content":"all good"}`, wantResponse: "all good"},
		{name: "extracts response and map error", convID: "s-err", transcript: `{"step_index":0,"source":"MODEL","type":"PLANNER_RESPONSE","content":"answering"}` + "\n" + `{"step_index":1,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","error":{"code":404}}`, wantResponse: "answering", wantError: `{"code":404}`},
		{name: "status error without error field and tool calls response", convID: "s-err-content", transcript: `{"step_index":0,"source":"MODEL","type":"PLANNER_RESPONSE","content":"","tool_calls":[{"name":"ls"}]}` + "\n" + `{"step_index":1,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"fallback error"}`, wantResponse: `[Tool Call Requested]: [{"name":"ls"}]`, wantError: "fallback error"},
		{name: "turn only contains generic messages without response or error", convID: "s-generic-only", transcript: `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"run"}` + "\n\n" + `{"step_index":1,"source":"MODEL","type":"GENERIC","content":"some intermediate info"}` + "\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			var mgr *Manager
			if !tc.isNilMgr {
				mgr = New(tmp, tmp)
				if tc.transcript != "" {
					writeTranscript(t, mgr, tc.convID, "transcript.jsonl", tc.transcript)
				}
			}
			resp, errStr := mgr.ExtractResponseAndError(tc.convID)
			if resp != tc.wantResponse || errStr != tc.wantError {
				t.Fatalf("ExtractResponseAndError=(%q, %q), want (%q, %q)", resp, errStr, tc.wantResponse, tc.wantError)
			}
		})
	}
}

func TestExtractLastTurnError(t *testing.T) {
	t0 := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)
	tests := []struct {
		name          string
		isNilMgr      bool
		convID        string
		transcript    string
		writeFilename string
		since         time.Time
		cancelCtx     bool
		wantError     string
		wantErr       bool
	}{
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
				`{"step_index":1,"source":"MODEL","type":"GENERIC","status":"ERROR","content":"skip generic","created_at":%q}`+"\n"+
				`{"step_index":2,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","content":"content err","created_at":%q}`+"\n",
				t0.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)),
			since:     t0,
			wantError: "content err",
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
			name:   "large file seek with fallback read",
			convID: "s-large-err",
			transcript: func() string {
				var sb strings.Builder
				sb.WriteString(fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","created_at":%q}`+"\n", t0.Format(time.RFC3339Nano)))
				padding := strings.Repeat("A", 1000)
				for i := 1; i <= 1050; i++ {
					sb.WriteString(fmt.Sprintf(`{"step_index":%d,"source":"MODEL","type":"PLANNER_RESPONSE","content":%q}`+"\n", i, padding))
				}
				sb.WriteString(fmt.Sprintf(`{"step_index":1051,"source":"MODEL","type":"ERROR_MESSAGE","status":"ERROR","error":"large chunk fail","created_at":%q}`+"\n", t1.Format(time.RFC3339Nano)))
				return sb.String()
			}(),
			since:     t0,
			wantError: "large chunk fail",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			var mgr *Manager
			if !tc.isNilMgr {
				mgr = New(tmp, tmp)
				if tc.transcript != "" {
					fn := "transcript_full.jsonl"
					if tc.writeFilename != "" {
						fn = tc.writeFilename
					}
					writeTranscript(t, mgr, tc.convID, fn, tc.transcript)
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
		})
	}
}

func TestExtractResponseSince(t *testing.T) {
	t0 := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Minute)

	t.Run("IsStepBefore variants", func(t *testing.T) {
		now := time.Now()
		if ExportIsStepBefore("", now) {
			t.Fatalf("expected false for empty createdAt")
		}
		if ExportIsStepBefore("2026-10-19T12:00:00Z", time.Time{}) {
			t.Fatalf("expected false for zero since")
		}
		if !ExportIsStepBefore("2026-10-19T12:00:00.123456789Z", now) {
			t.Fatalf("expected true for RFC3339Nano")
		}
		if ExportIsStepBefore("not-a-valid-ts", now) {
			t.Fatalf("expected false for invalid timestamp")
		}
	})

	tests := []struct {
		name       string
		isNilMgr   bool
		convID     string
		transcript string
		since      time.Time
		cancelCtx  bool
		useCtx     bool
		wantResp   string
	}{
		{name: "nil manager", isNilMgr: true, convID: "s1", since: t0},
		{name: "empty convID", convID: "", since: t0},
		{name: "path traversal", convID: "../bad", since: t0},
		{name: "canceled context", convID: "s1", since: t0, cancelCtx: true, useCtx: true},
		{name: "rescues response created after since", convID: "s-after", transcript: fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"Turn 1\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"substantive answer\",\"created_at\":%q}\n", t0.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)), since: t0, wantResp: "substantive answer"},
		{name: "rfc3339 without nanos format", convID: "s-rfc3339", transcript: fmt.Sprintf("{\"type\":\"PLANNER_RESPONSE\",\"content\":\"rfc3339 answer\",\"created_at\":%q}\n", t1.Format(time.RFC3339)), since: t0, wantResp: "rfc3339 answer"},
		{name: "response created before since", convID: "s-before", transcript: fmt.Sprintf("{\"type\":\"PLANNER_RESPONSE\",\"content\":\"stale answer\",\"created_at\":%q}\n", t0.Format(time.RFC3339Nano)), since: t1, wantResp: ""},
		{name: "invalid timestamp string", convID: "s-invalid-ts", transcript: "{\"type\":\"PLANNER_RESPONSE\",\"content\":\"bad ts answer\",\"created_at\":\"not-a-timestamp\"}\n", since: t0, wantResp: "bad ts answer"},
		{name: "skips tool call request and generic types and empty planner content", convID: "s-tool-generic", transcript: fmt.Sprintf("{\"type\":\"GENERIC\",\"content\":\"search results\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"[Tool Call Requested]: grep\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"   \",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"actual answer\",\"created_at\":%q}\n", t0.Format(time.RFC3339Nano), t0.Format(time.RFC3339Nano), t0.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)), since: t0, wantResp: "actual answer"},
		{name: "tail has tool call and empty content with older step before since", convID: "s-tail-empty", transcript: fmt.Sprintf("{\"type\":\"USER_INPUT\",\"created_at\":%q}\n{\"type\":\"GENERIC\",\"content\":\"skip me\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"[Tool Call Requested]: run\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"\",\"tool_calls\":[{\"name\":\"call\"}],\"created_at\":%q}\n", t0.Add(-time.Hour).Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)), since: t0, wantResp: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			var mgr *Manager
			if !tc.isNilMgr {
				mgr = New(tmp, tmp)
				if tc.transcript != "" {
					writeTranscript(t, mgr, tc.convID, "transcript_full.jsonl", tc.transcript)
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

func TestExtractTranscriptToolActions(t *testing.T) {
	t.Run("ExtractCommandOrArgs variants", func(t *testing.T) {
		argsTests := []struct {
			name     string
			raw      string
			expected string
		}{
			{"empty", "", ""},
			{"cmd line", `{"CommandLine":"ls -la"}`, "ls -la"},
			{"single string key", `{"path":"/tmp"}`, "/tmp"},
			{"single non-string key", `{"count":42}`, `{"count":42}`},
			{"multi key", `{"a":"1","b":"2"}`, `{"a":"1","b":"2"}`},
			{"json string", `"my string"`, "my string"},
			{"raw non-json", `raw value`, "raw value"},
			{"empty json map", `{}`, ""},
		}
		for _, at := range argsTests {
			t.Run(at.name, func(t *testing.T) {
				got := ExportExtractCommandOrArgs(json.RawMessage(at.raw))
				if got != at.expected {
					t.Fatalf("ExportExtractCommandOrArgs(%s)=%q, want %q", at.raw, got, at.expected)
				}
			})
		}
	})

	tests := []struct {
		name       string
		isNilMgr   bool
		convID     string
		transcript string
		wantSub    string
		wantPrefix string
		wantSuffix string
		maxLen     int
		wantEmpty  bool
	}{
		{name: "nil manager", isNilMgr: true, convID: "s1", wantEmpty: true},
		{name: "empty ID", convID: "", wantEmpty: true},
		{name: "path traversal", convID: "../bad", wantEmpty: true},
		{name: "empty transcript", convID: "s-empty", wantEmpty: true},
		{
			name:   "diverse args (cmd, single key, multi key, string, no args)",
			convID: "s-diverse",
			transcript: `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"run"}` + "\n" +
				`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","tool_calls":[{"name":"t_cmd","args":{"cmd":"echo cmd"}},{"name":"t_single","args":{"custom":"val"}},{"name":"t_multi","args":{"k1":"v1","k2":"v2"}},{"name":"t_str","args":"raw string"},{"name":"t_empty_map","args":{}},{"name":"t_noargs"}]}` + "\n" +
				`{"step_index":2,"source":"MODEL","type":"GENERIC","status":"DONE","content":"result ok"}` + "\n" +
				`{"step_index":3,"source":"MODEL","type":"GENERIC","status":"","error":"only err"}` + "\n" +
				`{"step_index":4,"source":"MODEL","type":"GENERIC","status":"DONE","content":"content with error","error":"detail"}` + "\n" +
				fmt.Sprintf(`{"step_index":5,"source":"MODEL","type":"GENERIC","status":"DONE","content":%q}`+"\n", strings.Repeat("LongSnippet", 50)),
			wantSub:    "echo cmd",
			wantPrefix: "<PREVIOUS_TURN_ACTIONS>\n",
			wantSuffix: "</PREVIOUS_TURN_ACTIONS>",
		},
		{
			name:   "single oversized action clamped",
			convID: "s-single-oversized",
			transcript: fmt.Sprintf(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"run"}`+"\n"+
				`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","tool_calls":[{"name":"cmd","args":{"CommandLine":%q}}]}`+"\n"+
				`{"step_index":2,"source":"MODEL","type":"GENERIC","status":"DONE","content":"out"}`+"\n",
				strings.Repeat("Z", 2500)),
			maxLen:     2000,
			wantPrefix: "<PREVIOUS_TURN_ACTIONS>",
			wantSuffix: "</PREVIOUS_TURN_ACTIONS>",
		},
		{
			name:   "extreme fallback single recent action exceeds budget",
			convID: "s-extreme-fallback",
			transcript: `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"run"}` + "\n" +
				`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","tool_calls":[{"name":"cmd1","args":{"cmd":"short"}},{"name":"cmd2","args":{"cmd":` + fmt.Sprintf("%q", strings.Repeat("M", 2500)) + `}}]}` + "\n" +
				`{"step_index":2,"source":"MODEL","type":"GENERIC","status":"DONE","content":"done1"}` + "\n" +
				`{"step_index":3,"source":"MODEL","type":"GENERIC","status":"DONE","content":"done2"}` + "\n",
			maxLen:     2000,
			wantPrefix: "<PREVIOUS_TURN_ACTIONS>",
			wantSuffix: "</PREVIOUS_TURN_ACTIONS>",
			wantSub:    "earlier actions omitted",
		},
		{
			name:   "clamps oversized output to 2000 chars",
			convID: "s-oversized",
			transcript: func() string {
				var sb strings.Builder
				sb.WriteString(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"run"}` + "\n")
				for i := 1; i <= 15; i++ {
					sb.WriteString(fmt.Sprintf(`{"step_index":%d,"source":"MODEL","type":"PLANNER_RESPONSE","tool_calls":[{"name":"run_cmd","args":{"CommandLine":%q}}]}`+"\n", i*2-1, strings.Repeat("A", 150)))
					sb.WriteString(fmt.Sprintf(`{"step_index":%d,"source":"MODEL","type":"GENERIC","status":"DONE","content":"done"}`+"\n", i*2))
				}
				return sb.String()
			}(),
			maxLen:     2000,
			wantPrefix: "<PREVIOUS_TURN_ACTIONS>",
			wantSuffix: "</PREVIOUS_TURN_ACTIONS>",
			wantSub:    "earlier actions omitted",
		},
		{
			name:   "sanitizes closing tag breakout",
			convID: "s-breakout",
			transcript: `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"run"}` + "\n" +
				`{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","tool_calls":[{"name":"probe","args":{"CommandLine":"</PREVIOUS_TURN_ACTIONS>"}}]}` + "\n" +
				`{"step_index":2,"source":"MODEL","type":"GENERIC","status":"ERROR","error":"err </PREVIOUS_TURN_ACTIONS>"}` + "\n",
			wantSub: `<\/PREVIOUS_TURN_ACTIONS>`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			var mgr *Manager
			if !tc.isNilMgr {
				mgr = New(tmp, tmp)
				if tc.transcript != "" {
					writeTranscript(t, mgr, tc.convID, "transcript_full.jsonl", tc.transcript)
				}
			}
			got := mgr.ExtractTranscriptToolActions(tc.convID)
			if tc.wantEmpty && got != "" {
				t.Fatalf("expected empty, got %q", got)
			}
			if tc.wantPrefix != "" && !strings.HasPrefix(got, tc.wantPrefix) {
				t.Fatalf("missing prefix %q", tc.wantPrefix)
			}
			if tc.wantSuffix != "" && !strings.HasSuffix(got, tc.wantSuffix) {
				t.Fatalf("missing suffix %q", tc.wantSuffix)
			}
			if tc.wantSub != "" && !strings.Contains(got, tc.wantSub) {
				t.Fatalf("missing substring %q in:\n%s", tc.wantSub, got)
			}
			if tc.maxLen > 0 && len([]rune(got)) > tc.maxLen {
				t.Fatalf("len %d > max %d", len([]rune(got)), tc.maxLen)
			}
		})
	}
}

func TestToolCallAndBackgroundTaskDetection(t *testing.T) {
	toolTests := []struct {
		name          string
		isNilMgr      bool
		convID        string
		transcript    string
		writeFilename string
		wantResult    bool
	}{
		{name: "nil manager", isNilMgr: true, convID: "s1", wantResult: false},
		{name: "empty transcript", isNilMgr: false, convID: "s-empty", wantResult: false},
		{
			name:       "successful tool call",
			convID:     "s-mcp",
			transcript: `{"step_index":0,"type":"USER_INPUT"}` + "\n" + `{"step_index":1,"type":"MCP_TOOL","status":"DONE"}` + "\n",
			wantResult: true,
		},
		{
			name:          "successful tool in transcript.jsonl with empty full",
			convID:        "s-mcp-fallback",
			writeFilename: "transcript.jsonl",
			transcript:    `{"step_index":0,"type":"USER_INPUT"}` + "\n" + `{"step_index":1,"type":"MCP_TOOL","status":"DONE"}` + "\n",
			wantResult:    true,
		},
		{
			name:       "failed tool call",
			convID:     "s-fail",
			transcript: `{"step_index":0,"type":"USER_INPUT"}` + "\n" + `{"step_index":1,"type":"RUN_COMMAND","status":"ERROR"}` + "\n",
			wantResult: false,
		},
	}
	for _, tc := range toolTests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			var mgr *Manager
			if !tc.isNilMgr {
				mgr = New(tmp, tmp)
				if tc.transcript != "" {
					fn := "transcript_full.jsonl"
					if tc.writeFilename != "" {
						fn = tc.writeFilename
					}
					writeTranscript(t, mgr, tc.convID, fn, tc.transcript)
				}
			}
			if got := mgr.HasSuccessfulToolCall(tc.convID); got != tc.wantResult {
				t.Fatalf("HasSuccessfulToolCall=%v, want %v", got, tc.wantResult)
			}
		})
	}

	taskTests := []struct {
		name       string
		isNilMgr   bool
		convID     string
		transcript string
		wantHas    bool
		wantTaskID string
	}{
		{"nil manager", true, "s1", "", false, ""},
		{"invalid ID", false, "../bad", "", false, ""},
		{"non-existent session", false, "s-none", "", false, ""},
		{name: "completed task via sender", convID: "s-comp", transcript: "{\"step_index\":0,\"type\":\"USER_INPUT\"}\n{\"step_index\":1,\"content\":\"Tool is running as a background task with task id: s-comp/t1\"}\n{\"step_index\":2,\"content\":\"[Message] sender=s-comp/t1 content=ok\"}\n"},
		{name: "completed task via sender suffix", convID: "s-comp-suffix", transcript: "{\"step_index\":0,\"type\":\"USER_INPUT\"}\n{\"step_index\":1,\"content\":\"Tool is running as a background task with task id: s-comp-suffix/t2\"}\n{\"step_index\":2,\"content\":\"[Message] sender=t2 content=ok\"}\n"},
		{name: "completed task via finish content", convID: "s-comp-fin", transcript: "{\"step_index\":0,\"type\":\"USER_INPUT\"}\n{\"step_index\":1,\"content\":\"Tool is running as a background task with task id: s-comp-fin/t3\"}\n{\"step_index\":2,\"content\":\"Task id \\\"s-comp-fin/t3\\\" finished with result:\\nall clear\"}\n"},
		{name: "unfinished background task", convID: "s-unfin", transcript: "{\"step_index\":0,\"type\":\"USER_INPUT\"}\n{\"step_index\":1,\"content\":\"Tool is running as a background task with task id: s-unfin/t4\"}\n", wantHas: true, wantTaskID: "s-unfin/t4"},
		{name: "plain text unmarshaled task and empty content lines", convID: "s-plain-task", transcript: "{\"step_index\":0,\"type\":\"USER_INPUT\",\"content\":\"\"}\nTool is running as a background task with task id: plain-unmarshal-99\n", wantHas: true, wantTaskID: "plain-unmarshal-99"},
	}
	for _, tc := range taskTests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			var mgr *Manager
			if !tc.isNilMgr {
				mgr = New(tmp, tmp)
				if tc.transcript != "" {
					writeTranscript(t, mgr, tc.convID, "transcript_full.jsonl", tc.transcript)
				}
			}
			has, taskID, err := mgr.HasUnfinishedBackgroundTask(tc.convID)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if has != tc.wantHas || taskID != tc.wantTaskID {
				t.Fatalf("HasUnfinishedBackgroundTask=(%v, %q), want (%v, %q)", has, taskID, tc.wantHas, tc.wantTaskID)
			}
		})
	}
}

func TestActiveTasks(t *testing.T) {
	tests := []struct {
		name       string
		isNilMgr   bool
		sessionID  string
		saveTasks  []TaskMetadata
		corrupt    bool
		blockDir   bool
		blockFile  bool
		wantCount  int
		wantErrGet bool
		wantErrSav bool
	}{
		{"nil manager", true, "sess-1", nil, false, false, false, 0, true, true},
		{"empty session", false, "", nil, false, false, false, 0, true, true},
		{"path traversal", false, "../bad", nil, false, false, false, 0, true, true},
		{"nonexistent tasks file", false, "sess-empty-tasks", nil, false, false, false, 0, false, false},
		{
			name:      "save and retrieve tasks",
			sessionID: "sess-active",
			saveTasks: []TaskMetadata{
				{TaskID: "task-1", ToolName: "run_command", CommandLine: "sleep 1", StartedAt: time.Now()},
			},
			wantCount: 1,
		},
		{
			name:       "corrupted active_tasks.json",
			sessionID:  "sess-corrupt",
			corrupt:    true,
			wantErrGet: true,
		},
		{
			name:       "mkdir failure on save",
			sessionID:  "sess-mkdir-fail",
			saveTasks:  []TaskMetadata{{TaskID: "t1"}},
			blockDir:   true,
			wantErrGet: true,
			wantErrSav: true,
		},
		{
			name:       "readfile error not exist",
			sessionID:  "sess-readfile-dir",
			blockFile:  true,
			wantErrGet: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			var mgr *Manager
			if !tc.isNilMgr {
				mgr = New(tmp, tmp)
			}
			if tc.blockDir {
				sessDir, _ := mgr.GetSessionDir(tc.sessionID)
				_ = os.MkdirAll(sessDir, 0o755)
				_ = os.WriteFile(filepath.Join(sessDir, ".system_generated"), []byte("blocking-file"), 0o644)
			} else if tc.blockFile {
				sessDir, _ := mgr.EnsureSessionDir(tc.sessionID)
				_ = os.MkdirAll(filepath.Join(sessDir, ".system_generated", "active_tasks.json"), 0o755)
			} else if tc.corrupt {
				sessDir, _ := mgr.EnsureSessionDir(tc.sessionID)
				_ = os.WriteFile(filepath.Join(sessDir, ".system_generated", "active_tasks.json"), []byte("invalid-json"), 0o644)
			} else if tc.wantErrSav || len(tc.saveTasks) > 0 {
				err := mgr.SaveActiveTasks(tc.sessionID, tc.saveTasks)
				if (err != nil) != tc.wantErrSav {
					t.Fatalf("SaveActiveTasks error=%v, wantErr=%v", err, tc.wantErrSav)
				}
			}
			loaded, err := mgr.GetActiveTasks(tc.sessionID)
			if (err != nil) != tc.wantErrGet {
				t.Fatalf("GetActiveTasks error=%v, wantErr=%v", err, tc.wantErrGet)
			}
			if len(loaded) != tc.wantCount {
				t.Fatalf("GetActiveTasks returned %d tasks, want %d", len(loaded), tc.wantCount)
			}
		})
	}
}

func TestSessionStorageAndRotationMetrics(t *testing.T) {
	tests := []struct {
		name       string
		isNilMgr   bool
		sessionID  string
		dbSize     int
		pbSize     int
		transSize  int
		fullSize   int
		stepCount  int
		turnCount  int
		rtDir      bool
		wantDBSize int64
		wantTrSize int64
		wantSteps  int
		wantTurns  int
		wantExists bool
	}{
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
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			var mgr *Manager
			if !tc.isNilMgr {
				mgr = New(tmp, tmp)
				convDir := filepath.Join(tmp, "conversations")
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
				if tc.transSize > 0 || (tc.name == "0-byte files do not exist") {
					sessDir, _ := mgr.EnsureSessionDir(tc.sessionID)
					var content string
					if tc.stepCount > 0 {
						var sb strings.Builder
						for i := 0; i < tc.stepCount; i++ {
							typ := "PLANNER_RESPONSE"
							source := "MODEL"
							if i < tc.turnCount {
								typ = "USER_INPUT"
								source = "USER_EXPLICIT"
							}
							sb.WriteString(fmt.Sprintf(`{"step_index":%d,"source":%q,"type":%q,"content":"msg"}`+"\n", i, source, typ))
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

	cleanupTests := []struct {
		name      string
		sessionID string
	}{
		{"cleanup valid and invalid sessions", "sess-ephemeral"},
	}
	for _, tc := range cleanupTests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			targetDir := filepath.Join(tmp, tc.sessionID)
			_ = os.MkdirAll(targetDir, 0o755)
			_ = os.MkdirAll(filepath.Join(tmp, ".gemini", "antigravity-cli", "brain", tc.sessionID), 0o755)
			_ = os.MkdirAll(filepath.Join(tmp, ".gemini", "antigravity", "brain", tc.sessionID), 0o755)
			_ = os.MkdirAll(filepath.Join(tmp, "brain", tc.sessionID), 0o755)

			CleanupEphemeralSession("")
			CleanupEphemeralSession("../bad", tmp)
			CleanupEphemeralSession(tc.sessionID, "", tmp)

			if _, err := os.Stat(targetDir); !os.IsNotExist(err) {
				t.Fatalf("expected targetDir to be deleted")
			}

			if DefaultMinSessionTurns != 3 || DefaultEmergencyRotationMultiplier != 3 ||
				DefaultMaxSessionTurns != 8 || DefaultMaxSessionSteps != 180 ||
				DefaultMaxTranscriptBytes != 1024*1024 || DefaultMaxSessionTranscriptBytes != 2*1024*1024 ||
				DefaultMaxSessionDBBytes != 4*1024*1024 || DefaultMaxQuotaPauseDBBytes != 3*1024*1024 {
				t.Fatalf("unexpected session rotation constant values")
			}
		})
	}
}


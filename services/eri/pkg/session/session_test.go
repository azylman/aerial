package session

import (
	"context"
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
		{name: "rescues response created after since", convID: "s-after", transcript: fmt.Sprintf("{\"type\":\"USER_INPUT\",\"content\":\"Turn 1\",\"created_at\":%q}\n{\"type\":\"PLANNER_RESPONSE\",\"content\":\"substantive answer\",\"created_at\":%q}\ninvalid-json-line\n", t0.Format(time.RFC3339Nano), t1.Format(time.RFC3339Nano)), since: t0, wantResp: "substantive answer"},
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

func TestSessionStorageAndRotationMetrics(t *testing.T) {
	tests := []struct {
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
		{
			name:        "stat non-not-exist error when candidate dir is a file",
			sessionID:   "s-stat-err",
			statErrFile: true,
			wantExists:  false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tmp := t.TempDir()
			var mgr *Manager
			if !tc.isNilMgr {
				mgr = New(tmp, tmp)
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

	if DefaultMinSessionTurns != 3 || DefaultEmergencyRotationMultiplier != 3 ||
		DefaultMaxSessionTurns != 8 || DefaultMaxSessionSteps != 180 ||
		DefaultMaxTranscriptBytes != 1024*1024 || DefaultMaxSessionTranscriptBytes != 2*1024*1024 ||
		DefaultMaxSessionDBBytes != 4*1024*1024 || DefaultMaxQuotaPauseDBBytes != 3*1024*1024 {
		t.Fatalf("unexpected session rotation constant values")
	}
}

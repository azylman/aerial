package session

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type museSessionLifecycleCase struct {
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

func resolveMuseWriteDir(location, homeDir, dataDir, sessionID string) (string, string) {
	switch location {
	case "data_muse_nested":
		return filepath.Join(dataDir, "muse", "sessions", "2026", "11", "15", sessionID), "session.jsonl"
	case "data_runtime_muse":
		return filepath.Join(dataDir, "runtimes", "discord", ".local", "share", "muse", "sessions"), sessionID + ".jsonl"
	case "home_muse":
		return filepath.Join(homeDir, ".local", "share", "muse", "sessions", sessionID), "session.jsonl"
	case "direct_muse":
		return filepath.Join(dataDir, "muse", "sessions"), sessionID + ".jsonl"
	default:
		return "", ""
	}
}

func setupMuseSessionFile(tc museSessionLifecycleCase, homeDir, dataDir string, t0 time.Time) {
	writeDir, fileName := resolveMuseWriteDir(tc.location, homeDir, dataDir, tc.sessionID)
	if writeDir == "" {
		return
	}
	_ = os.MkdirAll(writeDir, 0o755)
	filePath := filepath.Join(writeDir, fileName)
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

func assertMuseSessionMetrics(t *testing.T, mgr *Manager, tc museSessionLifecycleCase, dataDir string) {
	t.Helper()
	if got := mgr.MuseSessionExistsOnDisk(tc.sessionID); got != tc.wantExists {
		t.Fatalf("[%s] MuseSessionExistsOnDisk = %v, want %v", tc.name, got, tc.wantExists)
	}
	if got := mgr.SessionExistsOnDisk(tc.sessionID); got != tc.wantExists {
		t.Fatalf("[%s] SessionExistsOnDisk = %v, want %v", tc.name, got, tc.wantExists)
	}
	paths := mgr.museTranscriptPaths(tc.sessionID)
	if len(paths) != tc.wantPaths {
		t.Fatalf("[%s] museTranscriptPaths count = %d, want %d", tc.name, len(paths), tc.wantPaths)
	}

	size := mgr.GetTranscriptSize(tc.sessionID)
	if (tc.wantSize > 0 && size != tc.wantSize) || (tc.content != "" && size == 0) {
		t.Errorf("[%s] size got=%d want=%d", tc.name, size, tc.wantSize)
	}

	if st := mgr.CountTranscriptSteps(tc.sessionID); st != tc.wantSteps {
		t.Errorf("[%s] steps got=%d want=%d", tc.name, st, tc.wantSteps)
	}
	if tt := mgr.CountTranscriptTurns(tc.sessionID); tt != tc.wantTurns {
		t.Errorf("[%s] turns got=%d want=%d", tc.name, tt, tc.wantTurns)
	}

	root := ""
	if tc.name == "custom roots override in GetSessionLastActivity" {
		root = filepath.Join(dataDir, "muse", "sessions")
	}
	lastAct, err := evaluateMuseLastActivity(mgr, tc.sessionID, root)
	if err != nil {
		t.Fatalf("[%s] lastActivity err: %v", tc.name, err)
	}
	hasTime := !lastAct.IsZero()
	if hasTime != tc.wantNonZeroTime {
		t.Errorf("[%s] nonZeroTime got=%v want=%v", tc.name, hasTime, tc.wantNonZeroTime)
	}
}

func evaluateMuseLastActivity(mgr *Manager, sessionID, customRoot string) (time.Time, error) {
	if customRoot != "" {
		return mgr.GetSessionLastActivity(sessionID, customRoot)
	}
	return mgr.GetSessionLastActivity(sessionID)
}

func runMuseSessionLifecycleCase(t *testing.T, tc museSessionLifecycleCase, t0 time.Time) {
	t.Helper()
	tmp := t.TempDir()
	var mgr *Manager
	var dataDir string
	if !tc.isNilMgr {
		homeDir := filepath.Join(tmp, "home")
		dataDir = filepath.Join(tmp, "data")
		mgr = New(homeDir, dataDir)
		setupMuseSessionFile(tc, homeDir, dataDir, t0)
	}
	assertMuseSessionMetrics(t, mgr, tc, dataDir)
}

func TestMuseSessionLifecycle(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 11, 7, 10, 0, 0, 0, time.UTC)

	tests := []museSessionLifecycleCase{
		{
			name:      "nil manager",
			isNilMgr:  true,
			sessionID: "sess-muse-nil",
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
			name:      "data muse nested session.jsonl",
			sessionID: "sess-muse-data",
			location:  "data_muse_nested",
			content: strings.Join([]string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Initial prompt"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510002000000,"payload":{"event":{"kind":"assistant_message_committed","text":"Done"}}}`,
			}, "\n") + "\n",
			age:             10 * time.Minute,
			wantExists:      true,
			wantPaths:       1,
			wantSteps:       1,
			wantTurns:       1,
			wantNonZeroTime: true,
		},
		{
			name:      "data runtime discord session.jsonl direct",
			sessionID: "sess-muse-runtime",
			location:  "data_runtime_muse",
			content: strings.Join([]string{
				`{"payload_type":"runtime.session","recorded_at":"2026-11-07T10:00:00Z","payload":{"type":"message","role":"user","content":"User msg"}}`,
				`{"payload_type":"runtime.session","recorded_at":"2026-11-07T10:00:01Z","payload":{"type":"message","role":"assistant","content":"Assistant msg"}}`,
				`{"payload_type":"runtime.session","recorded_at":"2026-11-07T10:00:02Z","payload":{"event":{"kind":"tool_call","name":"bash","id":"call_1"}}}`,
				`{"payload_type":"runtime.session","recorded_at":"2026-11-07T10:00:03Z","payload":{"event":{"kind":"tool_completed","id":"call_1","text":"ok"}}}`,
			}, "\n") + "\n",
			wantExists:      true,
			wantPaths:       1,
			wantSteps:       3,
			wantTurns:       1,
			wantNonZeroTime: true,
		},
		{
			name:      "home muse session.jsonl",
			sessionID: "sess-muse-home",
			location:  "home_muse",
			content: strings.Join([]string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"[Chat Alex]: ambient message"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"model_completed","text":"model output"}}}`,
			}, "\n") + "\n",
			wantExists:      true,
			wantPaths:       1,
			wantSteps:       1,
			wantTurns:       0,
			wantNonZeroTime: true,
		},
		{
			name:       "zero byte muse session",
			sessionID:  "sess-muse-zero",
			location:   "direct_muse",
			zeroByte:   true,
			wantExists: false,
			wantPaths:  1,
		},
		{
			name:      "custom roots override in GetSessionLastActivity",
			sessionID: "sess-muse-custom-root",
			location:  "direct_muse",
			content: strings.Join([]string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Custom root test"}}}`,
			}, "\n") + "\n",
			wantExists:      true,
			wantPaths:       1,
			wantSteps:       0,
			wantTurns:       1,
			wantNonZeroTime: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runMuseSessionLifecycleCase(t, tc, t0)
		})
	}
}

type museTurnErrorTestCase struct {
	name    string
	lines   []string
	since   time.Time
	want    string
	wantErr bool
}

func museTurnErrorTerminalCases() []museTurnErrorTestCase {
	return []museTurnErrorTestCase{
		{
			name: "terminal failed with reason",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Run tool"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"terminal","terminal":"failed","reason":"Connection refused"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "Connection refused",
		},
		{
			name: "terminal failed with error bytes",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Run tool"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"terminal","terminal":"failed","error":{"message":"quota exceeded"}}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "quota exceeded",
		},
		{
			name: "error before cutoff ignored",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"terminal","terminal":"failed","reason":"Old error"}}}`,
			},
			since: time.Unix(1762510050, 0),
			want:  "",
		},
		{
			name: "terminal failed with fallback text",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Task"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"terminal","terminal":"failed"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "terminal failed",
		},
		{
			name: "payload reason error",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Task"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"reason":"disk quota exceeded"}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "disk quota exceeded",
		},
		{
			name: "payload error bytes",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Task"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"error":{"message":"network timeout"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "network timeout",
		},
		{
			name: "event reason without terminal",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Task"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"reason":"fatal error"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "fatal error",
		},
		{
			name: "record field payload error",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"record":{"kind":"terminal","reason":"record failure"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "record failure",
		},
		{
			name: "flat payload kind error",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"terminal","reason":"flat failure"}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "flat failure",
		},
		{
			name: "terminal failed with ev.Text",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Task"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"terminal","terminal":"failed","text":"text failure"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "text failure",
		},
		{
			name: "terminal completed with null error not reported as turn error",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Task"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"terminal","terminal":"completed","error":null}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
		{
			name: "terminal completed with text and null error not reported as turn error",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Task"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"terminal","terminal":"completed","text":"Task completed","error":null}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
	}
}

func museTurnErrorToolCompletedCases() []museTurnErrorTestCase {
	return []museTurnErrorTestCase{
		{
			name: "tool completed failed with output",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Task"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"tool_completed","status":"failed","output":"exit code 1"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "exit code 1",
		},
		{
			name: "tool completed is_error with result",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Task"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"tool_completed","is_error":true,"result":"command not found"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "command not found",
		},
		{
			name: "tool completed status error with content",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Task"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"tool_completed","status":"error","content":"permission denied"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "permission denied",
		},
		{
			name: "tool completed success ignored",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Task"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"tool_completed","status":"success","output":"OK"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
		{
			name: "invalid json and non-error lines ignored in error scan",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Task"}}}`,
				`{not-valid-json`,
				`{"payload_type":"other.event","payload":{}}`,
				`{"payload_type":"runtime.session","payload":"not-an-object"}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"assistant_message_committed","text":"Normal output"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
	}
}

func TestExtractMuseLinesForTurnError(t *testing.T) {
	t.Parallel()

	tests := append(museTurnErrorTerminalCases(), museTurnErrorToolCompletedCases()...)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := scanMuseLinesForTurnError(context.Background(), tc.lines, tc.since)
			if (err != nil) != tc.wantErr {
				t.Fatalf("scanMuseLinesForTurnError() err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("scanMuseLinesForTurnError() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestExtractMuseLinesForResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		lines []string
		since time.Time
		want  string
	}{
		{
			name: "assistant message committed extracted",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"What is 2+2?"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"assistant_message_committed","text":"4"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "4",
		},
		{
			name: "tool call requested placeholder ignored",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"assistant_message_committed","text":"[Tool Call Requested]: bash"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
		{
			name: "message role assistant extracted",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"type":"message","role":"user","content":"Hi"}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"type":"message","role":"assistant","content":[{"type":"text","text":"Hello there"}]}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "Hello there",
		},
		{
			name: "message role assistant with null error extracted",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"type":"message","role":"user","content":"Hi","error":null}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"type":"message","role":"assistant","content":"Done","error":null}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "Done",
		},
		{
			name: "tool call after assistant message cancels response",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"assistant_message_committed","text":"I will check"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510002000000,"payload":{"event":{"kind":"tool_call","name":"bash"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
		{
			name: "response before cutoff returns empty",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"assistant_message_committed","text":"Old reply"}}}`,
			},
			since: time.Unix(1762510050, 0),
			want:  "",
		},
		{
			name: "started user prompt stops backward scan",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"assistant_message_committed","text":"Old reply"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510002000000,"payload":{"event":{"kind":"started","prompt":"New non-ambient question"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
		{
			name: "user message stops backward scan",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"assistant_message_committed","text":"Old reply"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510002000000,"payload":{"type":"message","role":"user","content":"New question"}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
		{
			name: "model completed extracted as response",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Generate code"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"model_completed","text":"Final model output"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "Final model output",
		},
		{
			name: "subsecond fractional timestamp after cutoff extracted",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000.9,"payload":{"event":{"kind":"started","prompt":"Quick task"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510000.9,"payload":{"event":{"kind":"assistant_message_committed","text":"Subsecond result"}}}`,
			},
			since: time.Unix(1762510000, 500000000),
			want:  "Subsecond result",
		},
		{
			name: "subsecond fractional timestamp before cutoff ignored",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000.4,"payload":{"event":{"kind":"assistant_message_committed","text":"Old subsecond result"}}}`,
			},
			since: time.Unix(1762510000, 500000000),
			want:  "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := scanMuseLinesForResponse(context.Background(), tc.lines, tc.since)
			if got != tc.want {
				t.Fatalf("scanMuseLinesForResponse() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseMuseTimestampAndHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		raw       any
		wantZero  bool
		wantEpoch int64
	}{
		{name: "nil input", raw: nil, wantZero: true},
		{name: "empty string", raw: "", wantZero: true},
		{name: "invalid string", raw: "not-a-time", wantZero: true},
		{name: "rfc3339nano string", raw: "2026-11-07T10:00:00.123456789Z", wantEpoch: 1794045600},
		{name: "rfc3339 string", raw: "2026-11-07T10:00:00Z", wantEpoch: 1794045600},
		{name: "string number micro", raw: "1762510000000000", wantEpoch: 1762510000},
		{name: "string number nano", raw: "1762510000000000000", wantEpoch: 1762510000},
		{name: "string number seconds", raw: "1762510000", wantEpoch: 1762510000},
		{name: "string number non-positive", raw: "-10", wantZero: true},
		{name: "string number float", raw: "1762510000.5", wantEpoch: 1762510000},
		{name: "float64 nano", raw: float64(1762510000000000000), wantEpoch: 1762510000},
		{name: "float64 micro", raw: float64(1762510000000000), wantEpoch: 1762510000},
		{name: "float64 micro round up", raw: 1762510000999999.99, wantEpoch: 1762510001},
		{name: "float64 seconds with fraction", raw: float64(1762510000.9), wantEpoch: 1762510000},
		{name: "float64 seconds round up", raw: 1762510000.999999999, wantEpoch: 1762510001},
		{name: "float64 non-positive", raw: float64(0), wantZero: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ts := parseMuseTimestamp(tc.raw)
			if tc.wantZero && !ts.IsZero() {
				t.Fatalf("parseMuseTimestamp(%v) = %v, want zero time", tc.raw, ts)
			}
			if !tc.wantZero && ts.Unix() != tc.wantEpoch {
				t.Fatalf("parseMuseTimestamp(%v) = %v (unix %d), want %d", tc.raw, ts, ts.Unix(), tc.wantEpoch)
			}
		})
	}
}

func TestExtractMuseTimestampAndEntryHelpers(t *testing.T) {
	t.Parallel()

	t.Run("extractMuseTimestamp fields", func(t *testing.T) {
		t.Parallel()
		e1 := museTranscriptEntry{RecordedAt: "2026-11-07T10:00:00Z"}
		if extractMuseTimestamp(e1).IsZero() {
			t.Fatalf("expected non-zero for RecordedAt")
		}
		e2 := museTranscriptEntry{Timestamp: "2026-11-07T10:00:00Z"}
		if extractMuseTimestamp(e2).IsZero() {
			t.Fatalf("expected non-zero for Timestamp")
		}
		e3 := museTranscriptEntry{CreatedAt: "2026-11-07T10:00:00Z"}
		if extractMuseTimestamp(e3).IsZero() {
			t.Fatalf("expected non-zero for CreatedAt")
		}
		e4 := museTranscriptEntry{}
		if !extractMuseTimestamp(e4).IsZero() {
			t.Fatalf("expected zero for empty entry")
		}
	})

	t.Run("canceled context in scanMuseLinesForResponse", func(t *testing.T) {
		t.Parallel()
		line := `{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"assistant_message_committed","text":"Valid answer"}}}`
		if got := scanMuseLinesForResponse(context.Background(), []string{line}, time.Unix(1762509999, 0)); got != "Valid answer" {
			t.Fatalf("expected valid answer without cancellation, got %q", got)
		}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if got := scanMuseLinesForResponse(ctx, []string{line}, time.Unix(1762509999, 0)); got != "" {
			t.Fatalf("expected empty for canceled context, got %q", got)
		}
	})

	t.Run("isMuseStepLine invalid lines", func(t *testing.T) {
		t.Parallel()
		if isMuseStepLine("invalid-json") {
			t.Fatalf("expected false")
		}
		if isMuseStepLine(`{"payload_type":"other"}`) {
			t.Fatalf("expected false")
		}
		if isMuseStepLine(`{"payload_type":"runtime.session","payload":{}}`) {
			t.Fatalf("expected false for empty payload")
		}
	})

	t.Run("isMuseStepBeforeEntry variants", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		if isMuseStepBeforeEntry(museTranscriptEntry{}, time.Time{}) {
			t.Fatalf("expected false for zero since")
		}
		past := museTranscriptEntry{Timestamp: now.Add(-time.Hour).Format(time.RFC3339Nano)}
		if !isMuseStepBeforeEntry(past, now) {
			t.Fatalf("expected true for past entry")
		}
		future := museTranscriptEntry{Timestamp: now.Add(time.Hour).Format(time.RFC3339Nano)}
		if isMuseStepBeforeEntry(future, now) {
			t.Fatalf("expected false for future entry")
		}
		invalid := museTranscriptEntry{Timestamp: "not-a-timestamp"}
		if isMuseStepBeforeEntry(invalid, now) {
			t.Fatalf("expected false for invalid timestamp")
		}
	})
}

func TestExtractResponseFromMuseTranscriptFile(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()

	pNonExistent := filepath.Join(tmp, "does-not-exist.jsonl")
	if res := extractResponseFromMuseTranscriptFile(context.Background(), pNonExistent, time.Time{}); res != "" {
		t.Fatalf("expected empty for non-existent file, got %q", res)
	}

	pValid := filepath.Join(tmp, "valid.jsonl")
	content := strings.Join([]string{
		`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"event":{"kind":"started","prompt":"Task"}}}`,
		`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"event":{"kind":"assistant_message_committed","text":"Here is the result."}}}`,
	}, "\n") + "\n"
	_ = os.WriteFile(pValid, []byte(content), 0o644)

	res := extractResponseFromMuseTranscriptFile(context.Background(), pValid, time.Unix(1762509999, 0))
	if res != "Here is the result." {
		t.Fatalf("extractResponseFromMuseTranscriptFile() = %q, want %q", res, "Here is the result.")
	}
}

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
	name       string
	isNilMgr   bool
	sessionID  string
	location   string
	content    string
	zeroByte   bool
	age        time.Duration
	wantExists bool
	wantPaths  int
	wantSize   int64
	wantSteps  int
	wantTurns  int
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
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"Initial prompt"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510002000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"Done"}}}`,
			}, "\n") + "\n",
			age:        10 * time.Minute,
			wantExists: true,
			wantPaths:  1,
			wantSteps:  1,
			wantTurns:  1,
		},
		{
			name:      "data runtime discord session.jsonl direct",
			sessionID: "sess-muse-runtime",
			location:  "data_runtime_muse",
			content: strings.Join([]string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"User msg"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"Assistant msg"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510002000000,"payload":{"kind":"run","event":{"kind":"assistant_tool_calls_committed","tool_calls":[{"name":"bash","call_id":"call_1"}]}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510003000000,"payload":{"kind":"run","event":{"kind":"tool_result_batch_committed","results":[{"call_id":"call_1","output":"ok"}]}}}`,
			}, "\n") + "\n",
			wantExists: true,
			wantPaths:  1,
			wantSteps:  3,
			wantTurns:  1,
		},
		{
			name:      "home muse session.jsonl",
			sessionID: "sess-muse-home",
			location:  "home_muse",
			content: strings.Join([]string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"[Chat Alex]: ambient message"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"model output"}}}`,
			}, "\n") + "\n",
			wantExists: true,
			wantPaths:  1,
			wantSteps:  1,
			wantTurns:  0,
		},
		{
			name:       "zero byte muse session",
			sessionID:  "sess-muse-zero",
			location:   "direct_muse",
			zeroByte:   true,
			wantExists: false,
			wantPaths:  1,
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

func museTurnErrorCases() []museTurnErrorTestCase {
	return []museTurnErrorTestCase{
		{
			name: "terminal failed with reason",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"Run tool"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"terminal","terminal":"failed","reason":"Connection refused"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "Connection refused",
		},
		{
			name: "terminal failed with default fallback",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"Task"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"terminal","terminal":"failed"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "terminal failed",
		},
		{
			name: "error before cutoff ignored",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"run","event":{"kind":"terminal","terminal":"failed","reason":"Old error"}}}`,
			},
			since: time.Unix(1762510050, 0),
			want:  "",
		},
		{
			name: "terminal completed ignored",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"Task"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"terminal","terminal":"completed","reason":"all good"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
		{
			name: "non-run kind ignored",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"init","event":{"kind":"terminal","terminal":"failed","reason":"ignored"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
		{
			name: "invalid json and non-session lines ignored in error scan",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"Task"}}}`,
				`{not-valid-json`,
				`{"payload_type":"other.event","payload":{"kind":"run"}}`,
				`{"payload_type":"runtime.session","payload":"not-an-object"}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"Normal output"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
	}
}

func TestExtractMuseLinesForTurnError(t *testing.T) {
	t.Parallel()

	for _, tc := range museTurnErrorCases() {
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
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"What is 2+2?"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"4"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "4",
		},
		{
			name: "empty assistant message text ignored",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":""}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
		{
			name: "assistant tool calls committed cancels response",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"I will check"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510002000000,"payload":{"kind":"run","event":{"kind":"assistant_tool_calls_committed","tool_calls":[{"name":"bash"}]}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
		{
			name: "tool result batch committed cancels response",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"I will check"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510002000000,"payload":{"kind":"run","event":{"kind":"tool_result_batch_committed","results":[{"call_id":"call_1"}]}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
		{
			name: "response before cutoff returns empty",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"Old reply"}}}`,
			},
			since: time.Unix(1762510050, 0),
			want:  "",
		},
		{
			name: "started user prompt stops backward scan",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"Old reply"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510002000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"New non-ambient question"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
		{
			name: "started ambient prompt does not stop backward scan",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"Valid reply"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510002000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"[Chat Alex]: ambient ping"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "Valid reply",
		},
		{
			name: "subsecond fractional microsecond after cutoff extracted",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000900000,"payload":{"kind":"run","event":{"kind":"started","prompt":"Quick task"}}}`,
				`{"payload_type":"runtime.session","recorded_at":1762510000900000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"Subsecond result"}}}`,
			},
			since: time.Unix(1762510000, 500000000),
			want:  "Subsecond result",
		},
		{
			name: "subsecond microsecond before cutoff ignored",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510000400000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"Old subsecond result"}}}`,
			},
			since: time.Unix(1762510000, 500000000),
			want:  "",
		},
		{
			name: "non-run kind ignored in response scan",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"meta","event":{"kind":"assistant_message_committed","text":"Ignored"}}}`,
			},
			since: time.Unix(1762509999, 0),
			want:  "",
		},
		{
			name: "invalid json and non-session entries ignored in response scan",
			lines: []string{
				`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"Good reply"}}}`,
				`{"payload_type":"other.type","payload":{"kind":"run"}}`,
				`{invalid`,
			},
			since: time.Unix(1762509999, 0),
			want:  "Good reply",
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

func TestMuseSession_CanceledContext(t *testing.T) {
	t.Parallel()
	line := `{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"Valid answer"}}}`
	if got := scanMuseLinesForResponse(context.Background(), []string{line}, time.Unix(1762509999, 0)); got != "Valid answer" {
		t.Fatalf("expected valid answer without cancellation, got %q", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := scanMuseLinesForResponse(ctx, []string{line}, time.Unix(1762509999, 0)); got != "" {
		t.Fatalf("expected empty for canceled context, got %q", got)
	}
}

func TestMuseSessionLinePredicates(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		raw            string
		wantStep       bool
		wantNonAmbient bool
	}{
		{name: "invalid json", raw: "invalid-json"},
		{name: "other payload type", raw: `{"payload_type":"other"}`},
		{name: "non-run kind", raw: `{"payload_type":"runtime.session","payload":{"kind":"init"}}`},
		{name: "unknown event kind", raw: `{"payload_type":"runtime.session","payload":{"kind":"run","event":{"kind":"unknown"}}}`},
		{name: "empty prompt", raw: `{"payload_type":"runtime.session","payload":{"kind":"run","event":{"kind":"started","prompt":""}}}`},
		{name: "ambient prompt", raw: `{"payload_type":"runtime.session","payload":{"kind":"run","event":{"kind":"started","prompt":"[Chat Alex]: ambient"}}}`},
		{name: "regular prompt", raw: `{"payload_type":"runtime.session","payload":{"kind":"run","event":{"kind":"started","prompt":"Regular user question"}}}`, wantNonAmbient: true},
		{name: "assistant message committed", raw: `{"payload_type":"runtime.session","payload":{"kind":"run","event":{"kind":"assistant_message_committed"}}}`, wantStep: true},
		{name: "assistant tool calls committed", raw: `{"payload_type":"runtime.session","payload":{"kind":"run","event":{"kind":"assistant_tool_calls_committed"}}}`, wantStep: true},
		{name: "tool result batch committed", raw: `{"payload_type":"runtime.session","payload":{"kind":"run","event":{"kind":"tool_result_batch_committed"}}}`, wantStep: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isMuseStepLine(tc.raw); got != tc.wantStep {
				t.Errorf("isMuseStepLine(%s) = %v, want %v", tc.name, got, tc.wantStep)
			}
			if got := isNonAmbientMuseUserLine(tc.raw); got != tc.wantNonAmbient {
				t.Errorf("isNonAmbientMuseUserLine(%s) = %v, want %v", tc.name, got, tc.wantNonAmbient)
			}
		})
	}
}

func TestMuseSession_IsMuseStepBeforeEntry(t *testing.T) {
	t.Parallel()
	now := time.Now()
	tests := []struct {
		name  string
		entry museTranscriptEntry
		since time.Time
		want  bool
	}{
		{name: "zero since", entry: museTranscriptEntry{}, since: time.Time{}, want: false},
		{name: "past entry", entry: museTranscriptEntry{RecordedAt: now.Add(-time.Hour).UnixMicro()}, since: now, want: true},
		{name: "future entry", entry: museTranscriptEntry{RecordedAt: now.Add(time.Hour).UnixMicro()}, since: now, want: false},
		{name: "zero recorded_at", entry: museTranscriptEntry{RecordedAt: 0}, since: now, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isMuseStepBeforeEntry(tc.entry, tc.since); got != tc.want {
				t.Fatalf("isMuseStepBeforeEntry(%+v, %v) = %v, want %v", tc.entry, tc.since, got, tc.want)
			}
		})
	}
}

func TestExtractResponseFromMuseTranscriptFile(t *testing.T) {
	t.Parallel()
	tmp := t.TempDir()

	pNonExistent := filepath.Join(tmp, "does-not-exist.jsonl")
	if res := extractResponseFromMuseTranscriptFile(context.Background(), pNonExistent, time.Time{}); res != "" {
		t.Fatalf("expected empty for non-existent file, got %q", res)
	}

	pEmpty := filepath.Join(tmp, "empty.jsonl")
	_ = os.WriteFile(pEmpty, []byte(""), 0o644)
	if res := extractResponseFromMuseTranscriptFile(context.Background(), pEmpty, time.Time{}); res != "" {
		t.Fatalf("expected empty for empty file, got %q", res)
	}

	pValid := filepath.Join(tmp, "valid.jsonl")
	content := strings.Join([]string{
		`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"Task"}}}`,
		`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"Here is the result."}}}`,
	}, "\n") + "\n"
	_ = os.WriteFile(pValid, []byte(content), 0o644)

	res := extractResponseFromMuseTranscriptFile(context.Background(), pValid, time.Unix(1762509999, 0))
	if res != "Here is the result." {
		t.Fatalf("extractResponseFromMuseTranscriptFile() = %q, want %q", res, "Here is the result.")
	}
}

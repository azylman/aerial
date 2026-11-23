package session

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
				`{"payload_type":"runtime.session","recorded_at":1762510002000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"[Chat alpha]: ambient ping"}}}`,
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
		{name: "ambient prompt", raw: `{"payload_type":"runtime.session","payload":{"kind":"run","event":{"kind":"started","prompt":"[Chat alpha]: ambient"}}}`},
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
	if err := os.WriteFile(pEmpty, []byte(""), 0o644); err != nil {
		t.Fatalf("WriteFile(empty): %v", err)
	}
	if res := extractResponseFromMuseTranscriptFile(context.Background(), pEmpty, time.Time{}); res != "" {
		t.Fatalf("expected empty for empty file, got %q", res)
	}

	pValid := filepath.Join(tmp, "valid.jsonl")
	content := strings.Join([]string{
		`{"payload_type":"runtime.session","recorded_at":1762510000000000,"payload":{"kind":"run","event":{"kind":"started","prompt":"Task"}}}`,
		`{"payload_type":"runtime.session","recorded_at":1762510001000000,"payload":{"kind":"run","event":{"kind":"assistant_message_committed","text":"Here is the result."}}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(pValid, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile(valid): %v", err)
	}

	res := extractResponseFromMuseTranscriptFile(context.Background(), pValid, time.Unix(1762509999, 0))
	if res != "Here is the result." {
		t.Fatalf("extractResponseFromMuseTranscriptFile() = %q, want %q", res, "Here is the result.")
	}
}

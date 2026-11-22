package transcriptwire

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func testBatchBase() TranscriptStepBatch {
	return TranscriptStepBatch{
		Stack:       "aerial",
		SessionID:   "sess-1",
		ThreadID:    "thread-1",
		Engine:      "eri",
		Source:      "turn-completed",
		FileMtime:   time.Unix(1234567890, 123).UTC(),
		Reset:       true,
		Step0Prompt: "summarize this",
	}
}

// sizedBatchSteps returns n model steps indexed from base, each with size bytes of content.
func sizedBatchSteps(n, base, size int) []TranscriptStep {
	steps := make([]TranscriptStep, 0, n)
	for i := range n {
		steps = append(steps, TranscriptStep{StepIndex: base + i, Source: "model", Content: strings.Repeat("y", size)})
	}
	return steps
}

// splitBatchWant is the expected range, step count and final flag of one split batch.
type splitBatchWant struct {
	first, last, count int
	final              bool
}

// assertSplitBatch checks batch i against want: the envelope survives every split, reset and the
// step-0 prompt ride the first batch only, no step keeps thinking, and steps fit the byte budget.
func assertSplitBatch(t *testing.T, i int, b, base TranscriptStepBatch, want splitBatchWant, reset, prompt bool) {
	t.Helper()
	if got := (splitBatchWant{b.FirstStep, b.LastStep, len(b.Steps), b.Final}); got != want {
		t.Errorf("batch %d = %+v, want %+v", i, got, want)
	}
	if b.Stack != base.Stack || b.SessionID != base.SessionID || !b.FileMtime.Equal(base.FileMtime) {
		t.Errorf("batch %d lost envelope: %+v", i, b)
	}
	if b.Reset != (i == 0 && reset) || (b.Step0Prompt != "") != (i == 0 && prompt) {
		t.Errorf("batch %d reset=%v prompt=%v, want first batch only reset=%v prompt=%v", i, b.Reset, b.Step0Prompt != "", reset, prompt)
	}
	size := 0
	for _, s := range b.Steps {
		size += stepBytes(s)
		if s.Thinking != "" {
			t.Errorf("batch %d step %d kept %d chars of thinking", i, s.StepIndex, len(s.Thinking))
		}
	}
	if size > MaxBatchBytes {
		t.Errorf("batch %d encoded steps = %d bytes, want within %d", i, size, MaxBatchBytes)
	}
}

func TestSplitStepBatches(t *testing.T) {
	t.Parallel()

	big := strings.Repeat("x", MaxBatchBytes+1024)
	base := testBatchBase()
	empty := TranscriptStepBatch{Stack: "aerial", SessionID: "sess-1", BaseStep: 7, FileMtime: base.FileMtime}
	hugeArgs := []TranscriptToolCall{{Name: "t", Args: json.RawMessage(`"` + strings.Repeat("z", MaxBatchBytes) + `"`)}}
	badArgs := []TranscriptToolCall{{Name: "t", Args: json.RawMessage("{invalid")}}
	one := func(i int) []splitBatchWant { return []splitBatchWant{{i, i, 1, true}} }

	// wantSuffix0 and wantCalls0 describe step 0 of batch 0 after clipping/dropping.
	tests := []struct {
		name        string
		base        TranscriptStepBatch
		steps       []TranscriptStep
		want        []splitBatchWant
		wantReset   bool
		wantPrompt  bool
		wantSuffix0 string
		wantCalls0  int
	}{
		{"empty yields one empty final batch", empty, nil, []splitBatchWant{{7, 7, 0, true}}, false, false, "", 0},
		{"single batch keeps envelope and marks final", base, sizedBatchSteps(3, 5, 7), []splitBatchWant{{5, 7, 3, true}}, true, true, "yyyyyyy", 0},
		{
			"split chain carries contiguous ranges with final last", base, sizedBatchSteps(250, 0, 7),
			[]splitBatchWant{{0, 99, 100, false}, {100, 199, 100, false}, {200, 249, 50, true}},
			true, true, "yyyyyyy", 0,
		},
		{"oversize step clipped into one final batch", base, []TranscriptStep{{StepIndex: 3, Source: "model", Content: big}}, one(3), true, true, "...[clipped]", 0},
		{
			"byte budget splits before count limit", base, sizedBatchSteps(6, 10, 100*1024),
			[]splitBatchWant{{10, 14, 5, false}, {15, 15, 1, true}},
			true, true, "y", 0,
		},
		{"thinking dropped to fit budget", base, []TranscriptStep{{StepIndex: 4, Source: "model", Content: "small", Thinking: big}}, one(4), true, true, "small", 0},
		{
			"tool calls dropped to fit budget", base,
			[]TranscriptStep{{StepIndex: 6, Source: "model", Content: "small", ToolCalls: hugeArgs}},
			one(6), true, true, "small", 0,
		},
		{
			"unmarshable args never error", base,
			[]TranscriptStep{{StepIndex: 8, Source: "model", Content: "small", ToolCalls: badArgs}},
			one(8), true, true, "small", 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := SplitStepBatches(tc.base, tc.steps)
			if len(got) != len(tc.want) {
				t.Fatalf("batch count = %d, want %d", len(got), len(tc.want))
			}
			for i, b := range got {
				assertSplitBatch(t, i, b, tc.base, tc.want[i], tc.wantReset, tc.wantPrompt)
			}
			if len(tc.steps) == 0 {
				return
			}
			st := got[0].Steps[0]
			if !strings.HasSuffix(st.Content, tc.wantSuffix0) || len(st.ToolCalls) != tc.wantCalls0 {
				t.Errorf("step 0 content %.40q... calls %d, want suffix %q calls %d", st.Content, len(st.ToolCalls), tc.wantSuffix0, tc.wantCalls0)
			}
		})
	}
}

func TestMsgID(t *testing.T) {
	t.Parallel()

	base := testBatchBase()
	base.FirstStep = 0
	base.LastStep = 9
	later := base
	later.FileMtime = base.FileMtime.Add(time.Second)

	tests := []struct {
		name      string
		first     TranscriptStepBatch
		second    TranscriptStepBatch
		wantFirst string
		wantEqual bool
	}{
		{
			name:      "same batch reproduces id folding file mtime nanos",
			first:     base,
			second:    base,
			wantFirst: "transcript-batch:aerial:sess-1:0:9:1234567890000000123",
			wantEqual: true,
		},
		{
			name:      "rewritten file mtime changes id",
			first:     base,
			second:    later,
			wantFirst: "transcript-batch:aerial:sess-1:0:9:1234567890000000123",
			wantEqual: false,
		},
		{
			name:      "empty agent and stack defaults to aerial in msg id",
			first:     TranscriptStepBatch{SessionID: "sess-1", FirstStep: 0, LastStep: 9, FileMtime: base.FileMtime},
			second:    TranscriptStepBatch{SessionID: "sess-1", FirstStep: 0, LastStep: 9, FileMtime: base.FileMtime},
			wantFirst: "transcript-batch:aerial:sess-1:0:9:1234567890000000123",
			wantEqual: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := MsgID(tc.first)
			if got != tc.wantFirst {
				t.Errorf("MsgID = %q, want %q", got, tc.wantFirst)
			}
			again := MsgID(tc.second)
			if (again == got) != tc.wantEqual {
				t.Errorf("second MsgID %q equal = %v, want equal = %v", again, again == got, tc.wantEqual)
			}
		})
	}
}

func TestEffectiveAgent_TableDriven(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		batch     TranscriptStepBatch
		wantAgent string
	}{
		{"agent specified wins over stack", TranscriptStepBatch{Agent: "agent-alpha", Stack: "aerial"}, "agent-alpha"},
		{"agent empty falls back to stack", TranscriptStepBatch{Agent: "", Stack: "agent-beta"}, "agent-beta"},
		{"both empty returns empty", TranscriptStepBatch{Agent: "", Stack: ""}, ""},
		{"whitespace trimmed correctly", TranscriptStepBatch{Agent: "  agent-beta  ", Stack: "aerial"}, "agent-beta"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.batch.EffectiveAgent(); got != tc.wantAgent {
				t.Errorf("EffectiveAgent() = %q, want %q", got, tc.wantAgent)
			}
		})
	}
}

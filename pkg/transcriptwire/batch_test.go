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

func testBatchSteps(n, base int) []TranscriptStep {
	steps := make([]TranscriptStep, 0, n)
	for i := range n {
		steps = append(steps, TranscriptStep{
			StepIndex: base + i,
			Source:    "model",
			Content:   "content",
		})
	}
	return steps
}

func sizedBatchSteps(n, base, size int) []TranscriptStep {
	steps := make([]TranscriptStep, 0, n)
	for i := range n {
		steps = append(steps, TranscriptStep{
			StepIndex: base + i,
			Source:    "model",
			Content:   strings.Repeat("y", size),
		})
	}
	return steps
}

type splitBatchTestCase struct {
	name                 string
	base                 TranscriptStepBatch
	steps                []TranscriptStep
	wantBatches          int
	wantFirsts           []int
	wantLasts            []int
	wantCounts           []int
	wantFinals           []bool
	wantResetFirst       bool
	wantPromptFirst      bool
	wantClipped          bool
	wantThinkingDropped  bool
	wantToolCallsDropped bool
}

func checkSplitBatch(t *testing.T, tc splitBatchTestCase, i int, b TranscriptStepBatch) {
	t.Helper()
	if b.FirstStep != tc.wantFirsts[i] || b.LastStep != tc.wantLasts[i] {
		t.Errorf("batch %d range = [%d,%d], want [%d,%d]",
			i, b.FirstStep, b.LastStep, tc.wantFirsts[i], tc.wantLasts[i])
	}
	if len(b.Steps) != tc.wantCounts[i] {
		t.Errorf("batch %d step count = %d, want %d", i, len(b.Steps), tc.wantCounts[i])
	}
	if b.Final != tc.wantFinals[i] {
		t.Errorf("batch %d final = %v, want %v", i, b.Final, tc.wantFinals[i])
	}
	if b.Stack != tc.base.Stack || b.SessionID != tc.base.SessionID {
		t.Errorf("batch %d lost envelope: %+v", i, b)
	}
	if !b.FileMtime.Equal(tc.base.FileMtime) {
		t.Errorf("batch %d file mtime = %v, want %v", i, b.FileMtime, tc.base.FileMtime)
	}
	first := i == 0
	if first && b.Reset != tc.wantResetFirst {
		t.Errorf("first batch reset = %v, want %v", b.Reset, tc.wantResetFirst)
	}
	if first && (b.Step0Prompt != "") != tc.wantPromptFirst {
		t.Errorf("first batch step0 prompt present = %v, want %v", b.Step0Prompt != "", tc.wantPromptFirst)
	}
	if !first && b.Reset {
		t.Errorf("batch %d carries reset, want reset on first batch only", i)
	}
	if !first && b.Step0Prompt != "" {
		t.Errorf("batch %d carries step0 prompt, want prompt on first batch only", i)
	}
	size := 0
	for _, s := range b.Steps {
		size += stepBytes(s)
	}
	if size > MaxBatchBytes {
		t.Errorf("batch %d encoded steps = %d bytes, want within %d", i, size, MaxBatchBytes)
	}
}

func TestSplitStepBatches(t *testing.T) {
	t.Parallel()

	big := strings.Repeat("x", MaxBatchBytes+1024)
	base := testBatchBase()

	tests := []splitBatchTestCase{
		{
			name:            "empty yields one empty final batch",
			base:            TranscriptStepBatch{Stack: "aerial", SessionID: "sess-1", BaseStep: 7, FileMtime: base.FileMtime},
			steps:           nil,
			wantBatches:     1,
			wantFirsts:      []int{7},
			wantLasts:       []int{7},
			wantCounts:      []int{0},
			wantFinals:      []bool{true},
			wantResetFirst:  false,
			wantPromptFirst: false,
			wantClipped:     false,
		},
		{
			name:            "single batch keeps envelope and marks final",
			base:            base,
			steps:           testBatchSteps(3, 5),
			wantBatches:     1,
			wantFirsts:      []int{5},
			wantLasts:       []int{7},
			wantCounts:      []int{3},
			wantFinals:      []bool{true},
			wantResetFirst:  true,
			wantPromptFirst: true,
			wantClipped:     false,
		},
		{
			name:            "split chain carries contiguous ranges with final last",
			base:            base,
			steps:           testBatchSteps(250, 0),
			wantBatches:     3,
			wantFirsts:      []int{0, 100, 200},
			wantLasts:       []int{99, 199, 249},
			wantCounts:      []int{100, 100, 50},
			wantFinals:      []bool{false, false, true},
			wantResetFirst:  true,
			wantPromptFirst: true,
			wantClipped:     false,
		},
		{
			name:            "oversize step clipped into one final batch",
			base:            base,
			steps:           []TranscriptStep{{StepIndex: 3, Source: "model", Content: big}},
			wantBatches:     1,
			wantFirsts:      []int{3},
			wantLasts:       []int{3},
			wantCounts:      []int{1},
			wantFinals:      []bool{true},
			wantResetFirst:  true,
			wantPromptFirst: true,
			wantClipped:     true,
		},
		{
			name:            "byte budget splits before count limit",
			base:            base,
			steps:           sizedBatchSteps(6, 10, 100*1024),
			wantBatches:     2,
			wantFirsts:      []int{10, 15},
			wantLasts:       []int{14, 15},
			wantCounts:      []int{5, 1},
			wantFinals:      []bool{false, true},
			wantResetFirst:  true,
			wantPromptFirst: true,
		},
		{
			name:                "thinking dropped to fit budget",
			base:                base,
			steps:               []TranscriptStep{{StepIndex: 4, Source: "model", Content: "small", Thinking: big}},
			wantBatches:         1,
			wantFirsts:          []int{4},
			wantLasts:           []int{4},
			wantCounts:          []int{1},
			wantFinals:          []bool{true},
			wantResetFirst:      true,
			wantPromptFirst:     true,
			wantThinkingDropped: true,
		},
		{
			name:                 "tool calls dropped to fit budget",
			base:                 base,
			steps:                []TranscriptStep{{StepIndex: 6, Source: "model", Content: "small", ToolCalls: []TranscriptToolCall{{Name: "t", Args: json.RawMessage(`"` + strings.Repeat("z", MaxBatchBytes) + `"`)}}}},
			wantBatches:          1,
			wantFirsts:           []int{6},
			wantLasts:            []int{6},
			wantCounts:           []int{1},
			wantFinals:           []bool{true},
			wantResetFirst:       true,
			wantPromptFirst:      true,
			wantToolCallsDropped: true,
		},
		{
			name:            "unmarshable args never error",
			base:            base,
			steps:           []TranscriptStep{{StepIndex: 8, Source: "model", Content: "small", ToolCalls: []TranscriptToolCall{{Name: "t", Args: json.RawMessage("{invalid")}}}},
			wantBatches:     1,
			wantFirsts:      []int{8},
			wantLasts:       []int{8},
			wantCounts:      []int{1},
			wantFinals:      []bool{true},
			wantResetFirst:  true,
			wantPromptFirst: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSplitBatchCase(t, tc)
		})
	}
}

func runSplitBatchCase(t *testing.T, tc splitBatchTestCase) {
	t.Helper()
	got := SplitStepBatches(tc.base, tc.steps)
	if len(got) != tc.wantBatches {
		t.Fatalf("batch count = %d, want %d", len(got), tc.wantBatches)
	}
	for i, b := range got {
		checkSplitBatch(t, tc, i, b)
	}
	checkSplitBatchExtras(t, tc, got)
}

func checkSplitBatchExtras(t *testing.T, tc splitBatchTestCase, got []TranscriptStepBatch) {
	t.Helper()
	checkClippedContent(t, tc, got)
	checkThinkingDropped(t, tc, got)
	checkToolCallsDropped(t, tc, got)
}

func checkClippedContent(t *testing.T, tc splitBatchTestCase, got []TranscriptStepBatch) {
	t.Helper()
	if !tc.wantClipped {
		return
	}
	content := got[0].Steps[0].Content
	if !strings.HasSuffix(content, clipMarker) {
		t.Errorf("clipped step content %q does not end with clip marker", content)
	}
	if stepBytes(got[0].Steps[0]) > MaxBatchBytes {
		t.Errorf("clipped step still exceeds %d bytes", MaxBatchBytes)
	}
}

func checkThinkingDropped(t *testing.T, tc splitBatchTestCase, got []TranscriptStepBatch) {
	t.Helper()
	if !tc.wantThinkingDropped {
		return
	}
	st := got[0].Steps[0]
	if st.Thinking != "" {
		t.Errorf("thinking not dropped, still %d chars", len(st.Thinking))
	}
	if st.Content != "small" {
		t.Errorf("content = %q, want intact small content", st.Content)
	}
}

func checkToolCallsDropped(t *testing.T, tc splitBatchTestCase, got []TranscriptStepBatch) {
	t.Helper()
	if !tc.wantToolCallsDropped {
		return
	}
	st := got[0].Steps[0]
	if st.ToolCalls != nil {
		t.Errorf("tool calls not dropped, still %d calls", len(st.ToolCalls))
	}
	if st.Content != "small" {
		t.Errorf("content = %q, want intact small content", st.Content)
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

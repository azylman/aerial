package transcript

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/transcriptwire"
)

func testBatch(session string, first, last int, final bool) transcriptwire.TranscriptStepBatch {
	return transcriptwire.TranscriptStepBatch{
		Stack:     "aerial",
		SessionID: session,
		FirstStep: first,
		LastStep:  last,
		Final:     final,
		FileMtime: time.Unix(1000, 0).UTC(),
		Steps:     []transcriptwire.TranscriptStep{{StepIndex: first}},
	}
}

// recordingPublish is a declarative publish fake: it records batches in call
// order and fails the call at index failCall (-1 never fails).
type recordingPublish struct {
	failCall  int
	published []transcriptwire.TranscriptStepBatch
}

func (r *recordingPublish) publish(_ context.Context, b transcriptwire.TranscriptStepBatch) error {
	r.published = append(r.published, b)
	if len(r.published)-1 == r.failCall {
		return errors.New("publish boom")
	}
	return nil
}

func publishedFirstSteps(batches []transcriptwire.TranscriptStepBatch) []int {
	if len(batches) == 0 {
		return nil
	}
	got := make([]int, 0, len(batches))
	for _, b := range batches {
		got = append(got, b.FirstStep)
	}
	return got
}

func assertFirstStepsEqual(t *testing.T, got []transcriptwire.TranscriptStepBatch, want []int) {
	t.Helper()
	steps := publishedFirstSteps(got)
	if len(steps) != len(want) {
		t.Fatalf("published %v, want order %v", steps, want)
	}
	for i := range steps {
		if steps[i] != want[i] {
			t.Fatalf("published %v, want order %v", steps, want)
		}
	}
}

func countPending(p *Publisher) int {
	pending := 0
	for _, s := range p.PendingSessions() {
		pending += p.PendingCount(s)
	}
	return pending
}

func TestPublisher_EnqueueCoalesces(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		enqueue  []transcriptwire.TranscriptStepBatch
		session  string
		want     int
		sessions int
	}{
		{name: "empty publisher", session: "s1", want: 0, sessions: 0},
		{name: "single batch", enqueue: []transcriptwire.TranscriptStepBatch{testBatch("s1", 0, 0, true)}, session: "s1", want: 1, sessions: 1},
		{name: "coalesce same session", enqueue: []transcriptwire.TranscriptStepBatch{testBatch("s1", 0, 0, false), testBatch("s1", 1, 1, true)}, session: "s1", want: 2, sessions: 1},
		{name: "other session isolated", enqueue: []transcriptwire.TranscriptStepBatch{testBatch("s1", 0, 0, true), testBatch("s2", 0, 0, true)}, session: "s2", want: 1, sessions: 2},
		{name: "blank session dropped", enqueue: []transcriptwire.TranscriptStepBatch{testBatch("   ", 0, 0, true)}, session: "s1", want: 0, sessions: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := NewPublisher("aerial", nil)
			p.Enqueue(tc.enqueue...)
			if got := p.PendingCount(tc.session); got != tc.want {
				t.Errorf("PendingCount(%q) = %d, want %d", tc.session, got, tc.want)
			}
			if got := len(p.PendingSessions()); got != tc.sessions {
				t.Errorf("PendingSessions len = %d, want %d", got, tc.sessions)
			}
		})
	}
}

func TestPublisher_DrainStrictOrderAndChainAbandon(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		batches     []transcriptwire.TranscriptStepBatch
		failCall    int
		nilPublish  bool
		wantOrder   []int
		wantPending int
		wantErr     bool
	}{
		{name: "empty drain no error", failCall: -1, wantOrder: nil, wantPending: 0},
		{name: "strict order", batches: []transcriptwire.TranscriptStepBatch{testBatch("s1", 0, 0, false), testBatch("s1", 1, 1, false), testBatch("s1", 2, 2, true)}, failCall: -1, wantOrder: []int{0, 1, 2}, wantPending: 0},
		{name: "abandon chain on first failure", batches: []transcriptwire.TranscriptStepBatch{testBatch("s1", 0, 0, false), testBatch("s1", 1, 1, false), testBatch("s1", 2, 2, true)}, failCall: 1, wantOrder: []int{0, 1}, wantPending: 0, wantErr: true},
		{name: "failure isolates to session", batches: []transcriptwire.TranscriptStepBatch{testBatch("s1", 0, 0, true), testBatch("s2", 0, 0, false), testBatch("s2", 1, 1, true)}, failCall: 0, wantOrder: []int{0, 0, 1}, wantPending: 0, wantErr: true},
		{name: "nil publish errors and retains", batches: []transcriptwire.TranscriptStepBatch{testBatch("s1", 0, 0, true)}, nilPublish: true, wantOrder: nil, wantPending: 1, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &recordingPublish{failCall: tc.failCall}
			var p *Publisher
			if tc.nilPublish {
				p = NewPublisher("aerial", nil)
			} else {
				p = NewPublisher("aerial", rec.publish)
			}
			p.Enqueue(tc.batches...)
			err := p.Drain(context.Background())
			if tc.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			assertFirstStepsEqual(t, rec.published, tc.wantOrder)
			if pending := countPending(p); pending != tc.wantPending {
				t.Errorf("pending = %d, want %d", pending, tc.wantPending)
			}
		})
	}
}

// TestPublisher_DrainEnqueueDuringDrainKeepsSingleOrderEntry reproduces the
// window where Drain had deleted a session's queue but not its order entry:
// an Enqueue landing in that window duplicated the order entry. s1 fails
// fast while s2 blocks in publish; the test enqueues s1 while Drain is
// parked in s2, so any order/queue membership split surfaces as a duplicate.
func TestPublisher_DrainEnqueueDuringDrainKeepsSingleOrderEntry(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	publish := func(_ context.Context, b transcriptwire.TranscriptStepBatch) error {
		if b.SessionID == "s2" {
			close(started)
			<-release
			return nil
		}
		return errors.New("publish boom")
	}
	p := NewPublisher("aerial", publish)
	p.Enqueue(testBatch("s1", 0, 0, true), testBatch("s2", 0, 0, true))
	done := make(chan error, 1)
	go func() { done <- p.Drain(context.Background()) }()
	<-started
	p.Enqueue(testBatch("s1", 1, 1, true))
	close(release)
	if err := <-done; err == nil {
		t.Fatalf("expected Drain error from s1 failure, got nil")
	}
	if got := p.PendingSessions(); len(got) != 1 || got[0] != "s1" {
		t.Errorf("PendingSessions = %q, want exactly [s1]", got)
	}
	if n := p.PendingCount("s1"); n != 1 {
		t.Errorf("PendingCount(s1) = %d, want 1", n)
	}
}

// TestPublisher_DrainFailureKeepsBatchesEnqueuedDuringFlight covers an Enqueue
// to the same session landing while its publish is blocked in flight: when
// that publish fails, Drain must abandon only the snapshotted chain and keep
// the never-attempted batch for the next Drain instead of dropping it.
func TestPublisher_DrainFailureKeepsBatchesEnqueuedDuringFlight(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	calls := 0
	var published []transcriptwire.TranscriptStepBatch
	publish := func(_ context.Context, b transcriptwire.TranscriptStepBatch) error {
		calls++
		if calls == 1 {
			close(started)
			<-release
			return errors.New("publish boom")
		}
		published = append(published, b)
		return nil
	}
	p := NewPublisher("aerial", publish)
	p.Enqueue(testBatch("s1", 0, 0, false))
	done := make(chan error, 1)
	go func() { done <- p.Drain(context.Background()) }()
	<-started
	p.Enqueue(testBatch("s1", 1, 1, true))
	close(release)
	if err := <-done; err == nil {
		t.Fatalf("expected Drain error from s1 failure, got nil")
	}
	if got := p.PendingSessions(); len(got) != 1 || got[0] != "s1" {
		t.Fatalf("PendingSessions = %q, want exactly [s1]", got)
	}
	if n := p.PendingCount("s1"); n != 1 {
		t.Fatalf("PendingCount(s1) = %d, want 1", n)
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatalf("second Drain failed: %v", err)
	}
	assertEnvelopesEqual(t, published, []publishedEnvelope{{first: 1, last: 1, final: true}})
}

// TestPublisher_DrainConcurrentSerialization verifies that concurrent calls to Drain
// are serialized on drainMu so that no batch is published twice and no queued batch is dropped.
func TestPublisher_DrainConcurrentSerialization(t *testing.T) {
	t.Parallel()

	started := make(chan struct{})
	release := make(chan struct{})
	var publishedMu sync.Mutex
	var published []transcriptwire.TranscriptStepBatch

	var firstCall sync.Once
	publish := func(_ context.Context, b transcriptwire.TranscriptStepBatch) error {
		publishedMu.Lock()
		published = append(published, b)
		publishedMu.Unlock()

		firstCall.Do(func() {
			close(started)
			<-release
		})
		return nil
	}

	p := NewPublisher("aerial", publish)
	p.Enqueue(testBatch("s1", 0, 0, false), testBatch("s1", 1, 1, true))

	var wg sync.WaitGroup
	wg.Add(2)
	errs := make(chan error, 2)

	go func() {
		defer wg.Done()
		errs <- p.Drain(context.Background())
	}()

	<-started

	entered2 := make(chan struct{})
	go func() {
		defer wg.Done()
		close(entered2)
		errs <- p.Drain(context.Background())
	}()

	<-entered2
	close(release)

	wg.Wait()
	close(errs)

	for err := range errs {
		if err != nil {
			t.Fatalf("unexpected drain error: %v", err)
		}
	}

	publishedMu.Lock()
	defer publishedMu.Unlock()
	assertFirstStepsEqual(t, published, []int{0, 1})
	if pending := countPending(p); pending != 0 {
		t.Fatalf("pending = %d, want 0", pending)
	}
}

type publishedEnvelope struct {
	first int
	last  int
	final bool
}

func publishedEnvelopes(batches []transcriptwire.TranscriptStepBatch) []publishedEnvelope {
	if len(batches) == 0 {
		return nil
	}
	got := make([]publishedEnvelope, 0, len(batches))
	for _, b := range batches {
		got = append(got, publishedEnvelope{first: b.FirstStep, last: b.LastStep, final: b.Final})
	}
	return got
}

func assertEnvelopesEqual(t *testing.T, got []transcriptwire.TranscriptStepBatch, want []publishedEnvelope) {
	t.Helper()
	envelopes := publishedEnvelopes(got)
	if len(envelopes) != len(want) {
		t.Fatalf("published envelopes %v, want %v", envelopes, want)
	}
	for i := range envelopes {
		if envelopes[i] != want[i] {
			t.Fatalf("published envelopes %v, want %v", envelopes, want)
		}
	}
}

func TestPublisher_EnqueueStepsSplits(t *testing.T) {
	t.Parallel()
	base := transcriptwire.TranscriptStepBatch{Stack: "aerial", SessionID: "s1", FileMtime: time.Unix(1000, 0).UTC()}
	tests := []struct {
		name  string
		steps []transcriptwire.TranscriptStep
		want  []publishedEnvelope
	}{
		{name: "empty yields one final", steps: nil, want: []publishedEnvelope{{first: 0, last: 0, final: true}}},
		{name: "two steps one batch", steps: []transcriptwire.TranscriptStep{{StepIndex: 0}, {StepIndex: 1}}, want: []publishedEnvelope{{first: 0, last: 1, final: true}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &recordingPublish{failCall: -1}
			p := NewPublisher("aerial", rec.publish)
			if got := p.EnqueueSteps(base, tc.steps); got != len(tc.want) {
				t.Errorf("EnqueueSteps = %d, want %d", got, len(tc.want))
			}
			if n := p.PendingCount("s1"); n != len(tc.want) {
				t.Errorf("PendingCount = %d, want %d", n, len(tc.want))
			}
			if err := p.Drain(context.Background()); err != nil {
				t.Fatalf("Drain failed: %v", err)
			}
			assertEnvelopesEqual(t, rec.published, tc.want)
			for _, b := range rec.published {
				if b.Stack != "aerial" || b.SessionID != "s1" {
					t.Errorf("published batch lost envelope: stack=%q session=%q", b.Stack, b.SessionID)
				}
			}
		})
	}
}

func TestPublisher_BuildSessionBatches(t *testing.T) {
	t.Parallel()
	brainDir := t.TempDir()
	sessDir := filepath.Join(brainDir, "sess-1")
	if err := os.MkdirAll(sessDir, 0o755); err != nil {
		t.Fatalf("mkdir sessDir: %v", err)
	}
	transcriptContent := `{"step_index":0,"type":"USER_INPUT","content":"Hello world"}
{"step_index":1,"type":"PLANNER_RESPONSE","tool_name":"bash","content":"echo hi"}
{"step_index":2,"type":"TOOL","tool_name":"bash","content":"hi"}
{"step_index":3,"type":"PLANNER_RESPONSE","tool_name":"custom","tool_calls":[{"name":"custom","args":{"q":"test"}}]}
{"step_index":4,"type":"TOOL","tool_name":"trivial","content":"   "}
`
	if err := os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(transcriptContent), 0o644); err != nil {
		t.Fatalf("write transcript: %v", err)
	}

	tests := []struct {
		name         string
		params       SessionBatchParams
		wantErrSub   string
		wantBatches  int
		wantReset    bool
		wantBaseStep int
		wantFirst    int
		wantLast     int
	}{
		{
			name: "watermark negative collects all steps",
			params: SessionBatchParams{
				SessionID: "sess-1",
				BrainDir:  brainDir,
				Watermark: -1,
				Engine:    "agy",
			},
			wantBatches:  1,
			wantReset:    false,
			wantBaseStep: 0,
			wantFirst:    0,
			wantLast:     3,
		},
		{
			name: "watermark advances slices tail",
			params: SessionBatchParams{
				SessionID: "sess-1",
				BrainDir:  brainDir,
				Watermark: 1,
			},
			wantBatches:  1,
			wantReset:    false,
			wantBaseStep: 2,
			wantFirst:    2,
			wantLast:     3,
		},
		{
			name: "watermark higher than maxStepIndex resets",
			params: SessionBatchParams{
				SessionID: "sess-1",
				BrainDir:  brainDir,
				Watermark: 10,
			},
			wantBatches:  1,
			wantReset:    true,
			wantBaseStep: -1,
			wantFirst:    0,
			wantLast:     3,
		},
		{
			name: "empty session ID errors",
			params: SessionBatchParams{
				SessionID: "",
				BrainDir:  brainDir,
			},
			wantErrSub: "session ID cannot be empty",
		},
		{
			name: "missing session errors",
			params: SessionBatchParams{
				SessionID: "missing-sess",
				BrainDir:  brainDir,
			},
			wantErrSub: "transcript file not found",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			p := NewPublisher("aerial", nil)
			batches, err := p.BuildSessionBatches(tc.params)
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("BuildSessionBatches() err = %v, want substring %q", err, tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("BuildSessionBatches() unexpected err = %v", err)
			}
			if len(batches) != tc.wantBatches {
				t.Fatalf("batches len = %d, want %d", len(batches), tc.wantBatches)
			}
			if batches[0].Reset != tc.wantReset {
				t.Errorf("batch Reset = %v, want %v", batches[0].Reset, tc.wantReset)
			}
			if batches[0].BaseStep != tc.wantBaseStep {
				t.Errorf("batch BaseStep = %d, want %d", batches[0].BaseStep, tc.wantBaseStep)
			}
			if batches[0].FirstStep != tc.wantFirst {
				t.Errorf("batch FirstStep = %d, want %d", batches[0].FirstStep, tc.wantFirst)
			}
			if batches[0].LastStep != tc.wantLast {
				t.Errorf("batch LastStep = %d, want %d", batches[0].LastStep, tc.wantLast)
			}
		})
	}
}

func TestPublisher_PublishSessions(t *testing.T) {
	t.Parallel()
	brainDir := t.TempDir()
	sessDir := filepath.Join(brainDir, "sess-p1")
	if err := os.MkdirAll(sessDir, 0o755); err != nil {
		t.Fatalf("mkdir sessDir: %v", err)
	}
	transcriptContent := `{"step_index":0,"type":"USER_INPUT","content":"Hello world"}
{"step_index":1,"type":"PLANNER_RESPONSE","tool_name":"bash","content":"echo hi"}
`
	if err := os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(transcriptContent), 0o644); err != nil {
		t.Fatalf("write transcript: %v", err)
	}

	tests := []struct {
		name       string
		pubNil     bool
		brainDir   string
		watermarks map[string]int
		sessions   []string
		wantErrSub string
		wantCount  int
	}{
		{
			name:       "nil publisher errors",
			pubNil:     true,
			brainDir:   brainDir,
			sessions:   []string{"sess-p1"},
			wantErrSub: "publisher is nil",
		},
		{
			name:       "valid session publishes",
			pubNil:     false,
			brainDir:   brainDir,
			watermarks: map[string]int{"sess-p1": -1},
			sessions:   []string{"sess-p1"},
			wantCount:  1,
		},
		{
			name:       "auto-discover sessions when sessions empty",
			pubNil:     false,
			brainDir:   brainDir,
			watermarks: map[string]int{"sess-p1": -1},
			sessions:   nil,
			wantCount:  1,
		},
		{
			name:       "blank session in list skipped",
			pubNil:     false,
			brainDir:   brainDir,
			watermarks: map[string]int{"sess-p1": -1},
			sessions:   []string{"  ", "sess-p1"},
			wantCount:  1,
		},
		{
			name:       "missing session in list errors",
			pubNil:     false,
			brainDir:   brainDir,
			sessions:   []string{"sess-missing"},
			wantErrSub: "transcript file not found",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &recordingPublish{failCall: -1}
			var p *Publisher
			if !tc.pubNil {
				p = NewPublisher("aerial", rec.publish)
			}
			err := p.PublishSessions(context.Background(), tc.brainDir, tc.watermarks, tc.sessions...)
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("PublishSessions() err = %v, want substring %q", err, tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("PublishSessions() unexpected err = %v", err)
			}
			if len(rec.published) != tc.wantCount {
				t.Fatalf("published len = %d, want %d", len(rec.published), tc.wantCount)
			}
		})
	}
}

func TestPublisher_PublishSession(t *testing.T) {
	t.Parallel()
	brainDir := t.TempDir()
	sessDir := filepath.Join(brainDir, "sess-one")
	if err := os.MkdirAll(sessDir, 0o755); err != nil {
		t.Fatalf("mkdir sessDir: %v", err)
	}
	transcriptContent := `{"step_index":0,"type":"USER_INPUT","content":"Hello world"}`
	if err := os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(transcriptContent), 0o644); err != nil {
		t.Fatalf("write transcript: %v", err)
	}

	tests := []struct {
		name       string
		pubNil     bool
		params     SessionBatchParams
		wantErrSub string
		wantCount  int
	}{
		{
			name:       "nil publisher errors",
			pubNil:     true,
			params:     SessionBatchParams{SessionID: "sess-one", BrainDir: brainDir},
			wantErrSub: "publisher is nil",
		},
		{
			name:       "invalid session params errors",
			pubNil:     false,
			params:     SessionBatchParams{SessionID: "", BrainDir: brainDir},
			wantErrSub: "session ID cannot be empty",
		},
		{
			name:      "successful single session publish",
			pubNil:    false,
			params:    SessionBatchParams{SessionID: "sess-one", BrainDir: brainDir, Watermark: -1, Engine: "agy"},
			wantCount: 1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &recordingPublish{failCall: -1}
			var p *Publisher
			if !tc.pubNil {
				p = NewPublisher("aerial", rec.publish)
			}
			err := p.PublishSession(context.Background(), tc.params)
			if tc.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("PublishSession() err = %v, want substring %q", err, tc.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatalf("PublishSession() unexpected err = %v", err)
			}
			if len(rec.published) != tc.wantCount {
				t.Fatalf("published len = %d, want %d", len(rec.published), tc.wantCount)
			}
		})
	}
}

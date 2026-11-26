package transcript

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
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
	p.mu.Lock()
	defer p.mu.Unlock()
	pending := 0
	for _, q := range p.queues {
		pending += len(q)
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
			p.mu.Lock()
			gotCount := len(p.queues[tc.session])
			gotSessions := len(p.order)
			p.mu.Unlock()
			if gotCount != tc.want {
				t.Errorf("PendingCount(%q) = %d, want %d", tc.session, gotCount, tc.want)
			}
			if gotSessions != tc.sessions {
				t.Errorf("PendingSessions len = %d, want %d", gotSessions, tc.sessions)
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
	p.mu.Lock()
	gotSessions := append([]string(nil), p.order...)
	gotCount := len(p.queues["s1"])
	p.mu.Unlock()
	if len(gotSessions) != 1 || gotSessions[0] != "s1" {
		t.Errorf("PendingSessions = %q, want exactly [s1]", gotSessions)
	}
	if gotCount != 1 {
		t.Errorf("PendingCount(s1) = %d, want 1", gotCount)
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
	p.mu.Lock()
	gotSessions := append([]string(nil), p.order...)
	gotCount := len(p.queues["s1"])
	p.mu.Unlock()
	if len(gotSessions) != 1 || gotSessions[0] != "s1" {
		t.Fatalf("PendingSessions = %q, want exactly [s1]", gotSessions)
	}
	if gotCount != 1 {
		t.Fatalf("PendingCount(s1) = %d, want 1", gotCount)
	}
	if err := p.Drain(context.Background()); err != nil {
		t.Fatalf("second Drain failed: %v", err)
	}
	assertEnvelopesEqual(t, published, []publishedEnvelope{{first: 1, last: 1, final: true}})
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

type voiceRuntimeBatchCase struct {
	name       string
	sessionID  string
	wantEngine string
	wantErrSub string
}

func setupVoiceRuntimeTestDirs(t *testing.T) string {
	t.Helper()
	tmp := t.TempDir()
	dataDir := filepath.Join(tmp, "data")
	brainDir := filepath.Join(dataDir, "brain")
	voiceAgyDir := filepath.Join(dataDir, "runtimes", "voice", ".gemini", "antigravity-cli", "brain", "s-voice-agy")
	voiceClaudeDir := filepath.Join(dataDir, "runtimes", "voice", ".claude", "projects", "proj-voice")
	voiceCodexDir := filepath.Join(dataDir, "runtimes", "voice", ".codex", "sessions")
	voiceMuseDir := filepath.Join(dataDir, "runtimes", "voice", ".local", "share", "muse", "sessions")
	ephClaudeDir := filepath.Join(dataDir, "runtimes", "ephemeral", ".claude", "projects", "proj-eph")

	for _, d := range []string{brainDir, voiceAgyDir, voiceClaudeDir, voiceCodexDir, voiceMuseDir, ephClaudeDir} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("MkdirAll(%s): %v", d, err)
		}
	}

	if err := os.WriteFile(filepath.Join(voiceAgyDir, "transcript.jsonl"), []byte(`{"step_index":0,"type":"USER_INPUT","content":"Voice agy"}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(voiceClaudeDir, "s-voice-claude.jsonl"), []byte(`{"type":"user","timestamp":"2026-11-25T10:00:00Z","message":{"role":"user","content":"Voice claude"}}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(voiceCodexDir, "rollout-s-voice-codex.jsonl"), []byte(`{"type":"session_meta","payload":{"id":"s-voice-codex"}}`+"\n"+`{"type":"response_item","timestamp":"2026-11-25T10:00:01Z","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"Voice codex"}]}}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(voiceMuseDir, "s-voice-muse.jsonl"), []byte(`{"recorded_at":1000,"payload_type":"message","payload":{"role":"user","content":"Voice muse"}}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(ephClaudeDir, "s-eph-claude.jsonl"), []byte(`{"type":"user","timestamp":"2026-11-25T10:00:00Z","message":{"role":"user","content":"Ephemeral claude"}}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	return brainDir
}

func runVoiceRuntimeBatchCase(t *testing.T, brainDir string, tc voiceRuntimeBatchCase) {
	t.Helper()
	p := NewPublisher("aerial", nil)
	batches, err := p.BuildSessionBatches(SessionBatchParams{
		SessionID: tc.sessionID,
		BrainDir:  brainDir,
		Watermark: -1,
	})
	if tc.wantErrSub != "" {
		if err == nil || !strings.Contains(err.Error(), tc.wantErrSub) {
			t.Fatalf("BuildSessionBatches() err = %v, want substring %q", err, tc.wantErrSub)
		}
		return
	}
	if err != nil {
		t.Fatalf("BuildSessionBatches() err = %v", err)
	}
	if len(batches) == 0 {
		t.Fatalf("expected at least 1 batch, got 0")
	}
	if batches[0].Engine != tc.wantEngine {
		t.Errorf("batch Engine = %q, want %q", batches[0].Engine, tc.wantEngine)
	}
}

func TestPublisher_BuildSessionBatches_VoiceRuntimeEngines(t *testing.T) {
	t.Parallel()
	brainDir := setupVoiceRuntimeTestDirs(t)

	tests := []voiceRuntimeBatchCase{
		{name: "agy voice runtime session", sessionID: "s-voice-agy", wantEngine: "agy"},
		{name: "claude voice runtime session", sessionID: "s-voice-claude", wantEngine: "claude"},
		{name: "codex voice runtime session", sessionID: "s-voice-codex", wantEngine: "codex"},
		{name: "muse voice runtime session", sessionID: "s-voice-muse", wantEngine: "muse"},
		{name: "ephemeral claude session excluded", sessionID: "s-eph-claude", wantErrSub: "transcript file not found"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runVoiceRuntimeBatchCase(t, brainDir, tc)
		})
	}
}

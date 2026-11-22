package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/transcriptwire"
)

func decodeJSONBody(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(v)
}

// fakeSummaryLLM is a declarative LLMClient fake driven by struct fields.
type fakeSummaryLLM struct {
	response   string
	err        error
	calls      int
	lastPrompt string
}

func (f *fakeSummaryLLM) Complete(_ context.Context, prompt string) (string, error) {
	f.calls++
	f.lastPrompt = prompt
	if f.err != nil {
		return "", f.err
	}
	return f.response, nil
}

// fakeSummaryEmbedder is a declarative Embedder fake driven by struct fields.
type fakeSummaryEmbedder struct {
	vector []float32
	err    error
	calls  int
}

func (f *fakeSummaryEmbedder) GenerateEmbedding(_ context.Context, _ string) ([]float32, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.vector, nil
}

func seedSummarizerSession(t *testing.T, store *Store, session, thread string, first, last int, final bool) {
	t.Helper()
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	steps := make([]transcriptwire.TranscriptStep, 0, last-first+1)
	for i := first; i <= last; i++ {
		steps = append(steps, transcriptwire.TranscriptStep{StepIndex: i, Type: "model_response", Content: "content"})
	}
	batch := transcriptwire.TranscriptStepBatch{
		Stack: "aerial", SessionID: session, ThreadID: thread,
		FileMtime: epoch, BaseStep: first, FirstStep: first, LastStep: last,
		Final: final, Steps: steps,
	}
	if _, err := store.ApplyBatch(context.Background(), batch); err != nil {
		t.Fatalf("seed ApplyBatch: %v", err)
	}
}

func presetSummaryWatermark(t *testing.T, store *Store, session string, watermark int, summary string) {
	t.Helper()
	if _, err := store.db.ExecContext(context.Background(),
		`UPDATE session_summaries SET summary_step_watermark = $1, summary = $2 WHERE agent = 'aerial' AND session_id = $3`,
		watermark, summary, session); err != nil {
		t.Fatalf("preset watermark: %v", err)
	}
}

func readSummaryRow(t *testing.T, store *Store, session string) (summary string, watermark int, settled bool) {
	t.Helper()
	if err := store.db.QueryRowContext(context.Background(),
		`SELECT summary, summary_step_watermark, is_settled FROM session_summaries WHERE agent = 'aerial' AND session_id = $1`,
		session).Scan(&summary, &watermark, &settled); err != nil {
		t.Fatalf("read summary row: %v", err)
	}
	return summary, watermark, settled
}

func TestBuildSummaryPrompt(t *testing.T) {
	t.Parallel()
	longContent := strings.Repeat("x", maxSummaryPromptChars+100)
	tests := []struct {
		name         string
		session      string
		priorSummary string
		stepIndex    int
		stepType     string
		toolName     string
		content      string
		stepCount    int
		wantFrag     string
		wantAbsent   string
	}{
		{"empty steps names session", "sess-1", "", 0, "", "", "", 0, "sess-1", "Step 0"},
		{"single step carries content and tool", "sess-1", "", 2, "tool_result", "bash", "hello out", 1, "Step 2 (tool_result/bash): hello out", ""},
		{"typeless step falls back to step label", "sess-9", "", 0, "", "", "plain", 1, "Step 0 (step): plain", ""},
		{"oversize content clips with marker", "sess-1", "", 1, "model_response", "", longContent, 1, "...[clipped]", longContent},
		{"prior summary folded into prompt", "sess-1", "prior context from earlier", 1, "model_response", "", "more steps", 1, "Prior summary: prior context from earlier", ""},
		{"oversize prior summary clips with marker", "sess-1", longContent, 1, "model_response", "", "more steps", 1, "...[clipped]", longContent},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var steps []StepResult
			for range tc.stepCount {
				steps = append(steps, StepResult{SessionID: tc.session, StepIndex: tc.stepIndex, StepType: tc.stepType, ToolName: tc.toolName, Content: tc.content})
			}
			got := buildSummaryPrompt(tc.session, tc.priorSummary, steps)
			if !strings.Contains(got, tc.wantFrag) {
				t.Errorf("buildSummaryPrompt() missing %q in:\n%s", tc.wantFrag, got)
			}
			if tc.wantAbsent != "" && strings.Contains(got, tc.wantAbsent) {
				t.Errorf("buildSummaryPrompt() should clip content but kept full %d chars", len(tc.wantAbsent))
			}
		})
	}
}

type summarizerRunCase struct {
	name            string
	first           int
	last            int
	applyPreset     bool
	presetWatermark int
	presetSummary   string
	extraSessions   int
	batchLimit      int
	lockBusy        bool
	lockErr         bool
	llmErr          bool
	emptyLLM        bool
	embedErr        bool
	failStore       bool
	deleteSteps     bool
	maxSteps        int
	emptyThread     bool
	recentMtime     bool
	wantSummarized  int
	wantErr         bool
	wantWatermark   int
	wantSummaryFrag string
	wantLLMCalls    int
	wantEmbedCalls  int
	wantPromptFrag  string
}

func seedSummarizerRunCase(t *testing.T, tc summarizerRunCase) *Store {
	t.Helper()
	store := newTestStore(t, "aerial")
	thread := "thread-1"
	if tc.emptyThread {
		thread = ""
	}
	seedSummarizerSession(t, store, "sess-1", thread, tc.first, tc.last, false)
	for i := range tc.extraSessions {
		seedSummarizerSession(t, store, fmt.Sprintf("sess-extra-%d", i), "thread-1", 0, 1, false)
	}
	if tc.applyPreset {
		presetSummaryWatermark(t, store, "sess-1", tc.presetWatermark, tc.presetSummary)
	}
	if tc.recentMtime {
		testNow := time.Date(2026, 11, 18, 13, 0, 0, 0, time.UTC)
		if _, err := store.db.ExecContext(context.Background(),
			`UPDATE session_summaries SET last_mtime = $1 WHERE agent = 'aerial' AND session_id = 'sess-1'`,
			testNow.Add(-5*time.Minute)); err != nil {
			t.Fatalf("preset recent mtime: %v", err)
		}
	}
	if tc.deleteSteps {
		if _, err := store.db.ExecContext(context.Background(),
			`DELETE FROM transcript_steps WHERE agent = 'aerial' AND session_id = 'sess-1'`); err != nil {
			t.Fatalf("delete steps: %v", err)
		}
	}
	if tc.failStore {
		if _, err := store.db.ExecContext(context.Background(),
			`CREATE TRIGGER abort_summary_update BEFORE UPDATE ON session_summaries WHEN NEW.session_id = 'sess-1' BEGIN SELECT RAISE(ABORT, 'no summary'); END;`); err != nil {
			t.Fatalf("create trigger: %v", err)
		}
	}
	return store
}

func runSummarizerSweep(t *testing.T, store *Store, tc summarizerRunCase) (int, *fakeSummaryLLM, *fakeSummaryEmbedder, error) {
	t.Helper()
	llm := &fakeSummaryLLM{response: "fresh summary"}
	if tc.llmErr {
		llm.err = errors.New("llm down")
	}
	if tc.emptyLLM {
		llm.response = "  "
	}
	embedder := &fakeSummaryEmbedder{vector: []float32{0.1, 0.2}}
	if tc.embedErr {
		embedder.err = errors.New("senses down")
	}
	cfg := SummarizerConfig{Enabled: true, BatchLimit: tc.batchLimit, MaxSteps: tc.maxSteps}.withDefaults()
	sum := NewSummarizer(store, llm, embedder, cfg)
	testNow := time.Date(2026, 11, 18, 13, 0, 0, 0, time.UTC)
	sum.now = func() time.Time { return testNow }
	if tc.lockBusy {
		sum.acquire = func(context.Context, string) (func(), error) {
			return nil, ErrSummaryLockBusy
		}
	}
	if tc.lockErr {
		sum.acquire = func(context.Context, string) (func(), error) {
			return nil, errors.New("lock down")
		}
	}
	got, err := sum.RunOnce(context.Background())
	return got, llm, embedder, err
}

func TestStartSummarizerLoopEnabled(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		seed bool
	}{
		{"enabled sweeps idle sessions then stops", true},
		{"enabled with no sessions stops cleanly", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t, "aerial")
			if tc.seed {
				seedSummarizerSession(t, store, "sess-1", "thread-1", 0, 1, false)
			}
			cfg := &Config{Port: "4017", Stack: "aerial", Summarizer: SummarizerConfig{Enabled: true}}
			stop := startSummarizerLoop(context.Background(), store, cfg)
			stop()
		})
	}
}

func assertSummarizerSweep(t *testing.T, store *Store, tc summarizerRunCase, got int, err error, llm *fakeSummaryLLM, embedder *fakeSummaryEmbedder) {
	t.Helper()
	if (err != nil) != tc.wantErr {
		t.Fatalf("RunOnce() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if got != tc.wantSummarized {
		t.Errorf("RunOnce() = %d, want %d", got, tc.wantSummarized)
	}
	if llm.calls != tc.wantLLMCalls {
		t.Errorf("llm calls = %d, want %d", llm.calls, tc.wantLLMCalls)
	}
	if embedder.calls != tc.wantEmbedCalls {
		t.Errorf("embedder calls = %d, want %d", embedder.calls, tc.wantEmbedCalls)
	}
	if tc.wantPromptFrag != "" && !strings.Contains(llm.lastPrompt, tc.wantPromptFrag) {
		t.Errorf("llm prompt missing %q, got:\n%s", tc.wantPromptFrag, llm.lastPrompt)
	}
	if tc.wantWatermark == -2 {
		return
	}
	summary, watermark, settled := readSummaryRow(t, store, "sess-1")
	if watermark != tc.wantWatermark {
		t.Errorf("watermark = %d, want %d", watermark, tc.wantWatermark)
	}
	if tc.wantSummaryFrag != "" && !strings.Contains(summary, tc.wantSummaryFrag) {
		t.Errorf("summary = %q, want fragment %q", summary, tc.wantSummaryFrag)
	}
	if settled {
		t.Errorf("is_settled = true, summarizer must never settle on success or failure")
	}
}

func TestSummarizerRunOnce(t *testing.T) {
	t.Parallel()
	tests := []summarizerRunCase{
		{"due session summarizes from stored steps", 0, 2, false, 0, "", 0, 0, false, false, false, false, false, false, false, 0, false, false, 1, false, 2, "fresh summary", 1, 1, "content"},
		{"up-to-date session skips", 0, 2, true, 2, "old", 0, 0, false, false, false, false, false, false, false, 0, false, false, 0, false, 2, "old", 0, 0, ""},
		{"busy lock skips without error or llm call", 0, 2, false, 0, "", 0, 0, true, false, false, false, false, false, false, 0, false, false, 0, false, -1, "", 0, 0, ""},
		{"lock error fails the sweep", 0, 2, false, 0, "", 0, 0, false, true, false, false, false, false, false, 0, false, false, 0, true, -1, "", 0, 0, ""},
		{"llm failure keeps watermark and summary", 0, 2, false, 0, "", 0, 0, false, false, true, false, false, false, false, 0, false, false, 0, true, -1, "", 1, 0, ""},
		{"empty llm reply keeps watermark", 0, 2, false, 0, "", 0, 0, false, false, false, true, false, false, false, 0, false, false, 0, true, -1, "", 1, 0, ""},
		{"embed failure keeps watermark", 0, 2, false, 0, "", 0, 0, false, false, false, false, true, false, false, 0, false, false, 0, true, -1, "", 1, 1, ""},
		{"store failure keeps watermark", 0, 2, false, 0, "", 0, 0, false, false, false, false, false, true, false, 0, false, false, 0, true, -1, "", 1, 1, ""},
		{"pruned steps skip without calls", 0, 2, false, 0, "", 0, 0, false, false, false, false, false, false, true, 0, false, false, 0, false, -1, "", 0, 0, ""},
		{"max steps window summarizes tail", 0, 9, false, 0, "", 0, 0, false, false, false, false, false, false, false, 3, false, false, 1, false, 9, "fresh summary", 1, 1, "Step 9"},
		{"empty thread falls back to session lock", 0, 2, false, 0, "", 0, 0, false, false, false, false, false, false, false, 0, true, false, 1, false, 2, "fresh summary", 1, 1, "content"},
		{"batch limit summarizes one of three", 0, 1, false, 0, "", 2, 1, false, false, false, false, false, false, false, 0, false, false, 1, false, -2, "fresh summary", 1, 1, "content"},
		{"legacy pre-populated summary skips", 0, 2, true, 0, "legacy summary", 0, 0, false, false, false, false, false, false, false, 0, false, false, 0, false, 0, "legacy summary", 0, 0, ""},
		{"sub-delta steps skip re-summarization", 0, 25, true, 10, "prior summary", 0, 0, false, false, false, false, false, false, false, 0, false, false, 0, false, 10, "prior summary", 0, 0, ""},
		{"delta reaches threshold folds prior summary", 0, 35, true, 10, "prior summary", 0, 0, false, false, false, false, false, false, false, 0, false, false, 1, false, 35, "fresh summary", 1, 1, "Prior summary: prior summary"},
		{"active unsettled session skips until idle", 0, 2, false, 0, "", 0, 0, false, false, false, false, false, false, false, 0, false, true, 0, false, -1, "", 0, 0, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := seedSummarizerRunCase(t, tc)
			got, llm, embedder, err := runSummarizerSweep(t, store, tc)
			assertSummarizerSweep(t, store, tc, got, err, llm, embedder)
		})
	}
}

func TestSummarizerConfigWithDefaults(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		input             SummarizerConfig
		wantBatchLimit    int
		wantMaxSteps      int
		wantIdleThreshold time.Duration
		wantMinStepDelta  int
	}{
		{
			name:              "zero values fill defaults",
			input:             SummarizerConfig{},
			wantBatchLimit:    DefaultSummarizerBatchLimit,
			wantMaxSteps:      DefaultSummarizerMaxSteps,
			wantIdleThreshold: DefaultSummarizerIdleThreshold,
			wantMinStepDelta:  DefaultSummarizerMinStepDelta,
		},
		{
			name: "custom values preserved",
			input: SummarizerConfig{
				BatchLimit:    5,
				MaxSteps:      100,
				IdleThreshold: 30 * time.Minute,
				MinStepDelta:  50,
			},
			wantBatchLimit:    5,
			wantMaxSteps:      100,
			wantIdleThreshold: 30 * time.Minute,
			wantMinStepDelta:  50,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.input.withDefaults()
			if got.BatchLimit != tc.wantBatchLimit {
				t.Errorf("BatchLimit = %d, want %d", got.BatchLimit, tc.wantBatchLimit)
			}
			if got.MaxSteps != tc.wantMaxSteps {
				t.Errorf("MaxSteps = %d, want %d", got.MaxSteps, tc.wantMaxSteps)
			}
			if got.IdleThreshold != tc.wantIdleThreshold {
				t.Errorf("IdleThreshold = %v, want %v", got.IdleThreshold, tc.wantIdleThreshold)
			}
			if got.MinStepDelta != tc.wantMinStepDelta {
				t.Errorf("MinStepDelta = %d, want %d", got.MinStepDelta, tc.wantMinStepDelta)
			}
		})
	}
}

func TestSummarizerRunOnceDisabled(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		seed bool
	}{
		{"disabled with due session summarizes nothing", true},
		{"disabled without sessions summarizes nothing", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t, "aerial")
			if tc.seed {
				seedSummarizerSession(t, store, "sess-1", "thread-1", 0, 2, false)
			}
			sum := NewSummarizer(store, &fakeSummaryLLM{response: "x"}, &fakeSummaryEmbedder{}, SummarizerConfig{})
			got, err := sum.RunOnce(context.Background())
			if err != nil {
				t.Fatalf("RunOnce() err = %v", err)
			}
			if got != 0 {
				t.Errorf("RunOnce() = %d, want 0 while disabled", got)
			}
		})
	}
}

func TestSummarizerRunOnceNilDeps(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		nilStore   bool
		nilLLM     bool
		nilEmbed   bool
		closedDB   bool
		emptyStack bool
		wantSub    string
	}{
		{"nil store", true, false, false, false, false, "store is nil"},
		{"nil llm", false, true, false, false, false, "llm client is nil"},
		{"nil embedder", false, false, true, false, false, "embedder is nil"},
		{"closed db fails selection", false, false, false, true, false, "select summary candidates"},
		{"empty stack is read-only", false, false, false, false, true, "cannot be empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t, "aerial")
			s := store
			if tc.nilStore {
				s = nil
			}
			if tc.closedDB {
				if err := store.db.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}
			}
			if tc.emptyStack {
				s = NewStore(store.db, "")
			}
			var llm LLMClient = &fakeSummaryLLM{response: "x"}
			if tc.nilLLM {
				llm = nil
			}
			var embedder Embedder = &fakeSummaryEmbedder{}
			if tc.nilEmbed {
				embedder = nil
			}
			sum := NewSummarizer(s, llm, embedder, SummarizerConfig{Enabled: true})
			if _, err := sum.RunOnce(context.Background()); err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("RunOnce() err = %v, want substring %q", err, tc.wantSub)
			}
		})
	}
}

func TestStartSummarizerLoopDisabled(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		cfgNil  bool
		enabled bool
	}{
		{"nil config stops quietly", true, false},
		{"disabled config starts nothing", false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newTestStore(t, "aerial")
			var cfg *Config
			if !tc.cfgNil {
				cfg = &Config{Port: "4017", Stack: "aerial", Summarizer: SummarizerConfig{Enabled: tc.enabled}}
			}
			stop := startSummarizerLoop(context.Background(), store, cfg)
			stop()
		})
	}
}

func TestLLMClientComplete(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		serve        bool
		closedServer bool
		nilClient    bool
		status       int
		body         string
		prompt       string
		wantErr      bool
		wantFrag     string
	}{
		{"success returns response at low effort", true, false, false, http.StatusOK, `{"response":"hello summary"}`, "summarize this", false, "hello summary"},
		{"server error fails", true, false, false, http.StatusInternalServerError, "bad", "summarize this", true, ""},
		{"malformed json fails", true, false, false, http.StatusOK, "{broken", "summarize this", true, ""},
		{"unconfigured url fails", false, false, false, 0, "", "summarize this", true, ""},
		{"empty prompt fails", true, false, false, http.StatusOK, `{"response":"x"}`, "   ", true, ""},
		{"nil client fails", true, false, true, http.StatusOK, `{"response":"x"}`, "summarize this", true, ""},
		{"closed server fails", false, true, false, 0, "", "summarize this", true, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			url := ""
			var gotEffort string
			if tc.closedServer {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
				url = srv.URL
				srv.Close()
			} else if tc.serve {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					var req internalLLMRequest
					if err := decodeJSONBody(r, &req); err == nil {
						gotEffort = req.Effort
					}
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
				}))
				defer srv.Close()
				url = srv.URL
			}
			var client LLMClient
			client = NewLLMClient(url, nil)
			if tc.nilClient {
				client = (*httpLLMClient)(nil)
			}
			got, err := client.Complete(context.Background(), tc.prompt)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Complete() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if !strings.Contains(got, tc.wantFrag) {
				t.Errorf("Complete() = %q, want fragment %q", got, tc.wantFrag)
			}
			if gotEffort != llmEffortLow {
				t.Errorf("effort = %q, want %q", gotEffort, llmEffortLow)
			}
		})
	}
}

func TestEmbedderGenerate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		serve       bool
		status      int
		body        string
		text        string
		emptyStruct bool
		wantErr     bool
		wantLen     int
	}{
		{"success returns vector", true, http.StatusOK, `{"embedding":[0.1,0.2,0.3]}`, "hello", false, false, 3},
		{"server error fails", true, http.StatusInternalServerError, "bad", "hello", false, true, 0},
		{"empty embedding fails", true, http.StatusOK, `{"embedding":[]}`, "hello", false, true, 0},
		{"unconfigured url fails", false, 0, "", "hello", false, true, 0},
		{"empty text fails", true, http.StatusOK, `{"embedding":[0.1]}`, "   ", false, true, 0},
		{"zero value client fails", false, 0, "", "hello", true, true, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			baseURL := ""
			if tc.serve {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					w.WriteHeader(tc.status)
					_, _ = w.Write([]byte(tc.body))
				}))
				defer srv.Close()
				baseURL = srv.URL
			}
			var embedder Embedder
			embedder = NewEmbedder(baseURL, "", nil)
			if tc.emptyStruct {
				embedder = &httpEmbedder{}
			}
			got, err := embedder.GenerateEmbedding(context.Background(), tc.text)
			if (err != nil) != tc.wantErr {
				t.Fatalf("GenerateEmbedding() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if len(got) != tc.wantLen {
				t.Errorf("GenerateEmbedding() len = %d, want %d", len(got), tc.wantLen)
			}
		})
	}
}

func TestSummarizerMultiAgentSweep(t *testing.T) {
	t.Parallel()
	store := newTestStore(t, "aerial")
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

	// Seed session 1 for aerial
	seedSummarizerSession(t, store, "sess-aerial", "th-1", 0, 2, false)

	// Seed session 2 for agent-alpha
	batchAlpha := testBatch("sess-alpha", epoch, 0, 2, false, false)
	batchAlpha.Agent = "agent-alpha"
	batchAlpha.Stack = "agent-alpha"
	if _, err := store.ApplyBatch(context.Background(), batchAlpha); err != nil {
		t.Fatalf("seed agent-alpha batch: %v", err)
	}

	llm := &fakeSummaryLLM{response: "multi-agent summary"}
	embedder := &fakeSummaryEmbedder{vector: []float32{0.5}}
	sum := NewSummarizer(store, llm, embedder, SummarizerConfig{Enabled: true, BatchLimit: 10})
	testNow := time.Date(2026, 11, 18, 13, 0, 0, 0, time.UTC)
	sum.now = func() time.Time { return testNow }

	done, err := sum.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce() err = %v", err)
	}
	if done != 2 {
		t.Errorf("RunOnce() summarized %d, want 2", done)
	}

	// Verify aerial summary
	sumAerial, markAerial, _ := readSummaryRow(t, store, "sess-aerial")
	if markAerial != 2 || sumAerial != "multi-agent summary" {
		t.Errorf("aerial session: watermark = %d, summary = %q", markAerial, sumAerial)
	}

	// Verify agent-alpha summary
	var sumAlpha string
	var markAlpha int
	if err := store.db.QueryRowContext(context.Background(),
		`SELECT summary, summary_step_watermark FROM session_summaries WHERE agent = 'agent-alpha' AND session_id = 'sess-alpha'`).Scan(&sumAlpha, &markAlpha); err != nil {
		t.Fatalf("read agent-alpha summary: %v", err)
	}
	if markAlpha != 2 || sumAlpha != "multi-agent summary" {
		t.Errorf("agent-alpha session: watermark = %d, summary = %q", markAlpha, sumAlpha)
	}
}

func TestHTTPLLMClientIdempotencyKeyHeader(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		key  string
		want string
	}{
		{"key sent", "transcripts:summary:claude:s1:5", "transcripts:summary:claude:s1:5"},
		{"blank key omits header", "  ", ""},
		{"no key omits header", "", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Get("Idempotency-Key")
				_, _ = w.Write([]byte(`{"response":"ok"}`))
			}))
			defer srv.Close()
			ctx := context.Background()
			if tc.key != "" {
				ctx = withLLMKey(ctx, tc.key)
			}
			if _, err := NewLLMClient(srv.URL, nil).Complete(ctx, "p"); err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("Idempotency-Key = %q, want %q", got, tc.want)
			}
		})
	}
}

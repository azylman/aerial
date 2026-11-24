package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/transcriptwire"
	"github.com/azylman/aerial/services/transcripts/internal/config"
	"github.com/azylman/aerial/services/transcripts/internal/store"
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
	lastKey    string
}

func (f *fakeSummaryLLM) Complete(ctx context.Context, prompt string) (string, error) {
	f.calls++
	f.lastPrompt = prompt
	f.lastKey = eriwire.LLMKeyFrom(ctx)
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

func seedSummarizerSession(t *testing.T, st *store.Store, session, thread string, first, last int, final bool) {
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
	if _, err := st.ApplyBatch(context.Background(), batch); err != nil {
		t.Fatalf("seed ApplyBatch: %v", err)
	}
}

func presetSummaryWatermark(t *testing.T, db *sql.DB, session string, watermark int, summary string) {
	t.Helper()
	if _, err := db.ExecContext(context.Background(),
		`UPDATE session_summaries SET summary_step_watermark = $1, summary = $2 WHERE agent = 'aerial' AND session_id = $3`,
		watermark, summary, session); err != nil {
		t.Fatalf("preset watermark: %v", err)
	}
}

func readSummaryRow(t *testing.T, db *sql.DB, session string) (summary string, watermark int, settled bool) {
	t.Helper()
	if err := db.QueryRowContext(context.Background(),
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
			var steps []store.StepResult
			for range tc.stepCount {
				steps = append(steps, store.StepResult{SessionID: tc.session, StepIndex: tc.stepIndex, StepType: tc.stepType, ToolName: tc.toolName, Content: tc.content})
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

type summarizerEligibilityCase struct {
	name            string
	first           int
	last            int
	presetWatermark int
	presetSummary   string
	extraSessions   int
	batchLimit      int
	deleteSteps     bool
	maxSteps        int
	emptyThread     bool
	recentMtime     bool
	wantSummarized  int
	wantWatermark   int
	wantPromptFrag  string
}

func seedSummarizerEligibilityCase(t *testing.T, tc summarizerEligibilityCase) (*store.Store, *sql.DB) {
	t.Helper()
	st, db := newTestStoreWithDB(t, "aerial")
	thread := "thread-1"
	if tc.emptyThread {
		thread = ""
	}
	seedSummarizerSession(t, st, "sess-1", thread, tc.first, tc.last, false)
	for i := range tc.extraSessions {
		seedSummarizerSession(t, st, fmt.Sprintf("sess-extra-%d", i), "thread-1", 0, 1, false)
	}
	if tc.presetSummary != "" {
		presetSummaryWatermark(t, db, "sess-1", tc.presetWatermark, tc.presetSummary)
	}
	if tc.recentMtime {
		testNow := time.Date(2026, 11, 18, 13, 0, 0, 0, time.UTC)
		if _, err := db.ExecContext(context.Background(),
			`UPDATE session_summaries SET last_mtime = $1 WHERE agent = 'aerial' AND session_id = 'sess-1'`,
			testNow.Add(-5*time.Minute)); err != nil {
			t.Fatalf("preset recent mtime: %v", err)
		}
	}
	if tc.deleteSteps {
		if _, err := db.ExecContext(context.Background(),
			`DELETE FROM transcript_steps WHERE agent = 'aerial' AND session_id = 'sess-1'`); err != nil {
			t.Fatalf("delete steps: %v", err)
		}
	}
	return st, db
}

func runSummarizerEligibility(t *testing.T, st *store.Store, tc summarizerEligibilityCase) (int, *fakeSummaryLLM, *fakeSummaryEmbedder, error) {
	t.Helper()
	llm := &fakeSummaryLLM{response: "fresh summary"}
	embedder := &fakeSummaryEmbedder{vector: []float32{0.1, 0.2}}
	cfg := config.SummarizerConfig{Enabled: true, BatchLimit: tc.batchLimit, MaxSteps: tc.maxSteps}.WithDefaults()
	sum := NewSummarizer(st, llm, embedder, cfg)
	testNow := time.Date(2026, 11, 18, 13, 0, 0, 0, time.UTC)
	sum.now = func() time.Time { return testNow }
	got, err := sum.RunOnce(context.Background())
	return got, llm, embedder, err
}

func assertSummarizerEligibility(t *testing.T, db *sql.DB, tc summarizerEligibilityCase, got int, err error, llm *fakeSummaryLLM, embedder *fakeSummaryEmbedder) {
	t.Helper()
	if err != nil {
		t.Fatalf("RunOnce() err = %v, want nil", err)
	}
	if got != tc.wantSummarized {
		t.Errorf("RunOnce() = %d, want %d", got, tc.wantSummarized)
	}
	wantCalls := 0
	if tc.wantSummarized > 0 {
		wantCalls = 1
	}
	if llm.calls != wantCalls {
		t.Errorf("llm calls = %d, want %d", llm.calls, wantCalls)
	}
	if embedder.calls != wantCalls {
		t.Errorf("embedder calls = %d, want %d", embedder.calls, wantCalls)
	}
	if tc.wantPromptFrag != "" && !strings.Contains(llm.lastPrompt, tc.wantPromptFrag) {
		t.Errorf("llm prompt missing %q, got:\n%s", tc.wantPromptFrag, llm.lastPrompt)
	}
	if tc.wantWatermark == -2 {
		return
	}
	summary, watermark, settled := readSummaryRow(t, db, "sess-1")
	if watermark != tc.wantWatermark {
		t.Errorf("watermark = %d, want %d", watermark, tc.wantWatermark)
	}
	wantSummary := ""
	if tc.wantSummarized == 1 {
		wantSummary = "fresh summary"
	} else if tc.presetSummary != "" {
		wantSummary = tc.presetSummary
	}
	if wantSummary != "" && !strings.Contains(summary, wantSummary) {
		t.Errorf("summary = %q, want fragment %q", summary, wantSummary)
	}
	if settled {
		t.Errorf("is_settled = true, summarizer must never settle on success or failure")
	}
}

type summarizerFailureCase struct {
	name           string
	lockBusy       bool
	lockErr        bool
	llmErr         bool
	emptyLLM       bool
	embedErr       bool
	failStore      bool
	wantErr        bool
	wantLLMCalls   int
	wantEmbedCalls int
}

func TestSummarizerRunOnce_EligibilityAndWindowing(t *testing.T) {
	t.Parallel()
	tests := []summarizerEligibilityCase{
		{
			name:           "due session summarizes from stored steps",
			first:          0,
			last:           2,
			wantSummarized: 1,
			wantWatermark:  2,
			wantPromptFrag: "content",
		},
		{
			name:            "up-to-date session skips",
			first:           0,
			last:            2,
			presetWatermark: 2,
			presetSummary:   "old",
			wantSummarized:  0,
			wantWatermark:   2,
		},
		{
			name:           "pruned steps skip without calls",
			first:          0,
			last:           2,
			deleteSteps:    true,
			wantSummarized: 0,
			wantWatermark:  -1,
		},
		{
			name:           "max steps window summarizes tail",
			first:          0,
			last:           9,
			maxSteps:       3,
			wantSummarized: 1,
			wantWatermark:  9,
			wantPromptFrag: "Step 9",
		},
		{
			name:           "batch limit summarizes one of three",
			first:          0,
			last:           1,
			extraSessions:  2,
			batchLimit:     1,
			wantSummarized: 1,
			wantWatermark:  -2,
			wantPromptFrag: "content",
		},
		{
			name:            "legacy pre-populated summary skips",
			first:           0,
			last:            2,
			presetWatermark: 0,
			presetSummary:   "legacy summary",
			wantSummarized:  0,
			wantWatermark:   0,
		},
		{
			name:            "sub-delta steps skip re-summarization",
			first:           0,
			last:            25,
			presetWatermark: 10,
			presetSummary:   "prior summary",
			wantSummarized:  0,
			wantWatermark:   10,
		},
		{
			name:            "delta reaches threshold folds prior summary",
			first:           0,
			last:            35,
			presetWatermark: 10,
			presetSummary:   "prior summary",
			wantSummarized:  1,
			wantWatermark:   35,
			wantPromptFrag:  "Prior summary: prior summary",
		},
		{
			name:           "empty thread falls back to session lock",
			first:          0,
			last:           2,
			emptyThread:    true,
			wantSummarized: 1,
			wantWatermark:  2,
			wantPromptFrag: "content",
		},
		{
			name:           "active unsettled session skips until idle",
			first:          0,
			last:           2,
			recentMtime:    true,
			wantSummarized: 0,
			wantWatermark:  -1,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, db := seedSummarizerEligibilityCase(t, tc)
			got, llm, embedder, err := runSummarizerEligibility(t, st, tc)
			assertSummarizerEligibility(t, db, tc, got, err, llm, embedder)
		})
	}
}

func runSummarizerFailure(t *testing.T, tc summarizerFailureCase) {
	t.Helper()
	st, db := newTestStoreWithDB(t, "aerial")
	seedSummarizerSession(t, st, "sess-1", "thread-1", 0, 2, false)
	if tc.failStore {
		if _, err := db.ExecContext(context.Background(),
			`CREATE FUNCTION abort_write() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'no summary'; END $$;
			 CREATE TRIGGER abort_summary_update BEFORE UPDATE ON session_summaries FOR EACH ROW WHEN (NEW.session_id = 'sess-1') EXECUTE FUNCTION abort_write();`); err != nil {
			t.Fatalf("create trigger: %v", err)
		}
	}
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
	cfg := config.SummarizerConfig{Enabled: true}.WithDefaults()
	sum := NewSummarizer(st, llm, embedder, cfg)
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
	if (err != nil) != tc.wantErr {
		t.Fatalf("RunOnce() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if got != 0 {
		t.Errorf("RunOnce() = %d, want 0", got)
	}
	if llm.calls != tc.wantLLMCalls {
		t.Errorf("llm calls = %d, want %d", llm.calls, tc.wantLLMCalls)
	}
	if embedder.calls != tc.wantEmbedCalls {
		t.Errorf("embedder calls = %d, want %d", embedder.calls, tc.wantEmbedCalls)
	}
	_, watermark, settled := readSummaryRow(t, db, "sess-1")
	if watermark != -1 {
		t.Errorf("watermark = %d, want -1", watermark)
	}
	if settled {
		t.Errorf("is_settled = true, summarizer must never settle on success or failure")
	}
}

func TestSummarizerRunOnce_LocksAndFailures(t *testing.T) {
	t.Parallel()
	tests := []summarizerFailureCase{
		{name: "busy lock skips without error or llm call", lockBusy: true, wantErr: false, wantLLMCalls: 0, wantEmbedCalls: 0},
		{name: "lock error fails the sweep", lockErr: true, wantErr: true, wantLLMCalls: 0, wantEmbedCalls: 0},
		{name: "llm failure keeps watermark and summary", llmErr: true, wantErr: true, wantLLMCalls: 1, wantEmbedCalls: 0},
		{name: "empty llm reply keeps watermark", emptyLLM: true, wantErr: true, wantLLMCalls: 1, wantEmbedCalls: 0},
		{name: "embed failure keeps watermark", embedErr: true, wantErr: true, wantLLMCalls: 1, wantEmbedCalls: 1},
		{name: "store failure keeps watermark", failStore: true, wantErr: true, wantLLMCalls: 1, wantEmbedCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSummarizerFailure(t, tc)
		})
	}
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
			st := newTestStore(t, "aerial")
			if tc.seed {
				seedSummarizerSession(t, st, "sess-1", "thread-1", 0, 1, false)
			}
			cfg := &config.Config{Port: "4017", Stack: "aerial", Summarizer: config.SummarizerConfig{Enabled: true}}
			stop := startSummarizerLoop(context.Background(), st, cfg)
			stop()
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
			st := newTestStore(t, "aerial")
			if tc.seed {
				seedSummarizerSession(t, st, "sess-1", "thread-1", 0, 2, false)
			}
			sum := NewSummarizer(st, &fakeSummaryLLM{response: "x"}, &fakeSummaryEmbedder{}, config.SummarizerConfig{})
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
			st, db := newTestStoreWithDB(t, "aerial")
			s := st
			if tc.nilStore {
				s = nil
			}
			if tc.closedDB {
				if err := db.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}
			}
			if tc.emptyStack {
				s = store.NewStore(db, "")
			}
			var llm LLMClient = &fakeSummaryLLM{response: "x"}
			if tc.nilLLM {
				llm = nil
			}
			var embedder Embedder = &fakeSummaryEmbedder{}
			if tc.nilEmbed {
				embedder = nil
			}
			sum := NewSummarizer(s, llm, embedder, config.SummarizerConfig{Enabled: true})
			if _, err := sum.RunOnce(context.Background()); err == nil || !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("RunOnce() err = %v, want substring %q", err, tc.wantSub)
			}
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
					var req eriwire.InternalLLMRequest
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
				client = (*eriwire.InternalLLMClient)(nil)
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
			if gotEffort != eriwire.EffortLow {
				t.Errorf("effort = %q, want %q", gotEffort, eriwire.EffortLow)
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
	st, db := newTestStoreWithDB(t, "aerial")
	epoch := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

	// Seed session 1 for aerial
	seedSummarizerSession(t, st, "sess-aerial", "th-1", 0, 2, false)

	// Seed session 2 for agent-alpha
	batchAlpha := testBatch("sess-alpha", epoch, 0, 2, false, false)
	batchAlpha.Agent = "agent-alpha"
	batchAlpha.Stack = "agent-alpha"
	if _, err := st.ApplyBatch(context.Background(), batchAlpha); err != nil {
		t.Fatalf("seed agent-alpha batch: %v", err)
	}

	llm := &fakeSummaryLLM{response: "multi-agent summary"}
	embedder := &fakeSummaryEmbedder{vector: []float32{0.5}}
	sum := NewSummarizer(st, llm, embedder, config.SummarizerConfig{Enabled: true, BatchLimit: 10})
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
	sumAerial, markAerial, settled := readSummaryRow(t, db, "sess-aerial")
	if settled {
		t.Errorf("unexpected settled")
	}
	if markAerial != 2 || sumAerial != "multi-agent summary" {
		t.Errorf("aerial session: watermark = %d, summary = %q", markAerial, sumAerial)
	}

	// Verify agent-alpha summary
	var sumAlpha string
	var markAlpha int
	if err := db.QueryRowContext(context.Background(),
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

func TestSummarizerRejectedAnswerAttempts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		presetAttempts int
		response       string
		newSteps       bool
		wantKey        string
		wantLLMCalls   int
		wantAttempts   int
		wantWatermark  int
		wantExhausted  bool
		wantErr        bool
	}{
		{name: "first attempt uses bare key and accepts", response: "good", wantKey: "transcripts:summary:aerial:sess-1:0", wantLLMCalls: 1, wantAttempts: 0, wantWatermark: 2},
		{name: "first rejection records attempt one", response: " ", wantKey: "transcripts:summary:aerial:sess-1:0", wantLLMCalls: 1, wantAttempts: 1, wantWatermark: -1, wantErr: true},
		{name: "second attempt sends a2", presetAttempts: 1, response: " ", wantKey: "transcripts:summary:aerial:sess-1:0:a2", wantLLMCalls: 1, wantAttempts: 2, wantWatermark: -1, wantErr: true},
		{name: "third rejection fails terminally", presetAttempts: 2, response: " ", wantKey: "transcripts:summary:aerial:sess-1:0:a3", wantLLMCalls: 1, wantAttempts: 3, wantWatermark: -1, wantErr: true, wantExhausted: true},
		{name: "a3 accepted answer resets attempts", presetAttempts: 2, response: "good", wantKey: "transcripts:summary:aerial:sess-1:0:a3", wantLLMCalls: 1, wantAttempts: 0, wantWatermark: 2},
		{name: "exhausted session is skipped", presetAttempts: 3, response: "good", wantLLMCalls: 0, wantAttempts: 3, wantWatermark: -1},
		{name: "new steps reset exhausted session", presetAttempts: 3, response: "good", newSteps: true, wantKey: "transcripts:summary:aerial:sess-1:0", wantLLMCalls: 1, wantAttempts: 0, wantWatermark: 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, db := newTestStoreWithDB(t, "aerial")
			seedSummarizerSession(t, st, "sess-1", "thread-1", 0, 2, false)
			if _, err := db.ExecContext(context.Background(),
				`UPDATE session_summaries SET summary_attempts = $1 WHERE agent = 'aerial' AND session_id = 'sess-1'`, tc.presetAttempts); err != nil {
				t.Fatalf("preset attempts: %v", err)
			}
			if tc.newSteps {
				seedSummarizerSession(t, st, "sess-1", "thread-1", 3, 4, false)
			}
			llm := &fakeSummaryLLM{response: tc.response}
			cfg := config.SummarizerConfig{Enabled: true}.WithDefaults()
			sum := NewSummarizer(st, llm, &fakeSummaryEmbedder{vector: []float32{0.1}}, cfg)
			sum.now = func() time.Time { return time.Date(2026, 11, 18, 13, 0, 0, 0, time.UTC) }

			_, err := sum.RunOnce(context.Background())
			if (err != nil) != tc.wantErr {
				t.Fatalf("RunOnce() err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := errors.Is(err, ErrSummaryAttemptsExhausted); got != tc.wantExhausted {
				t.Errorf("RunOnce() exhausted = %v, want %v (err %v)", got, tc.wantExhausted, err)
			}
			if llm.calls != tc.wantLLMCalls {
				t.Errorf("llm calls = %d, want %d", llm.calls, tc.wantLLMCalls)
			}
			if llm.lastKey != tc.wantKey {
				t.Errorf("llm key = %q, want %q", llm.lastKey, tc.wantKey)
			}
			var attempts, watermark int
			if err := db.QueryRowContext(context.Background(),
				`SELECT summary_attempts, summary_step_watermark FROM session_summaries WHERE agent = 'aerial' AND session_id = 'sess-1'`).Scan(&attempts, &watermark); err != nil {
				t.Fatalf("read attempts: %v", err)
			}
			if attempts != tc.wantAttempts || watermark != tc.wantWatermark {
				t.Errorf("attempts, watermark = %d, %d; want %d, %d", attempts, watermark, tc.wantAttempts, tc.wantWatermark)
			}
		})
	}
}

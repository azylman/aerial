package server

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/restatetest"
	"github.com/azylman/aerial/services/coven/internal/store"
)

func TestWorker_ArmDreamerAndStart(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		callStart          bool
		role               string
		adminFailures      int
		armFailures        int
		cancelOn           string
		preCancel          bool
		wantErr            error
		wantPaths          []string
		wantArmIdempotency string
	}{
		{
			name:      "ArmDreamer_RegistersAndArms",
			callStart: false,
			cancelOn:  "never",
			wantPaths: []string{"/deployments", "/CovenDreamer/aerial/Arm"},
		},
		{
			name:          "ArmDreamer_RegistrationRetry",
			callStart:     false,
			adminFailures: 1,
			cancelOn:      "never",
			wantPaths:     []string{"/deployments", "/deployments", "/CovenDreamer/aerial/Arm"},
		},
		{
			name:      "ArmDreamer_RegistrationContextCancel",
			callStart: false,
			cancelOn:  "deploy",
			wantErr:   context.Canceled,
			wantPaths: []string{"/deployments"},
		},
		{
			name:        "ArmDreamer_PostContextCancel",
			callStart:   false,
			armFailures: 1,
			cancelOn:    "arm",
			wantErr:     context.Canceled,
			wantPaths:   []string{"/deployments", "/CovenDreamer/aerial/Arm"},
		},
		{
			name:               "Start_Success",
			callStart:          true,
			wantErr:            context.Canceled,
			wantPaths:          []string{"/deployments", "/CovenDreamer/aerial/Arm"},
			wantArmIdempotency: "coven:dreamer:arm:aerial",
		},
		{
			name:               "Start_ArmRetry",
			callStart:          true,
			armFailures:        1,
			wantErr:            context.Canceled,
			wantPaths:          []string{"/deployments", "/CovenDreamer/aerial/Arm", "/deployments", "/CovenDreamer/aerial/Arm"},
			wantArmIdempotency: "coven:dreamer:arm:aerial",
		},
		{
			name:      "Start_ExtractorRole",
			callStart: true,
			role:      "extractor",
			preCancel: true,
			wantErr:   context.Canceled,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			stub := &registrationAdminStub{
				failures:    tc.adminFailures,
				armFailures: tc.armFailures,
				cancelOn:    tc.cancelOn,
				cancel:      cancel,
			}
			server := httptest.NewServer(stub)
			defer server.Close()

			w := NewWorker(WorkerConfig{
				Stack:               "aerial",
				Role:                tc.role,
				RestateIngressURL:   server.URL,
				RestateAdminURL:     server.URL,
				RestateAdvertiseURL: "http://coven.aerial:9080",
				HTTPClient:          server.Client(),
				ArmRetryInterval:    5 * time.Millisecond,
			})

			if tc.preCancel {
				cancel()
			}

			var err error
			if tc.callStart {
				err = w.Start(ctx)
			} else {
				err = w.armDreamer(ctx)
			}

			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}

			stub.mu.Lock()
			defer stub.mu.Unlock()
			if tc.wantPaths != nil {
				require.Equal(t, tc.wantPaths, stub.paths)
			}
			if tc.wantArmIdempotency != "" {
				require.NotEmpty(t, stub.armHeaders)
				lastArmHeader := stub.armHeaders[len(stub.armHeaders)-1]
				require.Equal(t, tc.wantArmIdempotency, lastArmHeader.Get("Idempotency-Key"))
			}
		})
	}
}

type extractWorkerConfig struct {
	nilWorker bool
	closeDB   bool
	sabotage  string
	seedTurns []testTurn
	seedFact  bool
	llmResp   string
}

// extractGuardsCase tests guards and fault injection in CovenThread.Extract.
type extractGuardsCase struct {
	name         string
	key          string
	nilWorker    bool
	closeDB      bool
	sabotage     string
	llmResp      string
	initialState extractAttemptState
	wantErrSub   string
	wantTerminal bool
	wantStepErr  bool
}

func extractGuardsCases() []extractGuardsCase {
	const bad = `not json at all`
	const fresh = "Alex keeps the espresso grinder on setting twelve"
	const factsOnly = `{"facts":[{"fact_text":"` + fresh + `"}]}`
	const reinforceOnly = `{"facts":[],"reinforced_ids":[1]}`
	const procOnly = `{"facts":[],"procedural_candidates":[{"trigger":"When the espresso machine shows a descale warning","steps":["Run the descale cycle"]}]}`
	return []extractGuardsCase{
		{name: "empty thread id returns nil without running step", key: ""},
		{name: "nil worker returns error inside run step", key: "aerial/th-err", nilWorker: true, wantErrSub: "coven worker is nil", wantTerminal: true, wantStepErr: true},
		{name: "closed database is retryable, not terminal", key: "aerial/th-fail", closeDB: true, wantStepErr: true},
		{name: "missing table SQLSTATE 42 is terminal", key: "aerial/th-fail", sabotage: sabotageWatermarks, wantErrSub: "failed to get conversation fact watermark", wantTerminal: true, wantStepErr: true},
		{name: "missing facts table fails the reinforce step terminally", key: "aerial/th-apply", sabotage: sabotageFacts, llmResp: reinforceOnly, wantErrSub: "failed to reinforce trajectory fact 1", wantTerminal: true, wantStepErr: true},
		{name: "missing facts table fails the plan step terminally", key: "aerial/th-apply", sabotage: sabotageFacts, llmResp: factsOnly, wantErrSub: "FindNearestScoredFacts", wantTerminal: true, wantStepErr: true},
		{name: "check violation on insert fails the write step terminally", key: "aerial/th-apply", sabotage: `ALTER TABLE facts ADD CONSTRAINT sabotage CHECK (false) NOT VALID`, llmResp: factsOnly, wantErrSub: "error inserting fact", wantTerminal: true, wantStepErr: true},
		{name: "check violation on procedural insert fails the stage step terminally", key: "aerial/th-apply", sabotage: `ALTER TABLE procedural_candidates ADD CONSTRAINT sabotage CHECK (false) NOT VALID`, llmResp: procOnly, wantErrSub: "failed to insert procedural candidate", wantTerminal: true, wantStepErr: true},
		{name: "check violation on watermark write fails the advance step terminally", key: "aerial/th-apply", sabotage: `ALTER TABLE extraction_watermarks ADD CONSTRAINT sabotage CHECK (false) NOT VALID`, wantErrSub: "failed to update conversation fact watermark", wantTerminal: true, wantStepErr: true},
		{name: "check violation on skip write fails the skip step terminally", key: "aerial/th-apply", sabotage: `ALTER TABLE extraction_watermarks ADD CONSTRAINT sabotage CHECK (false) NOT VALID`, llmResp: bad, initialState: extractAttemptState{Watermark: 0, Attempt: 2}, wantErrSub: "failed to skip rejected extraction batch", wantTerminal: true, wantStepErr: true},
	}
}

// extractBatchCase tests batch extraction, ordinals, and reinforcement in CovenThread.Extract.
type extractBatchCase struct {
	name          string
	key           string
	req           ExtractJobRequest
	seedTurns     []testTurn
	seedFact      bool
	llmResp       string
	initialState  extractAttemptState
	wantLLMKeys   []string
	wantAgent     string
	wantThread    string
	wantWatermark int64
	wantFacts     []extractFactRow
	wantProcRows  int
}

func extractBatchCases() []extractBatchCase {
	const bad = `not json at all`
	const seeded = "Alex prefers oat milk in every latte"
	const fresh = "Alex keeps the espresso grinder on setting twelve"
	const full = `{"facts":[{"fact_text":"` + fresh + `"},{"action":"DELETE","fact_text":"Alex dropped an old latte habit"},{"fact_text":"short"}],` +
		`"reinforced_ids":[1,1,0,99],"procedural_candidates":[{"trigger":"When the espresso machine shows a descale warning","steps":["Run the descale cycle"]}]}`
	turn := []testTurn{adminTurn("th-apply", 1, "hi", "hello")}
	return []extractBatchCase{
		{name: "valid key extracts successfully", key: "aerial/th-ok", seedTurns: []testTurn{adminTurn("th-ok", 1, "hi", "hello")}, wantLLMKeys: []string{"coven:extract:aerial:th-ok:0"}, wantAgent: "aerial", wantThread: "th-ok", wantWatermark: 1},
		{name: "request agent and thread override key", key: "aerial/th-fallback", req: ExtractJobRequest{Agent: "custom-agent", ThreadID: "th-override"}, seedTurns: []testTurn{{agent: "custom-agent", thread: "th-override", rowID: 1, content: "hi", response: "hello", status: "COMPLETED", admin: true}}, wantLLMKeys: []string{"coven:extract:custom-agent:th-override:0"}, wantAgent: "custom-agent", wantThread: "th-override", wantWatermark: 1},
		{name: "three rejected answers mark the batch failed and advance past it", key: "aerial/th-bad", seedTurns: []testTurn{adminTurn("th-bad", 1, "hi", "hello")}, llmResp: bad, wantLLMKeys: []string{"coven:extract:aerial:th-bad:0", "coven:extract:aerial:th-bad:0:a1", "coven:extract:aerial:th-bad:0:a2"}, wantAgent: "aerial", wantThread: "th-bad", wantWatermark: 1},
		{name: "persisted attempt ordinal resumes at the last attempt", key: "aerial/th-bad", seedTurns: []testTurn{adminTurn("th-bad", 1, "hi", "hello")}, llmResp: bad, initialState: extractAttemptState{Watermark: 0, Attempt: 2}, wantLLMKeys: []string{"coven:extract:aerial:th-bad:0:a2"}, wantAgent: "aerial", wantThread: "th-bad", wantWatermark: 1},
		{name: "valid answer on a retry key applies and clears the ordinal", key: "aerial/th-retry", seedTurns: []testTurn{adminTurn("th-retry", 1, "hi", "hello")}, initialState: extractAttemptState{Watermark: 0, Attempt: 1}, wantLLMKeys: []string{"coven:extract:aerial:th-retry:0:a1"}, wantAgent: "aerial", wantThread: "th-retry", wantWatermark: 1},
		{name: "ordinal for an older batch is ignored", key: "aerial/th-stale", seedTurns: []testTurn{adminTurn("th-stale", 1, "hi", "hello")}, initialState: extractAttemptState{Watermark: 99, Attempt: 2}, wantLLMKeys: []string{"coven:extract:aerial:th-stale:0"}, wantAgent: "aerial", wantThread: "th-stale", wantWatermark: 1},
		{name: "valid answer applies each reinforcement, fact and procedural candidate as its own step", key: "aerial/th-apply", seedTurns: turn, seedFact: true, llmResp: full, wantLLMKeys: []string{"coven:extract:aerial:th-apply:0"}, wantFacts: []extractFactRow{{1, seeded, 2}, {2, fresh, 1}}, wantProcRows: 1, wantAgent: "aerial", wantThread: "th-apply", wantWatermark: 1},
	}
}

// newExtractWorker builds a Worker on a fresh test database whose LLM answers resp and
// records every Idempotency-Key it receives.
func newExtractWorker(t *testing.T, cfg extractWorkerConfig, llmKeys *[]string) (*Worker, *sql.DB) {
	t.Helper()
	if cfg.nilWorker {
		return nil, nil
	}
	db := newTestDB(t)
	seedTurns(t, db, cfg.seedTurns...)
	if cfg.seedFact {
		seedFacts(t, store.NewSQLStore(db), []string{"Alex prefers oat milk in every latte"}, [][]float32{testEmbeddingAt(0, 1)})
	}
	execSQL(t, db, cfg.sabotage)
	resp := cmp.Or(cfg.llmResp, `{"facts":[]}`)
	worker := NewWorker(WorkerConfig{
		Store:  store.NewSQLStore(db),
		Ollama: &MockOllamaClient{embedding: testEmbeddingAt(5, 1)},
		LLMClient: func(ctx context.Context, prompt string) (string, error) {
			*llmKeys = append(*llmKeys, eriwire.LLMKeyFrom(ctx))
			return resp, nil
		},
	})
	if cfg.closeDB {
		require.NoError(t, db.Close())
	}
	return worker, db
}

func TestCovenThread_Extract_GuardsAndFaults(t *testing.T) {
	t.Parallel()
	for _, tc := range extractGuardsCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var llmKeys []string
			var turns []testTurn
			if tc.key == "aerial/th-apply" {
				turns = []testTurn{adminTurn("th-apply", 1, "hi", "hello")}
			}
			worker, db := newExtractWorker(t, extractWorkerConfig{
				nilWorker: tc.nilWorker,
				closeDB:   tc.closeDB,
				sabotage:  tc.sabotage,
				seedTurns: turns,
				llmResp:   tc.llmResp,
			}, &llmKeys)
			require.True(t, db != nil || tc.nilWorker || tc.key == "")

			h := restatetest.NewObjectHarness(t, tc.key,
				restatetest.WithKeepTerminalOnly(),
				restatetest.WithInitialState(extractAttemptStateKey, tc.initialState),
			)

			err := NewCovenThread(worker).Extract(h.ObjCtx, ExtractJobRequest{})
			if tc.wantErrSub != "" {
				require.ErrorContains(t, err, tc.wantErrSub)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.key != "", h.Ran())
			require.False(t, h.HasRetryCap(), "CovenThread.Extract must not cap retry attempts")
			require.Equal(t, tc.wantTerminal, h.IsTerminal(), "terminal classification of the last step error")
			require.Equal(t, tc.wantStepErr, h.RunErr() != nil, "last step error = %v", h.RunErr())
			gotStateVal, ok := h.State(extractAttemptStateKey)
			var gotState extractAttemptState
			if ok && gotStateVal != nil {
				parsedState, castOk := gotStateVal.(extractAttemptState)
				require.True(t, castOk)
				gotState = parsedState
			}
			require.Equal(t, tc.initialState, gotState, "attempt state after the invocation")
		})
	}
}

func TestCovenThread_Extract_BatchAndRetryOrdinals(t *testing.T) {
	t.Parallel()
	for _, tc := range extractBatchCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var llmKeys []string
			worker, db := newExtractWorker(t, extractWorkerConfig{
				seedTurns: tc.seedTurns,
				seedFact:  tc.seedFact,
				llmResp:   tc.llmResp,
			}, &llmKeys)

			h := restatetest.NewObjectHarness(t, tc.key,
				restatetest.WithKeepTerminalOnly(),
				restatetest.WithInitialState(extractAttemptStateKey, tc.initialState),
			)

			err := NewCovenThread(worker).Extract(h.ObjCtx, tc.req)
			require.NoError(t, err)
			require.True(t, h.Ran())
			require.False(t, h.HasRetryCap(), "CovenThread.Extract must not cap retry attempts")
			require.False(t, h.IsTerminal())
			require.Nil(t, h.RunErr())
			require.Equal(t, tc.wantLLMKeys, llmKeys)
			gotStateVal, ok := h.State(extractAttemptStateKey)
			var gotState extractAttemptState
			if ok && gotStateVal != nil {
				parsedState, castOk := gotStateVal.(extractAttemptState)
				require.True(t, castOk)
				gotState = parsedState
			}
			require.Equal(t, extractAttemptState{}, gotState, "attempt state after the invocation")
			if tc.wantWatermark > 0 {
				require.Equal(t, tc.wantWatermark, readExtractWatermark(t, db, tc.wantAgent, tc.wantThread))
			}
			if len(tc.wantFacts) > 0 || tc.wantProcRows > 0 {
				require.Equal(t, tc.wantFacts, readExtractFacts(t, db))
				require.Equal(t, []string{strconv.Itoa(tc.wantProcRows)}, queryStrings(t, db, `SELECT count(*)::text FROM procedural_candidates`))
			}
		})
	}
}

func TestClassifyExtractError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		err          error
		wantNil      bool
		wantTerminal bool
		wantCode     restate.Code
	}{
		{name: "NilStaysNil", wantNil: true},
		{name: "TerminalPassesThrough", err: restate.ToTerminalError(errors.New("bad"), restate.WithErrorCode(412)), wantTerminal: true, wantCode: 412},
		{name: "MissingFactIs404", err: fmt.Errorf("reinforce: %w", store.ErrFactNotFound), wantTerminal: true, wantCode: 404},
		{name: "UniqueViolationIs400", err: &pgconn.PgError{Code: "23505"}, wantTerminal: true, wantCode: 400},
		{name: "UndefinedTableIs400", err: fmt.Errorf("query: %w", &pgconn.PgError{Code: "42P01"}), wantTerminal: true, wantCode: 400},
		{name: "SerializationFailureRetries", err: &pgconn.PgError{Code: "40001"}},
		{name: "DeadlockRetries", err: &pgconn.PgError{Code: "40P01"}},
		{name: "ConnectionFailureRetries", err: &pgconn.PgError{Code: "08006"}},
		{name: "ContextTimeoutRetries", err: context.DeadlineExceeded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := classifyExtractError(tc.err)
			if tc.wantNil {
				require.NoError(t, got)
				return
			}
			require.EqualError(t, got, tc.err.Error())
			require.Equal(t, tc.wantTerminal, restate.IsTerminalError(got))
			if tc.wantTerminal {
				require.Equal(t, tc.wantCode, restate.AsTerminalError(got).Code())
			}
		})
	}
}

func TestWorkerCallExtractStep(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		nilLLM       bool
		resp         string
		llmErr       error
		turns        int
		want         extractAnswer
		wantErr      bool
		wantTerminal bool
	}{
		{name: "NilLLMClientIsTerminal", nilLLM: true, turns: 1, wantErr: true, wantTerminal: true},
		{name: "TransportErrorRetries", llmErr: errors.New("connection reset"), turns: 2, wantErr: true},
		{name: "TimeoutOnMultiTurnBatchAsksToHalve", llmErr: context.DeadlineExceeded, turns: 2, want: extractAnswer{TimedOut: true}},
		{name: "TimeoutOnSingleTurnRetries", llmErr: context.DeadlineExceeded, turns: 1, wantErr: true},
		{name: "InvalidAnswerIsRejected", resp: "nope", turns: 1, want: extractAnswer{Rejected: "no json object found in raw output"}},
		{name: "ValidAnswerParses", resp: `{"facts":[{"fact_text":"Alex likes tea"}]}`, turns: 1, want: extractAnswer{Payload: ExtractedFactsPayload{Facts: []ExtractedFactItem{{FactText: "Alex likes tea"}}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := WorkerConfig{LLMClient: func(context.Context, string) (string, error) { return tc.resp, tc.llmErr }}
			if tc.nilLLM {
				cfg.LLMClient = nil
			}
			got, err := NewWorker(cfg).callExtractStep(context.Background(), "coven:extract:aerial:th:0", "aerial", extractBatch{Ready: true, Prompt: "p", Turns: tc.turns})
			require.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			require.Equal(t, tc.wantTerminal, restate.IsTerminalError(err))
			require.Equal(t, tc.want, got)
		})
	}
}

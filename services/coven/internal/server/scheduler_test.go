package server

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/coven/internal/store"
)

func newMockDeploymentServer(statusCode int, body string, expectedAdvertiseURL string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/deployments" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req["force"] != true || req["uri"] != expectedAdvertiseURL {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		status := statusCode
		if status == 0 {
			status = http.StatusOK
		}
		w.WriteHeader(status)
		if body != "" {
			_, _ = w.Write([]byte(body))
		}
	}))
}

func setupAdminTestEndpoint(unreachable, skipServer bool, statusCode int, responseBody, advertiseURL string) (string, *http.Client, func()) {
	if unreachable {
		return "http://127.0.0.1:1", nil, func() {}
	}
	if skipServer {
		return "", nil, func() {}
	}
	srv := newMockDeploymentServer(statusCode, responseBody, advertiseURL)
	return srv.URL, srv.Client(), srv.Close
}

func TestRegisterRestateDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		statusCode   int
		responseBody string
		advertiseURL string
		unreachable  bool
		skipServer   bool
		wantErr      bool
		wantSubstr   string
	}{
		{
			name:         "empty admin url returns nil",
			advertiseURL: "http://coven:9080",
			skipServer:   true,
			wantErr:      false,
		},
		{
			name:         "empty advertise url returns nil",
			statusCode:   http.StatusOK,
			advertiseURL: "",
			wantErr:      false,
		},
		{
			name:         "successful registration",
			statusCode:   http.StatusOK,
			responseBody: `{"id":"dep-123"}`,
			advertiseURL: "http://coven.aerial:9080",
			wantErr:      false,
		},
		{
			name:         "server error with bounded error body",
			statusCode:   http.StatusInternalServerError,
			responseBody: "deployment failed: internal error",
			advertiseURL: "http://coven.aerial:9080",
			wantErr:      true,
			wantSubstr:   "restate admin /deployments returned 500: deployment failed",
		},
		{
			name:         "server error with json envelope",
			statusCode:   http.StatusForbidden,
			responseBody: `{"message":"unauthorized"}`,
			advertiseURL: "http://coven.aerial:9080",
			wantErr:      true,
			wantSubstr:   "restate admin /deployments returned 403: unauthorized",
		},
		{
			name:         "connection error returns formatted error",
			advertiseURL: "http://coven.aerial:9080",
			unreachable:  true,
			wantErr:      true,
			wantSubstr:   "register coven restate deployment:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			adminURL, client, cleanup := setupAdminTestEndpoint(tc.unreachable, tc.skipServer, tc.statusCode, tc.responseBody, tc.advertiseURL)
			defer cleanup()

			err := registerRestateDeployment(context.Background(), client, adminURL, tc.advertiseURL)
			if tc.wantErr {
				require.Error(t, err)
				if tc.wantSubstr != "" {
					require.Contains(t, err.Error(), tc.wantSubstr)
				}
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestWorker_ArmDreamer_RegistersAndArms(t *testing.T) {
	t.Parallel()

	var deploymentRegistered atomic.Bool
	var dreamerArmed atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/deployments":
			deploymentRegistered.Store(true)
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{}`)); err != nil {
				return
			}
		case r.Method == http.MethodPost && r.URL.Path == "/CovenDreamer/aerial/Arm":
			if !deploymentRegistered.Load() {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			dreamerArmed.Store(true)
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{}`)); err != nil {
				return
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	w := NewWorker(WorkerConfig{
		Stack:               "aerial",
		RestateIngressURL:   server.URL,
		RestateAdminURL:     server.URL,
		RestateAdvertiseURL: "http://coven.aerial:9080",
		HTTPClient:          server.Client(),
		ArmRetryInterval:    5 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := w.armDreamer(ctx)
	require.NoError(t, err)
	require.True(t, deploymentRegistered.Load(), "deployment should have been registered")
	require.True(t, dreamerArmed.Load(), "dreamer should have been armed")
}

func TestWorker_ArmDreamer_RegistrationRetry(t *testing.T) {
	t.Parallel()

	var registrationAttempts atomic.Int32
	var dreamerArmed atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/deployments":
			if registrationAttempts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{}`)); err != nil {
				return
			}
		case r.Method == http.MethodPost && r.URL.Path == "/CovenDreamer/aerial/Arm":
			dreamerArmed.Store(true)
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{}`)); err != nil {
				return
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	w := NewWorker(WorkerConfig{
		Stack:               "aerial",
		RestateIngressURL:   server.URL,
		RestateAdminURL:     server.URL,
		RestateAdvertiseURL: "http://coven.aerial:9080",
		HTTPClient:          server.Client(),
		ArmRetryInterval:    5 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := w.armDreamer(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, registrationAttempts.Load(), int32(2))
	require.True(t, dreamerArmed.Load())
}

func TestWorker_ArmDreamer_RegistrationContextCancel(t *testing.T) {
	t.Parallel()

	canceled := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cancel()
		select {
		case <-canceled:
		default:
			close(canceled)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	w := NewWorker(WorkerConfig{
		Stack:               "aerial",
		RestateIngressURL:   server.URL,
		RestateAdminURL:     server.URL,
		RestateAdvertiseURL: "http://coven.aerial:9080",
		HTTPClient:          server.Client(),
		ArmRetryInterval:    50 * time.Millisecond,
	})

	err := w.armDreamer(ctx)
	require.ErrorIs(t, err, context.Canceled)
	<-canceled
}

func TestWorker_ArmDreamer_PostContextCancel(t *testing.T) {
	t.Parallel()

	canceled := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/deployments":
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Path == "/CovenDreamer/aerial/Arm":
			cancel()
			select {
			case <-canceled:
			default:
				close(canceled)
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	w := NewWorker(WorkerConfig{
		Stack:               "aerial",
		RestateIngressURL:   server.URL,
		RestateAdminURL:     server.URL,
		RestateAdvertiseURL: "http://coven.aerial:9080",
		HTTPClient:          server.Client(),
		ArmRetryInterval:    50 * time.Millisecond,
	})

	err := w.armDreamer(ctx)
	require.ErrorIs(t, err, context.Canceled)
	<-canceled
}

// covenThreadExtractCase is one CovenThread.Extract scenario.
type covenThreadExtractCase struct {
	seedFact      bool
	checkFacts    bool
	wantFacts     []extractFactRow
	wantProcRows  int
	name          string
	key           string
	req           ExtractJobRequest
	nilWorker     bool
	sabotage      string
	closeDB       bool
	seedTurns     []testTurn
	llmResp       string
	initialState  extractAttemptState
	wantLLMKeys   []string
	wantState     extractAttemptState
	wantAgent     string
	wantThread    string
	wantWatermark int64
	wantErrSub    string
	wantTerminal  bool
	wantStepErr   bool
	expectRanRun  bool
}

func covenThreadExtractCases() []covenThreadExtractCase {
	const bad = `not json at all`
	const seeded = "Alex prefers oat milk in every latte"
	const fresh = "Alex keeps the espresso grinder on setting twelve"
	const full = `{"facts":[{"fact_text":"` + fresh + `"},{"action":"DELETE","fact_text":"Alex dropped an old latte habit"},{"fact_text":"short"}],` +
		`"reinforced_ids":[1,1,0,99],"procedural_candidates":[{"trigger":"When the espresso machine shows a descale warning","steps":["Run the descale cycle"]}]}`
	const factsOnly = `{"facts":[{"fact_text":"` + fresh + `"}]}`
	const reinforceOnly = `{"facts":[],"reinforced_ids":[1]}`
	const procOnly = `{"facts":[],"procedural_candidates":[{"trigger":"When the espresso machine shows a descale warning","steps":["Run the descale cycle"]}]}`
	turn := []testTurn{adminTurn("th-apply", 1, "hi", "hello")}
	return []covenThreadExtractCase{
		{name: "empty thread id returns nil without running step", key: "", expectRanRun: false},
		{name: "nil worker returns error inside run step", key: "aerial/th-err", nilWorker: true, wantErrSub: "coven worker is nil", wantTerminal: true, wantStepErr: true, expectRanRun: true},
		{name: "closed database is retryable, not terminal", key: "aerial/th-fail", closeDB: true, wantStepErr: true, expectRanRun: true},
		{name: "missing table SQLSTATE 42 is terminal", key: "aerial/th-fail", sabotage: sabotageWatermarks, wantErrSub: "failed to get conversation fact watermark", wantTerminal: true, wantStepErr: true, expectRanRun: true},
		{name: "valid key extracts successfully", key: "aerial/th-ok", seedTurns: []testTurn{adminTurn("th-ok", 1, "hi", "hello")}, wantLLMKeys: []string{"coven:extract:aerial:th-ok:0"}, wantAgent: "aerial", wantThread: "th-ok", wantWatermark: 1, expectRanRun: true},
		{name: "request agent and thread override key", key: "aerial/th-fallback", req: ExtractJobRequest{Agent: "custom-agent", ThreadID: "th-override"}, seedTurns: []testTurn{{agent: "custom-agent", thread: "th-override", rowID: 1, content: "hi", response: "hello", status: "COMPLETED", admin: true}}, wantLLMKeys: []string{"coven:extract:custom-agent:th-override:0"}, wantAgent: "custom-agent", wantThread: "th-override", wantWatermark: 1, expectRanRun: true},
		{name: "three rejected answers mark the batch failed and advance past it", key: "aerial/th-bad", seedTurns: []testTurn{adminTurn("th-bad", 1, "hi", "hello")}, llmResp: bad, wantLLMKeys: []string{"coven:extract:aerial:th-bad:0", "coven:extract:aerial:th-bad:0:a1", "coven:extract:aerial:th-bad:0:a2"}, wantAgent: "aerial", wantThread: "th-bad", wantWatermark: 1, expectRanRun: true},
		{name: "persisted attempt ordinal resumes at the last attempt", key: "aerial/th-bad", seedTurns: []testTurn{adminTurn("th-bad", 1, "hi", "hello")}, llmResp: bad, initialState: extractAttemptState{Watermark: 0, Attempt: 2}, wantLLMKeys: []string{"coven:extract:aerial:th-bad:0:a2"}, wantAgent: "aerial", wantThread: "th-bad", wantWatermark: 1, expectRanRun: true},
		{name: "valid answer on a retry key applies and clears the ordinal", key: "aerial/th-retry", seedTurns: []testTurn{adminTurn("th-retry", 1, "hi", "hello")}, initialState: extractAttemptState{Watermark: 0, Attempt: 1}, wantLLMKeys: []string{"coven:extract:aerial:th-retry:0:a1"}, wantAgent: "aerial", wantThread: "th-retry", wantWatermark: 1, expectRanRun: true},
		{name: "ordinal for an older batch is ignored", key: "aerial/th-stale", seedTurns: []testTurn{adminTurn("th-stale", 1, "hi", "hello")}, initialState: extractAttemptState{Watermark: 99, Attempt: 2}, wantLLMKeys: []string{"coven:extract:aerial:th-stale:0"}, wantAgent: "aerial", wantThread: "th-stale", wantWatermark: 1, expectRanRun: true},
		{name: "valid answer applies each reinforcement, fact and procedural candidate as its own step", key: "aerial/th-apply", seedTurns: turn, seedFact: true, llmResp: full, wantLLMKeys: []string{"coven:extract:aerial:th-apply:0"}, checkFacts: true, wantFacts: []extractFactRow{{1, seeded, 2}, {2, fresh, 1}}, wantProcRows: 1, wantAgent: "aerial", wantThread: "th-apply", wantWatermark: 1, expectRanRun: true},
		{name: "missing facts table fails the reinforce step terminally", key: "aerial/th-apply", seedTurns: turn, sabotage: sabotageFacts, llmResp: reinforceOnly, wantLLMKeys: []string{"coven:extract:aerial:th-apply:0"}, wantErrSub: "failed to reinforce trajectory fact 1", wantTerminal: true, wantStepErr: true, expectRanRun: true},
		{name: "missing facts table fails the plan step terminally", key: "aerial/th-apply", seedTurns: turn, sabotage: sabotageFacts, llmResp: factsOnly, wantLLMKeys: []string{"coven:extract:aerial:th-apply:0"}, wantErrSub: "FindNearestScoredFacts", wantTerminal: true, wantStepErr: true, expectRanRun: true},
		{name: "check violation on insert fails the write step terminally", key: "aerial/th-apply", seedTurns: turn, sabotage: `ALTER TABLE facts ADD CONSTRAINT sabotage CHECK (false) NOT VALID`, llmResp: factsOnly, wantLLMKeys: []string{"coven:extract:aerial:th-apply:0"}, wantErrSub: "error inserting fact", wantTerminal: true, wantStepErr: true, expectRanRun: true},
		{name: "check violation on procedural insert fails the stage step terminally", key: "aerial/th-apply", seedTurns: turn, sabotage: `ALTER TABLE procedural_candidates ADD CONSTRAINT sabotage CHECK (false) NOT VALID`, llmResp: procOnly, wantLLMKeys: []string{"coven:extract:aerial:th-apply:0"}, wantErrSub: "failed to insert procedural candidate", wantTerminal: true, wantStepErr: true, expectRanRun: true},
		{name: "check violation on watermark write fails the advance step terminally", key: "aerial/th-apply", seedTurns: turn, sabotage: `ALTER TABLE extraction_watermarks ADD CONSTRAINT sabotage CHECK (false) NOT VALID`, wantLLMKeys: []string{"coven:extract:aerial:th-apply:0"}, wantErrSub: "failed to update conversation fact watermark", wantTerminal: true, wantStepErr: true, expectRanRun: true},
		{name: "check violation on skip write fails the skip step terminally", key: "aerial/th-apply", seedTurns: turn, sabotage: `ALTER TABLE extraction_watermarks ADD CONSTRAINT sabotage CHECK (false) NOT VALID`, llmResp: bad, initialState: extractAttemptState{Watermark: 0, Attempt: 2}, wantLLMKeys: []string{"coven:extract:aerial:th-apply:0:a2"}, wantState: extractAttemptState{Watermark: 0, Attempt: 2}, wantErrSub: "failed to skip rejected extraction batch", wantTerminal: true, wantStepErr: true, expectRanRun: true},
	}
}

// newExtractWorker builds a Worker on a fresh test database whose LLM answers resp and
// records every Idempotency-Key it receives.
func newExtractWorker(t *testing.T, tc covenThreadExtractCase, llmKeys *[]string) (*Worker, *sql.DB) {
	t.Helper()
	if tc.nilWorker {
		return nil, nil
	}
	db := newTestDB(t)
	seedTurns(t, db, tc.seedTurns...)
	if tc.seedFact {
		seedFacts(t, store.NewSQLStore(db), []string{"Alex prefers oat milk in every latte"}, [][]float32{testEmbeddingAt(0, 1)})
	}
	execSQL(t, db, tc.sabotage)
	resp := cmp.Or(tc.llmResp, `{"facts":[]}`)
	worker := NewWorker(WorkerConfig{
		Store:  store.NewSQLStore(db),
		Ollama: &MockOllamaClient{embedding: testEmbeddingAt(5, 1)},
		LLMClient: func(ctx context.Context, prompt string) (string, error) {
			*llmKeys = append(*llmKeys, llmKeyFrom(ctx))
			return resp, nil
		},
	})
	if tc.closeDB {
		require.NoError(t, db.Close())
	}
	return worker, db
}

// stubExtractAttemptState backs Get/Set/Clear of the attempt state key with *state.
func stubExtractAttemptState(mc *mocks.MockContext, state *extractAttemptState) {
	mc.On("Get", extractAttemptStateKey, mock.Anything).Run(func(args mock.Arguments) {
		if out, ok := args.Get(1).(*extractAttemptState); ok {
			*out = *state
		}
	}).Return(true, nil).Maybe()
	mc.On("Set", extractAttemptStateKey, mock.Anything).Run(func(args mock.Arguments) {
		if v, ok := args.Get(1).(extractAttemptState); ok {
			*state = v
		}
	}).Return().Maybe()
	mc.On("Clear", extractAttemptStateKey).Run(func(mock.Arguments) {
		*state = extractAttemptState{}
	}).Return().Maybe()
}

func TestCovenThread_Extract(t *testing.T) {
	t.Parallel()
	for _, tc := range covenThreadExtractCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var llmKeys []string
			worker, db := newExtractWorker(t, tc, &llmKeys)

			mc := mocks.NewMockContext(t)
			mc.On("Key").Return(tc.key).Maybe()
			state := tc.initialState
			stubExtractAttemptState(mc, &state)
			rec := stubCovenRun(mc, true)

			err := NewCovenThread(worker).Extract(restate.WithMockContext(mc), tc.req)
			if tc.wantErrSub != "" {
				require.ErrorContains(t, err, tc.wantErrSub)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectRanRun, rec.ran)
			require.False(t, rec.hasRetryCap, "CovenThread.Extract must not cap retry attempts")
			require.Equal(t, tc.wantTerminal, rec.isTerminal, "terminal classification of the last step error")
			require.Equal(t, tc.wantStepErr, rec.fnErr != nil, "last step error = %v", rec.fnErr)
			require.Equal(t, tc.wantLLMKeys, llmKeys)
			require.Equal(t, tc.wantState, state, "attempt state after the invocation")
			if tc.wantWatermark > 0 {
				require.Equal(t, tc.wantWatermark, readExtractWatermark(t, db, tc.wantAgent, tc.wantThread))
			}
			if tc.checkFacts {
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

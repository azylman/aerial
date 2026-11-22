package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pgtest"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/coven/internal/config"
	"github.com/azylman/aerial/services/coven/internal/store"
)

// mainBusFactory returns an initCovenJetStreamWithFactory factory that hands out b.
func mainBusFactory(b pubsub.Bus) func(context.Context, string) (pubsub.Bus, error) {
	return func(context.Context, string) (pubsub.Bus, error) { return b, nil }
}

// mainWaitHandled returns the next handler result reported on handled.
func mainWaitHandled(t *testing.T, handled chan error) error {
	t.Helper()
	select {
	case err := <-handled:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a handler to finish")
		return nil
	}
}

func TestCoven_InitJetStreamEdgeCases(t *testing.T) {
	ctx := t.Context()

	if bus := initCovenJetStream(ctx, nil, ""); bus != nil {
		t.Error("expected nil bus for empty natsURL")
	}

	errFactory := func(_ context.Context, _ string) (pubsub.Bus, error) {
		return nil, errors.New("connection failed")
	}
	if bus := initCovenJetStreamWithFactory(ctx, nil, "nats://mock", errFactory); bus != nil {
		t.Error("expected nil bus when factory returns error")
	}

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	// Ensure stream failure logs warning but returns bus
	brokenEnsure := &pubsubtest.FaultBus{Bus: memBus, EnsureErr: errors.New("ensure core streams failed")}
	if bus := initCovenJetStreamWithFactory(ctx, nil, "nats://mock", mainBusFactory(brokenEnsure)); bus == nil {
		t.Error("expected non-nil bus even if ensure stream fails")
	}

	// Subscribe failure logs warning, starts the background retry and returns bus
	worker := NewWorker(WorkerConfig{})
	brokenSub := &pubsubtest.FaultBus{Bus: memBus, SubscribeErr: errors.New("subscribe failed")}
	subCtx, subCancel := context.WithCancel(ctx)
	if bus := initCovenJetStreamWithFactory(subCtx, worker, "nats://mock", mainBusFactory(brokenSub)); bus == nil {
		t.Error("expected non-nil bus even if subscribe fails")
	}
	subCancel()

	// Unmarshal error on bad JSON payload
	wrapped := &pubsubtest.FaultBus{Bus: memBus, Handled: make(chan error, 1)}
	if bus := initCovenJetStreamWithFactory(ctx, worker, "nats://mock", mainBusFactory(wrapped)); bus == nil {
		t.Fatal("expected non-nil bus")
	}

	if err := memBus.Publish(ctx, pubsub.SubjectAerialTurnPrefix+"transcripts_indexed", "bad-sync-1", "not-a-valid-json"); err != nil {
		t.Fatalf("Publish bad json: %v", err)
	}
	if err := mainWaitHandled(t, wrapped.Handled); err != nil {
		t.Errorf("expected nil error on bad json unmarshal to avoid poison redelivery loop, got %v", err)
	}
}

func setSubscribeRetryIntervalForTest(d time.Duration) func() {
	prev := subscribeRetryIntervalNs.Swap(int64(d))
	return func() {
		subscribeRetryIntervalNs.Store(prev)
	}
}

func TestCoven_RetrySubscribeCovenTurns(t *testing.T) {
	restore := setSubscribeRetryIntervalForTest(5 * time.Millisecond)
	defer restore()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	consumers := &covenConsumers{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		retrySubscribeCovenConsumers(t.Context(), memBus, NewWorker(WorkerConfig{}), consumers)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for background retry to subscribe coven consumers")
	}
	if !consumers.subscribedTranscriptsIndexed || !consumers.subscribedExtract {
		t.Errorf("consumers = %+v, want both subscribed", *consumers)
	}
}

type handleCovenExtractJobTestCase struct {
	name           string
	msgData        string
	delivered      uint64
	role           string
	sabotage       string
	ingressURL     string
	wantErr        bool
	wantErrIs      error
	wantTerm       bool
	wantMinNak     time.Duration
	wantExactNak   time.Duration
	wantWatermarks map[string]int64
	wantFacts      []toolFactRow
}

// extractJobLLM extracts one admissible fact from every turn batch.
func extractJobLLM(context.Context, string) (string, error) {
	return `{"facts":[{"fact_text":"User always drinks oolong tea"}]}`, nil
}

// extractJobWatermarks reads extraction watermarks for thread th-ext-1 keyed by agent.
func extractJobWatermarks(t *testing.T, db store.DBTX) map[string]int64 {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), `SELECT agent, last_extracted_rowid FROM extraction_watermarks WHERE thread_id = 'th-ext-1'`)
	if err != nil {
		t.Fatalf("query watermarks: %v", err)
	}
	defer rows.Close()
	got := map[string]int64{}
	for rows.Next() {
		var agent string
		var rowID int64
		if err := rows.Scan(&agent, &rowID); err != nil {
			t.Fatalf("scan watermark: %v", err)
		}
		got[agent] = rowID
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read watermarks: %v", err)
	}
	return got
}

func runHandleCovenExtractJobCase(t *testing.T, tc handleCovenExtractJobTestCase) {
	t.Helper()
	ctx := context.Background()
	db := newTestDB(t)
	// One extractable admin turn per agent: aerial row 35, agent-alpha row 36.
	seedTurns(t, db,
		adminTurn("th-ext-1", 35, "test message", "resp"),
		testTurn{agent: "agent-alpha", thread: "th-ext-1", rowID: 36, content: "alpha message", response: "alpha resp", admin: true},
	)
	execSQL(t, db, tc.sabotage)
	worker := NewWorker(WorkerConfig{
		Role:              tc.role,
		Store:             store.NewSQLStore(db),
		Ollama:            &MockOllamaClient{embedding: testEmbedding(1.0)},
		LLMClient:         extractJobLLM,
		RestateIngressURL: tc.ingressURL,
	})
	msg := &pubsubtest.Message{
		Payload:   []byte(tc.msgData),
		Delivered: tc.delivered,
		Hdr:       nats.Header{pubsub.HeaderMsgID: []string{"msg-test-1"}},
	}

	err := handleCovenExtractJob(ctx, msg, worker)
	if (err != nil) != tc.wantErr {
		t.Fatalf("handleCovenExtractJob() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErrIs != nil && !errors.Is(err, tc.wantErrIs) {
		t.Errorf("err = %v, want %v", err, tc.wantErrIs)
	}
	if msg.Termed() != tc.wantTerm {
		t.Errorf("termed = %v, want %v", msg.Termed(), tc.wantTerm)
	}
	nakDelay, naked := msg.NakDelay()
	if wantNak := tc.wantMinNak > 0 || tc.wantExactNak > 0; naked != wantNak {
		t.Errorf("naked = %v, want %v", naked, wantNak)
	}
	if tc.wantMinNak > 0 && nakDelay < tc.wantMinNak {
		t.Errorf("nakDelay = %v, want >= %v", nakDelay, tc.wantMinNak)
	}
	if tc.wantExactNak > 0 && nakDelay != tc.wantExactNak {
		t.Errorf("nakDelay = %v, want %v", nakDelay, tc.wantExactNak)
	}
	if got := extractJobWatermarks(t, db); !maps.Equal(got, tc.wantWatermarks) {
		t.Errorf("watermarks = %v, want %v", got, tc.wantWatermarks)
	}
	if got := toolFactRows(t, db); !slices.Equal(got, tc.wantFacts) {
		t.Errorf("facts = %+v, want %+v", got, tc.wantFacts)
	}
}

func TestHandleCovenExtractJob_Scenarios(t *testing.T) {
	t.Parallel()
	none := map[string]int64{}
	aerialFact := []toolFactRow{{id: 1, text: "User always drinks oolong tea", reinforceCount: 1, sourceAgent: "aerial"}}

	restateSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "th-ext-fail") {
			http.Error(w, `{"error":"restate invoke error"}`, http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(restateSrv.Close)

	tests := []handleCovenExtractJobTestCase{
		{name: "UnmarshalErrorTermsMessageAndReturnsNil", msgData: "bad-json", delivered: 1, wantTerm: true, wantWatermarks: none},
		{name: "EmptyThreadIDTermsMessageAndReturnsNil", msgData: `{"thread_id":"   "}`, delivered: 1, wantTerm: true, wantWatermarks: none},
		{name: "MismatchedAgentSkipsExtraction", msgData: `{"thread_id":"th-ext-1","agent":"agent-alpha"}`, delivered: 1, wantWatermarks: none},
		{name: "RoleAllAcceptsAnyValidAgent", msgData: `{"thread_id":"th-ext-1","agent":"agent-alpha"}`, delivered: 1, role: "all", wantWatermarks: map[string]int64{"agent-alpha": 36}, wantFacts: []toolFactRow{{id: 1, text: "User always drinks oolong tea", reinforceCount: 1, sourceAgent: "agent-alpha"}}},
		{name: "WorkerSuccessAdvancesWatermark", msgData: `{"thread_id":"th-ext-1"}`, delivered: 1, wantWatermarks: map[string]int64{"aerial": 35}, wantFacts: aerialFact},
		{name: "WorkerErrorAttempt1NaksWithBackoff", msgData: `{"thread_id":"th-ext-1"}`, delivered: 1, sabotage: sabotageTurnReads, wantErr: true, wantMinNak: 1 * time.Second, wantWatermarks: none},
		{name: "WorkerErrorAttempt4NaksWithBackoff", msgData: `{"thread_id":"th-ext-1"}`, delivered: 4, sabotage: sabotageTurnReads, wantErr: true, wantMinNak: 8 * time.Second, wantWatermarks: none},
		{name: "WorkerErrorAttempt5NaksWithBackoff", msgData: `{"thread_id":"th-ext-1"}`, delivered: 5, sabotage: sabotageTurnReads, wantErr: true, wantMinNak: 16 * time.Second, wantWatermarks: none},
		{name: "RestateIngressSuccess", msgData: `{"thread_id":"th-ext-1"}`, delivered: 1, ingressURL: restateSrv.URL, wantWatermarks: none},
		{name: "RestateIngressErrorNaksWithBackoff", msgData: `{"thread_id":"th-ext-fail"}`, delivered: 2, ingressURL: restateSrv.URL, wantErr: true, wantMinNak: 2 * time.Second, wantWatermarks: none},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runHandleCovenExtractJobCase(t, tc)
		})
	}
}

func TestCoven_TranscriptsIndexedToCoalescedExtractIntegration(t *testing.T) {
	ctx := context.Background()

	db := newTestDB(t)
	seedTurns(t, db, adminTurn("th-e2e-1", 88, "I prefer matcha", "Noted"))
	s := store.NewSQLStore(db)

	client := &MockOllamaClient{embedding: testEmbedding(1.0)}
	llm := func(ctx context.Context, prompt string) (string, error) {
		return `{"facts":[]}`, nil
	}

	worker := NewWorker(WorkerConfig{
		Store:     s,
		Ollama:    client,
		LLMClient: llm,
	})

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	// Handled reports the transcripts-indexed handler and the coalesced extract handler.
	wrapped := &pubsubtest.FaultBus{Bus: memBus, Handled: make(chan error, 2)}
	if bus := initCovenJetStreamWithFactory(ctx, worker, "nats://mock", mainBusFactory(wrapped)); bus == nil {
		t.Fatal("expected non-nil bus")
	}

	ev := pubsub.TranscriptsIndexedEvent{
		Stack:        "aerial",
		SessionID:    "sess-e2e-1",
		ThreadID:     "th-e2e-1",
		StepsIndexed: 2,
		Final:        true,
		IndexedAt:    time.Now().UTC(),
	}
	if err := memBus.Publish(ctx, pubsub.SubjectAerialTurnPrefix+"transcripts_indexed", "sync-1", ev); err != nil {
		t.Fatalf("Publish TranscriptsIndexed: %v", err)
	}

	for range 2 {
		if err := mainWaitHandled(t, wrapped.Handled); err != nil {
			t.Fatalf("handler err = %v", err)
		}
	}

	wm, _, err := s.GetConversationFactWatermark(ctx, "th-e2e-1")
	if err != nil {
		t.Fatalf("GetConversationFactWatermark: %v", err)
	}
	if wm != 88 {
		t.Errorf("watermark = %d, want 88", wm)
	}
}

func TestCoven_SubscriptionEdgeCases(t *testing.T) {
	ctx := t.Context()

	worker := NewWorker(WorkerConfig{})
	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	// Nil bus or nil worker returns nil safely
	if err := subscribeCovenTranscriptsIndexed(ctx, nil, "aerial"); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if err := subscribeCovenExtract(ctx, nil, worker); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if err := subscribeCovenExtract(ctx, memBus, nil); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if err := (&covenConsumers{}).subscribeAll(ctx, nil, worker); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if err := (&covenConsumers{}).subscribeAll(ctx, memBus, nil); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}

	// Broken subscribe returns error
	brokenSub := &pubsubtest.FaultBus{Bus: memBus, SubscribeErr: errors.New("subscribe failed")}
	if err := subscribeCovenTranscriptsIndexed(ctx, brokenSub, "aerial"); err == nil {
		t.Error("expected error when subscribe fails")
	}
	if err := subscribeCovenExtract(ctx, brokenSub, worker); err == nil {
		t.Error("expected error when subscribe fails")
	}
	// Fails on coven-transcripts-indexed, the first consumer.
	if err := (&covenConsumers{}).subscribeAll(ctx, brokenSub, worker); err == nil {
		t.Error("expected error when subscribe coven-transcripts-indexed fails")
	}
	// Indexed already subscribed, so the failure comes from coven-extract.
	if err := (&covenConsumers{subscribedTranscriptsIndexed: true}).subscribeAll(ctx, brokenSub, worker); err == nil {
		t.Error("expected error when subscribe coven-extract fails")
	}

	// Canceled retry exits cleanly
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	consumers := &covenConsumers{}
	retrySubscribeCovenConsumers(canceledCtx, memBus, worker, consumers)
}

func TestCovenDurableNamesAndFilters(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name               string
		stack              string
		wantIndexedDurable string
		wantExtractDurable string
		wantExtractFilter  string
	}{
		{
			name:               "aerial stack defaults",
			stack:              "aerial",
			wantIndexedDurable: "coven-transcripts-indexed",
			wantExtractDurable: "coven-extract",
			wantExtractFilter:  "aerial.coalesced.coven_extract.>",
		},
		{
			name:               "empty stack defaults to aerial",
			stack:              "",
			wantIndexedDurable: "coven-transcripts-indexed",
			wantExtractDurable: "coven-extract",
			wantExtractFilter:  "aerial.coalesced.coven_extract.>",
		},
		{
			name:               "agent-alpha stack scoped",
			stack:              "agent-alpha",
			wantIndexedDurable: "coven-transcripts-indexed-agent-alpha",
			wantExtractDurable: "coven-extract-agent-alpha",
			wantExtractFilter:  "aerial.coalesced.coven_extract.agent-alpha.>",
		},
		{
			name:               "agent-beta stack scoped",
			stack:              "agent-beta",
			wantIndexedDurable: "coven-transcripts-indexed-agent-beta",
			wantExtractDurable: "coven-extract-agent-beta",
			wantExtractFilter:  "aerial.coalesced.coven_extract.agent-beta.>",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := covenTranscriptsIndexedDurable(tc.stack); got != tc.wantIndexedDurable {
				t.Errorf("covenTranscriptsIndexedDurable(%q) = %q, want %q", tc.stack, got, tc.wantIndexedDurable)
			}
			if got := covenExtractDurable(tc.stack); got != tc.wantExtractDurable {
				t.Errorf("covenExtractDurable(%q) = %q, want %q", tc.stack, got, tc.wantExtractDurable)
			}
			if got := covenExtractSubjectFilter(tc.stack); got != tc.wantExtractFilter {
				t.Errorf("covenExtractSubjectFilter(%q) = %q, want %q", tc.stack, got, tc.wantExtractFilter)
			}
		})
	}
}

func TestRunApp_Lifecycle(t *testing.T) {
	validDSN := pgtest.NewDSN(t)

	tests := []struct {
		name    string
		cfg     *config.Config
		wantErr bool
	}{
		{
			name:    "NilConfigFails",
			cfg:     nil,
			wantErr: true,
		},
		{
			name:    "InvalidDBFails",
			cfg:     &config.Config{DatabaseURL: "mysql://x"},
			wantErr: true,
		},
		{
			name:    "CleanLifecycleWithPostgres",
			cfg:     &config.Config{Port: "0", RestatePort: "0", DatabaseURL: validDSN},
			wantErr: false,
		},
		{
			name:    "CleanLifecycleExtractorWithTranscriptsURL",
			cfg:     &config.Config{Port: "0", RestatePort: "0", DatabaseURL: validDSN, TranscriptsURL: "http://mock-transcripts", Role: "extractor"},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := RunApp(ctx, tc.cfg)
			if tc.wantErr && err == nil {
				t.Fatal("expected error, got nil")
			}
			if !tc.wantErr && err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Fatalf("RunApp() unexpected err = %v", err)
			}
		})
	}
}

// covenRunContext is the restate.RunContext stub.
type covenRunContext struct {
	context.Context
}

func (covenRunContext) Log() *slog.Logger         { return slog.Default() }
func (covenRunContext) Request() *restate.Request { return nil }

type covenRunCapture struct {
	ran         bool
	fnErr       error
	isTerminal  bool
	hasRetryCap bool
}

func stubCovenRun(mc *mocks.MockContext, keepTerminal bool) *covenRunCapture {
	rec := &covenRunCapture{}
	run := func(fn func(restate.RunContext) (any, error), output any, opts ...restate.RunOption) restate.TerminalError {
		rec.ran = true
		for _, opt := range opts {
			if strings.Contains(fmt.Sprintf("%T", opt), "withMaxRetryAttempts") {
				rec.hasRetryCap = true
			}
		}
		res, err := fn(covenRunContext{Context: context.Background()})
		rec.fnErr = err
		rec.isTerminal = restate.IsTerminalError(err)
		switch {
		case err != nil && keepTerminal && restate.IsTerminalError(err):
			return restate.AsTerminalError(err)
		case err != nil && keepTerminal && rec.hasRetryCap:
			return restate.TerminalErrorf("retries exhausted: %v", err)
		case err != nil && keepTerminal:
			return nil
		case err != nil:
			return restate.TerminalErrorf("%v", err)
		}
		if out := reflect.ValueOf(output); res != nil && out.Kind() == reflect.Pointer && !out.IsNil() {
			out.Elem().Set(reflect.ValueOf(res))
		}
		return nil
	}
	for arity := 2; arity <= 6; arity++ {
		mc.On("Run", slices.Repeat([]any{mock.Anything}, arity)...).Return(run).Maybe()
	}
	return rec
}

type covenSentCall struct {
	service string
	key     string
	method  string
	input   any
	delayed bool
	option  restate.SendOption
}

type covenSendCapture struct {
	mu    sync.Mutex
	calls []covenSentCall
}

func (c *covenSendCapture) record(args mock.Arguments) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, covenSentCall{service: args.String(0), key: args.String(1), method: args.String(2)})
}

func (c *covenSendCapture) complete(input any, delayed bool, option restate.SendOption) {
	c.mu.Lock()
	defer c.mu.Unlock()
	last := &c.calls[len(c.calls)-1]
	last.input = input
	last.delayed = delayed
	last.option = option
}

func (c *covenSendCapture) snapshot() []covenSentCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]covenSentCall(nil), c.calls...)
}

func captureCovenSends(t *testing.T, mc *mocks.MockContext) *covenSendCapture {
	t.Helper()
	rec := &covenSendCapture{}
	client := mocks.NewMockClient(t)
	invocation := mocks.NewMockInvocation(t)
	mc.On("Object", mock.Anything, mock.Anything, mock.Anything).Run(rec.record).Return(client).Maybe()
	client.On("Send", mock.Anything).Run(func(args mock.Arguments) { rec.complete(args.Get(0), false, nil) }).Return(invocation).Maybe()
	client.On("Send", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		var opt restate.SendOption
		if len(args) > 1 {
			if o, ok := args.Get(1).(restate.SendOption); ok {
				opt = o
			}
		}
		rec.complete(args.Get(0), true, opt)
	}).Return(invocation).Maybe()
	return rec
}

func TestNewRestateServer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		thread      *CovenThread
		dreamer     *CovenDreamer
		wantThread  bool
		wantDreamer bool
	}{
		{
			name:        "both bound",
			thread:      NewCovenThread(nil),
			dreamer:     NewCovenDreamer(nil),
			wantThread:  true,
			wantDreamer: true,
		},
		{
			name:        "only thread bound",
			thread:      NewCovenThread(nil),
			dreamer:     nil,
			wantThread:  true,
			wantDreamer: false,
		},
		{
			name:        "only dreamer bound",
			thread:      nil,
			dreamer:     NewCovenDreamer(nil),
			wantThread:  false,
			wantDreamer: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := NewRestateServer(tc.thread, tc.dreamer)
			require.NotNil(t, srv)
			h, err := srv.Handler()
			require.NoError(t, err)

			req := httptest.NewRequest(http.MethodGet, "/discover", http.NoBody)
			req.Header.Set("Accept", "application/vnd.restate.endpointmanifest.v3+json")
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, req)

			var manifest struct {
				Services []struct {
					Name string `json:"name"`
				} `json:"services"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &manifest))

			hasThread := false
			hasDreamer := false
			for _, s := range manifest.Services {
				if s.Name == "CovenThread" {
					hasThread = true
				}
				if s.Name == "CovenDreamer" {
					hasDreamer = true
				}
			}
			require.Equal(t, tc.wantThread, hasThread, "CovenThread binding mismatch")
			require.Equal(t, tc.wantDreamer, hasDreamer, "CovenDreamer binding mismatch")
		})
	}
}

func TestCovenThread_Extract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		key           string
		req           ExtractJobRequest
		nilWorker     bool
		failStore     string
		seedTurns     []testTurn
		wantAgent     string
		wantThread    string
		wantWatermark int64
		wantErrSub    string
		wantTerminal  bool
		expectRanRun  bool
	}{
		{
			name:         "empty thread id returns nil without running step",
			key:          "",
			req:          ExtractJobRequest{},
			expectRanRun: false,
		},
		{
			name:         "nil worker returns error inside run step",
			key:          "aerial/th-err",
			req:          ExtractJobRequest{},
			nilWorker:    true,
			wantErrSub:   "coven worker is nil",
			wantTerminal: true,
			expectRanRun: true,
		},
		{
			name:         "transient extraction error is not terminal and keeps extraction retryable",
			key:          "aerial/th-fail",
			req:          ExtractJobRequest{},
			failStore:    "GetConversationFactWatermark",
			wantTerminal: false,
			expectRanRun: true,
		},
		{
			name:          "valid key extracts successfully",
			key:           "aerial/th-ok",
			req:           ExtractJobRequest{},
			seedTurns:     []testTurn{adminTurn("th-ok", 1, "hi", "hello")},
			wantAgent:     "aerial",
			wantThread:    "th-ok",
			wantWatermark: 1,
			expectRanRun:  true,
		},
		{
			name: "request agent and thread override key",
			key:  "aerial/th-fallback",
			req:  ExtractJobRequest{Agent: "custom-agent", ThreadID: "th-override"},
			seedTurns: []testTurn{
				{agent: "custom-agent", thread: "th-override", rowID: 1, content: "hi", response: "hello", status: "COMPLETED", admin: true},
			},
			wantAgent:     "custom-agent",
			wantThread:    "th-override",
			wantWatermark: 1,
			expectRanRun:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var worker *Worker
			var db store.DBTX
			if !tc.nilWorker {
				testDB := newTestDB(t)
				db = testDB
				if len(tc.seedTurns) > 0 {
					seedTurns(t, testDB, tc.seedTurns...)
				}
				if tc.failStore != "" {
					execSQL(t, testDB, sabotageWatermarks)
				}
				worker = NewWorker(WorkerConfig{
					Store: store.NewSQLStore(testDB),
					LLMClient: func(ctx context.Context, prompt string) (string, error) {
						return `{"facts":[]}`, nil
					},
				})
			}

			mc := mocks.NewMockContext(t)
			mc.On("Key").Return(tc.key).Maybe()
			rec := stubCovenRun(mc, true)

			ct := NewCovenThread(worker)
			err := ct.Extract(restate.WithMockContext(mc), tc.req)
			if tc.wantErrSub != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tc.wantErrSub)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.expectRanRun, rec.ran)
			if tc.expectRanRun {
				require.False(t, rec.hasRetryCap, "CovenThread.Extract must not cap retry attempts")
			}
			if tc.wantTerminal {
				require.True(t, rec.isTerminal, "expected terminal error inside run step")
			} else if tc.failStore != "" {
				require.False(t, rec.isTerminal, "transient extraction failure must not be terminal error")
				require.Error(t, rec.fnErr, "expected ExtractThread error")
			}
			if tc.wantWatermark > 0 {
				require.Equal(t, tc.wantWatermark, readExtractWatermark(t, db, tc.wantAgent, tc.wantThread))
				if tc.name == "request agent and thread override key" {
					require.Equal(t, int64(0), readExtractWatermark(t, db, "aerial", "th-fallback"))
				}
			}
		})
	}
}

type covenDreamerArmTestCase struct {
	name         string
	key          string
	alreadyArmed bool
	failGet      bool
	nilWorker    bool
	nilStore     bool
	failRun      error
	lastRunAt    *time.Time
	wantErrSub   string
	wantDelayed  bool
	wantStack    string
	wantSend     bool
}

func runCovenDreamerArmCase(t *testing.T, tc covenDreamerArmTestCase) {
	t.Helper()
	var worker *Worker
	if !tc.nilWorker {
		var s store.Store
		if !tc.nilStore {
			db := newTestDB(t)
			if tc.lastRunAt != nil {
				seedWatermark(t, db, config.NormalizeStack(tc.key), DreamingWatermarkThreadID, 1, tc.lastRunAt)
			}
			s = store.NewSQLStore(db)
		}
		worker = NewWorker(WorkerConfig{Store: s})
	}

	mc := mocks.NewMockContext(t)
	mc.On("Key").Return(tc.key).Maybe()
	sends := captureCovenSends(t, mc)
	switch {
	case tc.failGet:
		mc.On("Get", "armed", mock.Anything).Return(false, restate.ToTerminalError(errors.New("state load error")))
	case tc.alreadyArmed:
		mc.On("Get", "armed", mock.Anything).Run(func(args mock.Arguments) {
			if pp, ok := args.Get(1).(*bool); ok && pp != nil {
				*pp = true
			}
		}).Return(true, nil)
	default:
		mc.On("Get", "armed", mock.Anything).Return(false, nil).Maybe()
		mc.On("Set", "armed", true).Maybe()
	}
	if tc.failRun != nil {
		mc.On("Run", mock.Anything, mock.Anything, mock.Anything).Return(restate.TerminalErrorf("%v", tc.failRun))
	} else {
		stubCovenRun(mc, true)
	}

	dreamer := NewCovenDreamer(worker)
	err := dreamer.Arm(restate.WithMockContext(mc), struct{}{})
	if tc.wantErrSub != "" {
		require.Error(t, err)
		require.Contains(t, err.Error(), tc.wantErrSub)
		return
	}
	require.NoError(t, err)

	calls := sends.snapshot()
	if !tc.wantSend {
		require.Empty(t, calls)
		return
	}
	require.Len(t, calls, 1)
	require.Equal(t, "CovenDreamer", calls[0].service)
	require.Equal(t, tc.wantStack, calls[0].key)
	require.Equal(t, "Dream", calls[0].method)
	require.Equal(t, tc.wantDelayed, calls[0].delayed)
}

func TestCovenDreamer_Arm(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	recent := now.Add(-2 * time.Hour)
	stale := now.Add(-25 * time.Hour)

	tests := []covenDreamerArmTestCase{
		{
			name:         "already armed does nothing and sends no invocation",
			key:          "aerial",
			alreadyArmed: true,
			wantSend:     false,
		},
		{
			name:       "get armed error propagates error",
			key:        "aerial",
			failGet:    true,
			wantErrSub: "state load error",
		},
		{
			name:        "nil worker defaults delay to min interval",
			key:         "",
			nilWorker:   true,
			wantDelayed: true,
			wantStack:   config.DefaultStack,
			wantSend:    true,
		},
		{
			name:        "nil store defaults delay to min interval",
			key:         "agent-beta",
			nilStore:    true,
			wantDelayed: true,
			wantStack:   "agent-beta",
			wantSend:    true,
		},
		{
			name:        "zero watermark runs immediately without delay",
			key:         "aerial",
			lastRunAt:   nil,
			wantDelayed: false,
			wantStack:   "aerial",
			wantSend:    true,
		},
		{
			name:        "recent watermark arms with remaining delay",
			key:         "aerial",
			lastRunAt:   &recent,
			wantDelayed: true,
			wantStack:   "aerial",
			wantSend:    true,
		},
		{
			name:        "stale watermark runs immediately without delay",
			key:         "aerial",
			lastRunAt:   &stale,
			wantDelayed: false,
			wantStack:   "aerial",
			wantSend:    true,
		},
		{
			name:       "compute delay error propagates error",
			key:        "aerial",
			failRun:    errors.New("db compute error"),
			wantErrSub: "db compute error",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runCovenDreamerArmCase(t, tc)
		})
	}
}

func TestCovenDreamer_Dream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		key             string
		nilWorker       bool
		nilDreamer      bool
		failOp          string
		initialFailures int
		wantStack       string
		wantDelay       time.Duration
		wantErr         bool
	}{
		{
			name:       "nil dreamer returns error",
			nilDreamer: true,
			wantErr:    true,
		},
		{
			name:      "nil worker runs safely and schedules next cycle",
			key:       "",
			nilWorker: true,
			wantStack: config.DefaultStack,
			wantDelay: DefaultDreamingMinInterval,
		},
		{
			name:      "worker with components runs consolidation and schedules next cycle",
			key:       "stack-alpha",
			nilWorker: false,
			wantStack: "stack-alpha",
			wantDelay: DefaultDreamingMinInterval,
		},
		{
			name:      "consolidate facts error logs failure and schedules retry",
			key:       "stack-err",
			failOp:    "GetAllFactsWithEmbeddings",
			wantStack: "stack-err",
			wantDelay: DefaultDreamingRetryInterval,
			wantErr:   false,
		},
		{
			name:            "consolidate facts consecutive errors applies exponential backoff",
			key:             "stack-err-backoff",
			failOp:          "GetAllFactsWithEmbeddings",
			initialFailures: 2,
			wantStack:       "stack-err-backoff",
			wantDelay:       4 * DefaultDreamingRetryInterval,
			wantErr:         false,
		},
		{
			name:      "consolidate skills error logs failure and schedules retry",
			key:       "stack-err-skills",
			failOp:    "GetPendingProceduralCandidates",
			wantStack: "stack-err-skills",
			wantDelay: DefaultDreamingRetryInterval,
			wantErr:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mc := mocks.NewMockContext(t)
			mc.On("Key").Return(tc.key).Maybe()
			mc.On("Set", "armed", true).Maybe()
			failureCount := tc.initialFailures
			mc.On("Get", "failures", mock.Anything).Run(func(args mock.Arguments) {
				if outPtr, ok := args.Get(1).(*int); ok && outPtr != nil {
					*outPtr = failureCount
				}
			}).Return(true, nil).Maybe()
			mc.On("Set", "failures", mock.Anything).Run(func(args mock.Arguments) {
				if val, ok := args.Get(1).(int); ok {
					failureCount = val
				}
			}).Return().Maybe()
			mc.On("Clear", "failures").Run(func(args mock.Arguments) {
				failureCount = 0
			}).Return().Maybe()
			sends := captureCovenSends(t, mc)
			stubCovenRun(mc, true)

			var dreamer *CovenDreamer
			if !tc.nilDreamer {
				var worker *Worker
				if !tc.nilWorker {
					db := newTestDB(t)
					switch tc.failOp {
					case "GetAllFactsWithEmbeddings":
						execSQL(t, db, sabotageFacts)
					case "GetPendingProceduralCandidates":
						execSQL(t, db, sabotageProcedural)
					}
					worker = NewWorker(WorkerConfig{
						Store:  store.NewSQLStore(db),
						Ollama: &MockOllamaClient{embedding: testEmbedding(1.0)},
						LLMClient: func(ctx context.Context, prompt string) (string, error) {
							return `{"facts":[],"clusters":[]}`, nil
						},
						Publisher: &mockSkillPublisher{prNum: 101},
					})
				}
				dreamer = NewCovenDreamer(worker)
			}

			err := dreamer.Dream(restate.WithMockContext(mc), struct{}{})
			if tc.wantErr {
				require.Error(t, err)
				if !tc.nilDreamer {
					calls := sends.snapshot()
					require.Len(t, calls, 1)
					require.Equal(t, "CovenDreamer", calls[0].service)
					require.Equal(t, tc.wantStack, calls[0].key)
					require.Equal(t, "Dream", calls[0].method)
					require.True(t, calls[0].delayed)
					require.Equal(t, restate.WithDelay(tc.wantDelay), calls[0].option)
				}
				return
			}
			require.NoError(t, err)

			calls := sends.snapshot()
			require.Len(t, calls, 1)
			require.Equal(t, "CovenDreamer", calls[0].service)
			require.Equal(t, tc.wantStack, calls[0].key)
			require.Equal(t, "Dream", calls[0].method)
			require.True(t, calls[0].delayed)
			require.Equal(t, restate.WithDelay(tc.wantDelay), calls[0].option)
		})
	}
}

func TestWorker_StartDreamerArm_Success(t *testing.T) {
	t.Parallel()

	armHandled := make(chan struct{})
	var gotArmEndpoint string
	var gotArmIdempotency string
	ingressServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/CovenDreamer/aerial/Arm") {
			gotArmEndpoint = r.URL.Path
			gotArmIdempotency = r.Header.Get("Idempotency-Key")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
			select {
			case <-armHandled:
			default:
				close(armHandled)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ingressServer.Close()

	wArm := NewWorker(WorkerConfig{
		Stack:             "aerial",
		RestateIngressURL: ingressServer.URL,
		HTTPClient:        ingressServer.Client(),
	})

	ctxArm, cancelArm := context.WithCancel(context.Background())
	go func() {
		select {
		case <-armHandled:
		case <-time.After(2 * time.Second):
		}
		cancelArm()
	}()
	_ = wArm.Start(ctxArm)

	select {
	case <-armHandled:
	default:
		t.Fatal("expected arm to be called")
	}
	require.Equal(t, "/CovenDreamer/aerial/Arm", gotArmEndpoint)
	require.Equal(t, "coven:dreamer:arm:aerial", gotArmIdempotency)
}

func TestWorker_StartDreamerArm_Retry(t *testing.T) {
	t.Parallel()

	armRetryAttempts := atomic.Int32{}
	armRetryHandled := make(chan struct{})
	ingressRetryServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/CovenDreamer/aerial/Arm") {
			if armRetryAttempts.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
			select {
			case <-armRetryHandled:
			default:
				close(armRetryHandled)
			}
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer ingressRetryServer.Close()

	wArmRetry := NewWorker(WorkerConfig{
		Stack:             "aerial",
		RestateIngressURL: ingressRetryServer.URL,
		HTTPClient:        ingressRetryServer.Client(),
		ArmRetryInterval:  5 * time.Millisecond,
	})
	ctxArmRetry, cancelArmRetry := context.WithCancel(context.Background())
	go func() {
		select {
		case <-armRetryHandled:
		case <-time.After(2 * time.Second):
		}
		cancelArmRetry()
	}()
	_ = wArmRetry.Start(ctxArmRetry)

	select {
	case <-armRetryHandled:
	default:
		t.Fatal("expected arm retry to succeed")
	}
	require.GreaterOrEqual(t, armRetryAttempts.Load(), int32(2))
}

func TestWorker_StartDreamerArm_ErrorAndFallbacks(t *testing.T) {
	t.Parallel()

	errHandled := make(chan struct{})
	ingressErrServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		select {
		case <-errHandled:
		default:
			close(errHandled)
		}
	}))
	defer ingressErrServer.Close()
	wArmErr := NewWorker(WorkerConfig{
		Stack:             "aerial",
		RestateIngressURL: ingressErrServer.URL,
		HTTPClient:        ingressErrServer.Client(),
		ArmRetryInterval:  5 * time.Millisecond,
	})
	ctxArmErr, cancelArmErr := context.WithCancel(context.Background())
	go func() {
		select {
		case <-errHandled:
		case <-time.After(2 * time.Second):
		}
		cancelArmErr()
	}()
	_ = wArmErr.Start(ctxArmErr)

	select {
	case <-errHandled:
	default:
		t.Fatal("expected arm error endpoint to be called")
	}

	// Extractor role exits immediately on cancellation
	wExtractor := NewWorker(WorkerConfig{Role: "extractor"})
	ctxExt, cancelExt := context.WithCancel(context.Background())
	cancelExt()
	require.ErrorIs(t, wExtractor.Start(ctxExt), context.Canceled)

	// Worker with empty RestateIngressURL falls back to in-process ticker loop and exits on ctx cancellation
	wNoIngress := NewWorker(WorkerConfig{
		Stack:             "aerial",
		RestateIngressURL: "",
		TickInterval:      5 * time.Millisecond,
	})
	ctxNoIngress, cancelNoIngress := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancelNoIngress)
	require.ErrorIs(t, wNoIngress.Start(ctxNoIngress), context.Canceled)

	// When fallbackMu is held, runDaily skips immediately without overlapping
	wNoIngress.fallbackMu.Lock()
	wNoIngress.runDaily(context.Background())
	wNoIngress.fallbackMu.Unlock()
}

func TestAdminExtract_RestateIngressExtractFn(t *testing.T) {
	t.Parallel()

	var gotEndpoint string
	var gotIdempotency string
	mockRestate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotEndpoint = r.URL.Path
		gotIdempotency = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer mockRestate.Close()

	cfg := &config.Config{
		Stack:             "custom-stack",
		RestateIngressURL: mockRestate.URL,
	}

	th := NewToolHandler(cfg, store.NewSQLStore(newTestDB(t)), nil)
	worker := NewWorker(WorkerConfig{
		Stack:             "custom-stack",
		RestateIngressURL: mockRestate.URL,
		HTTPClient:        mockRestate.Client(),
	})
	extractFn := newExtractFn(cfg, worker, th)
	server := NewServer(th, extractFn)

	// Unscoped thread ID defaults to cfg.EffectiveStack() ("custom-stack")
	req := httptest.NewRequest(http.MethodPost, "/admin/extract?thread_id=th-test-admin", http.NoBody)
	rr := httptest.NewRecorder()
	server.Routes().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
	require.Contains(t, gotEndpoint, "/CovenThread/custom-stack/th-test-admin/Extract/send")
	require.Empty(t, gotIdempotency)

	// Explicitly scoped with DefaultStack ("aerial/th-default") preserves "aerial", does not rewrite to custom-stack
	reqAerial := httptest.NewRequest(http.MethodPost, "/admin/extract?thread_id=aerial/th-default", http.NoBody)
	rrAerial := httptest.NewRecorder()
	server.Routes().ServeHTTP(rrAerial, reqAerial)
	require.Equal(t, http.StatusOK, rrAerial.Code)
	require.Contains(t, gotEndpoint, "/CovenThread/aerial/th-default/Extract/send")
}

func TestDispatchCovenExtract(t *testing.T) {
	t.Parallel()

	require.NoError(t, dispatchCovenExtract(context.Background(), nil, "aerial", "th-1"))

	var gotEndpoint string
	var gotIdempotency string
	mockRestate := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotEndpoint = r.URL.Path
		gotIdempotency = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer mockRestate.Close()

	// Direct extraction without ingress URL
	directStore := store.NewSQLStore(newTestDB(t))
	wDirect := NewWorker(WorkerConfig{
		Stack:  "aerial",
		Store:  directStore,
		Ollama: &MockOllamaClient{embedding: testEmbedding(1.0)},
		LLMClient: func(context.Context, string) (string, error) {
			return `{"facts":[]}`, nil
		},
	})
	require.NoError(t, dispatchCovenExtract(context.Background(), wDirect, "aerial", "th-direct"))

	// Ingress with explicit message ID sets Idempotency-Key header and routes to /send
	wIngress := NewWorker(WorkerConfig{
		Stack:             "aerial",
		RestateIngressURL: mockRestate.URL,
		HTTPClient:        mockRestate.Client(),
		Store:             directStore,
	})
	require.NoError(t, dispatchCovenExtract(context.Background(), wIngress, "aerial", "th-msg", "custom-msg-id-123"))
	require.Equal(t, "coven:extract:aerial/th-msg:custom-msg-id-123", gotIdempotency)
	require.Contains(t, gotEndpoint, "/CovenThread/aerial/th-msg/Extract/send")

	// Ingress without msgKey omits Idempotency-Key header so distinct events are not collapsed
	require.NoError(t, dispatchCovenExtract(context.Background(), wIngress, "aerial", "th-nomsg"))
	require.Empty(t, gotIdempotency)
}

package server

import (
	"context"
	"database/sql"
	"errors"
	"maps"
	"net/http"
	"slices"
	"testing"
	"time"

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
	heldXactLock   string
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

// holdXactAdvisoryLock takes pg_advisory_xact_lock(hashtext(key)) inside an
// open transaction on a second connection until the test ends.
func holdXactAdvisoryLock(t *testing.T, db *sql.DB, key string) {
	t.Helper()
	if key == "" {
		return
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("pin lock holder conn: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		_ = conn.Close()
	})
	execSQL(t, conn, "BEGIN")
	execSQL(t, conn, `SELECT pg_advisory_xact_lock(hashtext($1))`, key)
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
	holdXactAdvisoryLock(t, db, tc.heldXactLock)
	worker := NewWorker(WorkerConfig{
		Role:      tc.role,
		Store:     store.NewSQLStore(db),
		Ollama:    &MockOllamaClient{embedding: testEmbedding(1.0)},
		LLMClient: extractJobLLM,
	})
	msg := &pubsubtest.Message{Payload: []byte(tc.msgData), Delivered: tc.delivered}

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
	tests := []handleCovenExtractJobTestCase{
		{name: "UnmarshalErrorTermsMessageAndReturnsNil", msgData: "bad-json", delivered: 1, wantTerm: true, wantWatermarks: none},
		{name: "EmptyThreadIDTermsMessageAndReturnsNil", msgData: `{"thread_id":"   "}`, delivered: 1, wantTerm: true, wantWatermarks: none},
		{name: "MismatchedAgentSkipsExtraction", msgData: `{"thread_id":"th-ext-1","agent":"agent-alpha"}`, delivered: 1, wantWatermarks: none},
		{name: "RoleAllAcceptsAnyValidAgent", msgData: `{"thread_id":"th-ext-1","agent":"agent-alpha"}`, delivered: 1, role: "all", wantWatermarks: map[string]int64{"agent-alpha": 36}, wantFacts: []toolFactRow{{id: 1, text: "User always drinks oolong tea", reinforceCount: 1, sourceAgent: "agent-alpha"}}},
		{name: "WorkerSuccessAdvancesWatermark", msgData: `{"thread_id":"th-ext-1"}`, delivered: 1, wantWatermarks: map[string]int64{"aerial": 35}, wantFacts: aerialFact},
		{name: "WorkerErrorAttempt1NaksWithBackoff", msgData: `{"thread_id":"th-ext-1"}`, delivered: 1, sabotage: sabotageTurnReads, wantErr: true, wantMinNak: 1 * time.Second, wantWatermarks: none},
		{name: "WorkerErrorAttempt4NaksWithBackoff", msgData: `{"thread_id":"th-ext-1"}`, delivered: 4, sabotage: sabotageTurnReads, wantErr: true, wantMinNak: 8 * time.Second, wantWatermarks: none},
		{name: "WorkerErrorAttempt5NaksWithBackoff", msgData: `{"thread_id":"th-ext-1"}`, delivered: 5, sabotage: sabotageTurnReads, wantErr: true, wantMinNak: 16 * time.Second, wantWatermarks: none},
		{name: "WorkerLockBusyNaksWithFixed5Seconds", msgData: `{"thread_id":"th-ext-1"}`, delivered: 5, heldXactLock: "coven:facts", wantErr: true, wantErrIs: store.ErrAdvisoryLockBusy, wantExactNak: 5 * time.Second, wantWatermarks: none},
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
			cfg:     &config.Config{Port: "0", DatabaseURL: validDSN},
			wantErr: false,
		},
		{
			name:    "CleanLifecycleExtractorWithTranscriptsURL",
			cfg:     &config.Config{Port: "0", DatabaseURL: validDSN, TranscriptsURL: "http://mock-transcripts", Role: "extractor"},
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

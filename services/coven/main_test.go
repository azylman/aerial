package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
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
	name         string
	msgData      string
	delivered    uint64
	role         string
	failErr      error
	wantErr      bool
	wantTerm     bool
	wantMinNak   time.Duration
	wantExactNak time.Duration
	wantWater    int64
}

func runHandleCovenExtractJobCase(t *testing.T, tc handleCovenExtractJobTestCase) {
	t.Helper()
	ctx := context.Background()
	memStore := newTestMemStore()
	memStore.activeThreads = []string{"th-ext-1"}
	memStore.unextractedTurns["th-ext-1"] = []Message{{RowID: 35, Content: "test message", ResponseText: "resp"}}
	if tc.failErr != nil {
		memStore.SetFail("GetUnextractedAdminTurns", tc.failErr)
	}
	worker := NewWorker(WorkerConfig{
		Role:      tc.role,
		Store:     memStore,
		Ollama:    &MockOllamaClient{embedding: make([]float32, ExpectedEmbeddingDim)},
		LLMClient: func(context.Context, string) (string, error) { return `{"facts":[]}`, nil },
	})
	msg := &pubsubtest.Message{Payload: []byte(tc.msgData), Delivered: tc.delivered}

	err := handleCovenExtractJob(ctx, msg, worker)
	if (err != nil) != tc.wantErr {
		t.Fatalf("handleCovenExtractJob() err = %v, wantErr = %v", err, tc.wantErr)
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
	if wm, _, _ := memStore.GetConversationFactWatermark(ctx, "th-ext-1"); wm != tc.wantWater {
		t.Errorf("watermark = %d, want %d", wm, tc.wantWater)
	}
}

func TestHandleCovenExtractJob_Scenarios(t *testing.T) {
	queryFailed := errors.New("query failed")
	tests := []handleCovenExtractJobTestCase{
		{name: "UnmarshalErrorTermsMessageAndReturnsNil", msgData: "bad-json", delivered: 1, wantTerm: true},
		{name: "EmptyThreadIDTermsMessageAndReturnsNil", msgData: `{"thread_id":"   "}`, delivered: 1, wantTerm: true},
		{name: "MismatchedAgentSkipsExtraction", msgData: `{"thread_id":"th-ext-1","agent":"agent-alpha"}`, delivered: 1},
		{name: "RoleAllAcceptsAnyValidAgent", msgData: `{"thread_id":"th-ext-1","agent":"agent-alpha"}`, delivered: 1, role: "all", wantWater: 35},
		{name: "WorkerSuccessAdvancesWatermark", msgData: `{"thread_id":"th-ext-1"}`, delivered: 1, wantWater: 35},
		{name: "WorkerErrorAttempt1NaksWithBackoff", msgData: `{"thread_id":"th-ext-1"}`, delivered: 1, failErr: queryFailed, wantErr: true, wantMinNak: 1 * time.Second},
		{name: "WorkerErrorAttempt4NaksWithBackoff", msgData: `{"thread_id":"th-ext-1"}`, delivered: 4, failErr: queryFailed, wantErr: true, wantMinNak: 8 * time.Second},
		{name: "WorkerErrorAttempt5NaksWithBackoff", msgData: `{"thread_id":"th-ext-1"}`, delivered: 5, failErr: queryFailed, wantErr: true, wantMinNak: 16 * time.Second},
		{name: "WorkerLockBusyNaksWithFixed5Seconds", msgData: `{"thread_id":"th-ext-1"}`, delivered: 5, failErr: ErrAdvisoryLockBusy, wantErr: true, wantExactNak: 5 * time.Second},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runHandleCovenExtractJobCase(t, tc)
		})
	}
}

func TestCoven_TranscriptsIndexedToCoalescedExtractIntegration(t *testing.T) {
	ctx := context.Background()

	memStore := newTestMemStore()
	memStore.activeThreads = []string{"th-e2e-1"}
	memStore.unextractedTurns["th-e2e-1"] = []Message{
		{RowID: 88, Content: "I prefer matcha", ResponseText: "Noted"},
	}

	validEmb := make([]float32, ExpectedEmbeddingDim)
	client := &MockOllamaClient{embedding: validEmb}
	llm := func(ctx context.Context, prompt string) (string, error) {
		return `{"facts":[]}`, nil
	}

	worker := NewWorker(WorkerConfig{
		Store:     memStore,
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

	wm, _, err := memStore.GetConversationFactWatermark(ctx, "th-e2e-1")
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

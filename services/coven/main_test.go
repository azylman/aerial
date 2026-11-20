package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestLoadConfigFile_EriKeysAndEnv(t *testing.T) {
	tmpDir := t.TempDir()

	eriYAML := `port: '4008'
eri_url: 'http://eri.aerial'
eri_llm_url: 'http://eri.aerial/internal/llm'
pr_register_url: 'http://synapse.aerial/internal/pr/register'
custom_skills_dir: '/custom/skills'
rules_dir: '/custom/rules'
config_repo_owner: 'azylman'
config_repo_name: 'aerial-config'
ollama:
  base_url: 'http://ollama:11434'
  model: 'all-minilm'
  num_ctx: 512
`
	eriFile := filepath.Join(tmpDir, "eri.yaml")
	if err := os.WriteFile(eriFile, []byte(eriYAML), 0o644); err != nil {
		t.Fatalf("failed to write eri config: %v", err)
	}

	tests := []struct {
		name          string
		path          string
		envDB         string
		envPassword   string
		envEriURL     string
		envEriLLMURL  string
		envPRRegURL   string
		envSynapseURL string
		wantEriURL    string
		wantEriLLMURL string
		wantPRRegURL  string
		wantDB        string
	}{
		{
			name:          "eri yaml loaded with eri and pr register fields",
			path:          eriFile,
			envDB:         "postgres://localhost/testdb",
			wantEriURL:    "http://eri.aerial",
			wantEriLLMURL: "http://eri.aerial/internal/llm",
			wantPRRegURL:  "http://synapse.aerial/internal/pr/register",
		},
		{
			name:          "env overrides take precedence over yaml",
			path:          eriFile,
			envDB:         "postgres://localhost/testdb",
			envEriURL:     "http://env.eri:8088",
			envEriLLMURL:  "http://env.eri:8088/internal/llm",
			envPRRegURL:   "http://env.synapse:4020/internal/pr/register",
			wantEriURL:    "http://env.eri:8088",
			wantEriLLMURL: "http://env.eri:8088/internal/llm",
			wantPRRegURL:  "http://env.synapse:4020/internal/pr/register",
		},
		{
			name:          "synapse pr register url env fallback",
			path:          eriFile,
			envDB:         "postgres://localhost/testdb",
			envSynapseURL: "http://fallback.synapse:4020/internal/pr/register",
			wantEriURL:    "http://eri.aerial",
			wantEriLLMURL: "http://eri.aerial/internal/llm",
			wantPRRegURL:  "http://fallback.synapse:4020/internal/pr/register",
		},
		{
			name:          "decomposed POSTGRES_PASSWORD resolution",
			path:          eriFile,
			envPassword:   "secretpass",
			wantEriURL:    "http://eri.aerial",
			wantEriLLMURL: "http://eri.aerial/internal/llm",
			wantPRRegURL:  "http://synapse.aerial/internal/pr/register",
			wantDB:        "postgres://aerial:secretpass@postgres:5432/aerial?sslmode=disable",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATABASE_URL", tc.envDB)
			t.Setenv("POSTGRES_URL", "")
			t.Setenv("POSTGRES_HOST", "")
			t.Setenv("POSTGRES_PORT", "")
			t.Setenv("POSTGRES_USER", "")
			t.Setenv("POSTGRES_PASSWORD", tc.envPassword)
			t.Setenv("POSTGRES_DB", "")
			t.Setenv("ERI_URL", tc.envEriURL)
			t.Setenv("ERI_LLM_URL", tc.envEriLLMURL)
			t.Setenv("PR_REGISTER_URL", tc.envPRRegURL)
			t.Setenv("SYNAPSE_PR_REGISTER_URL", tc.envSynapseURL)

			cfg, err := LoadConfig(tc.path)
			if err != nil {
				t.Fatalf("LoadConfig() error = %v", err)
			}
			if cfg.EriURL != tc.wantEriURL {
				t.Errorf("EriURL = %q, want %q", cfg.EriURL, tc.wantEriURL)
			}
			if cfg.EriLLMURL != tc.wantEriLLMURL {
				t.Errorf("EriLLMURL = %q, want %q", cfg.EriLLMURL, tc.wantEriLLMURL)
			}
			if cfg.PRRegisterURL != tc.wantPRRegURL {
				t.Errorf("PRRegisterURL = %q, want %q", cfg.PRRegisterURL, tc.wantPRRegURL)
			}
			if tc.wantDB != "" && cfg.DatabaseURL != tc.wantDB {
				t.Errorf("DatabaseURL = %q, want %q", cfg.DatabaseURL, tc.wantDB)
			}
		})
	}
}

type ackSubscribeBus struct {
	pubsub.Bus

	lastErr   error
	onHandled func()
}

func (a *ackSubscribeBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	return a.Bus.Subscribe(ctx, subject, durable, func(mCtx context.Context, msg pubsub.Message) error {
		defer a.onHandled()
		err := handler(mCtx, msg)
		a.lastErr = err
		return err
	})
}

type brokenBus struct {
	pubsub.Bus

	errOnEnsure    bool
	errOnSubscribe bool
	failDurable    string
}

func (b *brokenBus) EnsureCoreStreams(ctx context.Context) error {
	if b.errOnEnsure {
		return errors.New("ensure core streams failed")
	}
	return b.Bus.EnsureCoreStreams(ctx)
}

func (b *brokenBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	if b.errOnSubscribe || (b.failDurable != "" && durable == b.failDurable) {
		return nil, errors.New("subscribe failed")
	}
	return b.Bus.Subscribe(ctx, subject, durable, handler)
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
	brokenEnsure := &brokenBus{Bus: memBus, errOnEnsure: true}
	if bus := initCovenJetStreamWithFactory(ctx, nil, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return brokenEnsure, nil
	}); bus == nil {
		t.Error("expected non-nil bus even if ensure stream fails")
	}

	// Subscribe failure logs warning but returns bus
	worker := NewWorker(WorkerConfig{})
	brokenSub := &brokenBus{Bus: memBus, errOnSubscribe: true}
	subCtx, subCancel := context.WithCancel(ctx)
	if bus := initCovenJetStreamWithFactory(subCtx, worker, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return brokenSub, nil
	}); bus == nil {
		t.Error("expected non-nil bus even if subscribe fails")
	}
	subCancel()

	// Unmarshal error on bad JSON payload
	unmarshalDone := make(chan struct{})
	wrapped := &ackSubscribeBus{Bus: memBus, onHandled: func() { close(unmarshalDone) }}
	bus := initCovenJetStreamWithFactory(ctx, worker, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return wrapped, nil
	})
	if bus == nil {
		t.Fatal("expected non-nil bus")
	}

	if err := memBus.Publish(ctx, pubsub.SubjectAerialTurnPrefix+"transcripts_indexed", "bad-sync-1", "not-a-valid-json"); err != nil {
		t.Fatalf("Publish bad json: %v", err)
	}

	select {
	case <-unmarshalDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for bad json message to be handled")
	}
	if wrapped.lastErr != nil {
		t.Errorf("expected nil error on bad json unmarshal to avoid poison redelivery loop, got %v", wrapped.lastErr)
	}
}

type retrySubscribeBus struct {
	pubsub.Bus

	attempts  atomic.Int32
	onSuccess chan struct{}
}

func (r *retrySubscribeBus) EnsureCoreStreams(ctx context.Context) error {
	return r.Bus.EnsureCoreStreams(ctx)
}

func (r *retrySubscribeBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	if r.attempts.Add(1) == 1 {
		return nil, errors.New("initial subscribe failure")
	}
	sub, err := r.Bus.Subscribe(ctx, subject, durable, handler)
	if err == nil && r.onSuccess != nil {
		select {
		case r.onSuccess <- struct{}{}:
		default:
		}
	}
	return sub, err
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

	ctx := t.Context()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	retryBus := &retrySubscribeBus{
		Bus:       memBus,
		onSuccess: make(chan struct{}, 1),
	}

	worker := NewWorker(WorkerConfig{})
	bus := initCovenJetStreamWithFactory(ctx, worker, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return retryBus, nil
	})
	if bus == nil {
		t.Fatal("expected non-nil bus")
	}

	select {
	case <-retryBus.onSuccess:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for background retry to subscribe coven-turns")
	}
}

type mockPubsubMsg struct {
	data      []byte
	delivered uint64
	termed    bool
	naked     bool
	nakDelay  time.Duration
	termErr   error
	nakErr    error
}

func (m *mockPubsubMsg) Data() []byte         { return m.data }
func (*mockPubsubMsg) Subject() string        { return "aerial.turn.completed" }
func (*mockPubsubMsg) Header() nats.Header    { return nil }
func (m *mockPubsubMsg) NumDelivered() uint64 { return m.delivered }
func (*mockPubsubMsg) Ack() error             { return nil }
func (m *mockPubsubMsg) NakWithDelay(d time.Duration) error {
	m.naked = true
	m.nakDelay = d
	return m.nakErr
}

func (m *mockPubsubMsg) Term() error {
	m.termed = true
	return m.termErr
}
func (*mockPubsubMsg) Snooze(_ time.Duration) error { return nil }
func (*mockPubsubMsg) InProgress() error            { return nil }

type handleCovenExtractJobTestCase struct {
	name         string
	msgData      string
	delivered    uint64
	worker       *Worker
	wantErr      bool
	wantTerm     bool
	wantNak      bool
	wantMinNak   time.Duration
	wantExactNak time.Duration
	checkWater   bool
	wantWater    int64
}

func runHandleCovenExtractJobCase(ctx context.Context, t *testing.T, tc handleCovenExtractJobTestCase, memStore *TestMemStore) {
	t.Helper()
	msg := &mockPubsubMsg{
		data:      []byte(tc.msgData),
		delivered: tc.delivered,
	}

	err := handleCovenExtractJob(ctx, msg, tc.worker)
	if (err != nil) != tc.wantErr {
		t.Fatalf("handleCovenExtractJob() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantTerm && !msg.termed {
		t.Errorf("expected message to be termed")
	}
	if tc.wantNak && !msg.naked {
		t.Errorf("expected message to be naked")
	}
	if tc.wantMinNak > 0 && msg.nakDelay < tc.wantMinNak {
		t.Errorf("nakDelay = %v, want >= %v", msg.nakDelay, tc.wantMinNak)
	}
	if tc.wantExactNak > 0 && msg.nakDelay != tc.wantExactNak {
		t.Errorf("nakDelay = %v, want %v", msg.nakDelay, tc.wantExactNak)
	}
	if tc.checkWater && tc.worker != nil && memStore != nil {
		wm, _, _ := memStore.GetConversationFactWatermark(ctx, "th-ext-1")
		if wm != tc.wantWater {
			t.Errorf("watermark = %d, want %d", wm, tc.wantWater)
		}
	}
}

func TestHandleCovenExtractJob_Scenarios(t *testing.T) {
	ctx := context.Background()

	validEmb := make([]float32, ExpectedEmbeddingDim)
	client := &MockOllamaClient{embedding: validEmb}
	llm := func(ctx context.Context, prompt string) (string, error) {
		return `{"facts":[]}`, nil
	}

	memStore := newTestMemStore()
	memStore.activeThreads = []string{"th-ext-1"}
	memStore.unextractedTurns["th-ext-1"] = []Message{
		{RowID: 35, Content: "test message", ResponseText: "resp"},
	}
	worker := NewWorker(WorkerConfig{
		Store:     memStore,
		Ollama:    client,
		LLMClient: llm,
	})

	brokenStore := newTestMemStore()
	brokenStore.SetFail("GetUnextractedAdminTurns", errors.New("query failed"))
	workerErr := NewWorker(WorkerConfig{
		Store:     brokenStore,
		Ollama:    client,
		LLMClient: llm,
	})

	lockBusyStore := newTestMemStore()
	lockBusyStore.SetFail("GetUnextractedAdminTurns", ErrAdvisoryLockBusy)
	workerLockBusy := NewWorker(WorkerConfig{
		Store:     lockBusyStore,
		Ollama:    client,
		LLMClient: llm,
	})

	tests := []handleCovenExtractJobTestCase{
		{
			name:      "UnmarshalErrorTermsMessageAndReturnsNil",
			msgData:   "bad-json",
			delivered: 1,
			worker:    worker,
			wantTerm:  true,
		},
		{
			name:      "EmptyThreadIDTermsMessageAndReturnsNil",
			msgData:   `{"thread_id":"   "}`,
			delivered: 1,
			worker:    worker,
			wantTerm:  true,
		},
		{
			name:      "MismatchedAgentSkipsExtraction",
			msgData:   `{"thread_id":"th-ext-1","agent":"agent-alpha"}`,
			delivered: 1,
			worker:    worker,
		},
		{
			name:       "WorkerSuccessAdvancesWatermark",
			msgData:    `{"thread_id":"th-ext-1"}`,
			delivered:  1,
			worker:     worker,
			checkWater: true,
			wantWater:  35,
		},
		{
			name:       "WorkerErrorAttempt1NaksWithBackoff",
			msgData:    `{"thread_id":"th-ext-1"}`,
			delivered:  1,
			worker:     workerErr,
			wantErr:    true,
			wantNak:    true,
			wantMinNak: 1 * time.Second,
		},
		{
			name:       "WorkerErrorAttempt4NaksWithBackoff",
			msgData:    `{"thread_id":"th-ext-1"}`,
			delivered:  4,
			worker:     workerErr,
			wantErr:    true,
			wantNak:    true,
			wantMinNak: 8 * time.Second,
		},
		{
			name:       "WorkerErrorAttempt5NaksWithBackoff",
			msgData:    `{"thread_id":"th-ext-1"}`,
			delivered:  5,
			worker:     workerErr,
			wantErr:    true,
			wantNak:    true,
			wantMinNak: 16 * time.Second,
		},
		{
			name:         "WorkerLockBusyNaksWithFixed5Seconds",
			msgData:      `{"thread_id":"th-ext-1"}`,
			delivered:    5,
			worker:       workerLockBusy,
			wantErr:      true,
			wantNak:      true,
			wantExactNak: 5 * time.Second,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runHandleCovenExtractJobCase(ctx, t, tc, memStore)
		})
	}
}

type ackExtractBus struct {
	pubsub.Bus

	onExtractHandled func()
}

func (a *ackExtractBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	return a.Bus.Subscribe(ctx, subject, durable, func(msgCtx context.Context, msg pubsub.Message) error {
		err := handler(msgCtx, msg)
		if strings.HasPrefix(msg.Subject(), pubsub.SubjectAerialCoalescedPrefix) && a.onExtractHandled != nil {
			a.onExtractHandled()
		}
		return err
	})
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

	extractDone := make(chan struct{})
	wrapped := &ackExtractBus{
		Bus: memBus,
		onExtractHandled: func() {
			close(extractDone)
		},
	}

	bus := initCovenJetStreamWithFactory(ctx, worker, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return wrapped, nil
	})
	if bus == nil {
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

	select {
	case <-extractDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for coalesced extract job completion")
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
	brokenSub := &brokenBus{Bus: memBus, errOnSubscribe: true}
	if err := subscribeCovenTranscriptsIndexed(ctx, brokenSub, "aerial"); err == nil {
		t.Error("expected error when subscribe fails")
	}
	if err := subscribeCovenExtract(ctx, brokenSub, worker); err == nil {
		t.Error("expected error when subscribe fails")
	}
	if err := (&covenConsumers{}).subscribeAll(ctx, brokenSub, worker); err == nil {
		t.Error("expected error when subscribe fails")
	}

	brokenIndexed := &brokenBus{Bus: memBus, failDurable: "coven-transcripts-indexed"}
	if err := (&covenConsumers{}).subscribeAll(ctx, brokenIndexed, worker); err == nil {
		t.Error("expected error when subscribe coven-transcripts-indexed fails")
	}
	brokenExtract := &brokenBus{Bus: memBus, failDurable: "coven-extract"}
	if err := (&covenConsumers{}).subscribeAll(ctx, brokenExtract, worker); err == nil {
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

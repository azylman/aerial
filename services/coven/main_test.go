package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/nats-io/nats.go"
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
		wantEriURL    string
		wantEriLLMURL string
		wantPRRegURL  string
		wantDB        string
	}{
		{
			name:          "eri yaml loaded and copied to both eri and brain fields",
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

			cfg, err := LoadConfig(tc.path)
			if err != nil {
				t.Fatalf("LoadConfig() error = %v", err)
			}
			if cfg.EriURL != tc.wantEriURL || cfg.BrainURL != tc.wantEriURL {
				t.Errorf("EriURL = %q, BrainURL = %q, want %q", cfg.EriURL, cfg.BrainURL, tc.wantEriURL)
			}
			if cfg.EriLLMURL != tc.wantEriLLMURL || cfg.BrainLLMURL != tc.wantEriLLMURL {
				t.Errorf("EriLLMURL = %q, BrainLLMURL = %q, want %q", cfg.EriLLMURL, cfg.BrainLLMURL, tc.wantEriLLMURL)
			}
			if cfg.PRRegisterURL != tc.wantPRRegURL || cfg.BrainPRRegisterURL != tc.wantPRRegURL {
				t.Errorf("PRRegisterURL = %q, BrainPRRegisterURL = %q, want %q", cfg.PRRegisterURL, cfg.BrainPRRegisterURL, tc.wantPRRegURL)
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

func TestHandleTurnCompletedEvent_Scenarios(t *testing.T) {
	ctx := context.Background()

	validEmb := make([]float32, ExpectedEmbeddingDim)
	client := &MockOllamaClient{embedding: validEmb}
	llm := func(ctx context.Context, prompt string) (string, error) {
		return `{"facts":[]}`, nil
	}

	memStorePing := newTestMemStore()
	memStorePing.activeThreads = []string{"th-ping-1"}
	memStorePing.unextractedTurns["th-ping-1"] = []Message{
		{RowID: 15, Content: "ping message", ResponseText: "pong response"},
	}
	workerPing := NewWorker(WorkerConfig{
		Store:     memStorePing,
		Ollama:    client,
		LLMClient: llm,
	})

	memStoreErr := newTestMemStore()
	memStoreErr.SetFail("GetActiveConversationsForExtraction", errors.New("query failed"))
	workerErr := NewWorker(WorkerConfig{
		Store:     memStoreErr,
		Ollama:    client,
		LLMClient: llm,
	})

	tests := []struct {
		name        string
		worker      *Worker
		ev          pubsub.TurnCompletedEvent
		wantErr     bool
		checkStore  *TestMemStore
		checkThread string
		wantWater   int64
	}{
		{
			name:        "healthcheck ping returns nil without extraction",
			worker:      workerPing,
			ev:          pubsub.TurnCompletedEvent{SessionID: "healthcheck-ping", Source: "test"},
			wantErr:     false,
			checkStore:  memStorePing,
			checkThread: "th-ping-1",
			wantWater:   0,
		},
		{
			name:    "extraction failure is logged and returns error for redelivery",
			worker:  workerErr,
			ev:      pubsub.TurnCompletedEvent{SessionID: "sess-err", Source: "discord"},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.worker.HandleTurnCompletedEvent(ctx, tc.ev)
			if (err != nil) != tc.wantErr {
				t.Fatalf("HandleTurnCompletedEvent() err = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.checkStore != nil {
				if w := tc.checkStore.watermarks[tc.checkThread]; w != tc.wantWater {
					t.Errorf("watermark for %s = %d, want %d", tc.checkThread, w, tc.wantWater)
				}
			}
		})
	}
}

func TestCoven_TurnsConsumerIntegration(t *testing.T) {
	ctx := context.Background()

	memStore := newTestMemStore()
	memStore.activeThreads = []string{"th-int-1"}
	memStore.unextractedTurns["th-int-1"] = []Message{
		{RowID: 25, Content: "I prefer dark theme", ResponseText: "Understood"},
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

	done := make(chan struct{})
	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	wrapped := &ackSubscribeBus{Bus: memBus, onHandled: func() { close(done) }}
	bus := initCovenJetStreamWithFactory(ctx, worker, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return wrapped, nil
	})
	if bus == nil {
		t.Fatal("expected non-nil bus")
	}

	ev := pubsub.TurnCompletedEvent{
		SessionID:      "sess-int-1",
		ConversationID: "th-int-1",
		Source:         "discord",
		CompletedAt:    time.Now().UTC(),
	}
	if err := memBus.Publish(ctx, "aerial.turn.completed", "turn-1", ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for turn completed event delivery")
	}

	if memStore.watermarks["th-int-1"] != 25 {
		t.Errorf("watermark = %d, want 25", memStore.watermarks["th-int-1"])
	}
}

type brokenBus struct {
	pubsub.Bus

	errOnEnsure    bool
	errOnSubscribe bool
}

func (b *brokenBus) EnsureCoreStreams(ctx context.Context) error {
	if b.errOnEnsure {
		return errors.New("ensure core streams failed")
	}
	return b.Bus.EnsureCoreStreams(ctx)
}

func (b *brokenBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	if b.errOnSubscribe {
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
	if bus := initCovenJetStreamWithFactory(ctx, worker, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return brokenSub, nil
	}); bus == nil {
		t.Error("expected non-nil bus even if subscribe fails")
	}

	// Unmarshal error on bad JSON payload
	unmarshalDone := make(chan struct{})
	wrapped := &ackSubscribeBus{Bus: memBus, onHandled: func() { close(unmarshalDone) }}
	bus := initCovenJetStreamWithFactory(ctx, worker, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return wrapped, nil
	})
	if bus == nil {
		t.Fatal("expected non-nil bus")
	}

	if err := memBus.Publish(ctx, "aerial.turn.completed", "bad-turn-1", "not-a-valid-json"); err != nil {
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

func TestCoven_RetrySubscribeCovenTurns(t *testing.T) {
	origInterval := subscribeRetryInterval
	defer func() { subscribeRetryInterval = origInterval }()
	subscribeRetryInterval = 5 * time.Millisecond

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
}

func (m *mockPubsubMsg) Data() []byte                       { return m.data }
func (m *mockPubsubMsg) Subject() string                    { return "aerial.turn.completed" }
func (m *mockPubsubMsg) Header() nats.Header                { return nil }
func (m *mockPubsubMsg) NumDelivered() uint64               { return m.delivered }
func (m *mockPubsubMsg) Ack() error                         { return nil }
func (m *mockPubsubMsg) NakWithDelay(_ time.Duration) error { return nil }
func (m *mockPubsubMsg) Term() error                        { return nil }
func (m *mockPubsubMsg) Snooze(_ time.Duration) error       { return nil }
func (m *mockPubsubMsg) InProgress() error                  { return nil }

type captureSubscribeBus struct {
	pubsub.Bus

	handler pubsub.Handler
}

func (c *captureSubscribeBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	c.handler = handler
	return c.Bus.Subscribe(ctx, subject, durable, handler)
}

func TestCoven_TurnCompletedConsumerRetriesAndDrop(t *testing.T) {
	ctx := context.Background()
	memStore := newTestMemStore()
	memStore.SetFail("GetActiveConversationsForExtraction", errors.New("query failed"))
	worker := NewWorker(WorkerConfig{
		Store:     memStore,
		Ollama:    &MockOllamaClient{embedding: make([]float32, ExpectedEmbeddingDim)},
		LLMClient: func(context.Context, string) (string, error) { return `{"facts":[]}`, nil },
	})

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	captureBus := &captureSubscribeBus{Bus: memBus}

	bus := initCovenJetStreamWithFactory(ctx, worker, "nats://mock", func(_ context.Context, _ string) (pubsub.Bus, error) {
		return captureBus, nil
	})
	if bus == nil || captureBus.handler == nil {
		t.Fatal("expected non-nil bus and captured handler")
	}

	ev := pubsub.TurnCompletedEvent{SessionID: "sess-retry", Source: "discord"}
	data, _ := json.Marshal(ev)

	// Delivery attempt < 5 returns error to allow JetStream redelivery
	if err := captureBus.handler(ctx, &mockPubsubMsg{data: data, delivered: 1}); err == nil {
		t.Error("expected error on attempt 1")
	}

	// Delivery attempt >= 5 drops message and returns nil
	if err := captureBus.handler(ctx, &mockPubsubMsg{data: data, delivered: 5}); err != nil {
		t.Errorf("expected nil error on attempt 5, got %v", err)
	}
}

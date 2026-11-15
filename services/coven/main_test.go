package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
}

func (m *mockPubsubMsg) Data() []byte         { return m.data }
func (m *mockPubsubMsg) Subject() string      { return "aerial.turn.completed" }
func (m *mockPubsubMsg) Header() nats.Header  { return nil }
func (m *mockPubsubMsg) NumDelivered() uint64 { return m.delivered }
func (m *mockPubsubMsg) Ack() error           { return nil }
func (m *mockPubsubMsg) NakWithDelay(d time.Duration) error {
	m.naked = true
	m.nakDelay = d
	return nil
}
func (m *mockPubsubMsg) Term() error                  { m.termed = true; return nil }
func (m *mockPubsubMsg) Snooze(_ time.Duration) error { return nil }
func (m *mockPubsubMsg) InProgress() error            { return nil }

type captureSubscribeBus struct {
	pubsub.Bus

	handler  pubsub.Handler
	handlers map[string]pubsub.Handler
}

func (c *captureSubscribeBus) Subscribe(ctx context.Context, subject, durable string, handler pubsub.Handler) (pubsub.Subscription, error) {
	if c.handlers == nil {
		c.handlers = make(map[string]pubsub.Handler)
	}
	c.handlers[durable] = handler
	if durable == "coven-turns" {
		c.handler = handler
	}
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

func TestNakBackoffDelay(t *testing.T) {
	tests := []struct {
		delivered uint64
		want      time.Duration
	}{
		{delivered: 0, want: 1 * time.Second},
		{delivered: 1, want: 1 * time.Second},
		{delivered: 2, want: 2 * time.Second},
		{delivered: 3, want: 4 * time.Second},
		{delivered: 4, want: 8 * time.Second},
		{delivered: 5, want: 16 * time.Second},
		{delivered: 10, want: 16 * time.Second},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("delivered_%d", tc.delivered), func(t *testing.T) {
			got := nakBackoffDelay(tc.delivered)
			if got != tc.want {
				t.Errorf("nakBackoffDelay(%d) = %v, want %v", tc.delivered, got, tc.want)
			}
		})
	}
}

type handleTranscriptSyncedEventTestCase struct {
	name             string
	msgData          string
	busFail          bool
	wantErr          bool
	wantCoalescedJob bool
}

func runHandleTranscriptSyncedEventCase(ctx context.Context, t *testing.T, tc handleTranscriptSyncedEventTestCase) {
	t.Helper()
	msg := &mockPubsubMsg{data: []byte(tc.msgData), delivered: 1}

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	receivedChan := make(chan pubsub.CovenExtractJobArgs, 1)
	if tc.wantCoalescedJob {
		_, _ = memBus.Subscribe(ctx, pubsub.SubjectCovenExtractCoalesced("th-sync-1"), "test-sub", func(_ context.Context, m pubsub.Message) error {
			var args pubsub.CovenExtractJobArgs
			_ = json.Unmarshal(m.Data(), &args)
			receivedChan <- args
			return nil
		})
	}
	if tc.busFail {
		_ = memBus.Close()
	}

	err := handleTranscriptSyncedEvent(ctx, msg, memBus)
	if (err != nil) != tc.wantErr {
		t.Fatalf("handleTranscriptSyncedEvent() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantCoalescedJob {
		var receivedArgs pubsub.CovenExtractJobArgs
		select {
		case receivedArgs = <-receivedChan:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for coalesced extract job on bus")
		}
		expected := pubsub.CovenExtractJobArgs{
			ThreadID:  "th-sync-1",
			SessionID: "sess-1",
		}
		if receivedArgs != expected {
			t.Errorf("received coalesced extract args = %+v, want %+v", receivedArgs, expected)
		}
	}
}

func TestHandleTranscriptSyncedEvent_Scenarios(t *testing.T) {
	ctx := context.Background()

	tests := []handleTranscriptSyncedEventTestCase{
		{
			name:    "InvalidJSONLoggedAndReturnsNil",
			msgData: "not-json",
		},
		{
			name:    "EmptyConversationIDIgnored",
			msgData: `{"session_id":"sess-1","conversation_id":""}`,
		},
		{
			name:    "HealthcheckPingSessionIgnored",
			msgData: `{"session_id":"healthcheck-ping","conversation_id":"th-sync-1"}`,
		},
		{
			name:             "BusPublishesCoalescedExtractJob",
			msgData:          `{"session_id":"sess-1","conversation_id":"th-sync-1"}`,
			wantCoalescedJob: true,
		},
		{
			name:    "BusPublishErrorReturnsError",
			msgData: `{"session_id":"sess-1","conversation_id":"th-sync-1"}`,
			busFail: true,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			runHandleTranscriptSyncedEventCase(ctx, t, tc)
		})
	}
}

type handleCovenExtractJobTestCase struct {
	name       string
	msgData    string
	delivered  uint64
	worker     *Worker
	wantErr    bool
	wantTerm   bool
	wantNak    bool
	wantMinNak time.Duration
	checkWater bool
	wantWater  int64
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

	tests := []handleCovenExtractJobTestCase{
		{
			name:      "UnmarshalErrorTermsMessageAndReturnsNil",
			msgData:   "bad-json",
			delivered: 1,
			worker:    worker,
			wantTerm:  true,
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
			name:      "WorkerErrorAttempt5TermsAndReturnsNil",
			msgData:   `{"thread_id":"th-ext-1"}`,
			delivered: 5,
			worker:    workerErr,
			wantTerm:  true,
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

func TestCoven_TranscriptSyncedToCoalescedExtractIntegration(t *testing.T) {
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

	ev := pubsub.TranscriptSyncedEvent{
		SessionID:      "sess-e2e-1",
		ConversationID: "th-e2e-1",
		StepsInserted:  2,
		SyncedAt:       time.Now().UTC(),
	}
	if err := memBus.Publish(ctx, pubsub.SubjectAerialTurnTranscriptSynced, "sync-1", ev); err != nil {
		t.Fatalf("Publish TranscriptSynced: %v", err)
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
	if err := subscribeCovenTranscriptSynced(ctx, nil); err != nil {
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
	if err := subscribeCovenTranscriptSynced(ctx, brokenSub); err == nil {
		t.Error("expected error when subscribe fails")
	}
	if err := subscribeCovenExtract(ctx, brokenSub, worker); err == nil {
		t.Error("expected error when subscribe fails")
	}
	if err := (&covenConsumers{}).subscribeAll(ctx, brokenSub, worker); err == nil {
		t.Error("expected error when subscribe fails")
	}

	brokenTranscript := &brokenBus{Bus: memBus, failDurable: "coven-transcript-synced"}
	if err := (&covenConsumers{}).subscribeAll(ctx, brokenTranscript, worker); err == nil {
		t.Error("expected error when subscribe coven-transcript-synced fails")
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

	// Delivery attempt < 5 returns error on closed bus (publish fails)
	openBus := pubsub.NewMemoryBus()
	captureClosed := &captureSubscribeBus{Bus: openBus}
	if err := subscribeCovenTranscriptSynced(ctx, captureClosed); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	_ = openBus.Close()
	tsHandlerClosed := captureClosed.handlers["coven-transcript-synced"]
	validData, _ := json.Marshal(pubsub.TranscriptSyncedEvent{ConversationID: "th-retry", SessionID: "sess-1"})

	if err := tsHandlerClosed(ctx, &mockPubsubMsg{data: validData, delivered: 1}); err == nil {
		t.Error("expected error on attempt 1 with closed bus")
	}
	if err := tsHandlerClosed(ctx, &mockPubsubMsg{data: validData, delivered: 5}); err != nil {
		t.Errorf("expected nil error on attempt 5 drop, got %v", err)
	}
}

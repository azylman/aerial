package transcript

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/nats-io/nats.go"
)

func TestShouldRunTranscriptSync_TableDriven(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.October, 22, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		now      time.Time
		lastSync time.Time
		interval time.Duration
		expected bool
	}{
		{"interval zero", now, time.Time{}, 0, false},
		{"interval negative", now, time.Time{}, -time.Minute, false},
		{"lastSync zero runs immediately", now, time.Time{}, 5 * time.Minute, true},
		{"lastSync exactly interval elapsed", now, now.Add(-5 * time.Minute), 5 * time.Minute, true},
		{"lastSync more than interval elapsed", now, now.Add(-10 * time.Minute), 5 * time.Minute, true},
		{"lastSync less than interval elapsed", now, now.Add(-4 * time.Minute), 5 * time.Minute, false},
		{"lastSync in future", now, now.Add(1 * time.Minute), 5 * time.Minute, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ShouldRunTranscriptSync(tc.now, tc.lastSync, tc.interval)
			if got != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestNewOllamaEmbedder_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		cfg       *config.Config
		roots     []string
		wantNil   bool
		testInput string
	}{
		{"nil config returns nil", nil, nil, true, "hello"},
		{"valid config returns embedder func", config.NewFromData(&config.ConfigData{
			Ollama: config.OllamaConfig{BaseURL: "http://127.0.0.1:9", Model: "all-minilm"},
		}), []string{"/tmp/brain"}, false, "sample text"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fn := NewOllamaEmbedder(tc.cfg, tc.roots...)
			if (fn == nil) != tc.wantNil {
				t.Fatalf("expected fn nil=%v, got nil=%v", tc.wantNil, fn == nil)
			}
			if fn != nil {
				// Calling with mock unreachable server returns error without panic
				ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()
				_, _ = fn(ctx, tc.testInput)
			}
		})
	}
}

type channelTranscriptStore struct {
	db.TranscriptStore

	notify chan struct{}
	err    error
}

func (s *channelTranscriptStore) GetSessionSyncStates(ctx context.Context) (map[string]db.SessionSyncState, error) {
	if s.notify != nil {
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.TranscriptStore.GetSessionSyncStates(ctx)
}

func TestRunTranscriptSync_TableDriven(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	brainDir := filepath.Join(tempDir, "brain")
	_ = os.MkdirAll(brainDir, 0o755)

	homeDir := filepath.Join(tempDir, "home")
	geminiBrainDir := filepath.Join(homeDir, ".gemini", "antigravity-cli", "brain")
	_ = os.MkdirAll(geminiBrainDir, 0o755)

	syncedBrainDir := filepath.Join(tempDir, "synced-brain")
	_ = os.MkdirAll(syncedBrainDir, 0o755)
	writeTranscriptFile(t, syncedBrainDir, "sess-1", `{"step_index":0,"source":"USER","content":"hi"}`+"\n", time.Now().Add(-time.Hour))

	tests := []struct {
		name          string
		nilStore      bool
		brainDir      string
		useConfig     bool
		geminiHomeDir string
		withLLM       bool
		wantErr       bool
	}{
		{"nil store returns empty stats without error", true, brainDir, false, "", false, false},
		{"valid store with existing brainDir", false, brainDir, true, "", false, false},
		{"valid store with non-existent brainDir", false, filepath.Join(tempDir, "nonexistent"), true, "", false, true},
		{"empty brainDir derived from config DataDir", false, "", true, "", true, false},
		{"empty brainDir derived from config GeminiHomeDir", false, "", true, homeDir, false, false},
		{"valid store with transcripts synced", false, syncedBrainDir, true, "", false, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var store db.TranscriptStore
			if !tc.nilStore {
				store = dbtest.NewFakeStore()
			}
			var cfg *config.Config
			if tc.useConfig {
				dataDir := tempDir
				if tc.geminiHomeDir != "" {
					dataDir = ""
				}
				cfg = config.NewFromData(&config.ConfigData{
					DataDir:        dataDir,
					GeminiHomeDir:  tc.geminiHomeDir,
					LowEffortModel: "gemini-3.8-flash-low",
					Ollama:         config.OllamaConfig{BaseURL: "http://127.0.0.1:9", Model: "all-minilm"},
				})
			}
			var llm LLMClientFunc
			if tc.withLLM {
				llm = func(ctx context.Context, prompt string) (string, error) {
					return "summary", nil
				}
			}
			stats, err := RunTranscriptSync(context.Background(), store, cfg, tc.brainDir, llm)
			if (err != nil) != tc.wantErr {
				t.Errorf("RunTranscriptSync error = %v, wantErr = %v", err, tc.wantErr)
			}
			if tc.nilStore && (stats.Synced != 0 || stats.StepsInserted != 0) {
				t.Errorf("expected 0 stats on nil store, got %+v", stats)
			}
		})
	}
}

func TestStartTranscriptSync_TableDriven(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	brainDir := filepath.Join(tempDir, "brain")
	_ = os.MkdirAll(brainDir, 0o755)

	tests := []struct {
		name        string
		interval    time.Duration
		withError   bool
		expectTicks int
	}{
		{"custom short interval", 2 * time.Millisecond, false, 2},
		{"default fallback interval", 0, false, 1},
		{"startup and periodic sync error logging", 2 * time.Millisecond, true, 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := t.Context()
			notify := make(chan struct{}, 10)
			fakeStore := dbtest.NewFakeStore()
			var store db.TranscriptStore
			if tc.withError {
				store = &channelTranscriptStore{TranscriptStore: fakeStore, notify: notify, err: errors.New("simulated sync error")}
			} else {
				store = &channelTranscriptStore{TranscriptStore: fakeStore, notify: notify}
			}
			cfg := config.NewFromData(&config.ConfigData{DataDir: tempDir})
			StartTranscriptSync(ctx, store, cfg, brainDir, nil, tc.interval)
			for i := range tc.expectTicks {
				select {
				case <-notify:
				case <-time.After(2 * time.Second):
					t.Fatalf("timed out waiting for transcript sync tick %d", i+1)
				}
			}
		})
	}
}

func TestConfiguredMCPServerNames(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cur  *config.ConfigData
		want []string
	}{
		{name: "nil snapshot", cur: nil, want: nil},
		{name: "no servers", cur: &config.ConfigData{}, want: nil},
		{
			name: "union across categories sorted and deduplicated",
			cur: &config.ConfigData{McpServers: config.TargetMcpConfig{
				Common:    map[string]json.RawMessage{"github": nil, "home_assistant": nil},
				Discord:   map[string]json.RawMessage{"discord": nil, "github": nil},
				Voice:     map[string]json.RawMessage{"home": nil},
				Ephemeral: map[string]json.RawMessage{"docket": nil},
			}},
			want: []string{"discord", "docket", "github", "home", "home_assistant"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := configuredMCPServerNames(tc.cur); !slices.Equal(got, tc.want) {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

type fakePubSubMessage struct {
	data    []byte
	subject string
}

func (m *fakePubSubMessage) Data() []byte                     { return m.data }
func (m *fakePubSubMessage) Subject() string                  { return m.subject }
func (m *fakePubSubMessage) Header() nats.Header              { return nil }
func (m *fakePubSubMessage) NumDelivered() uint64             { return 1 }
func (m *fakePubSubMessage) Ack() error                       { return nil }
func (m *fakePubSubMessage) NakWithDelay(time.Duration) error { return nil }
func (m *fakePubSubMessage) Term() error                      { return nil }
func (m *fakePubSubMessage) Snooze(time.Duration) error       { return nil }
func (m *fakePubSubMessage) InProgress() error                { return nil }

type failPublishBus struct {
	pubsub.Bus
}

func (b *failPublishBus) Publish(ctx context.Context, subject, msgID string, data any) error {
	return errors.New("simulated bus publish failure")
}

type handleTurnCompletedTestCase struct {
	name                  string
	nilMsg                bool
	rawJSON               string
	event                 pubsub.TurnCompletedEvent
	hasTranscript         bool
	transcriptData        string
	withEmbedderServer    bool
	failPublish           bool
	wantErr               bool
	errContains           string
	wantPublished         bool
	wantStepsInserted     int
	wantNonEmptyEmbedding bool
}

func setupTestEmbedderServer(t *testing.T, enabled bool) *httptest.Server {
	t.Helper()
	if !enabled {
		return nil
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		vec := make([]float32, db.ExpectedEmbeddingDim)
		vec[0] = 0.5
		_ = json.NewEncoder(w).Encode(map[string]any{"embedding": vec})
	}))
}

func assertStoredEmbedding(ctx context.Context, t *testing.T, store db.TranscriptStore, tc handleTurnCompletedTestCase) {
	t.Helper()
	if !tc.wantNonEmptyEmbedding {
		return
	}
	sums, err := store.SearchSessionSummaries(ctx, "", nil, "hello", 10, -1.0)
	if err != nil {
		t.Fatalf("SearchSessionSummaries failed: %v", err)
	}
	if len(sums) == 0 {
		t.Fatalf("expected stored session summary, got none")
	}
	if len(sums[0].Embedding) == 0 {
		t.Errorf("expected non-empty Embedding in stored session summary, got empty")
	}
}

func buildHandleTurnCompletedMsg(t *testing.T, tc handleTurnCompletedTestCase) pubsub.Message {
	t.Helper()
	if tc.nilMsg {
		return nil
	}
	if tc.rawJSON != "" {
		return &fakePubSubMessage{data: []byte(tc.rawJSON), subject: pubsub.SubjectAerialTurnCompleted}
	}
	payload, err := json.Marshal(tc.event)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	return &fakePubSubMessage{data: payload, subject: pubsub.SubjectAerialTurnCompleted}
}

func assertHandleTurnPublished(t *testing.T, publishedCh <-chan pubsub.TranscriptSyncedEvent, tc handleTurnCompletedTestCase) {
	t.Helper()
	if !tc.wantPublished {
		select {
		case ev := <-publishedCh:
			t.Fatalf("unexpected event published: %+v", ev)
		case <-time.After(50 * time.Millisecond):
		}
		return
	}
	select {
	case ev := <-publishedCh:
		if ev.SessionID != tc.event.SessionID {
			t.Errorf("ev.SessionID = %q, want %q", ev.SessionID, tc.event.SessionID)
		}
		if ev.ConversationID != tc.event.ConversationID {
			t.Errorf("ev.ConversationID = %q, want %q", ev.ConversationID, tc.event.ConversationID)
		}
		if ev.StepsInserted != tc.wantStepsInserted {
			t.Errorf("ev.StepsInserted = %d, want %d", ev.StepsInserted, tc.wantStepsInserted)
		}
		if ev.SyncedAt.IsZero() {
			t.Error("expected non-zero SyncedAt")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for TranscriptSyncedEvent")
	}
}

func runHandleTurnCompletedTest(t *testing.T, tc handleTurnCompletedTestCase) {
	t.Helper()
	ctx := context.Background()
	tempDir := t.TempDir()
	brainDir := filepath.Join(tempDir, "brain")
	_ = os.MkdirAll(brainDir, 0o755)

	store := dbtest.NewFakeStore()

	if tc.hasTranscript && tc.event.SessionID != "" {
		writeTranscriptFile(t, brainDir, tc.event.SessionID, tc.transcriptData, time.Now().Add(-5*time.Minute))
	}

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	publishedCh := make(chan pubsub.TranscriptSyncedEvent, 1)
	_, subErr := memBus.Subscribe(ctx, pubsub.SubjectAerialTurnTranscriptSynced, "test-sub", func(_ context.Context, msg pubsub.Message) error {
		var ev pubsub.TranscriptSyncedEvent
		if err := json.Unmarshal(msg.Data(), &ev); err == nil {
			select {
			case publishedCh <- ev:
			default:
			}
		}
		return nil
	})
	if subErr != nil {
		t.Fatalf("subscribe to memory bus: %v", subErr)
	}

	var bus pubsub.Bus = memBus
	if tc.failPublish {
		bus = &failPublishBus{Bus: memBus}
	}

	embedSrv := setupTestEmbedderServer(t, tc.withEmbedderServer)
	if embedSrv != nil {
		defer embedSrv.Close()
	}

	cfgData := &config.ConfigData{DataDir: tempDir}
	if embedSrv != nil {
		cfgData.Ollama = config.OllamaConfig{
			BaseURL: embedSrv.URL,
			Model:   "all-minilm",
		}
	}
	cfg := config.NewFromData(cfgData)

	msg := buildHandleTurnCompletedMsg(t, tc)
	err := HandleTurnCompletedTranscriptSync(ctx, msg, bus, store, cfg, brainDir, nil)
	if tc.wantErr {
		if err == nil {
			t.Fatalf("expected error containing %q, got nil", tc.errContains)
		}
		if !strings.Contains(err.Error(), tc.errContains) {
			t.Fatalf("expected error containing %q, got: %v", tc.errContains, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	assertHandleTurnPublished(t, publishedCh, tc)
	assertStoredEmbedding(ctx, t, store, tc)
}

func TestHandleTurnCompletedTranscriptSync_TableDriven(t *testing.T) {
	t.Parallel()
	const sampleJSONL = "{\"step_index\":0,\"source\":\"USER_EXPLICIT\",\"type\":\"USER_INPUT\",\"content\":\"hello\"}\n{\"step_index\":1,\"source\":\"MODEL\",\"type\":\"PLANNER_RESPONSE\",\"content\":\"world\"}\n"

	tests := []handleTurnCompletedTestCase{
		{
			name:          "nil message returns nil without processing",
			nilMsg:        true,
			wantPublished: false,
		},
		{
			name:          "invalid JSON payload returns nil and logs warning",
			rawJSON:       "not-valid-json",
			wantPublished: false,
		},
		{
			name: "empty conversation id ignored",
			event: pubsub.TurnCompletedEvent{
				SessionID:      "sess-1",
				ConversationID: "",
			},
			wantPublished: false,
		},
		{
			name: "healthcheck-ping session id ignored",
			event: pubsub.TurnCompletedEvent{
				SessionID:      "healthcheck-ping",
				ConversationID: "conv-1",
			},
			wantPublished: false,
		},
		{
			name: "empty session id ignored",
			event: pubsub.TurnCompletedEvent{
				SessionID:      "",
				ConversationID: "conv-1",
			},
			wantPublished: false,
		},
		{
			name: "valid turn with existing transcript indexes steps and publishes event",
			event: pubsub.TurnCompletedEvent{
				SessionID:      "sess-indexed",
				ConversationID: "conv-indexed",
				Source:         "discord",
				CompletedAt:    time.Now().UTC(),
			},
			hasTranscript:     true,
			transcriptData:    sampleJSONL,
			wantPublished:     true,
			wantStepsInserted: 2,
		},
		{
			name: "valid turn with embedder configured stores non-empty embedding",
			event: pubsub.TurnCompletedEvent{
				SessionID:      "sess-with-emb",
				ConversationID: "conv-with-emb",
				Source:         "discord",
				CompletedAt:    time.Now().UTC(),
			},
			hasTranscript:         true,
			transcriptData:        sampleJSONL,
			withEmbedderServer:    true,
			wantPublished:         true,
			wantStepsInserted:     2,
			wantNonEmptyEmbedding: true,
		},
		{
			name: "valid turn without transcript file unconditionally publishes event with zero steps",
			event: pubsub.TurnCompletedEvent{
				SessionID:      "sess-missing",
				ConversationID: "conv-missing",
				Source:         "voice",
				CompletedAt:    time.Now().UTC(),
			},
			hasTranscript:     false,
			wantPublished:     true,
			wantStepsInserted: 0,
		},
		{
			name: "bus publish error returns error",
			event: pubsub.TurnCompletedEvent{
				SessionID:      "sess-pub-err",
				ConversationID: "conv-pub-err",
				Source:         "discord",
				CompletedAt:    time.Now().UTC(),
			},
			failPublish: true,
			wantErr:     true,
			errContains: "simulated bus publish failure",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runHandleTurnCompletedTest(t, tc)
		})
	}
}

type startTurnSubscriberTestCase struct {
	name        string
	nilBus      bool
	publishTurn bool
	wantErr     bool
	errContains string
}

func runStartTurnSubscriberTest(t *testing.T, tc startTurnSubscriberTestCase, brainDir, tempDir string) {
	t.Helper()
	ctx := t.Context()
	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	var bus pubsub.Bus = memBus
	if tc.nilBus {
		bus = nil
	}

	store := dbtest.NewFakeStore()
	cfg := config.NewFromData(&config.ConfigData{DataDir: tempDir})

	syncedCh := make(chan pubsub.TranscriptSyncedEvent, 1)
	if !tc.nilBus {
		_, _ = memBus.Subscribe(ctx, pubsub.SubjectAerialTurnTranscriptSynced, "test-verify-sub", func(_ context.Context, msg pubsub.Message) error {
			var ev pubsub.TranscriptSyncedEvent
			if err := json.Unmarshal(msg.Data(), &ev); err == nil {
				select {
				case syncedCh <- ev:
				default:
				}
			}
			return nil
		})
	}

	err := StartTurnTranscriptSyncSubscriber(ctx, bus, store, cfg, brainDir, nil)
	if tc.wantErr {
		if err == nil {
			t.Fatalf("expected error containing %q, got nil", tc.errContains)
		}
		if !strings.Contains(err.Error(), tc.errContains) {
			t.Fatalf("expected error containing %q, got: %v", tc.errContains, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if tc.publishTurn {
		turnEv := pubsub.TurnCompletedEvent{
			SessionID:      "sub-turn-sess",
			ConversationID: "sub-turn-conv",
			Source:         "discord",
			CompletedAt:    time.Now().UTC(),
			Status:         "success",
		}
		if pubErr := memBus.Publish(ctx, pubsub.SubjectAerialTurnCompleted, "turn-1", turnEv); pubErr != nil {
			t.Fatalf("publish turn event: %v", pubErr)
		}
		select {
		case ev := <-syncedCh:
			if ev.SessionID != "sub-turn-sess" {
				t.Errorf("ev.SessionID = %q, want %q", ev.SessionID, "sub-turn-sess")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for synced event from queue consumer")
		}
	}
}

func TestStartTurnTranscriptSyncSubscriber_TableDriven(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	brainDir := filepath.Join(tempDir, "brain")
	_ = os.MkdirAll(brainDir, 0o755)

	tests := []startTurnSubscriberTestCase{
		{
			name:        "nil bus returns error",
			nilBus:      true,
			wantErr:     true,
			errContains: "bus is nil",
		},
		{
			name:        "subscribes durable queue and receives turn completed event",
			nilBus:      false,
			publishTurn: true,
			wantErr:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runStartTurnSubscriberTest(t, tc, brainDir, tempDir)
		})
	}
}

package queue

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/transcriptwire"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/delivery"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

func TestTurnExecution_TouchedSessionIDs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		touches    []string
		wantResult []string
	}{
		{
			name:       "empty touches returns empty slice",
			touches:    nil,
			wantResult: nil,
		},
		{
			name:       "deduplicates and preserves arrival order",
			touches:    []string{"sess-1", "sess-2", "sess-1", "  ", "sess-3", "sess-2"},
			wantResult: []string{"sess-1", "sess-2", "sess-3"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			te := &turnExecution{}
			for _, sid := range tc.touches {
				te.touchSessionIDLocked(sid)
			}
			got := te.touchedSessionIDs()
			if len(got) != len(tc.wantResult) {
				t.Fatalf("touchedSessionIDs() len = %d, want %d: %v", len(got), len(tc.wantResult), got)
			}
			for i := range got {
				if got[i] != tc.wantResult[i] {
					t.Errorf("touchedSessionIDs()[%d] = %q, want %q", i, got[i], tc.wantResult[i])
				}
			}
		})
	}

	t.Run("enqueueTranscriptPublish with custom trigger source", func(t *testing.T) {
		t.Parallel()
		p := &WorkerPool{
			eventBus:              pubsub.NewMemoryBus(),
			transcriptsIndexerURL: "http://indexer.local",
			transcriptPublishCh:   make(chan transcriptPublishJob, 5),
		}
		te := &turnExecution{
			pool:        p,
			threadID:    "thread-voice",
			triggerType: "voice",
		}
		te.touchSessionIDLocked("sess-v1")
		te.enqueueTranscriptPublish()

		select {
		case j := <-p.transcriptPublishCh:
			if j.source != "voice" || j.sessionID != "sess-v1" {
				t.Fatalf("unexpected job: %+v", j)
			}
		default:
			t.Fatal("expected published job on channel")
		}

		var nilTe *turnExecution
		nilTe.enqueueTranscriptPublish()
		if nilTe.touchedSessionIDs() != nil {
			t.Errorf("expected nil from nil te.touchedSessionIDs")
		}
	})
}

func TestWorkerPool_TranscriptPublishWorker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	var mu sync.Mutex
	watermarkQueryCounts := make(map[string]int)
	mockIndexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if strings.HasPrefix(r.URL.Path, "/watermark") {
			sid := r.URL.Query().Get("session_id")
			watermarkQueryCounts[sid]++
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"last_indexed_step":0}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer mockIndexer.Close()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	receivedBatches := make(chan transcriptwire.TranscriptStepBatch, 10)
	_, err := memBus.Subscribe(ctx, pubsub.SubjectTranscriptsStepsPrefix+"*", "test-tx-worker-sub", func(_ context.Context, msg pubsub.Message) error {
		var batch transcriptwire.TranscriptStepBatch
		if err := json.Unmarshal(msg.Data(), &batch); err == nil {
			receivedBatches <- batch
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	tmpDir := t.TempDir()
	sessID := "sess-publish-test"
	sessDir := filepath.Join(tmpDir, "brain", sessID, ".system_generated", "logs")
	if err := os.MkdirAll(sessDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	transcriptPath := filepath.Join(sessDir, "transcript.jsonl")
	content := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"reply 1"}
{"step_index":2,"source":"TOOL","type":"GENERIC","content":"result 2"}
`
	if err := os.WriteFile(transcriptPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	store := dbtest.NewFakeStore()
	if err := store.SaveSessionID(ctx, "thread-tx-test", sessID); err != nil {
		t.Fatalf("SaveSessionID: %v", err)
	}

	cfg := config.NewFromData(&config.ConfigData{
		DataDir: tmpDir,
		Transcripts: config.TranscriptsConfig{
			IndexerURL: mockIndexer.URL,
			Stack:      "aerial",
		},
	})

	pool := New(cfg, WorkerPoolConfig{
		Store:                  store,
		EventBus:               memBus,
		TranscriptsIndexerURL:  mockIndexer.URL,
		TranscriptRecheckDelay: 20 * time.Millisecond,
	})
	pool.Start()
	defer pool.Stop()

	// Enqueue transcript publish
	pool.EnqueueTranscriptPublish(sessID, "thread-tx-test", "discord")

	select {
	case batch := <-receivedBatches:
		if batch.SessionID != sessID {
			t.Errorf("batch.SessionID = %q, want %q", batch.SessionID, sessID)
		}
		if batch.ThreadID != "thread-tx-test" {
			t.Errorf("batch.ThreadID = %q, want %q", batch.ThreadID, "thread-tx-test")
		}
		if len(batch.Steps) != 2 {
			t.Errorf("len(batch.Steps) = %d, want 2", len(batch.Steps))
		}
		if batch.Steps[0].StepIndex != 1 || batch.Steps[1].StepIndex != 2 {
			t.Errorf("step indices = [%d, %d], want [1, 2]", batch.Steps[0].StepIndex, batch.Steps[1].StepIndex)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for transcript batch publish")
	}

	mu.Lock()
	qCount := watermarkQueryCounts[sessID]
	mu.Unlock()
	if qCount == 0 {
		t.Errorf("expected watermark query for session %s, got 0", sessID)
	}
}

func TestWorkerPool_TranscriptPublishWorker_ParkMiss(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mockIndexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"last_indexed_step":0}`))
	}))
	defer mockIndexer.Close()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	receivedBatches := make(chan transcriptwire.TranscriptStepBatch, 10)
	_, err := memBus.Subscribe(ctx, pubsub.SubjectTranscriptsStepsPrefix+"*", "test-park-sub", func(_ context.Context, msg pubsub.Message) error {
		var batch transcriptwire.TranscriptStepBatch
		if err := json.Unmarshal(msg.Data(), &batch); err == nil {
			receivedBatches <- batch
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	tmpDir := t.TempDir()
	sessID := "sess-park-miss-test"
	sessDir := filepath.Join(tmpDir, "brain", sessID, ".system_generated", "logs")
	if err := os.MkdirAll(sessDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	transcriptPath := filepath.Join(sessDir, "transcript.jsonl")
	content := `{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"parked reply"}
`
	if err := os.WriteFile(transcriptPath, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	store := dbtest.NewFakeStore()
	// Pre-seed an unrelated session so store has turn rows and does not treat allowlist as nil
	_ = store.SaveSessionID(ctx, "thread-other", "sess-other")

	cfg := config.NewFromData(&config.ConfigData{
		DataDir: tmpDir,
		Transcripts: config.TranscriptsConfig{
			IndexerURL: mockIndexer.URL,
			Stack:      "aerial",
		},
	})

	pool := New(cfg, WorkerPoolConfig{
		Store:                  store,
		EventBus:               memBus,
		TranscriptsIndexerURL:  mockIndexer.URL,
		TranscriptRecheckDelay: 25 * time.Millisecond,
	})
	pool.Start()
	defer pool.Stop()

	// Enqueue before adding to allowlist -> should park
	pool.EnqueueTranscriptPublish(sessID, "thread-park-test", "voice")

	// Now add to allowlist while parked
	if err := store.SaveSessionID(ctx, "thread-park-test", sessID); err != nil {
		t.Fatalf("SaveSessionID: %v", err)
	}

	// Wait for parked retry to process and publish
	select {
	case batch := <-receivedBatches:
		if batch.SessionID != sessID {
			t.Errorf("batch.SessionID = %q, want %q", batch.SessionID, sessID)
		}
		if len(batch.Steps) != 1 || batch.Steps[0].StepIndex != 1 {
			t.Errorf("steps = %v, want 1 step at index 1", batch.Steps)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for parked retry transcript publish")
	}
}

func TestWorkerPool_RunStartupResend(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mockIndexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if strings.HasPrefix(r.URL.Path, "/watermarks") {
			_, _ = w.Write([]byte(`{"watermarks":[{"session_id":"sess-resend-1","last_indexed_step":1}]}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer mockIndexer.Close()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	receivedBatches := make(chan transcriptwire.TranscriptStepBatch, 10)
	_, err := memBus.Subscribe(ctx, pubsub.SubjectTranscriptsStepsPrefix+"*", "test-resend-sub", func(_ context.Context, msg pubsub.Message) error {
		var batch transcriptwire.TranscriptStepBatch
		if err := json.Unmarshal(msg.Data(), &batch); err == nil {
			receivedBatches <- batch
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	tmpDir := t.TempDir()
	// sess-resend-1 has steps 0, 1, 2; watermark is 1, so only step 2 should be published.
	s1Dir := filepath.Join(tmpDir, "brain", "sess-resend-1", ".system_generated", "logs")
	_ = os.MkdirAll(s1Dir, 0o755)
	_ = os.WriteFile(filepath.Join(s1Dir, "transcript.jsonl"), []byte(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"step 1"}
{"step_index":2,"source":"MODEL","type":"PLANNER_RESPONSE","content":"step 2"}
`), 0o644)

	store := dbtest.NewFakeStore()
	_ = store.SaveSessionID(ctx, "thread-1", "sess-resend-1")

	cfg := config.NewFromData(&config.ConfigData{
		DataDir: tmpDir,
		Transcripts: config.TranscriptsConfig{
			IndexerURL: mockIndexer.URL,
			Stack:      "aerial",
		},
	})

	pool := New(cfg, WorkerPoolConfig{
		Store:                 store,
		EventBus:              memBus,
		TranscriptsIndexerURL: mockIndexer.URL,
	})

	if err := pool.RunStartupResend(ctx); err != nil {
		t.Fatalf("RunStartupResend error: %v", err)
	}

	select {
	case batch := <-receivedBatches:
		if batch.SessionID != "sess-resend-1" {
			t.Errorf("batch.SessionID = %q, want sess-resend-1", batch.SessionID)
		}
		if len(batch.Steps) != 1 || batch.Steps[0].StepIndex != 2 {
			t.Errorf("batch.Steps = %v, want 1 step at index 2", batch.Steps)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for resend batch publish")
	}
}

func TestWorkerPool_StopDrainsQueuedTranscriptJob(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mockIndexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"last_indexed_step":0}`))
	}))
	defer mockIndexer.Close()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	receivedBatches := make(chan transcriptwire.TranscriptStepBatch, 10)
	_, err := memBus.Subscribe(ctx, pubsub.SubjectTranscriptsStepsPrefix+"*", "test-stop-drain-sub", func(_ context.Context, msg pubsub.Message) error {
		var batch transcriptwire.TranscriptStepBatch
		if err := json.Unmarshal(msg.Data(), &batch); err == nil {
			receivedBatches <- batch
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	tmpDir := t.TempDir()
	sessID := "sess-stop-drain"
	sessDir := filepath.Join(tmpDir, "brain", sessID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0o755)
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"reply before stop"}
`), 0o644)

	store := dbtest.NewFakeStore()
	_ = store.SaveSessionID(ctx, "thread-stop-drain", sessID)

	cfg := config.NewFromData(&config.ConfigData{
		DataDir: tmpDir,
		Transcripts: config.TranscriptsConfig{
			IndexerURL: mockIndexer.URL,
			Stack:      "aerial",
		},
	})

	pool := New(cfg, WorkerPoolConfig{
		Store:                 store,
		EventBus:              memBus,
		TranscriptsIndexerURL: mockIndexer.URL,
	})
	// Initialize context and channel without starting background worker to test shutdown drain
	pool.ctx, pool.cancel = context.WithCancel(context.Background())
	pool.transcriptPublishCh = make(chan transcriptPublishJob, 10)
	pool.transcriptPublishCh <- transcriptPublishJob{
		sessionID: sessID,
		threadID:  "thread-stop-drain",
		source:    "turn-end",
	}

	// Stop pool -> must drain queued jobs and publish before exiting
	pool.Stop()

	select {
	case batch := <-receivedBatches:
		if batch.SessionID != sessID {
			t.Errorf("batch.SessionID = %q, want %q", batch.SessionID, sessID)
		}
		if len(batch.Steps) != 1 || batch.Steps[0].StepIndex != 1 {
			t.Errorf("steps = %v, want 1 step at index 1", batch.Steps)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for shutdown drain transcript publish")
	}
}

func TestWorkerPool_ConcurrentPublishAndStartupResend(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mockIndexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if strings.HasPrefix(r.URL.Path, "/watermarks") {
			_, _ = w.Write([]byte(`{"watermarks":[{"session_id":"sess-concurrent-tx","last_indexed_step":0}]}`))
			return
		}
		if strings.HasPrefix(r.URL.Path, "/watermark") {
			_, _ = w.Write([]byte(`{"last_indexed_step":0}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer mockIndexer.Close()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	if err := pubsub.EnsureCoreStreams(ctx, memBus); err != nil {
		t.Fatalf("EnsureCoreStreams: %v", err)
	}

	receivedBatches := make(chan transcriptwire.TranscriptStepBatch, 20)
	_, err := memBus.Subscribe(ctx, pubsub.SubjectTranscriptsStepsPrefix+"*", "test-resend-concurrent-sub", func(_ context.Context, msg pubsub.Message) error {
		var batch transcriptwire.TranscriptStepBatch
		if err := json.Unmarshal(msg.Data(), &batch); err == nil {
			receivedBatches <- batch
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	tmpDir := t.TempDir()
	sessID := "sess-concurrent-tx"
	sessDir := filepath.Join(tmpDir, "brain", sessID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0o755)
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"reply 1"}
{"step_index":2,"source":"MODEL","type":"PLANNER_RESPONSE","content":"reply 2"}
`), 0o644)

	store := dbtest.NewFakeStore()
	_ = store.SaveSessionID(ctx, "thread-concurrent-tx", sessID)

	cfg := config.NewFromData(&config.ConfigData{
		DataDir: tmpDir,
		Transcripts: config.TranscriptsConfig{
			IndexerURL: mockIndexer.URL,
			Stack:      "aerial",
		},
	})

	pool := New(cfg, WorkerPoolConfig{
		Store:                  store,
		EventBus:               memBus,
		TranscriptsIndexerURL:  mockIndexer.URL,
		TranscriptRecheckDelay: 10 * time.Millisecond,
	})
	pool.Start()
	defer pool.Stop()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		pool.EnqueueTranscriptPublish(sessID, "thread-concurrent-tx", "turn-end")
	}()
	go func() {
		defer wg.Done()
		_ = pool.RunStartupResend(ctx)
	}()
	wg.Wait()

	select {
	case batch := <-receivedBatches:
		if batch.SessionID != sessID {
			t.Errorf("batch.SessionID = %q, want %q", batch.SessionID, sessID)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for concurrent batch publish")
	}
}

func TestWorkerPool_TranscriptPublish_Guards(t *testing.T) {
	t.Parallel()
	var nilPool *WorkerPool
	nilPool.EnqueueTranscriptPublish("s1", "t1", "test")

	pool := &WorkerPool{}
	pool.EnqueueTranscriptPublish("", "t1", "test")
	pool.EnqueueTranscriptPublish("   ", "t1", "test")
}

func TestWorkerPool_RunStartupResend_ErrorsAndEdges(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1. Watermarks error from indexer returns error
	errIndexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}))
	defer errIndexer.Close()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	_ = pubsub.EnsureCoreStreams(ctx, memBus)

	poolErr := New(nil, WorkerPoolConfig{
		EventBus:              memBus,
		TranscriptsIndexerURL: errIndexer.URL,
	})
	if err := poolErr.RunStartupResend(ctx); err == nil {
		t.Errorf("expected error on failed watermarks fetch, got nil")
	}

	// 2. Unconfigured indexer returns nil
	poolNoIndexer := New(nil, WorkerPoolConfig{EventBus: memBus})
	if err := poolNoIndexer.RunStartupResend(ctx); err != nil {
		t.Errorf("expected nil error on unconfigured indexer, got %v", err)
	}

	// 3. Store error on GetKnownTurnSessionIDs logs warning and continues
	okIndexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"watermarks":[]}`))
	}))
	defer okIndexer.Close()

	tmpDir := t.TempDir()
	storeErr := dbtest.NewFakeStore()
	storeErr.FailNext("GetKnownTurnSessionIDs", errors.New("db error"))

	cfg := config.NewFromData(&config.ConfigData{
		DataDir: tmpDir,
		Transcripts: config.TranscriptsConfig{
			IndexerURL: okIndexer.URL,
			Stack:      "aerial",
		},
	})
	poolStoreErr := New(cfg, WorkerPoolConfig{
		Store:                 storeErr,
		EventBus:              memBus,
		TranscriptsIndexerURL: okIndexer.URL,
	})
	if err := poolStoreErr.RunStartupResend(ctx); err != nil {
		t.Errorf("expected nil error on store failure, got %v", err)
	}

	// 4. Empty brainDir returns nil
	poolNoBrain := New(nil, WorkerPoolConfig{
		EventBus:              memBus,
		TranscriptsIndexerURL: okIndexer.URL,
	})
	if err := poolNoBrain.RunStartupResend(ctx); err != nil {
		t.Errorf("expected nil error on empty brainDir, got %v", err)
	}

	// 5. Mixed valid and corrupt sessions in brainDir: skips corrupt and sends valid
	sessValidDir := filepath.Join(tmpDir, "sess-valid", ".system_generated", "logs")
	_ = os.MkdirAll(sessValidDir, 0o755)
	_ = os.WriteFile(filepath.Join(sessValidDir, "transcript.jsonl"), []byte(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"step 1"}
`), 0o644)

	sessBadDir := filepath.Join(tmpDir, "sess-bad", ".system_generated", "logs")
	_ = os.MkdirAll(sessBadDir, 0o755)
	_ = os.WriteFile(filepath.Join(sessBadDir, "transcript.jsonl"), []byte(`corrupt non-json content`), 0o644)

	storeMulti := dbtest.NewFakeStore()
	_ = storeMulti.SaveSessionID(ctx, "t-v", "sess-valid")
	_ = storeMulti.SaveSessionID(ctx, "t-b", "sess-bad")

	poolMulti := New(config.NewFromData(&config.ConfigData{
		DataDir: tmpDir,
		Transcripts: config.TranscriptsConfig{
			IndexerURL: okIndexer.URL,
			Stack:      "aerial",
		},
	}), WorkerPoolConfig{
		Store:                 storeMulti,
		EventBus:              memBus,
		TranscriptsIndexerURL: okIndexer.URL,
	})
	if err := poolMulti.RunStartupResend(ctx); err != nil {
		t.Errorf("expected RunStartupResend to succeed skipping bad session, got %v", err)
	}
}

func TestWorkerPool_TranscriptPublish_ErrorBranches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// 1. Watermark fetch error falls back to -1 and still publishes
	errIndexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "indexer unavailable", http.StatusBadGateway)
	}))
	defer errIndexer.Close()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	_ = pubsub.EnsureCoreStreams(ctx, memBus)

	tmpDir := t.TempDir()
	sessID := "sess-wm-err"
	sessDir := filepath.Join(tmpDir, "brain", sessID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0o755)
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"step 1"}
`), 0o644)

	store := dbtest.NewFakeStore()
	_ = store.SaveSessionID(ctx, "thread-wm-err", sessID)

	cfg := config.NewFromData(&config.ConfigData{
		DataDir: tmpDir,
		Transcripts: config.TranscriptsConfig{
			IndexerURL: errIndexer.URL,
			Stack:      "aerial",
		},
		McpServers: config.TargetMcpConfig{
			Common:  map[string]json.RawMessage{"mem": []byte("{}")},
			Discord: map[string]json.RawMessage{"disc": []byte("{}")},
			Voice:   map[string]json.RawMessage{"sens": []byte("{}")},
		},
	})

	pool := New(cfg, WorkerPoolConfig{
		Store:                 store,
		EventBus:              memBus,
		TranscriptsIndexerURL: errIndexer.URL,
	})
	pool.ctx, pool.cancel = context.WithCancel(context.Background())
	defer pool.cancel()

	// Process job with watermark fetch error -> still processes
	pool.processTranscriptPublishJob(transcriptPublishJob{
		sessionID: sessID,
		threadID:  "thread-wm-err",
		source:    "turn-end",
	})

	// Process with missing transcript file and retried=true -> drops
	pool.processTranscriptPublishJob(transcriptPublishJob{
		sessionID: "sess-missing-file",
		threadID:  "thread-missing",
		retried:   true,
	})

	// Process with cancelled context -> returns immediately
	cancelCtx, cancelFn := context.WithCancel(context.Background())
	cancelFn()
	pool.processTranscriptPublishJobWithContext(cancelCtx, transcriptPublishJob{sessionID: sessID})

	// Process job with corrupt transcript file -> logs warning and returns
	sessCorruptID := "sess-corrupt"
	sessCorruptDir := filepath.Join(tmpDir, "brain", sessCorruptID, ".system_generated", "logs")
	_ = os.MkdirAll(sessCorruptDir, 0o755)
	_ = os.WriteFile(filepath.Join(sessCorruptDir, "transcript.jsonl"), []byte(`corrupt invalid content`), 0o644)
	_ = store.SaveSessionID(ctx, "thread-corrupt", sessCorruptID)
	pool.processTranscriptPublishJob(transcriptPublishJob{
		sessionID: sessCorruptID,
		threadID:  "thread-corrupt",
	})

	// transcriptRoots with sessionMgr
	sm := session.New(tmpDir, tmpDir)
	poolWithSM := New(nil, WorkerPoolConfig{SessionManager: sm})
	bDir, eDirs := poolWithSM.transcriptRoots()
	roots := sm.Roots()
	if len(roots) > 0 && (bDir != roots[0] || len(eDirs) != len(roots)-1) {
		t.Errorf("unexpected roots: %s, %v", bDir, eDirs)
	}
}

func TestWorkerPool_TranscriptPublish_RecheckAndPark(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	txPath := filepath.Join(tmpDir, "transcript.jsonl")
	_ = os.WriteFile(txPath, []byte("line1\n"), 0o644)
	fi, _ := os.Stat(txPath)

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	p := &WorkerPool{
		eventBus:                memBus,
		transcriptsIndexerURL:   "http://indexer.local",
		transcriptRecheckDelay:  1 * time.Millisecond,
		transcriptPublishCh:     make(chan transcriptPublishJob, 10),
		transcriptWorkerStarted: true,
	}
	p.ctx, p.cancel = context.WithCancel(context.Background())
	defer p.cancel()

	// 1. File modified -> triggers EnqueueTranscriptPublish
	_ = os.WriteFile(txPath, []byte("line1\nline2\n"), 0o644)
	p.recheckSessionTranscript("s1", "t1", txPath, fi.ModTime().UTC().Add(-time.Hour), fi.Size())

	select {
	case job := <-p.transcriptPublishCh:
		if job.source != "recheck" {
			t.Fatalf("job.source = %q, want recheck", job.source)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for recheck publish job")
	}

	// 2. File unchanged -> returns without enqueueing
	fi2, _ := os.Stat(txPath)
	p.recheckSessionTranscript("s2", "t2", txPath, fi2.ModTime().UTC(), fi2.Size())

	// 3. Stat error -> returns
	p.recheckSessionTranscript("s3", "t3", "/tmp/does-not-exist-file-123456", time.Now(), 0)

	// Park job
	p.parkTranscriptJob(transcriptPublishJob{sessionID: "s-park"})
	select {
	case j := <-p.transcriptPublishCh:
		if !j.retried || j.sessionID != "s-park" {
			t.Fatalf("unexpected parked job: %+v", j)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for parked job")
	}

	// Unknown session in allowlist dropped
	store := dbtest.NewFakeStore()
	_ = store.SaveSessionID(context.Background(), "t-other", "s-other")
	pAllow := &WorkerPool{cfg: WorkerPoolConfig{Store: store}}
	pAllow.ctx, pAllow.cancel = context.WithCancel(context.Background())
	defer pAllow.cancel()
	pAllow.processTranscriptPublishJob(transcriptPublishJob{sessionID: "s-unknown", retried: true})
}

func TestWorkerPool_TranscriptPublish_InterruptAndMatch(t *testing.T) {
	t.Parallel()
	logInterruptWarning("op", nil)
	logInterruptWarning("op", errors.New("simulated error"))

	if matchesDispatchTarget(nil, "d1", "b1") {
		t.Errorf("expected false for nil dispatch")
	}
	d1 := &db.Dispatch{ID: "d1", TargetID: "b1"}
	if !matchesDispatchTarget(d1, "d1", "") {
		t.Errorf("expected true for matching dispatch ID")
	}
	if !matchesDispatchTarget(d1, "", "b1") {
		t.Errorf("expected true for matching build ID")
	}
	if matchesDispatchTarget(d1, "other", "other") {
		t.Errorf("expected false for non-matching")
	}

	var nilPool *WorkerPool
	nilPool.CloseThreadProcesses("", "test")
	nilPool.CloseThreadProcesses("thread-1", "test")
	nilPool.SetEventBus(nil)
	if nilPool.EventBus() != nil {
		t.Errorf("expected nil bus")
	}
	if err := nilPool.DeliverDirect("ch", "msg"); err == nil {
		t.Errorf("expected error on nil pool")
	}
	if err := nilPool.DeliverDirectWithAttachments("ch", "msg", []*delivery.Attachment{{Filename: "f.txt"}}); err == nil {
		t.Errorf("expected error on nil pool")
	}
	DrainThreadPendingMessages(context.Background(), nil, "", "")
}

func TestWorkerPool_TranscriptPublish_NilAndFallbackAccessors(t *testing.T) {
	t.Parallel()
	var nilPool *WorkerPool
	if nilPool.transcriptsIndexerBaseURL() != "" || nilPool.stackName() != "" {
		t.Errorf("expected empty string for nil pool")
	}
	if nilPool.resolvedTranscriptStore() != nil || nilPool.mcpServerNames() != nil {
		t.Errorf("expected nil for nil pool")
	}
	if b, e := nilPool.transcriptRoots(); b != "" || e != nil {
		t.Errorf("expected empty roots for nil pool")
	}
	nilPool.ensureTranscriptPublishWorker()
	nilPool.parkTranscriptJob(transcriptPublishJob{})
	if err := nilPool.RunStartupResend(context.Background()); err != nil {
		t.Errorf("expected nil error on nil pool startup resend")
	}

	p := &WorkerPool{}
	if p.ensureTranscriptPublisherLocked() != nil {
		t.Errorf("expected nil publisher without bus")
	}
	if p.ensureIndexerClientLocked() != nil {
		t.Errorf("expected nil indexer without URL")
	}
	if p.transcriptsIndexerBaseURL() != "" || p.stackName() != "" {
		t.Errorf("expected empty string for unconfigured pool")
	}
	if p.mcpServerNames() != nil {
		t.Errorf("expected nil mcp servers")
	}
	if b, e := p.transcriptRoots(); b != "" || e != nil {
		t.Errorf("expected empty roots for unconfigured pool")
	}

	fakeStore := dbtest.NewFakeStore()
	pWithStore := &WorkerPool{transcriptStore: fakeStore}
	if pWithStore.resolvedTranscriptStore() != fakeStore {
		t.Errorf("expected explicit transcriptStore")
	}
	pWithCfgStore := &WorkerPool{cfg: WorkerPoolConfig{Store: fakeStore}}
	if pWithCfgStore.resolvedTranscriptStore() != fakeStore {
		t.Errorf("expected cfg transcriptStore")
	}
}

func TestWorkerPool_TranscriptPublish_EdgeBranchesAndDrains(t *testing.T) {
	t.Parallel()
	pStopped := &WorkerPool{
		stopped:               true,
		transcriptsIndexerURL: "http://indexer.local",
		eventBus:              pubsub.NewMemoryBus(),
	}
	pStopped.EnqueueTranscriptPublish("s-stopped", "t1", "discord")
	pStopped.ensureTranscriptPublishWorker()

	pNoCfg := &WorkerPool{cfg: WorkerPoolConfig{NATSURL: "nats://fallback:4222"}}
	if pNoCfg.NATSURL() != "nats://fallback:4222" {
		t.Errorf("fallback NATSURL = %q", pNoCfg.NATSURL())
	}

	var nilP *WorkerPool
	nilP.drainTranscriptPublisher(context.Background())

	pCred := &WorkerPool{cfg: WorkerPoolConfig{AgyBin: "/bin/agy", APIKey: "key"}}
	bin, key := pCred.runnerCredentials()
	if bin != "/bin/agy" || key != "key" {
		t.Errorf("runnerCredentials = %q, %q", bin, key)
	}

	pCache := &WorkerPool{
		eventBus:              pubsub.NewMemoryBus(),
		transcriptsIndexerURL: "http://indexer.local",
	}
	pub1 := pCache.ensureTranscriptPublisher()
	pub2 := pCache.ensureTranscriptPublisher()
	if pub1 == nil || pub1 != pub2 {
		t.Errorf("publisher caching failed")
	}

	idx1 := pCache.ensureIndexerClient()
	idx2 := pCache.ensureIndexerClient()
	if idx1 == nil || idx1 != idx2 {
		t.Errorf("indexer caching failed")
	}

	pCache.transcriptWorkerStarted = true
	pCache.ensureTranscriptPublishWorker()

	store := dbtest.NewFakeStore()
	_ = store.InsertMessage(context.Background(), db.Message{ID: "m1", ThreadID: "t-drain", Status: db.StatusPending})
	DrainThreadPendingMessages(context.Background(), store, "t-drain", "cancelled")
	m, _ := store.GetMessage(context.Background(), "m1")
	if m == nil || m.Status != db.StatusFailed {
		t.Errorf("expected m1 to be failed")
	}
}

func TestWorkerPool_TranscriptPublish_CallbacksAndErrors(t *testing.T) {
	t.Parallel()
	store := dbtest.NewFakeStore()
	_ = store.SaveSessionID(context.Background(), "k1", "sess-1")
	store.FailNext("GetSessionTurnCount", errors.New("err"))
	getRec, onRot := makeSessionCallbacks(store)
	rec, err := getRec(context.Background(), "k1")
	if err != nil || rec.TurnCount != 0 {
		t.Errorf("expected 0 turn count on error")
	}

	store.FailNext("GetPreviousSessionID", errors.New("err"))
	rec2, err := getRec(context.Background(), "k1")
	if err != nil || rec2.PreviousSessionID != "" {
		t.Errorf("expected empty previous session ID on error")
	}

	if err := onRot(context.Background(), "k1", "sess-1", "sess-2"); err != nil {
		t.Errorf("rotation failed: %v", err)
	}

	storeErr := dbtest.NewFakeStore()
	storeErr.FailNext("GetPendingOrProcessingMessages", errors.New("simulated error"))
	DrainThreadPendingMessages(context.Background(), storeErr, "t-err", "cancelled")
}

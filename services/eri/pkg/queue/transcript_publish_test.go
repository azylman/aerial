package queue

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
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/transcriptwire"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
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
			eventBus:                pubsub.NewMemoryBus(),
			transcriptsIndexerURL:   "http://indexer.local",
			transcriptPublishCh:     make(chan transcriptPublishJob, 5),
			transcriptWorkerStarted: true,
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
	sessValidDir := filepath.Join(tmpDir, "brain", "sess-valid", ".system_generated", "logs")
	_ = os.MkdirAll(sessValidDir, 0o755)
	_ = os.WriteFile(filepath.Join(sessValidDir, "transcript.jsonl"), []byte(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"step 1"}
`), 0o644)

	sessBadDir := filepath.Join(tmpDir, "brain", "sess-bad", ".system_generated", "logs")
	_ = os.MkdirAll(sessBadDir, 0o755)
	_ = os.WriteFile(filepath.Join(sessBadDir, "transcript.jsonl"), []byte(`corrupt non-json content`), 0o644)

	storeMulti := dbtest.NewFakeStore()
	_ = storeMulti.SaveSessionID(ctx, "t-v", "sess-valid")
	_ = storeMulti.SaveSessionID(ctx, "t-b", "sess-bad")

	validBatches := make(chan transcriptwire.TranscriptStepBatch, 10)
	_, subErr := memBus.Subscribe(ctx, pubsub.SubjectTranscriptsStepsPrefix+"sess-valid", "test-valid-sub", func(_ context.Context, msg pubsub.Message) error {
		var batch transcriptwire.TranscriptStepBatch
		if uErr := json.Unmarshal(msg.Data(), &batch); uErr == nil {
			validBatches <- batch
		}
		return nil
	})
	if subErr != nil {
		t.Fatalf("Subscribe sess-valid: %v", subErr)
	}

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
	if resendErr := poolMulti.RunStartupResend(ctx); resendErr != nil {
		t.Errorf("expected RunStartupResend to succeed skipping bad session, got %v", resendErr)
	}

	select {
	case batchValid := <-validBatches:
		if batchValid.SessionID != "sess-valid" || len(batchValid.Steps) != 2 {
			t.Errorf("unexpected batch: %+v", batchValid)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for valid session batch")
	}
}

func TestWorkerPool_RunStartupResend_ExtraRootSession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	okIndexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"watermarks":[]}`))
	}))
	defer okIndexer.Close()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	_ = pubsub.EnsureCoreStreams(ctx, memBus)

	tmpDir := t.TempDir()
	storeMulti := dbtest.NewFakeStore()

	extraHome := t.TempDir()
	sessExtraDir := filepath.Join(extraHome, ".gemini", "antigravity-cli", "brain", "sess-extra", ".system_generated", "logs")
	_ = os.MkdirAll(sessExtraDir, 0o755)
	_ = os.WriteFile(filepath.Join(sessExtraDir, "transcript.jsonl"), []byte(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"extra hello"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"extra step"}
`), 0o644)
	_ = storeMulti.SaveSessionID(ctx, "t-extra", "sess-extra")

	extraBatches := make(chan transcriptwire.TranscriptStepBatch, 10)
	_, err := memBus.Subscribe(ctx, pubsub.SubjectTranscriptsStepsPrefix+"sess-extra", "test-extra-sub", func(_ context.Context, msg pubsub.Message) error {
		var batch transcriptwire.TranscriptStepBatch
		if uErr := json.Unmarshal(msg.Data(), &batch); uErr == nil {
			extraBatches <- batch
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Subscribe sess-extra: %v", err)
	}

	smWithExtra := session.New(extraHome, tmpDir)
	poolExtra := New(config.NewFromData(&config.ConfigData{
		DataDir: tmpDir,
		Transcripts: config.TranscriptsConfig{
			IndexerURL: okIndexer.URL,
			Stack:      "aerial",
		},
	}), WorkerPoolConfig{
		Store:                 storeMulti,
		EventBus:              memBus,
		TranscriptsIndexerURL: okIndexer.URL,
		SessionManager:        smWithExtra,
	})
	if rErr := poolExtra.RunStartupResend(ctx); rErr != nil {
		t.Errorf("expected RunStartupResend with extra root to succeed, got %v", rErr)
	}

	select {
	case batchExtra := <-extraBatches:
		if batchExtra.SessionID != "sess-extra" || len(batchExtra.Steps) != 2 {
			t.Errorf("unexpected extra batch: %+v", batchExtra)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for extra session batch")
	}
}

func TestWorkerPool_TranscriptPublish_WatermarkFallbackPublishes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	errIndexer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "indexer unavailable", http.StatusBadGateway)
	}))
	defer errIndexer.Close()

	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()
	_ = pubsub.EnsureCoreStreams(ctx, memBus)

	tmpDir := t.TempDir()
	sessID := "sess-wm-fallback"
	sessDir := filepath.Join(tmpDir, "brain", sessID, ".system_generated", "logs")
	_ = os.MkdirAll(sessDir, 0o755)
	_ = os.WriteFile(filepath.Join(sessDir, "transcript.jsonl"), []byte(`{"step_index":0,"source":"USER_EXPLICIT","type":"USER_INPUT","content":"hello"}
{"step_index":1,"source":"MODEL","type":"PLANNER_RESPONSE","content":"step 1"}
`), 0o644)

	store := dbtest.NewFakeStore()
	_ = store.SaveSessionID(ctx, "thread-wm-fallback", sessID)

	receivedBatches := make(chan transcriptwire.TranscriptStepBatch, 10)
	_, err := memBus.Subscribe(ctx, pubsub.SubjectTranscriptsStepsPrefix+sessID, "test-wm-fallback-sub", func(_ context.Context, msg pubsub.Message) error {
		var batch transcriptwire.TranscriptStepBatch
		if err := json.Unmarshal(msg.Data(), &batch); err == nil {
			receivedBatches <- batch
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed to subscribe to session batch subject: %v", err)
	}

	cfg := config.NewFromData(&config.ConfigData{
		DataDir: tmpDir,
		Transcripts: config.TranscriptsConfig{
			IndexerURL: errIndexer.URL,
			Stack:      "aerial",
		},
	})

	pool := New(cfg, WorkerPoolConfig{
		Store:                 store,
		EventBus:              memBus,
		TranscriptsIndexerURL: errIndexer.URL,
	})
	pool.ctx, pool.cancel = context.WithCancel(context.Background())
	defer pool.cancel()

	pool.processTranscriptPublishJob(transcriptPublishJob{
		sessionID: sessID,
		threadID:  "thread-wm-fallback",
		source:    "turn-end",
	})

	select {
	case batch := <-receivedBatches:
		if batch.SessionID != sessID || batch.FirstStep != 0 || batch.LastStep != 1 {
			t.Errorf("unexpected fallback batch: %+v", batch)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for fallback batch publish")
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

	// 2. File unmodified -> does not enqueue
	newFi, _ := os.Stat(txPath)
	p.recheckSessionTranscript("s1-unmod", "t1", txPath, newFi.ModTime().UTC(), newFi.Size())
	select {
	case j := <-p.transcriptPublishCh:
		t.Fatalf("unexpected job for unmodified file: %+v", j)
	case <-time.After(50 * time.Millisecond):
	}

	// 3. Nonexistent file -> does not enqueue
	p.recheckSessionTranscript("s1-missing", "t1", filepath.Join(tmpDir, "missing.jsonl"), time.Now().UTC(), 0)
	select {
	case j := <-p.transcriptPublishCh:
		t.Fatalf("unexpected job for missing file: %+v", j)
	case <-time.After(50 * time.Millisecond):
	}

	// 4. Park job
	p.parkTranscriptJob(transcriptPublishJob{sessionID: "s-park"})
	select {
	case j := <-p.transcriptPublishCh:
		if !j.retried || j.sessionID != "s-park" {
			t.Fatalf("unexpected parked job: %+v", j)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for parked job")
	}

	// 5. Cancelled context does not park or recheck
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	pCancelled := &WorkerPool{
		ctx:                    cancelledCtx,
		transcriptRecheckDelay: 1 * time.Millisecond,
		transcriptPublishCh:    make(chan transcriptPublishJob, 10),
	}
	pCancelled.parkTranscriptJob(transcriptPublishJob{sessionID: "s-cancel"})
	pCancelled.recheckSessionTranscript("s-cancel", "t-cancel", txPath, fi.ModTime().UTC().Add(-time.Hour), fi.Size())
	select {
	case j := <-pCancelled.transcriptPublishCh:
		t.Fatalf("unexpected job on cancelled context: %+v", j)
	case <-time.After(50 * time.Millisecond):
	}

	// 6. Nil-context recheck does not panic
	pNilCtx := &WorkerPool{
		transcriptRecheckDelay: 1 * time.Millisecond,
	}
	pNilCtx.recheckSessionTranscript("s-nil", "t-nil", txPath, fi.ModTime().UTC().Add(-time.Hour), fi.Size())
}

func TestWorkerPool_TranscriptPublish_JobHandling(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	memBus := pubsub.NewMemoryBus()
	defer memBus.Close()

	// 1. EnqueueTranscriptPublish rejection rules
	p := &WorkerPool{
		eventBus:                memBus,
		transcriptsIndexerURL:   "http://indexer.local",
		transcriptPublishCh:     make(chan transcriptPublishJob, 10),
		transcriptWorkerStarted: true,
	}
	p.EnqueueTranscriptPublish("", "t1", "turn-end")
	p.EnqueueTranscriptPublish("   ", "t1", "turn-end")

	pStopped := &WorkerPool{
		eventBus:              memBus,
		transcriptsIndexerURL: "http://indexer.local",
		transcriptPublishCh:   make(chan transcriptPublishJob, 10),
		stopped:               true,
	}
	pStopped.EnqueueTranscriptPublish("s-stopped", "t1", "turn-end")

	pNoBus := &WorkerPool{
		transcriptsIndexerURL: "http://indexer.local",
		transcriptPublishCh:   make(chan transcriptPublishJob, 10),
	}
	pNoBus.EnqueueTranscriptPublish("s-nobus", "t1", "turn-end")

	pNoURL := &WorkerPool{
		eventBus:            memBus,
		transcriptPublishCh: make(chan transcriptPublishJob, 10),
	}
	pNoURL.EnqueueTranscriptPublish("s-nourl", "t1", "turn-end")

	select {
	case j := <-p.transcriptPublishCh:
		t.Fatalf("unexpected job enqueued for blank session: %+v", j)
	case j := <-pStopped.transcriptPublishCh:
		t.Fatalf("unexpected job enqueued on stopped pool: %+v", j)
	case j := <-pNoBus.transcriptPublishCh:
		t.Fatalf("unexpected job enqueued with no bus: %+v", j)
	case j := <-pNoURL.transcriptPublishCh:
		t.Fatalf("unexpected job enqueued with no URL: %+v", j)
	default:
	}

	// 2. processTranscriptPublishJobWithContext missing transcript and recheck source
	pProcess := &WorkerPool{
		eventBus:                memBus,
		transcriptsIndexerURL:   "http://indexer.local",
		transcriptRecheckDelay:  1 * time.Millisecond,
		transcriptPublishCh:     make(chan transcriptPublishJob, 10),
		transcriptWorkerStarted: true,
	}
	pProcess.processTranscriptPublishJobWithContext(ctx, transcriptPublishJob{
		sessionID: "s-missing-tx",
		retried:   false,
	})
	select {
	case j := <-pProcess.transcriptPublishCh:
		if !j.retried || j.sessionID != "s-missing-tx" {
			t.Fatalf("unexpected parked job: %+v", j)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for parked missing-transcript job")
	}

	pProcess.processTranscriptPublishJobWithContext(ctx, transcriptPublishJob{
		sessionID: "s-missing-tx-2",
		retried:   true,
	})
	select {
	case j := <-pProcess.transcriptPublishCh:
		t.Fatalf("retried missing transcript should not be parked: %+v", j)
	case <-time.After(50 * time.Millisecond):
	}

	cancelledCtx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	pProcess.processTranscriptPublishJobWithContext(cancelledCtx2, transcriptPublishJob{sessionID: "s-cancel"})
}

func TestWorkerPool_TranscriptPublish_Allowlist(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store := dbtest.NewFakeStore()
	_ = store.SaveSessionID(ctx, "t-known", "s-known")
	pStore := &WorkerPool{
		cfg: WorkerPoolConfig{Store: store},
	}
	if !pStore.checkAllowlist(ctx, transcriptPublishJob{sessionID: "s-known"}) {
		t.Errorf("expected known session to be allowed")
	}

	pStoreCh := &WorkerPool{
		cfg:                    WorkerPoolConfig{Store: store},
		transcriptRecheckDelay: 1 * time.Millisecond,
		transcriptPublishCh:    make(chan transcriptPublishJob, 10),
	}
	if pStoreCh.checkAllowlist(ctx, transcriptPublishJob{sessionID: "s-unknown", retried: false}) {
		t.Errorf("expected unknown session to be disallowed")
	}
	select {
	case j := <-pStoreCh.transcriptPublishCh:
		if !j.retried || j.sessionID != "s-unknown" {
			t.Fatalf("unexpected parked job: %+v", j)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for parked unknown session")
	}

	if pStoreCh.checkAllowlist(ctx, transcriptPublishJob{sessionID: "s-unknown-retried", retried: true}) {
		t.Errorf("expected retried unknown session to be disallowed")
	}
	select {
	case j := <-pStoreCh.transcriptPublishCh:
		t.Fatalf("retried unknown session should not be re-parked: %+v", j)
	case <-time.After(50 * time.Millisecond):
	}

	storeErr := dbtest.NewFakeStore()
	storeErr.FailNext("GetKnownTurnSessionIDs", errors.New("db failure"))
	pStoreErr := &WorkerPool{
		cfg: WorkerPoolConfig{Store: storeErr},
	}
	if !pStoreErr.checkAllowlist(ctx, transcriptPublishJob{sessionID: "s-any"}) {
		t.Errorf("expected allowlist to fail open on store error")
	}
}

func TestWorkerPool_TranscriptPublish_ConfigResolution(t *testing.T) {
	t.Parallel()

	// 1. Config resolution: transcriptsIndexerBaseURL and stackName
	appCfg := config.NewFromData(&config.ConfigData{
		Transcripts: config.TranscriptsConfig{
			IndexerURL: "http://from-config.local",
			Stack:      "stack-config",
		},
	})
	pCfg := &WorkerPool{appCfg: appCfg}
	if got := pCfg.transcriptsIndexerBaseURL(); got != "http://from-config.local" {
		t.Errorf("transcriptsIndexerBaseURL() = %q, want http://from-config.local", got)
	}
	if got := pCfg.stackName(); got != "stack-config" {
		t.Errorf("stackName() = %q, want stack-config", got)
	}
	var pNil *WorkerPool
	if got := pNil.transcriptsIndexerBaseURL(); got != "" {
		t.Errorf("nil transcriptsIndexerBaseURL() = %q, want empty", got)
	}
	if got := pNil.stackName(); got != "" {
		t.Errorf("nil stackName() = %q, want empty", got)
	}
	if got := pNil.resolvedTranscriptStore(); got != nil {
		t.Errorf("nil resolvedTranscriptStore() = %v, want nil", got)
	}

	// 2. mcpServerNames and pool accessors
	appCfgMCP := config.NewFromData(&config.ConfigData{
		NATSURL: "nats://test-nats:4222",
		McpServers: config.TargetMcpConfig{
			Common:    map[string]json.RawMessage{"srv-b": []byte(`{}`), "srv-a": []byte(`{}`)},
			Discord:   map[string]json.RawMessage{"srv-c": []byte(`{}`)},
			Voice:     map[string]json.RawMessage{"srv-d": []byte(`{}`)},
			Ephemeral: map[string]json.RawMessage{"srv-e": []byte(`{}`)},
		},
	})
	pMCP := &WorkerPool{appCfg: appCfgMCP}
	servers := pMCP.mcpServerNames()
	expectedServers := []string{"srv-a", "srv-b", "srv-c", "srv-d", "srv-e"}
	if !slices.Equal(servers, expectedServers) {
		t.Errorf("mcpServerNames() = %v, want %v", servers, expectedServers)
	}
	if pMCP.NATSURL() != "nats://test-nats:4222" {
		t.Errorf("NATSURL() = %q, want nats://test-nats:4222", pMCP.NATSURL())
	}
	if pMCP.AppConfig() != appCfgMCP {
		t.Errorf("AppConfig() did not match")
	}
	if pNil.mcpServerNames() != nil {
		t.Errorf("nil mcpServerNames() should be nil")
	}
	if pNil.NATSURL() != "" {
		t.Errorf("nil NATSURL() should be empty")
	}
	if pNil.AppConfig() != nil {
		t.Errorf("nil AppConfig() should be nil")
	}
}

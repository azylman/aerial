package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/transcriptwire"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/transcript"
)

type transcriptFixture struct {
	pool      *WorkerPool
	brainDir  string
	store     *fakeTranscriptStore
	pubCh     chan transcriptwire.TranscriptStepBatch
	published []transcriptwire.TranscriptStepBatch
	mu        sync.Mutex
}

type fakeTranscriptStore struct {
	db.TranscriptStore

	mu      sync.Mutex
	known   map[string]bool
	checkCh chan struct{}
}

func (s *fakeTranscriptStore) GetKnownTurnSessionIDs(context.Context) (map[string]bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checkCh != nil {
		select {
		case s.checkCh <- struct{}{}:
		default:
		}
	}
	return maps.Clone(s.known), nil
}

func (s *fakeTranscriptStore) setKnown(id string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.known[id] = ok
}

func newTranscriptFixture(t *testing.T, delay time.Duration, watermark int) *transcriptFixture {
	t.Helper()
	f := &transcriptFixture{
		brainDir: t.TempDir(),
		store:    &fakeTranscriptStore{known: make(map[string]bool), checkCh: make(chan struct{}, 16)},
		pubCh:    make(chan transcriptwire.TranscriptStepBatch, 16),
	}
	pub := transcript.NewPublisher("aerial", func(_ context.Context, b transcriptwire.TranscriptStepBatch) error {
		f.mu.Lock()
		f.published = append(f.published, b)
		f.mu.Unlock()
		f.pubCh <- b
		return nil
	})
	bus := pubsub.NewMemoryBus()
	t.Cleanup(func() { _ = bus.Close() })
	appCfg := config.NewFromData(&config.ConfigData{DataDir: f.brainDir, Transcripts: config.TranscriptsConfig{Stack: "aerial"}})
	f.pool = New(appCfg, WorkerPoolConfig{TranscriptStore: f.store, TranscriptsIndexerURL: "http://test-indexer", TranscriptRecheckDelay: delay, EventBus: bus})
	f.pool.initTranscriptPublisher()
	f.pool.TranscriptPublisher.Enqueue(transcriptwire.TranscriptStepBatch{SessionID: "init"})
	_ = f.pool.TranscriptPublisher.Drain(context.Background())
	f.pool.TranscriptPublisher = pub
	if watermark >= 0 {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]int{"last_indexed_step": watermark})
		}))
		t.Cleanup(srv.Close)
		f.pool.TranscriptIndexerClient = transcript.NewIndexerClient(srv.URL, nil)
	}
	return f
}

func writeTestTranscript(t *testing.T, brainDir, sessionID string, steps ...int) {
	t.Helper()
	dir := filepath.Join(brainDir, sessionID)
	_ = os.MkdirAll(dir, 0o755)
	f, err := os.OpenFile(filepath.Join(dir, "transcript.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open transcript: %v", err)
	}
	defer f.Close()
	for _, s := range steps {
		_, _ = fmt.Fprintf(f, `{"step_index":%d,"type":"PLANNER_RESPONSE","content":"step %d"}`+"\n", s, s)
	}
}

type publishTestCase struct {
	name                      string
	initialSteps, appendSteps []int
	watermark                 int
	allowInit, allowDelayed   bool
	isRecheck, stopToDrain    bool
	wantFirst, wantLast       int
	wantEmpty                 bool
}

func (f *transcriptFixture) executePublishCase(t *testing.T, sessionID string, tc publishTestCase) {
	t.Helper()
	switch {
	case tc.stopToDrain:
		f.pool.Stop()
		if f.pool.EnqueueTranscriptPublish(TranscriptPublishJob{SessionID: sessionID}) {
			t.Fatal("expected post-stop enqueue to return false")
		}
	case tc.allowDelayed:
		f.pool.Start()
		defer f.pool.Stop()
		select {
		case <-f.store.checkCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for initial check")
		}
		f.store.setKnown(sessionID, true)
		select {
		case <-f.pubCh:
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting for parked publish")
		}
	default:
		if err := f.pool.drainTranscriptPublisher(context.Background()); err != nil {
			t.Fatalf("drain error: %v", err)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if tc.wantEmpty {
		if len(f.published) != 0 {
			t.Fatalf("expected 0 batches, got %d", len(f.published))
		}
		return
	}
	if len(f.published) == 0 {
		t.Fatalf("expected batches, got none")
	}
	got := f.published[len(f.published)-1]
	if len(got.Steps) != tc.wantLast-tc.wantFirst+1 || got.FirstStep != tc.wantFirst || got.LastStep != tc.wantLast {
		t.Errorf("got [%d, %d] len %d; want [%d, %d]", got.FirstStep, got.LastStep, len(got.Steps), tc.wantFirst, tc.wantLast)
	}
}

func TestWorkerPool_TranscriptPublish(t *testing.T) {
	t.Parallel()
	tests := []publishTestCase{
		{name: "publishes_steps_above_watermark", initialSteps: []int{0, 1, 2}, watermark: 1, allowInit: true, wantFirst: 2, wantLast: 2},
		{name: "parks_unknown_session_until_allowlisted", initialSteps: []int{0, 1}, watermark: -1, allowDelayed: true, wantFirst: 0, wantLast: 1},
		{name: "rechecks_modified_file", initialSteps: []int{0}, appendSteps: []int{1}, watermark: 0, allowInit: true, isRecheck: true, wantFirst: 1, wantLast: 1},
		{name: "recheck_unmodified_skips_publish", initialSteps: []int{0}, watermark: 0, allowInit: true, isRecheck: true, wantEmpty: true},
		{name: "missing_file_skips_publish", watermark: -1, allowInit: true, wantEmpty: true},
		{name: "   ", watermark: -1, wantEmpty: true},
		{name: "stop_drains_queued_job", initialSteps: []int{0, 1}, watermark: -1, allowInit: true, stopToDrain: true, wantFirst: 0, wantLast: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newTranscriptFixture(t, 20*time.Millisecond, tc.watermark)
			sessionID := tc.name
			if len(tc.initialSteps) > 0 {
				writeTestTranscript(t, f.brainDir, sessionID, tc.initialSteps...)
			}
			if tc.allowInit {
				f.store.setKnown(sessionID, true)
			}
			job := TranscriptPublishJob{SessionID: sessionID, BrainDir: f.brainDir}
			if tc.isRecheck {
				fi, _ := os.Stat(filepath.Join(f.brainDir, sessionID, "transcript.jsonl"))
				job.LastMtime, job.LastSize, job.IsRecheck = fi.ModTime().UTC(), fi.Size(), true
				if len(tc.appendSteps) > 0 {
					writeTestTranscript(t, f.brainDir, sessionID, tc.appendSteps...)
				}
			}
			f.pool.EnqueueTranscriptPublish(job)
			f.executePublishCase(t, sessionID, tc)
		})
	}
}

func TestWorkerPool_MCPServerNames(t *testing.T) {
	t.Parallel()
	raw := json.RawMessage(`{}`)
	for _, tc := range []struct {
		name string
		cfg  *config.ConfigData
		want []string
	}{
		{"nil", nil, nil},
		{"sorted", &config.ConfigData{McpServers: config.TargetMcpConfig{
			Common: map[string]json.RawMessage{"zeta": raw, "alpha": raw}, Discord: map[string]json.RawMessage{"beta": raw},
			Voice: map[string]json.RawMessage{"alpha": raw, "gamma": raw}, Ephemeral: map[string]json.RawMessage{"delta": raw},
		}}, []string{"alpha", "beta", "delta", "gamma", "zeta"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			appCfg := (*config.Config)(nil)
			if tc.cfg != nil {
				appCfg = config.NewFromData(tc.cfg)
			}
			if got := New(appCfg, WorkerPoolConfig{}).mcpServerNames(); !slices.Equal(got, tc.want) {
				t.Errorf("mcpServerNames() = %v, want %v", got, tc.want)
			}
		})
	}
}

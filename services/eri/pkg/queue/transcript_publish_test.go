package queue

import (
	"context"
	"encoding/json"
	"fmt"
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
}

type fakeTranscriptStore struct {
	db.TranscriptStore

	known sync.Map
	ch    chan struct{}
}

func (s *fakeTranscriptStore) GetKnownTurnSessionIDs(context.Context) (map[string]bool, error) {
	select {
	case s.ch <- struct{}{}:
	default:
	}
	out := make(map[string]bool)
	s.known.Range(func(k, v any) bool {
		if vb, ok := v.(bool); ok {
			out[fmt.Sprint(k)] = vb
		}
		return true
	})
	return out, nil
}

func (s *fakeTranscriptStore) setKnown(id string, ok bool) { s.known.Store(id, ok) }

type fakeBus struct {
	pubsub.Bus

	fn func(ctx context.Context, s, id string, d any) error
}

func (b *fakeBus) Publish(ctx context.Context, s, id string, d any) error { return b.fn(ctx, s, id, d) }

func newTranscriptFixture(t *testing.T, delay time.Duration, watermark int) *transcriptFixture {
	t.Helper()
	f := &transcriptFixture{brainDir: t.TempDir(), store: &fakeTranscriptStore{ch: make(chan struct{}, 16)}, pubCh: make(chan transcriptwire.TranscriptStepBatch, 16)}
	f.pool = New(nil, WorkerPoolConfig{AgentName: "aerial", TranscriptStore: f.store, TranscriptsIndexerURL: "http://test-indexer", TranscriptRecheckDelay: delay})
	f.pool.SetEventBus(&fakeBus{fn: func(_ context.Context, _, _ string, data any) error {
		if b, ok := data.(transcriptwire.TranscriptStepBatch); ok {
			f.published = append(f.published, b)
			f.pubCh <- b
		}
		return nil
	}})
	if watermark >= 0 {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintf(w, `{"last_indexed_step":%d}`, watermark) }))
		t.Cleanup(srv.Close)
		f.pool.TranscriptIndexerClient = transcript.NewIndexerClient(srv.URL, nil)
	}
	return f
}

func writeTestTranscript(brainDir, sessionID string, steps ...int) {
	p := filepath.Join(brainDir, sessionID, "transcript.jsonl")
	_ = os.MkdirAll(filepath.Dir(p), 0o755)
	var b []byte
	for _, s := range steps {
		b = fmt.Appendf(b, `{"step_index":%d,"type":"PLANNER_RESPONSE","content":"x"}`+"\n", s)
	}
	_ = os.WriteFile(p, b, 0o644)
}

func TestWorkerPool_TranscriptPublish(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		init        []int
		wm          int
		knownInit   bool
		first, last int
	}{
		{"publishes_steps_above_watermark", []int{0, 1, 2}, 1, true, 2, 2},
		{"parks_unknown_session_until_allowlisted", []int{0, 1}, -1, false, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, sID := newTranscriptFixture(t, 20*time.Millisecond, tc.wm), "sess-"+tc.name
			writeTestTranscript(f.brainDir, sID, tc.init...)
			f.store.setKnown(sID, tc.knownInit)
			f.pool.Start()
			defer f.pool.Stop()
			f.pool.EnqueueTranscriptPublish(TranscriptPublishJob{SessionID: sID, BrainDir: f.brainDir})
			if !tc.knownInit {
				<-f.store.ch
				f.pool.transcriptPublishSem <- struct{}{}
				f.pool.EnqueueTranscriptPublish(TranscriptPublishJob{SessionID: sID, BrainDir: f.brainDir})
				n := len(f.pool.transcriptPublishCh)
				<-f.pool.transcriptPublishSem
				if n != 0 {
					t.Fatalf("expected duplicate enqueue while parked to coalesce, got %d queued jobs", n)
				}
				f.store.setKnown(sID, true)
			}
			if b := <-f.pubCh; b.FirstStep != tc.first || b.LastStep != tc.last {
				t.Fatalf("step mismatch: first=%d last=%d", b.FirstStep, b.LastStep)
			}
			f.pool.Stop()
			if len(f.published) != 1 {
				t.Fatalf("expected exactly 1 published batch, got %d", len(f.published))
			}
		})
	}
}

func TestWorkerPool_TranscriptPublish_StopDrain(t *testing.T) {
	t.Parallel()
	f, sID := newTranscriptFixture(t, 20*time.Millisecond, -1), "sess-stop"
	for _, id := range []string{sID, sID + "-q"} {
		writeTestTranscript(f.brainDir, id, 0, 1)
		f.store.setKnown(id, true)
	}
	inFlight, canceled := make(chan struct{}, 1), false
	f.pool.SetEventBus(&fakeBus{fn: func(ctx context.Context, _, _ string, data any) error {
		if b, ok := data.(transcriptwire.TranscriptStepBatch); ok {
			if b.SessionID == sID && !canceled {
				inFlight <- struct{}{}
				<-ctx.Done()
				canceled = true
				return ctx.Err()
			}
			f.published = append(f.published, b)
		}
		return nil
	}})
	f.pool.Start()
	f.pool.EnqueueTranscriptPublish(TranscriptPublishJob{SessionID: sID, BrainDir: f.brainDir})
	<-inFlight
	f.pool.EnqueueTranscriptPublish(TranscriptPublishJob{SessionID: sID + "-q", BrainDir: f.brainDir})
	f.pool.Stop()
	if !canceled || len(f.published) != 2 || f.published[0].SessionID != sID || f.published[1].SessionID != sID+"-q" {
		t.Fatalf("expected in-flight cancel and 2 drained sessions, got canceled=%v published=%v", canceled, f.published)
	}
}

func TestWorkerPool_TranscriptPublishAuxiliary(t *testing.T) {
	t.Parallel()
	f := newTranscriptFixture(t, 5*time.Millisecond, -1)
	defer f.pool.Stop()
	f.pool.appCfg = config.NewFromData(&config.ConfigData{Transcripts: config.TranscriptsConfig{Stack: "miorine"}, McpServers: config.TargetMcpConfig{Common: map[string]json.RawMessage{"a": []byte(`{}`)}}})
	if f.pool.stackName() != "miorine" || !slices.Equal(f.pool.mcpServerNames(), []string{"a"}) || f.pool.EnqueueTranscriptPublish(TranscriptPublishJob{}) {
		t.Errorf("config or empty enqueue mismatch")
	}
	f.pool.transcriptPublishCh = make(chan TranscriptPublishJob, 1)
	f.pool.transcriptPublishCh <- TranscriptPublishJob{SessionID: "s-full"}
	if f.pool.EnqueueTranscriptPublish(TranscriptPublishJob{SessionID: "s-full-init"}) || f.pool.EnqueueTranscriptPublish(TranscriptPublishJob{SessionID: "s-retry", IsRecheck: true}) {
		t.Errorf("expected enqueue to fail on full channel")
	}
	<-f.pool.transcriptPublishCh
	if !f.pool.EnqueueTranscriptPublish(TranscriptPublishJob{SessionID: "s-dedup"}) || !f.pool.EnqueueTranscriptPublish(TranscriptPublishJob{SessionID: "s-dedup", ExtraDirs: []string{"x"}}) || len(f.pool.transcriptPublishCh) != 1 {
		t.Errorf("expected deduplication to coalesce into 1 channel entry")
	}
	<-f.pool.transcriptPublishCh
	if j := <-f.pool.transcriptPublishCh; j.SessionID != "s-retry" || len(f.pool.transcriptPublishCh) > 0 {
		t.Fatalf("expected only s-retry, got %s", j.SessionID)
	}
	f.store.setKnown("s-park", false)
	writeTestTranscript(f.brainDir, "s-park", 0)
	_ = f.pool.processTranscriptPublishJobWithContext(context.Background(), TranscriptPublishJob{SessionID: "s-park", BrainDir: f.brainDir})
	if !f.pool.EnqueueTranscriptPublish(TranscriptPublishJob{SessionID: "s-park", BrainDir: f.brainDir}) || len(f.pool.transcriptPublishCh) != 0 {
		t.Fatalf("expected parked session to coalesce duplicate enqueue into 0 channel entries, got %d", len(f.pool.transcriptPublishCh))
	}
	f.pool.transcriptPublishSem <- struct{}{}
	toCtx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	if f.pool.drainTranscriptPublisher(toCtx) == nil {
		t.Fatalf("expected timeout error when sem is held")
	}
	cancel()
	<-f.pool.transcriptPublishSem
	f.store.setKnown("s-rc", true)
	writeTestTranscript(f.brainDir, "s-rc", 0)
	fi, _ := os.Stat(filepath.Join(f.brainDir, "s-rc", "transcript.jsonl"))
	if f.pool.processTranscriptPublishJobWithContext(context.Background(), TranscriptPublishJob{SessionID: "s-rc", BrainDir: f.brainDir, LastMtime: fi.ModTime().UTC(), LastSize: fi.Size(), IsRecheck: true}) != nil || len(f.published) != 0 {
		t.Fatalf("unchanged recheck failed")
	}
	for _, b := range []pubsub.Bus{nil, &fakeBus{fn: func(context.Context, string, string, any) error { return fmt.Errorf("fail") }}} {
		f.pool.SetEventBus(b)
		if f.pool.processTranscriptPublishJobWithContext(context.Background(), TranscriptPublishJob{SessionID: "s-rc", BrainDir: f.brainDir}) == nil {
			t.Fatalf("expected error")
		}
	}
}

package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"

	"github.com/azylman/aerial/pkg/transcriptwire"
	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/azylman/aerial/services/eri/pkg/db"
	"github.com/azylman/aerial/services/eri/pkg/db/dbtest"
	"github.com/azylman/aerial/services/eri/pkg/session"
)

type resendFileSpec struct {
	dirKind    string // "brain", "extra", "plain"
	sessionID  string
	steps      []int
	corrupt    bool
	unreadable bool
}

type runStartupResendTestCase struct {
	name              string
	files             []resendFileSpec
	knownSessions     []string
	watermarks        map[string]int
	indexerStatus     int
	useAppCfg         bool
	useStore          bool
	storeErr          bool
	cancelCtx         bool
	noIndexer         bool
	concurrentPublish bool
	wantErr           bool
	wantThreadID      string
	wantSteps         map[string][]int
}

type resendTranscriptStore struct {
	db.TranscriptStore

	known sync.Map
	fail  bool
}

func (s *resendTranscriptStore) GetKnownTurnSessionIDs(context.Context) (map[string]bool, error) {
	if s.fail {
		return nil, errors.New("failed to get known sessions")
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

func (s *resendTranscriptStore) setKnown(id string, ok bool) { s.known.Store(id, ok) }

func (*resendTranscriptStore) ResolveThreadForSession(context.Context, string) (string, error) {
	return "th-resend", nil
}

func writeResendTestFile(targetBrain, targetExtra string, f resendFileSpec) {
	var p string
	switch f.dirKind {
	case "brain":
		d := filepath.Join(targetBrain, f.sessionID)
		_ = os.MkdirAll(d, 0o755)
		p = filepath.Join(d, "transcript.jsonl")
	case "extra":
		d := filepath.Join(targetExtra, f.sessionID)
		_ = os.MkdirAll(d, 0o755)
		p = filepath.Join(d, "transcript.jsonl")
	case "plain":
		p = filepath.Join(targetBrain, f.sessionID+".jsonl")
	}

	if f.corrupt {
		_ = os.WriteFile(p, []byte("{corrupt invalid json\n"), 0o644)
		return
	}
	if f.unreadable {
		_ = os.WriteFile(p, []byte("content\n"), 0o000)
		return
	}
	var b []byte
	for _, s := range f.steps {
		b = fmt.Appendf(b, `{"step_index":%d,"type":"PLANNER_RESPONSE","content":"content-%d"}`+"\n", s, s)
	}
	_ = os.WriteFile(p, b, 0o644)
}

func setupResendDirectories(t *testing.T) (*session.Manager, string, string) {
	t.Helper()
	brainDir := t.TempDir()
	extraDir := t.TempDir()
	sessMgr := session.New(extraDir, brainDir)
	roots := sessMgr.Roots()
	if len(roots) < 2 {
		t.Fatalf("expected at least 2 roots from session.New, got %d", len(roots))
	}
	targetBrain, targetExtra := roots[0], roots[1]
	_ = os.MkdirAll(targetBrain, 0o755)
	_ = os.MkdirAll(targetExtra, 0o755)
	return sessMgr, targetBrain, targetExtra
}

func startResendIndexerServer(t *testing.T, tc runStartupResendTestCase) string {
	t.Helper()
	if tc.noIndexer {
		return ""
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tc.indexerStatus != 0 && tc.indexerStatus != http.StatusOK {
			http.Error(w, "indexer error", tc.indexerStatus)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"sessions": tc.watermarks, "last_indexed_step": tc.watermarks[r.URL.Query().Get("session_id")]})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func runSingleStartupResendTest(t *testing.T, tc runStartupResendTestCase) {
	t.Helper()
	var sessMgr *session.Manager
	var targetBrain, targetExtra string
	var appCfg *config.Config
	if !tc.useAppCfg {
		sessMgr, targetBrain, targetExtra = setupResendDirectories(t)
	} else {
		targetBrain = t.TempDir()
		_ = os.MkdirAll(targetBrain, 0o755)
		sessMgr = session.New("", "")
		appCfg = newTestConfig(func(d *config.ConfigData) {
			d.GeminiHomeDir = targetBrain
		})
	}

	for _, f := range tc.files {
		writeResendTestFile(targetBrain, targetExtra, f)
	}

	store := &resendTranscriptStore{fail: tc.storeErr}
	for _, sid := range tc.knownSessions {
		store.setKnown(sid, true)
	}

	indexerURL := startResendIndexerServer(t, tc)

	var publishedMu sync.Mutex
	published := make(map[string][]int)
	threadIDs := make(map[string]string)

	poolCfg := WorkerPoolConfig{
		AgentName:             "aerial",
		TranscriptStore:       store,
		TranscriptsIndexerURL: indexerURL,
		SessionManager:        sessMgr,
	}
	if tc.useStore {
		fakeStore := dbtest.NewFakeStore()
		fakeStore.FailNext("ResolveThreadForSession", errors.New("store err"))
		poolCfg.Store = fakeStore
	}

	pool := New(appCfg, poolCfg)
	defer pool.Stop()
	pool.SetEventBus(&fakeBus{fn: func(_ context.Context, _, _ string, data any) error {
		if b, ok := data.(transcriptwire.TranscriptStepBatch); ok {
			publishedMu.Lock()
			if b.ThreadID != "" {
				threadIDs[b.SessionID] = b.ThreadID
			}
			for _, s := range b.Steps {
				published[b.SessionID] = append(published[b.SessionID], s.StepIndex)
			}
			publishedMu.Unlock()
		}
		return nil
	}})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if tc.cancelCtx {
		cancel()
	}

	var wg sync.WaitGroup
	if tc.concurrentPublish && len(tc.knownSessions) > 0 {
		wg.Go(func() {
			_ = pool.publishJobLocked(ctx, TranscriptPublishJob{SessionID: tc.knownSessions[0], BrainDir: targetBrain}, "concurrent")
		})
	}
	err := pool.RunStartupResend(ctx)
	wg.Wait()
	if (err != nil) != tc.wantErr {
		t.Fatalf("RunStartupResend() err = %v, wantErr = %v", err, tc.wantErr)
	}
	if tc.wantErr && len(tc.wantSteps) == 0 {
		return
	}

	publishedMu.Lock()
	defer publishedMu.Unlock()
	for sid, wantSteps := range tc.wantSteps {
		gotSteps := slices.Compact(append([]int(nil), func() []int { s := published[sid]; slices.Sort(s); return s }()...))
		slices.Sort(wantSteps)
		if !reflect.DeepEqual(gotSteps, wantSteps) {
			t.Errorf("session %s published steps = %v, want %v", sid, gotSteps, wantSteps)
		}
		if tc.wantThreadID != "" && threadIDs[sid] != tc.wantThreadID {
			t.Errorf("session %s threadID = %q, want %q", sid, threadIDs[sid], tc.wantThreadID)
		}
	}
	if len(published) != len(tc.wantSteps) {
		t.Errorf("published sessions count = %d, want %d (published: %+v)", len(published), len(tc.wantSteps), published)
	}
}

func TestWorkerPool_RunStartupResend(t *testing.T) {
	t.Parallel()

	tests := []runStartupResendTestCase{
		{
			name: "DeltaResendAboveWatermark",
			files: []resendFileSpec{
				{dirKind: "brain", sessionID: "sess-brain-dir", steps: []int{0, 1, 2, 3}},
				{dirKind: "extra", sessionID: "sess-extra-dir", steps: []int{0, 1, 2}},
				{dirKind: "plain", sessionID: "sess-plain", steps: []int{0, 1}},
			},
			knownSessions: []string{"sess-brain-dir", "sess-extra-dir", "sess-plain"},
			watermarks: map[string]int{
				"sess-brain-dir": 1,
				"sess-extra-dir": 0,
			},
			useStore:          true,
			concurrentPublish: true,
			indexerStatus:     http.StatusOK,
			wantThreadID:      "th-resend",
			wantSteps: map[string][]int{
				"sess-brain-dir": {2, 3},
				"sess-extra-dir": {1, 2},
				"sess-plain":     {0, 1},
			},
		},
		{
			name: "CorruptSkippedValidPublishes",
			files: []resendFileSpec{
				{dirKind: "brain", sessionID: "sess-corrupt", corrupt: true},
				{dirKind: "brain", sessionID: "sess-valid", steps: []int{0, 1}},
			},
			knownSessions: []string{"sess-corrupt", "sess-valid"},
			indexerStatus: http.StatusOK,
			wantSteps: map[string][]int{
				"sess-valid": {0, 1},
			},
		},
		{
			name: "IndexerWatermarksHTTPError",
			files: []resendFileSpec{
				{dirKind: "brain", sessionID: "sess-err", steps: []int{0}},
			},
			knownSessions: []string{"sess-err"},
			indexerStatus: http.StatusInternalServerError,
			wantErr:       true,
		},
		{
			name:          "AppConfigHomeDirFallback",
			files:         []resendFileSpec{{dirKind: "brain", sessionID: "sess-appcfg", steps: []int{0}}},
			knownSessions: []string{"sess-appcfg"},
			useAppCfg:     true,
			wantSteps: map[string][]int{
				"sess-appcfg": {0},
			},
		},
		{
			name:     "StoreGetKnownSessionsError",
			storeErr: true,
			wantErr:  true,
		},
		{
			name: "TruncatedFileWithResetBatchPublishesBelowWatermark",
			files: []resendFileSpec{
				{
					dirKind:   "brain",
					sessionID: "sess-truncated",
					steps: func() []int {
						s := make([]int, 150)
						for i := range s {
							s[i] = i
						}
						return s
					}(),
				},
			},
			knownSessions: []string{"sess-truncated"},
			watermarks: map[string]int{
				"sess-truncated": 200,
			},
			indexerStatus: http.StatusOK,
			wantSteps: map[string][]int{
				"sess-truncated": func() []int {
					s := make([]int, 150)
					for i := range s {
						s[i] = i
					}
					return s
				}(),
			},
		},
		{
			name: "UnreadableTranscriptScanErrorFailsResend",
			files: []resendFileSpec{
				{dirKind: "brain", sessionID: "sess-unreadable", unreadable: true},
				{dirKind: "brain", sessionID: "sess-valid", steps: []int{0, 1}},
			},
			knownSessions: []string{"sess-unreadable", "sess-valid"},
			indexerStatus: http.StatusOK,
			wantErr:       true,
			wantSteps: map[string][]int{
				"sess-valid": {0, 1},
			},
		},
		{
			name:          "CancelledContextAbortsLoop",
			files:         []resendFileSpec{{dirKind: "brain", sessionID: "sess-cancel", steps: []int{0}}},
			knownSessions: []string{"sess-cancel"},
			noIndexer:     true,
			cancelCtx:     true,
			wantErr:       true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runSingleStartupResendTest(t, tc)
		})
	}
}

func TestCollectStartupSessionIDs_TableDriven(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	brainDir := filepath.Join(tmpDir, "brain")
	extraDir1 := filepath.Join(tmpDir, "extra1")
	extraDir2 := filepath.Join(tmpDir, "extra2")
	_ = os.MkdirAll(filepath.Join(brainDir, "sess-a"), 0o755)
	_ = os.MkdirAll(filepath.Join(brainDir, "sess-b"), 0o755)
	_ = os.MkdirAll(filepath.Join(brainDir, ".hidden"), 0o755)
	_ = os.MkdirAll(filepath.Join(extraDir1, "sess-c"), 0o755)
	_ = os.MkdirAll(filepath.Join(extraDir2, "sess-d"), 0o755)

	tests := []struct {
		name      string
		brainDir  string
		extraDirs []string
		known     map[string]bool
		wantIDs   []string
	}{
		{
			name:      "NilKnownDiscoversAllDirectories",
			brainDir:  brainDir,
			extraDirs: []string{extraDir1, extraDir2},
			known:     nil,
			wantIDs:   []string{"sess-a", "sess-b", "sess-c", "sess-d"},
		},
		{
			name:      "KnownFiltersDirectoriesAndUnionsMissingDirs",
			brainDir:  brainDir,
			extraDirs: []string{extraDir1},
			known: map[string]bool{
				"sess-a":     true,
				"sess-plain": true,
				"sess-b":     false,
			},
			wantIDs: []string{"sess-a", "sess-plain"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := collectStartupSessionIDs(tc.brainDir, tc.extraDirs, tc.known)
			if !reflect.DeepEqual(got, tc.wantIDs) {
				t.Errorf("collectStartupSessionIDs() = %v, want %v", got, tc.wantIDs)
			}
		})
	}
}

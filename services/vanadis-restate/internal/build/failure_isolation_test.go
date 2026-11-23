package build

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

func isolationChunk(id, status string, deps ...string) model.VanadisChunk {
	return model.VanadisChunk{ID: id, BuildID: "b1", Status: status, Mode: "pr", Repo: "aerial", DependsOn: deps}
}

// isolationWakes records the first line of every wake prompt.
type isolationWakes struct {
	mu      sync.Mutex
	headers []string
	err     error
}

func (w *isolationWakes) client() *mockEriClient {
	return &mockEriClient{promptFn: func(_ context.Context, _, prompt string) error {
		w.mu.Lock()
		defer w.mu.Unlock()
		if w.err != nil {
			return w.err
		}
		header, rest, ok := strings.Cut(prompt, "\n")
		_ = rest
		_ = ok
		w.headers = append(w.headers, header)
		return nil
	}}
}

func TestBuildQuiesced(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		chunks []model.VanadisChunk
		want   bool
	}{
		{"lone failed", []model.VanadisChunk{isolationChunk("c1", "failed")}, true},
		{"failed plus running sibling", []model.VanadisChunk{isolationChunk("c1", "failed"), isolationChunk("c2", "running")}, false},
		{"failed plus independent pending ready", []model.VanadisChunk{isolationChunk("c1", "failed"), isolationChunk("c2", "pending")}, false},
		{"failed plus only a dependent pending", []model.VanadisChunk{isolationChunk("c1", "failed"), isolationChunk("c2", "pending", "c1")}, true},
		{"failed plus transitive queued dependent", []model.VanadisChunk{isolationChunk("c1", "failed"), isolationChunk("c2", "pending", "c1"), isolationChunk("c3", "queued", "c2")}, true},
		{"deploy_failed is in flight", []model.VanadisChunk{isolationChunk("c1", "failed"), isolationChunk("c2", "deploy_failed")}, false},
		{"cancelled and completed", []model.VanadisChunk{isolationChunk("c1", "cancelled"), isolationChunk("c2", "completed")}, true},
		{"all completed", []model.VanadisChunk{isolationChunk("c1", "completed"), isolationChunk("c2", "completed")}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, buildQuiesced(tc.chunks))
		})
	}
}

func TestFailureNotifyKey(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 11, 21, 10, 0, 0, 123456000, time.UTC)
	tests := []struct {
		name  string
		chunk model.VanadisChunk
		want  string
	}{
		{"utc nanos", model.VanadisChunk{ID: "c1", CompletedAt: at}, "c1@2026-11-21T10:00:00.123456Z"},
		{"offset normalized to utc", model.VanadisChunk{ID: "c1", CompletedAt: at.In(time.FixedZone("x", -8*3600))}, "c1@2026-11-21T10:00:00.123456Z"},
		{"retried failure gets a new key", model.VanadisChunk{ID: "c1", CompletedAt: at.Add(time.Minute)}, "c1@2026-11-21T10:01:00.123456Z"},
		{"zero completion", model.VanadisChunk{ID: "c2"}, "c2@0001-01-01T00:00:00Z"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, failureNotifyKey(tc.chunk))
		})
	}
}

func TestSeedFailureNotified(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		prior *model.BuildObjectState
		want  map[string]bool
	}{
		{"no saved state", nil, map[string]bool{}},
		{"legacy state seeds failed chunks", &model.BuildObjectState{Chunks: []model.VanadisChunk{isolationChunk("c1", "failed"), isolationChunk("c2", "running")}}, map[string]bool{"c1@0001-01-01T00:00:00Z": true}},
		{"saved ledger is copied", &model.BuildObjectState{FailureNotified: map[string]bool{"c9@x": true}, Chunks: []model.VanadisChunk{isolationChunk("c1", "failed")}}, map[string]bool{"c9@x": true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, seedFailureNotified(tc.prior))
		})
	}
}

func TestEvaluateBuildTerminalStatus(t *testing.T) {
	t.Parallel()
	k1, k2 := "c1@0001-01-01T00:00:00Z", "c2@0001-01-01T00:00:00Z"
	failed1 := isolationChunk("c1", "failed")
	capFailed1 := isolationChunk("c1", "failed")
	capFailed1.Banner = "Chunk 1 failed: ci_remediation_cap_reached"
	capFailed2 := isolationChunk("c2", "failed")
	capFailed2.Banner = "Chunk 2 failed: ci_remediation_cap_reached"
	tests := []struct {
		name         string
		buildStatus  string
		quiet        bool
		chunks       []model.VanadisChunk
		notified     map[string]bool
		promptErr    error
		failStatus   bool
		wantStatus   string
		wantStored   string
		wantErr      string
		wantHeaders  []string
		wantNotified map[string]bool
	}{
		{name: "running to failed notifies once", buildStatus: "running", chunks: []model.VanadisChunk{failed1}, notified: map[string]bool{}, wantStatus: "failed", wantHeaders: []string{"[VANADIS BUILD FAILED]"}, wantNotified: map[string]bool{k1: true}, wantStored: "failed"},
		{name: "cap-reached quiesced build suppresses build wake prompt", buildStatus: "running", chunks: []model.VanadisChunk{capFailed1}, notified: map[string]bool{}, wantStatus: "failed", wantNotified: map[string]bool{k1: true}, wantStored: "failed"},
		{name: "failed stays failed with no wake", buildStatus: "failed", chunks: []model.VanadisChunk{failed1}, notified: map[string]bool{k1: true}, wantStatus: "failed", wantNotified: map[string]bool{k1: true}, wantStored: "failed"},
		{name: "failed to running when launchable", buildStatus: "failed", chunks: []model.VanadisChunk{failed1, isolationChunk("c2", "pending")}, notified: map[string]bool{k1: true}, wantStatus: "running", wantNotified: map[string]bool{k1: true}, wantStored: "running"},
		{name: "failure with running sibling wakes the chunk", buildStatus: "running", chunks: []model.VanadisChunk{failed1, isolationChunk("c2", "running")}, notified: map[string]bool{}, wantStatus: "running", wantHeaders: []string{"[VANADIS CHUNK FAILED]"}, wantNotified: map[string]bool{k1: true}, wantStored: "running"},
		{name: "cap-reached failure with running sibling suppresses chunk wake prompt", buildStatus: "running", chunks: []model.VanadisChunk{capFailed1, isolationChunk("c2", "running")}, notified: map[string]bool{}, wantStatus: "running", wantNotified: map[string]bool{k1: true}, wantStored: "running"},
		{name: "new failure on failed build wakes the chunk", buildStatus: "failed", chunks: []model.VanadisChunk{failed1, isolationChunk("c2", "failed")}, notified: map[string]bool{k1: true}, wantStatus: "failed", wantHeaders: []string{"[VANADIS CHUNK FAILED]"}, wantNotified: map[string]bool{k1: true, k2: true}, wantStored: "failed"},
		{name: "new cap-reached failure on failed build suppresses chunk wake prompt", buildStatus: "failed", chunks: []model.VanadisChunk{failed1, capFailed2}, notified: map[string]bool{k1: true}, wantStatus: "failed", wantNotified: map[string]bool{k1: true, k2: true}, wantStored: "failed"},
		{name: "mixed failures: cap-reached suppressed but non-cap failure wakes build", buildStatus: "running", chunks: []model.VanadisChunk{capFailed1, isolationChunk("c2", "failed")}, notified: map[string]bool{}, wantStatus: "failed", wantHeaders: []string{"[VANADIS BUILD FAILED]"}, wantNotified: map[string]bool{k1: true, k2: true}, wantStored: "failed"},
		{name: "quiet build records without waking", buildStatus: "running", quiet: true, chunks: []model.VanadisChunk{failed1, isolationChunk("c2", "running")}, notified: map[string]bool{}, wantStatus: "running", wantNotified: map[string]bool{k1: true}, wantStored: "running"},
		{name: "stale ledger keys are pruned", buildStatus: "running", chunks: []model.VanadisChunk{isolationChunk("c1", "running")}, notified: map[string]bool{"c1@old": true}, wantStatus: "running", wantNotified: map[string]bool{}, wantStored: "running"},
		{name: "all completed", buildStatus: "running", chunks: []model.VanadisChunk{isolationChunk("c1", "completed")}, notified: map[string]bool{}, wantStatus: "completed", wantHeaders: []string{"[VANADIS BUILD COMPLETED]"}, wantNotified: map[string]bool{}, wantStored: "completed"},
		{name: "chunk wake failure retries the step", buildStatus: "running", chunks: []model.VanadisChunk{failed1, isolationChunk("c2", "running")}, notified: map[string]bool{}, promptErr: errors.New("prompt down"), wantErr: "prompt down", wantNotified: map[string]bool{}, wantStored: "running"},
		{name: "chunk wake failure on failed build retries the step", buildStatus: "failed", chunks: []model.VanadisChunk{failed1, isolationChunk("c2", "failed")}, notified: map[string]bool{k1: true}, promptErr: errors.New("prompt down"), wantErr: "prompt down", wantNotified: map[string]bool{k1: true}, wantStored: "failed"},
		{name: "build wake failure retries the step", buildStatus: "running", chunks: []model.VanadisChunk{failed1}, notified: map[string]bool{}, promptErr: errors.New("prompt down"), wantErr: "prompt down", wantNotified: map[string]bool{}, wantStored: "running"},
		{name: "reopen status write failure", buildStatus: "failed", chunks: []model.VanadisChunk{failed1, isolationChunk("c2", "pending")}, notified: map[string]bool{k1: true}, failStatus: true, wantErr: "sabotaged UPDATE on vanadis_builds", wantNotified: map[string]bool{k1: true}, wantStored: "failed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wakes := &isolationWakes{err: tc.promptErr}
			bld := model.VanadisBuild{ID: "b1", Status: tc.buildStatus, ThreadID: "th-1", Quiet: tc.quiet}
			st, pool := seededStore(t, []model.VanadisBuild{bld}, nil)
			if tc.failStatus {
				mustExec(t, pool, failStatement("vanadis_builds", "UPDATE"))
			}
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", wakes.client(), pubsub.NewMemoryBus(), "token", "example-owner", nil))
			got, err := bo.evaluateBuildTerminalStatus(context.Background(), &bld, tc.chunks, tc.notified, true)
			if tc.wantErr != "" {
				assert.ErrorContains(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantStatus, got)
			assert.Equal(t, tc.wantHeaders, wakes.headers)
			assert.Equal(t, tc.wantNotified, tc.notified)
			stored, chunks := loadBuild(t, st, "b1")
			assert.Empty(t, chunks)
			assert.Equal(t, tc.wantStored, stored.Status)
		})
	}
}

// isolationSeed assigns sequential chunk indexes so the chunks persist under
// the per-build unique index.
func isolationSeed(chunks []model.VanadisChunk) []model.VanadisChunk {
	out := make([]model.VanadisChunk, len(chunks))
	for i, c := range chunks {
		c.ChunkIndex = i
		out[i] = c
	}
	return out
}

func TestExecuteAuthoritativeTransition_FailureIsolation(t *testing.T) {
	t.Parallel()
	k1 := "c1@0001-01-01T00:00:00Z"
	failed1 := isolationChunk("c1", "failed")
	tests := []struct {
		name          string
		buildStatus   string
		chunks        []model.VanadisChunk
		prior         *model.BuildObjectState
		wantStatus    string
		wantLaunched  []string
		wantHeaders   []string
		wantIsolation bool
		wantNotified  map[string]bool
		wantStored    map[string]string
		wantBuild     string
	}{
		{name: "failed chunk with independent pending launches it", buildStatus: "running", chunks: []model.VanadisChunk{failed1, isolationChunk("c2", "pending")}, wantStatus: "running", wantLaunched: []string{"c2"}, wantHeaders: []string{"[VANADIS CHUNK FAILED]"}, wantIsolation: true, wantNotified: map[string]bool{k1: true}, wantStored: map[string]string{"c1": "failed", "c2": "running"}, wantBuild: "running"},
		{name: "dependent of failed chunk stays blocked", buildStatus: "running", chunks: []model.VanadisChunk{failed1, isolationChunk("c2", "pending", "c1")}, wantStatus: "failed", wantHeaders: []string{"[VANADIS BUILD FAILED]"}, wantIsolation: true, wantNotified: map[string]bool{k1: true}, wantStored: map[string]string{"c1": "failed", "c2": "pending"}, wantBuild: "failed"},
		{name: "legacy state with pre-failed chunk sends no wake", buildStatus: "running", chunks: []model.VanadisChunk{failed1, isolationChunk("c2", "deploying")}, prior: &model.BuildObjectState{Chunks: []model.VanadisChunk{failed1}}, wantStatus: "running", wantIsolation: true, wantNotified: map[string]bool{k1: true}, wantStored: map[string]string{"c1": "failed", "c2": "deploying"}, wantBuild: "running"},
		{name: "legacy failed build never relaunches", buildStatus: "failed", chunks: []model.VanadisChunk{failed1, isolationChunk("c2", "pending")}, prior: &model.BuildObjectState{Chunks: []model.VanadisChunk{failed1}}, wantStatus: "failed", wantNotified: map[string]bool{k1: true}, wantStored: map[string]string{"c1": "failed", "c2": "pending"}, wantBuild: "failed"},
		{name: "isolated failed build launches newly ready chunk", buildStatus: "failed", chunks: []model.VanadisChunk{failed1, isolationChunk("c2", "pending")}, prior: &model.BuildObjectState{FailureIsolation: true, FailureNotified: map[string]bool{k1: true}}, wantStatus: "running", wantLaunched: []string{"c2"}, wantIsolation: true, wantNotified: map[string]bool{k1: true}, wantStored: map[string]string{"c1": "failed", "c2": "running"}, wantBuild: "running"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wakes := &isolationWakes{}
			bld := model.VanadisBuild{ID: "b1", Status: tc.buildStatus, ThreadID: "th-1"}
			st := seededStoreOnly(t, []model.VanadisBuild{bld}, isolationSeed(tc.chunks))
			bo := NewBuildObject("", nil).WithAuthority(st, executor.NewExecutor("", wakes.client(), pubsub.NewMemoryBus(), "token", "example-owner", nil))
			out, err := bo.executeAuthoritativeTransition(context.Background(), "b1", nil, tc.prior, nil, false)
			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, out.BuildStatus)
			assert.Equal(t, tc.wantLaunched, out.LaunchedChunks)
			assert.Equal(t, tc.wantHeaders, wakes.headers)
			require.NotNil(t, out.State)
			assert.Equal(t, tc.wantIsolation, out.State.FailureIsolation)
			assert.Equal(t, tc.wantNotified, out.State.FailureNotified)
			assert.Equal(t, tc.wantStored, chunkStatuses(t, st, "b1"))
			stored, chunks := loadBuild(t, st, "b1")
			require.Len(t, chunks, len(tc.chunks))
			assert.Equal(t, tc.wantBuild, stored.Status)
		})
	}
}

func TestChunkFailureLine(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		chunk model.VanadisChunk
		want  string
	}{
		{"banner", model.VanadisChunk{ID: "c1", Banner: " Chunk 1 failed: boom "}, "Chunk 1 failed: boom [c1]"},
		{"no banner", model.VanadisChunk{ID: "c2", ChunkIndex: 1}, "Chunk 2 failed [c2]"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, chunkFailureLine(tc.chunk))
		})
	}
}

func TestPublishChunkFailed(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		faultErr    error
		chunk       model.VanadisChunk
		wantTargets []string
	}{
		{name: "success publishes failed chunk event", chunk: model.VanadisChunk{ID: "c1", Banner: "banner"}, wantTargets: []string{"c1"}},
		{name: "bus error logged without panic", faultErr: errors.New("bus fail"), chunk: model.VanadisChunk{ID: "c2"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rec := &recordingChunkBus{}
			var bus pubsub.Bus = rec
			if tc.faultErr != nil {
				bus = &pubsubtest.FaultBus{Bus: rec, PublishErr: tc.faultErr}
			}
			exec := executor.NewExecutor("", nil, bus, "token", "example-owner", nil)
			bo := NewBuildObject("", nil).WithAuthority(nil, exec)
			bo.publishChunkFailed(context.Background(), tc.chunk)
			var gotTargets []string
			for _, ev := range rec.events {
				if ev.Source == "vanadis_chunk" && ev.EventType == "failed" {
					gotTargets = append(gotTargets, ev.Targets...)
				}
			}
			assert.Equal(t, tc.wantTargets, gotTargets)
		})
	}
}

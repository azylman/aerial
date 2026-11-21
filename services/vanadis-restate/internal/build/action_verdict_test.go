package build

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

// verdictStore is an in-memory build authority for CompleteChunk tests.
type verdictStore struct {
	store.VanadisStore

	mu               sync.Mutex
	build            model.VanadisBuild
	chunks           []model.VanadisChunk
	getBuildCalls    int
	getBuildFailFrom int
	completeErr      error
}

func (s *verdictStore) GetBuild(context.Context, string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getBuildCalls++
	if s.getBuildFailFrom > 0 && s.getBuildCalls >= s.getBuildFailFrom {
		return nil, nil, errors.New("postgres unavailable")
	}
	bld := s.build
	return &bld, append([]model.VanadisChunk(nil), s.chunks...), nil
}

func (s *verdictStore) CompleteActionChunk(_ context.Context, chunkID, dispatchID string) (*store.VanadisAdvancementResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.completeErr != nil {
		return nil, s.completeErr
	}
	res := &store.VanadisAdvancementResult{Matched: true, BuildID: s.build.ID}
	for i, c := range s.chunks {
		if c.ID != chunkID || dispatchID != model.ActionDispatchID(c) || c.Mode != "action" || c.PRNumber != 0 {
			continue
		}
		switch {
		case c.Status == "running" && s.build.Status == "running":
			s.chunks[i].Status = "completed"
			res.CompletionAccepted, res.Advanced = true, true
		case c.Status == "completed" && (s.build.Status == "running" || s.build.Status == "completed"):
			res.CompletionAccepted = true
		}
	}
	return res, nil
}

func (s *verdictStore) UpdateBuildStatus(_ context.Context, _, status string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.build.Status = status
	return nil
}

func (*verdictStore) UpdateBuildStatusMessageID(context.Context, string, string) error { return nil }

func (s *verdictStore) chunkStatus(id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.chunks {
		if c.ID == id {
			return c.Status
		}
	}
	return ""
}

type completeChunkCase struct {
	name             string
	noStore          bool
	runner           string
	buildStatus      string
	chunks           []model.VanadisChunk
	req              CompleteChunkRequest
	calls            int
	getBuildFailFrom int
	completeErr      error
	want             CompleteChunkResponse
	wantErrText      string
	wantTerminal     bool
	wantRetriable    bool
	wantChunk        string
	wantBuild        string
	wantBanner       string
	wantEvent        string
}

func TestBuildObject_CompleteChunk(t *testing.T) {
	t.Parallel()
	running := model.VanadisChunk{ID: "c1", BuildID: "b1", ChunkIndex: 1, Mode: "action", Status: "running"}
	opened := model.VanadisChunk{ID: "c2", BuildID: "b1", ChunkIndex: 2, Mode: "pr", Status: "pr_opened", PRNumber: 7}
	done := model.VanadisChunk{ID: "c0", BuildID: "b1", ChunkIndex: 0, Mode: "action", Status: "completed"}
	verdict := CompleteChunkRequest{BuildID: "b1", ChunkID: "c1", DispatchID: "vanadis-c1-turn-0", Summary: "Verified live.\nAll checks green."}
	tests := []completeChunkCase{
		{name: "verdict completes running chunk and shows summary", buildStatus: "running", chunks: []model.VanadisChunk{done, running, opened}, req: verdict, calls: 1, want: CompleteChunkResponse{Status: "completed", BuildID: "b1", ChunkID: "c1", Completed: true}, wantChunk: "completed", wantBuild: "running", wantBanner: "✅ Chunk 2 completed: Verified live.", wantEvent: "aerial.events.vanadis_chunk.completed"},
		{name: "last verdict completes build", buildStatus: "running", chunks: []model.VanadisChunk{done, running}, req: verdict, calls: 1, want: CompleteChunkResponse{Status: "completed", BuildID: "b1", ChunkID: "c1", Completed: true}, wantChunk: "completed", wantBuild: "completed", wantBanner: "Build completed", wantEvent: "aerial.events.vanadis_chunk.completed"},
		{name: "repeat verdict reports already completed", buildStatus: "running", chunks: []model.VanadisChunk{done, running, opened}, req: verdict, calls: 2, want: CompleteChunkResponse{Status: "already_completed", BuildID: "b1", ChunkID: "c1", Completed: true}, wantChunk: "completed", wantBuild: "running", wantBanner: "✅ Chunk 2 completed: Verified live.", wantEvent: "aerial.events.vanadis_chunk.completed"},
		{name: "padded fields are trimmed", buildStatus: "running", chunks: []model.VanadisChunk{done, running, opened}, calls: 1, req: CompleteChunkRequest{BuildID: " b1 ", ChunkID: " c1 ", DispatchID: " vanadis-c1-turn-0 ", Summary: "  ok  "}, want: CompleteChunkResponse{Status: "completed", BuildID: "b1", ChunkID: "c1", Completed: true}, wantChunk: "completed", wantBuild: "running", wantBanner: "✅ Chunk 2 completed: ok", wantEvent: "aerial.events.vanadis_chunk.completed"},
		{name: "long summary line is truncated on banner", buildStatus: "running", chunks: []model.VanadisChunk{done, running, opened}, calls: 1, req: CompleteChunkRequest{BuildID: "b1", ChunkID: "c1", DispatchID: "vanadis-c1-turn-0", Summary: strings.Repeat("a", 301)}, want: CompleteChunkResponse{Status: "completed", BuildID: "b1", ChunkID: "c1", Completed: true}, wantChunk: "completed", wantBuild: "running", wantBanner: "✅ Chunk 2 completed: " + strings.Repeat("a", 299) + "…", wantEvent: "aerial.events.vanadis_chunk.completed"},
		{name: "missing authority", noStore: true, req: verdict, calls: 1, wantTerminal: true},
		{name: "foreign runner refused", runner: "agent-alpha", buildStatus: "running", chunks: []model.VanadisChunk{running}, req: verdict, calls: 1, wantErrText: "executed by another runner", wantTerminal: true, wantChunk: "running", wantBuild: "running"},
		{name: "build key mismatch", buildStatus: "running", chunks: []model.VanadisChunk{running}, calls: 1, req: CompleteChunkRequest{BuildID: "b2", ChunkID: "c1", DispatchID: "vanadis-c1-turn-0", Summary: "ok"}, wantErrText: "invalid vanadis build input", wantTerminal: true, wantChunk: "running", wantBuild: "running"},
		{name: "blank dispatch", buildStatus: "running", chunks: []model.VanadisChunk{running}, calls: 1, req: CompleteChunkRequest{BuildID: "b1", ChunkID: "c1", DispatchID: " ", Summary: "ok"}, wantErrText: "invalid vanadis build input", wantTerminal: true, wantChunk: "running", wantBuild: "running"},
		{name: "blank summary", buildStatus: "running", chunks: []model.VanadisChunk{running}, calls: 1, req: CompleteChunkRequest{BuildID: "b1", ChunkID: "c1", DispatchID: "vanadis-c1-turn-0", Summary: "  "}, wantErrText: "invalid vanadis build input", wantTerminal: true, wantChunk: "running", wantBuild: "running"},
		{name: "summary over limit", buildStatus: "running", chunks: []model.VanadisChunk{running}, calls: 1, req: CompleteChunkRequest{BuildID: "b1", ChunkID: "c1", DispatchID: "vanadis-c1-turn-0", Summary: strings.Repeat("é", 4001)}, wantErrText: "invalid vanadis build input", wantTerminal: true, wantChunk: "running", wantBuild: "running"},
		{name: "chunk outside build", buildStatus: "running", chunks: []model.VanadisChunk{done}, req: verdict, calls: 1, wantErrText: "invalid vanadis build input", wantTerminal: true, wantBuild: "running"},
		{name: "pr mode chunk refused", buildStatus: "running", chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Status: "running"}}, req: verdict, calls: 1, wantErrText: "invalid vanadis build input", wantTerminal: true, wantChunk: "running", wantBuild: "running"},
		{name: "stale dispatch conflicts", buildStatus: "running", chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "action", Status: "running", StallNudges: 1}}, req: verdict, calls: 1, wantErrText: "vanadis build state conflict", wantTerminal: true, wantChunk: "running", wantBuild: "running"},
		{name: "failed chunk conflicts", buildStatus: "failed", chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed"}}, req: verdict, calls: 1, wantErrText: "vanadis build state conflict", wantTerminal: true, wantChunk: "failed", wantBuild: "failed"},
		{name: "store read error retries", buildStatus: "running", chunks: []model.VanadisChunk{running}, req: verdict, calls: 1, getBuildFailFrom: 2, wantRetriable: true, wantChunk: "running", wantBuild: "running"},
		{name: "completion write error retries", buildStatus: "running", chunks: []model.VanadisChunk{running}, req: verdict, calls: 1, completeErr: errors.New("tx aborted"), wantRetriable: true, wantChunk: "running", wantBuild: "running"},
		{name: "transition read error after verdict", buildStatus: "running", chunks: []model.VanadisChunk{done, running, opened}, req: verdict, calls: 1, getBuildFailFrom: 3, wantRetriable: true, wantChunk: "completed", wantBuild: "running"},
		{name: "effects read error after transition", buildStatus: "running", chunks: []model.VanadisChunk{done, running, opened}, req: verdict, calls: 1, getBuildFailFrom: 5, wantRetriable: true, wantChunk: "completed", wantBuild: "running", wantEvent: "aerial.events.vanadis_chunk.completed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &verdictStore{build: model.VanadisBuild{ID: "b1", ThreadID: "th-1", Status: tc.buildStatus, RunnerAgent: tc.runner, Quiet: true}, chunks: tc.chunks, getBuildFailFrom: tc.getBuildFailFrom, completeErr: tc.completeErr}
			eri, bus := &mockEriClient{}, &mockBusExecutor{}
			var authority store.VanadisStore = st
			if tc.noStore {
				authority = nil
			}
			bo := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return newFakeObjectRuntime("b1") }).WithAuthority(authority, executor.NewExecutor(t.TempDir(), eri, bus, "", nil))
			var got CompleteChunkResponse
			var err error
			for range tc.calls {
				got, err = bo.CompleteChunk(nil, tc.req)
			}
			assertCompleteChunk(t, tc, got, err, st, eri, bus)
		})
	}
}

func assertCompleteChunk(t *testing.T, tc completeChunkCase, got CompleteChunkResponse, err error, st *verdictStore, eri *mockEriClient, bus *mockBusExecutor) {
	t.Helper()
	wantErr := tc.wantTerminal || tc.wantRetriable
	if (err != nil) != wantErr || got != tc.want {
		t.Fatalf("got=%+v err=%v, want %+v (err=%v)", got, err, tc.want, wantErr)
	}
	if err != nil && restate.IsTerminalError(err) != tc.wantTerminal {
		t.Fatalf("terminal=%v, want %v: %v", restate.IsTerminalError(err), tc.wantTerminal, err)
	}
	if tc.wantErrText != "" && !strings.Contains(err.Error(), tc.wantErrText) {
		t.Fatalf("err=%v, want %q", err, tc.wantErrText)
	}
	if c := st.chunkStatus("c1"); c != tc.wantChunk {
		t.Fatalf("chunk status=%q, want %q", c, tc.wantChunk)
	}
	if st.build.Status != tc.wantBuild {
		t.Fatalf("build status=%q, want %q", st.build.Status, tc.wantBuild)
	}
	banner := ""
	if eri.lastWidget != nil {
		banner = eri.lastWidget.Banner
	}
	if banner != tc.wantBanner || bus.lastSubj != tc.wantEvent {
		t.Fatalf("banner=%q event=%q, want %q %q", banner, bus.lastSubj, tc.wantBanner, tc.wantEvent)
	}
}

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/pubsub"
)

func TestNormalizeWidgetStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"ci_failed", "ci_failed", "failed"},
		{"deploy_failed", "deploy_failed", "failed"},
		{"RUNNING", "RUNNING", "running"},
		{"open", "open", "open"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := normalizeWidgetStatus(tc.in); got != tc.want {
				t.Errorf("normalizeWidgetStatus(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestResolveVanadisChunkWidgetStatus(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		chunkStatus string
		prStatus    string
		want        string
	}{
		{"failed_open", "failed", "open", "open"},
		{"merged_open", "merged", "open", "merged"},
		{"failed_deployed", "failed", "deployed", "deployed"},
		{"pending_deployed", "pending", "deployed", "deployed"},
		{"pending_deploying", "pending", "deploying", "deploying"},
		{"pending_synced", "pending", "synced", "synced"},
		{"pending_failed", "pending", "failed", "failed"},
		{"pending_conflict", "pending", "conflict", "conflict"},
		{"pending_merged", "pending", "merged", "merged"},
		{"merged_closed", "merged", "closed", "merged"},
		{"pending_closed", "pending", "closed", "failed"},
		{"coding_unknown", "coding", "unknown", "coding"},
		{"empty", "", "", "open"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := resolveVanadisChunkWidgetStatus(tc.chunkStatus, tc.prStatus); got != tc.want {
				t.Errorf("resolveVanadisChunkWidgetStatus(%q, %q) = %q, want %q", tc.chunkStatus, tc.prStatus, got, tc.want)
			}
		})
	}
}

func TestIsVanadisChunkStreamingPhase(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{"open", "open", true},
		{"pr_opened", "pr_opened", true},
		{"merged", "merged", true},
		{"synced", "synced", true},
		{"deploying", "deploying", true},
		{"deployed", "deployed", false},
		{"failed", "failed", false},
		{"pending", "pending", false},
		{"running", "running", false},
		{"empty", "", false},
		{"padded_uppercase_open", "  OPEN  ", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isVanadisChunkStreamingPhase(tc.in); got != tc.want {
				t.Errorf("isVanadisChunkStreamingPhase(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

// widgetBuild is a one-line VanadisBuild fixture for widget sync tests.
func widgetBuild(id, status, statusMessageID, title string, createdAt time.Time) VanadisBuild {
	return VanadisBuild{ID: id, Status: status, StatusMessageID: statusMessageID, Title: title, CreatedAt: createdAt}
}

// widgetPRReq is a one-line widget sync request for a found aerial PR.
func widgetPRReq(buildID string, prNumber int, prStatus, activeStep string) SyncChunkPRWidgetRequest {
	return SyncChunkPRWidgetRequest{BuildID: buildID, Repo: "aerial", PRNumber: prNumber, PRStatus: prStatus, ActiveStep: activeStep, PRFound: true}
}

func TestBuildSyncWidgetRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                string
		build               VanadisBuild
		chunks              []VanadisChunk
		req                 SyncChunkPRWidgetRequest
		wantStatus          string
		wantSkipSink        bool
		wantActiveStep      string
		wantChunkRepo       string
		wantChunkStatus     string
		wantZeroCompletedAt bool
	}{
		{name: "preserves active step and allows sink when chunk is open and build running with message", build: widgetBuild("build-1", "running", "msg-123", "Test Build", time.Unix(1760000000, 0)), chunks: []VanadisChunk{{ID: "c1", ChunkIndex: 0, PRNumber: 101, Status: "open"}}, req: widgetPRReq("build-1", 101, "open", "Waiting on CI (2/5 passed)"), wantStatus: "running", wantActiveStep: "Waiting on CI (2/5 passed)"},
		{name: "sets active step on pr_opened chunk", build: widgetBuild("build-2", "running", "msg-456", "Test Build 2", time.Time{}), chunks: []VanadisChunk{{ID: "c2", ChunkIndex: 0, PRNumber: 102, Status: "pr_opened"}}, req: widgetPRReq("build-2", 102, "open", "Waiting on CI (3/3 passed)"), wantStatus: "running", wantActiveStep: "Waiting on CI (3/3 passed)"},
		{name: "omits skipsink when build status message ID is empty", build: widgetBuild("build-3", "running", "", "Test Build 3", time.Time{}), chunks: []VanadisChunk{{ID: "c3", ChunkIndex: 0, PRNumber: 103, Status: "open"}}, req: widgetPRReq("build-3", 103, "open", "Waiting on CI (1/2 passed)"), wantStatus: "running", wantActiveStep: "Waiting on CI (1/2 passed)"},
		{name: "omits skipsink and active step when pr status is failed", build: widgetBuild("build-4", "running", "msg-789", "Test Build 4", time.Time{}), chunks: []VanadisChunk{{ID: "c4", ChunkIndex: 0, PRNumber: 104, Status: "pr_opened"}}, req: widgetPRReq("build-4", 104, "failed", "Waiting on CI (0/2 passed)"), wantStatus: "failed", wantActiveStep: ""},
		{name: "omits skipsink when active step is whitespace", build: widgetBuild("build-5", "running", "msg-999", "Test Build 5", time.Time{}), chunks: []VanadisChunk{{ID: "c5", ChunkIndex: 0, PRNumber: 105, Status: "open"}}, req: widgetPRReq("build-5", 105, "open", "   "), wantStatus: "running", wantActiveStep: ""},
		{name: "preserves active step and allows sink when chunk is merged", build: widgetBuild("build-merged", "running", "msg-merged", "Test Build Merged", time.Time{}), chunks: []VanadisChunk{{ID: "c-merged", ChunkIndex: 0, PRNumber: 106, Status: "open"}}, req: widgetPRReq("build-merged", 106, "merged", "Building container images (1/2 completed)"), wantStatus: "running", wantActiveStep: "Building container images (1/2 completed)"},
		{name: "preserves active step and allows sink when chunk is synced", build: widgetBuild("build-synced", "running", "msg-synced", "Test Build Synced", time.Time{}), chunks: []VanadisChunk{{ID: "c-synced", ChunkIndex: 0, PRNumber: 107, Status: "merged"}}, req: widgetPRReq("build-synced", 107, "synced", "Building container images (2/2 completed)"), wantStatus: "running", wantActiveStep: "Building container images (2/2 completed)"},
		{name: "preserves active step and allows sink when chunk is deploying", build: widgetBuild("build-deploying", "running", "msg-deploying", "Test Build Deploying", time.Time{}), chunks: []VanadisChunk{{ID: "c-deploying", ChunkIndex: 0, PRNumber: 108, Status: "synced"}}, req: widgetPRReq("build-deploying", 108, "deploying", "`eri`: 🔄 starting"), wantStatus: "running", wantActiveStep: "`eri`: 🔄 starting"},
		{name: "clears active step on deployed chunk", build: widgetBuild("build-deployed", "running", "msg-deployed", "Test Build Deployed", time.Time{}), chunks: []VanadisChunk{{ID: "c-deployed", ChunkIndex: 0, PRNumber: 109, Status: "deploying"}}, req: widgetPRReq("build-deployed", 109, "deployed", "All jobs healthy"), wantStatus: "running", wantActiveStep: ""},
		{name: "recovers failed build and failed chunk on open pr", build: widgetBuild("build-recover", "failed", "msg-recover", "Test Build Recover", time.Unix(1760000000, 0)), chunks: []VanadisChunk{{ID: "c-recover", ChunkIndex: 0, PRNumber: 110, Status: "failed", CompletedAt: time.Unix(1760001000, 0)}}, req: widgetPRReq("build-recover", 110, "open", "Waiting on CI (1/4 passed)"), wantStatus: "running", wantActiveStep: "Waiting on CI (1/4 passed)", wantZeroCompletedAt: true},
		{name: "resolves repo to action for action chunk", build: widgetBuild("build-action-widget", "running", "msg-action", "Test Action Widget", time.Time{}), chunks: []VanadisChunk{{ID: "c-action", ChunkIndex: 0, Mode: "action", Repo: "", Status: "running"}}, req: SyncChunkPRWidgetRequest{BuildID: "build-action-widget"}, wantStatus: "running", wantChunkRepo: "action"},
		{name: "preserves repo for action chunk with attached pr", build: widgetBuild("build-action-pr", "running", "msg-action-pr", "Test Action PR Widget", time.Time{}), chunks: []VanadisChunk{{ID: "c-action-pr", ChunkIndex: 0, Mode: "action", Repo: "azylman/aerial", PRNumber: 905, Status: "open"}}, req: SyncChunkPRWidgetRequest{BuildID: "build-action-pr"}, wantStatus: "running", wantChunkRepo: "azylman/aerial"},
		{name: "maps completed action chunk to deployed status", build: widgetBuild("build-action-completed", "completed", "msg-action-completed", "Test Action Completed Widget", time.Time{}), chunks: []VanadisChunk{{ID: "c-action-done", ChunkIndex: 0, Mode: "action", Repo: "", Status: "completed"}}, req: SyncChunkPRWidgetRequest{BuildID: "build-action-completed"}, wantStatus: "completed", wantChunkRepo: "action", wantChunkStatus: "deployed"},
		{name: "maps merged chunk without active step to deployed status", build: widgetBuild("build-merged-idle", "running", "msg-merged-idle", "Test Merged Idle Widget", time.Time{}), chunks: []VanadisChunk{{ID: "c-merged-idle", ChunkIndex: 0, Mode: "pr", Repo: "aerial", Status: "merged", PRNumber: 120}}, req: SyncChunkPRWidgetRequest{BuildID: "build-merged-idle"}, wantStatus: "running", wantChunkStatus: "deployed"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := buildSyncWidgetRequest(tc.build, tc.chunks, tc.req)
			if got.Status != tc.wantStatus || got.SkipSink != tc.wantSkipSink {
				t.Errorf("buildSyncWidgetRequest Status = %q SkipSink = %v, want %q %v", got.Status, got.SkipSink, tc.wantStatus, tc.wantSkipSink)
			}
			if len(got.Chunks) != 1 {
				t.Fatalf("buildSyncWidgetRequest returned %d chunks, want 1", len(got.Chunks))
			}
			c := got.Chunks[0]
			if c.ActiveStep != tc.wantActiveStep {
				t.Errorf("buildSyncWidgetRequest Chunk[0].ActiveStep = %q, want %q", c.ActiveStep, tc.wantActiveStep)
			}
			if tc.wantChunkRepo != "" && c.Repo != tc.wantChunkRepo {
				t.Errorf("buildSyncWidgetRequest Chunk[0].Repo = %q, want %q", c.Repo, tc.wantChunkRepo)
			}
			if tc.wantChunkStatus != "" && c.Status != tc.wantChunkStatus {
				t.Errorf("buildSyncWidgetRequest Chunk[0].Status = %q, want %q", c.Status, tc.wantChunkStatus)
			}
			if tc.wantZeroCompletedAt && !c.CompletedAt.IsZero() {
				t.Errorf("buildSyncWidgetRequest Chunk[0].CompletedAt = %v, want zero", c.CompletedAt)
			}
			if !tc.build.CreatedAt.IsZero() && !got.StartedAt.Equal(tc.build.CreatedAt) {
				t.Errorf("buildSyncWidgetRequest StartedAt = %v, want %v", got.StartedAt, tc.build.CreatedAt)
			}
		})
	}
}

func setupTestStoreWithBuild() (*memoryVanadisStore, string, []string) {
	s := newMemoryVanadisStore()
	buildID := "build-test-1"
	b := mkBuild(buildID, "123456789012345678", "Test Build Title", "Test Objective", "running")
	c1 := VanadisChunk{
		ID:         "chunk-1",
		BuildID:    buildID,
		ChunkIndex: 0,
		Mode:       "pr",
		Repo:       "aerial",
		Title:      "Chunk 1",
		Status:     "pending",
		Effort:     "low",
		DependsOn:  []string{},
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	c2 := VanadisChunk{
		ID:         "chunk-2",
		BuildID:    buildID,
		ChunkIndex: 1,
		Mode:       "action",
		Repo:       "aerial",
		Title:      "Chunk 2",
		Status:     "pending",
		Effort:     "medium",
		DependsOn:  []string{"chunk-1"},
		CreatedAt:  time.Now().UTC(),
		UpdatedAt:  time.Now().UTC(),
	}
	_ = s.CreateBuild(context.Background(), b, []VanadisChunk{c1, c2})
	return s, buildID, []string{"chunk-1", "chunk-2"}
}

func TestRepoMatching_TableDriven(t *testing.T) {
	candidateTests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty repo returns nil",
			input:    "",
			expected: nil,
		},
		{
			name:     "bare repo adds default owner prefix",
			input:    "aerial",
			expected: []string{"aerial", "azylman/aerial"},
		},
		{
			name:     "bare repo with whitespace trims and adds default owner prefix",
			input:    "  aerial  ",
			expected: []string{"aerial", "azylman/aerial"},
		},
		{
			name:     "prefixed repo with default owner adds bare candidate",
			input:    "azylman/aerial",
			expected: []string{"azylman/aerial", "aerial"},
		},
		{
			name:     "other org repo returns single candidate",
			input:    "other-org/aerial",
			expected: []string{"other-org/aerial"},
		},
		{
			name:     "nested slash under default owner returns single candidate",
			input:    "azylman/sub/repo",
			expected: []string{"azylman/sub/repo"},
		},
	}

	for _, tc := range candidateTests {
		t.Run("candidates_"+tc.name, func(t *testing.T) {
			got := repoMatchCandidates(tc.input)
			if !slices.Equal(got, tc.expected) {
				t.Errorf("repoMatchCandidates(%q) = %v, want %v", tc.input, got, tc.expected)
			}
		})
	}
}

func TestResolveChunkRef(t *testing.T) {
	chunks := []VanadisChunk{
		{ID: "chunk-a", ChunkIndex: 0},
		{ID: "chunk-b", ChunkIndex: 1},
		{ID: "10", ChunkIndex: 2},
	}
	tests := []struct {
		name     string
		chunks   []VanadisChunk
		chunkRef string
		wantIdx  int
	}{
		{"empty_ref", chunks, "", -1},
		{"whitespace_ref", chunks, "   ", -1},
		{"match_id_exact", chunks, "chunk-a", 0},
		{"match_id_with_whitespace", chunks, "  chunk-b  ", 1},
		{"match_index_string", chunks, "0", 0},
		{"match_index_second", chunks, "1", 1},
		{"match_id_takes_precedence_over_index", chunks, "10", 2},
		{"no_match_index_out_of_bounds", chunks, "99", -1},
		{"no_match_unknown_id", chunks, "unknown", -1},
		{"empty_slice", nil, "chunk-a", -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := resolveChunkRef(tc.chunks, tc.chunkRef)
			if got != tc.wantIdx {
				t.Errorf("resolveChunkRef() = %d, want %d", got, tc.wantIdx)
			}
		})
	}
}

const wantStallNotice = "[VANADIS CHUNK STALLED]\nYour previous turn for chunk c-stall ended without a successful `aerial-pr.sh submit` and without calling the vanadis MCP tool fail_chunk, so no PR was linked. This is the single stall retry: reuse the existing scratch workspace from your prior attempt if it still exists instead of re-initializing, finish the remaining implementation and verification in the foreground, then submit with `--vanadis-chunk c-stall` or call fail_chunk with the blocker. If this turn also ends without either, the chunk and build will be failed.\n\n"

// stallDispatch is the observable shape of one /dispatch request made by a stall re-dispatch.
type stallDispatch struct {
	ID, SessionID, TargetID, ChunkID, Notice string
	SkipIfActive                             bool
}

// stallBrain records /dispatch and /prompt traffic from ProcessStalledChunkCheck.
type stallBrain struct {
	mu           sync.Mutex
	dispatchCode int
	dispatches   []stallDispatch
	wakeTails    []string
}

func (b *stallBrain) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch r.URL.Path {
	case "/dispatch":
		var req BrainDispatchRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		notice, _, _ := strings.Cut(req.Prompt, "[VANADIS BUILD CHUNK EXECUTION]")
		b.dispatches = append(b.dispatches, stallDispatch{ID: req.ID, SessionID: req.SessionID, TargetID: req.TargetID, ChunkID: req.VanadisChunkID, Notice: notice, SkipIfActive: req.SkipIfActive})
		w.WriteHeader(b.dispatchCode)
	case "/prompt":
		var p map[string]string
		_ = json.NewDecoder(r.Body).Decode(&p)
		b.wakeTails = append(b.wakeTails, p["prompt"][strings.LastIndex(p["prompt"], "\n")+1:])
		w.WriteHeader(http.StatusAccepted)
	default:
		w.WriteHeader(http.StatusOK)
	}
}

func TestProcessStalledChunkCheck(t *testing.T) {
	t.Parallel()
	if NewVanadisServer(Config{}, nil, nil).ProcessStalledChunkCheck(context.Background(), "c-stall") == nil {
		t.Fatal("expected error when store is nil")
	}
	redispatch := []stallDispatch{{ID: "vanadis-c-stall-turn-1", SessionID: "vanadis-c-stall", TargetID: "123456789012345678", ChunkID: "c-stall", Notice: wantStallNotice, SkipIfActive: true}}
	tests := []struct {
		name                         string
		claimID, status, buildStatus string
		prNumber, priorNudges        int
		dispatchID                   string
		dispatchCode                 int
		wantDispatches               []stallDispatch
		wantChunkStatus              string
		wantBuildStatus              string
		wantWakeTails                []string
	}{
		{name: "first stall re-dispatches with notice", claimID: "c-stall", status: "running", buildStatus: "running", dispatchCode: http.StatusAccepted, wantDispatches: redispatch, wantChunkStatus: "running", wantBuildStatus: "running"},
		{name: "second stall fails chunk and wakes thread", claimID: "c-stall", status: "running", buildStatus: "running", priorNudges: 1, dispatchCode: http.StatusAccepted, wantChunkStatus: "failed", wantBuildStatus: "failed", wantWakeTails: []string{"Chunk 1/1 (Stalled Chunk) failed: chunk stalled twice: its turn ended without submitting a PR or calling fail_chunk, both on the original dispatch and on the stall re-dispatch"}},
		{name: "re-dispatch failure fails chunk", claimID: "c-stall", status: "running", buildStatus: "running", dispatchCode: http.StatusInternalServerError, wantDispatches: redispatch, wantChunkStatus: "failed", wantBuildStatus: "failed", wantWakeTails: []string{"Chunk 1/1 (Stalled Chunk) failed: chunk stalled (turn ended without submitting a PR or calling fail_chunk) and the stall re-dispatch failed: /dispatch returned status 500:"}},
		{name: "linked pr is left alone", claimID: "c-stall", status: "pr_opened", buildStatus: "running", prNumber: 9, dispatchCode: http.StatusAccepted, wantChunkStatus: "pr_opened", wantBuildStatus: "running"},
		{name: "missing chunk is skipped", claimID: "c-gone", status: "running", buildStatus: "running", dispatchCode: http.StatusAccepted, wantChunkStatus: "running", wantBuildStatus: "running"},
		{name: "duplicate delivery with mismatched dispatch id is noop", claimID: "c-stall", status: "running", buildStatus: "running", priorNudges: 1, dispatchID: "vanadis-c-stall-turn-0", dispatchCode: http.StatusAccepted, wantChunkStatus: "running", wantBuildStatus: "running"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			recorder := &stallBrain{dispatchCode: tc.dispatchCode}
			brain := httptest.NewServer(recorder)
			defer brain.Close()
			store := newMemoryVanadisStore()
			c := mkChunk("c-stall", "b-stall", 0, "Stalled Chunk", "do it", "low", tc.status)
			c.PRNumber = tc.prNumber
			_ = store.CreateBuild(context.Background(), mkBuild("b-stall", "123456789012345678", "T", "O", tc.buildStatus), []VanadisChunk{c})
			store.stallNudges = map[string]int{"c-stall": tc.priorNudges}
			srv := NewVanadisServer(Config{BrainURL: brain.URL}, store, brain.Client())
			if err := srv.ProcessStalledChunkCheck(context.Background(), tc.claimID, tc.dispatchID); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			b, chunks, _ := store.GetBuild(context.Background(), "b-stall")
			recorder.mu.Lock()
			defer recorder.mu.Unlock()
			if !reflect.DeepEqual(recorder.dispatches, tc.wantDispatches) {
				t.Fatalf("dispatches = %+v, want %+v", recorder.dispatches, tc.wantDispatches)
			}
			if !reflect.DeepEqual(recorder.wakeTails, tc.wantWakeTails) {
				t.Fatalf("wake tails = %q, want %q", recorder.wakeTails, tc.wantWakeTails)
			}
			if chunks[0].Status != tc.wantChunkStatus || b.Status != tc.wantBuildStatus {
				t.Fatalf("chunk=%q build=%q, want chunk=%q build=%q", chunks[0].Status, b.Status, tc.wantChunkStatus, tc.wantBuildStatus)
			}
		})
	}
}

func TestServer_CompleteChunkRunsStallCheck(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var dispatches, targetIDs, chunkIDs []string
	brain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/dispatch" {
			var req BrainDispatchRequest
			_ = json.NewDecoder(r.Body).Decode(&req)
			mu.Lock()
			dispatches = append(dispatches, req.SessionID)
			targetIDs = append(targetIDs, req.TargetID)
			chunkIDs = append(chunkIDs, req.VanadisChunkID)
			mu.Unlock()
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer brain.Close()
	store := newMemoryVanadisStore()
	_ = store.CreateBuild(context.Background(), mkBuild("b-done", "123456789012345678", "T", "O", "running"), []VanadisChunk{mkChunk("c-done", "b-done", 0, "Done", "do it", "", "running")})
	server := NewVanadisServer(Config{BrainURL: brain.URL}, store, brain.Client())
	ev := pubsub.DispatchOutcomeEvent{
		DispatchID:     "vanadis-c-done-turn-0",
		VanadisChunkID: "c-done",
		Status:         "completed",
	}
	evData, _ := json.Marshal(ev)
	if err := server.HandleDispatchOutcomeEvent(context.Background(), &memoryMsg{data: evData, subject: "aerial.dispatch.completed"}); err != nil {
		t.Fatalf("HandleDispatchOutcomeEvent failed: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(dispatches, []string{"vanadis-c-done"}) {
		t.Fatalf("dispatches = %v, want [vanadis-c-done]", dispatches)
	}
	if !reflect.DeepEqual(targetIDs, []string{"123456789012345678"}) || !reflect.DeepEqual(chunkIDs, []string{"c-done"}) {
		t.Fatalf("targetIDs = %v chunkIDs = %v, want [123456789012345678], [c-done]", targetIDs, chunkIDs)
	}
}

package build

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/restatetest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

type executePRAttachTestCase struct {
	name            string
	req             store.AttachChunkPRRequest
	mode, repo      string
	extraBuilds     []model.VanadisBuild
	extraChunks     []model.VanadisChunk
	prResponse      *executor.GitHubPullRequest
	githubStatus    int
	cancelErr       error
	busErr          error
	closed          bool
	wantCode        restate.Code
	wantMessage     string
	wantErr         bool
	wantChunkStatus string
	wantBuildStatus string
}

func runAttachMockCallback(t *testing.T, tc executePRAttachTestCase, attachStepWidgetCalls *atomic.Int32, fn func(restate.RunContext) (any, error), output any) restate.TerminalError {
	t.Helper()
	beforeCalls := attachStepWidgetCalls.Load()
	res, err := fn(restatetest.MockRunContext{Context: context.Background()})
	if tc.wantErr {
		require.Error(t, err)
		assert.Contains(t, err.Error(), tc.wantMessage)
		var terminal restate.TerminalError
		if tc.wantCode > 0 {
			require.True(t, errors.As(err, &terminal), "journal callback must stop retries: %v", err)
			assert.Equal(t, tc.wantCode, terminal.Code())
		} else {
			assert.False(t, errors.As(err, &terminal), "transient failures must remain retryable")
		}
		return restate.ToTerminalError(err)
	}
	require.NoError(t, err)
	assert.Equal(t, beforeCalls, attachStepWidgetCalls.Load(), "attach-pr-postgres step must not call SyncBuildWidget")
	if output != nil && res != nil {
		outVal := reflect.ValueOf(output)
		resVal := reflect.ValueOf(res)
		if outVal.Kind() == reflect.Pointer && !outVal.IsNil() && outVal.Elem().Type() == resVal.Type() {
			outVal.Elem().Set(resVal)
		}
	}
	return nil
}

func assertAttachOutcome(t *testing.T, tc executePRAttachTestCase, st *store.Store, resp model.AttachChunkPRResponse, err error) {
	t.Helper()
	if tc.wantErr {
		require.Error(t, err)
		return
	}
	require.NoError(t, err)
	assert.Equal(t, "attached", resp.Status)
	assert.Equal(t, "b1", resp.BuildID)
	bld, chunksRes := loadBuild(t, st, "b1")
	require.Len(t, chunksRes, 1)
	assert.Equal(t, 101, chunksRes[0].PRNumber)
	assert.Equal(t, "aerial", chunksRes[0].Repo)
	wantStatus := statusPROpened
	if tc.wantChunkStatus != "" {
		wantStatus = tc.wantChunkStatus
	}
	assert.Equal(t, wantStatus, chunksRes[0].Status)
	if tc.wantBuildStatus != "" {
		assert.Equal(t, tc.wantBuildStatus, bld.Status)
	}
}

func stubGitHubOpenPR() *executor.GitHubPullRequest {
	mergedFalse := false
	return &executor.GitHubPullRequest{
		Number: 101,
		NodeID: "PR_kwDO123",
		State:  "open",
		Merged: &mergedFalse,
		Head: struct {
			SHA  string `json:"sha"`
			Ref  string `json:"ref"`
			Repo *struct {
				Fork     bool   `json:"fork"`
				FullName string `json:"full_name"`
			} `json:"repo"`
		}{
			SHA: "head-sha-123",
			Ref: "feat/branch",
			Repo: &struct {
				Fork     bool   `json:"fork"`
				FullName string `json:"full_name"`
			}{
				Fork:     false,
				FullName: "example-owner/aerial",
			},
		},
		Base: struct {
			Repo *struct {
				FullName string `json:"full_name"`
			} `json:"repo"`
		}{
			Repo: &struct {
				FullName string `json:"full_name"`
			}{
				FullName: "example-owner/aerial",
			},
		},
	}
}

func stubGitHubMergedPR() *executor.GitHubPullRequest {
	mergedTrue := true
	return &executor.GitHubPullRequest{
		Number: 101,
		State:  "closed",
		Merged: &mergedTrue,
		Head: struct {
			SHA  string `json:"sha"`
			Ref  string `json:"ref"`
			Repo *struct {
				Fork     bool   `json:"fork"`
				FullName string `json:"full_name"`
			} `json:"repo"`
		}{
			SHA: "head-sha-123",
		},
		MergeCommitSHA: "merge-sha-456",
	}
}

func newAttachHTTPServer(tc executePRAttachTestCase, graphQLHit *atomic.Bool) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost && r.URL.Path == "/graphql" {
			graphQLHit.Store(true)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"data": map[string]any{
					"enablePullRequestAutoMerge": map[string]any{
						"pullRequest": map[string]any{
							"autoMergeRequest": map[string]any{
								"enabledAt":   "2026-11-26T00:00:00Z",
								"mergeMethod": "SQUASH",
							},
						},
					},
				},
			})
			return
		}
		if tc.githubStatus != 0 {
			w.WriteHeader(tc.githubStatus)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "error"})
			return
		}
		if tc.prResponse != nil {
			_ = json.NewEncoder(w).Encode(tc.prResponse)
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"message": "Not Found"})
	}))
}

// Inspect the callback error before the SDK can retry the journaled operation.
func TestExecutePRAttachment(t *testing.T) {
	t.Parallel()
	githubOpenPR := stubGitHubOpenPR()
	githubMergedPR := stubGitHubMergedPR()

	tests := []executePRAttachTestCase{
		{name: "missing action repo", req: attachRequest("c1", ""), mode: "action", repo: "", closed: false, wantCode: 400, wantMessage: "repo is required", wantErr: true},
		{name: "blank action repo", req: attachRequest("c1", "  "), mode: "action", repo: "", closed: false, wantCode: 400, wantMessage: "repo is required", wantErr: true},
		{name: "standalone repo", req: attachRequest("", ""), mode: "pr", repo: "aerial", closed: false, wantCode: 400, wantMessage: "repo is required", wantErr: true},
		{name: "invalid number", req: store.AttachChunkPRRequest{PRNumber: -1}, mode: "pr", repo: "aerial", closed: false, wantCode: 400, wantMessage: "valid pr number", wantErr: true},
		{name: "transient database error", req: attachRequest("c1", "aerial"), mode: "pr", repo: "aerial", closed: true, wantCode: 0, wantMessage: "closed pool", wantErr: true},
		{name: "successful attach skips sync widget during attach step", req: attachRequest("c1", "aerial"), mode: "pr", repo: "aerial", closed: false, wantCode: 0, wantErr: false, wantChunkStatus: statusPROpened, wantBuildStatus: statusRunning},
		{
			name:            "action chunk attach skips pr reconcile",
			req:             attachRequest("c1", "aerial"),
			mode:            "action",
			repo:            "",
			wantChunkStatus: statusPROpened,
			wantBuildStatus: statusRunning,
		},
		{
			name:            "github api error is logged and swallowed",
			req:             attachRequest("c1", "aerial"),
			mode:            "pr",
			repo:            "aerial",
			githubStatus:    http.StatusInternalServerError,
			wantChunkStatus: statusPROpened,
			wantBuildStatus: statusRunning,
		},
		{
			name:            "donor chunk completed transitions chunk to completed and completes build",
			req:             attachRequest("c1", "aerial"),
			mode:            "pr",
			repo:            "aerial",
			extraBuilds:     []model.VanadisBuild{{ID: "b_donor", ThreadID: "th-2", Status: statusRunning, Title: "Donor Build"}},
			extraChunks:     []model.VanadisChunk{{ID: "c_donor", BuildID: "b_donor", Status: statusCompleted, Repo: "aerial", Mode: "pr", PRNumber: 101}},
			wantChunkStatus: statusCompleted,
			wantBuildStatus: statusCompleted,
		},
		{
			name:            "donor chunk merged with cancel and bus errors",
			req:             attachRequest("c1", "aerial"),
			mode:            "pr",
			repo:            "aerial",
			extraBuilds:     []model.VanadisBuild{{ID: "b_donor", ThreadID: "th-2", Status: statusRunning, Title: "Donor Build"}},
			extraChunks:     []model.VanadisChunk{{ID: "c_donor", BuildID: "b_donor", Status: statusMerged, Repo: "aerial", Mode: "pr", PRNumber: 101}},
			cancelErr:       errors.New("cancel error"),
			busErr:          errors.New("bus error"),
			wantChunkStatus: statusMerged,
			wantBuildStatus: statusRunning,
		},
		{
			name:            "open pr enables auto-merge via graphql",
			req:             attachRequest("c1", "aerial"),
			mode:            "pr",
			repo:            "aerial",
			prResponse:      githubOpenPR,
			wantChunkStatus: statusPROpened,
			wantBuildStatus: statusRunning,
		},
		{
			name:            "github merged transitions chunk to merged",
			req:             attachRequest("c1", "aerial"),
			mode:            "pr",
			repo:            "aerial",
			prResponse:      githubMergedPR,
			wantChunkStatus: statusMerged,
			wantBuildStatus: statusRunning,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			builds := []model.VanadisBuild{{ID: "b1", ThreadID: "th-1", Status: statusRunning, Title: "Build", StatusMessageID: "msg-123"}}
			builds = append(builds, tc.extraBuilds...)
			chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", Status: statusRunning, Repo: tc.repo, Mode: tc.mode}}
			chunks = append(chunks, tc.extraChunks...)
			st := seededStoreOnly(t, builds, chunks)
			if tc.closed {
				st = closedStore(t)
			}
			var attachStepWidgetCalls atomic.Int32
			eri := &mockEriClient{
				cancelFn: func(ctx context.Context, chunkID string) error {
					return tc.cancelErr
				},
				widgetFn: func(ctx context.Context, req executor.StatusWidgetRequest) (string, error) {
					attachStepWidgetCalls.Add(1)
					return "msg-123", nil
				},
			}
			var bus pubsub.Bus = pubsub.NewMemoryBus()
			if tc.busErr != nil {
				bus = &pubsubtest.FaultBus{PublishErr: tc.busErr}
			}
			var graphQLHit atomic.Bool
			srv := newAttachHTTPServer(tc, &graphQLHit)
			defer srv.Close()
			exec := executor.NewExecutor(t.TempDir(), eri, bus, "token", "example-owner", nil)
			exec.GitHubAPIURL = srv.URL
			fakeRT := newFakeObjectRuntime("b1")
			bo := NewBuildObject("", nil, func(restate.ObjectContext) objectRuntime { return fakeRT }).WithAuthority(st, exec)
			mc := mocks.NewMockContext(t)
			mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(func(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
				return runAttachMockCallback(t, tc, &attachStepWidgetCalls, fn, output)
			})
			resp, err := bo.executePRAttachment(restate.WithMockContext(mc), tc.req)
			assertAttachOutcome(t, tc, st, resp, err)
			if tc.prResponse == githubOpenPR {
				assert.True(t, graphQLHit.Load(), "expected graphql endpoint to be hit for auto-merge")
			}
		})
	}
}

func TestBuildObject_RefreshFailedPRWidget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		build     *model.VanadisBuild
		closed    bool
		syncErr   error
		noExec    bool
		wantCalls int
	}{
		{name: "nil executor no-op", noExec: true, wantCalls: 0},
		{name: "store read error skips widget sync", closed: true, wantCalls: 0},
		{name: "non-failed build skips widget sync", build: &model.VanadisBuild{ID: "b1", Status: "running"}, wantCalls: 0},
		{name: "failed build sync widget error logged", build: &model.VanadisBuild{ID: "b1", Status: "failed"}, syncErr: errors.New("sync error"), wantCalls: 1},
		{name: "failed build sync widget success", build: &model.VanadisBuild{ID: "b1", Status: "failed"}, wantCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := openStore(t)
			if tc.build != nil {
				seedBuild(t, pool, *tc.build)
			}
			if tc.closed {
				pool.Close()
			}
			var widgetCalls int
			var exec *executor.Executor
			if !tc.noExec {
				eri := &mockEriClient{
					widgetFn: func(context.Context, executor.StatusWidgetRequest) (string, error) {
						widgetCalls++
						return "", tc.syncErr
					},
				}
				exec = executor.NewExecutor(t.TempDir(), eri, pubsub.NewMemoryBus(), "token", "example-owner", nil)
			}
			bo := NewBuildObject("", nil).WithAuthority(st, exec)
			bo.refreshFailedPRWidget(context.Background(), "b1")
			assert.Equal(t, tc.wantCalls, widgetCalls)
		})
	}
}

func TestBuildObject_ReconcileAttachedChunkState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		nilStore    bool
		buildID     string
		chunkID     string
		prNumber    int
		build       *model.VanadisBuild
		chunks      []model.VanadisChunk
		closed      bool
		noExec      bool
		wantErr     bool
		wantMessage string
	}{
		{name: "nil store returns nil", nilStore: true, buildID: "b1", chunkID: "c1", prNumber: 101},
		{name: "build not found returns nil", buildID: "b-nonexistent", chunkID: "c1", prNumber: 101},
		{name: "store read error returns error", buildID: "b1", chunkID: "c1", prNumber: 101, closed: true, wantErr: true, wantMessage: "closed pool"},
		{name: "chunk not found returns nil", buildID: "b1", chunkID: "c-missing", prNumber: 101, build: &model.VanadisBuild{ID: "b1", Status: statusRunning}, chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: statusPROpened}}},
		{name: "action mode chunk returns nil", buildID: "b1", chunkID: "c1", prNumber: 101, build: &model.VanadisBuild{ID: "b1", Status: statusRunning}, chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "action", Status: "running"}}},
		{name: "merged status with no exec returns nil", buildID: "b1", chunkID: "c1", prNumber: 101, build: &model.VanadisBuild{ID: "b1", Status: statusRunning}, chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Repo: "aerial", Status: statusMerged, PRNumber: 101}}, noExec: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st, pool := openStore(t)
			if tc.build != nil {
				seedBuild(t, pool, *tc.build)
			}
			for _, c := range tc.chunks {
				seedChunk(t, pool, c)
			}
			if tc.closed {
				pool.Close()
			}
			var exec *executor.Executor
			if !tc.noExec {
				exec = executor.NewExecutor(t.TempDir(), &mockEriClient{}, pubsub.NewMemoryBus(), "token", "example-owner", nil)
			}
			boStore := st
			if tc.nilStore {
				boStore = nil
			}
			bo := NewBuildObject("", nil).WithAuthority(boStore, exec)
			err := bo.reconcileAttachedChunkState(context.Background(), tc.buildID, tc.chunkID, tc.prNumber)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantMessage)
				return
			}
			require.NoError(t, err)
		})
	}
}

package prflow

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/githubapitest"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/pkg/pubsubtest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/eritest"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

type checkStatusSpyStore struct {
	store.VanadisStore

	lifecycleStatus string
	getBuildErr     error
	chunks          []model.VanadisChunk
	nilBuild        bool
	lookup          *store.VanadisChunkPRLookup
	lookupErr       error
	calls           []string
	resets          []string
	failed          []string
	banners         []string
	attached        []string
	attachErr       error
	failCountCalls  []string
	failCount       int
	failCountErr    error
	err             error
	resetErr        error
	statusErr       error
	bannerErr       error
	failErr         error
	skipFailApply   bool
	failBanner      string
	quiet           bool
	agent           string
}

func (s *checkStatusSpyStore) chunk(chunkID string) *model.VanadisChunk {
	for i := range s.chunks {
		if s.chunks[i].ID == chunkID {
			return &s.chunks[i]
		}
	}
	s.chunks = append(s.chunks, model.VanadisChunk{ID: chunkID, BuildID: "b1", Effort: effortMedium, CIFailCount: s.failCount})
	return &s.chunks[len(s.chunks)-1]
}

func (s *checkStatusSpyStore) IncrementChunkCIFailCount(_ context.Context, chunkID, banner string, headSHA ...string) (int, error) {
	s.failCountCalls = append(s.failCountCalls, chunkID+":"+banner)
	if s.failCountErr != nil {
		return 0, s.failCountErr
	}
	s.failCount++
	c := s.chunk(chunkID)
	c.CIFailCount = s.failCount
	if len(headSHA) > 0 && strings.TrimSpace(headSHA[0]) != "" {
		c.RemediatedSHA = strings.TrimSpace(headSHA[0])
	}
	return s.failCount, nil
}

func (s *checkStatusSpyStore) GetBuild(_ context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	if s.getBuildErr != nil {
		return nil, nil, s.getBuildErr
	}
	if s.nilBuild {
		return nil, nil, nil
	}
	chunks := s.chunks
	if len(chunks) == 0 {
		chunks = []model.VanadisChunk{{ID: "c1", BuildID: buildID, Effort: effortMedium, CIFailCount: s.failCount}}
	}
	return &model.VanadisBuild{ID: buildID, Status: statusRunning, ThreadID: "th-1", Agent: s.agent}, chunks, nil
}

func (s *checkStatusSpyStore) RecordPRCheckStatus(_ context.Context, chunkID, headSHA, checkName, checkState string) error {
	s.calls = append(s.calls, chunkID+":"+headSHA+":"+checkName+":"+checkState)
	return s.err
}

func (s *checkStatusSpyStore) ResetPRCheckState(_ context.Context, chunkID, headSHA, mergeSHA string) error {
	s.resets = append(s.resets, chunkID+":"+headSHA+":"+mergeSHA)
	if s.resetErr != nil {
		return s.resetErr
	}
	if sha := strings.TrimSpace(headSHA); sha != "" {
		s.chunk(chunkID).HeadSHA = sha
	}
	return nil
}

func (s *checkStatusSpyStore) UpdatePRLifecycleStatus(_ context.Context, _ string, status string) error {
	s.lifecycleStatus = status
	return s.statusErr
}

func (s *checkStatusSpyStore) FailChunk(_ context.Context, chunkID, reason, _ string) (*model.VanadisBuild, *model.VanadisChunk, bool, error) {
	s.failed = append(s.failed, chunkID+":"+reason)
	if s.failErr != nil {
		return nil, nil, false, s.failErr
	}
	c := s.chunk(chunkID)
	banner := c.Banner
	if s.failBanner != "" {
		banner = s.failBanner
	} else if !s.skipFailApply && banner == "" {
		banner = "Chunk 1 failed: " + reason
	}
	return &model.VanadisBuild{ID: "b1", Status: statusRunning, Objective: "Obj", ThreadID: "th-1", Quiet: s.quiet, Agent: s.agent}, &model.VanadisChunk{ID: chunkID, BuildID: "b1", Status: statusFailed, Banner: banner}, !s.skipFailApply, nil
}

func (s *checkStatusSpyStore) UpdateChunkBannerAndStatus(_ context.Context, chunkID, status, banner string) error {
	s.banners = append(s.banners, chunkID+":"+status+":"+banner)
	return s.bannerErr
}

func (*checkStatusSpyStore) CompareAndSwapBuildStatusMessageID(context.Context, string, string, string) (bool, error) {
	return true, nil
}

func (s *checkStatusSpyStore) GetChunkByPR(_ context.Context, _ string, _ int) (*store.VanadisChunkPRLookup, error) {
	if s.lookupErr != nil {
		return nil, s.lookupErr
	}
	if s.lookup != nil {
		return s.lookup, nil
	}
	return &store.VanadisChunkPRLookup{ChunkID: "c1", BuildID: "b1", Matched: true}, nil
}

func (s *checkStatusSpyStore) AttachChunkPR(_ context.Context, req store.AttachChunkPRRequest) (string, string, string, error) {
	if s.attachErr != nil {
		return "", "", "", s.attachErr
	}
	s.attached = append(s.attached, req.ChunkID)
	return "b1", req.ChunkID, "th-1", nil
}

var (
	errLifecycleDB   = errors.New("db error")
	errLifecycleRead = errors.New("read unavailable")
)

const (
	lifecycleSkipped  = "pr lifecycle transition not applied"
	lifecycleCIBanner = `c1:ci_failed:CI check "unit-tests" failed`
	lifecycleCapFail  = "c1:ci_remediation_cap_reached"
	lifecycleCapText  = "Chunk 1 failed: ci_remediation_cap_reached"
	lifecycleConflict = "c1:conflict:Merge conflict detected"
)

func ciFailEv(checkName, headSHA string) pubsub.PRLifecycleEvent {
	return pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: checkName, HeadSHA: headSHA, Repo: "aerial", PRNumber: 42, Details: "test failure"}
}

func conflictEv(headSHA string) pubsub.PRLifecycleEvent {
	return pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: headSHA, Repo: "aerial", PRNumber: 42}
}

// newLifecycleExecutor builds the executor every Flow table uses: a recording
// eri fake whose cancel/message/widget calls and bus publishes all fail with
// notifyErr, and an optional scripted GitHub API.
func newLifecycleExecutor(t *testing.T, eri *eritest.Client, notifyErr error, apiStatus int, apiBody string, routes ...map[string][]githubapitest.Response) *executor.Executor {
	t.Helper()
	var bus pubsub.Bus
	if notifyErr != nil {
		bus = &pubsubtest.FaultBus{PublishErr: notifyErr}
	}
	exec := executor.NewExecutor(t.TempDir(), eri, bus, "token", "example-owner", nil)
	var r map[string][]githubapitest.Response
	if len(routes) > 0 {
		r = routes[0]
	}
	if len(r) > 0 {
		api := githubapitest.New(t, r)
		exec.GitHubAPIURL = api.URL()
	} else if apiStatus != 0 {
		api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(apiStatus)
			if _, err := w.Write([]byte(apiBody)); err != nil {
				t.Errorf("write response: %v", err)
			}
		}))
		t.Cleanup(api.Close)
		exec.GitHubAPIURL = api.URL
	}
	return exec
}

func assertLifecycleErr(t *testing.T, want string, err error) {
	t.Helper()
	if want == "" {
		assert.NoError(t, err)
		return
	}
	assert.ErrorContains(t, err, want)
}

func TestFlow_HandlePRCheckEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		event     string
		ev        pubsub.PRLifecycleEvent
		storeNil  bool
		err       error
		wantErr   string
		wantCalls []string
	}{
		{name: "empty check name is ignored", event: "check_updated", ev: pubsub.PRLifecycleEvent{CheckName: ""}},
		{name: "empty check state on check_updated is ignored", event: "check_updated", ev: pubsub.PRLifecycleEvent{CheckName: "test"}},
		{name: "check_updated records exact state", event: "check_updated", ev: pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test", CheckState: "running"}, wantCalls: []string{"c1:sha-1:test:running"}},
		{name: "check_failed defaults empty state to failed", event: "check_failed", ev: pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test"}, wantCalls: []string{"c1:sha-1:test:failed"}},
		{name: "store error propagates", event: "check_updated", ev: pubsub.PRLifecycleEvent{HeadSHA: "merge-sha-3", CheckName: "test", CheckState: "passed"}, err: errLifecycleDB, wantErr: "db error", wantCalls: []string{"c1:merge-sha-3:test:passed"}},
		{name: "nil store returns nil", event: "check_updated", ev: pubsub.PRLifecycleEvent{HeadSHA: "head", CheckName: "lint", CheckState: "passed"}, storeNil: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{err: tc.err}
			var st store.VanadisStore = spy
			if tc.storeNil {
				st = nil
			}
			assertLifecycleErr(t, tc.wantErr, NewFlow(st, nil).HandlePRCheckEvent(context.Background(), "c1", tc.event, tc.ev))
			assert.Equal(t, tc.wantCalls, spy.calls)
		})
	}
}

func TestFlow_LocateOrAttachPRChunk(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		buildID     string
		chunks      []model.VanadisChunk
		storeNil    bool
		getBuildErr error
		withExec    bool
		lookup      *store.VanadisChunkPRLookup
		lookupErr   error
		ev          pubsub.PRLifecycleEvent
		wantChunk   string
		wantErr     string
	}{
		{name: "matched lookup returns chunk", ev: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12}, wantChunk: "c1"},
		{name: "own build chunk", lookup: &store.VanadisChunkPRLookup{ChunkID: "c1", BuildID: "b1", Matched: true}, ev: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12}, wantChunk: "c1"},
		{name: "blank owner keeps chunk", lookup: &store.VanadisChunkPRLookup{ChunkID: "c1", Matched: true}, ev: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12}, wantChunk: "c1"},
		{name: "foreign build chunk is never acted on", lookup: &store.VanadisChunkPRLookup{ChunkID: "c9", BuildID: "b2", Matched: true}, ev: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12}},
		{name: "unmatched lookup without executor resolves nothing", lookup: &store.VanadisChunkPRLookup{Matched: false}, ev: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12}},
		{name: "lookup error propagates", lookupErr: errLifecycleRead, ev: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12}, wantErr: "read unavailable"},
		{name: "nil store returns empty", storeNil: true, ev: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12}},
		{
			name:      "own build attached chunk preferred over foreign pr lookup",
			chunks:    []model.VanadisChunk{{ID: "c-own", BuildID: "b1", Repo: "repo", PRNumber: 12, Status: "running"}},
			lookup:    &store.VanadisChunkPRLookup{ChunkID: "c-other", BuildID: "b2", Matched: true},
			ev:        pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12},
			wantChunk: "c-own",
		},
		{
			name:      "own build attached chunk preferred over foreign pr lookup with executor",
			chunks:    []model.VanadisChunk{{ID: "c-own-exec", BuildID: "b1", Repo: "example-owner/repo", PRNumber: 12, Status: "running"}},
			lookup:    &store.VanadisChunkPRLookup{ChunkID: "c-other", BuildID: "b2", Matched: true},
			ev:        pubsub.PRLifecycleEvent{Repo: "repo", PRNumber: 12},
			withExec:  true,
			wantChunk: "c-own-exec",
		},
		{
			name:        "store get build error falls back to lookup",
			getBuildErr: errors.New("get build error"),
			lookup:      &store.VanadisChunkPRLookup{ChunkID: "c-fallback", BuildID: "b1", Matched: true},
			ev:          pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12},
			wantChunk:   "c-fallback",
		},
		{
			name:      "blank build id skips build scan",
			buildID:   "  ",
			chunks:    []model.VanadisChunk{{ID: "c-scanned", BuildID: "b1", Repo: "owner/repo", PRNumber: 12, Status: "running"}},
			lookup:    &store.VanadisChunkPRLookup{ChunkID: "c-lookup", Matched: true},
			ev:        pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12},
			wantChunk: "c-lookup",
		},
		{
			name:      "own build cancelled chunk ignored",
			chunks:    []model.VanadisChunk{{ID: "c-canc", BuildID: "b1", Repo: "repo", PRNumber: 12, Status: "cancelled"}},
			lookup:    &store.VanadisChunkPRLookup{ChunkID: "c-active", BuildID: "b1", Matched: true},
			ev:        pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12},
			wantChunk: "c-active",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var st store.VanadisStore
			if !tc.storeNil {
				st = &checkStatusSpyStore{chunks: tc.chunks, lookup: tc.lookup, lookupErr: tc.lookupErr, getBuildErr: tc.getBuildErr}
			}
			var exec *executor.Executor
			if tc.withExec {
				exec = newLifecycleExecutor(t, &eritest.Client{}, nil, 0, "")
			}
			f := NewFlow(st, exec)
			bID := "b1"
			if tc.buildID != "" {
				bID = tc.buildID
			}
			got, err := f.LocateOrAttachPRChunk(context.Background(), bID, tc.ev)
			assertLifecycleErr(t, tc.wantErr, err)
			assert.Equal(t, tc.wantChunk, got)
		})
	}
}

func TestFlow_SyncAttachedPRWidget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		execNil     bool
		getBuildErr error
		widgetErr   error
		wantWidget  bool
	}{
		{name: "nil executor skips widget", execNil: true},
		{name: "build read error skips widget", getBuildErr: errLifecycleRead},
		{name: "attached build syncs widget", wantWidget: true},
		{name: "widget error is swallowed", widgetErr: errors.New("widget unavailable"), wantWidget: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eri := &eritest.Client{WidgetErr: tc.widgetErr}
			var exec *executor.Executor
			if !tc.execNil {
				exec = newLifecycleExecutor(t, eri, nil, 0, "")
			}
			NewFlow(&checkStatusSpyStore{getBuildErr: tc.getBuildErr}, exec).SyncAttachedPRWidget(context.Background(), "b1")
			assert.Equal(t, tc.wantWidget, eri.LastWidget != nil)
		})
	}
}

func TestFlow_SyncBuildWidget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		execNil     bool
		storeNil    bool
		flowNil     bool
		getBuildErr error
		nilBuild    bool
		widgetErr   error
		wantWidget  bool
	}{
		{name: "success syncs widget", wantWidget: true},
		{name: "nil flow skips", flowNil: true},
		{name: "nil executor skips", execNil: true},
		{name: "nil store skips", storeNil: true},
		{name: "get build error skips", getBuildErr: errLifecycleRead},
		{name: "nil build skips", nilBuild: true},
		{name: "widget error swallowed", widgetErr: errors.New("widget unavailable"), wantWidget: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eri := &eritest.Client{WidgetErr: tc.widgetErr}
			var exec *executor.Executor
			if !tc.execNil {
				exec = newLifecycleExecutor(t, eri, nil, 0, "")
			}
			spy := &checkStatusSpyStore{getBuildErr: tc.getBuildErr, nilBuild: tc.nilBuild}
			var st store.VanadisStore = spy
			if tc.storeNil {
				st = nil
			}
			f := NewFlow(st, exec)
			if tc.flowNil {
				f = nil
			}
			f.syncBuildWidget(context.Background(), "b1", "notice")
			assert.Equal(t, tc.wantWidget, eri.LastWidget != nil)
		})
	}
}

func TestFlow_HandleCIRemediationCapReached(t *testing.T) {
	t.Parallel()
	ev := ciFailEv("unit-tests", "abcdef1234")
	tests := []struct {
		name     string
		flowNil  bool
		storeNil bool
		execNil  bool
	}{
		{name: "nil flow returns nil", flowNil: true},
		{name: "nil store returns nil", storeNil: true},
		{name: "nil exec returns nil", execNil: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var st store.VanadisStore = &checkStatusSpyStore{}
			if tc.storeNil {
				st = nil
			}
			var exec *executor.Executor
			if !tc.execNil {
				exec = newLifecycleExecutor(t, &eritest.Client{}, nil, 0, "")
			}
			f := NewFlow(st, exec)
			if tc.flowNil {
				f = nil
			}
			assert.NoError(t, f.HandleCIRemediationCapReached(context.Background(), "c1", 5, ev))
		})
	}
}

func TestFlow_HandleClosedPREvent(t *testing.T) {
	t.Parallel()
	unmerged := pubsub.PRLifecycleEvent{Event: "closed_unmerged"}
	failed := []string{"c1:pr_closed_unmerged"}
	tests := []struct {
		name          string
		ev            pubsub.PRLifecycleEvent
		failErr       error
		getBuildErr   error
		skipFailApply bool
		failBanner    string
		cancelErr     error
		promptErr     error
		widgetErr     error
		busErr        error
		wantErr       string
		wantFailed    []string
		wantCancel    string
	}{
		{name: "closed unmerged fails chunk and leaves the wake to reconcile", ev: pubsub.PRLifecycleEvent{Event: "closed", Status: "closed"}, wantFailed: failed, wantCancel: "c1"},
		{name: "closed with status merged skips fail chunk", ev: pubsub.PRLifecycleEvent{Event: "closed", Status: "merged"}},
		{name: "already failed chunk skips notifications", ev: unmerged, skipFailApply: true, wantFailed: failed},
		{name: "retry with matching banner completes notifications", ev: unmerged, skipFailApply: true, failBanner: "Chunk 1 failed: pr_closed_unmerged", wantFailed: failed, wantCancel: "c1"},
		{name: "fail chunk error propagates", ev: unmerged, failErr: errLifecycleDB, wantErr: "db error", wantFailed: failed},
		{name: "cancel error swallowed", ev: unmerged, cancelErr: errors.New("cancel unavailable"), wantFailed: failed, wantCancel: "c1"},
		{name: "read error propagates", ev: unmerged, getBuildErr: errLifecycleRead, wantErr: "read unavailable", wantFailed: failed, wantCancel: "c1"},
		{name: "widget error swallowed", ev: unmerged, widgetErr: errors.New("widget unavailable"), busErr: errors.New("bus unavailable"), wantFailed: failed, wantCancel: "c1"},
		{name: "wake left to reconcile", ev: unmerged, promptErr: errors.New("prompt unavailable"), busErr: errors.New("bus unavailable"), wantFailed: failed, wantCancel: "c1"},
		{name: "publish errors swallowed", ev: unmerged, busErr: errors.New("bus unavailable"), wantFailed: failed, wantCancel: "c1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{getBuildErr: tc.getBuildErr, failErr: tc.failErr, skipFailApply: tc.skipFailApply, failBanner: tc.failBanner}
			eri := &eritest.Client{CancelErr: tc.cancelErr, PromptErr: tc.promptErr, WidgetErr: tc.widgetErr}
			var bus pubsub.Bus
			if tc.busErr != nil {
				bus = &pubsubtest.FaultBus{PublishErr: tc.busErr}
			}
			f := NewFlow(spy, executor.NewExecutor(t.TempDir(), eri, bus, "token", "example-owner", nil))
			err := f.HandleClosedPREvent(context.Background(), "c1", tc.ev)
			assertLifecycleErr(t, tc.wantErr, err)
			assert.Equal(t, tc.wantFailed, spy.failed)
			assert.Equal(t, tc.wantCancel, eri.LastCancel)
			assert.Empty(t, eri.LastPrompt)
		})
	}
}

func TestFlow_HandleDeployFailureEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		details        string
		execNil        bool
		storeNil       bool
		getBuildErr    error
		nilBuild       bool
		dispatchErr    error
		wantErr        string
		wantNotFound   bool
		wantDispatch   bool
		wantDispatchID string
	}{
		{name: "success dispatches silent wake", wantDispatch: true},
		{name: "nil executor skips silent wake", execNil: true},
		{name: "nil store skips banner and silent wake", storeNil: true},
		{name: "read error propagates", getBuildErr: errLifecycleDB, wantErr: "db error"},
		{name: "missing build reports not found", nilBuild: true, wantErr: "not found", wantNotFound: true},
		{name: "silent wake error propagates", dispatchErr: errLifecycleDB, wantErr: "db error", wantDispatch: true},
		{name: "active dispatch treated as handled", dispatchErr: eriwire.ErrDispatchActive, wantDispatch: true},
		// Redeliveries of one occurrence reuse the ID; a distinct failure gets its own.
		{name: "first occurrence dispatch id is stable across redelivery", details: "crashloop-1", wantDispatch: true, wantDispatchID: "vanadis-deploy-fail-c1-6036eca51e3809fa"},
		{name: "distinct occurrence gets a distinct dispatch id", details: "crashloop-2", wantDispatch: true, wantDispatchID: "vanadis-deploy-fail-c1-86465ab6c9979e95"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{getBuildErr: tc.getBuildErr, nilBuild: tc.nilBuild}
			eri := &eritest.Client{DispatchErr: tc.dispatchErr}
			var st store.VanadisStore = spy
			if tc.storeNil {
				st = nil
			}
			var exec *executor.Executor
			if !tc.execNil {
				exec = newLifecycleExecutor(t, eri, nil, 0, "")
			}
			err := NewFlow(st, exec).HandleDeployFailureEvent(context.Background(), "b1", "c1", pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "synapse", Details: tc.details})
			assertLifecycleErr(t, tc.wantErr, err)
			if tc.wantNotFound {
				assert.ErrorIs(t, err, store.ErrBuildNotFound)
			}
			assert.Empty(t, eri.LastPrompt)
			if !tc.wantDispatch {
				assert.Nil(t, eri.LastDispatch)
				return
			}
			require.NotNil(t, eri.LastDispatch)
			assert.Contains(t, eri.LastDispatch.Prompt, "Deployment failed")
			if tc.wantDispatchID != "" {
				assert.Equal(t, tc.wantDispatchID, eri.LastDispatch.ID)
			}
		})
	}
}

func newMockTurnEndedServer(t *testing.T, prStatus int, prBody, commentsBody string, checkRunsStatus int, checkRunsBody string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/example-owner/aerial/pulls/42":
			w.WriteHeader(prStatus)
			_, _ = w.Write([]byte(prBody))
		case "/repos/example-owner/aerial/issues/42/comments":
			if commentsBody == "" {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			_, _ = w.Write([]byte(commentsBody))
		case "/repos/example-owner/aerial/commits/abcdef1234567/check-runs":
			status := checkRunsStatus
			body := checkRunsBody
			if status == 0 && body == "" {
				status = http.StatusOK
				body = `{"check_runs":[{"name":"unit-tests","status":"completed","conclusion":"failure"}]}`
			}
			w.WriteHeader(status)
			_, _ = w.Write([]byte(body))
		default:
			t.Errorf("unexpected GitHub request %s", r.URL.Path)
			w.WriteHeader(http.StatusTeapot)
		}
	}))
}

func TestFlow_HandleCIRemediationTurnEnded(t *testing.T) {
	t.Parallel()
	const (
		head       = "abcdef1234567"
		openPR     = `{"state":"open","merged":false,"head":{"sha":"abcdef1234567"}}`
		movedPR    = `{"state":"open","merged":false,"head":{"sha":"fedcba7654321"}}`
		closedPR   = `{"state":"closed","merged":false,"head":{"sha":"abcdef1234567"}}`
		mergedPR   = `{"state":"closed","merged":true,"head":{"sha":"abcdef1234567"}}`
		override   = `[{"id":1,"body":"## roborev-override (` + "`abcdef1`" + `)"}]`
		noComments = `[]`
		pauseFail  = "c1:" + CIRemediationNoOutcomeReason
		pauseMsg   = "🛑 [aerial#42] CI fix-up turn ended without pushing a fix, recording an override, or calling fail_chunk. Chunk paused for a decision."
		pauseNote  = "CI fix-up turn ended without an outcome — awaiting a decision"
	)
	ciFailed := func(checks map[string]string, banner string) model.VanadisChunk {
		return model.VanadisChunk{ID: "c1", BuildID: "b1", Repo: "aerial", Status: statusCIFailed, PRNumber: 42, CIFailCount: 2, CIChecks: checks, Banner: banner}
	}
	tests := []struct {
		name            string
		dispatchID      string
		chunk           model.VanadisChunk
		quiet           bool
		prStatus        int
		prBody          string
		commentsBody    string
		checkRunsStatus int
		checkRunsBody   string
		wantErr         bool
		wantFailed      []string
		wantMsg         string
		wantCancel      string
		wantWidget      string
	}{
		{name: "head same pauses with alert and banner", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(nil, `CI check "unit-tests" failed`), prStatus: 200, prBody: openPR, wantFailed: []string{pauseFail}, wantMsg: pauseMsg, wantCancel: "c1", wantWidget: pauseNote},
		{name: "quiet build pauses without alert", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(nil, ""), quiet: true, prStatus: 200, prBody: openPR, wantFailed: []string{pauseFail}, wantCancel: "c1", wantWidget: pauseNote},
		{name: "head moved is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(nil, ""), prStatus: 200, prBody: movedPR},
		{name: "closed pr is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(nil, ""), prStatus: 200, prBody: closedPR},
		{name: "merged pr is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(nil, ""), prStatus: 200, prBody: mergedPR},
		{name: "missing pr is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(nil, ""), prStatus: 404, prBody: `{}`},
		{name: "github outage stays retryable", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(nil, ""), prStatus: 500, prBody: `{}`, wantErr: true},
		{name: "chunk already failed is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Repo: "aerial", Status: statusFailed, PRNumber: 42, CIFailCount: 2}},
		{name: "stale attempt is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", head, 1), chunk: ciFailed(nil, "")},
		{name: "chunk without pr is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: model.VanadisChunk{ID: "c1", BuildID: "b1", Status: statusCIFailed, CIFailCount: 2}},
		{name: "sha-less dispatch is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", "", 2), chunk: ciFailed(nil, "")},
		{name: "other chunk dispatch is a no-op", dispatchID: executor.CIRemediationDispatchID("c2", head, 2), chunk: ciFailed(nil, "")},
		{name: "unknown chunk is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: model.VanadisChunk{ID: "c9", BuildID: "b1"}},
		{name: "roborev override on head is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(map[string]string{"roborev": "failed"}, ""), prStatus: 200, prBody: openPR, commentsBody: override},
		{name: "roborev banner override on head is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(nil, `CI check "RoboRev Code Review" failed`), prStatus: 200, prBody: openPR, commentsBody: override},
		{name: "roborev without override pauses", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(map[string]string{"roborev": "failed"}, ""), prStatus: 200, prBody: openPR, commentsBody: noComments, wantFailed: []string{pauseFail}, wantMsg: pauseMsg, wantCancel: "c1", wantWidget: pauseNote},
		{name: "passed roborev skips override lookup and pauses", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(map[string]string{"roborev": "passed", "unit-tests": "failed"}, `CI check unquoted`), prStatus: 200, prBody: openPR, commentsBody: override, wantFailed: []string{pauseFail}, wantMsg: pauseMsg, wantCancel: "c1", wantWidget: pauseNote},
		{name: "mixed roborev and lint failure pauses even with override", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(map[string]string{"roborev": "failed", "lint": "failed"}, ""), prStatus: 200, prBody: openPR, commentsBody: override, wantFailed: []string{pauseFail}, wantMsg: pauseMsg, wantCancel: "c1", wantWidget: pauseNote},
		{name: "roborev with underscore key is handled", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(map[string]string{"_": "pending", "roborev": "failed"}, ""), prStatus: 200, prBody: openPR, commentsBody: override},
		{name: "override lookup outage stays retryable", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(map[string]string{"roborev": "failed"}, ""), prStatus: 200, prBody: openPR, wantErr: true},
		{name: "active queued check run is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(map[string]string{"unit-tests": "failed"}, ""), prStatus: 200, prBody: openPR, checkRunsStatus: 200, checkRunsBody: `{"check_runs":[{"name":"unit-tests","status":"queued","conclusion":""}]}`},
		{name: "active in_progress check run is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(map[string]string{"unit-tests": "failed"}, ""), prStatus: 200, prBody: openPR, checkRunsStatus: 200, checkRunsBody: `{"check_runs":[{"name":"unit-tests","status":"in_progress","conclusion":""}]}`},
		{name: "all check runs recovered is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(map[string]string{"unit-tests": "failed"}, ""), prStatus: 200, prBody: openPR, checkRunsStatus: 200, checkRunsBody: `{"check_runs":[{"name":"unit-tests","status":"completed","conclusion":"success"}]}`},
		{name: "local pending check in chunk is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(map[string]string{"unit-tests": "pending"}, "")},
		{name: "local running check in chunk is a no-op", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(map[string]string{"unit-tests": "running"}, "")},
		{name: "check-runs outage stays retryable", dispatchID: executor.CIRemediationDispatchID("c1", head, 2), chunk: ciFailed(map[string]string{"unit-tests": "failed"}, ""), prStatus: 200, prBody: openPR, checkRunsStatus: 502, checkRunsBody: `{}`, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := newMockTurnEndedServer(t, tc.prStatus, tc.prBody, tc.commentsBody, tc.checkRunsStatus, tc.checkRunsBody)
			t.Cleanup(api.Close)
			eri := &eritest.Client{}
			exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", "example-owner", api.Client())
			exec.GitHubAPIURL = api.URL
			spy := &checkStatusSpyStore{chunks: []model.VanadisChunk{tc.chunk}, quiet: tc.quiet}
			err := NewFlow(spy, exec).HandleCIRemediationTurnEnded(context.Background(), "b1", "c1", tc.dispatchID)
			assert.Equal(t, tc.wantErr, err != nil, err)
			assert.Equal(t, tc.wantFailed, spy.failed)
			assert.Equal(t, tc.wantMsg, eri.LastMsgCont)
			assert.Equal(t, tc.wantCancel, eri.LastCancel)
			if tc.wantWidget == "" {
				assert.Nil(t, eri.LastWidget)
				return
			}
			require.NotNil(t, eri.LastWidget)
			assert.Equal(t, tc.wantWidget, eri.LastWidget.Banner)
		})
	}
}

func TestFlow_NoPRTurnAction(t *testing.T) {
	t.Parallel()
	pr := func(status string, prNumber int) model.VanadisChunk {
		return model.VanadisChunk{ID: "c1", Mode: "pr", Status: status, PRNumber: prNumber}
	}
	act := func(status string, prNumber int) model.VanadisChunk {
		return model.VanadisChunk{ID: "c1", Mode: "action", Status: status, PRNumber: prNumber, StallNudges: 1}
	}
	tests := []struct {
		name        string
		chunk       model.VanadisChunk
		buildStatus string
		dispatchID  string
		want        NoPRAction
	}{
		{"running pr chunk launch turn resumes", pr("running", 0), "running", "vanadis-c1-turn-0", NoPRActionResume},
		{"coding pr chunk launch turn resumes", pr("coding", 0), "running", "vanadis-c1-turn-0", NoPRActionResume},
		{"legacy empty mode launch turn resumes", model.VanadisChunk{ID: "c1", Status: "running"}, "running", "vanadis-c1-turn-0", NoPRActionResume},
		{"resume turn without pr fails", pr("running", 0), "running", "vanadis-c1-nopr-resume-1", NoPRActionFail},
		{"attached pr ignored", pr("running", 12), "running", "vanadis-c1-turn-0", NoPRActionNone},
		{"pr opened status ignored", pr("pr_opened", 0), "running", "vanadis-c1-turn-0", NoPRActionNone},
		{"failed chunk ignored", pr("failed", 0), "running", "vanadis-c1-nopr-resume-1", NoPRActionNone},
		{"cancelled chunk ignored", pr("cancelled", 0), "running", "vanadis-c1-turn-0", NoPRActionNone},
		{"failed build resumes", pr("running", 0), "failed", "vanadis-c1-turn-0", NoPRActionResume},
		{"cancelled build ignored", pr("running", 0), "cancelled", "vanadis-c1-nopr-resume-1", NoPRActionNone},
		{"ci remediation dispatch ignored", pr("running", 0), "running", "vanadis-c1-cifail-abc123-1", NoPRActionNone},
		{"conflict remediation dispatch ignored", pr("running", 0), "running", "vanadis-conflict-c1-abc123", NoPRActionNone},
		{"other chunk resume id ignored", pr("running", 0), "running", "vanadis-c2-nopr-resume-1", NoPRActionNone},
		{"action launch turn resumes", act("running", 0), "running", "vanadis-c1-turn-1", NoPRActionResume},
		{"action resume below cap resumes", act("running", 0), "running", "vanadis-c1-turn-1-resume-11", NoPRActionResume},
		{"action last resume fails", act("running", 0), "running", "vanadis-c1-turn-1-resume-12", NoPRActionFailAction},
		{"action repair pr opened resumes", act("pr_opened", 7), "running", "vanadis-c1-turn-1", NoPRActionResume},
		{"action repair deploying resumes", act("deploying", 7), "running", "vanadis-c1-turn-1-resume-2", NoPRActionResume},
		{"action repair deploy failed at cap fails", act("deploy_failed", 7), "running", "vanadis-c1-turn-1-resume-12", NoPRActionFailAction},
		{"action pr status without pr ignored", act("pr_opened", 0), "running", "vanadis-c1-turn-1", NoPRActionNone},
		{"completed action ignored", act("completed", 0), "running", "vanadis-c1-turn-1", NoPRActionNone},
		{"failed action ignored", act("failed", 0), "running", "vanadis-c1-turn-1-resume-12", NoPRActionNone},
		{"completed action with pr ignored", act("completed", 7), "running", "vanadis-c1-turn-1", NoPRActionNone},
		{"action on failed build resumes", act("running", 0), "failed", "vanadis-c1-turn-1", NoPRActionResume},
		{"action stale generation ignored", act("running", 0), "running", "vanadis-c1-turn-0", NoPRActionNone},
		{"action ci remediation ignored", act("running", 0), "running", "vanadis-c1-cifail-abc-1", NoPRActionNone},
		{"action no-pr resume id ignored", act("running", 0), "running", "vanadis-c1-nopr-resume-1", NoPRActionNone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, NoPRTurnAction(tc.chunk, tc.buildStatus, tc.dispatchID))
		})
	}
}

func TestFlow_FindChunkIndex(t *testing.T) {
	t.Parallel()
	present := []model.VanadisChunk{{ID: "c1"}, {ID: "c2"}}
	tests := []struct {
		name   string
		chunks []model.VanadisChunk
		id     string
		want   int
	}{
		{"first match", present, "c1", 0},
		{"later match", present, "c2", 1},
		{"missing chunk", present, "c9", -1},
		{"empty list", nil, "c1", -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, FindChunkIndex(tc.chunks, tc.id))
		})
	}
}

func TestFlow_SelectUnattachedTargetChunk(t *testing.T) {
	t.Parallel()
	now := time.Now()
	running := func(id string) model.VanadisChunk {
		return model.VanadisChunk{ID: id, BuildID: "b1", Mode: "pr", Status: "running", Repo: "owner/repo", StartedAt: now}
	}
	build := &model.VanadisBuild{ID: "b1", ThreadID: "th-1", Status: "running"}
	threadEv := pubsub.PRLifecycleEvent{TargetID: "th-1", Repo: "owner/repo", PRNumber: 12}
	tests := []struct {
		name   string
		build  *model.VanadisBuild
		chunks []model.VanadisChunk
		ev     pubsub.PRLifecycleEvent
		want   string
	}{
		{"nil build matches nothing", nil, []model.VanadisChunk{running("c1")}, threadEv, ""},
		{"branch-encoded chunk matches", build, []model.VanadisChunk{running("c1")}, pubsub.PRLifecycleEvent{Repo: "owner/repo", Branch: "feat/repo-pr-vanadis-c1-123456", TargetID: "th-1"}, "c1"},
		{"branch-encoded attached chunk rejected", build, []model.VanadisChunk{{ID: "c1", BuildID: "b1", Mode: "pr", Status: "running", Repo: "owner/repo", StartedAt: now, PRNumber: 12}}, pubsub.PRLifecycleEvent{Repo: "owner/repo", Branch: "feat/repo-pr-vanadis-c1-123456", TargetID: "th-1"}, ""},
		{"branch-encoded unknown chunk rejected", build, []model.VanadisChunk{running("c1")}, pubsub.PRLifecycleEvent{Repo: "owner/repo", Branch: "feat/repo-pr-vanadis-c9-123456", TargetID: "th-1"}, ""},
		{"thread fallback single match", build, []model.VanadisChunk{running("c1")}, threadEv, "c1"},
		{"thread fallback ambiguous match rejected", build, []model.VanadisChunk{running("c1"), running("c2")}, threadEv, ""},
		{"thread mismatch rejected", build, []model.VanadisChunk{running("c1")}, pubsub.PRLifecycleEvent{TargetID: "other", Repo: "owner/repo", PRNumber: 12}, ""},
		{"quiet event rejected", build, []model.VanadisChunk{running("c1")}, pubsub.PRLifecycleEvent{TargetID: "th-1", Repo: "owner/repo", PRNumber: 12, Quiet: true}, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := SelectUnattachedTargetChunk("example-owner", tc.build, tc.chunks, tc.ev)
			if tc.want == "" {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, tc.want, got.ID)
		})
	}
}

func TestFlow_ResolveOrAttachPRChunk(t *testing.T) {
	t.Parallel()
	now := time.Now()
	newChunk := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "running", Repo: "owner/repo", StartedAt: now, Effort: "medium"}
	otherChunk := model.VanadisChunk{ID: "c2", BuildID: "b1", Mode: "pr", Status: "running", Repo: "owner/repo", StartedAt: now, Effort: "medium"}
	branch := "feat/repo-pr-vanadis-c1-123456"
	trailer := `notes <!-- aerial-pr:{"target_id":"th-1","vanadis_chunk_id":"c1"} -->`
	validBody := fmt.Sprintf(`{"state":"open","merged":false,"body":%q,"user":{"login":"owner"},"author_association":"MEMBER","head":{"sha":"deadbee","ref":%q,"repo":{"fork":false,"full_name":"owner/repo"}},"base":{"repo":{"full_name":"owner/repo"}}}`,
		trailer, branch)
	attachedEv := pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, Branch: branch, HeadSHA: "deadbee", TargetID: "th-1"}

	tests := []struct {
		name         string
		execNil      bool
		apiStatus    int
		apiBody      string
		chunks       []model.VanadisChunk
		attachErr    error
		getBuildErr  error
		widgetErr    bool
		event        pubsub.PRLifecycleEvent
		wantChunk    string
		wantErr      bool
		wantAttached []string
	}{
		{name: "nil executor resolves nothing", execNil: true, chunks: []model.VanadisChunk{newChunk}, event: pubsub.PRLifecycleEvent{Repo: "owner/repo", Branch: branch, TargetID: "th-1"}},
		{name: "valid attachment attaches chunk and syncs widget", apiStatus: http.StatusOK, apiBody: validBody, chunks: []model.VanadisChunk{newChunk}, event: attachedEv, wantChunk: "c1", wantAttached: []string{"c1"}},
		{name: "branch mismatch skips without attaching", apiStatus: http.StatusOK, apiBody: validBody, chunks: []model.VanadisChunk{newChunk}, event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, Branch: "other", HeadSHA: "deadbee", TargetID: "th-1"}},
		{name: "build not found resolves nothing", chunks: []model.VanadisChunk{newChunk}, getBuildErr: store.ErrBuildNotFound, event: pubsub.PRLifecycleEvent{Repo: "owner/repo", Branch: branch, TargetID: "th-1"}},
		{name: "read error propagates", chunks: []model.VanadisChunk{newChunk}, getBuildErr: errors.New("read unavailable"), event: pubsub.PRLifecycleEvent{Repo: "owner/repo", Branch: branch, TargetID: "th-1"}, wantErr: true},
		{name: "attach conflict resolves nothing", apiStatus: http.StatusOK, apiBody: validBody, chunks: []model.VanadisChunk{newChunk}, attachErr: store.ErrBuildConflict, event: attachedEv},
		{name: "ambiguous thread match resolves nothing", chunks: []model.VanadisChunk{newChunk, otherChunk}, event: pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, TargetID: "th-1"}},
		{name: "attachment validation transport error propagates", apiStatus: http.StatusInternalServerError, apiBody: "{}", chunks: []model.VanadisChunk{newChunk}, event: attachedEv, wantErr: true},
		{name: "attach transport error propagates", apiStatus: http.StatusOK, apiBody: validBody, chunks: []model.VanadisChunk{newChunk}, attachErr: errors.New("attach unavailable"), event: attachedEv, wantErr: true},
		{name: "widget error after attach is swallowed", apiStatus: http.StatusOK, apiBody: validBody, chunks: []model.VanadisChunk{newChunk}, widgetErr: true, event: attachedEv, wantChunk: "c1", wantAttached: []string{"c1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.apiStatus)
				if _, err := w.Write([]byte(tc.apiBody)); err != nil {
					t.Errorf("write response: %v", err)
				}
			}))
			defer api.Close()
			eri := &eritest.Client{}
			if tc.widgetErr {
				eri.WidgetErr = errors.New("widget unavailable")
			}
			exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", "example-owner", nil)
			exec.GitHubAPIURL = api.URL
			spy := &checkStatusSpyStore{chunks: tc.chunks, attachErr: tc.attachErr, getBuildErr: tc.getBuildErr}
			ex := exec
			if tc.execNil {
				ex = nil
			}
			f := NewFlow(spy, ex)
			got, err := f.ResolveOrAttachPRChunk(context.Background(), "b1", tc.event)
			assert.Equal(t, tc.wantErr, err != nil)
			assert.Equal(t, tc.wantChunk, got)
			assert.Equal(t, tc.wantAttached, spy.attached)
		})
	}
}

func TestFlow_ResumeNoPRChunk(t *testing.T) {
	t.Parallel()
	bld := &model.VanadisBuild{ID: "b1", ThreadID: "th-1", Title: "Build", Objective: "Objective", Status: "running"}
	chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", ChunkIndex: 0, Mode: "pr", Status: "running", Repo: "aerial", Title: "Chunk 1", Instructions: "Do it"}}

	tests := []struct {
		name        string
		dispatchErr bool
		widgetErr   bool
		wantErr     bool
	}{
		{name: "resume dispatch succeeds", wantErr: false},
		{name: "dispatch error propagates", dispatchErr: true, wantErr: true},
		{name: "widget error swallowed", widgetErr: true, wantErr: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{}
			eri := &eritest.Client{}
			if tc.dispatchErr {
				eri.DispatchErr = errors.New("dispatch unavailable")
			}
			if tc.widgetErr {
				eri.WidgetErr = errors.New("widget unavailable")
			}
			exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", "example-owner", nil)
			f := NewFlow(spy, exec)
			err := f.ResumeNoPRChunk(context.Background(), bld, chunks, 0)
			assert.Equal(t, tc.wantErr, err != nil)
			require.NotNil(t, eri.LastDispatch)
			assert.Equal(t, "vanadis-c1-nopr-resume-1", eri.LastDispatch.ID)
			assert.False(t, eri.LastDispatch.SkipIfActive)
		})
	}
}

func TestFlow_NextActionResume(t *testing.T) {
	t.Parallel()
	chunk := model.VanadisChunk{ID: "c1", Mode: "action", StallNudges: 2}
	tests := []struct {
		name       string
		dispatchID string
		wantK      int
		wantOK     bool
	}{
		{"launch turn yields first resume", "vanadis-c1-turn-2", 1, true},
		{"resume eleven yields last resume", "vanadis-c1-turn-2-resume-11", 12, true},
		{"last resume yields none", "vanadis-c1-turn-2-resume-12", 0, false},
		{"stale generation yields none", "vanadis-c1-turn-1", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			k, ok := NextActionResume(chunk, tc.dispatchID)
			assert.Equal(t, tc.wantK, k)
			assert.Equal(t, tc.wantOK, ok)
		})
	}
}

func TestFlow_ResumeActionChunk(t *testing.T) {
	t.Parallel()
	bld := &model.VanadisBuild{ID: "b1", ThreadID: "th-1", Title: "Build", Objective: "Objective", Status: "running"}
	chunks := []model.VanadisChunk{{ID: "c1", BuildID: "b1", ChunkIndex: 0, Mode: "action", Status: "running", StallNudges: 1, Title: "Chunk 1", Instructions: "Verify it live"}}
	tests := []struct {
		name         string
		k            int
		dispatchErr  error
		widgetErr    error
		wantErr      bool
		wantID       string
		wantPrefix   string
		wantBanner   string
		wantNoWidget bool
	}{
		{name: "first resume dispatched", k: 1, wantID: "vanadis-c1-turn-1-resume-1", wantPrefix: "[VANADIS AUTOMATIC RESUME 1/12]\n", wantBanner: "Chunk 1 turn ended without complete_chunk — automatic resume 1/12 dispatched"},
		{name: "last resume dispatched", k: 12, wantID: "vanadis-c1-turn-1-resume-12", wantPrefix: "[VANADIS AUTOMATIC RESUME 12/12]\n", wantBanner: "Chunk 1 turn ended without complete_chunk — automatic resume 12/12 dispatched"},
		{name: "dispatch error propagates", k: 3, dispatchErr: errors.New("dispatch unavailable"), wantErr: true, wantID: "vanadis-c1-turn-1-resume-3", wantPrefix: "[VANADIS AUTOMATIC RESUME 3/12]\n", wantNoWidget: true},
		{name: "widget error swallowed", k: 2, widgetErr: errors.New("widget unavailable"), wantID: "vanadis-c1-turn-1-resume-2", wantPrefix: "[VANADIS AUTOMATIC RESUME 2/12]\n", wantBanner: "Chunk 1 turn ended without complete_chunk — automatic resume 2/12 dispatched"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			eri := &eritest.Client{DispatchErr: tc.dispatchErr, WidgetErr: tc.widgetErr, WidgetID: "msg-1"}
			f := NewFlow(&checkStatusSpyStore{}, executor.NewExecutor(t.TempDir(), eri, nil, "token", "example-owner", nil))
			err := f.ResumeActionChunk(context.Background(), bld, chunks, 0, tc.k)
			assert.Equal(t, tc.wantErr, err != nil)
			require.NotNil(t, eri.LastDispatch)
			assert.Equal(t, tc.wantID, eri.LastDispatch.ID)
			assert.Equal(t, "vanadis-c1", eri.LastDispatch.SessionID)
			assert.False(t, eri.LastDispatch.SkipIfActive)
			assert.True(t, strings.HasPrefix(eri.LastDispatch.Prompt, tc.wantPrefix), eri.LastDispatch.Prompt)
			assert.Contains(t, eri.LastDispatch.Prompt, "Verify it live")
			if tc.wantNoWidget {
				assert.Nil(t, eri.LastWidget)
				return
			}
			require.NotNil(t, eri.LastWidget)
			assert.Contains(t, eri.LastWidget.Banner, tc.wantBanner)
		})
	}
}

func TestFlow_NotifyChunkFailure_Guards(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		nilFlow bool
		nilExec bool
	}{
		{name: "nil flow skips failure notification", nilFlow: true},
		{name: "nil executor skips failure notification", nilExec: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var f *Flow
			if !tc.nilFlow {
				var exec *executor.Executor
				if !tc.nilExec {
					exec = executor.NewExecutor(t.TempDir(), &eritest.Client{}, nil, "token", "example-owner", nil)
				}
				f = NewFlow(&checkStatusSpyStore{}, exec)
			}
			err := f.NotifyChunkFailure(context.Background(), model.VanadisBuild{ID: "b1"}, "c1", "boom")
			assert.NoError(t, err)
		})
	}
}

func TestFlow_IsEligibleUnattachedChunk(t *testing.T) {
	t.Parallel()
	now := time.Now()
	eligible := model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "running", Repo: "owner/repo", StartedAt: now}
	build := &model.VanadisBuild{ID: "b1", ThreadID: "th-1", Status: "running"}
	ev := pubsub.PRLifecycleEvent{TargetID: "th-1", Repo: "owner/repo", PRNumber: 12}
	tests := []struct {
		name  string
		build *model.VanadisBuild
		chunk *model.VanadisChunk
		event pubsub.PRLifecycleEvent
		want  bool
	}{
		{name: "nil build rejected", chunk: &eligible, event: ev},
		{name: "nil chunk rejected", build: build, event: ev},
		{name: "non-pr mode rejected", build: build, chunk: &model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "running", StartedAt: now}, event: ev},
		{name: "attached chunk rejected", build: build, chunk: &model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "running", StartedAt: now, PRNumber: 12}, event: ev},
		{name: "unstarted chunk rejected", build: build, chunk: &model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "running"}, event: ev},
		{name: "failed status rejected", build: build, chunk: &model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "failed", StartedAt: now}, event: ev},
		{name: "thread mismatch rejected", build: build, chunk: &eligible, event: pubsub.PRLifecycleEvent{TargetID: "other", Repo: "owner/repo", PRNumber: 12}},
		{name: "eligible chunk accepted", build: build, chunk: &eligible, event: ev, want: true},
		{
			name:  "branch match running action chunk accepted",
			build: build,
			chunk: &model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "running", Repo: "owner/repo"},
			event: pubsub.PRLifecycleEvent{TargetID: "th-1", Repo: "owner/repo", Branch: "feat/repo-pr-vanadis-c1-123456", PRNumber: 12},
			want:  true,
		},
		{
			name:  "branch match non-running action chunk rejected",
			build: build,
			chunk: &model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "action", Status: "failed", Repo: "owner/repo"},
			event: pubsub.PRLifecycleEvent{TargetID: "th-1", Repo: "owner/repo", Branch: "feat/repo-pr-vanadis-c1-123456", PRNumber: 12},
			want:  false,
		},
		{
			name:  "branch match first worker failure pr chunk accepted",
			build: build,
			chunk: &model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "failed", CIFailCount: 0, StartedAt: now, Repo: "owner/repo"},
			event: pubsub.PRLifecycleEvent{TargetID: "th-1", Repo: "owner/repo", Branch: "feat/repo-pr-vanadis-c1-123456", PRNumber: 12},
			want:  true,
		},
		{
			name:  "branch match ci failure chunk rejected",
			build: build,
			chunk: &model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "failed", CIFailCount: 1, StartedAt: now, Repo: "owner/repo"},
			event: pubsub.PRLifecycleEvent{TargetID: "th-1", Repo: "owner/repo", Branch: "feat/repo-pr-vanadis-c1-123456", PRNumber: 12},
			want:  false,
		},
		{
			name:  "branch match unstarted pr chunk rejected",
			build: build,
			chunk: &model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "pr", Status: "running", Repo: "owner/repo"},
			event: pubsub.PRLifecycleEvent{TargetID: "th-1", Repo: "owner/repo", Branch: "feat/repo-pr-vanadis-c1-123456", PRNumber: 12},
			want:  false,
		},
		{
			name:  "branch match non-pr mode rejected",
			build: build,
			chunk: &model.VanadisChunk{ID: "c1", BuildID: "b1", Mode: "other", Status: "running", StartedAt: now, Repo: "owner/repo"},
			event: pubsub.PRLifecycleEvent{TargetID: "th-1", Repo: "owner/repo", Branch: "feat/repo-pr-vanadis-c1-123456", PRNumber: 12},
			want:  false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, IsEligibleUnattachedChunk("example-owner", tc.build, tc.chunk, tc.event))
		})
	}
}

func TestFlow_TryAttachOpenPRForChunk(t *testing.T) {
	t.Parallel()
	bld := &model.VanadisBuild{ID: "b1", ThreadID: "th-1", Status: "running"}
	chunk := model.VanadisChunk{ID: "c1", BuildID: "b1", Repo: "aerial", Status: "running"}
	chunks := []model.VanadisChunk{chunk}

	matchingPRList := `[{"number":42,"title":"PR Title","head":{"ref":"feat/aerial-pr-vanadis-c1-123456"}}]`
	otherPRList := `[{"number":43,"title":"PR Other","head":{"ref":"feat/aerial-pr-vanadis-c2-123456"}}]`
	validPRDetail := `{"state":"open","merged":false,"body":"desc","node_id":"n1","author_association":"MEMBER","user":{"login":"example-owner"},"head":{"sha":"abc","ref":"feat/aerial-pr-vanadis-c1-123456","repo":{"fork":false,"full_name":"example-owner/aerial"}},"base":{"repo":{"full_name":"example-owner/aerial"}}}`
	untrustedPRDetail := `{"state":"open","merged":false,"body":"desc","node_id":"n1","author_association":"NONE","user":{"login":"stranger"},"head":{"sha":"abc","ref":"feat/aerial-pr-vanadis-c1-123456","repo":{"fork":true,"full_name":"stranger/aerial"}},"base":{"repo":{"full_name":"example-owner/aerial"}}}`

	tests := []struct {
		name         string
		nilFlow      bool
		nilExec      bool
		nilStore     bool
		nilBuild     bool
		apiStatus    int
		apiBody      string
		detailStatus int
		detailBody   string
		attachErr    error
		wantAttached []string
		wantOK       bool
		wantErr      bool
	}{
		{name: "nil flow returns false", nilFlow: true},
		{name: "nil executor returns false", nilExec: true},
		{name: "nil store returns false", nilStore: true},
		{name: "nil build returns false", nilBuild: true},
		{name: "api error propagates", apiStatus: http.StatusInternalServerError, apiBody: "{}", wantErr: true},
		{name: "no pr matches branch returns false", apiStatus: http.StatusOK, apiBody: otherPRList, wantOK: false},
		{name: "matching pr attaches and syncs widget", apiStatus: http.StatusOK, apiBody: matchingPRList, detailStatus: http.StatusOK, detailBody: validPRDetail, wantAttached: []string{"c1"}, wantOK: true},
		{name: "matching branch but untrusted author skips attachment", apiStatus: http.StatusOK, apiBody: matchingPRList, detailStatus: http.StatusOK, detailBody: untrustedPRDetail, wantOK: false},
		{name: "matching branch detail error propagates", apiStatus: http.StatusOK, apiBody: matchingPRList, detailStatus: http.StatusInternalServerError, detailBody: `{}`, wantErr: true},
		{name: "attach conflict returns false without error", apiStatus: http.StatusOK, apiBody: matchingPRList, detailStatus: http.StatusOK, detailBody: validPRDetail, attachErr: store.ErrBuildConflict, wantOK: false},
		{name: "attach chunk not found returns false without error", apiStatus: http.StatusOK, apiBody: matchingPRList, detailStatus: http.StatusOK, detailBody: validPRDetail, attachErr: store.ErrChunkNotFound, wantOK: false},
		{name: "attach error propagates", apiStatus: http.StatusOK, apiBody: matchingPRList, detailStatus: http.StatusOK, detailBody: validPRDetail, attachErr: errors.New("attach failed"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/repos/example-owner/aerial/pulls":
					w.WriteHeader(tc.apiStatus)
					_, err := w.Write([]byte(tc.apiBody))
					assert.NoError(t, err)
				case "/repos/example-owner/aerial/pulls/42":
					status := tc.detailStatus
					if status == 0 {
						status = http.StatusOK
					}
					body := tc.detailBody
					if body == "" {
						body = validPRDetail
					}
					w.WriteHeader(status)
					_, err := w.Write([]byte(body))
					assert.NoError(t, err)
				default:
					w.WriteHeader(http.StatusOK)
					_, err := w.Write([]byte(`{}`))
					assert.NoError(t, err)
				}
			}))
			t.Cleanup(api.Close)

			eri := &eritest.Client{}
			var exec *executor.Executor
			if !tc.nilExec {
				exec = executor.NewExecutor(t.TempDir(), eri, nil, "token", "example-owner", api.Client())
				exec.GitHubAPIURL = api.URL
			}
			spy := &checkStatusSpyStore{chunks: chunks, attachErr: tc.attachErr}
			var st store.VanadisStore = spy
			if tc.nilStore {
				st = nil
			}
			f := NewFlow(st, exec)
			if tc.nilFlow {
				f = nil
			}
			b := bld
			if tc.nilBuild {
				b = nil
			}
			ok, err := f.TryAttachOpenPRForChunk(context.Background(), b, chunks, chunk)
			assert.Equal(t, tc.wantErr, err != nil)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantAttached, spy.attached)
		})
	}
}

func TestChunkFailureNotice(t *testing.T) {
	t.Parallel()
	chunks := []model.VanadisChunk{{ID: "c1", ChunkIndex: 0}, {ID: "c2", ChunkIndex: 1}}
	tests := []struct {
		name, chunkID, want string
	}{
		{"indexed chunk", "c2", "Chunk 2 failed: boom (other chunks continue)"},
		{"unknown chunk", "c9", "Chunk failed: boom (other chunks continue)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, chunkFailureNotice(chunks, tc.chunkID, "boom"))
		})
	}
}

func TestFlow_EnsureOpenedPRAutoMerge(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		buildID       string
		nilBuild      bool
		chunkID       string
		prNumber      int
		apiError      bool
		wantPatched   bool
		wantAutomerge bool
	}{
		{
			name:          "missing build returns early",
			buildID:       "b1",
			nilBuild:      true,
			chunkID:       "c1",
			prNumber:      10,
			wantPatched:   false,
			wantAutomerge: false,
		},
		{
			name:          "missing chunk returns early",
			buildID:       "b1",
			chunkID:       "missing",
			prNumber:      10,
			wantPatched:   false,
			wantAutomerge: false,
		},
		{
			name:          "matching build and chunk triggers patch and automerge",
			buildID:       "b1",
			chunkID:       "c1",
			prNumber:      10,
			wantPatched:   true,
			wantAutomerge: true,
		},
		{
			name:          "api error swallowed with warning",
			buildID:       "b1",
			chunkID:       "c1",
			prNumber:      10,
			apiError:      true,
			wantPatched:   false,
			wantAutomerge: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var patched bool
			var automerged bool
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.apiError {
					w.WriteHeader(http.StatusInternalServerError)
					_, _ = w.Write([]byte(`{}`))
					return
				}
				switch {
				case r.Method == http.MethodGet:
					w.WriteHeader(http.StatusOK)
					_, err := w.Write([]byte(`{"state":"open","merged":false,"body":"desc","node_id":"n1","head":{"sha":"abc","ref":"feat/branch","repo":{"fork":false,"full_name":"owner/repo"}},"base":{"repo":{"full_name":"owner/repo"}}}`))
					assert.NoError(t, err)
				case r.Method == http.MethodPatch:
					patched = true
					w.WriteHeader(http.StatusOK)
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/graphql"):
					automerged = true
					w.WriteHeader(http.StatusOK)
					_, err := w.Write([]byte(`{"data":{"enablePullRequestAutoMerge":{}}}`))
					assert.NoError(t, err)
				default:
					w.WriteHeader(http.StatusOK)
				}
			}))
			defer api.Close()

			chunks := []model.VanadisChunk{{ID: "c1", Repo: "owner/repo"}}
			spy := &checkStatusSpyStore{chunks: chunks, nilBuild: tc.nilBuild}
			eri := &eritest.Client{}
			exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", "example-owner", api.Client())
			exec.GitHubAPIURL = api.URL

			f := NewFlow(spy, exec)
			f.ensureOpenedPRAutoMerge(context.Background(), tc.buildID, tc.chunkID, tc.prNumber)
			assert.Equal(t, tc.wantPatched, patched)
			assert.Equal(t, tc.wantAutomerge, automerged)
		})
	}
}

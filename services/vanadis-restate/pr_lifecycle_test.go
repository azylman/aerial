package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/azylman/aerial/pkg/pubsub"
)

type checkStatusSpyStore struct {
	VanadisStore

	lifecycleStatus string
	getBuildErr     error
	chunks          []VanadisChunk
	calls           []string
	resets          []string
	failed          []string
	banners         []string
	failCountCalls  []string
	failCount       int
	failCountErr    error
	err             error
	resetErr        error
	statusErr       error
	bannerErr       error
	failErr         error
	skipFailApply   bool
}

func (s *checkStatusSpyStore) chunk(chunkID string) *VanadisChunk {
	for i := range s.chunks {
		if s.chunks[i].ID == chunkID {
			return &s.chunks[i]
		}
	}
	s.chunks = append(s.chunks, VanadisChunk{ID: chunkID, BuildID: "b1", Effort: effortMedium, CIFailCount: s.failCount})
	return &s.chunks[len(s.chunks)-1]
}

func (s *checkStatusSpyStore) IncrementChunkCIFailCount(_ context.Context, chunkID, banner string) (int, error) {
	s.failCountCalls = append(s.failCountCalls, chunkID+":"+banner)
	if s.failCountErr != nil {
		return 0, s.failCountErr
	}
	s.failCount++
	c := s.chunk(chunkID)
	c.CIFailCount = s.failCount
	if c.HeadSHA != "" {
		c.RemediatedSHA = c.HeadSHA
	}
	return s.failCount, nil
}

func (s *checkStatusSpyStore) GetBuild(_ context.Context, buildID string) (*VanadisBuild, []VanadisChunk, error) {
	if s.getBuildErr != nil {
		return nil, nil, s.getBuildErr
	}
	chunks := s.chunks
	if len(chunks) == 0 {
		chunks = []VanadisChunk{{ID: "c1", BuildID: buildID, Effort: effortMedium, CIFailCount: s.failCount}}
	}
	return &VanadisBuild{ID: buildID, Status: statusRunning, ThreadID: "th-1"}, chunks, nil
}

func (s *checkStatusSpyStore) RecordPRCheckStatus(_ context.Context, chunkID, headSHA, checkName, checkState string) error {
	s.calls = append(s.calls, chunkID+":"+headSHA+":"+checkName+":"+checkState)
	if s.err != nil {
		return s.err
	}
	c := s.chunk(chunkID)
	if sha := strings.TrimSpace(headSHA); sha != "" && c.HeadSHA == "" && c.MergeSHA == "" && c.Status != statusMerged && c.Status != statusSynced && c.Status != statusDeployFailed {
		c.HeadSHA = sha
	}
	if c.CIChecks == nil {
		c.CIChecks = make(map[string]string)
	}
	c.CIChecks[checkName] = checkState
	return nil
}

func (s *checkStatusSpyStore) ResetPRCheckState(_ context.Context, chunkID, headSHA, mergeSHA string) error {
	s.resets = append(s.resets, chunkID+":"+headSHA+":"+mergeSHA)
	return s.resetErr
}

func (s *checkStatusSpyStore) UpdatePRLifecycleStatus(_ context.Context, _ string, status string) error {
	s.lifecycleStatus = status
	return s.statusErr
}

func TestBuildObject_MergeRevisionAfterLifecycle(t *testing.T) {
	t.Parallel()
	unavailable := errors.New("reset unavailable")
	tests := []struct {
		name, mergeSHA    string
		resetErr, wantErr error
	}{
		{"missing optional merge SHA", "", errPRTransitionSkipped, nil},
		{"rejected merge reset reloads lifecycle", "merge-event", errPRTransitionSkipped, errPRTransitionSkipped},
		{"failed merge reset retries", "merge-event", unavailable, unavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &checkStatusSpyStore{resetErr: tc.resetErr, chunks: []VanadisChunk{{ID: "c1", BuildID: "b1", Status: "merged", HeadSHA: "head"}}}
			bo := NewBuildObject("", nil).WithAuthority(store, nil)
			bo.setChunkRevision("c1", prRevisionState{headSHA: "head"})
			ctx := mockRunContext{Context: context.Background()}
			err := bo.handlePREventType(ctx, "b1", "c1", "merged", pubsub.PRLifecycleEvent{HeadSHA: "head", MergeSHA: tc.mergeSHA})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("merge error=%v want=%v", err, tc.wantErr)
			}
			if err := bo.handlePREventType(ctx, "b1", "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "merge-actual", CheckName: "build", CheckState: "passed"}); err != nil {
				t.Fatalf("merge check rejected: %v", err)
			}
			if err := bo.handlePREventType(ctx, "b1", "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "head", CheckName: "lint", CheckState: "passed"}); !errors.Is(err, errPRTransitionSkipped) {
				t.Fatalf("head check error=%v", err)
			}
		})
	}
}

func TestBuildObject_RejectedCheckFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
	}{{"skipped write", errPRTransitionSkipped}, {"failed write", errors.New("database unavailable")}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &checkStatusSpyStore{err: tc.err, getBuildErr: errors.New("read unavailable")}
			bo := NewBuildObject("", nil).WithAuthority(store, nil)
			err := bo.handlePREventType(mockRunContext{Context: context.Background()}, "b1", "c1", "check_failed", pubsub.PRLifecycleEvent{HeadSHA: "new", CheckName: "lint"})
			if !errors.Is(err, tc.err) || store.lifecycleStatus != "" {
				t.Fatalf("error=%v lifecycle=%s", err, store.lifecycleStatus)
			}
		})
	}
}

func (s *checkStatusSpyStore) FailChunk(_ context.Context, chunkID, reason, _ string) (*VanadisBuild, *VanadisChunk, bool, error) {
	s.failed = append(s.failed, chunkID+":"+reason)
	if s.failErr != nil {
		return nil, nil, false, s.failErr
	}
	return &VanadisBuild{ID: "b1", Status: statusFailed, Objective: "Obj", ThreadID: "th-1"}, &VanadisChunk{ID: chunkID, BuildID: "b1", Status: statusFailed}, !s.skipFailApply, nil
}

func (s *checkStatusSpyStore) UpdateChunkBannerAndStatus(_ context.Context, chunkID, status, banner string) error {
	s.banners = append(s.banners, chunkID+":"+status+":"+banner)
	return s.bannerErr
}

func (*checkStatusSpyStore) UpdateBuildStatusMessageID(_ context.Context, _, _ string) error {
	return nil
}

func (*checkStatusSpyStore) GetChunkByPR(_ context.Context, _ string, _ int) (*VanadisChunkPRLookup, error) {
	return &VanadisChunkPRLookup{ChunkID: "c1", BuildID: "b1", Matched: true}, nil
}

func TestBuildObject_HandlePRCheckEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	spy := &checkStatusSpyStore{}
	var currentHead atomic.Value
	currentHead.Store("sha-2")
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"state":"open","merged":false,"head":{"sha":%q}}`, currentHead.Load())
	}))
	defer api.Close()
	executor := NewExecutor("", nil, nil, "", api.Client())
	executor.gitHubAPIURL = api.URL
	bo := NewBuildObject("", nil).WithAuthority(spy, executor)

	// Empty check name is ignored
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{CheckName: ""}))
	assert.Empty(t, spy.calls)

	// Empty check state on check_updated is ignored
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{CheckName: "test", CheckState: ""}))
	assert.Empty(t, spy.calls)

	// check_updated with state records exact state on initial head sha-1
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test", CheckState: "running"}))
	assert.Equal(t, []string{"c1:sha-1:test:running"}, spy.calls)

	// check_failed defaults empty state to failed on matching head sha-1
	spy.calls = nil
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_failed", pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test"}))
	assert.Equal(t, []string{"c1:sha-1:test:failed"}, spy.calls)

	// synchronize advances tracked revision to sha-2 and resets check state
	t2 := time.Date(2026, 11, 16, 23, 0, 0, 0, time.UTC)
	assert.NoError(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-2", OccurredAt: t2}))
	assert.Equal(t, []string{"c1:sha-2:"}, spy.resets)

	// delayed synchronize for superseded sha-1 or older timestamp is rejected without mutating revision
	spy.resets = nil
	assert.ErrorIs(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-1"}), errPRTransitionSkipped)
	assert.ErrorIs(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-0", OccurredAt: t2.Add(-time.Minute)}), errPRTransitionSkipped)
	assert.Empty(t, spy.resets)

	// delayed check_updated and check_failed from old head sha-1 are rejected
	spy.calls = nil
	assert.ErrorIs(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test", CheckState: "pending"}), errPRTransitionSkipped)
	assert.ErrorIs(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "ci_failed", pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test"}), errPRTransitionSkipped)
	assert.Empty(t, spy.calls)

	// check_updated for new head sha-2 succeeds
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "sha-2", CheckName: "test", CheckState: "passed"}))
	assert.Equal(t, []string{"c1:sha-2:test:passed"}, spy.calls)

	// legitimate newer force-push returning branch to previous head sha-1 (with newer OccurredAt) is accepted
	spy.resets = nil
	spy.calls = nil
	t3 := t2.Add(time.Minute)
	currentHead.Store("sha-1")
	assert.NoError(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-1", OccurredAt: t3}))
	assert.Equal(t, []string{"c1:sha-1:"}, spy.resets)
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test", CheckState: "passed"}))
	assert.Equal(t, []string{"c1:sha-1:test:passed"}, spy.calls)

	// advance back to sha-2 with newer timestamp t4
	spy.resets = nil
	t4 := t3.Add(time.Minute)
	currentHead.Store("sha-2")
	assert.NoError(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-2", OccurredAt: t4}))
	assert.Equal(t, []string{"c1:sha-2:"}, spy.resets)

	// untimestamped synchronize on c2 clears headAt so delayed sha-a with older timestamp is rejected
	currentHead.Store("sha-a")
	assert.NoError(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c2", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-a", OccurredAt: t2}))
	currentHead.Store("sha-b")
	assert.NoError(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c2", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-b"}))
	assert.ErrorIs(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c2", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-a", OccurredAt: t3}), errPRTransitionSkipped)
	assert.True(t, prHeadAtFromContext(nil).IsZero())
	assert.True(t, prHeadAtFromContext(context.Background()).IsZero())
	assert.Nil(t, withPRHeadAt(nil, time.Time{}))
	assert.Equal(t, t3.UTC(), prHeadAtFromContext(withPRHeadAt(nil, t3)))
	var parsedChunk VanadisChunk
	applyRawChunkCIChecks(&parsedChunk, []byte(`{"_head_sha":"sha-1","_merge_sha":"m-1","_remediated_sha":"sha-1","_head_at":"1700000000000000000","_seen_heads":"sha-0,sha-1","lint":"passed"}`))
	assert.Equal(t, "sha-1", parsedChunk.HeadSHA)
	assert.Equal(t, "sha-1", parsedChunk.RemediatedSHA)
	assert.Equal(t, []string{"sha-0", "sha-1"}, parsedChunk.SeenHeads)
	assert.False(t, parsedChunk.HeadAt.IsZero())
	assert.Equal(t, map[string]string{"lint": "passed"}, parsedChunk.CIChecks)
	spy.resets = nil

	// failed or skipped ResetPRCheckState on synchronize does not overwrite cached revision
	currentHead.Store("sha-unapplied")
	spy.resetErr = errPRTransitionSkipped
	assert.ErrorIs(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-unapplied"}), errPRTransitionSkipped)
	spy.resetErr = nil
	spy.calls = nil
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "sha-2", CheckName: "test", CheckState: "passed"}))
	assert.Equal(t, []string{"c1:sha-2:test:passed"}, spy.calls)

	// merged resets check state for merge commit and rejects delayed pre-merge head sha-2 synchronize and checks while accepting merge-sha-3 checks
	spy.calls = nil
	spy.resets = nil
	assert.NoError(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "merged", pubsub.PRLifecycleEvent{HeadSHA: "sha-2", MergeSHA: "merge-sha-3"}))
	assert.Equal(t, []string{"c1:sha-2:merge-sha-3"}, spy.resets)
	assert.ErrorIs(t, bo.handlePREventType(mockRunContext{Context: ctx}, "b1", "c1", "synchronize", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12, HeadSHA: "sha-4"}), errPRTransitionSkipped)
	assert.ErrorIs(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "sha-2", CheckName: "test", CheckState: "pending"}), errPRTransitionSkipped)
	assert.Empty(t, spy.calls)
	assert.NoError(t, bo.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "merge-sha-3", CheckName: "Build & Push Images (eri)", CheckState: "passed"}))
	assert.Equal(t, []string{"c1:merge-sha-3:Build & Push Images (eri):passed"}, spy.calls)

	// store error on recordPRMergeRevision prevents cached revision overwrite; store error on check_updated propagates
	spyErr := &checkStatusSpyStore{err: errors.New("db error"), resetErr: errors.New("reset err")}
	boErr := NewBuildObject("", nil).WithAuthority(spyErr, nil)
	assert.Error(t, boErr.recordPRMergeRevision(ctx, "b1", "c1", "sha-2", "merge-sha-3"))
	assert.Error(t, boErr.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "merge-sha-3", CheckName: "test", CheckState: "passed"}))
	assert.Equal(t, []string{"c1:merge-sha-3:test:passed"}, spyErr.calls)

	// restart after deploy_failed reconstructs merged revision state and rejects delayed pre-merge checks
	spyRestart := &checkStatusSpyStore{
		chunks: []VanadisChunk{
			{ID: "c-df-1", BuildID: "b1", Status: statusDeployFailed, HeadSHA: "sha-pre", MergeSHA: "sha-merge", RemediatedSHA: "sha-pre", CIChecks: map[string]string{"lint": "failed"}},
			{ID: "c-df-2", BuildID: "b1", Status: statusDeployFailed, HeadSHA: "sha-pre"},
		},
	}
	boRestart := NewBuildObject("", nil).WithAuthority(spyRestart, nil)
	assert.ErrorIs(t, boRestart.handlePREventType(nil, "b1", "c-df-1", "ci_failed", pubsub.PRLifecycleEvent{HeadSHA: "sha-pre", CheckName: "test"}), errPRTransitionSkipped)
	assert.ErrorIs(t, boRestart.handlePREventType(nil, "b1", "c-df-2", "ci_failed", pubsub.PRLifecycleEvent{HeadSHA: "sha-pre", CheckName: "test"}), errPRTransitionSkipped)
	assert.NoError(t, boRestart.handlePREventType(nil, "b1", "c-df-2", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "sha-post-merge", CheckName: "test", CheckState: "passed"}))
	assert.Equal(t, []string{"c-df-2:sha-post-merge:test:passed"}, spyRestart.calls)
	// Rejected post-merge check writes must not pin an unaccepted merge SHA.
	mergedStore := &checkStatusSpyStore{err: errPRTransitionSkipped}
	mergedObject := NewBuildObject("", nil).WithAuthority(mergedStore, nil)
	mergedObject.setChunkRevision("c1", prRevisionState{headSHA: "head", merged: true})
	assert.ErrorIs(t, mergedObject.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "rejected-merge", CheckName: "build", CheckState: "passed"}), errPRTransitionSkipped)
	mergedStore.err = nil
	assert.NoError(t, mergedObject.handlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "accepted-merge", CheckName: "build", CheckState: "passed"}))
}

type mockBusError struct {
	pubsub.Bus

	err error
}

func (m *mockBusError) Publish(context.Context, string, string, any) error {
	return m.err
}

func TestBuildObject_ExecutorErrorBranches(t *testing.T) {
	t.Parallel()
	b1 := VanadisBuild{ID: "b-err", Status: "running", ThreadID: "th-1"}
	c1 := VanadisChunk{ID: "c1", BuildID: "b-err", ChunkIndex: 0, Title: "C1", Status: "running", Mode: "action"}
	store, _ := dbScript{
		buildRow: &mockRow{b: &b1},
		chunks:   []VanadisChunk{c1},
		execTag:  "UPDATE 1",
	}.open()
	eriErr := &mockEriClient{
		cancelFn:  func(context.Context, string) error { return errors.New("cancel err") },
		promptFn:  func(context.Context, string, string) error { return errors.New("prompt err") },
		widgetFn:  func(context.Context, StatusWidgetRequest) (string, error) { return "", errors.New("widget err") },
		messageFn: func(context.Context, string, string) error { return errors.New("msg err") },
	}
	mockBusErr := &mockBusError{err: errors.New("pub err")}
	execErr := NewExecutor(t.TempDir(), eriErr, mockBusErr, "token", nil)
	boErr := NewBuildObject("", nil).WithAuthority(store, execErr)

	// notifyBuildTerminal error branches
	boErr.notifyBuildTerminal(context.Background(), &b1, []VanadisChunk{c1}, "failed", "chunk failed", "TITLE", "MSG")

	// notifyChunkFailure error branches
	ctx := makeObjectMockContext(t, "b-err")
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		boErr.notifyChunkFailure(rc, b1, "c1", "reason")
		return true, nil
	})

	// dispatchThreadWakePrompt error branches
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		boErr.dispatchThreadWakePrompt(rc, "b-err", "prompt")
		return true, nil
	})

	// persistBuildCreation error branches
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		_, _ = boErr.persistBuildCreation(rc, b1, []VanadisChunk{c1})
		return true, nil
	})

	// handlePREventType error branches
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		boErr.handlePREventType(rc, "b-err", "c1", "check_failed", pubsub.PRLifecycleEvent{Event: "check_failed", CheckName: "unit-tests", HeadSHA: "head1", Repo: "aerial"})
		boErr.handlePREventType(rc, "b-err", "c1", "conflict", pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "head1"})
		boErr.handlePREventType(rc, "b-err", "c1", "merged", pubsub.PRLifecycleEvent{Event: "merged"})
		return true, nil
	})

	storeErr, _ := dbScript{bare: nilDB}.open()
	boStoreErr := NewBuildObject("", nil).WithAuthority(storeErr, execErr)
	for _, act := range []string{"chunk_merged", "chunk_deploy_success", "action_completed", "chunk_failed", "cancelled"} {
		boStoreErr.applyMutation(context.Background(), "b-err", &BuildMutationRequest{Action: act, ChunkID: "c1", Reason: "test"})
	}
	_, _ = boStoreErr.AttachPR(ctx, AttachChunkPRRequest{ChunkID: "c1", Repo: "aerial"})
	_, _ = boErr.AttachPR(ctx, AttachChunkPRRequest{ChunkID: "c1", Repo: "aerial"})
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (bool, error) {
		boStoreErr.dispatchThreadWakePrompt(rc, "b-err", "prompt")
		return true, nil
	})
	_ = boStoreErr.OnPREvent(ctx, pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101, Event: "check_failed", RolloutState: &pubsub.PRRolloutState{}})
}

func TestBuildObject_HandlePREventType_Deployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		event         string
		prEvent       pubsub.PRLifecycleEvent
		bannerErr     error
		failErr       error
		skipFailApply bool
		wantErr       bool
		wantFailed    []string
		wantBanners   []string
		wantPromptIn  string
	}{
		{
			name:         "closed unmerged fails chunk and dispatches failure prompt",
			event:        "closed",
			prEvent:      pubsub.PRLifecycleEvent{Event: "closed", Status: "closed"},
			wantFailed:   []string{"c1:pr_closed_unmerged"},
			wantPromptIn: "[VANADIS BUILD FAILED]",
		},
		{
			name:       "closed with status merged skips fail chunk",
			event:      "closed",
			prEvent:    pubsub.PRLifecycleEvent{Event: "closed", Status: "merged"},
			wantFailed: nil,
		},
		{
			name:         "closed_unmerged event fails chunk and dispatches failure prompt",
			event:        "closed_unmerged",
			prEvent:      pubsub.PRLifecycleEvent{Event: "closed_unmerged"},
			wantFailed:   []string{"c1:pr_closed_unmerged"},
			wantPromptIn: "[VANADIS BUILD FAILED]",
		},
		{
			name:          "closed_unmerged already terminal skips notifications",
			event:         "closed_unmerged",
			prEvent:       pubsub.PRLifecycleEvent{Event: "closed_unmerged"},
			skipFailApply: true,
			wantFailed:    []string{"c1:pr_closed_unmerged"},
		},
		{
			name:       "closed_unmerged fail chunk error propagates",
			event:      "closed_unmerged",
			prEvent:    pubsub.PRLifecycleEvent{Event: "closed_unmerged"},
			failErr:    errors.New("db fail error"),
			wantErr:    true,
			wantFailed: []string{"c1:pr_closed_unmerged"},
		},
		{
			name:         "deploy_failed sets status banner and dispatches wake prompt",
			event:        "deploy_failed",
			prEvent:      pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "eri", Details: "crashloop", PRNumber: 101, Repo: "aerial"},
			wantBanners:  []string{"c1:deploy_failed:Deployment failed for eri: crashloop"},
			wantPromptIn: "Deployment failed for job eri on aerial (PR #101",
		},
		{
			name:         "sync_failed sets status banner and dispatches wake prompt",
			event:        "sync_failed",
			prEvent:      pubsub.PRLifecycleEvent{Event: "sync_failed", Details: "git pull conflict", PRNumber: 102, Repo: "aerial"},
			wantBanners:  []string{"c1:deploy_failed:Git sync failed: git pull conflict"},
			wantPromptIn: "Git sync failed for PR #102 on aerial",
		},
		{
			name:        "deploy_failed banner update error propagates",
			event:       "deploy_failed",
			prEvent:     pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "synapse"},
			bannerErr:   errors.New("db banner error"),
			wantErr:     true,
			wantBanners: []string{"c1:deploy_failed:Deployment failed for synapse"},
		},
		{
			name:        "deploy_failed on completed chunk skips wake prompt",
			event:       "deploy_failed",
			prEvent:     pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "synapse"},
			bannerErr:   errPRTransitionSkipped,
			wantErr:     true,
			wantBanners: []string{"c1:deploy_failed:Deployment failed for synapse"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{bannerErr: tc.bannerErr, failErr: tc.failErr, skipFailApply: tc.skipFailApply}
			eri := &mockEriClient{}
			exec := NewExecutor(t.TempDir(), eri, nil, "token", nil)
			bo := NewBuildObject("", nil).WithAuthority(spy, exec)

			err := bo.handlePREventType(nil, "b1", "c1", tc.event, tc.prEvent)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.wantFailed, spy.failed)
			assert.Equal(t, tc.wantBanners, spy.banners)
			if tc.wantPromptIn != "" {
				assert.Contains(t, eri.lastPrompt, tc.wantPromptIn)
				assert.Equal(t, "th-1", eri.lastThreadID)
			} else {
				assert.Empty(t, eri.lastPrompt)
			}
		})
	}
}

func newRemediationTestHarness(t *testing.T, spy *checkStatusSpyStore, dispatchErr, notifyErr error, isMerged bool) (*BuildObject, *mockEriClient) {
	t.Helper()
	eri := &mockEriClient{}
	if dispatchErr != nil {
		eri.dispatchFn = func(context.Context, BrainDispatchRequest) error { return dispatchErr }
	}
	var bus pubsub.Bus
	if notifyErr != nil {
		eri.cancelFn = func(context.Context, string) error { return notifyErr }
		eri.messageFn = func(context.Context, string, string) error { return notifyErr }
		eri.widgetFn = func(context.Context, StatusWidgetRequest) (string, error) { return "", notifyErr }
		bus = &mockBusError{err: notifyErr}
	}
	exec := NewExecutor(t.TempDir(), eri, bus, "token", nil)
	bo := NewBuildObject("", nil).WithAuthority(spy, exec)
	if isMerged {
		bo.setChunkRevision("c1", prRevisionState{merged: true})
	}
	return bo, eri
}

func assertRemediationDispatch(t *testing.T, eri *mockEriClient, wantDispatch bool, wantDispatchID string) {
	t.Helper()
	if !wantDispatch {
		assert.Nil(t, eri.lastDispatch)
		return
	}
	assert.NotNil(t, eri.lastDispatch)
	assert.Equal(t, "vanadis-c1", eri.lastDispatch.SessionID)
	assert.Equal(t, "c1", eri.lastDispatch.VanadisChunkID)
	assert.True(t, eri.lastDispatch.SkipIfActive)
	if wantDispatchID != "" {
		assert.Equal(t, wantDispatchID, eri.lastDispatch.ID)
	}
}

func ciFailEv(checkName, headSHA string) pubsub.PRLifecycleEvent {
	return pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: checkName, HeadSHA: headSHA, Repo: "aerial", PRNumber: 42, Details: "test failure"}
}

func TestBuildObject_HandlePREventType_Remediation(t *testing.T) {
	t.Parallel()
	dbErr := errors.New("db error")
	sixChecks := []pubsub.PRLifecycleEvent{
		ciFailEv("check-2", "abcdef1234"), ciFailEv("check-3", "abcdef1234"),
		ciFailEv("check-4", "abcdef1234"), ciFailEv("check-5", "abcdef1234"), ciFailEv("check-6", "abcdef1234"),
	}
	sixBanners := []string{
		`c1:ci_failed:CI check "check-1" failed`, `c1:ci_failed:CI check "check-2" failed`,
		`c1:ci_failed:CI check "check-3" failed`, `c1:ci_failed:CI check "check-4" failed`,
		`c1:ci_failed:CI check "check-5" failed`, `c1:ci_failed:CI check "check-6" failed`,
	}

	tests := []struct {
		name               string
		event              string
		prEvent            pubsub.PRLifecycleEvent
		extraEvents        []pubsub.PRLifecycleEvent
		failCount          int
		failCountErr       error
		getBuildErr        error
		bannerErr          error
		failErr            error
		dispatchErr        error
		notifyErr          error
		isMerged           bool
		wantErr            bool
		wantFailed         []string
		wantBanners        []string
		wantFailCountCalls int
		wantDispatch       bool
		wantDispatchID     string
		wantCancel         string
		wantMsgChan        string
	}{
		{name: "ci_failed gate aggregator check is ignored", event: "ci_failed", prEvent: ciFailEv("CI Gate", "sha-1")},
		{name: "ci_failed check with suffix gate is ignored", event: "ci_failed", prEvent: ciFailEv("Build Gate", "sha-1")},
		{name: "ci_failed within cap dispatches background remediation and increments accepted count", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), failCount: 1, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 1, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-abcdef1234-2"},
		{name: "six failing checks on same head coalesce into one accepted remediation attempt", event: "ci_failed", prEvent: ciFailEv("check-1", "abcdef1234"), extraEvents: sixChecks, wantBanners: sixBanners, wantFailCountCalls: 1, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-abcdef1234-1"},
		{name: "failing check after restart coalesces when prior failed check remains active on same head", event: "ci_failed", prEvent: ciFailEv("roborev", "sha-1"), extraEvents: []pubsub.PRLifecycleEvent{ciFailEv("lint", "sha-1"), {Event: "check_updated", CheckName: "roborev", CheckState: "passed", HeadSHA: "sha-1"}, {Event: "restart"}, ciFailEv("unit-tests", "sha-1")}, wantBanners: []string{`c1:ci_failed:CI check "roborev" failed`, `c1:ci_failed:CI check "lint" failed`, `c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 1, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-sha-1-1"},
		{name: "overlapping check failure on fifth attempt head coalesces without cancelling active worker", event: "ci_failed", prEvent: ciFailEv("unit-tests", "sha-5"), extraEvents: []pubsub.PRLifecycleEvent{ciFailEv("lint", "sha-5")}, failCount: 4, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`, `c1:ci_failed:CI check "lint" failed`}, wantFailCountCalls: 1, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-sha-5-5"},
		{name: "check passing after override clears head coalescing for subsequent check failure", event: "ci_failed", prEvent: ciFailEv("roborev", "sha-1"), extraEvents: []pubsub.PRLifecycleEvent{{Event: "check_updated", CheckName: "roborev", CheckState: "passed", HeadSHA: "sha-1"}, ciFailEv("unit-tests", "sha-1")}, wantBanners: []string{`c1:ci_failed:CI check "roborev" failed`, `c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 2, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-sha-1-2"},
		{name: "deferred ci_failed after check passes is ignored without dispatching", event: "ci_failed", prEvent: ciFailEv("roborev", "sha-1"), extraEvents: []pubsub.PRLifecycleEvent{{Event: "check_updated", CheckName: "roborev", CheckState: "passed", HeadSHA: "sha-1"}, {Event: "ci_failed", Status: prEventDeferred, CheckName: "roborev", HeadSHA: "sha-1", Repo: "aerial", PRNumber: 42}}, wantBanners: []string{`c1:ci_failed:CI check "roborev" failed`}, wantFailCountCalls: 1, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-sha-1-1"},
		{name: "ci_failed skipped_active dispatch returns errDispatchActive without consuming remediation attempt budget", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), dispatchErr: errDispatchActive, wantErr: true, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 0, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-abcdef1234-1"},
		{name: "ci_failed dispatch transport error propagates without consuming remediation attempt budget", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), dispatchErr: dbErr, wantErr: true, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 0, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-abcdef1234-1"},
		{name: "ci_failed banner update error propagates", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), bannerErr: dbErr, wantErr: true, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}},
		{name: "ci_failed get build error propagates", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), getBuildErr: dbErr, wantErr: true, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}},
		{name: "ci_failed increment count error after accepted dispatch propagates", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), failCountErr: dbErr, wantErr: true, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 1, wantDispatch: true},
		{name: "ci_failed increment count error at cap propagates", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, failCountErr: dbErr, wantErr: true, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 1},
		{name: "ci_failed exceeding cap marks chunk failed and sends alert message", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, wantFailed: []string{"c1:ci_remediation_cap_reached"}, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 1, wantCancel: "c1", wantMsgChan: "th-1"},
		{name: "ci_failed exceeding cap preserves chunk failure despite notification errors", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, notifyErr: dbErr, wantFailed: []string{"c1:ci_remediation_cap_reached"}, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 1, wantCancel: "c1", wantMsgChan: "th-1"},
		{name: "ci_failed exceeding cap fail chunk error propagates", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, failErr: dbErr, wantErr: true, wantFailed: []string{"c1:ci_remediation_cap_reached"}, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 1},
		{name: "conflict updates chunk banner and dispatches background remediation", event: "conflict", prEvent: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42}, wantBanners: []string{"c1:conflict:Merge conflict detected"}, wantDispatch: true},
		{name: "conflict get build error propagates", event: "conflict", prEvent: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42}, getBuildErr: dbErr, wantErr: true, wantBanners: []string{"c1:conflict:Merge conflict detected"}},
		{name: "conflict skipped_active dispatch returns errDispatchActive", event: "conflict", prEvent: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42}, dispatchErr: errDispatchActive, wantErr: true, wantBanners: []string{"c1:conflict:Merge conflict detected"}, wantDispatch: true, wantDispatchID: "vanadis-conflict-c1-abcdef1"},
		{name: "conflict dispatch transport error propagates", event: "conflict", prEvent: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42}, dispatchErr: dbErr, wantErr: true, wantBanners: []string{"c1:conflict:Merge conflict detected"}, wantDispatch: true, wantDispatchID: "vanadis-conflict-c1-abcdef1"},
		{name: "conflict on merged chunk skipped", event: "conflict", prEvent: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, isMerged: true, wantErr: true},
		{name: "conflict update banner error propagates", event: "conflict", prEvent: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, bannerErr: dbErr, wantErr: true, wantBanners: []string{"c1:conflict:Merge conflict detected"}},
		{name: "merged cancels chunk dispatch and resets check state", event: "merged", prEvent: pubsub.PRLifecycleEvent{Event: "merged", HeadSHA: "head-sha", MergeSHA: "merge-sha"}, wantCancel: "c1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{
				getBuildErr:  tc.getBuildErr,
				bannerErr:    tc.bannerErr,
				failErr:      tc.failErr,
				failCount:    tc.failCount,
				failCountErr: tc.failCountErr,
			}
			bo, eri := newRemediationTestHarness(t, spy, tc.dispatchErr, tc.notifyErr, tc.isMerged)

			err := bo.handlePREventType(nil, "b1", "c1", tc.event, tc.prEvent)
			for _, extra := range tc.extraEvents {
				if err != nil {
					break
				}
				if extra.Event == "restart" {
					bo.revMu.Lock()
					bo.prRevisions = nil
					bo.revMu.Unlock()
					continue
				}
				err = bo.handlePREventType(nil, "b1", "c1", extra.Event, extra)
			}
			assert.Equal(t, tc.wantErr, err != nil)
			assert.Equal(t, tc.wantFailed, spy.failed)
			assert.Equal(t, tc.wantBanners, spy.banners)
			assert.Len(t, spy.failCountCalls, tc.wantFailCountCalls)
			assertRemediationDispatch(t, eri, tc.wantDispatch, tc.wantDispatchID)
			assert.Equal(t, tc.wantCancel, eri.lastCancel)
			assert.Equal(t, tc.wantMsgChan, eri.lastMsgChan)
		})
	}
}

func TestBuildObject_OnPREvent_RemediationDeferralAndRetry(t *testing.T) {
	t.Parallel()
	transportErr := errors.New("eri unavailable")

	tests := []struct {
		name               string
		ev                 pubsub.PRLifecycleEvent
		firstDispatchErr   error
		wantFirstErr       bool
		wantDeferred       bool
		wantFailCountCalls int
		wantDispatchID     string
	}{
		{
			name:               "ci_failed skipped_active defers OnPREvent and dispatches after active worker finishes",
			ev:                 ciFailEv("unit-tests", "abcdef1234"),
			firstDispatchErr:   errDispatchActive,
			wantDeferred:       true,
			wantFailCountCalls: 1,
			wantDispatchID:     "vanadis-c1-cifail-abcdef1234-1",
		},
		{
			name:             "conflict skipped_active defers OnPREvent and dispatches after active worker finishes",
			ev:               pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42},
			firstDispatchErr: errDispatchActive,
			wantDeferred:     true,
			wantDispatchID:   "vanadis-conflict-c1-abcdef1",
		},
		{
			name:             "ci_failed transport error fails OnPREvent without deferring or consuming budget",
			ev:               ciFailEv("unit-tests", "abcdef1234"),
			firstDispatchErr: transportErr,
			wantFirstErr:     true,
		},
		{
			name:             "conflict transport error fails OnPREvent without deferring",
			ev:               pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42},
			firstDispatchErr: transportErr,
			wantFirstErr:     true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{chunks: []VanadisChunk{{ID: "c1", BuildID: "b1", Status: statusRunning, PRNumber: 42, Effort: effortMedium}}}
			bo, eri := newRemediationTestHarness(t, spy, tc.firstDispatchErr, nil, false)

			var deferred []pubsub.PRLifecycleEvent
			mc := mocks.NewMockContext(t)
			mc.EXPECT().Key().Return("b1").Maybe()
			mc.EXPECT().Get("state", mock.Anything).Return(false, nil).Maybe()
			mc.EXPECT().Get("launched", mock.Anything).Return(false, nil).Maybe()
			mc.EXPECT().Run(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(runMockRunContext).Maybe()
			mc.EXPECT().Set("state", mock.Anything, mock.Anything).Return().Maybe()
			mc.EXPECT().Set("launched", mock.Anything, mock.Anything).Return().Maybe()
			mc.EXPECT().ClearAll().Return().Maybe()
			mClient := mocks.NewMockClient(t)
			mInv := mocks.NewMockInvocation(t)
			mc.EXPECT().Object("Build", "b1", "OnPREvent").Return(mClient).Maybe()
			mClient.EXPECT().Send(mock.Anything, mock.Anything).Run(func(input any, _ ...restate.SendOption) {
				if ev, ok := input.(pubsub.PRLifecycleEvent); ok {
					deferred = append(deferred, ev)
				}
			}).Return(mInv).Maybe()
			ctx := restate.WithMockContext(mc)

			err := bo.OnPREvent(ctx, tc.ev)
			assert.Equal(t, tc.wantFirstErr, err != nil)
			assert.Empty(t, spy.failCountCalls)
			assert.Equal(t, tc.wantDeferred, len(deferred) == 1)
			if !tc.wantDeferred {
				return
			}

			eri.dispatchFn = nil
			eri.lastDispatch = nil
			assert.NoError(t, bo.OnPREvent(ctx, deferred[0]))
			assert.Len(t, spy.failCountCalls, tc.wantFailCountCalls)
			assertRemediationDispatch(t, eri, true, tc.wantDispatchID)
		})
	}
}

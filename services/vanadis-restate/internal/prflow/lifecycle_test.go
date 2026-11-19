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

	"github.com/azylman/aerial/pkg/pubsub"
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
	return &model.VanadisBuild{ID: buildID, Status: statusRunning, ThreadID: "th-1"}, chunks, nil
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
	return &model.VanadisBuild{ID: "b1", Status: statusFailed, Objective: "Obj", ThreadID: "th-1"}, &model.VanadisChunk{ID: chunkID, BuildID: "b1", Status: statusFailed}, !s.skipFailApply, nil
}

func (s *checkStatusSpyStore) UpdateChunkBannerAndStatus(_ context.Context, chunkID, status, banner string) error {
	s.banners = append(s.banners, chunkID+":"+status+":"+banner)
	return s.bannerErr
}

func (*checkStatusSpyStore) UpdateBuildStatusMessageID(_ context.Context, _, _ string) error {
	return nil
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

type mockEriClient struct {
	dispatchFn func(ctx context.Context, req executor.BrainDispatchRequest) error
	cancelFn   func(ctx context.Context, chunkID string) error
	promptFn   func(ctx context.Context, threadID, prompt string) error
	widgetFn   func(ctx context.Context, req executor.StatusWidgetRequest) (string, error)
	messageFn  func(ctx context.Context, channelID, content string) error

	lastDispatch *executor.BrainDispatchRequest
	lastCancel   string
	lastThreadID string
	lastPrompt   string
	lastWidget   *executor.StatusWidgetRequest
	lastMsgChan  string
	lastMsgCont  string
}

func (m *mockEriClient) DispatchAccepted(ctx context.Context, req executor.BrainDispatchRequest) error {
	return m.Dispatch(ctx, req)
}

func (m *mockEriClient) Dispatch(ctx context.Context, req executor.BrainDispatchRequest) error {
	m.lastDispatch = &req
	if m.dispatchFn != nil {
		return m.dispatchFn(ctx, req)
	}
	return nil
}

func (m *mockEriClient) CancelDispatch(ctx context.Context, chunkID string) error {
	m.lastCancel = chunkID
	if m.cancelFn != nil {
		return m.cancelFn(ctx, chunkID)
	}
	return nil
}

func (m *mockEriClient) Prompt(ctx context.Context, threadID, prompt string) error {
	m.lastThreadID = threadID
	m.lastPrompt = prompt
	if m.promptFn != nil {
		return m.promptFn(ctx, threadID, prompt)
	}
	return nil
}

func (m *mockEriClient) Widget(ctx context.Context, req executor.StatusWidgetRequest) (string, error) {
	m.lastWidget = &req
	if m.widgetFn != nil {
		return m.widgetFn(ctx, req)
	}
	return "msg-123", nil
}

func (m *mockEriClient) Message(ctx context.Context, channelID, content string) error {
	m.lastMsgChan = channelID
	m.lastMsgCont = content
	if m.messageFn != nil {
		return m.messageFn(ctx, channelID, content)
	}
	return nil
}

type mockBusError struct {
	pubsub.Bus

	err error
}

func (m *mockBusError) Publish(context.Context, string, string, any) error {
	return m.err
}

func TestBuildObject_MergeRevisionAfterLifecycle(t *testing.T) {
	t.Parallel()
	unavailable := errors.New("reset unavailable")
	tests := []struct {
		name, mergeSHA    string
		resetErr, wantErr error
		wantResets        []string
	}{
		{"missing optional merge SHA", "", ErrPRTransitionSkipped, nil, nil},
		{"rejected merge reset reloads lifecycle", "merge-event", ErrPRTransitionSkipped, ErrPRTransitionSkipped, []string{"c1:head:merge-event"}},
		{"failed merge reset retries", "merge-event", unavailable, unavailable, []string{"c1:head:merge-event"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &checkStatusSpyStore{resetErr: tc.resetErr, chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Status: "merged", HeadSHA: "head"}}}
			f := NewFlow(st, nil)
			ctx := context.Background()
			err := f.HandlePREventType(ctx, "b1", "c1", "merged", pubsub.PRLifecycleEvent{HeadSHA: "head", MergeSHA: tc.mergeSHA})
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("merge error=%v want=%v", err, tc.wantErr)
			}
			assert.Equal(t, "merged", st.lifecycleStatus)
			assert.Equal(t, tc.wantResets, st.resets)
		})
	}
}

func TestBuildObject_RejectedCheckFailure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
	}{{"skipped write", ErrPRTransitionSkipped}, {"failed write", errors.New("database unavailable")}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &checkStatusSpyStore{err: tc.err, getBuildErr: errors.New("read unavailable")}
			f := NewFlow(st, nil)
			err := f.HandlePREventType(context.Background(), "b1", "c1", "check_failed", pubsub.PRLifecycleEvent{HeadSHA: "new", CheckName: "lint"})
			if !errors.Is(err, tc.err) || st.lifecycleStatus != "" {
				t.Fatalf("error=%v lifecycle=%s", err, st.lifecycleStatus)
			}
		})
	}
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
		wantSilentIn  string
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
			name:          "closed_unmerged on already failed chunk skips notifications",
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
			name:         "deploy_failed sets status banner and dispatches silent wake",
			event:        "deploy_failed",
			prEvent:      pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "eri", Details: "crashloop", PRNumber: 101, Repo: "aerial"},
			wantBanners:  []string{"c1:deploy_failed:Deployment failed for eri: crashloop"},
			wantSilentIn: "Deployment failed for job eri on aerial (PR #101",
		},
		{
			name:         "sync_failed sets status banner and dispatches silent wake",
			event:        "sync_failed",
			prEvent:      pubsub.PRLifecycleEvent{Event: "sync_failed", Details: "git pull conflict", PRNumber: 102, Repo: "aerial"},
			wantBanners:  []string{"c1:deploy_failed:Git sync failed: git pull conflict"},
			wantSilentIn: "Git sync failed for PR #102 on aerial",
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
			bannerErr:   ErrPRTransitionSkipped,
			wantErr:     true,
			wantBanners: []string{"c1:deploy_failed:Deployment failed for synapse"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{
				bannerErr:     tc.bannerErr,
				failErr:       tc.failErr,
				skipFailApply: tc.skipFailApply,
			}
			eri := &mockEriClient{}
			exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", nil)
			f := NewFlow(spy, exec)

			err := f.HandlePREventType(context.Background(), "b1", "c1", tc.event, tc.prEvent)
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tc.wantFailed, spy.failed)
			assert.Equal(t, tc.wantBanners, spy.banners)
			switch {
			case tc.wantSilentIn != "":
				assert.Empty(t, eri.lastPrompt)
				if assert.NotNil(t, eri.lastDispatch) {
					assert.Contains(t, eri.lastDispatch.Prompt, tc.wantSilentIn)
					assert.Equal(t, "th-1", eri.lastDispatch.TargetID)
					assert.Equal(t, "vanadis-c1", eri.lastDispatch.SessionID)
					assert.True(t, eri.lastDispatch.SkipIfActive)
					assert.NotEmpty(t, eri.lastDispatch.ID)
				}
			case tc.wantPromptIn != "":
				assert.Contains(t, eri.lastPrompt, tc.wantPromptIn)
				assert.Equal(t, "th-1", eri.lastThreadID)
			default:
				assert.Empty(t, eri.lastPrompt)
			}
		})
	}
}

func newRemediationTestHarness(t *testing.T, spy *checkStatusSpyStore, dispatchErr, notifyErr error) (*Flow, *mockEriClient) {
	t.Helper()
	eri := &mockEriClient{}
	var active bool
	var activeDispatch *executor.BrainDispatchRequest
	eri.dispatchFn = func(_ context.Context, req executor.BrainDispatchRequest) error {
		if dispatchErr != nil {
			return dispatchErr
		}
		if req.SkipIfActive && active {
			eri.lastDispatch = activeDispatch
			return executor.ErrDispatchActive
		}
		active = true
		activeDispatch = &req
		return nil
	}
	var bus pubsub.Bus
	if notifyErr != nil {
		eri.cancelFn = func(context.Context, string) error { return notifyErr }
		eri.messageFn = func(context.Context, string, string) error { return notifyErr }
		eri.widgetFn = func(context.Context, executor.StatusWidgetRequest) (string, error) { return "", notifyErr }
		bus = &mockBusError{err: notifyErr}
	}
	exec := executor.NewExecutor(t.TempDir(), eri, bus, "token", nil)
	f := NewFlow(spy, exec)
	return f, eri
}

func assertRemediationDispatch(t *testing.T, eri *mockEriClient, wantDispatch bool, wantDispatchID string, wantSkipIfActive ...bool) {
	t.Helper()
	if !wantDispatch {
		assert.Nil(t, eri.lastDispatch)
		return
	}
	assert.NotNil(t, eri.lastDispatch)
	assert.Equal(t, "vanadis-c1", eri.lastDispatch.SessionID)
	assert.Equal(t, "c1", eri.lastDispatch.VanadisChunkID)
	if len(wantSkipIfActive) > 0 {
		assert.Equal(t, wantSkipIfActive[0], eri.lastDispatch.SkipIfActive)
	}
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
	trueVal := true
	falseVal := false
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
		skipFailApply      bool
		headSHA            string
		remediatedSHA      string
		wantErr            bool
		wantFailed         []string
		wantBanners        []string
		wantFailCountCalls int
		wantDispatch       bool
		wantDispatchID     string
		wantSkipIfActive   *bool
		wantRemediatedSHA  string
		wantCancel         string
		wantMsgChan        string
	}{
		{name: "ci_failed gate aggregator check is ignored", event: "ci_failed", prEvent: ciFailEv("CI Gate", "sha-1")},
		{name: "ci_failed check with suffix gate is ignored", event: "ci_failed", prEvent: ciFailEv("Build Gate", "sha-1")},
		{name: "ci_failed within cap dispatches background remediation and increments accepted count", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), failCount: 1, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 1, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-abcdef1234-2", wantSkipIfActive: &falseVal},
		{name: "six failing checks on same head coalesce into one accepted remediation attempt", event: "ci_failed", prEvent: ciFailEv("check-1", "abcdef1234"), extraEvents: sixChecks, wantBanners: sixBanners, wantFailCountCalls: 1, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-abcdef1234-1", wantSkipIfActive: &falseVal},
		{name: "ci_failed skipped_active dispatch returns nil without consuming remediation attempt budget", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), remediatedSHA: "abcdef1234", dispatchErr: executor.ErrDispatchActive, wantErr: false, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 0, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-abcdef1234-1", wantSkipIfActive: &trueVal},
		{name: "ci_failed dispatch transport error propagates without consuming remediation attempt budget", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), dispatchErr: dbErr, wantErr: true, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 0, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-abcdef1234-1", wantSkipIfActive: &falseVal},
		{name: "ci_failed banner update error propagates", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), bannerErr: dbErr, wantErr: true, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}},
		{name: "ci_failed get build error propagates", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), getBuildErr: dbErr, wantErr: true, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}},
		{name: "ci_failed increment count error after accepted dispatch propagates", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), failCountErr: dbErr, wantErr: true, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 1, wantDispatch: true, wantSkipIfActive: &falseVal},
		{name: "ci_failed increment count error at cap propagates", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, failCountErr: dbErr, wantErr: true, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 1},
		{name: "ci_failed exceeding cap marks chunk failed and sends alert message", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, wantFailed: []string{"c1:ci_remediation_cap_reached"}, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 1, wantCancel: "c1", wantMsgChan: "th-1"},
		{name: "ci_failed exceeding cap preserves chunk failure despite notification errors", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, notifyErr: dbErr, wantFailed: []string{"c1:ci_remediation_cap_reached"}, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 1, wantCancel: "c1", wantMsgChan: "th-1"},
		{name: "ci_failed exceeding cap fail chunk error propagates", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, failErr: dbErr, wantErr: true, wantFailed: []string{"c1:ci_remediation_cap_reached"}, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 1},
		{name: "ci_failed at cap with unapplied fail chunk skips cap without cancelling", event: "ci_failed", prEvent: ciFailEv("unit-tests", "abcdef1234"), failCount: 5, skipFailApply: true, wantFailed: []string{"c1:ci_remediation_cap_reached"}, wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 1},
		{name: "ci_failed on new head_sha dispatches with SkipIfActive false and records RemediatedSHA", event: "ci_failed", prEvent: ciFailEv("unit-tests", "new-head-sha"), remediatedSHA: "old-head-sha", wantBanners: []string{`c1:ci_failed:CI check "unit-tests" failed`}, wantFailCountCalls: 1, wantDispatch: true, wantDispatchID: "vanadis-c1-cifail-new-head-sha-1", wantSkipIfActive: &falseVal, wantRemediatedSHA: "new-head-sha"},
		{name: "conflict updates chunk banner and dispatches background remediation", event: "conflict", prEvent: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42}, wantBanners: []string{"c1:conflict:Merge conflict detected"}, wantDispatch: true, wantSkipIfActive: &trueVal},
		{name: "conflict on superseded head skipped", event: "conflict", prEvent: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "superseded-sha"}, headSHA: "current-sha", wantErr: true},
		{name: "conflict get build error propagates", event: "conflict", prEvent: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42}, getBuildErr: dbErr, wantErr: true},
		{name: "conflict skipped_active dispatch returns nil", event: "conflict", prEvent: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234", Repo: "aerial", PRNumber: 42}, dispatchErr: executor.ErrDispatchActive, wantErr: false, wantBanners: []string{"c1:conflict:Merge conflict detected"}, wantDispatch: true, wantDispatchID: "vanadis-conflict-c1-abcdef1", wantSkipIfActive: &trueVal},
		{name: "conflict dispatch transport error propagates", event: "conflict", prEvent: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, dispatchErr: dbErr, wantErr: true, wantBanners: []string{"c1:conflict:Merge conflict detected"}, wantDispatch: true, wantDispatchID: "vanadis-conflict-c1-abcdef1", wantSkipIfActive: &trueVal},
		{name: "conflict update banner error propagates", event: "conflict", prEvent: pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abcdef1234"}, bannerErr: dbErr, wantErr: true, wantBanners: []string{"c1:conflict:Merge conflict detected"}},
		{name: "merged cancels chunk dispatch and resets check state", event: "merged", prEvent: pubsub.PRLifecycleEvent{Event: "merged", HeadSHA: "head-sha", MergeSHA: "merge-sha"}, wantCancel: "c1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{
				getBuildErr:   tc.getBuildErr,
				bannerErr:     tc.bannerErr,
				failErr:       tc.failErr,
				failCount:     tc.failCount,
				failCountErr:  tc.failCountErr,
				skipFailApply: tc.skipFailApply,
			}
			if tc.headSHA != "" {
				c := spy.chunk("c1")
				c.HeadSHA = tc.headSHA
			}
			if tc.remediatedSHA != "" {
				c := spy.chunk("c1")
				c.RemediatedSHA = tc.remediatedSHA
			}
			f, eri := newRemediationTestHarness(t, spy, tc.dispatchErr, tc.notifyErr)

			err := f.HandlePREventType(context.Background(), "b1", "c1", tc.event, tc.prEvent)
			for _, extra := range tc.extraEvents {
				if err != nil {
					break
				}
				err = f.HandlePREventType(context.Background(), "b1", "c1", extra.Event, extra)
			}
			assert.Equal(t, tc.wantErr, err != nil)
			assert.Equal(t, tc.wantFailed, spy.failed)
			assert.Equal(t, tc.wantBanners, spy.banners)
			assert.Len(t, spy.failCountCalls, tc.wantFailCountCalls)
			if tc.wantSkipIfActive != nil {
				assertRemediationDispatch(t, eri, tc.wantDispatch, tc.wantDispatchID, *tc.wantSkipIfActive)
			} else {
				assertRemediationDispatch(t, eri, tc.wantDispatch, tc.wantDispatchID)
			}
			if tc.wantRemediatedSHA != "" {
				assert.Equal(t, tc.wantRemediatedSHA, spy.chunk("c1").RemediatedSHA)
			}
			assert.Equal(t, tc.wantCancel, eri.lastCancel)
			assert.Equal(t, tc.wantMsgChan, eri.lastMsgChan)
		})
	}
}

func TestFlow_TrailingSameSHAFailureAtCapDoesNotCancelActiveRemediation(t *testing.T) {
	t.Parallel()
	spy := &checkStatusSpyStore{failCount: 4}
	f, eri := newRemediationTestHarness(t, spy, nil, nil)
	ctx := context.Background()

	require.NoError(t, f.HandlePREventType(ctx, "b1", "c1", "ci_failed", ciFailEv("unit-tests", "abcdef1234")))
	require.NotNil(t, eri.lastDispatch)
	require.Equal(t, 1, len(spy.failCountCalls))
	require.Equal(t, 5, spy.chunk("c1").CIFailCount)

	trailing := ciFailEv("unit-tests", "abcdef1234")
	require.NoError(t, f.HandlePREventType(ctx, "b1", "c1", "ci_failed", trailing))

	assert.Equal(t, 1, len(spy.failCountCalls), "trailing same-SHA failure must not increment ci_fail_count to 6")
	assert.Empty(t, spy.failed, "trailing same-SHA failure must not trip the remediation cap")
	assert.Empty(t, eri.lastCancel, "trailing same-SHA failure must not cancel the active remediation turn")
}

func TestBuildObject_HandlePRCheckEvent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	spy := &checkStatusSpyStore{}
	f := NewFlow(spy, nil)

	// Empty check name is ignored
	assert.NoError(t, f.HandlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{CheckName: ""}))
	assert.Empty(t, spy.calls)

	// Empty check state on check_updated is ignored
	assert.NoError(t, f.HandlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{CheckName: "test", CheckState: ""}))
	assert.Empty(t, spy.calls)

	// check_updated with state records exact state
	assert.NoError(t, f.HandlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test", CheckState: "running"}))
	assert.Equal(t, []string{"c1:sha-1:test:running"}, spy.calls)

	// check_failed defaults empty state to failed
	spy.calls = nil
	assert.NoError(t, f.HandlePRCheckEvent(ctx, "c1", "check_failed", pubsub.PRLifecycleEvent{HeadSHA: "sha-1", CheckName: "test"}))
	assert.Equal(t, []string{"c1:sha-1:test:failed"}, spy.calls)

	// store error on recordPRMergeRevision and handlePRCheckEvent propagates
	spyErr := &checkStatusSpyStore{err: errors.New("db error"), resetErr: errors.New("reset err")}
	fErr := NewFlow(spyErr, nil)
	assert.Error(t, fErr.RecordPRMergeRevision(ctx, "c1", "sha-2", "merge-sha-3"))
	assert.Error(t, fErr.HandlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "merge-sha-3", CheckName: "test", CheckState: "passed"}))
	assert.Equal(t, []string{"c1:merge-sha-3:test:passed"}, spyErr.calls)
}

func TestFlow_NoPRTurnAction(t *testing.T) {
	t.Parallel()
	pr := func(status string, prNumber int) model.VanadisChunk {
		return model.VanadisChunk{ID: "c1", Mode: "pr", Status: status, PRNumber: prNumber}
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
		{"failed build ignored", pr("running", 0), "failed", "vanadis-c1-turn-0", NoPRActionNone},
		{"cancelled build ignored", pr("running", 0), "cancelled", "vanadis-c1-nopr-resume-1", NoPRActionNone},
		{"ci remediation dispatch ignored", pr("running", 0), "running", "vanadis-c1-cifail-abc123-1", NoPRActionNone},
		{"conflict remediation dispatch ignored", pr("running", 0), "running", "vanadis-conflict-c1-abc123", NoPRActionNone},
		{"other chunk resume id ignored", pr("running", 0), "running", "vanadis-c2-nopr-resume-1", NoPRActionNone},
		{"action chunk ignored", model.VanadisChunk{ID: "c1", Mode: "action", Status: "running"}, "running", "vanadis-c1-turn-0", NoPRActionNone},
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
			got := SelectUnattachedTargetChunk(tc.build, tc.chunks, tc.ev)
			if tc.want == "" {
				assert.Nil(t, got)
				return
			}
			require.NotNil(t, got)
			assert.Equal(t, tc.want, got.ID)
		})
	}
}

func TestFlow_LocateOrAttachPRChunk(t *testing.T) {
	t.Parallel()
	spy := &checkStatusSpyStore{}
	f := NewFlow(spy, nil)
	got, err := f.LocateOrAttachPRChunk(context.Background(), "b1", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12})
	assert.NoError(t, err)
	assert.Equal(t, "c1", got)

	spyErr := &checkStatusSpyStore{lookupErr: errors.New("read unavailable")}
	fErr := NewFlow(spyErr, nil)
	_, err = fErr.LocateOrAttachPRChunk(context.Background(), "b1", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12})
	assert.Error(t, err)
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
				_, _ = w.Write([]byte(tc.apiBody))
			}))
			defer api.Close()
			eri := &mockEriClient{}
			if tc.widgetErr {
				eri.widgetFn = func(context.Context, executor.StatusWidgetRequest) (string, error) {
					return "", errors.New("widget unavailable")
				}
			}
			exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", nil)
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
			eri := &mockEriClient{}
			if tc.dispatchErr {
				eri.dispatchFn = func(context.Context, executor.BrainDispatchRequest) error {
					return errors.New("dispatch unavailable")
				}
			}
			if tc.widgetErr {
				eri.widgetFn = func(context.Context, executor.StatusWidgetRequest) (string, error) {
					return "", errors.New("widget unavailable")
				}
			}
			exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", nil)
			f := NewFlow(spy, exec)
			err := f.ResumeNoPRChunk(context.Background(), bld, chunks, 0)
			assert.Equal(t, tc.wantErr, err != nil)
			require.NotNil(t, eri.lastDispatch)
			assert.Equal(t, "vanadis-c1-nopr-resume-1", eri.lastDispatch.ID)
			assert.False(t, eri.lastDispatch.SkipIfActive)
		})
	}
}

func TestFlow_SyncAttachedPRWidgetNilExecutor(t *testing.T) {
	t.Parallel()
	f := NewFlow(&checkStatusSpyStore{}, nil)
	assert.NotPanics(t, func() {
		f.SyncAttachedPRWidget(context.Background(), "b1")
	})
}

func TestFlow_NilStoreReturnsNil(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	f := NewFlow(nil, nil)
	assert.NoError(t, f.RecordPRHeadRevision(ctx, "c1", pubsub.PRLifecycleEvent{HeadSHA: ""}))
	assert.NoError(t, f.RecordPRMergeRevision(ctx, "c1", "head", "merge"))
	assert.NoError(t, f.HandlePRCheckEvent(ctx, "c1", "check_updated", pubsub.PRLifecycleEvent{HeadSHA: "head", CheckName: "lint", CheckState: "passed"}))
	assert.NoError(t, f.HandleDeployFailureEvent(ctx, "b1", "c1", pubsub.PRLifecycleEvent{Event: "deploy_failed"}))
}

func TestFlow_HandleClosedPREventNotifyBranches(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		cancelErr   error
		promptErr   error
		widgetErr   error
		busErr      error
		getBuildErr error
		wantErr     bool
	}{
		{name: "cancel error swallowed", cancelErr: errors.New("cancel unavailable")},
		{name: "read error propagates", getBuildErr: errors.New("read unavailable"), wantErr: true},
		{name: "widget error propagates", widgetErr: errors.New("widget unavailable"), busErr: errors.New("bus unavailable"), wantErr: true},
		{name: "wake prompt error propagates", promptErr: errors.New("prompt unavailable"), busErr: errors.New("bus unavailable"), wantErr: true},
		{name: "publish errors swallowed", busErr: errors.New("bus unavailable")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{getBuildErr: tc.getBuildErr}
			eri := &mockEriClient{}
			if tc.cancelErr != nil {
				eri.cancelFn = func(context.Context, string) error { return tc.cancelErr }
			}
			if tc.promptErr != nil {
				eri.promptFn = func(context.Context, string, string) error { return tc.promptErr }
			}
			if tc.widgetErr != nil {
				eri.widgetFn = func(context.Context, executor.StatusWidgetRequest) (string, error) { return "", tc.widgetErr }
			}
			var bus pubsub.Bus
			if tc.busErr != nil {
				bus = &mockBusError{err: tc.busErr}
			}
			exec := executor.NewExecutor(t.TempDir(), eri, bus, "token", nil)
			f := NewFlow(spy, exec)
			err := f.HandleClosedPREvent(context.Background(), "c1", pubsub.PRLifecycleEvent{Event: "closed_unmerged"})
			assert.Equal(t, tc.wantErr, err != nil)
			assert.Equal(t, []string{"c1:pr_closed_unmerged"}, spy.failed)
		})
	}
}

func TestFlow_HandleDeployFailureEventBranches(t *testing.T) {
	t.Parallel()
	dbErr := errors.New("db error")
	tests := []struct {
		name         string
		execNil      bool
		storeNil     bool
		getBuildErr  error
		nilBuild     bool
		promptErr    error
		wantErr      bool
		wantNotFound bool
	}{
		{name: "success dispatches silent wake"},
		{name: "nil executor skips silent wake", execNil: true},
		{name: "nil store skips banner and silent wake", storeNil: true},
		{name: "read error propagates", getBuildErr: dbErr, wantErr: true},
		{name: "missing build reports not found", nilBuild: true, wantErr: true, wantNotFound: true},
		{name: "silent wake error propagates", promptErr: dbErr, wantErr: true},
		{name: "active dispatch treated as handled", promptErr: executor.ErrDispatchActive},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spy := &checkStatusSpyStore{getBuildErr: tc.getBuildErr, nilBuild: tc.nilBuild}
			eri := &mockEriClient{}
			if tc.promptErr != nil {
				eri.dispatchFn = func(context.Context, executor.BrainDispatchRequest) error { return tc.promptErr }
			}
			exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", nil)
			var st store.VanadisStore = spy
			ex := exec
			if tc.storeNil {
				st = nil
			}
			if tc.execNil {
				ex = nil
			}
			f := NewFlow(st, ex)
			err := f.HandleDeployFailureEvent(context.Background(), "b1", "c1", pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "synapse"})
			assert.Equal(t, tc.wantErr, err != nil)
			if tc.wantNotFound {
				assert.ErrorIs(t, err, store.ErrBuildNotFound)
			}
			if tc.execNil || tc.storeNil {
				assert.Empty(t, eri.lastPrompt)
				assert.Nil(t, eri.lastDispatch)
				return
			}
			if tc.wantErr {
				return
			}
			assert.Empty(t, eri.lastPrompt)
			if assert.NotNil(t, eri.lastDispatch) {
				assert.Contains(t, eri.lastDispatch.Prompt, "Deployment failed")
			}
		})
	}
}

func TestFlow_HandleDeployFailureEvent_OccurrenceDistinguishesDispatchID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dispatch := func(occurredAt time.Time) string {
		t.Helper()
		spy := &checkStatusSpyStore{}
		eri := &mockEriClient{}
		f := NewFlow(spy, executor.NewExecutor(t.TempDir(), eri, nil, "token", nil))
		ev := pubsub.PRLifecycleEvent{Event: "deploy_failed", JobName: "synapse", OccurredAt: occurredAt}
		if err := f.HandleDeployFailureEvent(ctx, "b1", "c1", ev); err != nil {
			t.Fatal(err)
		}
		if assert.NotNil(t, eri.lastDispatch) {
			return eri.lastDispatch.ID
		}
		return ""
	}
	at := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	first := dispatch(at)
	if redelivered := dispatch(at); redelivered != first {
		t.Errorf("redelivery ID = %q, want stable %q", redelivered, first)
	}
	if second := dispatch(at.Add(time.Minute)); second == first {
		t.Errorf("distinct occurrence reused dispatch ID %q", second)
	}
}

func TestFlow_NilExecutorSkipsSideEffects(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	spy := &checkStatusSpyStore{}
	f := NewFlow(spy, nil)
	assert.NoError(t, f.HandleConflictEvent(ctx, "b1", "c1", pubsub.PRLifecycleEvent{Event: "conflict", HeadSHA: "abc"}))
	assert.NoError(t, f.HandleCIFailureEvent(ctx, "b1", "c1", "ci_failed", ciFailEv("unit-tests", "abcdef1234")))
	assert.Equal(t, []string{"c1:conflict:Merge conflict detected", `c1:ci_failed:CI check "unit-tests" failed`}, spy.banners)
	assert.Empty(t, spy.failCountCalls)
	assert.NoError(t, NewFlow(nil, nil).HandleConflictEvent(ctx, "b1", "c1", pubsub.PRLifecycleEvent{Event: "conflict"}))
	assert.NoError(t, NewFlow(nil, nil).HandleCIFailureEvent(ctx, "b1", "c1", "ci_failed", ciFailEv("unit-tests", "abcdef1234")))
}

func TestFlow_CIFailureDetailsErrorPropagates(t *testing.T) {
	t.Parallel()
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer api.Close()
	eri := &mockEriClient{}
	exec := executor.NewExecutor(t.TempDir(), eri, nil, "token", nil)
	exec.GitHubAPIURL = api.URL
	spy := &checkStatusSpyStore{}
	f := NewFlow(spy, exec)
	err := f.HandlePREventType(context.Background(), "b1", "c1", "ci_failed", pubsub.PRLifecycleEvent{Event: "ci_failed", CheckName: "roborev", HeadSHA: "abc123", Repo: "aerial", PRNumber: 42, Details: "failure"})
	assert.Error(t, err)
	assert.Equal(t, []string{`c1:ci_failed:CI check "roborev" failed`}, spy.banners)
	assert.Empty(t, spy.failCountCalls)
}

func TestFlow_HandlePREventTypeOpenedAndUnknown(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	spy := &checkStatusSpyStore{}
	f := NewFlow(spy, nil)
	assert.NoError(t, f.HandlePREventType(ctx, "b1", "c1", "opened", pubsub.PRLifecycleEvent{Event: "opened"}))
	assert.Equal(t, []string{"c1::"}, spy.resets)
	assert.NoError(t, f.HandlePREventType(ctx, "b1", "c1", "check_updated", pubsub.PRLifecycleEvent{Event: "check_updated", HeadSHA: "sha-9", CheckName: "lint", CheckState: "passed"}))
	assert.Equal(t, []string{"c1:sha-9:lint:passed"}, spy.calls)
	assert.NoError(t, f.HandlePREventType(ctx, "b1", "c1", "labeled", pubsub.PRLifecycleEvent{Event: "labeled"}))
}

func TestFlow_HandlePREventTypeMergedBranches(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dbErr := errors.New("db error")

	statSpy := &checkStatusSpyStore{statusErr: dbErr, chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Status: "merged", HeadSHA: "head"}}}
	assert.Error(t, NewFlow(statSpy, nil).HandlePREventType(ctx, "b1", "c1", "merged", pubsub.PRLifecycleEvent{HeadSHA: "head", MergeSHA: "merge"}))

	resetSpy := &checkStatusSpyStore{resetErr: dbErr, chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b1", Status: "merged", HeadSHA: "head"}}}
	assert.Error(t, NewFlow(resetSpy, nil).HandlePREventType(ctx, "b1", "c1", "merged", pubsub.PRLifecycleEvent{HeadSHA: "head", MergeSHA: "merge"}))

	notifySpy := &checkStatusSpyStore{}
	notifyErr := errors.New("notify unavailable")
	eri := &mockEriClient{}
	bus := &mockBusError{err: notifyErr}
	eri.cancelFn = func(context.Context, string) error { return notifyErr }
	eri.widgetFn = func(context.Context, executor.StatusWidgetRequest) (string, error) { return "", notifyErr }
	exec := executor.NewExecutor(t.TempDir(), eri, bus, "token", nil)
	assert.NoError(t, NewFlow(notifySpy, exec).HandlePREventType(ctx, "b1", "c1", "merged", pubsub.PRLifecycleEvent{HeadSHA: "head", MergeSHA: "merge"}))
	assert.Equal(t, "merged", notifySpy.lifecycleStatus)
	assert.Equal(t, "c1", eri.lastCancel)
}

func TestFlow_LocateOrAttachPRChunkUnmatched(t *testing.T) {
	t.Parallel()
	spy := &checkStatusSpyStore{lookup: &store.VanadisChunkPRLookup{Matched: false}}
	f := NewFlow(spy, nil)
	got, err := f.LocateOrAttachPRChunk(context.Background(), "b1", pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 12})
	assert.NoError(t, err)
	assert.Empty(t, got)
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
					exec = executor.NewExecutor(t.TempDir(), &mockEriClient{}, nil, "token", nil)
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
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, IsEligibleUnattachedChunk(tc.build, tc.chunk, tc.event))
		})
	}
}

package deploygate

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"slices"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/eriwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/vanadis-restate/internal/executor"
	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
)

// Frozen wire names, keys, and timing. Values stay byte-identical with the
// previous internal/build copies.
func TestDeploygateFrozenWireValues(t *testing.T) {
	t.Parallel()
	require.Equal(t, "deployment_deadlines", DeploymentDeadlineKey)
	require.Equal(t, "deploy_started", DeployStartedEvent)
	require.Equal(t, 15*time.Minute, MaxDeployTime)
}

type fakeRuntime struct {
	key                string
	now                time.Time
	nowErr             error
	attempts           map[string]Attempt
	getErr             error
	scheduled          []DeploymentDeadlineRequest
	delays             []time.Duration
	runStepErr         error
	stepsRun           []string
	reconcileScheduled bool
}

func (r *fakeRuntime) Key() string { return r.key }

func (r *fakeRuntime) GetAttempts() (map[string]Attempt, error) { return r.attempts, r.getErr }

func (r *fakeRuntime) SetAttempts(a map[string]Attempt) { r.attempts = a }

func (r *fakeRuntime) Now() (time.Time, error) { return r.now, r.nowErr }

func (r *fakeRuntime) ScheduleDeadline(req DeploymentDeadlineRequest, d time.Duration) {
	r.scheduled = append(r.scheduled, req)
	if d < 0 {
		d = 0
	}
	r.delays = append(r.delays, d)
}

func (r *fakeRuntime) ScheduleReconcile() { r.reconcileScheduled = true }

func (r *fakeRuntime) RunStep(name string, fn func(ctx context.Context) (StepOutput, error)) (StepOutput, error) {
	r.stepsRun = append(r.stepsRun, name)
	if r.runStepErr != nil {
		return StepOutput{}, r.runStepErr
	}
	return fn(context.Background())
}

type fakeStore struct {
	buildStatus     string
	nilBuild        bool
	getBuildErr     error
	statusErr       error
	bannerErr       error
	rolloutErr      error
	chunks          []model.VanadisChunk
	lifecycleStatus string
}

func (s *fakeStore) chunk(id string) *model.VanadisChunk {
	for i := range s.chunks {
		if s.chunks[i].ID == id {
			return &s.chunks[i]
		}
	}
	s.chunks = append(s.chunks, model.VanadisChunk{ID: id})
	return &s.chunks[len(s.chunks)-1]
}

func (s *fakeStore) GetBuild(_ context.Context, buildID string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	if s.getBuildErr != nil {
		return nil, nil, s.getBuildErr
	}
	if s.nilBuild {
		return nil, nil, nil
	}
	status := s.buildStatus
	if status == "" {
		status = statusRunning
	}
	return &model.VanadisBuild{ID: buildID, Status: status, ThreadID: "th-1"}, append([]model.VanadisChunk(nil), s.chunks...), nil
}

func (s *fakeStore) UpdatePRLifecycleStatus(_ context.Context, id, status string) error {
	s.lifecycleStatus = status
	if s.statusErr != nil {
		return s.statusErr
	}
	s.chunk(id).Status = status
	return nil
}

func (s *fakeStore) UpdateChunkBannerAndStatus(_ context.Context, id, status, banner string) error {
	if s.bannerErr != nil {
		return s.bannerErr
	}
	c := s.chunk(id)
	c.Status = status
	c.Banner = banner
	return nil
}

func (s *fakeStore) RecordPRRolloutState(_ context.Context, id, status string, rollout *pubsub.PRRolloutState) error {
	if s.rolloutErr != nil {
		return s.rolloutErr
	}
	c := s.chunk(id)
	if status != "" {
		c.Status = status
	}
	c.RolloutState = rollout
	return nil
}

// fakeEriClient records DispatchAccepted calls; each call pops the next
// dispatchErrs entry (nil once exhausted).
type fakeEriClient struct {
	dispatchErrs []error
	lastDispatch *eriwire.BrainDispatchRequest
	dispatchIDs  []string
}

func (*fakeEriClient) Dispatch(context.Context, eriwire.BrainDispatchRequest) error {
	return nil
}

func (f *fakeEriClient) DispatchAccepted(_ context.Context, req eriwire.BrainDispatchRequest) error {
	cp := req
	f.lastDispatch = &cp
	f.dispatchIDs = append(f.dispatchIDs, req.ID)
	if len(f.dispatchErrs) == 0 {
		return nil
	}
	err := f.dispatchErrs[0]
	f.dispatchErrs = f.dispatchErrs[1:]
	return err
}

func (*fakeEriClient) CancelDispatch(context.Context, string) error { return nil }

func (*fakeEriClient) Prompt(context.Context, string, string) error { return nil }

func (*fakeEriClient) Widget(context.Context, executor.StatusWidgetRequest) (string, error) {
	return "", nil
}

func (*fakeEriClient) DeleteWidget(context.Context, string, string, string) error { return nil }

func (*fakeEriClient) Message(context.Context, string, string) error { return nil }

var deadlineNow = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// deadlineAttempt is chunk c's attempt for PR 1.
func deadlineAttempt(revision string, generation int, deadline time.Time, delivered bool) Attempt {
	return Attempt{ChunkID: "c", PRNumber: 1, Revision: revision, Generation: generation, Deadline: deadline, Delivered: delivered}
}

func deadlineState(status string, chunks ...model.VanadisChunk) *model.BuildObjectState {
	return &model.BuildObjectState{Build: model.VanadisBuild{Status: status}, Chunks: chunks}
}

func requireDeadlineMap[V any](t *testing.T, want, got map[string]V) {
	t.Helper()
	if len(want) == 0 {
		require.Empty(t, got)
		return
	}
	require.Equal(t, want, got)
}

func requireDeadlineErr(t *testing.T, err error, wantErr string) {
	t.Helper()
	if wantErr == "" {
		require.NoError(t, err)
		return
	}
	require.EqualError(t, err, wantErr)
}

var (
	syncIn15      = deadlineNow.Add(MaxDeployTime)
	syncTwoJobs   = &pubsub.PRRolloutState{StartedJobs: []string{"one", "two"}}
	syncDeploying = model.VanadisChunk{ID: "c", Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying, StartedAt: deadlineNow.Add(-time.Hour), RolloutState: syncTwoJobs}
	syncCompleted = model.VanadisChunk{ID: "completed", PRNumber: 2, MergeSHA: "done", Status: statusSynced}
)

// syncDeadlinesCase is one SyncDeadlines row.
type syncDeadlinesCase struct {
	name            string
	rt              fakeRuntime
	nilRuntime      bool
	store           fakeStore
	state           *model.BuildObjectState
	ev              *pubsub.PRLifecycleEvent
	retry           *RetryRequest
	wantErr         string
	wantAttempts    map[string]Attempt
	wantDelays      []time.Duration
	wantReconcile   bool
	wantStoreStatus string
	wantBuildStatus string
}

// syncDeadlinesCases is package-level only to keep TestSyncDeadlines under funlen.
var syncDeadlinesCases = []syncDeadlinesCase{
	{name: "nil runtime", nilRuntime: true, state: deadlineState("", syncDeploying)},
	{name: "nil state", rt: fakeRuntime{now: deadlineNow}},
	{
		name: "adopts deploying chunk at sync time and skips completed chunks", rt: fakeRuntime{now: deadlineNow}, state: deadlineState("", syncDeploying, syncCompleted),
		wantAttempts: map[string]Attempt{"c": {ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1, Deadline: syncIn15}}, wantDelays: []time.Duration{15 * time.Minute},
	},
	{
		name: "event start backdates the deadline", rt: fakeRuntime{now: deadlineNow}, state: deadlineState("", syncDeploying),
		ev:           &pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", OccurredAt: deadlineNow.Add(-time.Minute)},
		wantAttempts: map[string]Attempt{"c": {ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1, Deadline: deadlineNow.Add(14 * time.Minute)}},
		wantDelays:   []time.Duration{14 * time.Minute},
	},
	{
		name: "future event timestamp starts now", rt: fakeRuntime{now: deadlineNow}, state: deadlineState("", syncDeploying),
		ev:           &pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", OccurredAt: deadlineNow.Add(time.Hour)},
		wantAttempts: map[string]Attempt{"c": {ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1, Deadline: syncIn15}}, wantDelays: []time.Duration{15 * time.Minute},
	},
	{
		name: "repeat sync with more started jobs keeps the attempt",
		rt:   fakeRuntime{now: deadlineNow.Add(5 * time.Minute), attempts: map[string]Attempt{"c": deadlineAttempt("sha", 1, syncIn15, false)}}, state: deadlineState("", syncDeploying),
		wantAttempts: map[string]Attempt{"c": deadlineAttempt("sha", 1, syncIn15, false)},
	},
	{
		name: "same revision explicit retry rearms only the retried chunk",
		rt:   fakeRuntime{now: deadlineNow.Add(5 * time.Minute), attempts: map[string]Attempt{"c": deadlineAttempt("sha", 1, syncIn15, false)}}, state: deadlineState("", syncCompleted, syncDeploying),
		retry:        &RetryRequest{ChunkID: "c", Revision: "sha", Generation: 1},
		wantAttempts: map[string]Attempt{"c": deadlineAttempt("sha", 2, deadlineNow.Add(20*time.Minute), false)}, wantDelays: []time.Duration{15 * time.Minute},
	},
	{
		name: "stale retry generation", rt: fakeRuntime{now: deadlineNow, attempts: map[string]Attempt{"c": deadlineAttempt("sha", 1, syncIn15, false)}}, state: deadlineState("", syncDeploying),
		retry: &RetryRequest{ChunkID: "c", Revision: "sha"}, wantErr: "deployment retry attempt no longer current",
		wantAttempts: map[string]Attempt{"c": deadlineAttempt("sha", 1, syncIn15, false)},
	},
	{
		name: "stale retry revision", rt: fakeRuntime{now: deadlineNow, attempts: map[string]Attempt{"c": deadlineAttempt("sha", 1, syncIn15, false)}}, state: deadlineState("", syncDeploying),
		retry: &RetryRequest{ChunkID: "c", Revision: "old", Generation: 1}, wantErr: "deployment retry attempt no longer current",
		wantAttempts: map[string]Attempt{"c": deadlineAttempt("sha", 1, syncIn15, false)},
	},
	{name: "attempt read fails", rt: fakeRuntime{getErr: errors.New("state read")}, state: deadlineState("", syncDeploying), wantErr: "state read"},
	{name: "clock fails", rt: fakeRuntime{nowErr: errors.New("clock")}, state: deadlineState("", syncDeploying), wantErr: "clock"},
}

func TestSyncDeadlines(t *testing.T) {
	t.Parallel()
	for _, tc := range syncDeadlinesCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Rows are shared package state: clone everything the gate mutates.
			rt, st := tc.rt, tc.store
			rt.key, rt.attempts = "b", maps.Clone(tc.rt.attempts)
			st.chunks = slices.Clone(tc.store.chunks)
			var state *model.BuildObjectState
			if tc.state != nil {
				state = &model.BuildObjectState{Build: tc.state.Build, Chunks: slices.Clone(tc.state.Chunks)}
			}
			var run Runtime = &rt
			if tc.nilRuntime {
				run = nil
			}
			err := NewGate(&st, nil).SyncDeadlines(run, state, tc.ev, tc.retry)
			requireDeadlineErr(t, err, tc.wantErr)
			requireDeadlineMap(t, tc.wantAttempts, rt.attempts)
			require.Equal(t, tc.wantDelays, rt.delays)
			require.Equal(t, tc.wantReconcile, rt.reconcileScheduled)
			if len(st.chunks) > 0 {
				require.Equal(t, tc.wantStoreStatus, st.chunks[0].Status)
			}
			require.Equal(t, tc.wantDelays, rt.delays)
			require.Equal(t, tc.wantReconcile, rt.reconcileScheduled)
			if len(st.chunks) > 0 {
				require.Equal(t, tc.wantStoreStatus, st.chunks[0].Status)
			}
			if state != nil {
				require.Equal(t, tc.wantBuildStatus, state.Build.Status)
			}
		})
	}
}

func TestRetryDeployment(t *testing.T) {
	t.Parallel()
	expired := deadlineAttempt("sha", 1, deadlineNow.Add(-time.Minute), true)
	rearmed := deadlineAttempt("sha", 2, deadlineNow.Add(MaxDeployTime), false)
	failedChunk := model.VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeployFailed, Banner: "expired"}
	tests := []struct {
		name          string
		rt            fakeRuntime
		store         *fakeStore
		req           RetryRequest
		wantErr       string
		wantCode      int
		wantOut       RetryOutcome
		wantAttempt   Attempt
		wantChunk     model.VanadisChunk
		wantReconcile bool
	}{
		{
			name: "same revision retry rearms", store: &fakeStore{chunks: []model.VanadisChunk{failedChunk}}, req: RetryRequest{ChunkID: "c", Revision: "sha", Generation: 1},
			wantOut: RetryOutcome{BuildID: "b", Status: statusRunning}, wantAttempt: rearmed, wantChunk: model.VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying}, wantReconcile: true,
		},
		{
			name: "retry of a deploying chunk resumes after a reset", store: &fakeStore{chunks: []model.VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying}}},
			req: RetryRequest{ChunkID: "c", Revision: "sha", Generation: 1}, wantOut: RetryOutcome{BuildID: "b", Status: statusRunning}, wantAttempt: rearmed,
			wantChunk: model.VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying}, wantReconcile: true,
		},
		{
			name: "failed build retry accepted", store: &fakeStore{buildStatus: statusFailed, chunks: []model.VanadisChunk{failedChunk}}, req: RetryRequest{ChunkID: "c", Revision: "sha", Generation: 1},
			wantOut: RetryOutcome{BuildID: "b", Status: statusRunning}, wantAttempt: rearmed, wantChunk: model.VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying}, wantReconcile: true,
		},
		{
			name: "stale generation", store: &fakeStore{chunks: []model.VanadisChunk{failedChunk}}, req: RetryRequest{ChunkID: "c", Revision: "sha"},
			wantErr: "deployment retry attempt no longer current", wantCode: http.StatusConflict, wantAttempt: expired, wantChunk: failedChunk,
		},
		{
			name: "stale revision", store: &fakeStore{chunks: []model.VanadisChunk{failedChunk}}, req: RetryRequest{ChunkID: "c", Revision: "old", Generation: 1},
			wantErr: "deployment retry attempt no longer current", wantCode: http.StatusConflict, wantAttempt: expired, wantChunk: failedChunk,
		},
		{
			name: "terminal build retry rejected", store: &fakeStore{buildStatus: statusCompleted, chunks: []model.VanadisChunk{failedChunk}}, req: RetryRequest{ChunkID: "c", Revision: "sha", Generation: 1},
			wantErr: "deployment build not running", wantCode: http.StatusConflict, wantAttempt: expired, wantChunk: failedChunk,
		},
		{
			name: "nil build retry rejected", store: &fakeStore{nilBuild: true, chunks: []model.VanadisChunk{failedChunk}}, req: RetryRequest{ChunkID: "c", Revision: "sha", Generation: 1},
			wantErr: "deployment build not running", wantCode: http.StatusConflict, wantAttempt: expired, wantChunk: failedChunk,
		},
		{
			name: "missing chunk retry rejected", store: &fakeStore{chunks: []model.VanadisChunk{failedChunk}}, req: RetryRequest{ChunkID: "missing", Revision: "sha", Generation: 1},
			wantErr: "deployment chunk not retryable", wantCode: http.StatusConflict, wantAttempt: expired, wantChunk: failedChunk,
		},
		{
			name: "unsupported authority", req: RetryRequest{ChunkID: "c", Revision: "sha", Generation: 1},
			wantErr: "deployment retry requires store", wantAttempt: expired,
		},
		{
			name: "snapshot unavailable", store: &fakeStore{getBuildErr: errors.New("snapshot unavailable"), chunks: []model.VanadisChunk{failedChunk}}, req: RetryRequest{ChunkID: "c", Revision: "sha", Generation: 1},
			wantErr: "snapshot unavailable", wantAttempt: expired, wantChunk: failedChunk,
		},
		{
			name: "attempt state unavailable", rt: fakeRuntime{getErr: errors.New("state unavailable")}, store: &fakeStore{chunks: []model.VanadisChunk{failedChunk}},
			req: RetryRequest{ChunkID: "c", Revision: "sha", Generation: 1}, wantErr: "state unavailable", wantAttempt: expired, wantChunk: failedChunk,
		},
		{
			name: "durable reset unavailable", store: &fakeStore{bannerErr: errors.New("write unavailable"), chunks: []model.VanadisChunk{failedChunk}},
			req: RetryRequest{ChunkID: "c", Revision: "sha", Generation: 1}, wantErr: "write unavailable", wantAttempt: expired, wantChunk: failedChunk,
		},
		{
			name: "clock failure after the durable reset", rt: fakeRuntime{nowErr: errors.New("clock unavailable")}, store: &fakeStore{chunks: []model.VanadisChunk{failedChunk}},
			req: RetryRequest{ChunkID: "c", Revision: "sha", Generation: 1}, wantErr: "clock unavailable", wantAttempt: expired,
			wantChunk: model.VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := tc.rt
			rt.key, rt.now = "b", deadlineNow
			rt.attempts = map[string]Attempt{"c": expired}
			var st Store
			if tc.store != nil {
				st = tc.store
			}
			out, err := NewGate(st, nil).RetryDeployment(&rt, tc.req)
			requireDeadlineErr(t, err, tc.wantErr)
			if tc.wantCode != 0 {
				var term restate.TerminalError
				require.ErrorAs(t, err, &term)
				require.Equal(t, tc.wantCode, int(term.Code()))
			}
			require.Equal(t, tc.wantOut, out)
			require.Equal(t, tc.wantAttempt, rt.attempts["c"])
			require.Equal(t, tc.wantReconcile, rt.reconcileScheduled)
			if tc.store != nil {
				require.Equal(t, []model.VanadisChunk{tc.wantChunk}, tc.store.chunks)
			}
		})
	}
}

func TestArmDeploymentStart(t *testing.T) {
	t.Parallel()
	deploying := model.VanadisChunk{ID: "c", Status: statusDeploying, PRNumber: 1, Repo: "owner/repo", MergeSHA: "sha"}
	shaStart := pubsub.PRLifecycleEvent{Repo: "owner/repo", PRNumber: 1, MergeSHA: "sha", OccurredAt: deadlineNow.Add(-time.Minute)}
	tests := []struct {
		name         string
		rt           fakeRuntime
		store        fakeStore
		ev           pubsub.PRLifecycleEvent
		wantErr      string
		wantAttempts map[string]Attempt
		wantDelays   []time.Duration
	}{
		{
			name: "starts the deadline from the event time", store: fakeStore{chunks: []model.VanadisChunk{deploying}}, ev: shaStart,
			wantAttempts: map[string]Attempt{"c": deadlineAttempt("sha", 1, deadlineNow.Add(14*time.Minute), false)}, wantDelays: []time.Duration{14 * time.Minute},
		},
		{name: "snapshot read fails", store: fakeStore{getBuildErr: errors.New("unavailable"), chunks: []model.VanadisChunk{deploying}}, ev: shaStart, wantErr: "unavailable"},
		{name: "snapshot journal fails", rt: fakeRuntime{runStepErr: errors.New("journal")}, store: fakeStore{chunks: []model.VanadisChunk{deploying}}, ev: shaStart, wantErr: "journal"},
		{name: "terminal snapshot skips arming", store: fakeStore{buildStatus: statusCompleted, chunks: []model.VanadisChunk{deploying}}, ev: shaStart},
		{name: "attempt read fails", rt: fakeRuntime{getErr: errors.New("state read")}, store: fakeStore{chunks: []model.VanadisChunk{deploying}}, wantErr: "state read"},
		{name: "clock fails", rt: fakeRuntime{nowErr: errors.New("clock")}, store: fakeStore{chunks: []model.VanadisChunk{deploying}}, wantErr: "clock"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt, st := tc.rt, tc.store
			rt.key = "b"
			if rt.now.IsZero() {
				rt.now = deadlineNow
			}
			err := NewGate(&st, nil).ArmDeploymentStart(&rt, "b", tc.ev)
			requireDeadlineErr(t, err, tc.wantErr)
			requireDeadlineMap(t, tc.wantAttempts, rt.attempts)
			require.Equal(t, tc.wantDelays, rt.delays)
		})
	}
}

func TestDeploymentDeadlineAttempts(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	old := Attempt{ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1, Deadline: now.Add(15 * time.Minute)}
	tests := []struct {
		name           string
		prior          Attempt
		chunk          model.VanadisChunk
		retry          bool
		wantGeneration int
		wantDeadline   time.Time
	}{
		{"first", Attempt{}, model.VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "sha"}, false, 1, now.Add(15 * time.Minute)},
		{"duplicate", old, model.VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "sha"}, false, 1, old.Deadline},
		{"explicit same revision retry", old, model.VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "sha"}, true, 2, now.Add(15 * time.Minute)},
		{"new revision", old, model.VanadisChunk{ID: "c", PRNumber: 1, MergeSHA: "new"}, false, 2, now.Add(15 * time.Minute)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := nextAttempt(tc.prior, tc.chunk, now, tc.retry)
			require.Equal(t, tc.wantGeneration, got.Generation)
			require.Equal(t, tc.wantDeadline, got.Deadline)
		})
	}
}

type mockRateLimitTransport struct {
	resetAt time.Time
}

func (*mockRateLimitTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, nil
}

func (m *mockRateLimitTransport) RateLimitResetAt() time.Time {
	return m.resetAt
}

func TestDeploymentDeadlineLifecycle(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name           string
		status         string
		revision       string
		generation     int
		delivered      bool
		absent         bool
		early          bool
		dispatchErrs   []error
		rateLimitReset time.Time
		wantDelivered  bool
		wantRetry      bool
		wantWake       bool
	}{
		{name: "expiry", status: statusDeploying, revision: "sha", generation: 1, wantDelivered: true, wantWake: true},
		{name: "rate limit delay pauses deadline", status: statusDeploying, revision: "sha", generation: 1, rateLimitReset: now.Add(5 * time.Minute), wantDelivered: false, wantRetry: true, wantWake: false},
		{name: "completion races expiry", status: statusSynced, revision: "sha", generation: 1, wantDelivered: true},
		{name: "superseded revision", status: statusDeploying, revision: "new", generation: 1, wantDelivered: true},
		{name: "stale retry", status: statusDeploying, revision: "sha", generation: 2},
		{name: "already delivered", status: statusDeploying, revision: "sha", generation: 1, delivered: true, wantDelivered: true},
		{name: "terminal clear", status: statusDeploying, revision: "sha", generation: 1, absent: true},
		{name: "early callback", status: statusDeploying, revision: "sha", generation: 1, early: true, wantRetry: true},
		{name: "delivery retry", status: statusDeploying, revision: "sha", generation: 1, dispatchErrs: []error{errors.New("delivery unavailable")}, wantRetry: true, wantWake: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := &fakeRuntime{key: "b", now: now, attempts: map[string]Attempt{}}
			deadline := now
			if tc.early {
				deadline = now.Add(time.Minute)
			}
			if !tc.absent {
				rt.attempts["c"] = Attempt{ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: tc.generation, Deadline: deadline, Delivered: tc.delivered}
			}
			st := &fakeStore{chunks: []model.VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: tc.revision, Status: tc.status, Repo: "owner/repo", RolloutState: &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}}}}
			eri := &fakeEriClient{dispatchErrs: tc.dispatchErrs}
			exec := &executor.Executor{EriClient: eri}
			if !tc.rateLimitReset.IsZero() {
				exec.HTTPClient = &http.Client{Transport: &mockRateLimitTransport{resetAt: tc.rateLimitReset}}
			}
			g := NewGate(st, exec)
			require.NoError(t, g.ExpireDeadline(rt, DeploymentDeadlineRequest{ChunkID: "c", Revision: "sha", Generation: 1}))
			require.Equal(t, tc.wantDelivered, rt.attempts["c"].Delivered)
			require.Equal(t, tc.wantRetry, len(rt.scheduled) > 0)
			require.Equal(t, tc.wantWake, eri.lastDispatch != nil)
			if !tc.rateLimitReset.IsZero() {
				require.Equal(t, tc.rateLimitReset.Add(time.Minute), rt.attempts["c"].Deadline)
			}
			if eri.lastDispatch != nil {
				require.Contains(t, eri.lastDispatch.Prompt, "Readiness remains unconfirmed")
				require.Contains(t, eri.lastDispatch.Prompt, "web")
				require.Equal(t, "vanadis-deploy-deadline-c-1-sha-1", eri.lastDispatch.ID)
				require.Equal(t, "vanadis-restate", eri.lastDispatch.Source)
				require.Equal(t, "vanadis-c", eri.lastDispatch.SessionID)
				require.Equal(t, "c", eri.lastDispatch.VanadisChunkID)
			}
			if len(tc.dispatchErrs) > 0 {
				require.Equal(t, statusDeployFailed, st.chunks[0].Status)
				require.NoError(t, g.ExpireDeadline(rt, DeploymentDeadlineRequest{ChunkID: "c", Revision: "sha", Generation: 1}))
				require.True(t, rt.attempts["c"].Delivered)
				require.Equal(t, []string{"vanadis-deploy-deadline-c-1-sha-1", "vanadis-deploy-deadline-c-1-sha-1"}, eri.dispatchIDs)
			}
		})
	}
}

func TestDeploymentStartValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, status, revision string
		rollout                *pubsub.PRRolloutState
		storeErr, statusErr    error
		buildStatus            string
		mode                   string
		prNumber               int
		nilBuild               bool
		wantValid, wantError   bool
		wantStatus             string
	}{
		{name: "nil rollout start", status: statusMerged, revision: "sha", wantValid: true, wantStatus: statusDeploying},
		{name: "rollout start preserves snapshot", status: statusDeploying, revision: "sha", rollout: &pubsub.PRRolloutState{ExpectedJobs: []string{"web"}}, wantValid: true},
		{name: "stale revision", status: statusDeploying, revision: "old"},
		{name: "completed", status: statusSynced, revision: "sha"},
		{name: "failed chunk preserves failure", status: statusDeployFailed, revision: "sha", wantValid: true},
		{name: "failed pr chunk with pr_number in running build", status: statusFailed, revision: "sha", mode: "pr", prNumber: 101, wantValid: true, wantStatus: statusDeploying},
		{name: "failed pr chunk without pr_number in running build", status: statusFailed, revision: "sha", mode: "pr", prNumber: 0, wantValid: false},
		{name: "failed action chunk in statusFailed", status: statusFailed, revision: "sha", mode: "action", prNumber: 101, wantValid: false},
		{name: "failed pr chunk accepted in failed build", status: statusFailed, revision: "sha", buildStatus: statusFailed, mode: "pr", prNumber: 101, wantValid: true, wantStatus: statusDeploying},
		{name: "nil build", nilBuild: true, revision: "sha"},
		{name: "terminal build", buildStatus: statusCompleted, revision: "sha"},
		{name: "read unavailable", storeErr: errors.New("read unavailable"), wantError: true},
		{name: "status unavailable", status: statusMerged, revision: "sha", statusErr: errors.New("write unavailable"), wantError: true, wantStatus: statusDeploying},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mode := tc.mode
			if mode == "" {
				mode = "pr"
			}
			st := &fakeStore{getBuildErr: tc.storeErr, statusErr: tc.statusErr, buildStatus: tc.buildStatus, nilBuild: tc.nilBuild, chunks: []model.VanadisChunk{{ID: "c", Status: tc.status, Mode: mode, PRNumber: tc.prNumber, MergeSHA: "sha"}}}
			g := NewGate(st, nil)
			valid, _, err := g.RecordDeploymentStart(context.Background(), "b", "c", pubsub.PRLifecycleEvent{MergeSHA: tc.revision, RolloutState: tc.rollout})
			require.Equal(t, tc.wantValid, valid)
			require.Equal(t, tc.wantError, err != nil)
			require.Equal(t, tc.wantStatus, st.lifecycleStatus)
		})
	}
}

func TestDeploymentDeadlineRuntimeFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                    string
		getErr, nowErr, stepErr error
		nilRuntime              bool
	}{
		{"state read", errors.New("state read"), nil, nil, false},
		{"clock", nil, errors.New("clock"), nil, false},
		{"journal", nil, nil, errors.New("journal"), false},
		{"nil runtime", nil, nil, nil, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rt := &fakeRuntime{key: "b"}
			rt.now = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			rt.getErr = tc.getErr
			rt.nowErr = tc.nowErr
			rt.runStepErr = tc.stepErr
			rt.attempts = map[string]Attempt{"c": {ChunkID: "c", PRNumber: 1, Revision: "sha", Generation: 1}}
			var run Runtime
			run = rt
			if tc.nilRuntime {
				run = nil
			}
			require.Error(t, NewGate(nil, nil).ExpireDeadline(run, DeploymentDeadlineRequest{ChunkID: "c", Revision: "sha", Generation: 1}))
			require.False(t, rt.attempts["c"].Delivered)
		})
	}
}

func TestDeploymentDeadlineDeliveryFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		store       *fakeStore
		exec        bool
		buildStatus string
		chunkID     string
		wantError   bool
	}{
		{"store unavailable", nil, true, "", "c", true},
		{"read failed", &fakeStore{getBuildErr: errors.New("read failed")}, true, "", "c", true},
		{"banner failed", &fakeStore{bannerErr: errors.New("banner failed")}, true, "", "c", true},
		{"executor unavailable", &fakeStore{}, false, "", "c", true},
		{"terminal build skips delivery", &fakeStore{}, true, statusCompleted, "c", false},
		{"unknown chunk skips delivery", &fakeStore{}, true, "", "other", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var st Store
			if tc.store != nil {
				tc.store.buildStatus = tc.buildStatus
				tc.store.chunks = []model.VanadisChunk{{ID: "c", PRNumber: 1, MergeSHA: "sha", Status: statusDeploying}}
				st = tc.store
			}
			var exec *executor.Executor
			if tc.exec {
				exec = &executor.Executor{EriClient: &fakeEriClient{}}
			}
			out, err := NewGate(st, exec).DeliverDeadline(context.Background(), "b", Attempt{ChunkID: tc.chunkID, PRNumber: 1, Revision: "sha", Generation: 1})
			require.NoError(t, err)
			require.Equal(t, tc.wantError, out.Error != "")
		})
	}
}

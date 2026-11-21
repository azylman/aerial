package build

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/azylman/aerial/services/vanadis-restate/internal/model"
	"github.com/azylman/aerial/services/vanadis-restate/internal/store"
)

func TestAdvanceDispatchDeferrals(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	tests := []struct {
		name        string
		prior       map[string]model.DispatchDeferral
		deferred    []string
		wantNext    map[string]model.DispatchDeferral
		wantExpired []string
	}{
		{name: "nothing deferred drops prior entries", prior: map[string]model.DispatchDeferral{"c1": {FirstDeferredAt: time.Date(2026, 11, 21, 9, 58, 0, 0, time.UTC), Count: 3}}},
		{name: "first deferral starts lease", deferred: []string{"c1"}, wantNext: map[string]model.DispatchDeferral{"c1": {FirstDeferredAt: now, Count: 1}}},
		{name: "continued deferral inside lease counts up", prior: map[string]model.DispatchDeferral{"c1": {FirstDeferredAt: time.Date(2026, 11, 21, 9, 50, 1, 0, time.UTC), Count: 3}}, deferred: []string{"c1"}, wantNext: map[string]model.DispatchDeferral{"c1": {FirstDeferredAt: time.Date(2026, 11, 21, 9, 50, 1, 0, time.UTC), Count: 4}}},
		{name: "ten minutes of deferral expires", prior: map[string]model.DispatchDeferral{"c1": {FirstDeferredAt: time.Date(2026, 11, 21, 9, 50, 0, 0, time.UTC), Count: 9}}, deferred: []string{"c1"}, wantExpired: []string{"c1"}},
		{name: "zero start restarts lease", prior: map[string]model.DispatchDeferral{"c1": {Count: 7}}, deferred: []string{"c1"}, wantNext: map[string]model.DispatchDeferral{"c1": {FirstDeferredAt: now, Count: 1}}},
		{name: "accepted sibling dropped while other expires", prior: map[string]model.DispatchDeferral{"c1": {FirstDeferredAt: time.Date(2026, 11, 21, 9, 0, 0, 0, time.UTC), Count: 20}, "c2": {FirstDeferredAt: time.Date(2026, 11, 21, 9, 59, 0, 0, time.UTC), Count: 1}}, deferred: []string{"c1", "c3"}, wantNext: map[string]model.DispatchDeferral{"c3": {FirstDeferredAt: now, Count: 1}}, wantExpired: []string{"c1"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			next, expired := advanceDispatchDeferrals(tc.prior, tc.deferred, now)
			assert.Equal(t, tc.wantNext, next)
			assert.Equal(t, tc.wantExpired, expired)
		})
	}
}

func TestDeferralDelay(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		counts []int
		want   time.Duration
	}{
		{name: "no entries", want: time.Second},
		{name: "first deferral", counts: []int{1}, want: time.Second},
		{name: "second deferral doubles", counts: []int{2}, want: 2 * time.Second},
		{name: "third deferral doubles again", counts: []int{3}, want: 4 * time.Second},
		{name: "sixth deferral", counts: []int{6}, want: 32 * time.Second},
		{name: "seventh deferral caps at a minute", counts: []int{7}, want: time.Minute},
		{name: "long deferral stays capped", counts: []int{400}, want: time.Minute},
		{name: "freshest entry sets the pace", counts: []int{5, 1}, want: time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			entries := map[string]model.DispatchDeferral{}
			for i, c := range tc.counts {
				entries[string(rune('a'+i))] = model.DispatchDeferral{Count: c}
			}
			assert.Equal(t, tc.want, deferralDelay(entries))
		})
	}
}

func TestPlanDeferralSeq(t *testing.T) {
	t.Parallel()
	one := map[string]model.DispatchDeferral{"c1": {Count: 1}}
	tests := []struct {
		name           string
		priorSeq, tick int
		deferrals      map[string]model.DispatchDeferral
		want           int
	}{
		{name: "no deferral no chain", want: 0},
		{name: "deferrals cleared end the chain", priorSeq: 3, tick: 3, want: 0},
		{name: "first deferral starts chain", deferrals: one, want: 1},
		{name: "other transition keeps pending chain", priorSeq: 3, deferrals: one, want: 3},
		{name: "stale tick keeps pending chain", priorSeq: 3, tick: 2, deferrals: one, want: 3},
		{name: "own tick advances chain", priorSeq: 3, tick: 3, deferrals: one, want: 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, planDeferralSeq(tc.priorSeq, tc.tick, tc.deferrals))
		})
	}
}

func TestScheduledDeferralReconcile(t *testing.T) {
	t.Parallel()
	active := "dispatch active; reconciliation deferred"
	tests := []struct {
		name       string
		out        reconcileStepOutput
		priorSeq   int
		wantDelay  time.Duration
		wantSeq    int
		wantOK     bool
		wantLegacy bool
	}{
		{name: "legacy deferral output keeps 1s recovery", out: reconcileStepOutput{BuildStatus: "running", Error: active, State: &model.BuildObjectState{}}, wantLegacy: true},
		{name: "legacy deferral output without state", out: reconcileStepOutput{BuildStatus: "running", Error: active}, wantLegacy: true},
		{name: "advanced seq schedules tick", out: reconcileStepOutput{BuildStatus: "running", Error: active, State: &model.BuildObjectState{DispatchDeferrals: map[string]model.DispatchDeferral{"c1": {Count: 3}}, DeferralReconcileSeq: 2}}, priorSeq: 1, wantDelay: 4 * time.Second, wantSeq: 2, wantOK: true},
		{name: "pending chain schedules nothing", out: reconcileStepOutput{BuildStatus: "running", Error: active, State: &model.BuildObjectState{DispatchDeferrals: map[string]model.DispatchDeferral{"c1": {Count: 3}}, DeferralReconcileSeq: 2}}, priorSeq: 2},
		{name: "failed build schedules nothing", out: reconcileStepOutput{BuildStatus: "failed", Error: active, State: &model.BuildObjectState{DispatchDeferrals: map[string]model.DispatchDeferral{"c1": {Count: 1}}, DeferralReconcileSeq: 1}}},
		{name: "no deferral schedules nothing", out: reconcileStepOutput{BuildStatus: "running", State: &model.BuildObjectState{}}},
	}
	bo := NewBuildObject("", nil).WithAuthority(&leaseStore{}, nil)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			delay, seq, ok := scheduledDeferralReconcile(tc.out, tc.priorSeq)
			assert.Equal(t, tc.wantDelay, delay)
			assert.Equal(t, tc.wantSeq, seq)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantLegacy, bo.shouldScheduleReconcile(tc.out))
		})
	}
}

func TestBuildObject_FailNeverAcceptedLaunches(t *testing.T) {
	t.Parallel()
	missing := errors.New("chunk lookup unavailable")
	tests := []struct {
		name         string
		chunks       []model.VanadisChunk
		expired      []string
		getErr       error
		reserveErr   error
		failErr      error
		skipFail     bool
		wantErr      error
		wantReserved []string
		wantFailed   []string
	}{
		{name: "queued pr chunk reserved then failed", chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b", Status: "queued", Mode: "pr"}}, expired: []string{"c1"}, wantReserved: []string{"c1"}, wantFailed: []string{"c1 vanadis-c1-turn-0 RESUMABLE: dispatch never accepted by eri (skipped_active for 10m)"}},
		{name: "unknown key fails nothing", chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b", Status: "pending", Mode: "action"}}, expired: []string{"c9"}},
		{name: "vanished pending chunk still failed", chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b", Status: "pending", Mode: "pr"}}, expired: []string{"c1"}, reserveErr: store.ErrChunkNotFound, wantReserved: []string{"c1"}, wantFailed: []string{"c1 vanadis-c1-turn-0 RESUMABLE: dispatch never accepted by eri (skipped_active for 10m)"}},
		{name: "reservation failure retries step", chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b", Status: "pending", Mode: "action"}}, expired: []string{"c1"}, reserveErr: missing, wantErr: missing, wantReserved: []string{"c1"}},
		{name: "fail write failure retries step", chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b", Status: "running", Mode: "action"}}, expired: []string{"c1"}, failErr: missing, wantErr: missing},
		{name: "superseded generation skip is ignored", chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b", Status: "running", Mode: "action"}}, expired: []string{"c1"}, skipFail: true},
		{name: "build lookup failure retries step", chunks: []model.VanadisChunk{{ID: "c1", BuildID: "b", Status: "running", Mode: "action"}}, expired: []string{"c1"}, getErr: missing, wantErr: missing},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			st := &leaseStore{VanadisStore: &chunkListStore{chunks: tc.chunks, err: tc.getErr}, reserveErr: tc.reserveErr, failErr: tc.failErr, skipFail: tc.skipFail}
			bo := NewBuildObject("", nil).WithAuthority(st, nil)
			err := bo.failNeverAcceptedLaunches(context.Background(), "b", tc.chunks, tc.expired)
			assert.ErrorIs(t, err, tc.wantErr)
			assert.Equal(t, tc.wantReserved, st.reserved)
			assert.Equal(t, tc.wantFailed, st.failed)
		})
	}
}

// chunkListStore serves one fixed build for chunk validation.
type chunkListStore struct {
	store.VanadisStore

	chunks []model.VanadisChunk
	err    error
}

func (s *chunkListStore) GetBuild(_ context.Context, id string) (*model.VanadisBuild, []model.VanadisChunk, error) {
	if s.err != nil {
		return nil, nil, s.err
	}
	return &model.VanadisBuild{ID: id, Status: "running"}, append([]model.VanadisChunk(nil), s.chunks...), nil
}

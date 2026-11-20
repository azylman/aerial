package router

// Authoritative conflict and CI-recovery tests: when restate_authoritative
// is on, the Restate PRObject verdict — never the Postgres projection —
// decides whether synapse publishes a conflict republish or recovers from
// CI failure. The projection stays only on the legacy fallback path.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

const (
	conflictPublishBody = `{"disposition":"apply","should_publish":true,"deduped":false}`
	conflictDedupedBody = `{"disposition":"apply","should_publish":false,"deduped":true}`
	ciFailedBody        = `{"disposition":"apply","cifailed":true}`
	ciCleanBody         = `{"disposition":"apply","cifailed":false}`
)

// conflictRecoveryServer routes Restate ingress calls by handler: the
// conflict decision endpoint answers with the configured body, the
// conflict publish-recorded ack and the CI recovery resolve answer with
// fixed bodies. Call counts prove which path ran.
func conflictRecoveryServer(t *testing.T, conflictCalls, ackCalls, recoveryCalls *atomic.Int64, decisionStatus int, decisionBody string, ackStatus int, recoveryStatus int, recoveryBody string) *httptest.Server {
	t.Helper()
	if recoveryStatus == 0 {
		recoveryStatus = http.StatusAccepted
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/HandleConflictDetected"):
			conflictCalls.Add(1)
			w.WriteHeader(decisionStatus)
			_, _ = io.WriteString(w, decisionBody)
		case strings.HasSuffix(r.URL.Path, "/HandleConflictPublished/send"):
			ackCalls.Add(1)
			w.WriteHeader(ackStatus)
		case strings.HasSuffix(r.URL.Path, "/HandleCIRecovery"):
			recoveryCalls.Add(1)
			w.WriteHeader(recoveryStatus)
			_, _ = io.WriteString(w, recoveryBody)
		default:
			w.WriteHeader(http.StatusAccepted)
		}
	}))
}

func countConflictPublishes(bus *mockEventBus) int {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	n := 0
	for _, ev := range bus.publishedPREvents {
		if ev.Event == prConflict {
			n++
		}
	}
	return n
}

func authoritativeConflictServer(t *testing.T, conflictCalls, ackCalls, recoveryCalls *atomic.Int64, decisionStatus int, decisionBody string, ackStatus int) *httptest.Server {
	t.Helper()
	return conflictRecoveryServer(t, conflictCalls, ackCalls, recoveryCalls, decisionStatus, decisionBody, ackStatus, 0, ciCleanBody)
}

func TestResolveVerdictsSkipMissingPR(t *testing.T) {
	t.Parallel()
	var conflictCalls atomic.Int64
	var ackCalls atomic.Int64
	var recoveryCalls atomic.Int64
	cfg := lifecycleCfg()
	cfg.RestateAuthoritative = true
	capture := conflictRecoveryServer(t, &conflictCalls, &ackCalls, &recoveryCalls, 202, conflictPublishBody, 202, 0, ciFailedBody)
	t.Cleanup(capture.Close)
	cfg.RestateURL = capture.URL
	srv := newLifecycleServer(cfg, &mockEventBus{}, &mockPRRegistry{}, &mockOutboundDispatcher{})
	ctx := context.Background()
	conflict, err := srv.resolveConflictDispatch(ctx, "azylman/aerial", "feat/branch", "sha123", 0)
	if err != nil || conflict.ShouldPublish || conflict.Deduped {
		t.Fatalf("resolveConflictDispatch(pr 0) = %+v, %v, want zero verdict", conflict, err)
	}
	recovery, err := srv.resolveCIRecovery(ctx, "azylman/aerial", 0, "sha123")
	if err != nil || recovery.CIFailed {
		t.Fatalf("resolveCIRecovery(pr 0) = %+v, %v, want zero verdict", recovery, err)
	}
	if n := conflictCalls.Load() + recoveryCalls.Load(); n != 0 {
		t.Fatalf("verdict calls for pr 0 = %d, want 0", n)
	}
}

func TestResolveCIRecovery(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		authoritative  bool
		capture        bool
		recoveryStatus int
		recoveryBody   string
		prNumber       int
		wantErr        bool
		wantCalls      int64
		wantCIFailed   bool
	}{
		{name: "authoritative ci-failed resolve", authoritative: true, capture: true, recoveryStatus: 202, recoveryBody: ciFailedBody, prNumber: 42, wantCalls: 1, wantCIFailed: true},
		{name: "authoritative clean resolve", authoritative: true, capture: true, recoveryStatus: 202, recoveryBody: ciCleanBody, prNumber: 42, wantCalls: 1},
		{name: "authoritative ingress failure surfaces", authoritative: true, capture: true, recoveryStatus: 500, recoveryBody: "boom", prNumber: 42, wantErr: true, wantCalls: 1},
		{name: "authoritative malformed body surfaces", authoritative: true, capture: true, recoveryStatus: 202, recoveryBody: "not-json{", prNumber: 42, wantErr: true, wantCalls: 1},
		{name: "authoritative without url surfaces misconfiguration", authoritative: true, prNumber: 42, wantErr: true},
		{name: "legacy returns zero without calls", capture: true, recoveryStatus: 202, recoveryBody: ciFailedBody, prNumber: 42},
		{name: "non-pr resolve returns zero without calls", authoritative: true, capture: true, recoveryStatus: 202, recoveryBody: ciFailedBody},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var conflictCalls atomic.Int64
			var ackCalls atomic.Int64
			var recoveryCalls atomic.Int64
			url := ""
			if tc.capture {
				srv := conflictRecoveryServer(t, &conflictCalls, &ackCalls, &recoveryCalls, 202, conflictPublishBody, 202, tc.recoveryStatus, tc.recoveryBody)
				t.Cleanup(srv.Close)
				url = srv.URL
			}
			srv := NewRouterServer(fixedConfig(Config{RestateAuthoritative: tc.authoritative, RestateURL: url}), nil)
			got, err := srv.resolveCIRecovery(context.Background(), "azylman/aerial", tc.prNumber, "sha123")
			if (err != nil) != tc.wantErr {
				t.Fatalf("resolveCIRecovery() error = %v, wantErr %v", err, tc.wantErr)
			}
			if n := recoveryCalls.Load(); n != tc.wantCalls {
				t.Fatalf("recovery calls = %d, want %d", n, tc.wantCalls)
			}
			if !tc.wantErr && got.CIFailed != tc.wantCIFailed {
				t.Fatalf("CIFailed = %v, want %v", got.CIFailed, tc.wantCIFailed)
			}
		})
	}
}

func TestTransitionRegisteredConflictAuthoritative(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		authoritative     bool
		firstCallUpdated  bool
		projectionHeadSHA string
		decisionStatus    int
		decisionBody      string
		ackStatus         int
		busErr            bool
		wantErr           bool
		wantHandled       bool
		wantPublishes     int
		wantDecisions     int64
		wantAcks          int64
	}{
		{name: "authoritative publishes new sha despite disagreeing projection", authoritative: true, firstCallUpdated: true, projectionHeadSHA: "stale-projection-sha", decisionStatus: 202, decisionBody: conflictPublishBody, ackStatus: 202, wantHandled: true, wantPublishes: 1, wantDecisions: 1, wantAcks: 1},
		{name: "authoritative dedupes same sha despite disagreeing projection", authoritative: true, projectionHeadSHA: "other-sha", decisionStatus: 202, decisionBody: conflictDedupedBody, ackStatus: 202, wantHandled: true, wantDecisions: 1},
		{name: "authoritative decision failure surfaces before publish", authoritative: true, firstCallUpdated: true, decisionStatus: 500, decisionBody: "boom", ackStatus: 202, wantErr: true, wantDecisions: 1},
		{name: "authoritative ack failure surfaces after publish", authoritative: true, firstCallUpdated: true, decisionStatus: 202, decisionBody: conflictPublishBody, ackStatus: 500, wantErr: true, wantHandled: true, wantPublishes: 1, wantDecisions: 1, wantAcks: 1},
		{name: "authoritative publish failure surfaces before ack", authoritative: true, firstCallUpdated: true, decisionStatus: 202, decisionBody: conflictPublishBody, ackStatus: 202, busErr: true, wantErr: true, wantDecisions: 1},
		{name: "legacy fallback publishes new sha via projection", firstCallUpdated: true, projectionHeadSHA: "stale-projection-sha", wantHandled: true, wantPublishes: 1},
		{name: "legacy fallback suppresses same sha via projection", projectionHeadSHA: "sha-conflict-1", wantHandled: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var conflictCalls atomic.Int64
			var ackCalls atomic.Int64
			var recoveryCalls atomic.Int64
			cfg := lifecycleCfg()
			cfg.RestateAuthoritative = tc.authoritative
			if tc.authoritative {
				srv := authoritativeConflictServer(t, &conflictCalls, &ackCalls, &recoveryCalls, tc.decisionStatus, tc.decisionBody, tc.ackStatus)
				t.Cleanup(srv.Close)
				cfg.RestateURL = srv.URL
			}
			bus := &mockEventBus{}
			if tc.busErr {
				bus.err = errors.New("boom")
			}
			reg := &mockPRRegistry{
				conflictTargetID: lifecycleTargetID,
				conflictUpdated:  tc.firstCallUpdated,
				resolveTargetID:  lifecycleTargetID,
				buildLifecycle:   PRBuildLifecycle{Status: prConflict, HeadSHA: tc.projectionHeadSHA},
			}
			srv := newLifecycleServer(cfg, bus, reg, &mockOutboundDispatcher{})
			handled, err := srv.transitionRegisteredConflict(context.Background(), "azylman/aerial", 42, "feat/branch", "sha-conflict-1")
			if (err != nil) != tc.wantErr {
				t.Fatalf("transitionRegisteredConflict() error = %v, wantErr %v", err, tc.wantErr)
			}
			if !tc.wantErr && handled != tc.wantHandled {
				t.Fatalf("handled = %v, want %v", handled, tc.wantHandled)
			}
			if got := countConflictPublishes(bus); got != tc.wantPublishes {
				t.Fatalf("conflict publishes = %d, want %d", got, tc.wantPublishes)
			}
			if n := conflictCalls.Load(); n != tc.wantDecisions {
				t.Fatalf("conflict decision calls = %d, want %d", n, tc.wantDecisions)
			}
			if n := ackCalls.Load(); n != tc.wantAcks {
				t.Fatalf("conflict ack calls = %d, want %d", n, tc.wantAcks)
			}
			if n := recoveryCalls.Load(); n != 0 {
				t.Fatalf("recovery calls = %d, want 0", n)
			}
		})
	}
}

func TestCanRecoverCheckSuccessAuthoritative(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                 string
		authoritative        bool
		recoveryStatus       int
		recoveryBody         string
		presetRegistryFailed bool
		presetRegistryHead   string
		wantRecover          bool
		wantRecoveryCalls    int64
	}{
		{name: "authoritative recovers from restate despite clean projection", authoritative: true, recoveryBody: ciFailedBody, presetRegistryHead: "sha10", wantRecover: true, wantRecoveryCalls: 1},
		{name: "authoritative stays clean when restate clean despite failed projection", authoritative: true, recoveryBody: ciCleanBody, presetRegistryFailed: true, presetRegistryHead: "sha10", wantRecoveryCalls: 1},
		{name: "authoritative resolve failure falls back to failed projection", authoritative: true, recoveryStatus: 500, recoveryBody: "boom", presetRegistryFailed: true, presetRegistryHead: "sha10", wantRecover: true, wantRecoveryCalls: 1},
		{name: "authoritative resolve failure falls back to clean projection", authoritative: true, recoveryStatus: 500, recoveryBody: "boom", presetRegistryHead: "sha10", wantRecoveryCalls: 1},
		{name: "legacy fallback recovers from failed projection", presetRegistryFailed: true, presetRegistryHead: "sha10", wantRecover: true},
		{name: "legacy fallback stays clean on clean projection", presetRegistryHead: "sha10"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var conflictCalls atomic.Int64
			var ackCalls atomic.Int64
			var recoveryCalls atomic.Int64
			cfg := lifecycleCfg()
			cfg.RestateAuthoritative = tc.authoritative
			if tc.authoritative {
				srv := conflictRecoveryServer(t, &conflictCalls, &ackCalls, &recoveryCalls, 202, conflictPublishBody, 202, tc.recoveryStatus, tc.recoveryBody)
				t.Cleanup(srv.Close)
				cfg.RestateURL = srv.URL
			}
			status := prOpen
			if tc.presetRegistryFailed {
				status = prCIFailed
			}
			reg := &mockPRRegistry{
				buildLifecycle: PRBuildLifecycle{Status: status, HeadSHA: tc.presetRegistryHead},
			}
			srv := newLifecycleServer(cfg, &mockEventBus{}, reg, &mockOutboundDispatcher{})
			got := srv.canRecoverCheckSuccess(context.Background(), "azylman/aerial", 42, "lint", "sha10")
			if got != tc.wantRecover {
				t.Fatalf("canRecoverCheckSuccess() = %v, want %v", got, tc.wantRecover)
			}
			if n := recoveryCalls.Load(); n != tc.wantRecoveryCalls {
				t.Fatalf("recovery calls = %d, want %d", n, tc.wantRecoveryCalls)
			}
		})
	}
}

func TestRecordConflictPublished(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		authoritative bool
		ackStatus     int
		prNumber      int
		wantErr       bool
		wantAcks      int64
	}{
		{name: "authoritative ack records publish", authoritative: true, ackStatus: 202, prNumber: 42, wantAcks: 1},
		{name: "authoritative ack failure surfaces", authoritative: true, ackStatus: 500, prNumber: 42, wantErr: true, wantAcks: 1},
		{name: "authoritative skips pr-less ack", authoritative: true, ackStatus: 202},
		{name: "legacy ack is a no-op", ackStatus: 202, prNumber: 42},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var conflictCalls atomic.Int64
			var ackCalls atomic.Int64
			var recoveryCalls atomic.Int64
			cfg := lifecycleCfg()
			cfg.RestateAuthoritative = tc.authoritative
			if tc.authoritative {
				srv := authoritativeConflictServer(t, &conflictCalls, &ackCalls, &recoveryCalls, 202, conflictPublishBody, tc.ackStatus)
				t.Cleanup(srv.Close)
				cfg.RestateURL = srv.URL
			}
			srv := newLifecycleServer(cfg, &mockEventBus{}, &mockPRRegistry{}, &mockOutboundDispatcher{})
			err := srv.recordConflictPublished(context.Background(), "azylman/aerial", "feat/branch", "sha-conflict", tc.prNumber)
			if (err != nil) != tc.wantErr {
				t.Fatalf("recordConflictPublished() error = %v, wantErr %v", err, tc.wantErr)
			}
			if n := ackCalls.Load(); n != tc.wantAcks {
				t.Fatalf("ack calls = %d, want %d", n, tc.wantAcks)
			}
		})
	}
}

func TestShouldRetryCIFailureCompletionAuthoritative(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                 string
		authoritative        bool
		recoveryStatus       int
		recoveryBody         string
		presetRegistryFailed bool
		wantRetry            bool
		wantRecoveryCalls    int64
	}{
		{name: "authoritative retries from restate despite clean projection", authoritative: true, recoveryBody: ciFailedBody, wantRetry: true, wantRecoveryCalls: 1},
		{name: "authoritative skips when restate clean despite failed projection", authoritative: true, recoveryBody: ciCleanBody, presetRegistryFailed: true, wantRecoveryCalls: 1},
		{name: "authoritative resolve failure falls back to failed projection", authoritative: true, recoveryStatus: 500, recoveryBody: "boom", presetRegistryFailed: true, wantRetry: true, wantRecoveryCalls: 1},
		{name: "authoritative resolve failure falls back to clean projection", authoritative: true, recoveryStatus: 500, recoveryBody: "boom", wantRecoveryCalls: 1},
		{name: "legacy fallback retries from failed projection", presetRegistryFailed: true, wantRetry: true},
		{name: "legacy fallback skips on clean projection"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var conflictCalls atomic.Int64
			var ackCalls atomic.Int64
			var recoveryCalls atomic.Int64
			cfg := lifecycleCfg()
			cfg.RestateAuthoritative = tc.authoritative
			if tc.authoritative {
				srv := conflictRecoveryServer(t, &conflictCalls, &ackCalls, &recoveryCalls, 202, conflictPublishBody, 202, tc.recoveryStatus, tc.recoveryBody)
				t.Cleanup(srv.Close)
				cfg.RestateURL = srv.URL
			}
			status := prOpen
			if tc.presetRegistryFailed {
				status = prCIFailed
			}
			reg := &mockPRRegistry{
				resolveTargetID: lifecycleTargetID,
				buildLifecycle:  PRBuildLifecycle{Status: status, HeadSHA: "sha10"},
			}
			srv := newLifecycleServer(cfg, &mockEventBus{}, reg, &mockOutboundDispatcher{})
			res := &ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 42, HeadSHA: "sha10", TargetID: lifecycleTargetID}
			got := srv.shouldRetryCIFailureCompletion(context.Background(), res)
			if got != tc.wantRetry {
				t.Fatalf("shouldRetryCIFailureCompletion() = %v, want %v", got, tc.wantRetry)
			}
			if n := recoveryCalls.Load(); n != tc.wantRecoveryCalls {
				t.Fatalf("recovery calls = %d, want %d", n, tc.wantRecoveryCalls)
			}
		})
	}
}

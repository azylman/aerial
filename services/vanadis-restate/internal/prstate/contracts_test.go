package prstate

import (
	"context"
	"errors"
	"testing"

	"github.com/azylman/aerial/pkg/pubsub"
)

var errFakeRoute = errors.New("fake route error")

// TestPRStateContracts freezes the handler surface, key formats, and the
// unmanaged-PR rule. Any rename here is a cross-chunk breaking change.
func TestPRStateContracts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "pr object key", got: PRObjectKey("aerial", 101), want: "azylman/aerial#101"},
		{name: "pr object key normalizes bare repo", got: PRObjectKey("aerial", 7), want: "azylman/aerial#7"},
		{name: "repo object key", got: RepoObjectKey("aerial"), want: "azylman/aerial"},
		{name: "deploy pr key", got: DeployPRKey("aerial", 101), want: "pr:azylman/aerial:101"},
		{name: "deploy sha job key", got: DeploySHAJobKey("aerial", "eri", "abc123"), want: "sha:azylman/aerial:eri:abc123"},
		{name: "canonical roborev", got: CanonicalFailedCheckKey("RoboRev Code Review"), want: "roborev"},
		{name: "canonical trims and lowers", got: CanonicalFailedCheckKey("  Lint  "), want: "lint"},
		{name: "effect key string", got: EffectKey{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", GateGeneration: 3, Kind: EffectDiscordAlert}.String(), want: "azylman/aerial#101@abc123:gen3:discord_alert"},
		{name: "effect key deploy carries job", got: EffectKey{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", GateGeneration: 1, Kind: EffectDeployDispatch, JobName: "eri"}.String(), want: "azylman/aerial#101@abc123:gen1:deploy_dispatch:job=eri"},
		{name: "effect key ci carries check", got: EffectKey{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", GateGeneration: 1, Kind: EffectGitHubRerun, CheckName: "Lint"}.String(), want: "azylman/aerial#101@abc123:gen1:github_rerun:check=lint"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.got != tc.want {
				t.Fatalf("got %q, want %q", tc.got, tc.want)
			}
		})
	}
}

func TestPRStateOwnershipRule(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		buildID   string
		unmanaged bool
		want      PROwnership
		wantBlock bool
	}{
		{name: "build owned routes", buildID: "build-1", unmanaged: false, want: PROwned, wantBlock: false},
		{name: "unmanaged collaborator never routes", buildID: "", unmanaged: true, want: PRUnmanaged, wantBlock: true},
		{name: "unmanaged flag wins over build id", buildID: "build-1", unmanaged: true, want: PRUnmanaged, wantBlock: true},
		{name: "empty build id without flag is unmanaged", buildID: "", unmanaged: false, want: PRUnmanaged, wantBlock: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ClassifyOwnership(tc.buildID, tc.unmanaged); got != tc.want {
				t.Fatalf("ClassifyOwnership = %q, want %q", got, tc.want)
			}
			if got := UnmanagedMustNotRoute(ClassifyOwnership(tc.buildID, tc.unmanaged)); got != tc.wantBlock {
				t.Fatalf("UnmanagedMustNotRoute = %v, want %v", got, tc.wantBlock)
			}
		})
	}
}

func TestPRStateStaleGuard(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		current string
		event   string
		want    bool
	}{
		{name: "matching sha applies", current: "abc123", event: "abc123", want: false},
		{name: "case insensitive match applies", current: "ABC123", event: "abc123", want: false},
		{name: "mismatched sha drops", current: "abc123", event: "def456", want: true},
		{name: "empty event sha never stale", current: "abc123", event: "", want: false},
		{name: "empty current sha never stale", current: "", event: "abc123", want: false},
		{name: "zero sha never stale", current: "abc123", event: "0000000", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsStaleEvent(tc.current, tc.event); got != tc.want {
				t.Fatalf("IsStaleEvent(%q, %q) = %v, want %v", tc.current, tc.event, got, tc.want)
			}
		})
	}
}

// stubDeployDispatchedErr adapts the verdict-returning dispatch stub to the
// error-only stub table without a per-row closure.
func stubDeployDispatchedErr(ctx context.Context, ev PREvent) error {
	_, err := PRObjectStub{}.HandleDeployDispatched(ctx, ev)
	return err
}

// stubDeployPublishedErr adapts the outcome-returning publish-ack stub to the
// error-only stub table without a per-row closure.
func stubDeployPublishedErr(ctx context.Context, ev PREvent) error {
	_, err := PRObjectStub{}.HandleDeploySuccessPublished(ctx, ev)
	return err
}

// stubConflictDetectedErr adapts the verdict-returning conflict stub to the
// error-only stub table without a per-row closure.
func stubConflictDetectedErr(ctx context.Context, ev PREvent) error {
	_, err := PRObjectStub{}.HandleConflictDetected(ctx, ev)
	return err
}

// stubConflictPublishedErr adapts the outcome-returning conflict ack stub to
// the error-only stub table without a per-row closure.
func stubConflictPublishedErr(ctx context.Context, ev PREvent) error {
	_, err := PRObjectStub{}.HandleConflictPublished(ctx, ev)
	return err
}

// stubCIRecoveryErr adapts the verdict-returning recovery stub to the
// error-only stub table without a per-row closure.
func stubCIRecoveryErr(ctx context.Context, ev PREvent) error {
	_, err := PRObjectStub{}.HandleCIRecovery(ctx, ev)
	return err
}

func stubRouteBySHAErr(ctx context.Context) error {
	_, err := RepoObjectStub{}.RouteBySHA(ctx, SHAKindCheckRun, SHAEvent{})
	return err
}

func stubExpireHeldErr(ctx context.Context) error {
	_, err := RepoObjectStub{}.ExpireHeld(ctx, "aerial", 1)
	return err
}

func TestPRStateStubHandlersNoop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ev := PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", Event: "check_failed"}
	rev := RepoEvent{Repo: "aerial", MainSHA: "abc123"}
	tests := []struct {
		name string
		got  error
	}{
		{name: "check failed", got: PRObjectStub{}.HandleCheckFailed(ctx, ev)},
		{name: "check passed", got: PRObjectStub{}.HandleCheckPassed(ctx, ev)},
		{name: "check rerun", got: PRObjectStub{}.HandleCheckRerunRequested(ctx, ev)},
		{name: "collaborator alert", got: PRObjectStub{}.HandleCollaboratorAlert(ctx, ev)},
		{name: "conflict detected", got: stubConflictDetectedErr(ctx, ev)},
		{name: "conflict published", got: stubConflictPublishedErr(ctx, ev)},
		{name: "ci recovery", got: stubCIRecoveryErr(ctx, ev)},
		{name: "conflict resolved", got: PRObjectStub{}.HandleConflictResolved(ctx, ev)},
		{name: "merged", got: PRObjectStub{}.HandleMerged(ctx, ev)},
		{name: "closed", got: PRObjectStub{}.HandleClosed(ctx, ev)},
		{name: "reopened", got: PRObjectStub{}.HandleReopened(ctx, ev)},
		{name: "readiness retry", got: PRObjectStub{}.HandleReadinessRetry(ctx, ev)},
		{name: "rollout progress", got: PRObjectStub{}.HandleRolloutProgress(ctx, ev)},
		{name: "deploy dispatched", got: stubDeployDispatchedErr(ctx, ev)},
		{name: "deploy success published", got: stubDeployPublishedErr(ctx, ev)},
		{name: "kustomization requested", got: PRObjectStub{}.HandleKustomizationRequested(ctx, ev)},
		{name: "repo main push", got: RepoObjectStub{}.HandleMainPush(ctx, rev)},
		{name: "repo fanout", got: RepoObjectStub{}.HandleFanout(ctx, rev)},
		{name: "repo kustomization status", got: RepoObjectStub{}.HandleKustomizationStatus(ctx, rev)},
		{name: "main advanced", got: PRObjectStub{}.HandleMainAdvanced(ctx, ev)},
		{name: "repo pr index", got: RepoObjectStub{}.HandlePRIndex(ctx, PRIndexEvent{})},
		{name: "repo held replay", got: RepoObjectStub{}.ReplayHeld(ctx, "aerial", "abc123")},
		{name: "repo route by sha", got: stubRouteBySHAErr(ctx)},
		{name: "repo held expiry", got: stubExpireHeldErr(ctx)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.got != nil {
				t.Fatalf("stub handler returned %v, want nil", tc.got)
			}
		})
	}
}

func TestPRStateFakeBuildRouterInvoker(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		routes int
		want   int
	}{
		{name: "records one route", routes: 1, want: 1},
		{name: "records three routes", routes: 3, want: 3},
	}
	ctx := context.Background()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			inner := &FakeBuildRouterInvoker{}
			for i := range tc.routes {
				ev := pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101 + i}
				if err := inner.RoutePREvent(ctx, "build-1", ev); err != nil {
					t.Fatalf("RoutePREvent returned %v, want nil", err)
				}
			}
			if got := inner.CallCount(); got != tc.want {
				t.Fatalf("CallCount = %d, want %d", got, tc.want)
			}
		})
	}
	fake := &FakeBuildRouterInvoker{}
	if err := fake.RoutePREvent(ctx, "build-1", pubsub.PRLifecycleEvent{Repo: "aerial", PRNumber: 101}); err != nil {
		t.Fatalf("RoutePREvent returned %v, want nil", err)
	}
	if got := len(fake.CallsFor("build-1")); got != 1 {
		t.Fatalf("CallsFor(build-1) = %d, want 1", got)
	}
	if got := len(fake.CallsFor("missing")); got != 0 {
		t.Fatalf("CallsFor(missing) = %d, want 0", got)
	}
	fake.Err = errFakeRoute
	if err := fake.RoutePREvent(ctx, "build-1", pubsub.PRLifecycleEvent{}); !errors.Is(err, errFakeRoute) {
		t.Fatalf("RoutePREvent err = %v, want fake error", err)
	}
	fake.Err = nil
	fake.Reset()
	if got := fake.CallCount(); got != 0 {
		t.Fatalf("CallCount after Reset = %d, want 0", got)
	}
	pr, br, ok := (RepoObjectStub{}).ResolvePRBySHA(ctx, "aerial", "abc123")
	if pr != 0 || br != "" || ok {
		t.Fatalf("stub ResolvePRBySHA = (%d,%q,%v), want (0,\"\",false)", pr, br, ok)
	}
}

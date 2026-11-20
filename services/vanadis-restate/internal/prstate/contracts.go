// Package prstate freezes the Restate Virtual Object contracts for synapse PR
// lifecycle state: PRObject (key repo#prNumber) is the only writer/authority
// for per-PR state, RepoObject (key repo) holds only the bounded main-push
// index, dispatched-SHA tombstones, and fan-out sends.
//
// Authority invariants frozen here:
//   - Postgres pr_registry is a read-only projection + SHA lookup index.
//     Routing may use it; authority may not.
//   - Every external effect runs as a journaled step keyed by EffectKey;
//     replays return the recorded result, never re-fire.
//   - Cross-object sends carry head or merge SHA; handlers drop stale events.
//     Terminal states are absorbing within a reopen epoch.
//   - Deploy dedup keeps BOTH keys: PR-keyed and job-plus-SHA WasDispatched
//     semantics. Bare PR-key dedup that never expires is forbidden.
//   - Unmanaged collaborator PRs stay inside PRObject for alerts and NEVER
//     signal BuildRouter.RoutePREvent.
//   - Readiness retry never mints a generation and never clears effect keys;
//     generations bump only when a new gate generation starts.
//
// This chunk registers no Restate handlers and wires no live callers; the
// stub types below are the registration placeholders for chunk 07.
package prstate

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/azylman/aerial/pkg/pubsub"
)

// Effect kinds for journaled external effects. Each effect fires at most once
// per EffectKey; replay returns the recorded result.
const (
	EffectDiscordAlert       = "discord_alert"
	EffectGitHubUpdateBranch = "github_update_branch"
	EffectGitHubRerun        = "github_rerun"
	EffectImagePromote       = "image_promote"
	EffectFluxForward        = "flux_forward"
	EffectDeployDispatch     = "deploy_dispatch"
)

// MaxMainPushIndex bounds the RepoObject main-push SHA history.
const MaxMainPushIndex = 50

// NormalizeRepo normalizes repository names with the owner prefix.
// Mirrors services/synapse/internal/model NormalizeRepo semantics.
func NormalizeRepo(repo string) string {
	repo = strings.TrimSpace(repo)
	if repo == "" {
		return ""
	}
	if !strings.Contains(repo, "/") {
		return "azylman/" + repo
	}
	return repo
}

// IsZeroSHA reports whether sha is empty or consists entirely of '0' characters.
func IsZeroSHA(sha string) bool {
	return strings.Trim(sha, "0") == ""
}

// PRObjectKey returns the Restate K/V key for a PR object: repo#prNumber.
func PRObjectKey(repo string, prNumber int) string {
	return fmt.Sprintf("%s#%d", NormalizeRepo(repo), prNumber)
}

// RepoObjectKey returns the Restate K/V key for a repo object: repo.
func RepoObjectKey(repo string) string {
	return NormalizeRepo(repo)
}

// IsRoboRevCheck returns true for the RoboRev review gate check names.
// Mirrors services/synapse/internal/github IsRoboRevCheck semantics.
func IsRoboRevCheck(checkName string) bool {
	trimmed := strings.TrimSpace(checkName)
	return strings.EqualFold(trimmed, "roborev") || strings.EqualFold(trimmed, "RoboRev Code Review")
}

// CanonicalFailedCheckKey canonicalizes check names, grouping roborev variants.
func CanonicalFailedCheckKey(checkName string) string {
	if IsRoboRevCheck(checkName) {
		return "roborev"
	}
	return strings.ToLower(strings.TrimSpace(checkName))
}

// EffectKey is the journaled idempotency key for one external effect. Keys are
// generation-scoped: repo + PR + headSHA + gateGeneration + effectKind, plus
// jobName for deploy effects and the canonical check name for CI effects.
type EffectKey struct {
	Repo           string
	PRNumber       int
	HeadSHA        string
	GateGeneration uint64
	Kind           string
	JobName        string
	CheckName      string
}

// String renders the canonical journaled key.
func (k EffectKey) String() string {
	sb := strings.Builder{}
	fmt.Fprintf(&sb, "%s#%d@%s:gen%d:%s", NormalizeRepo(k.Repo), k.PRNumber, strings.TrimSpace(k.HeadSHA), k.GateGeneration, strings.TrimSpace(k.Kind))
	if clean := strings.TrimSpace(k.JobName); clean != "" {
		fmt.Fprintf(&sb, ":job=%s", clean)
	}
	if canon := CanonicalFailedCheckKey(k.CheckName); canon != "" {
		fmt.Fprintf(&sb, ":check=%s", canon)
	}
	return sb.String()
}

// DeployPRKey returns the PR-keyed deploy dedup key. Mirrors the pr: key in
// services/synapse/internal/flux DeployDedupTracker.
func DeployPRKey(repo string, prNumber int) string {
	return fmt.Sprintf("pr:%s:%d", NormalizeRepo(repo), prNumber)
}

// DeploySHAJobKey returns the job-plus-SHA deploy dedup key. Mirrors the sha:
// key in services/synapse/internal/flux DeployDedupTracker.
func DeploySHAJobKey(repo, jobName, sha string) string {
	return fmt.Sprintf("sha:%s:%s:%s", NormalizeRepo(repo), strings.TrimSpace(jobName), strings.TrimSpace(sha))
}

// IsStaleEvent reports whether an event SHA is stale against current state.
// An empty or zero event SHA is never stale; a mismatch drops the event.
func IsStaleEvent(currentHeadSHA, eventSHA string) bool {
	current := strings.TrimSpace(currentHeadSHA)
	event := strings.TrimSpace(eventSHA)
	if current == "" || event == "" || IsZeroSHA(event) {
		return false
	}
	return !strings.EqualFold(current, event)
}

// PROwnership classifies whether a PR is owned by a Vanadis build.
type PROwnership string

const (
	// PROwned marks a build-owned PR whose events route to the Build object.
	PROwned PROwnership = "owned"
	// PRUnmanaged marks a collaborator PR handled inside PRObject only.
	PRUnmanaged PROwnership = "unmanaged"
)

// ClassifyOwnership returns PROwned only for non-unmanaged PRs carrying a
// build ID; every other PR is unmanaged.
func ClassifyOwnership(buildID string, unmanaged bool) PROwnership {
	if unmanaged || strings.TrimSpace(buildID) == "" {
		return PRUnmanaged
	}
	return PROwned
}

// UnmanagedMustNotRoute reports whether the PR must never signal
// BuildRouter.RoutePREvent. True for every unmanaged collaborator PR.
func UnmanagedMustNotRoute(o PROwnership) bool {
	return o != PROwned
}

// TerminalState is the absorbing lifecycle end state of a PR within one
// reopen epoch. Reopened PRs start a new epoch so terminal-absorbing never
// bricks a reopen.
type TerminalState string

const (
	TerminalNone   TerminalState = ""
	TerminalMerged TerminalState = "merged"
	TerminalClosed TerminalState = "closed"
)

// Absorbing reports whether the state absorbs same-epoch events.
func (t TerminalState) Absorbing() bool {
	return t == TerminalMerged || t == TerminalClosed
}

// EventDisposition is the pure routing decision for an inbound event.
type EventDisposition string

const (
	// DispositionApply applies the event to current state.
	DispositionApply EventDisposition = "apply"
	// DispositionDropStale drops the event as stale against current head SHA.
	DispositionDropStale EventDisposition = "drop_stale"
	// DispositionAbsorbTerminal absorbs the event into an absorbing terminal state.
	DispositionAbsorbTerminal EventDisposition = "absorb_terminal"
)

// PREvent is the canonical PRObject payload. It carries the head or merge SHA
// every cross-object send needs for the stale-SHA guard, plus the gate
// generation and reopen epoch the disposition logic compares.
// PREvent marshals with the snake_case wire format the synapse ingress
// invoker emits; the tags are load-bearing for cross-module interop.
type PREvent struct {
	Event          string `json:"event"`
	Repo           string `json:"repo"`
	PRNumber       int    `json:"pr_number"`
	HeadSHA        string `json:"head_sha"`
	MergeSHA       string `json:"merge_sha"`
	CheckName      string `json:"check_name"`
	JobName        string `json:"job_name"`
	Branch         string `json:"branch"`
	Title          string `json:"title"`
	GateGeneration uint64 `json:"gate_generation"`
	ReopenEpoch    uint64 `json:"reopen_epoch"`
	Unmanaged      bool   `json:"unmanaged"`
	BuildID        string `json:"build_id"`
}

// RepoEvent is the canonical RepoObject payload: main-push indexing,
// dispatched-SHA tombstones, and fan-out sends only.
// RepoEvent marshals with the snake_case wire format the synapse ingress
// invoker emits; the tags are load-bearing for cross-module interop.
type RepoEvent struct {
	Repo       string `json:"repo"`
	MainSHA    string `json:"main_sha"`
	Generation uint64 `json:"generation"`
	PRNumber   int    `json:"pr_number"`
	HeadSHA    string `json:"head_sha"`
}

// PRObject is the frozen PR handler surface. One method per lifecycle event;
// implementations live in chunks 04-06, dark registration in chunk 07.
type PRObject interface {
	HandleCheckFailed(ctx context.Context, ev PREvent) error
	HandleCheckPassed(ctx context.Context, ev PREvent) error
	HandleCheckRerunRequested(ctx context.Context, ev PREvent) error
	HandleCollaboratorAlert(ctx context.Context, ev PREvent) error
	HandleConflictDetected(ctx context.Context, ev PREvent) (ConflictDispatchDecision, error)
	HandleConflictPublished(ctx context.Context, ev PREvent) (ConflictPublishedOutcome, error)
	HandleCIRecovery(ctx context.Context, ev PREvent) (CIRecoveryDecision, error)
	HandleConflictResolved(ctx context.Context, ev PREvent) error
	HandleMerged(ctx context.Context, ev PREvent) error
	HandleClosed(ctx context.Context, ev PREvent) error
	HandleReopened(ctx context.Context, ev PREvent) error
	HandleReadinessRetry(ctx context.Context, ev PREvent) error
	HandleRolloutProgress(ctx context.Context, ev PREvent) error
	HandleDeployDispatched(ctx context.Context, ev PREvent) (DeployDispatchDecision, error)
	HandleDeploySuccessPublished(ctx context.Context, ev PREvent) (DeployPublishedOutcome, error)
	HandleKustomizationRequested(ctx context.Context, ev PREvent) error
}

// RepoObject is the frozen repo handler surface: main-push index,
// dispatched-SHA tombstones, fan-out sends, and durable SHA resolution.
type RepoObject interface {
	HandleMainPush(ctx context.Context, ev RepoEvent) error
	HandleFanout(ctx context.Context, ev RepoEvent) error
	HandleKustomizationStatus(ctx context.Context, ev RepoEvent) error
	ResolvePRBySHA(ctx context.Context, repo, sha string) (int, string, bool)
}

// PRObjectStub is the empty registered handler stub for PRObject. Chunk 07
// binds the real implementations; until then every handler is a no-op.
type PRObjectStub struct{}

// Compile-time surface freeze: stub must satisfy the full interface.
var _ PRObject = PRObjectStub{}

func (PRObjectStub) HandleCheckFailed(context.Context, PREvent) error         { return nil }
func (PRObjectStub) HandleCheckPassed(context.Context, PREvent) error         { return nil }
func (PRObjectStub) HandleCheckRerunRequested(context.Context, PREvent) error { return nil }
func (PRObjectStub) HandleCollaboratorAlert(context.Context, PREvent) error   { return nil }
func (PRObjectStub) HandleConflictDetected(context.Context, PREvent) (ConflictDispatchDecision, error) {
	return ConflictDispatchDecision{}, nil
}

func (PRObjectStub) HandleConflictPublished(context.Context, PREvent) (ConflictPublishedOutcome, error) {
	return ConflictPublishedOutcome{}, nil
}

func (PRObjectStub) HandleCIRecovery(context.Context, PREvent) (CIRecoveryDecision, error) {
	return CIRecoveryDecision{}, nil
}
func (PRObjectStub) HandleConflictResolved(context.Context, PREvent) error { return nil }
func (PRObjectStub) HandleMerged(context.Context, PREvent) error           { return nil }
func (PRObjectStub) HandleClosed(context.Context, PREvent) error           { return nil }
func (PRObjectStub) HandleReopened(context.Context, PREvent) error         { return nil }
func (PRObjectStub) HandleReadinessRetry(context.Context, PREvent) error   { return nil }
func (PRObjectStub) HandleRolloutProgress(context.Context, PREvent) error  { return nil }
func (PRObjectStub) HandleDeployDispatched(context.Context, PREvent) (DeployDispatchDecision, error) {
	return DeployDispatchDecision{}, nil
}

func (PRObjectStub) HandleDeploySuccessPublished(context.Context, PREvent) (DeployPublishedOutcome, error) {
	return DeployPublishedOutcome{}, nil
}

func (PRObjectStub) HandleKustomizationRequested(context.Context, PREvent) error {
	return nil
}

// RepoObjectStub is the empty registered handler stub for RepoObject.
type RepoObjectStub struct{}

// Compile-time surface freeze: stub must satisfy the full interface.
var _ RepoObject = RepoObjectStub{}

func (RepoObjectStub) HandleMainPush(context.Context, RepoEvent) error            { return nil }
func (RepoObjectStub) HandleFanout(context.Context, RepoEvent) error              { return nil }
func (RepoObjectStub) HandleKustomizationStatus(context.Context, RepoEvent) error { return nil }
func (RepoObjectStub) ResolvePRBySHA(context.Context, string, string) (int, string, bool) {
	return 0, "", false
}

// BuildRouterInvoker routes build-owned PR events to the owning Build object.
// Unmanaged collaborator PRs never reach this interface.
type BuildRouterInvoker interface {
	RoutePREvent(ctx context.Context, buildID string, ev pubsub.PRLifecycleEvent) error
}

// RoutedCall records one FakeBuildRouterInvoker route.
type RoutedCall struct {
	BuildID string
	Event   pubsub.PRLifecycleEvent
}

// FakeBuildRouterInvoker is the ONE shared fake BuildRouter invoker for this
// package. Chunks 03a-03c reuse it; no chunk registers a second fake.
type FakeBuildRouterInvoker struct {
	mu    sync.Mutex
	Calls []RoutedCall
	Err   error
}

// Compile-time freeze: the shared fake must satisfy the invoker interface.
var _ BuildRouterInvoker = (*FakeBuildRouterInvoker)(nil)

// RoutePREvent records the route and returns the configured error.
func (f *FakeBuildRouterInvoker) RoutePREvent(ctx context.Context, buildID string, ev pubsub.PRLifecycleEvent) error {
	_ = ctx
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = append(f.Calls, RoutedCall{BuildID: buildID, Event: ev})
	return f.Err
}

// CallCount returns the number of recorded routes.
func (f *FakeBuildRouterInvoker) CallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.Calls)
}

// CallsFor returns the recorded routes for one build ID.
func (f *FakeBuildRouterInvoker) CallsFor(buildID string) []RoutedCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []RoutedCall
	for _, c := range f.Calls {
		if c.BuildID == buildID {
			out = append(out, c)
		}
	}
	return out
}

// Reset clears all recorded routes.
func (f *FakeBuildRouterInvoker) Reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Calls = nil
}

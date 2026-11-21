// Package prstate freezes the Restate Virtual Object contracts for synapse PR
// lifecycle state: PRObject (key repo#prNumber) is the only writer/authority
// for per-PR state, RepoObject (key repo) holds only repo-level state: the
// bounded main-push index, dispatched-SHA tombstones, the SHA->PR routing
// index with open/deploying sets that PRObject feeds, held SHA-only events,
// and fan-out sends. The two objects talk only through one-way sends.
//
// Authority invariants frozen here:
//   - Postgres pr_registry is a read-only projection; authority never reads
//     it. SHA->PR routing uses the RepoObject durable index, never the table.
//   - PRObject and RepoObject exchange only one-way sends, never
//     request-response between exclusive handlers.
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
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

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

// Eri-bound effect kinds. They run through the owner's Restate session path
// (ThreadSession, DispatchSession, and the agent-routed Discord sender),
// never eri HTTP.
const (
	// EffectEriPrompt is the owning agent's prompt turn in a thread.
	EffectEriPrompt = "eri_prompt"
	// EffectEriBackground is a headless background dispatch.
	EffectEriBackground = "eri_background"
	// EffectEriDirectMessage is a plain Discord direct message.
	EffectEriDirectMessage = "eri_direct_message"
)

// DefaultAgent is the fallback agent identifier.
const DefaultAgent = "aerial"

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
	return k.render(true)
}

// MatchKey renders the key without the gate generation. Synapse never knows
// the generation, so its [synapse-effect] log and prstate's intended-effect
// record meet on this form (repo#pr@sha:kind[:job=..][:check=..]); the
// synapse restateclient EffectMatchKey renders the identical string.
func (k EffectKey) MatchKey() string {
	return k.render(false)
}

func (k EffectKey) render(withGeneration bool) string {
	sb := strings.Builder{}
	fmt.Fprintf(&sb, "%s#%d@%s:", NormalizeRepo(k.Repo), k.PRNumber, strings.TrimSpace(k.HeadSHA))
	if withGeneration {
		fmt.Fprintf(&sb, "gen%d:", k.GateGeneration)
	}
	sb.WriteString(strings.TrimSpace(k.Kind))
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

// EffectsOwner says who performs an event's side effects. Synapse stamps
// every event exactly once when it first accepts it. For legacy events
// prstate still applies the state transition and commits effect keys but
// skips the side effect itself; for restate events prstate executes it.
type EffectsOwner string

// Effect owner values on the wire.
const (
	EffectsOwnerLegacy  EffectsOwner = "legacy"
	EffectsOwnerRestate EffectsOwner = "restate"
)

// ParseEffectsOwner maps a wire stamp to an owner. Only an exact
// (case-insensitive) restate stamp hands effects to prstate; a missing,
// empty, or unknown stamp is legacy, so a malformed value can never make
// prstate double-fire an effect synapse already performed.
func ParseEffectsOwner(raw string) EffectsOwner {
	if strings.EqualFold(strings.TrimSpace(raw), string(EffectsOwnerRestate)) {
		return EffectsOwnerRestate
	}
	return EffectsOwnerLegacy
}

// PREvent is the canonical PRObject payload. It carries the head or merge SHA
// every cross-object send needs for the stale-SHA guard, plus the gate
// generation and reopen epoch the disposition logic compares.
// PREvent marshals with the snake_case wire format the synapse ingress
// invoker emits; the tags are load-bearing for cross-module interop.
type PREvent struct {
	Event          string       `json:"event"`
	Repo           string       `json:"repo"`
	PRNumber       int          `json:"pr_number"`
	Agent          string       `json:"agent,omitempty"`
	HeadSHA        string       `json:"head_sha"`
	MergeSHA       string       `json:"merge_sha"`
	CheckName      string       `json:"check_name"`
	JobName        string       `json:"job_name"`
	Branch         string       `json:"branch"`
	Title          string       `json:"title"`
	GateGeneration uint64       `json:"gate_generation"`
	ReopenEpoch    uint64       `json:"reopen_epoch"`
	Unmanaged      bool         `json:"unmanaged"`
	BuildID        string       `json:"build_id"`
	EffectsOwner   EffectsOwner `json:"effects_owner,omitempty"`
	// Kustomization names the Flux Kustomization a forwarded
	// kustomization status observed; empty for every other event.
	Kustomization string `json:"kustomization,omitempty"`

	// Registry row payload. Each field is omitted when empty, so events
	// that carry no registry data keep the frozen wire key set.
	//
	// BeforeSHA is the head a synchronize moved away from; the head only
	// advances when it matches the stored head (compare-and-set).
	BeforeSHA string `json:"before_sha,omitempty"`
	// TargetID is the Discord target synapse resolved for the PR.
	TargetID string `json:"target_id,omitempty"`
	// CheckState is the ci_checks value HandleCheckStatus records
	// (passed/pending).
	CheckState string `json:"check_state,omitempty"`
	// Jobs are the deploy jobs a kustomization request starts.
	Jobs []string `json:"jobs,omitempty"`
	// DeployedJobs are jobs a rollout observation found ready.
	DeployedJobs []string `json:"deployed_jobs,omitempty"`
	// JobState is the job_states value recorded for JobName.
	JobState string `json:"job_state,omitempty"`
	// KustomizationRequests replaces the PR's recorded reconcile requests.
	KustomizationRequests map[string]KustomizationRequest `json:"kustomization_requests,omitempty"`
	// DeployedKustomizations are Kustomizations a rollout observation
	// found complete.
	DeployedKustomizations []string `json:"deployed_kustomizations,omitempty"`
	// Banner is the failure banner a deploy failure records.
	Banner string `json:"banner,omitempty"`
	// Acknowledged marks a deploy-failed event as the publish ack.
	Acknowledged bool `json:"acknowledged,omitempty"`
	// BuildMetadata is one build-metadata snapshot for the PR revision.
	BuildMetadata *BuildMetadata `json:"build_metadata,omitempty"`
	// EriEffects are the rendered eri-bound effects synapse resolved for
	// this event (collaborator DMs today). PRObject executes them for
	// effects_owner=restate and records them as intended otherwise.
	EriEffects []EriEffect `json:"eri_effects,omitempty"`
	// Facts are the rendered aerial.pr.> lifecycle facts synapse resolved
	// for this event. Verdict handlers and the deploy-failure deadline
	// publish them for effects_owner=restate once their decision commits,
	// and record them as intended otherwise.
	Facts []pubsub.PRLifecycleEvent `json:"facts,omitempty"`
}

// EriEffect is one rendered eri-bound effect. Synapse renders it from
// stateless inputs (config lookups, GitHub payloads, message templates);
// PRObject decides whether and to which owner it fires. An empty ChannelID
// falls back to the PR's stored target.
type EriEffect struct {
	Kind         string `json:"kind"`
	ChannelID    string `json:"channel_id,omitempty"`
	Content      string `json:"content"`
	Effort       string `json:"effort,omitempty"`
	SkipIfActive bool   `json:"skip_if_active,omitempty"`
}

// RepoEvent is the canonical RepoObject payload: main-push indexing,
// dispatched-SHA tombstones, and fan-out sends only.
// RepoEvent marshals with the snake_case wire format the synapse ingress
// invoker emits; the tags are load-bearing for cross-module interop.
type RepoEvent struct {
	Repo         string       `json:"repo"`
	MainSHA      string       `json:"main_sha"`
	Generation   uint64       `json:"generation"`
	PRNumber     int          `json:"pr_number"`
	HeadSHA      string       `json:"head_sha"`
	EffectsOwner EffectsOwner `json:"effects_owner,omitempty"`
	// Kustomization names the Flux Kustomization a status event observed.
	Kustomization string `json:"kustomization,omitempty"`
}

// PRObject handler names RepoObject targets with one-way sends.
const (
	PRHandlerCheckFailed         = "HandleCheckFailed"
	PRHandlerCheckPassed         = "HandleCheckPassed"
	PRHandlerCheckRerunRequested = "HandleCheckRerunRequested"
	PRHandlerCollaboratorAlert   = "HandleCollaboratorAlert"
	PRHandlerMerged              = "HandleMerged"
	PRHandlerRolloutProgress     = "HandleRolloutProgress"
	PRHandlerMainAdvanced        = "HandleMainAdvanced"
	PRHandlerDeployFailed        = "HandleDeployFailed"
	PRHandlerBuildMetadata       = "HandleBuildMetadata"
	PRHandlerSynchronized        = "HandleSynchronized"
	PRHandlerCheckStatus         = "HandleCheckStatus"
	// PRHandlerConflict and the handlers below are the merged
	// decide-commit-execute verdicts and the deploy-failure deadline pair.
	PRHandlerConflict              = "HandleConflict"
	PRHandlerDeploySuccess         = "HandleDeploySuccess"
	PRHandlerCIRecovered           = "HandleCIRecovered"
	PRHandlerDeployAttemptFailed   = "HandleDeployAttemptFailed"
	PRHandlerDeployFailureDeadline = "HandleDeployFailureDeadline"
)

// RepoObject handler names PRObject and RepoObject itself target with
// one-way (optionally delayed) sends.
const (
	RepoHandlerPRIndex    = "HandlePRIndex"
	RepoHandlerHeldReplay = "HandleHeldReplay"
	RepoHandlerHeldExpiry = "HandleHeldExpiry"
)

// SHA-only event kinds RepoObject routes through its SHA index.
const (
	SHAKindCheckRun    = "check_run"
	SHAKindWorkflowRun = "workflow_run"
	SHAKindPush        = "push"
	SHAKindImageReady  = "image_ready"
)

// shaRouteHandlers is the per-kind allowlist of PRObject handlers a SHA-only
// event may target. Only state-mutating handlers that are safe as one-way
// sends appear; verdict handlers need request-response and never route.
var shaRouteHandlers = map[string]map[string]bool{
	SHAKindCheckRun:    {PRHandlerCheckFailed: true, PRHandlerCheckPassed: true, PRHandlerCheckRerunRequested: true, PRHandlerCollaboratorAlert: true},
	SHAKindWorkflowRun: {PRHandlerCheckFailed: true, PRHandlerCheckPassed: true, PRHandlerCheckRerunRequested: true, PRHandlerCollaboratorAlert: true},
	SHAKindPush:        {PRHandlerMerged: true, PRHandlerRolloutProgress: true},
	SHAKindImageReady:  {PRHandlerRolloutProgress: true},
}

// IsSHARouteAllowed reports whether a SHA-only event of kind may target the
// PRObject handler.
func IsSHARouteAllowed(kind, handler string) bool {
	return shaRouteHandlers[kind][handler]
}

// Held SHA-only event timing. A miss is held until its SHA is indexed (a
// squash-merge push often lands before pull_request.closed carries the
// merge SHA) and expires after HeldSHAEventMaxAge via a delayed self-send.
// Indexing a held SHA schedules the replay as its own invocation after
// HeldReplayDelay. Both delays are data for Restate delayed sends, never
// slept on.
const (
	HeldSHAEventMaxAge = 30 * time.Minute
	HeldReplayDelay    = time.Second
)

// SHAEvent is the RepoObject payload for an event that carries only a
// commit SHA. RepoObject resolves the SHA to a PR and forwards Event to the
// PRObject Handler with the PR number (and branch, when Event has none)
// filled in. Event keeps the synapse effects_owner stamp untouched.
type SHAEvent struct {
	Repo    string  `json:"repo"`
	SHA     string  `json:"sha"`
	Handler string  `json:"handler"`
	Event   PREvent `json:"event"`
}

// PR index lifecycle ops carried by PRIndexEvent.
const (
	IndexActive    = "active"
	IndexMerged    = "merged"
	IndexClosed    = "closed"
	IndexDeploying = "deploying"
	IndexDeployed  = "deployed"
)

// PRIndexEvent is the one-way update PRObject sends RepoObject after a state
// commit that changes what RepoObject routes on: head or merge SHA, reopen
// epoch, terminal state, or deploy status.
type PRIndexEvent struct {
	Repo        string `json:"repo"`
	PRNumber    int    `json:"pr_number"`
	Branch      string `json:"branch,omitempty"`
	HeadSHA     string `json:"head_sha,omitempty"`
	MergeSHA    string `json:"merge_sha,omitempty"`
	ReopenEpoch uint64 `json:"reopen_epoch"`
	Generation  uint64 `json:"generation"`
	Lifecycle   string `json:"lifecycle"`
}

// HeldReplayRequest is the delayed RepoObject self-send that replays held
// events once their SHA is indexed.
type HeldReplayRequest struct {
	SHA string `json:"sha"`
}

// HeldExpiryRequest is the delayed RepoObject self-send that drops one held
// event if it is still waiting.
type HeldExpiryRequest struct {
	ID uint64 `json:"id"`
}

// SHARouteOutcome reports what RepoObject did with one SHA-only event.
type SHARouteOutcome struct {
	Forwarded bool
	PRNumber  int
	Held      bool
	HeldID    uint64
	Evicted   []HeldSHAEvent
}

// UnmarshalJSON decodes the wire payload and normalizes EffectsOwner, so
// every decoded PREvent carries legacy or restate, never empty.
func (ev *PREvent) UnmarshalJSON(data []byte) error {
	type wirePREvent PREvent
	var w wirePREvent
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	*ev = PREvent(w)
	ev.EffectsOwner = ParseEffectsOwner(string(ev.EffectsOwner))
	return nil
}

// UnmarshalJSON decodes the wire payload and normalizes EffectsOwner, so
// every decoded RepoEvent carries legacy or restate, never empty.
func (ev *RepoEvent) UnmarshalJSON(data []byte) error {
	type wireRepoEvent RepoEvent
	var w wireRepoEvent
	if err := json.Unmarshal(data, &w); err != nil {
		return err
	}
	*ev = RepoEvent(w)
	ev.EffectsOwner = ParseEffectsOwner(string(ev.EffectsOwner))
	return nil
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
	HandleMainAdvanced(ctx context.Context, ev PREvent) error
	HandleDeployFailed(ctx context.Context, ev PREvent) error
	HandleBuildMetadata(ctx context.Context, ev PREvent) error
	HandleSynchronized(ctx context.Context, ev PREvent) error
	HandleCheckStatus(ctx context.Context, ev PREvent) error
}

// RepoObject is the frozen repo handler surface: main-push index,
// dispatched-SHA tombstones, fan-out sends, the PRObject-fed routing index,
// SHA-only event routing with held replays, and durable SHA resolution.
type RepoObject interface {
	HandleMainPush(ctx context.Context, ev RepoEvent) error
	HandleFanout(ctx context.Context, ev RepoEvent) error
	HandleKustomizationStatus(ctx context.Context, ev RepoEvent) error
	HandlePRIndex(ctx context.Context, ev PRIndexEvent) error
	RouteBySHA(ctx context.Context, kind string, ev SHAEvent) (SHARouteOutcome, error)
	ReplayHeld(ctx context.Context, repo, sha string) error
	ExpireHeld(ctx context.Context, repo string, id uint64) (bool, error)
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

func (PRObjectStub) HandleMainAdvanced(context.Context, PREvent) error  { return nil }
func (PRObjectStub) HandleDeployFailed(context.Context, PREvent) error  { return nil }
func (PRObjectStub) HandleBuildMetadata(context.Context, PREvent) error { return nil }
func (PRObjectStub) HandleSynchronized(context.Context, PREvent) error  { return nil }
func (PRObjectStub) HandleCheckStatus(context.Context, PREvent) error   { return nil }

// RepoObjectStub is the empty registered handler stub for RepoObject.
type RepoObjectStub struct{}

// Compile-time surface freeze: stub must satisfy the full interface.
var _ RepoObject = RepoObjectStub{}

func (RepoObjectStub) HandleMainPush(context.Context, RepoEvent) error            { return nil }
func (RepoObjectStub) HandleFanout(context.Context, RepoEvent) error              { return nil }
func (RepoObjectStub) HandleKustomizationStatus(context.Context, RepoEvent) error { return nil }
func (RepoObjectStub) HandlePRIndex(context.Context, PRIndexEvent) error          { return nil }
func (RepoObjectStub) RouteBySHA(context.Context, string, SHAEvent) (SHARouteOutcome, error) {
	return SHARouteOutcome{}, nil
}
func (RepoObjectStub) ReplayHeld(context.Context, string, string) error { return nil }
func (RepoObjectStub) ExpireHeld(context.Context, string, uint64) (bool, error) {
	return false, nil
}

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

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
	"fmt"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/prwire"
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
	if current == "" || event == "" || prwire.IsZeroSHA(event) {
		return false
	}
	return !strings.EqualFold(current, event)
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
	// DispositionRecoverStale applies a synchronize whose stored head matches
	// neither its head nor its before SHA: PRObject missed an earlier
	// synchronize, so the head moves last-write-wins like the legacy row.
	DispositionRecoverStale EventDisposition = "recover_stale"
)

// authorAllowed reports whether author is on the auto-update allowlist.
func authorAllowed(p *AutoUpdatePolicy, author string) bool {
	clean := strings.TrimSpace(author)
	if p == nil || clean == "" {
		return false
	}
	for _, a := range p.Authors {
		if strings.EqualFold(clean, strings.TrimSpace(a)) {
			return true
		}
	}
	return false
}

// collaborator returns the alert rule for author, case-insensitively.
func collaborator(p *AutoUpdatePolicy, author string) (CollaboratorAlert, bool) {
	clean := strings.TrimSpace(author)
	if p == nil || clean == "" {
		return CollaboratorAlert{}, false
	}
	for _, c := range p.Collaborators {
		if strings.EqualFold(clean, strings.TrimSpace(c.Author)) && strings.TrimSpace(c.ChannelID) != "" && strings.TrimSpace(c.Mention) != "" {
			return c, true
		}
	}
	return CollaboratorAlert{}, false
}

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
	SHAKindCheckRun:    {prwire.PRHandlerCheckFailed: true, prwire.PRHandlerCheckPassed: true, prwire.PRHandlerCheckRerunRequested: true, prwire.PRHandlerCollaboratorAlert: true},
	SHAKindWorkflowRun: {prwire.PRHandlerCheckFailed: true, prwire.PRHandlerCheckPassed: true, prwire.PRHandlerCheckRerunRequested: true, prwire.PRHandlerCollaboratorAlert: true, prwire.PRHandlerDeployFailed: true},
	SHAKindPush:        {prwire.PRHandlerMerged: true, prwire.PRHandlerRolloutProgress: true},
	SHAKindImageReady:  {prwire.PRHandlerRolloutProgress: true},
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

// Package prwire is the PR lifecycle wire contract between synapse (the
// Restate ingress invoker) and vanadis-restate prstate (the PRObject and
// RepoObject Virtual Objects).
//
// Every type here is copied verbatim from prstate: field order, JSON tags
// and doc comments. The field order is load-bearing: prstate marshals these
// payloads inside Restate handlers (ObjectSend payloads, journaled state)
// and synapse digests the marshaled bytes into its send Idempotency-Key, so
// reordering or retagging a field changes bytes on the wire.
package prwire

import (
	"strings"

	"github.com/azylman/aerial/pkg/pubsub"
)

// Frozen Restate service names. Every agent's PRs live in these single
// objects: the owning agent is PRState data, never part of the service name.
const (
	PRObjectServiceName   = "PRObject"
	RepoObjectServiceName = "RepoObject"
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

// GitHub effect kinds beyond update-branch and rerun. The auto-update
// cascade is one repo-level effect per trigger; the conflict check is the
// per-PR mergeability read.
const (
	EffectGitHubAutoUpdate    = "github_auto_update"
	EffectGitHubConflictCheck = "github_conflict_check"
)

// Flux effect kinds. Git-push dispatch and untracked reconciles are
// repo-level (RepoObject); a PR rollout's reconcile requests are per-PR
// (PRObject). Image promotion uses EffectImagePromote.
const (
	EffectFluxGitPush   = "flux_git_push"
	EffectFluxReconcile = "flux_reconcile"
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

// Build-metadata snapshot kinds. BuildMetadataFinal is the terminal kind;
// every other recordable kind (manifest, progress) is phase 1.
const (
	BuildMetadataManifest = "manifest"
	BuildMetadataFinal    = "final"
	BuildMetadataResult   = "result"
	BuildMetadataProgress = "progress"
)

// ReadinessJobName scopes the readiness reconcile effect in match keys;
// the journaled key appends the retry ordinal.
const ReadinessJobName = "readiness"

// PRObject handler names.
const (
	PRHandlerCheckFailed         = "HandleCheckFailed"
	PRHandlerCheckPassed         = "HandleCheckPassed"
	PRHandlerCheckRerunRequested = "HandleCheckRerunRequested"
	PRHandlerCollaboratorAlert   = "HandleCollaboratorAlert"
	PRHandlerMerged              = "HandleMerged"
	PRHandlerOpened              = "HandleOpened"
	PRHandlerRolloutProgress     = "HandleRolloutProgress"
	PRHandlerMainAdvanced        = "HandleMainAdvanced"
	PRHandlerDeployFailed        = "HandleDeployFailed"
	PRHandlerBuildMetadata       = "HandleBuildMetadata"
	PRHandlerSynchronized        = "HandleSynchronized"
	PRHandlerSelfRegistered      = "HandleSelfRegistered"
	PRHandlerCheckStatus         = "HandleCheckStatus"
	// PRHandlerConflict and the handlers below are the merged
	// decide-commit-execute verdicts and the deploy-failure deadline pair.
	PRHandlerConflict              = "HandleConflict"
	PRHandlerDeploySuccess         = "HandleDeploySuccess"
	PRHandlerCIRecovered           = "HandleCIRecovered"
	PRHandlerDeployAttemptFailed   = "HandleDeployAttemptFailed"
	PRHandlerDeployFailureDeadline = "HandleDeployFailureDeadline"
	// PRHandlerConflictCheck is the per-PR mergeability check and its
	// delayed recheck; PRHandlerBranchConflict applies a conflict a branch
	// update observed; PRHandlerHeadRefMissing closes a PR whose head ref
	// vanished.
	PRHandlerConflictCheck  = "HandleConflictCheck"
	PRHandlerBranchConflict = "HandleBranchConflict"
	PRHandlerHeadRefMissing = "HandleHeadRefMissing"
	// PRHandlerKustomizationRequested records a rollout's reconcile
	// requests (and, restate-owned, patches them); a promoted rollout's
	// RepoObject forward targets it. PRHandlerRetryFluxReconcile is the
	// delayed retry of a failed PR reconcile.
	PRHandlerKustomizationRequested = "HandleKustomizationRequested"
	PRHandlerRetryFluxReconcile     = "RetryFluxReconcile"
	// PRHandlerDeployObserved applies one deploy-alert observation and
	// PRHandlerReconcileLost re-patches a lost reconcile request.
	PRHandlerDeployObserved = "HandleDeployObserved"
	PRHandlerReconcileLost  = "HandleReconcileLost"
	// PRHandlerConflictResolved, PRHandlerClosed, PRHandlerReopened and
	// PRHandlerReadinessRetry are the remaining one-way lifecycle events
	// synapse sends.
	PRHandlerConflictResolved = "HandleConflictResolved"
	PRHandlerClosed           = "HandleClosed"
	PRHandlerReopened         = "HandleReopened"
	PRHandlerReadinessRetry   = "HandleReadinessRetry"
	// PRHandlerRegister and PRHandlerRetryReadiness are the exclusive
	// request-response calls; PRHandlerGetDeployView and
	// PRHandlerGetBuildLifecycle are the shared read-only queries.
	PRHandlerRegister          = "Register"
	PRHandlerRetryReadiness    = "RetryReadiness"
	PRHandlerGetDeployView     = "GetDeployView"
	PRHandlerGetBuildLifecycle = "GetBuildLifecycle"
)

// RepoObject handler names.
const (
	RepoHandlerPRIndex    = "HandlePRIndex"
	RepoHandlerHeldReplay = "HandleHeldReplay"
	RepoHandlerHeldExpiry = "HandleHeldExpiry"
	// RepoHandlerAutoUpdate is the open-PR auto-update cascade and
	// RepoHandlerWorkflowRerun the workflow rerun-failed-jobs call; both
	// also receive their own delayed retries.
	RepoHandlerAutoUpdate    = "HandleAutoUpdate"
	RepoHandlerWorkflowRerun = "HandleWorkflowRerun"

	RepoHandlerGitPush        = "HandleGitPush"
	RepoHandlerImagePromotion = "HandleImagePromotion"

	// RepoHandlerRolloutIndex applies one PRObject rollout-index update on
	// the RolloutIndexKey object.
	RepoHandlerRolloutIndex = "HandleRolloutIndex"

	// RepoHandlerMainPush indexes a main push, RepoHandlerFanout fans a
	// repo event out to its PRs, and RepoHandlerKustomizationStatus applies
	// one Flux Kustomization status observation.
	RepoHandlerMainPush            = "HandleMainPush"
	RepoHandlerFanout              = "HandleFanout"
	RepoHandlerKustomizationStatus = "HandleKustomizationStatus"
	// RepoHandlerCheckRunBySHA, RepoHandlerWorkflowRunBySHA and
	// RepoHandlerPushBySHA are the SHA-only routing handlers: each wraps a
	// PRObject event RepoObject forwards once it resolves the SHA from its
	// durable index.
	RepoHandlerCheckRunBySHA    = "HandleCheckRunBySHA"
	RepoHandlerWorkflowRunBySHA = "HandleWorkflowRunBySHA"
	RepoHandlerPushBySHA        = "HandlePushBySHA"
	// RepoHandlerResolvePRBySHA and RepoHandlerListRollouts are the shared
	// read-only queries.
	RepoHandlerResolvePRBySHA = "ResolvePRBySHA"
	RepoHandlerListRollouts   = "ListRollouts"
)

// IsZeroSHA reports whether sha is empty or consists entirely of '0' characters.
func IsZeroSHA(sha string) bool {
	return strings.Trim(sha, "0") == ""
}

// PREvent is the canonical PRObject payload. It carries the head or merge SHA
// every cross-object send needs for the stale-SHA guard, plus the gate
// generation and reopen epoch the disposition logic compares.
// PREvent marshals with the snake_case wire format the synapse ingress
// invoker emits; the tags are load-bearing for cross-module interop.
type PREvent struct {
	Event          string `json:"event"`
	Repo           string `json:"repo"`
	PRNumber       int    `json:"pr_number"`
	Agent          string `json:"agent,omitempty"`
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
	// Kustomization names the Flux Kustomization a forwarded
	// kustomization status observed; empty for every other event.
	Kustomization string `json:"kustomization,omitempty"`
	// AutoUpdate is the auto-update policy a restate-owned
	// HandleCIRecovered carries: when the recovery applies, PRObject
	// forwards RepoObject.HandleAutoUpdate for the recovered check.
	AutoUpdate *AutoUpdatePolicy `json:"auto_update,omitempty"`

	// Registry row payload. Each field is omitted when empty, so events
	// that carry no registry data keep the frozen wire key set.
	//
	// BeforeSHA is the head a synchronize moved away from; the head only
	// advances when it matches the stored head (compare-and-set).
	BeforeSHA string `json:"before_sha,omitempty"`
	// TargetID is the Discord target synapse resolved for the PR.
	TargetID string `json:"target_id,omitempty"`
	// CheckState is the check-run state (passed/pending) synapse reports
	// with HandleCheckStatus.
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
	// BuildMetadata is one build-metadata snapshot for the PR revision.
	BuildMetadata *BuildMetadata `json:"build_metadata,omitempty"`
	// EriEffects are the rendered eri-bound effects synapse resolved for
	// this event (collaborator DMs today). PRObject executes them once
	// the event's transition commits.
	EriEffects []EriEffect `json:"eri_effects,omitempty"`
	// Facts are the rendered aerial.pr.> lifecycle facts synapse resolved
	// for this event. Verdict handlers and the deploy-failure deadline
	// publish them once their decision commits.
	Facts []pubsub.PRLifecycleEvent `json:"facts,omitempty"`
	// ExpectedJobs are the deploy jobs a deploy alert announced
	// (RecordExpectedJobs); HandleDeployObserved records them.
	ExpectedJobs []string `json:"expected_jobs,omitempty"`
	// ObservedReconcileAt is the lastHandledReconcileAt the watcher saw on
	// a Kustomization whose recorded reconcile request was lost; it scopes
	// the HandleReconcileLost re-patch key to that observation.
	ObservedReconcileAt string `json:"observed_reconcile_at,omitempty"`
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
	Repo       string `json:"repo"`
	MainSHA    string `json:"main_sha"`
	Generation uint64 `json:"generation"`
	PRNumber   int    `json:"pr_number"`
	HeadSHA    string `json:"head_sha"`
	// Kustomization names the Flux Kustomization a status event observed.
	Kustomization string `json:"kustomization,omitempty"`

	// GitHub effect payload, omitted when empty so other repo events keep
	// the frozen wire key set.
	//
	// CheckName is the check whose outcome triggered an auto-update
	// cascade or a workflow rerun; it scopes the repo effect key.
	CheckName string `json:"check_name,omitempty"`
	// RunID is the workflow run HandleWorkflowRerun reruns.
	RunID int64 `json:"run_id,omitempty"`
	// Attempt is the delayed-retry ordinal of a repo GitHub effect.
	Attempt int `json:"attempt,omitempty"`
	// AutoUpdate is the auto-update policy synapse rendered from its
	// config for HandleAutoUpdate.
	AutoUpdate *AutoUpdatePolicy `json:"auto_update,omitempty"`

	// Flux and image effect payload, omitted when empty.
	//
	// Ref is the pushed ref HandleGitPush forwards (main only).
	Ref string `json:"ref,omitempty"`
	// TargetID is the Discord target synapse resolved for the push or
	// promotion; it rides the Flux receiver payload.
	TargetID string `json:"target_id,omitempty"`
	// Branch is the PR head branch a promotion's rollout forward carries.
	Branch string `json:"branch,omitempty"`
	// Images are the image refs HandleImagePromotion promotes to HeadSHA.
	Images []string `json:"images,omitempty"`
	// Jobs are the deploy jobs a promoted PR rollout starts.
	Jobs []string `json:"jobs,omitempty"`
	// KustomizationRequests are the reconcile requests a promotion's
	// rollout patches, keyed by Kustomization name.
	KustomizationRequests map[string]KustomizationRequest `json:"kustomization_requests,omitempty"`
	// Facts ride the PRObject rollout forward a successful promotion sends
	// (the deploy_started fact).
	Facts []pubsub.PRLifecycleEvent `json:"facts,omitempty"`
	// DeployFailedFacts arm the PRObject deploy-failure deadline when a
	// restate-owned promotion fails; the failure fills their details.
	DeployFailedFacts []pubsub.PRLifecycleEvent `json:"deploy_failed_facts,omitempty"`
}

// AutoUpdatePolicy is the stateless config the auto-update cascade needs:
// which PR authors get automatic branch updates, whether this deployment is
// the default owner (every eligible open PR, not only indexed ones), and the
// collaborator alert rules for the repo's conflict DMs.
type AutoUpdatePolicy struct {
	Authors       []string            `json:"authors,omitempty"`
	DefaultOwner  bool                `json:"default_owner,omitempty"`
	Collaborators []CollaboratorAlert `json:"collaborators,omitempty"`
}

// CollaboratorAlert routes one collaborator's conflict DM: the channel and
// the mention to prefix.
type CollaboratorAlert struct {
	Author    string `json:"author"`
	ChannelID string `json:"channel_id"`
	Mention   string `json:"mention"`
}

// SHAEvent is the RepoObject payload for an event that carries only a
// commit SHA. RepoObject resolves the SHA to a PR and forwards Event to the
// PRObject Handler with the PR number (and branch, when Event has none)
// filled in.
type SHAEvent struct {
	Repo    string  `json:"repo"`
	SHA     string  `json:"sha"`
	Handler string  `json:"handler"`
	Event   PREvent `json:"event"`
}

// ResolveSHAResponse is the SHA lookup result.
type ResolveSHAResponse struct {
	PRNumber int    `json:"pr_number"`
	Branch   string `json:"branch"`
	Found    bool   `json:"found"`
}

// KustomizationRequest is the reconcile request recorded for one Flux
// Kustomization of a PR rollout (metadata.kustomization_requests values).
type KustomizationRequest struct {
	Namespace   string `json:"namespace"`
	RequestedAt string `json:"requested_at"`
}

// BuildSelection is one selected image in a build-metadata snapshot.
type BuildSelection struct {
	Service   string `json:"service"`
	ImageName string `json:"image_name"`
}

// BuildResult is one per-image result in a build-metadata snapshot.
type BuildResult struct {
	Service       string   `json:"service"`
	ImageName     string   `json:"image_name"`
	State         string   `json:"state"`
	Pushed        bool     `json:"pushed"`
	SourceAttempt int      `json:"source_attempt"`
	ImageRefs     []string `json:"image_refs,omitempty"`
	Digest        string   `json:"digest,omitempty"`
}

// BuildMetadata is the build-metadata snapshot stored in
// metadata.build_metadata. It is an untrusted transport snapshot until the
// caller validates its provenance against authoritative workflow and PR API
// evidence.
type BuildMetadata struct {
	Version       int              `json:"version"`
	Kind          string           `json:"kind"`
	Repository    string           `json:"repository"`
	WorkflowPath  string           `json:"workflow_path"`
	RunID         int64            `json:"run_id"`
	RunNumber     int64            `json:"run_number"`
	RunAttempt    int              `json:"run_attempt"`
	Event         string           `json:"event"`
	SourceSHA     string           `json:"source_sha"`
	ExecutionSHA  string           `json:"execution_sha"`
	PRNumber      *int             `json:"pr_number"`
	Selected      []BuildSelection `json:"selected"`
	Results       []BuildResult    `json:"results,omitempty"`
	Service       string           `json:"service,omitempty"`
	ImageName     string           `json:"image_name,omitempty"`
	State         string           `json:"state,omitempty"`
	Pushed        bool             `json:"pushed,omitempty"`
	SourceAttempt int              `json:"source_attempt,omitempty"`
	ImageRefs     []string         `json:"image_refs,omitempty"`
	Digest        string           `json:"digest,omitempty"`
}

// RegisterRequest is the PRObject.Register input: the normalized synapse
// register payload plus the owner fence and the headless target fallback.
type RegisterRequest struct {
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
	Branch   string `json:"branch"`
	HeadSHA  string `json:"head_sha"`
	TargetID string `json:"target_id"`
	Title    string `json:"title,omitempty"`
	Agent    string `json:"agent,omitempty"`
	BotID    string `json:"bot_id,omitempty"`
	// Metadata is the string-valued register metadata synapse forwards
	// opaquely; ApplyRegister records the typed keys it knows.
	Metadata map[string]string `json:"metadata,omitempty"`
	// FallbackTargetID is the system channel used when neither the request
	// nor the stored row carries a valid Discord snowflake.
	FallbackTargetID string `json:"fallback_target_id,omitempty"`
	// EnforceOwner turns on the existing-owner fence: synapse authorized the
	// caller to update ExpectedOwner (empty means no existing owner), and a
	// PR already owned by someone else is rejected.
	EnforceOwner  bool   `json:"enforce_owner,omitempty"`
	ExpectedOwner string `json:"expected_owner,omitempty"`
}

// RegisterResponse is the PRObject.Register result.
type RegisterResponse struct {
	TargetID string `json:"target_id"`
	Agent    string `json:"agent"`
	Status   string `json:"status"`
}

// BuildLifecycle is the PRObject.GetBuildLifecycle result: the registry
// lifecycle fence (LoadPRBuildLifecycle) plus the fenced build metadata
// snapshot (LoadPRBuildMetadata).
type BuildLifecycle struct {
	Found            bool           `json:"found"`
	Status           string         `json:"status"`
	HeadSHA          string         `json:"head_sha"`
	MergeSHA         string         `json:"merge_sha"`
	Started          bool           `json:"started"`
	HasBuildMetadata bool           `json:"has_build_metadata"`
	BuildMetadata    *BuildMetadata `json:"build_metadata,omitempty"`
	// Agent is the stored owner, empty when no authoritative source has
	// claimed the PR yet, so synapse shadow lookups can compare owners.
	Agent string `json:"agent,omitempty"`
}

// DeployView is the per-PR deploy read model. Found is false for a PR
// Restate has never seen and for an untracked PR (PRState.Untracked), for
// which the legacy registry keeps no row either.
type DeployView struct {
	Found                  bool                            `json:"found"`
	Repo                   string                          `json:"repo"`
	PRNumber               int                             `json:"pr_number"`
	Status                 string                          `json:"status"`
	Branch                 string                          `json:"branch"`
	HeadSHA                string                          `json:"head_sha"`
	MergeSHA               string                          `json:"merge_sha"`
	TargetID               string                          `json:"target_id"`
	Agent                  string                          `json:"agent,omitempty"`
	Jobs                   []string                        `json:"jobs,omitempty"`
	DeployedJobs           []string                        `json:"deployed_jobs,omitempty"`
	KustomizationRequests  map[string]KustomizationRequest `json:"kustomization_requests,omitempty"`
	DeployedKustomizations []string                        `json:"deployed_kustomizations,omitempty"`
	DeploySuccessPending   bool                            `json:"deploy_success_pending,omitempty"`
	HasExpectedDeployJobs  bool                            `json:"has_expected_deploy_jobs,omitempty"`
}

// RolloutRef names one indexed rollout.
type RolloutRef struct {
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
}

// RetryReadinessRequest is the PRObject.RetryReadiness input: the merge SHA
// the operator expects the gate at, the caller's agent hint, and the
// existing-owner fence
// (EnforceOwner: a stored owner must equal ExpectedOwner, the owner synapse
// authorized the caller for), exactly like Register.
type RetryReadinessRequest struct {
	MergeSHA      string `json:"merge_sha"`
	Agent         string `json:"agent,omitempty"`
	EnforceOwner  bool   `json:"enforce_owner,omitempty"`
	ExpectedOwner string `json:"expected_owner,omitempty"`
}

// RetryReadinessResponse is the readiness decision synapse's
// /internal/pr/retry-readiness returns: 202 with the re-requested
// Kustomizations when a matching merged rollout gate exists, 409 otherwise.
type RetryReadinessResponse struct {
	Status    int                             `json:"status"`
	Requests  map[string]KustomizationRequest `json:"kustomization_requests,omitempty"`
	EffectKey string                          `json:"effect_key,omitempty"`
}

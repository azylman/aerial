package model

import (
	"context"
	"errors"
	"time"

	"github.com/bwmarrin/discordgo"
)

var ErrInvalidTargetID = errors.New("target_id must be a valid Discord snowflake (17-20 digits)")

// DeployingPR holds metadata for a PR currently undergoing deployment rollout.
type DeployingPR struct {
	ID       int64          `json:"id"`
	Repo     string         `json:"repo"`
	PRNumber int            `json:"pr_number"`
	Branch   string         `json:"branch"`
	HeadSHA  string         `json:"head_sha"`
	MergeSHA string         `json:"merge_sha"`
	TargetID string         `json:"target_id"`
	Metadata map[string]any `json:"metadata"`
}

// PRRegisterRequest defines the request payload for POST /internal/pr/register.
type PRRegisterRequest struct {
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
	Branch   string `json:"branch"`
	HeadSHA  string `json:"head_sha"`
	TargetID string `json:"target_id"`
	Title    string `json:"title,omitempty"`
	Metadata string `json:"metadata,omitempty"`
}

// PRBuildLifecycle is the registry lifecycle fence image-build consumers read
// without inspecting transport metadata.
type PRBuildLifecycle struct {
	Status   string
	HeadSHA  string
	MergeSHA string
	Started  bool
}

// KustomizationRequest is the reconcile request recorded for one Flux Kustomization of a PR rollout.
type KustomizationRequest struct {
	Namespace   string `json:"namespace"`
	RequestedAt string `json:"requested_at"`
}

// PRRegistryUpdater defines the interface for updating PR lifecycle state in PostgreSQL pr_registry.
type PRRegistryUpdater interface {
	LoadPRBuildLifecycle(ctx context.Context, repo string, prNumber int) (PRBuildLifecycle, error)
	RecordPRBuildMetadata(ctx context.Context, repo string, prNumber int, metadata BuildMetadata) (bool, error)
	LoadPRBuildMetadata(ctx context.Context, repo string, prNumber int) (BuildMetadata, bool, error)
	RegisterPR(ctx context.Context, req PRRegisterRequest) (string, error)
	UpdatePRMerged(ctx context.Context, repo string, prNumber int, mergeSHA string) (targetID string, err error)
	UpdatePRSync(ctx context.Context, repo string, prNumber int, headSHA string) (targetID string, err error)
	UpdatePRClosedUnmerged(ctx context.Context, repo string, prNumber int) (targetID string, err error)
	AtomicTransitionCIFailed(ctx context.Context, repo string, prNumber int) (targetID string, updated bool, err error)
	AtomicTransitionCIFailedByHeadSHA(ctx context.Context, repo string, headSHA string) (targetID string, prNumber int, updated bool, err error)
	BackfillPushMergeSHA(ctx context.Context, repo string, prNumber int, mergeSHA string) (targetID string, err error)
	ResolvePRBySHA(ctx context.Context, repo string, sha string) (prNumber int, branch, targetID string, err error)
	ResolvePRByNumber(ctx context.Context, repo string, prNumber int) (targetID string, branch, headSHA, mergeSHA string, err error)
	AtomicTransitionConflict(ctx context.Context, repo string, prNumber int) (targetID string, updated bool, err error)
	TransitionDeploying(ctx context.Context, repo string, prNumber int, jobs []string) (targetID string, err error)
	AtomicTransitionDeployFailed(ctx context.Context, repo string, prNumber int) (targetID string, updated bool, err error)
	ListDeployingPRs(ctx context.Context) ([]DeployingPR, error)
	RecordPRCheckStatus(ctx context.Context, repo string, prNumber int, headSHA, checkName, checkState string) (bool, error)
	EnsurePRRegistered(ctx context.Context, repo string, prNumber int, branch, headSHA, title, targetID string) (string, error)
	RecordExpectedJobs(ctx context.Context, repo string, prNumber int, jobs []string) (bool, error)
	HasExpectedDeployJobs(ctx context.Context, repo string, prNumber int) (bool, error)
	RecordPRJobState(ctx context.Context, jobName, jobState string) (repo string, prNumber int, updated bool, err error)
	AppendPRJobs(ctx context.Context, repo string, prNumber int, jobs []string) error
	RecordKustomizationRequests(ctx context.Context, repo string, prNumber int, requests map[string]KustomizationRequest) error
	MarkKustomizationsDeployed(ctx context.Context, repo string, prNumber int, names []string) (transitioned bool, err error)
	// ClaimDeploySuccess atomically claims the deploy_success publish for one
	// pod: it flips the row to deployed with deploy_success_pending and
	// deploy_success_claimed set, performing the merged/synced/deploying to
	// deployed transition itself for unowned/no-deploy paths. It returns the
	// target and true only to the pod that first claimed this generation; a
	// re-claim of an already-claimed row returns the target with false so a
	// crash-recovery pass can republish at most once.
	ClaimDeploySuccess(ctx context.Context, repo string, prNumber int) (targetID string, claimed bool, err error)
	// AcknowledgeDeploySuccessPublished clears the deploy_success claim after
	// publish so the row stops re-entering the deploy watcher.
	AcknowledgeDeploySuccessPublished(ctx context.Context, repo string, prNumber int) error
	// RecordFailedCheckRun records one failed check run in durable check_runs
	// metadata, resetting the sets when headSHA changes generation. It returns
	// emit/was-active/had-prior semantics equivalent to the in-memory
	// granular tracker: emitAdditional is true when this failure newly arms an
	// alert for a generation that already failed before.
	RecordFailedCheckRun(ctx context.Context, repo string, prNumber int, headSHA, checkKey string, isRoboRev bool) (emitAdditional, wasActive, hadPriorEverFailed bool, err error)
	// RollbackFailedCheckRun undoes a RecordFailedCheckRun whose dispatch
	// failed, restoring the prior active/ever sets. It is a no-op when
	// wasActive is true.
	RollbackFailedCheckRun(ctx context.Context, repo string, prNumber int, headSHA, checkKey string, isRoboRev bool, wasActive, hadPriorEverFailed bool) error
	// RecoverNonGateCheckSuccess clears one check from the active failure set
	// on success and returns true when no non-roborev failure remains active
	// and no roborev failure is active.
	RecoverNonGateCheckSuccess(ctx context.Context, repo string, prNumber int, headSHA, checkKey string) (recovered bool, err error)
	// RecordConflictPublished atomically records a conflict lifecycle publish
	// for the head SHA, returning true when this call newly recorded it.
	RecordConflictPublished(ctx context.Context, repo string, prNumber int, headSHA string) (isNew bool, err error)
	WasConflictPublished(ctx context.Context, repo string, prNumber int, headSHA string) (bool, error)
	ClearConflictPublished(ctx context.Context, repo string, prNumber int) error
	// MarkCollaboratorAlerted atomically records a collaborator CI alert for
	// the head SHA, returning true when this call newly recorded it.
	MarkCollaboratorAlerted(ctx context.Context, repo string, prNumber int, headSHA string) (isNew bool, err error)
	ClearCollaboratorAlerted(ctx context.Context, repo string, prNumber int) error
	// MarkCollaboratorConflict atomically records a collaborator conflict
	// alert for the head SHA, returning true when newly recorded.
	MarkCollaboratorConflict(ctx context.Context, repo string, prNumber int, headSHA string) (isNew bool, err error)
	ClearCollaboratorConflict(ctx context.Context, repo string, prNumber int) error
	// NoteMainPush records a main-branch push at repo granularity and clears
	// every tracked PR's updated-for-main flag. It returns true only when the
	// push SHA is newly seen for the repo; pushes with no PR rows are still
	// recorded and redeliveries are no-ops.
	NoteMainPush(ctx context.Context, repo string, sha string) (isNew bool, err error)
	LoadMainPushSHA(ctx context.Context, repo string) (string, error)
	MarkUpdatedForMain(ctx context.Context, repo string, prNumber int, mainSHA string) error
	WasUpdatedForMain(ctx context.Context, repo string, prNumber int, mainSHA string) (bool, error)
	ClearUpdatedForMain(ctx context.Context, repo string, prNumber int) error
	HasKustomizationRequests(ctx context.Context, repo string, prNumber int) (bool, error)
	UpdatePRStatus(ctx context.Context, repo string, prNumber int, status string, metadata map[string]any) error
	AcknowledgeDeployFailedPublished(ctx context.Context, repo string, prNumber int) error
	LoadRawPRMetadata(ctx context.Context, repo string, prNumber int) (map[string]any, error)
	IsPRDeployed(ctx context.Context, repo string, prNumber int) (bool, error)
}

// ReadinessRetryStore defines the interface for retrying PR readiness rollout gates.
type ReadinessRetryStore interface {
	RetryPRReadiness(ctx context.Context, repo, merge string, number int) (map[string]KustomizationRequest, error)
}

// DeployEvent represents a deployment or sync lifecycle event.
type DeployEvent struct {
	Event        string    `json:"event"`
	JobName      string    `json:"job_name"`
	SourceKind   string    `json:"source_kind,omitempty"`
	ExpectedJobs []string  `json:"expected_jobs,omitempty"`
	Repo         string    `json:"repo,omitempty"`
	CommitSHA    string    `json:"commit_sha,omitempty"`
	PRNumber     int       `json:"pr_number,omitempty"`
	TargetID     string    `json:"target_id,omitempty"`
	Image        string    `json:"image,omitempty"`
	Digest       string    `json:"digest,omitempty"`
	Status       string    `json:"status"`
	DeploymentID string    `json:"deployment_id,omitempty"`
	Details      string    `json:"details,omitempty"`
	Timestamp    time.Time `json:"timestamp"`
}

// FluxInvolvedObject identifies the resource associated with a Flux notification event.
type FluxInvolvedObject struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// FluxAlertEvent represents an alert event payload emitted by Flux notification-controller.
type FluxAlertEvent struct {
	InvolvedObject FluxInvolvedObject `json:"involvedObject"`
	Severity       string             `json:"severity"`
	Timestamp      time.Time          `json:"timestamp"`
	Message        string             `json:"message"`
	Reason         string             `json:"reason"`
	Metadata       map[string]string  `json:"metadata,omitempty"`
	Repo           string             `json:"repo,omitempty"`
}

// FluxKustomizationRef identifies a Flux Kustomization object.
type FluxKustomizationRef struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

// FluxSourceRef is a Kustomization spec.sourceRef.
type FluxSourceRef struct {
	Kind      string `json:"kind"`
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// FluxCondition represents a condition status in Flux and Kubernetes resources.
type FluxCondition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// KustomizationObservation is the live state of one Flux Kustomization as seen by a LIST or WATCH.
type KustomizationObservation struct {
	Ref                    FluxKustomizationRef
	Suspended              bool
	RequestedAt            string
	LastHandledReconcileAt string
	LastAppliedRevision    string
	SourceRef              FluxSourceRef
	Conditions             []FluxCondition
}

// WorkloadRollout is the rollout state of one Deployment, StatefulSet, DaemonSet, RestateDeployment or RestateCluster.
type WorkloadRollout struct {
	UID                                                             string
	Selector                                                        *KubeLabelSelector
	StoppedChildren                                                 *StoppedChildObservation
	Kind, Namespace, Name                                           string
	Images                                                          []string
	ChangedAt                                                       time.Time
	Annotations                                                     map[string]string
	ConfigMapRefs                                                   []string
	Generation, ObservedGeneration                                  int64
	Replicas, UpdatedReplicas, AvailableReplicas, StatusReplicas    int32
	UpdateRevision, CurrentRevision                                 string
	DesiredNumberScheduled, UpdatedNumberScheduled, NumberAvailable int32
	ProgressDeadlineExceeded                                        bool
	ReadyReplicas                                                   int32
	DeploymentID                                                    string
	ReadyConditionTrue                                              bool
	ManagedWorkloads                                                []WorkloadRollout
	Env                                                             map[string]string
	Resources                                                       KubeResourceRequirements
}

// ConfigMapChange records when a Flux-managed ConfigMap was last written.
type ConfigMapChange struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	ChangedAt time.Time `json:"changed_at"`
}

// WorkloadImageRef is the subset of a workload needed to map images to their owning Kustomization.
type WorkloadImageRef struct {
	Labels map[string]string `json:"labels"`
	Images []string          `json:"images"`
}

// KubeLabelSelector defines Kubernetes label match requirements.
type KubeLabelSelector struct {
	MatchLabels      map[string]string `json:"matchLabels"`
	MatchExpressions []struct {
		Key      string   `json:"key"`
		Operator string   `json:"operator"`
		Values   []string `json:"values"`
	} `json:"matchExpressions"`
}

// KubeOwnerReference represents a Kubernetes metadata.ownerReferences item.
type KubeOwnerReference struct {
	APIVersion string `json:"apiVersion"`
	Kind       string `json:"kind"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Controller bool   `json:"controller"`
}

// KubeManagedField is one metadata.managedFields entry.
type KubeManagedField struct {
	Time        time.Time `json:"time"`
	Subresource string    `json:"subresource"`
}

// KubeObjectMeta is the object metadata read from Kubernetes workloads and ConfigMaps.
type KubeObjectMeta struct {
	UID             string               `json:"uid"`
	Labels          map[string]string    `json:"labels"`
	OwnerReferences []KubeOwnerReference `json:"ownerReferences"`
	Name            string               `json:"name"`
	Namespace       string               `json:"namespace"`
	Generation      int64                `json:"generation"`
	Annotations     map[string]string    `json:"annotations"`
	ManagedFields   []KubeManagedField   `json:"managedFields"`
}

// KubeResourceRequirements defines CPU and memory limits and requests.
type KubeResourceRequirements struct {
	Limits   map[string]string `json:"limits,omitempty"`
	Requests map[string]string `json:"requests,omitempty"`
}

// StoppedReplicaSet represents a ReplicaSet examined during stopped workload checks.
type StoppedReplicaSet struct {
	Metadata KubeObjectMeta `json:"metadata"`
	Spec     struct {
		Replicas *int32 `json:"replicas"`
	} `json:"spec"`
	Status struct {
		ObservedGeneration   int64 `json:"observedGeneration"`
		Replicas             int32 `json:"replicas"`
		ReadyReplicas        int32 `json:"readyReplicas"`
		AvailableReplicas    int32 `json:"availableReplicas"`
		FullyLabeledReplicas int32 `json:"fullyLabeledReplicas"`
	} `json:"status"`
}

// StoppedPod represents a Pod examined during stopped workload checks.
type StoppedPod struct {
	Metadata KubeObjectMeta `json:"metadata"`
}

// StoppedChildObservation proves both inventories were read completely and the parent was re-read.
type StoppedChildObservation struct {
	ReplicaSets []StoppedReplicaSet
	Pods        []StoppedPod
}

// GitHubWebhookArgs contains payload data for durable background processing of GitHub webhooks.
type GitHubWebhookArgs struct {
	Event    string `json:"event"`
	Delivery string `json:"delivery"`
	Body     []byte `json:"body"`
}

// DeployWebhookArgs contains payload data for durable background processing of deployment webhooks.
type DeployWebhookArgs struct {
	IdempotencyKey string      `json:"idempotency_key,omitempty"`
	Event          DeployEvent `json:"event"`
}

// DiscordMessageIngressRequest represents the payload delivered to Eri for message ingress.
type DiscordMessageIngressRequest struct {
	BotUserID string             `json:"bot_user_id,omitempty"`
	RoleNames map[string]string  `json:"role_names,omitempty"`
	Message   *discordgo.Message `json:"message"`
}

// DiscordIngressArgs contains payload data for JetStream queueing of Discord message ingress events.
type DiscordIngressArgs struct {
	MessageID string             `json:"message_id"`
	BotUserID string             `json:"bot_user_id,omitempty"`
	RoleNames map[string]string  `json:"role_names,omitempty"`
	Message   *discordgo.Message `json:"message"`
}

// DiscordInteractionArgs contains payload data for JetStream queueing of Discord interaction events.
type DiscordInteractionArgs struct {
	InteractionID string                 `json:"interaction_id"`
	Interaction   *discordgo.Interaction `json:"interaction"`
}

type GitPushEventRequest struct {
	Repo     string `json:"repo"`
	Ref      string `json:"ref"`
	Commit   string `json:"commit"`
	TargetID string `json:"target_id,omitempty"`
	PRNumber int    `json:"pr_number,omitempty"`
}

type ImageReadyEventRequest struct {
	Image     string `json:"image"`
	Digest    string `json:"digest,omitempty"`
	Repo      string `json:"repo,omitempty"`
	CommitSHA string `json:"commit_sha,omitempty"`
	PRNumber  int    `json:"pr_number,omitempty"`
	TargetID  string `json:"target_id,omitempty"`
}

type PromptRequest struct {
	ChannelID string `json:"channel_id"`
	Prompt    string `json:"prompt"`
	Effort    string `json:"effort,omitempty"`
}

type DispatchRequest struct {
	ID           string `json:"id,omitempty"`
	Source       string `json:"source"`
	SessionID    string `json:"session_id,omitempty"`
	TargetID     string `json:"target_id,omitempty"`
	Prompt       string `json:"prompt"`
	Effort       string `json:"effort,omitempty"`
	SkipIfActive bool   `json:"skip_if_active,omitempty"`
}

type DirectMessageRequest struct {
	ChannelID string `json:"channel_id"`
	Content   string `json:"content"`
}

// OutboundDispatcher delivers outbound events, prompts, and cluster updates.
type OutboundDispatcher interface {
	DispatchGitPush(ctx context.Context, req GitPushEventRequest) error
	DiscoverImageKustomizations(ctx context.Context, imageNames []string) ([]FluxKustomizationRef, error)
	PromoteImageTag(ctx context.Context, req ImageReadyEventRequest) (workloadName string, err error)
	ReconcileKustomization(ctx context.Context, ref FluxKustomizationRef, requestedAt string) error
	ListKustomizationWorkloads(ctx context.Context, ref FluxKustomizationRef) ([]WorkloadRollout, error)
	ListKustomizationConfigMaps(ctx context.Context, ref FluxKustomizationRef) ([]ConfigMapChange, error)
	ResolveFluxSourceRepo(ctx context.Context, obj FluxInvolvedObject) (string, error)
	DiscoverRepoKustomizations(ctx context.Context, repo string) ([]FluxKustomizationRef, error)
	DispatchPrompt(ctx context.Context, req PromptRequest) error
	DispatchDirectMessage(ctx context.Context, req DirectMessageRequest) error
	DispatchBackground(ctx context.Context, req DispatchRequest) error
	DispatchDiscordMessage(ctx context.Context, req DiscordMessageIngressRequest) error
	DispatchDiscordInteraction(ctx context.Context, interaction *discordgo.Interaction) error
}

type GitHubPRRef struct {
	Ref string `json:"ref"`
	SHA string `json:"sha"`
}

type GitHubPullRequest struct {
	NodeID         string      `json:"node_id"`
	Number         int         `json:"number"`
	State          string      `json:"state"`
	Title          string      `json:"title"`
	Body           string      `json:"body"`
	Head           GitHubPRRef `json:"head"`
	Base           GitHubPRRef `json:"base"`
	Draft          bool        `json:"draft"`
	Merged         bool        `json:"merged"`
	MergeCommitSHA string      `json:"merge_commit_sha"`
	Mergeable      *bool       `json:"mergeable,omitempty"`
	MergeableState string      `json:"mergeable_state,omitempty"`
	User           struct {
		Login string `json:"login"`
	} `json:"user"`
}

// AerialPRMetadata captures structured metadata trailers in PR descriptions.
type AerialPRMetadata struct {
	TargetID string `json:"target_id,omitempty"`
	BotID    string `json:"bot_id,omitempty"`
}

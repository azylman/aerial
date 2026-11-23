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
	Jobs     []string       `json:"jobs,omitempty"`
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
	BotID    string `json:"bot_id,omitempty"`
	Agent    string `json:"agent,omitempty"`
}

// OwningAgent returns the explicit Agent if set, otherwise falling back to BotID.
func (r PRRegisterRequest) OwningAgent() string {
	if r.Agent != "" {
		return r.Agent
	}
	return r.BotID
}

// PRBuildLifecycle is the PRObject lifecycle fence image-build consumers read
// without inspecting transport metadata.
type PRBuildLifecycle struct {
	Status   string
	HeadSHA  string
	MergeSHA string
	Started  bool
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
	DeletionTimestamp                                               string
	Finalizers                                                      []string
}

// ConfigMapChange records when a Flux-managed ConfigMap was last written.
type ConfigMapChange struct {
	Namespace string    `json:"namespace"`
	Name      string    `json:"name"`
	ChangedAt time.Time `json:"changed_at"`
}

// WorkloadImageRef is the subset of a workload needed to map images to their owning Kustomization.
type WorkloadImageRef struct {
	Labels            map[string]string `json:"labels"`
	Images            []string          `json:"images"`
	DeletionTimestamp string            `json:"deletion_timestamp,omitempty"`
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
	UID               string               `json:"uid"`
	Labels            map[string]string    `json:"labels"`
	OwnerReferences   []KubeOwnerReference `json:"ownerReferences"`
	Name              string               `json:"name"`
	Namespace         string               `json:"namespace"`
	Generation        int64                `json:"generation"`
	Annotations       map[string]string    `json:"annotations"`
	ManagedFields     []KubeManagedField   `json:"managedFields"`
	DeletionTimestamp string               `json:"deletionTimestamp,omitempty"`
	Finalizers        []string             `json:"finalizers,omitempty"`
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
	Agent     string             `json:"agent,omitempty"`
	BotUserID string             `json:"bot_user_id,omitempty"`
	RoleNames map[string]string  `json:"role_names,omitempty"`
	Message   *discordgo.Message `json:"message"`
}

// DiscordIngressArgs contains payload data for JetStream queueing of Discord message ingress events.
type DiscordIngressArgs struct {
	MessageID string             `json:"message_id"`
	Agent     string             `json:"agent,omitempty"`
	BotUserID string             `json:"bot_user_id,omitempty"`
	RoleNames map[string]string  `json:"role_names,omitempty"`
	Message   *discordgo.Message `json:"message"`
}

// DiscordInteractionArgs contains payload data for JetStream queueing of Discord interaction events.
type DiscordInteractionArgs struct {
	InteractionID string                 `json:"interaction_id"`
	Agent         string                 `json:"agent,omitempty"`
	Interaction   *discordgo.Interaction `json:"interaction"`
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

// OutboundDispatcher delivers outbound events and prompts and reads cluster
// state; it never writes to Kubernetes.
type OutboundDispatcher interface {
	DiscoverImageKustomizations(ctx context.Context, imageNames []string) ([]FluxKustomizationRef, error)
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

func (pr GitHubPullRequest) GetBody() string {
	return pr.Body
}

func (pr GitHubPullRequest) GetNumber() int {
	return pr.Number
}

// AerialPRMetadata captures structured metadata trailers in PR descriptions.
type AerialPRMetadata struct {
	TargetID string `json:"target_id,omitempty"`
	BotID    string `json:"bot_id,omitempty"`
	Agent    string `json:"agent,omitempty"`
}

// OK reports whether any metadata fields are set.
func (m AerialPRMetadata) OK() bool {
	return m.TargetID != "" || m.BotID != "" || m.Agent != ""
}

// OwningAgent returns the explicit Agent if set, otherwise falling back to BotID.
func (m AerialPRMetadata) OwningAgent() string {
	if m.Agent != "" {
		return m.Agent
	}
	return m.BotID
}

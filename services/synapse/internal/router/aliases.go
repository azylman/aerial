package router

import (
	"github.com/azylman/aerial/services/synapse/internal/alerts"
	"github.com/azylman/aerial/services/synapse/internal/buildmeta"
	"github.com/azylman/aerial/services/synapse/internal/config"
	"github.com/azylman/aerial/services/synapse/internal/flux"
	"github.com/azylman/aerial/services/synapse/internal/kube"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

type (
	ConfigSource                 = config.ConfigSource
	Config                       = config.Config
	CollaboratorCIAlertRule      = config.CollaboratorCIAlertRule
	OutboundDispatcher           = model.OutboundDispatcher
	ConsumerStatsQuerier         = metrics.ConsumerStatsQuerier
	consumerStatsQuerier         = metrics.ConsumerStatsQuerier
	workloadRollout              = model.WorkloadRollout
	configMapChange              = model.ConfigMapChange
	fluxCondition                = model.FluxCondition
	prRolloutInput               = flux.PRRolloutInput
	fluxKustomizationRef         = model.FluxKustomizationRef
	DeployEvent                  = model.DeployEvent
	DeployWebhookArgs            = model.DeployWebhookArgs
	GitHubWebhookArgs            = model.GitHubWebhookArgs
	DiscordIngressArgs           = model.DiscordIngressArgs
	DiscordInteractionArgs       = model.DiscordInteractionArgs
	DiscordMessageIngressRequest = model.DiscordMessageIngressRequest
	GitPushEventRequest          = model.GitPushEventRequest
	ImageReadyEventRequest       = model.ImageReadyEventRequest
	PromptRequest                = model.PromptRequest
	DispatchRequest              = model.DispatchRequest
	DirectMessageRequest         = model.DirectMessageRequest
	PRBuildLifecycle             = model.PRBuildLifecycle
	DeployingPR                  = model.DeployingPR
	PRRegisterRequest            = model.PRRegisterRequest
	FluxAlertEvent               = model.FluxAlertEvent
	FluxInvolvedObject           = model.FluxInvolvedObject
	EventWebhookRequest          = alerts.EventWebhookRequest
	VMAlertNotification          = alerts.VMAlertNotification
	WorkloadRollout              = model.WorkloadRollout
	ConfigMapChange              = model.ConfigMapChange
	FluxKustomizationRef         = model.FluxKustomizationRef
	KustomizationObservation     = model.KustomizationObservation
	GitHubCheckRunsResponse      = model.GitHubCheckRunsResponse
	GitHubMetadataCheck          = model.GitHubMetadataCheck
	LegacyBuildWorkflow          = config.LegacyBuildWorkflow
)

const (
	deployedKustomizationsKey    = "deployed_kustomizations"
	kustomizationRequestsKey     = "kustomization_requests"
	fluxSystemNamespace          = "flux-system"
	eventDeploySuccess           = "deploy_success"
	eventDeployFailed            = "deploy_failed"
	eventDeployStarted           = "deploy_started"
	eventDeployRollback          = "deploy_rollback"
	eventSyncSuccess             = "sync_success"
	eventSyncFailed              = "sync_failed"
	kindKustomization            = "Kustomization"
	kindHelmRelease              = "HelmRelease"
	statusAccepted               = "accepted"
	prConflict                   = "conflict"
	prSynced                     = "synced"
	fieldStatus                  = "status"
	fieldError                   = "error"
	fieldEvent                   = "event"
	fieldJobName                 = "job_name"
	fieldPRNumber                = "pr_number"
	errMsgInvalidJSONPayload     = "invalid json payload"
	buildMetadataCheckName       = model.BuildMetadataCheckName
	buildMetadataResultCheckName = model.BuildMetadataResultCheckName
	buildEventPush               = model.BuildEventPush
	buildEventDispatch           = model.BuildEventDispatch
	buildResultRunning           = model.BuildResultRunning
	buildResultFailure           = model.BuildResultFailure
	buildResultCancelled         = model.BuildResultCancelled
	jobGitSync                   = "git-sync"
	jobGitMerge                  = "git-merge"
	maxPromptRunes               = model.MaxPromptRunes
	maxRoboRevCommentRunes       = model.MaxRoboRevCommentRunes
)

var (
	prRolloutStatus                 = flux.PRRolloutStatus
	LoadConfig                      = config.LoadConfig
	LoadConfigStrict                = config.LoadConfigStrict
	LoadConfigFromEnv               = config.LoadConfigFromEnv
	RestartRequired                 = config.RestartRequired
	dedupeTrimmedStrings            = model.DedupeTrimmedStrings
	firstNonEmpty                   = model.FirstNonEmpty
	randomNonce                     = model.RandomNonce
	formatDirectMessagePrefix       = model.FormatDirectMessagePrefix
	isValidDiscordSnowflake         = model.IsValidDiscordSnowflake
	isValidDiscordChannel           = model.IsValidDiscordChannel
	isHeadlessSessionKey            = model.IsHeadlessSessionKey
	IsHeadlessSessionKey            = model.IsHeadlessSessionKey
	formatErrorDetailsBlock         = model.FormatErrorDetailsBlock
	metadataWorkflowPath            = buildmeta.MetadataWorkflowPath
	vmAlertFingerprint              = alerts.VMAlertFingerprint
	isVMAlertResolved               = alerts.IsVMAlertResolved
	formatVMAlertMessage            = alerts.FormatVMAlertMessage
	translateFluxAlertToDeployEvent = flux.TranslateFluxAlertToDeployEvent
	collectJetStreamQueueMetrics    = metrics.CollectJetStreamQueueMetrics
	setDiscordGatewayLatency        = metrics.SetDiscordGatewayLatency
	recordJobEnqueued               = metrics.RecordJobEnqueued
	queueDefault                    = metrics.QueueDefault
	normalizeRepo                   = model.NormalizeRepo
	cleanRevisionSHA                = flux.CleanRevisionSHA
	isBuildMetadataCheck            = model.IsBuildMetadataCheck
	ParseBuildMetadata              = model.ParseBuildMetadata
	ParseStoredBuildMetadata        = model.ParseStoredBuildMetadata
	ResolveImageTag                 = kube.ResolveImageTag
	ExtractImageWorkloadName        = model.ExtractImageWorkloadName
	truncatePromptRunes             = model.TruncatePromptRunes
	resolveImageTag                 = kube.ResolveImageTag
)

func (s *RouterServer) hasCollaboratorCIAlertsForRepo(repo string) bool {
	if s == nil || s.cfgSrc == nil {
		return false
	}
	return config.HasCollaboratorCIAlertsForRepo(s.cfgSrc.Current(), repo)
}

func (s *RouterServer) lookupCollaboratorCIAlert(repo, author string) (channelID, mention string, ok bool) {
	if s == nil || s.cfgSrc == nil {
		return "", "", false
	}
	return config.LookupCollaboratorCIAlert(s.cfgSrc.Current(), repo, author)
}

const (
	QueueDiscordIngress     = metrics.QueueDiscordIngress
	QueueDiscordInteraction = metrics.QueueDiscordInteraction
	QueueDefault            = metrics.QueueDefault
)

var (
	alertPayloadRepo         = flux.AlertPayloadRepo
	NormalizeDiscordTargetID = model.NormalizeDiscordTargetID
	IsValidDiscordSnowflake  = model.IsValidDiscordSnowflake
	IsValidDiscordChannel    = model.IsValidDiscordChannel
	RecordJobDropped         = metrics.RecordJobDropped
	RecordJobEnqueued        = metrics.RecordJobEnqueued
	RecordJobRetry           = metrics.RecordJobRetry
	JobRetriesTotal          = metrics.JobRetriesTotal
	QueueDepth               = metrics.QueueDepth
	SetDiscordGatewayLatency = metrics.SetDiscordGatewayLatency
)

var (
	is4xxClientError        = model.Is4xxClientError
	loadConfigFromEnvStrict = config.LoadConfigFromEnvStrict
	synapseRestartRequired  = config.RestartRequired
	runServer               = RunServer
	runApp                  = RunApp
)

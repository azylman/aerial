package router

import (
	"github.com/azylman/aerial/services/synapse/internal/alerts"
	"github.com/azylman/aerial/services/synapse/internal/buildmeta"
	"github.com/azylman/aerial/services/synapse/internal/config"
	"github.com/azylman/aerial/services/synapse/internal/flux"
	"github.com/azylman/aerial/services/synapse/internal/kube"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
	"github.com/azylman/aerial/services/synapse/internal/model"
	"github.com/azylman/aerial/services/synapse/internal/registry"
)

type (
	ConfigSource                 = config.ConfigSource
	Config                       = config.Config
	CollaboratorCIAlertRule      = config.CollaboratorCIAlertRule
	PRRegistryUpdater            = model.PRRegistryUpdater
	OutboundDispatcher           = model.OutboundDispatcher
	ConsumerStatsQuerier         = metrics.ConsumerStatsQuerier
	consumerStatsQuerier         = metrics.ConsumerStatsQuerier
	workloadRollout              = model.WorkloadRollout
	configMapChange              = model.ConfigMapChange
	fluxCondition                = model.FluxCondition
	prRolloutInput               = flux.PRRolloutInput
	kustomizationRequest         = model.KustomizationRequest
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
	BuildMetadata                = model.BuildMetadata
	PRBuildLifecycle             = model.PRBuildLifecycle
	DeployingPR                  = model.DeployingPR
	PRRegisterRequest            = model.PRRegisterRequest
	FluxAlertEvent               = model.FluxAlertEvent
	FluxInvolvedObject           = model.FluxInvolvedObject
	EventWebhookRequest          = alerts.EventWebhookRequest
	VMAlertNotification          = alerts.VMAlertNotification
	PostgresPRRegistry           = registry.PostgresPRRegistry
	WorkloadRollout              = model.WorkloadRollout
	ConfigMapChange              = model.ConfigMapChange
	FluxKustomizationRef         = model.FluxKustomizationRef
	KustomizationObservation     = model.KustomizationObservation
	BuildSelection               = model.BuildSelection
	BuildResult                  = model.BuildResult
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
	buildMetadataFinal           = model.BuildMetadataFinal
	buildMetadataManifest        = model.BuildMetadataManifest
	buildMetadataResult          = model.BuildMetadataResult
	buildMetadataProgress        = model.BuildMetadataProgress
	buildMetadataCheckName       = model.BuildMetadataCheckName
	buildMetadataResultCheckName = model.BuildMetadataResultCheckName
	buildEventPush               = model.BuildEventPush
	buildEventDispatch           = model.BuildEventDispatch
	buildResultRunning           = model.BuildResultRunning
	buildResultFailure           = model.BuildResultFailure
	buildResultCancelled         = model.BuildResultCancelled
	jobGitSync                   = "git-sync"
	maxPromptRunes               = model.MaxPromptRunes
	maxRoboRevCommentRunes       = model.MaxRoboRevCommentRunes
)

var (
	prRolloutStatus                 = flux.PRRolloutStatus
	NewPostgresPRRegistry           = registry.NewPostgresPRRegistry
	LoadConfig                      = config.LoadConfig
	LoadConfigStrict                = config.LoadConfigStrict
	LoadConfigFromEnv               = config.LoadConfigFromEnv
	RestartRequired                 = config.RestartRequired
	extractStringSliceFromMetadata  = model.ExtractStringSliceFromMetadata
	extractJobsFromMetadata         = model.ExtractJobsFromMetadata
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
	parseStoredBuildMetadata        = model.ParseStoredBuildMetadata
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

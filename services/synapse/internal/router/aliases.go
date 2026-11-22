package router

import (
	"github.com/azylman/aerial/services/synapse/internal/alerts"
	"github.com/azylman/aerial/services/synapse/internal/config"
	"github.com/azylman/aerial/services/synapse/internal/flux"
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
	runServer = RunServer
	runApp    = RunApp
)

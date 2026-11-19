package flux

import (
	"strings"
	"time"

	"github.com/azylman/aerial/services/synapse/internal/kube"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

const (
	jobGitSync         = "git-sync"
	severityError      = "error"
	kindKustomization  = "Kustomization"
	reasonProgressing  = "Progressing"
	eventSyncSuccess   = "sync_success"
	eventSyncFailed    = "sync_failed"
	eventDeployStarted = "deploy_started"
	eventDeploySuccess = "deploy_success"
	eventDeployFailed  = "deploy_failed"
	conclusionSuccess  = "success"
	statusFailed       = "failed"
)

// AlertPayloadRepo returns the repo an alert carries in its payload or metadata, if any.
func AlertPayloadRepo(alert model.FluxAlertEvent) string {
	if candidate := strings.TrimSpace(alert.Repo); candidate != "" {
		return candidate
	}
	return strings.TrimSpace(alert.Metadata["repo"])
}

// ResolveFluxRepo determines the normalized repo name from an alert event.
func ResolveFluxRepo(alert model.FluxAlertEvent) string {
	if candidate := AlertPayloadRepo(alert); candidate != "" {
		return model.NormalizeRepo(candidate)
	}
	return "azylman/aerial"
}

// CleanRevisionSHA extracts the commit SHA from a Flux revision metadata string.
func CleanRevisionSHA(raw string) string {
	return kube.CleanRevisionSHA(raw)
}

// TranslateFluxAlertToDeployEvent translates a Flux alert event into an equivalent DeployEvent.
// Returns false if the alert kind/reason is not recognized or does not map to an actionable deploy/sync event.
func TranslateFluxAlertToDeployEvent(alert model.FluxAlertEvent) (model.DeployEvent, bool) {
	repo := ResolveFluxRepo(alert)
	var commitSHA string
	if alert.Metadata != nil {
		commitSHA = CleanRevisionSHA(alert.Metadata["revision"])
	}

	ts := alert.Timestamp
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	kind := strings.TrimSpace(alert.InvolvedObject.Kind)
	reason := strings.TrimSpace(alert.Reason)
	severity := strings.ToLower(strings.TrimSpace(alert.Severity))

	switch kind {
	case "GitRepository":
		switch {
		case reason == "NewArtifact" || reason == "GitOperationSucceeded":
			return model.DeployEvent{
				Event:      eventSyncSuccess,
				Status:     conclusionSuccess,
				SourceKind: kind,
				JobName:    jobGitSync,
				Repo:       repo,
				CommitSHA:  commitSHA,
				Details:    alert.Message,
				Timestamp:  ts,
			}, true
		case severity == severityError || reason == "GitOperationFailed":
			return model.DeployEvent{
				Event:      eventSyncFailed,
				Status:     statusFailed,
				SourceKind: kind,
				JobName:    jobGitSync,
				Repo:       repo,
				CommitSHA:  commitSHA,
				Details:    alert.Message,
				Timestamp:  ts,
			}, true
		default:
			return model.DeployEvent{}, false
		}

	case kindKustomization, "HelmRelease":
		jobName := strings.TrimSpace(alert.InvolvedObject.Name)
		switch {
		case reason == reasonProgressing:
			return model.DeployEvent{
				Event:      eventDeployStarted,
				Status:     "started",
				SourceKind: kind,
				JobName:    jobName,
				Repo:       repo,
				CommitSHA:  commitSHA,
				Details:    alert.Message,
				Timestamp:  ts,
			}, true
		case reason == "ReconciliationSucceeded":
			return model.DeployEvent{
				Event:      eventDeploySuccess,
				Status:     conclusionSuccess,
				SourceKind: kind,
				JobName:    jobName,
				Repo:       repo,
				CommitSHA:  commitSHA,
				Details:    alert.Message,
				Timestamp:  ts,
			}, true
		case severity == severityError || IsFluxFailureReason(reason):
			return model.DeployEvent{
				Event:      eventDeployFailed,
				Status:     statusFailed,
				SourceKind: kind,
				JobName:    jobName,
				Repo:       repo,
				CommitSHA:  commitSHA,
				Details:    alert.Message,
				Timestamp:  ts,
			}, true
		default:
			return model.DeployEvent{}, false
		}

	default:
		return model.DeployEvent{}, false
	}
}

// IsFluxFailureReason reports whether reason indicates a terminal reconciliation failure.
func IsFluxFailureReason(reason string) bool {
	switch reason {
	case "ReconciliationFailed", "HealthCheckFailed", "BuildFailed", "ArtifactFailed", "ValidationFailed":
		return true
	default:
		return false
	}
}

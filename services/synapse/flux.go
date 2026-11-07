package main

import (
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
)

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

// cleanRevisionSHA extracts the commit SHA from a Flux revision metadata string.
// Formats observed from Flux: "branch@sha1:1234abcd...", "sha1:1234abcd...", or bare "1234abcd...".
func cleanRevisionSHA(raw string) string {
	raw = strings.TrimSpace(raw)
	if idx := strings.Index(raw, "@sha1:"); idx != -1 {
		return strings.TrimSpace(raw[idx+len("@sha1:"):])
	}
	if after, found := strings.CutPrefix(raw, "sha1:"); found {
		return strings.TrimSpace(after)
	}
	if idx := strings.Index(raw, "@"); idx != -1 {
		return strings.TrimSpace(raw[idx+1:])
	}
	return raw
}

// resolveFluxRepo determines the normalized repo name from an alert event.
func resolveFluxRepo(alert FluxAlertEvent) string {
	candidate := strings.TrimSpace(alert.Repo)
	if candidate == "" && alert.Metadata != nil {
		candidate = strings.TrimSpace(alert.Metadata["repo"])
	}
	if candidate == "" && strings.TrimSpace(alert.InvolvedObject.Name) == "aerial-config" {
		candidate = "azylman/aerial-config"
	}
	if candidate != "" {
		return normalizeRepo(candidate)
	}
	return "azylman/aerial"
}

// TranslateFluxAlertToHangarEvent translates a Flux alert event into an equivalent HangarDeployEvent.
// Returns false if the alert kind/reason is not recognized or does not map to an actionable deploy/sync event.
func TranslateFluxAlertToHangarEvent(alert FluxAlertEvent) (HangarDeployEvent, bool) {
	repo := resolveFluxRepo(alert)
	var commitSHA string
	if alert.Metadata != nil {
		commitSHA = cleanRevisionSHA(alert.Metadata["revision"])
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
			return HangarDeployEvent{
				Event:     "sync_success",
				Status:    "success",
				JobName:   "git-sync",
				Repo:      repo,
				CommitSHA: commitSHA,
				Details:   alert.Message,
				Timestamp: ts,
			}, true
		case severity == "error" || reason == "GitOperationFailed":
			return HangarDeployEvent{
				Event:     "sync_failed",
				Status:    "failed",
				JobName:   "git-sync",
				Repo:      repo,
				CommitSHA: commitSHA,
				Details:   alert.Message,
				Timestamp: ts,
			}, true
		default:
			return HangarDeployEvent{}, false
		}

	case "Kustomization", "HelmRelease":
		jobName := strings.TrimSpace(alert.InvolvedObject.Name)
		switch {
		case reason == "Progressing":
			return HangarDeployEvent{
				Event:     "deploy_started",
				Status:    "started",
				JobName:   jobName,
				Repo:      repo,
				CommitSHA: commitSHA,
				Details:   alert.Message,
				Timestamp: ts,
			}, true
		case reason == "ReconciliationSucceeded":
			return HangarDeployEvent{
				Event:     "deploy_success",
				Status:    "success",
				JobName:   jobName,
				Repo:      repo,
				CommitSHA: commitSHA,
				Details:   alert.Message,
				Timestamp: ts,
			}, true
		case severity == "error" || isFluxFailureReason(reason):
			return HangarDeployEvent{
				Event:     "deploy_failed",
				Status:    "failed",
				JobName:   jobName,
				Repo:      repo,
				CommitSHA: commitSHA,
				Details:   alert.Message,
				Timestamp: ts,
			}, true
		default:
			return HangarDeployEvent{}, false
		}

	default:
		return HangarDeployEvent{}, false
	}
}

func isFluxFailureReason(reason string) bool {
	switch reason {
	case "ReconciliationFailed", "HealthCheckFailed", "BuildFailed", "ArtifactFailed", "ValidationFailed":
		return true
	default:
		return false
	}
}

func (s *RouterServer) handleFluxWebhook(w http.ResponseWriter, r *http.Request) {
	var alert FluxAlertEvent
	if !readWebhookJSON(w, r, 1<<20, false, &alert) {
		return
	}

	hangarEvt, ok := TranslateFluxAlertToHangarEvent(alert)
	if !ok {
		log.Printf("[synapse] [flux] ignoring unhandled flux alert: kind=%s name=%s reason=%s severity=%s",
			alert.InvolvedObject.Kind, alert.InvolvedObject.Name, alert.Reason, alert.Severity)
		httpkit.WriteJSON(w, http.StatusOK, map[string]any{
			"status":  "ignored",
			"message": "unhandled flux alert event",
		})
		return
	}

	s.enqueueOrProcessHangarEvent(w, r, hangarEvt, "flux")
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/azylman/aerial/pkg/httpkit"
	"github.com/azylman/aerial/services/synapse/internal/alerts"
	"github.com/azylman/aerial/services/synapse/internal/model"
)

type (
	EventWebhookRequest = alerts.EventWebhookRequest
	VMAlertNotification = alerts.VMAlertNotification
)

func vmAlertFingerprint(labels map[string]string) string {
	return alerts.VMAlertFingerprint(labels)
}

func isVMAlertResolved(a VMAlertNotification, now time.Time) bool {
	return alerts.IsVMAlertResolved(a, now)
}

func formatVMAlertMessage(a VMAlertNotification) string {
	return alerts.FormatVMAlertMessage(a)
}

func (s *RouterServer) handleEventsWebhook(w http.ResponseWriter, r *http.Request) {
	body, ok := readWebhookBody(w, r, 1<<20)
	if !ok {
		return
	}
	if s != nil && s.cfgSrc != nil && len(s.cfgSrc.Current().WebhookForwardURLs) > 0 {
		reqPath := r.URL.RequestURI()
		if reqPath == "" {
			reqPath = r.URL.Path
		}
		s.forwardRawWebhookToPeers(r.Context(), reqPath, r.Header, body)
	}

	var req EventWebhookRequest
	if err := json.Unmarshal(body, &req); err != nil {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{fieldError: errMsgInvalidJSONPayload})
		return
	}
	source := strings.ToLower(strings.TrimSpace(req.Source))
	eventType := strings.ToLower(strings.TrimSpace(req.EventType))
	targets := dedupeTrimmedStrings(append([]string{req.Target}, req.Targets...))
	if source == "" || eventType == "" || len(targets) == 0 {
		httpkit.WriteJSON(w, http.StatusBadRequest, map[string]string{fieldError: "missing required fields"})
		return
	}

	contextHeader := strings.TrimSpace(req.ContextHeader)
	if contextHeader == "" {
		contextHeader = fmt.Sprintf("External %s event %s on %s", source, eventType, strings.Join(targets, ", "))
		if details := strings.TrimSpace(req.Details); details != "" {
			contextHeader += ": " + details
		}
	}

	if s == nil || s.bus == nil {
		httpkit.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{fieldError: "event bus not initialized"})
		return
	}

	if err := s.evaluateEventTriggers(r.Context(), source, eventType, targets, contextHeader); err != nil {
		httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{fieldError: "failed to evaluate triggers"})
		return
	}
	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		fieldStatus:  statusAccepted,
		"source":     source,
		"targets":    targets,
		"event_type": eventType,
	})
}

func (s *RouterServer) evaluateEventTriggers(ctx context.Context, source, eventType string, targets []string, contextHeader string) error {
	if s == nil || s.bus == nil {
		return errors.New("event bus not initialized")
	}
	return alerts.EvaluateEventTriggers(ctx, s.bus, source, eventType, targets, contextHeader)
}

func (s *RouterServer) evaluateFluxEventTriggers(ctx context.Context, evt DeployEvent) {
	if s == nil || s.bus == nil {
		return
	}
	alerts.EvaluateFluxEventTriggers(ctx, s.bus, model.DeployEvent{
		Event:   evt.Event,
		Repo:    evt.Repo,
		Details: evt.Details,
	})
}

func (s *RouterServer) evaluateGitHubEventTriggers(ctx context.Context, res *ResolvedGitHubEvent, body []byte) {
	if s == nil || s.bus == nil || res == nil {
		return
	}
	alerts.EvaluateGitHubEventTriggers(ctx, s.bus, &alerts.ResolvedGitHubEvent{
		Event:      res.Event,
		Action:     res.Action,
		Repo:       res.Repo,
		Branch:     res.Branch,
		Merged:     res.Merged,
		Conclusion: res.Conclusion,
	}, body)
}

func (s *RouterServer) handleAlertsWebhook(w http.ResponseWriter, r *http.Request) {
	var alertsList []VMAlertNotification
	if !readWebhookJSON(w, r, 1<<20, &alertsList) {
		return
	}

	dispatchedCount := 0
	now := time.Now().UTC()
	for _, alert := range alertsList {
		fp := vmAlertFingerprint(alert.Labels)
		if isVMAlertResolved(alert, now) {
			s.alertTracker.ClearFiring(fp)
			continue
		}
		if status := strings.TrimSpace(alert.Status); status != "" && !strings.EqualFold(status, "firing") {
			continue
		}

		if s.dispatcher == nil || s.cfgSrc == nil {
			httpkit.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{fieldError: "dispatcher or config not available"})
			return
		}
		channelID := strings.TrimSpace(s.cfgSrc.Current().SystemChannelID)
		if !s.isValidAlertTarget(channelID) {
			httpkit.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{fieldError: "system channel not configured or invalid"})
			return
		}

		if !s.alertTracker.MarkFiring(fp) {
			continue
		}
		msg := formatVMAlertMessage(alert)
		if err := s.dispatcher.DispatchDirectMessage(r.Context(), DirectMessageRequest{ChannelID: channelID, Content: msg}); err != nil {
			s.alertTracker.ClearFiring(fp)
			slog.ErrorContext(r.Context(), "[synapse] [alerts] error dispatching alert", "fingerprint", fp, "error", err)
			httpkit.WriteJSON(w, http.StatusInternalServerError, map[string]string{fieldError: "failed to dispatch alert message"})
			return
		}
		dispatchedCount++
	}

	httpkit.WriteJSON(w, http.StatusOK, map[string]any{
		fieldStatus:  statusAccepted,
		"dispatched": dispatchedCount,
	})
}

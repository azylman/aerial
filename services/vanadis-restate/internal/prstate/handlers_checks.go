package prstate

// PRObject check-run and collaborator-alert handlers on the frozen contracts.
// Restate K/V in PRObject (key repo#prNumber) is the ONLY writer/authority;
// pr_registry stays a read-only projection updated by a journaled send after
// the state commit (chunk 02) and is never consulted to decide a transition.
//
// Journaled-effect contract: every Discord alert fires at most once per
// generation-scoped EffectKey (repo + PR + headSHA + gateGeneration +
// effectKind + canonical check name). The caller binds SendDiscordAlert to a
// Restate journaled step in chunk 07; replay then returns the recorded result
// and never re-fires. The Discord send API accepts no external idempotency
// key, so the accepted tolerance is at-least-once across full rebuilds
// (replay restores the dispatched mark from Restate K/V; only a state loss
// outside Restate can duplicate a send). Keys bump/clear only when a new gate
// generation starts (RecordKustomizationRequests); RetryPRReadiness never
// mints a generation and never clears effect keys.
//
// Routing contract: every event carries head or merge SHA, handlers drop
// stale events against current state, terminal states absorb same-epoch
// events, and unmanaged collaborator PRs stay inside PRObject for alerts and
// NEVER signal BuildRouter.RoutePREvent.
//
// This chunk is dark: the free functions below are pure Restate-ready logic
// with injected hermetic boundaries. Zero wiring into existing caller
// entrypoints; chunk 07 binds them to Restate handlers.

import (
	"context"
	"fmt"
	"strings"

	"github.com/azylman/aerial/pkg/pubsub"
)

// Check event names carried in PREvent.Event for HandleCheckEvent.
const (
	checkEventFailed         = "check_failed"
	checkEventCIFailed       = "ci_failed"
	checkEventPassed         = "check_passed"
	checkEventCIPassed       = "ci_passed"
	checkEventSuccess        = "check_success"
	checkEventRerunRequested = "check_rerun_requested"
)

// DiscordAlerter is the journaled Discord send boundary for check and
// collaborator alerts. The implementation passes key.String() wherever the
// downstream API supports an external idempotency key; Discord channel sends
// do not, so dedup rests on the generation-scoped EffectKey (see contract
// above). Fakes record keys to prove exactly-once-per-key firing.
type DiscordAlerter interface {
	SendDiscordAlert(ctx context.Context, key EffectKey, message string) error
}

// CheckOutcome is the decision record for one check or collaborator event.
type CheckOutcome struct {
	// Disposition is the pure routing decision: apply, drop_stale, or
	// absorb_terminal. Non-apply outcomes fire no effect and route nothing.
	Disposition EventDisposition
	// AlertFired reports whether a Discord alert fired on this call.
	AlertFired bool
	// Routed reports whether the event was signalled to the owning Build.
	// Always false for unmanaged collaborator PRs.
	Routed bool
}

// isGateAggregatorCheck mirrors services/synapse/internal/github
// IsGateAggregatorCheck semantics: overarching gate checks never drive
// per-PR failure alerts or remediation, but owned events still route.
func isGateAggregatorCheck(checkName string) bool {
	lower := strings.ToLower(strings.TrimSpace(checkName))
	return lower == "ci gate" || strings.HasSuffix(lower, " gate")
}

// usableHeadSHA returns the trimmed head SHA, or "" when it cannot anchor a
// failure record or an effect key.
func usableHeadSHA(headSHA string) string {
	clean := strings.TrimSpace(headSHA)
	if clean == "" || IsZeroSHA(clean) {
		return ""
	}
	return clean
}

// alertableCheck reports whether the check name can anchor a Discord alert.
func alertableCheck(checkName string) bool {
	if strings.TrimSpace(checkName) == "" || isGateAggregatorCheck(checkName) {
		return false
	}
	return true
}

// formatCheckAlertMessage renders the Discord alert body for a CI failure.
func formatCheckAlertMessage(repo string, prNumber int, title, branch, headSHA, checkName string) string {
	short := strings.TrimSpace(headSHA)
	if len(short) > 7 {
		short = short[:7]
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "[%s#%d] CI check %q failed on `%s`", NormalizeRepo(repo), prNumber, strings.TrimSpace(checkName), short)
	if branch = strings.TrimSpace(branch); branch != "" {
		fmt.Fprintf(&sb, " (branch `%s`)", branch)
	}
	if title = strings.TrimSpace(title); title != "" {
		fmt.Fprintf(&sb, ": %s", title)
	}
	return sb.String()
}

// toLifecycleEvent projects a PREvent onto the BuildRouter event shape.
func toLifecycleEvent(ev PREvent) pubsub.PRLifecycleEvent {
	return pubsub.PRLifecycleEvent{
		Event:     strings.TrimSpace(ev.Event),
		Repo:      NormalizeRepo(ev.Repo),
		PRNumber:  ev.PRNumber,
		Branch:    strings.TrimSpace(ev.Branch),
		HeadSHA:   strings.TrimSpace(ev.HeadSHA),
		MergeSHA:  strings.TrimSpace(ev.MergeSHA),
		Title:     strings.TrimSpace(ev.Title),
		CheckName: strings.TrimSpace(ev.CheckName),
		JobName:   strings.TrimSpace(ev.JobName),
	}
}

// fireDiscordAlertOnce fires the keyed Discord alert unless its journaled
// EffectKey already fired. It marks the key dispatched only after a
// successful send so a failed send retries on replay.
func fireDiscordAlertOnce(ctx context.Context, s *PRState, alerter DiscordAlerter, kind, headSHA, checkName, message string) (bool, error) {
	if s == nil {
		return false, nil
	}
	key := s.EffectKeyFor(kind, headSHA, "", checkName)
	if s.WasEffectDispatched(key) {
		return false, nil
	}
	if alerter == nil {
		return false, nil
	}
	if err := alerter.SendDiscordAlert(ctx, key, message); err != nil {
		return false, err
	}
	s.MarkEffectDispatched(key)
	return true, nil
}

// HandleCheckEvent applies one check-run event to PR state: it records
// failures into the granular tracker, clears on passes, fires the Discord
// alert exactly once per generation-scoped EffectKey, and signals the owning
// Build for build-owned PRs. Unmanaged collaborator PRs record and alert
// inside PRObject and NEVER signal the BuildRouter.
func HandleCheckEvent(ctx context.Context, s *PRState, ev PREvent, router BuildRouterInvoker, alerter DiscordAlerter) (CheckOutcome, error) {
	if s == nil {
		return CheckOutcome{Disposition: DispositionDropStale}, nil
	}
	if disp := s.ApplyEvent(ev); disp != DispositionApply {
		return CheckOutcome{Disposition: disp}, nil
	}
	out := CheckOutcome{Disposition: DispositionApply}
	headSHA := usableHeadSHA(ev.HeadSHA)
	switch strings.ToLower(strings.TrimSpace(ev.Event)) {
	case checkEventFailed, checkEventCIFailed:
		if headSHA == "" || !alertableCheck(ev.CheckName) {
			break
		}
		unmanaged := ClassifyOwnership(ev.BuildID, ev.Unmanaged) == PRUnmanaged
		s.RecordFailedCISHA(headSHA)
		if !IsRoboRevCheck(ev.CheckName) {
			s.RecordNonRoboRevFailedSHA(headSHA)
		}
		_, wasActive, hadPrior := s.RecordFailedCheckRunDetailed(headSHA, ev.CheckName)
		newlyMarked := false
		if unmanaged {
			newlyMarked = s.MarkCollaboratorAlerted(headSHA)
		}
		if s.WasEffectDispatched(s.EffectKeyFor(EffectDiscordAlert, headSHA, "", ev.CheckName)) {
			break
		}
		fired, err := fireDiscordAlertOnce(ctx, s, alerter, EffectDiscordAlert, headSHA, ev.CheckName,
			formatCheckAlertMessage(ev.Repo, ev.PRNumber, ev.Title, ev.Branch, headSHA, ev.CheckName))
		if err != nil {
			s.RollbackFailedCheckRun(headSHA, ev.CheckName, wasActive, hadPrior)
			if newlyMarked {
				s.ClearCollaboratorAlerted(headSHA)
			}
			return out, err
		}
		out.AlertFired = fired
	case checkEventPassed, checkEventCIPassed, checkEventSuccess:
		if headSHA == "" || strings.TrimSpace(ev.CheckName) == "" {
			break
		}
		s.ClearFailedCheckRun(headSHA, ev.CheckName)
		if !IsRoboRevCheck(ev.CheckName) {
			s.CanRecoverNonGateCheckSuccess(ev.CheckName, headSHA)
		}
	case checkEventRerunRequested:
		// Rerun carries no state mutation and no alert here; owned PRs
		// still route so the Build can re-drive the gate.
	default:
		// Unknown check event kinds apply without state mutation; owned
		// PRs still route so no Build signal is silently swallowed.
	}
	if ClassifyOwnership(ev.BuildID, ev.Unmanaged) == PRUnmanaged {
		return out, nil
	}
	if router == nil {
		return out, nil
	}
	if err := router.RoutePREvent(ctx, strings.TrimSpace(ev.BuildID), toLifecycleEvent(ev)); err != nil {
		return out, err
	}
	out.Routed = true
	return out, nil
}

// HandleCollaboratorAlert applies one collaborator CI-failure notice: it
// dedups per SHA, fires the Discord DM exactly once per generation-scoped
// EffectKey, rolls the alert mark back when the send fails, and NEVER
// signals the BuildRouter — even when the event carries a build ID. The
// router argument exists only so tests can prove zero calls; it is never
// invoked.
func HandleCollaboratorAlert(ctx context.Context, s *PRState, ev PREvent, _ BuildRouterInvoker, alerter DiscordAlerter) (CheckOutcome, error) {
	if s == nil {
		return CheckOutcome{Disposition: DispositionDropStale}, nil
	}
	if disp := s.ApplyEvent(ev); disp != DispositionApply {
		return CheckOutcome{Disposition: disp}, nil
	}
	out := CheckOutcome{Disposition: DispositionApply}
	headSHA := usableHeadSHA(ev.HeadSHA)
	if headSHA == "" || !alertableCheck(ev.CheckName) {
		return out, nil
	}
	if alerter == nil {
		return out, nil
	}
	if !s.MarkCollaboratorAlerted(headSHA) {
		return out, nil
	}
	if s.WasEffectDispatched(s.EffectKeyFor(EffectDiscordAlert, headSHA, "", ev.CheckName)) {
		return out, nil
	}
	fired, err := fireDiscordAlertOnce(ctx, s, alerter, EffectDiscordAlert, headSHA, ev.CheckName,
		formatCheckAlertMessage(ev.Repo, ev.PRNumber, ev.Title, ev.Branch, headSHA, ev.CheckName))
	if err != nil {
		s.ClearCollaboratorAlerted(headSHA)
		return out, err
	}
	out.AlertFired = fired
	return out, nil
}

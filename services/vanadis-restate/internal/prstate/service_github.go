package prstate

// Restate bindings for GitHub effects.
//
// RepoObject owns the repo-level GitHub effects: the open-PR auto-update
// cascade (HandleAutoUpdate, synapse checkOpenPRConflicts) and workflow
// rerun-failed-jobs (HandleWorkflowRerun). PRObject owns the per-PR ones:
// the mergeability conflict check with its delayed recheck
// (HandleConflictCheck over ConflictHandlers), the conflict an update-branch
// observed (HandleBranchConflict), and closing a PR whose head ref vanished
// (HandleHeadRefMissing).
//
// Every GitHub call runs as one journaled restate.Run, so a replay returns
// the recorded result instead of calling GitHub again. Inside the Run,
// transport failures and 5xx return a plain error and Restate retries the
// step a bounded number of times; terminal 4xx return
// restate.ToTerminalError with the HTTP status so Restate never retries
// them; rate limits return as data because seconds of backoff cannot clear
// them. A retryable failure that survives the step retries leaves the effect
// key uncommitted and schedules a delayed self-send, so no exclusive object
// lock is held across a long backoff.

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"
	"time"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/pubsub"
)

// GitHub step and delayed-retry policy.
const (
	githubRunAttempts           = 3
	githubRunRetryInterval      = time.Second
	maxRepoEffectRetries        = 6
	DefaultRepoEffectRetryDelay = 2 * time.Minute
)

// githubCall is the journaled result of one GitHub call: the value plus a
// data-encoded rate-limit failure.
type githubCall[T any] struct {
	Value T            `json:"value"`
	Err   *GitHubError `json:"err,omitempty"`
}

// journalGitHub runs call as one journaled step and classifies the outcome.
func journalGitHub[T any](ctx restate.Context, name string, call func(context.Context) (T, *GitHubError)) (T, *GitHubError) {
	res, err := restate.Run(ctx, func(rc restate.RunContext) (githubCall[T], error) {
		v, gerr := call(rc)
		switch {
		case gerr == nil || gerr.RateLimited:
			return githubCall[T]{Value: v, Err: gerr}, nil
		case gerr.Retryable():
			return githubCall[T]{}, gerr
		default:
			return githubCall[T]{}, restate.ToTerminalError(gerr, restate.WithErrorCode(restate.Code(gerr.Status)))
		}
	}, restate.WithName(name), restate.WithMaxRetryAttempts(githubRunAttempts), restate.WithInitialRetryInterval(githubRunRetryInterval))
	if err != nil {
		var zero T
		return zero, githubStepError(err)
	}
	return res.Value, res.Err
}

// githubStepError maps a failed journaled step back to a GitHubError: a
// terminal 4xx keeps its status (terminal), anything else (exhausted
// retries) is retryable.
func githubStepError(err error) *GitHubError {
	if restate.IsTerminalError(err) {
		if code := int(restate.AsTerminalError(err).Code()); code >= http.StatusBadRequest && code < http.StatusInternalServerError {
			return &GitHubError{Status: code, Message: err.Error()}
		}
	}
	return &GitHubError{Message: err.Error()}
}

// journaledGitHub binds GitHubEffector to journaled steps on one Restate
// invocation. The ctx argument of each method is that same invocation.
type journaledGitHub struct {
	ctx restate.Context
	gh  GitHubEffector
}

func (j journaledGitHub) ListOpenPRs(_ context.Context, repo string) ([]GitHubPR, *GitHubError) {
	return journalGitHub(j.ctx, "github-list-open-prs", func(rc context.Context) ([]GitHubPR, *GitHubError) {
		return j.gh.ListOpenPRs(rc, repo)
	})
}

func (j journaledGitHub) HeadChecksBlocked(_ context.Context, repo, sha string) bool {
	blocked, gerr := journalGitHub(j.ctx, "github-head-checks", func(rc context.Context) (bool, *GitHubError) {
		return j.gh.HeadChecksBlocked(rc, repo, sha), nil
	})
	return gerr != nil || blocked
}

func (j journaledGitHub) UpdateBranch(_ context.Context, repo string, prNumber int) (UpdateBranchOutcome, *GitHubError) {
	return journalGitHub(j.ctx, "github-update-branch", func(rc context.Context) (UpdateBranchOutcome, *GitHubError) {
		return j.gh.UpdateBranch(rc, repo, prNumber)
	})
}

func (j journaledGitHub) FetchPR(_ context.Context, repo string, prNumber int) (GitHubPR, *GitHubError) {
	return journalGitHub(j.ctx, "github-fetch-pr", func(rc context.Context) (GitHubPR, *GitHubError) {
		return j.gh.FetchPR(rc, repo, prNumber)
	})
}

func (j journaledGitHub) FetchPRMergeability(_ context.Context, repo string, prNumber int) (GitHubPR, *GitHubError) {
	return journalGitHub(j.ctx, "github-fetch-pr-mergeability", func(rc context.Context) (GitHubPR, *GitHubError) {
		return j.gh.FetchPRMergeability(rc, repo, prNumber)
	})
}

func (j journaledGitHub) RerunFailedJobs(_ context.Context, repo string, runID int64) *GitHubError {
	_, gerr := journalGitHub(j.ctx, "github-rerun-failed-jobs", func(rc context.Context) (struct{}, *GitHubError) {
		return struct{}{}, j.gh.RerunFailedJobs(rc, repo, runID)
	})
	return gerr
}

// journaled wraps gh for ctx, or returns nil when no client is wired.
func journaled(ctx restate.Context, gh GitHubEffector) GitHubEffector {
	if gh == nil {
		return nil
	}
	return journaledGitHub{ctx: ctx, gh: gh}
}

// WithGitHub injects the GitHub boundary and returns s. Without it
// restate-owned repo GitHub effects schedule retries until one is wired.
func (s *RepoObjectService) WithGitHub(gh GitHubEffector) *RepoObjectService {
	s.github = gh
	return s
}

// HandleAutoUpdate runs one open-PR auto-update cascade trigger.
func (s *RepoObjectService) HandleAutoUpdate(ctx restate.ObjectContext, ev RepoEvent) error {
	out, err := s.handlers(ctx).HandleAutoUpdate(ctx, ev)
	return s.reportRepoEffect(ctx, RepoHandlerAutoUpdate, ev, out, err)
}

// HandleWorkflowRerun reruns the failed jobs of one workflow run.
func (s *RepoObjectService) HandleWorkflowRerun(ctx restate.ObjectContext, ev RepoEvent) error {
	out, err := s.handlers(ctx).HandleWorkflowRerun(ctx, ev)
	return s.reportRepoEffect(ctx, RepoHandlerWorkflowRerun, ev, out, err)
}

// reportRepoEffect journals the metrics and logs for one repo effect and
// schedules its delayed retry. Malformed input fails terminally; a PRObject
// forward failure surfaces for the invocation retry (the committed key
// keeps GitHub from being called again).
func (s *RepoObjectService) reportRepoEffect(ctx restate.ObjectContext, handler string, ev RepoEvent, out RepoEffectOutcome, err error) error {
	if errors.Is(err, ErrInvalidRepoEvent) {
		return terminalOnInvalid(err)
	}
	if out.Key.Kind == "" {
		// The repo state failed to load before the effect was keyed: there
		// is nothing to report, and the invocation retry reloads it.
		return err
	}
	kind := out.Key.Kind
	switch {
	case out.Retry:
		s.scheduleRepoEffectRetry(ctx, handler, ev, out)
	case out.Deduped:
	default:
		logRepoEffect(ctx, out)
	}
	if err != nil {
		slog.WarnContext(ctx, "[prstate] repo effect forward failed", "repo", out.Key.Repo, "kind", kind, "error", err)
	}
	return err
}

// logRepoEffect journals the executed (or skipped / terminally failed) repo
// effect plus one line per update-branch call, keyed like synapse's
// [synapse-effect] github_update_branch lines.
func logRepoEffect(ctx restate.ObjectContext, out RepoEffectOutcome) {
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
		kind := out.Key.Kind
		switch {
		case out.Skipped:
			effectsTotal.WithLabelValues(kind, effectResultUnconfigured).Inc()
		case out.Executed:
			effectsTotal.WithLabelValues(kind, effectResultOK).Inc()
		default:
			effectsTotal.WithLabelValues(kind, effectResultError).Inc()
		}
		attrs := []any{"repo", out.Key.Repo, metricLabelKind, kind, "effect_key", out.Key.String(), "match_key", out.Key.MatchKey(), "skipped", out.Skipped, "forwards", out.Forwards}
		if msg := out.errText(); msg != "" {
			attrs = append(attrs, "error", msg)
		}
		if len(out.Promoted) > 0 {
			attrs = append(attrs, "promoted", strings.Join(out.Promoted, ","))
		}
		slog.InfoContext(rc, "[prstate] repo effect executed", attrs...)
		for _, u := range out.Updates {
			key := EffectKey{Repo: out.Key.Repo, PRNumber: u.PRNumber, HeadSHA: u.HeadSHA, Kind: EffectGitHubUpdateBranch}
			effectsTotal.WithLabelValues(EffectGitHubUpdateBranch, effectResultOK).Inc()
			slog.InfoContext(rc, "[prstate] effect executed", "pr", PRObjectKey(out.Key.Repo, u.PRNumber), "kind", EffectGitHubUpdateBranch,
				"effect_key", key.String(), "match_key", key.MatchKey(), "outcome", string(u.Outcome))
		}
		return restate.Void{}, nil
	}, restate.WithName("report-"+out.Key.Kind))
}

// scheduleRepoEffectRetry sends a delayed self-send retrying the effect, or
// drops it once maxRepoEffectRetries delayed retries failed.
func (s *RepoObjectService) scheduleRepoEffectRetry(ctx restate.ObjectContext, handler string, ev RepoEvent, out RepoEffectOutcome) {
	kind := out.Key.Kind
	msg := out.errText()
	if ev.Attempt >= maxRepoEffectRetries {
		_, _ = restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
			effectsTotal.WithLabelValues(kind, effectResultDropped).Inc()
			slog.ErrorContext(rc, "[prstate] repo effect dropped after retries", "repo", out.Key.Repo, "kind", kind, "effect_key", out.Key.String(), "attempts", ev.Attempt, "error", msg)
			return restate.Void{}, nil
		}, restate.WithName("drop-"+kind))
		return
	}
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
		effectsTotal.WithLabelValues(kind, effectResultRetry).Inc()
		slog.WarnContext(rc, "[prstate] repo effect retry scheduled", "repo", out.Key.Repo, "kind", kind, "effect_key", out.Key.String(), "attempt", ev.Attempt+1, "error", msg)
		return restate.Void{}, nil
	}, restate.WithName("retry-"+kind))
	next := ev
	next.Attempt++
	restate.ObjectSend(ctx, RepoObjectServiceName, RepoObjectKey(ev.Repo), handler).Send(next, restate.WithDelay(s.retryDelay))
}

// WithGitHub injects the GitHub boundary and returns s. Without it
// restate-owned conflict checks fail and schedule their delayed recheck.
func (s *PRObjectService) WithGitHub(gh GitHubEffector) *PRObjectService {
	s.github = gh
	return s
}

// HandleConflictCheck runs the per-PR mergeability conflict check through
// ConflictHandlers over this
// object's K/V: the journaled mergeability read, a delayed recheck on a
// failed or conflicted read, and on a fresh conflict the branch-conflict
// follow-up (conflict fact for a registered PR, the carried collaborator DM
// otherwise). A failed read never fails the invocation: the delayed recheck
// is its retry.
func (s *PRObjectService) HandleConflictCheck(ctx restate.ObjectContext, ev PREvent) error {
	if NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 || usableHeadSHA(ev.HeadSHA) == "" {
		return nil
	}
	store := &restatePRStore{ctx: ctx, svc: s, ev: ev}
	h, err := NewConflictHandlers(store, restateConflictChecker{gh: journaled(ctx, s.github)}, restateConflictRescheduler{ctx: ctx})
	if err != nil {
		return restate.ToTerminalError(err, restate.WithErrorCode(500))
	}
	out, err := h.HandleConflictCheck(ctx, PRObjectKey(ev.Repo, ev.PRNumber), ev)
	if store.err != nil {
		return store.err
	}
	if err != nil {
		slog.WarnContext(ctx, "[prstate] conflict check failed; recheck scheduled", "pr", PRObjectKey(ev.Repo, ev.PRNumber),
			"head_sha", ev.HeadSHA, "recheck_scheduled", out.RecheckScheduled, "error", err)
		return nil
	}
	if !out.Conflicted {
		return nil
	}
	return s.HandleBranchConflict(ctx, ev)
}

// HandleBranchConflict applies one observed merge conflict, mirroring
// synapse handleDirtyPRConflict. A registered PR (it has a target) runs the
// conflict verdict: an unpublished head commits the conflict keys and row
// status, then publishes the conflict fact rendered here. An unregistered
// collaborator PR marks the conflict and fires the collaborator DM the event
// carries.
func (s *PRObjectService) HandleBranchConflict(ctx restate.ObjectContext, ev PREvent) error {
	if NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 {
		return nil
	}
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return err
	}
	if st.ApplyEvent(ev) != DispositionApply {
		return nil
	}
	if usableHeadSHA(ev.HeadSHA) == "" {
		ev.HeadSHA = st.HeadSHA
	}
	applyEventRow(st, ev)
	if strings.TrimSpace(st.TargetID) == "" {
		st.MarkCollaboratorConflict(ev.HeadSHA)
		restate.Set(ctx, s.stateKey, st)
		s.runEriEffects(ctx, st, ev)
		return nil
	}
	dec, err := ResolveConflictDispatchDecision(st, ev)
	if err != nil {
		return err
	}
	if !dec.ShouldPublish {
		restate.Set(ctx, s.stateKey, st)
		return nil
	}
	if _, err := s.commitConflictPublished(ctx, st, ev); err != nil {
		return err
	}
	s.runFacts(ctx, st, []pubsub.PRLifecycleEvent{prFact(st, FactEventConflict, ev.Branch, ev.HeadSHA, journaledNow(ctx))})
	s.afterCommit(ctx, st)
	return nil
}

// HandleHeadRefMissing closes a PR whose head ref vanished and GitHub shows
// closed unmerged, mirroring synapse handleStaleOpenPR: the close transition
// commits, then the closed_unmerged fact rendered here publishes. Like synapse's stale publish, the fact is not quiet.
func (s *PRObjectService) HandleHeadRefMissing(ctx restate.ObjectContext, ev PREvent) error {
	if NormalizeRepo(ev.Repo) == "" || ev.PRNumber <= 0 {
		return nil
	}
	st, err := s.loadOrCreatePRState(ctx, ev)
	if err != nil {
		return err
	}
	if st.ApplyEvent(ev) != DispositionApply {
		return nil
	}
	before := snapshotIndexView(st)
	applyEventRow(st, ev)
	st.ClearFailedCISHA()
	st.DisarmDeployFailure()
	st.MarkTerminal(TerminalClosed)
	st.ApplyRegistryClosed()
	restate.Set(ctx, s.stateKey, st)
	if lifecycle := indexLifecycle(before, snapshotIndexView(st)); lifecycle != "" {
		sendPRIndex(ctx, st, ev, lifecycle)
	}
	fact := prFact(st, FactEventClosedUnmerged, ev.Branch, ev.HeadSHA, journaledNow(ctx))
	fact.Quiet = false
	s.runFacts(ctx, st, []pubsub.PRLifecycleEvent{fact})
	s.afterCommit(ctx, st)
	return nil
}

// journaledNow records the wall clock once so a replay renders the same
// fact timestamp.
func journaledNow(ctx restate.Context) time.Time {
	now, err := restate.Run(ctx, func(restate.RunContext) (time.Time, error) {
		return time.Now().UTC(), nil
	}, restate.WithName("fact-occurred-at"))
	if err != nil {
		return time.Time{}
	}
	return now
}

// restatePRStore binds PRStateStore to this PRObject's K/V. LoadPR creates
// first-touch state from the event exactly like every other handler; a K/V
// failure is kept in err for the handler to surface.
type restatePRStore struct {
	ctx restate.ObjectContext
	svc *PRObjectService
	ev  PREvent
	err error
}

func (s *restatePRStore) LoadPR(string) (*PRState, bool) {
	st, err := s.svc.loadOrCreatePRState(s.ctx, s.ev)
	if err != nil {
		s.err = err
		return nil, false
	}
	return st, true
}

func (s *restatePRStore) SavePR(st *PRState) {
	restate.Set(s.ctx, s.svc.stateKey, st)
}

// restateConflictChecker is the production ConflictChecker: one journaled
// mergeability read. Mirroring synapse resolveAutoMergeDisabledPR, a merged
// or closed PR, or one whose head moved, is not a conflict; a terminal read
// failure (the PR is gone or inaccessible) is not a conflict either, since
// no recheck can succeed.
type restateConflictChecker struct {
	gh GitHubEffector
}

func (c restateConflictChecker) CheckConflict(ctx context.Context, repo string, prNumber int, headSHA string) (bool, error) {
	if c.gh == nil {
		return false, errGitHubUnconfigured
	}
	pr, gerr := c.gh.FetchPRMergeability(ctx, repo, prNumber)
	if gerr != nil {
		if gerr.Retryable() {
			return false, gerr
		}
		return false, nil
	}
	if pr.Merged || (strings.TrimSpace(pr.State) != "" && !strings.EqualFold(strings.TrimSpace(pr.State), "open")) {
		return false, nil
	}
	if head := strings.TrimSpace(pr.Head.SHA); head != "" && !strings.EqualFold(head, strings.TrimSpace(headSHA)) {
		return false, nil
	}
	return IsMergeConflict(pr), nil
}

// restateConflictRescheduler binds ConflictRescheduler to a delayed one-way
// HandleConflictCheck self-send.
type restateConflictRescheduler struct {
	ctx restate.ObjectContext
}

func (r restateConflictRescheduler) ScheduleConflictRecheck(_ context.Context, delay time.Duration, ev PREvent) error {
	restate.ObjectSend(r.ctx, PRObjectServiceName, PRObjectKey(ev.Repo, ev.PRNumber), PRHandlerConflictCheck).Send(ev, restate.WithDelay(delay))
	return nil
}

package prstate

// Restate bindings for Flux and image effects.
//
// RepoObject owns the repo-level ones, which run whether or not a PR
// resolves (a direct commit to main must still deploy): git-push dispatch to
// the Flux receiver (HandleGitPush) and image-tag promotion into
// ConfigMap/aerial-image-tags with its rollout start (HandleImagePromotion).
// PRObject owns the per-PR Flux reconcile requests: the rollout requests
// HandleKustomizationRequested records, and the readiness retry
// (RetryReadiness request-response, HandleReadinessRetry one-way) that
// re-requests the original gate.
//
// Every Kubernetes or Flux receiver call runs as one journaled restate.Run,
// so a replay returns the recorded result instead of writing again. Inside
// the Run, transport failures, 5xx, 409 and 429 return a plain error and
// Restate retries the step a bounded number of times; other 4xx return
// restate.ToTerminalError with the HTTP status. A retryable failure that
// survives the step retries leaves the effect key uncommitted and schedules
// a delayed self-send, so no exclusive object lock is held across a long
// backoff.

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strings"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/prwire"
)

// Kube step policy; the delayed retry reuses the repo-effect bound and
// delay.
const kubeRunAttempts = 3

// kubeCall is the journaled result of one Kubernetes call.
type kubeCall[T any] struct {
	Value T `json:"value"`
}

// journalKube runs call as one journaled step and classifies the outcome.
func journalKube[T any](ctx restate.Context, name string, call func(context.Context) (T, *KubeError)) (T, *KubeError) {
	res, err := restate.Run(ctx, func(rc restate.RunContext) (kubeCall[T], error) {
		v, kerr := call(rc)
		switch {
		case kerr == nil:
			return kubeCall[T]{Value: v}, nil
		case kerr.Retryable():
			return kubeCall[T]{}, kerr
		default:
			return kubeCall[T]{}, restate.ToTerminalError(kerr, restate.WithErrorCode(restate.Code(kerr.Status)))
		}
	}, restate.WithName(name), restate.WithMaxRetryAttempts(kubeRunAttempts), restate.WithInitialRetryInterval(githubRunRetryInterval))
	if err != nil {
		var zero T
		return zero, kubeStepError(err)
	}
	return res.Value, nil
}

// kubeStepError maps a failed journaled step back to a KubeError: a terminal
// 4xx keeps its status (terminal), anything else (exhausted retries) is
// retryable.
func kubeStepError(err error) *KubeError {
	if restate.IsTerminalError(err) {
		if code := int(restate.AsTerminalError(err).Code()); code >= http.StatusBadRequest && code < http.StatusInternalServerError {
			return &KubeError{Status: code, Message: err.Error()}
		}
	}
	return &KubeError{Message: err.Error()}
}

// journaledKube binds KubeEffector to journaled steps on one Restate
// invocation. The ctx argument of each method is that same invocation.
type journaledKube struct {
	ctx  restate.Context
	kube KubeEffector
}

func (j journaledKube) DispatchGitPush(_ context.Context, req GitPushRequest) *KubeError {
	_, kerr := journalKube(j.ctx, "flux-git-push", func(rc context.Context) (struct{}, *KubeError) {
		return struct{}{}, j.kube.DispatchGitPush(rc, req)
	})
	return kerr
}

func (j journaledKube) PromoteImageTag(_ context.Context, req ImagePromotionRequest) (string, *KubeError) {
	return journalKube(j.ctx, "image-promote", func(rc context.Context) (string, *KubeError) {
		return j.kube.PromoteImageTag(rc, req)
	})
}

func (j journaledKube) ReconcileKustomization(_ context.Context, namespace, name, requestedAt string) *KubeError {
	_, kerr := journalKube(j.ctx, "flux-reconcile", func(rc context.Context) (struct{}, *KubeError) {
		return struct{}{}, j.kube.ReconcileKustomization(rc, namespace, name, requestedAt)
	})
	return kerr
}

// journaledKubeFor wraps k for ctx, or returns nil when no client is wired.
func journaledKubeFor(ctx restate.Context, k KubeEffector) KubeEffector {
	if k == nil {
		return nil
	}
	return journaledKube{ctx: ctx, kube: k}
}

// WithKube injects the Kubernetes/Flux boundary and returns s. Without it
// restate-owned repo Flux effects schedule retries until one is wired.
func (s *RepoObjectService) WithKube(k KubeEffector) *RepoObjectService {
	s.kube = k
	return s
}

// HandleGitPush forwards one main push to the Flux receiver.
func (s *RepoObjectService) HandleGitPush(ctx restate.ObjectContext, ev RepoEvent) error {
	out, err := s.handlers(ctx).HandleGitPush(ctx, ev)
	return s.reportRepoEffect(ctx, prwire.RepoHandlerGitPush, ev, out, err)
}

// HandleUntrackedReconcile patches an untracked PR commit's Kustomizations.
func (s *RepoObjectService) HandleUntrackedReconcile(ctx restate.ObjectContext, ev RepoEvent) error {
	out, err := s.handlers(ctx).HandleUntrackedReconcile(ctx, ev)
	return s.reportRepoEffect(ctx, prwire.RepoHandlerUntrackedReconcile, ev, out, err)
}

// HandleImagePromotion promotes one commit's images and starts its rollout.
func (s *RepoObjectService) HandleImagePromotion(ctx restate.ObjectContext, ev RepoEvent) error {
	out, err := s.handlers(ctx).HandleImagePromotion(ctx, ev)
	return s.reportRepoEffect(ctx, prwire.RepoHandlerImagePromotion, ev, out, err)
}

// WithKube injects the Kubernetes/Flux boundary and returns s. Without it
// restate-owned PR reconciles schedule retries until one is wired.
func (s *PRObjectService) WithKube(k KubeEffector) *PRObjectService {
	s.kube = k
	return s
}

// HandleKustomizationRequested starts a new gate generation for one
// kustomization request. It is a deploy-phase event, so merged PRs apply
// it: carried jobs move the PR to deploying (TransitionDeploying) and the
// carried requests replace the recorded set (RecordKustomizationRequests),
// in the registry's order. The generation bump sends RepoObject a deploying
// index update so kustomization status fans out to this PR. Only after that
// commit are the recorded requests patched, so a watch event can never observe an unrecorded
// request.
//
// A carried deploy_started fact publishes only when the jobs moved the PR to
// deploying (the registry TransitionDeploying target ID gate); every other
// carried fact publishes on any applied request.
//
// An untracked PR (PRState.Untracked: an upstream Flux source commit
// resolved to a PR nobody registered) drops the request entirely.
func (s *PRObjectService) HandleKustomizationRequested(ctx restate.ObjectContext, ev PREvent) error {
	started, rest := SplitDeployStartedFacts(ev.Facts)
	ev.Facts = nil
	var recorded, committed *PRState
	var transitioned bool
	if err := s.mutateRolloutEvent(ctx, ev, prwire.PRHandlerKustomizationRequested, func(st *PRState) {
		st.DisarmDeployFailure()
		transitioned = st.TransitionDeploying(ev.Jobs)
		if st.RecordKustomizationRequestSet(ev.KustomizationRequests) {
			recorded = st
		}
		st.RecordKustomizationRequests()
		committed = st
	}); err != nil || committed == nil {
		return err
	}
	s.runFacts(ctx, committed, gatedStartedFacts(started, rest, transitioned))
	if recorded == nil {
		return nil
	}
	st := recorded
	key := RolloutReconcileKey(st, ev.HeadSHA)
	s.runReconcile(ctx, st, reconcileRun{key: key, match: key.MatchKey()})
	return nil
}

// RetryReadiness is the request-response readiness retry. It makes the
// decision synapse readiness_retry.go makes today (409 when no matching
// merged rollout gate exists, else 202), applies RetryReadinessGate, commits,
// re-indexes the PR as deploying so kustomization status fans out to it
// again, and re-requests the original reconciles. The reconcile never gates the answer: a failure
// schedules its delayed retry. A missing merge SHA fails terminally (400),
// and a fenced caller not authorized for the stored owner with 409.
func (s *PRObjectService) RetryReadiness(ctx restate.ObjectContext, req RetryReadinessRequest) (RetryReadinessResponse, error) {
	merge := strings.TrimSpace(req.MergeSHA)
	if merge == "" {
		return RetryReadinessResponse{}, restate.ToTerminalError(errReadinessInvalid, restate.WithErrorCode(http.StatusBadRequest))
	}
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil {
		return RetryReadinessResponse{}, err
	}
	if st == nil {
		return RetryReadinessResponse{Status: http.StatusConflict}, nil
	}
	if stored := strings.TrimSpace(st.Agent); req.EnforceOwner && stored != "" && stored != strings.TrimSpace(req.ExpectedOwner) {
		return RetryReadinessResponse{}, restate.ToTerminalError(errReadinessOwnerMismatch, restate.WithErrorCode(http.StatusConflict))
	}
	st.ApplyAgentHint(req.Agent)
	return s.applyReadinessRetry(ctx, st, merge), nil
}

// Readiness retry error causes, wrapped as terminal errors.
var (
	errReadinessInvalid       = errors.New("prstate: readiness retry requires merge_sha")
	errReadinessOwnerMismatch = errors.New("prstate: readiness retry caller is not authorized for the existing owner")
)

// HandleReadinessRetry is the one-way readiness retry. It applies the same
// gate transition as RetryReadiness and re-requests the reconcile; retry never mints a generation and
// never clears effect keys.
func (s *PRObjectService) HandleReadinessRetry(ctx restate.ObjectContext, ev PREvent) error {
	merge := strings.TrimSpace(ev.MergeSHA)
	if strings.TrimSpace(ev.Repo) == "" || ev.PRNumber <= 0 || merge == "" {
		return nil
	}
	st, stored, err := s.loadPRState(ctx, ev)
	if err != nil {
		return err
	}
	if s.dropUntrackedRollout(ctx, st, stored, "HandleReadinessRetry") || st.ApplyDeployEvent(ev) != DispositionApply {
		return nil
	}
	if resp := s.applyReadinessRetry(ctx, st, merge); resp.Status != http.StatusAccepted {
		restate.Set(ctx, s.stateKey, st)
	}
	return nil
}

// applyReadinessRetry runs the shared readiness transition and effects.
func (s *PRObjectService) applyReadinessRetry(ctx restate.ObjectContext, st *PRState, merge string) RetryReadinessResponse {
	requests, ok := st.RetryReadinessGate(merge)
	if !ok {
		return RetryReadinessResponse{Status: http.StatusConflict}
	}
	restate.Set(ctx, s.stateKey, st)
	sendPRIndex(ctx, st, PREvent{MergeSHA: merge, Branch: st.Branch}, IndexDeploying)
	key, match := ReadinessReconcileKey(st, merge)
	s.runReconcile(ctx, st, reconcileRun{key: key, match: match})
	s.afterCommit(ctx, st)
	return RetryReadinessResponse{Status: http.StatusAccepted, Requests: requests, EffectKey: key.String()}
}

// reconcileRun is one PR reconcile effect: its key, the match key synapse
// logs, and the delayed-retry ordinal.
// requests narrows the patch to a subset of the recorded requests (a lost
// reconcile re-patch); nil patches every recorded request.
type reconcileRun struct {
	key      EffectKey
	match    string
	attempt  int
	requests map[string]KustomizationRequest
}

// patchSet returns the requests the run patches.
func (run reconcileRun) patchSet(st *PRState) map[string]KustomizationRequest {
	if run.requests != nil {
		return run.requests
	}
	return st.KustomizationRequests
}

// FluxReconcileRetryRequest is the delayed RetryFluxReconcile input naming
// the reconcile key's generation, SHA and job, plus the retry ordinal.
type FluxReconcileRetryRequest struct {
	Generation uint64 `json:"generation"`
	HeadSHA    string `json:"head_sha"`
	JobName    string `json:"job_name,omitempty"`
	Attempt    int    `json:"attempt"`
}

// RetryFluxReconcile re-requests a failed restate-owned PR reconcile against
// the current recorded requests. A moved generation (a newer request set
// re-fires its own reconcile) or a recorded key does nothing.
func (s *PRObjectService) RetryFluxReconcile(ctx restate.ObjectContext, req FluxReconcileRetryRequest) error {
	st, err := s.loadRetryState(ctx, req.Generation)
	if err != nil || st == nil {
		return err
	}
	key := st.EffectKeyFor(prwire.EffectFluxReconcile, req.HeadSHA, req.JobName, "")
	s.runReconcile(ctx, st, reconcileRun{key: key, match: key.MatchKey(), attempt: req.Attempt})
	return nil
}

// runReconcile patches the PR's recorded requests once per key. A retryable
// failure leaves the key uncommitted and schedules a delayed retry (dropped
// after maxRepoEffectRetries); success or a terminal failure commits it.
func (s *PRObjectService) runReconcile(ctx restate.ObjectContext, st *PRState, run reconcileRun) {
	if st.WasEffectDispatched(run.key) || len(st.KustomizationRequests) == 0 {
		return
	}
	kerr := errKubeUnconfigured
	if s.kube != nil {
		kerr = reconcileRequests(ctx, journaledKubeFor(ctx, s.kube), run.patchSet(st))
	}
	if kerr != nil && kerr.Retryable() {
		s.scheduleReconcileRetry(ctx, st, run, kerr)
		return
	}
	logReconcile(ctx, run, kerr)
	st.MarkEffectDispatched(run.key)
	restate.Set(ctx, s.stateKey, st)
}

// logReconcile journals the executed (or terminally failed) PR reconcile.
func logReconcile(ctx restate.ObjectContext, run reconcileRun, kerr *KubeError) {
	failure := kerr.Error()
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
		attrs := []any{"pr", prwire.PRObjectKey(run.key.Repo, run.key.PRNumber), metricLabelKind, prwire.EffectFluxReconcile, "effect_key", run.key.String(), "match_key", run.match}
		if failure != "" {
			effectsTotal.WithLabelValues(prwire.EffectFluxReconcile, effectResultError).Inc()
			slog.ErrorContext(rc, "[prstate] effect failed terminally", append(attrs, "error", failure)...)
		} else {
			effectsTotal.WithLabelValues(prwire.EffectFluxReconcile, effectResultOK).Inc()
			slog.InfoContext(rc, "[prstate] effect executed", attrs...)
		}
		return restate.Void{}, nil
	}, restate.WithName("report-"+prwire.EffectFluxReconcile))
}

// scheduleReconcileRetry sends a delayed RetryFluxReconcile, or drops the
// reconcile once maxRepoEffectRetries delayed retries failed.
func (s *PRObjectService) scheduleReconcileRetry(ctx restate.ObjectContext, st *PRState, run reconcileRun, kerr *KubeError) {
	result, msg := effectResultRetry, "[prstate] effect retry scheduled"
	if run.attempt >= maxRepoEffectRetries {
		result, msg = effectResultDropped, "[prstate] effect dropped after retries"
	}
	_, _ = restate.Run(ctx, func(rc restate.RunContext) (restate.Void, error) {
		effectsTotal.WithLabelValues(prwire.EffectFluxReconcile, result).Inc()
		slog.WarnContext(rc, msg, "pr", prwire.PRObjectKey(st.Repo, st.PRNumber), metricLabelKind, prwire.EffectFluxReconcile,
			"effect_key", run.key.String(), "attempt", run.attempt, "error", kerr.Error())
		return restate.Void{}, nil
	}, restate.WithName(result+"-"+prwire.EffectFluxReconcile))
	if result == effectResultDropped {
		return
	}
	restate.ObjectSend(ctx, prwire.PRObjectServiceName, prwire.PRObjectKey(st.Repo, st.PRNumber), prwire.PRHandlerRetryFluxReconcile).
		Send(FluxReconcileRetryRequest{Generation: run.key.GateGeneration, HeadSHA: run.key.HeadSHA, JobName: run.key.JobName, Attempt: run.attempt + 1}, restate.WithDelay(s.reconcileRetryDelay))
}

package prstate

// Restate-owned deploy path support: the read model and observation handlers
// synapse's deploy paths use (the Flux/Kustomization watcher, deploy alerts
// and main-branch workflow runs).
//
//   - GetDeployView (shared, read-only) is the per-PR deploy read model the
//     deploy paths route on: status, identity, recorded reconcile requests
//     and their progress. It replaces the registry reads (ResolvePRByNumber,
//     HasKustomizationRequests, HasExpectedDeployJobs, ListDeployingPRs rows).
//   - The rollout index is the cross-repo set of PRs whose completion is
//     gated on recorded Kustomization requests (the registry
//     ListDeployingPRs scan). PRObject maintains it after every commit with a
//     per-PR sequence so reordered one-way sends never resurrect a finished
//     rollout; it lives on the RepoObject keyed RolloutIndexKey under its own
//     K/V slot, and ListRollouts reads it shared.
//   - HandleDeployObserved applies one deploy-alert observation (expected
//     jobs, a started job) and publishes the carried facts, deploy_started
//     only when the PR actually transitioned to deploying.
//   - HandleReconcileLost re-patches one recorded reconcile request Flux
//     lost, once per observed lastHandledReconcileAt.

import (
	"maps"
	"slices"
	"sort"
	"strconv"
	"strings"

	restate "github.com/restatedev/sdk-go"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/pubsub"
)

// RolloutIndexKey is the RepoObject key holding the cross-repo rollout
// index. It is not a valid owner/repo, so it never collides with a repo key.
const RolloutIndexKey = "_rollouts"

// rolloutIndexStateKey is the K/V slot of the rollout index, distinct from
// the RepoState slot.
const rolloutIndexStateKey = "rollouts"

// MaxRolloutIndexEntries bounds the rollout index; beyond it the entry with
// the oldest insertion order is evicted.
const MaxRolloutIndexEntries = 512

// DeployViewRequest is the empty GetDeployView input; the object key names
// the PR.
type DeployViewRequest struct{}

// DeployViewOf renders the deploy read model of st. HasExpectedDeployJobs
// mirrors the registry HasExpectedDeployJobs query: recorded jobs, deployed
// jobs, or a deploying/deployed status.
func DeployViewOf(st *PRState) DeployView {
	if st == nil || st.Untracked() {
		return DeployView{}
	}
	return DeployView{
		Found: true, Repo: st.Repo, PRNumber: st.PRNumber, Status: st.RegistryStatus(), Branch: st.Branch,
		HeadSHA: st.HeadSHA, MergeSHA: st.MergeSHA, TargetID: st.TargetID, Agent: st.OwnerAgent(),
		Jobs: slices.Clone(st.Jobs), DeployedJobs: slices.Clone(st.DeployedJobs),
		KustomizationRequests: cloneRequests(st.KustomizationRequests), DeployedKustomizations: slices.Clone(st.DeployedKustomizations),
		DeploySuccessPending: st.DeploySuccessPending,
		HasExpectedDeployJobs: len(st.Jobs) > 0 || len(st.DeployedJobs) > 0 ||
			statusIn(st.RegistryStatus(), RegistryStatusDeploying, RegistryStatusDeployed),
	}
}

func cloneRequests(in map[string]KustomizationRequest) map[string]KustomizationRequest {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]KustomizationRequest, len(in))
	maps.Copy(out, in)
	return out
}

// GetDeployView returns the PR's deploy read model. It never takes the
// exclusive lock and never writes.
func (s *PRObjectService) GetDeployView(ctx restate.ObjectSharedContext, _ DeployViewRequest) (DeployView, error) {
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil {
		return DeployView{}, err
	}
	return DeployViewOf(st), nil
}

// InRollout reports whether st is a rollout the Kustomization watcher gates:
// deploying with at least one recorded reconcile request.
func (s *PRState) InRollout() bool {
	return s != nil && s.RegistryStatus() == RegistryStatusDeploying && len(s.KustomizationRequests) > 0
}

// TakeRolloutIndexUpdate reports the rollout-index update st owes after a
// commit, advancing its index sequence. ok is false when the indexed
// membership already matches.
func (s *PRState) TakeRolloutIndexUpdate() (RolloutIndexEvent, bool) {
	if s == nil || s.Repo == "" || s.PRNumber <= 0 {
		return RolloutIndexEvent{}, false
	}
	active := s.InRollout()
	if active == s.RolloutIndexed {
		return RolloutIndexEvent{}, false
	}
	s.RolloutIndexed = active
	s.RolloutIndexSeq++
	return RolloutIndexEvent{Repo: s.Repo, PRNumber: s.PRNumber, Seq: s.RolloutIndexSeq, Active: active}, true
}

// syncRolloutIndex sends the rollout index the membership change st owes,
// after persisting the advanced sequence.
func (s *PRObjectService) syncRolloutIndex(ctx restate.ObjectContext, st *PRState) {
	ev, ok := st.TakeRolloutIndexUpdate()
	if !ok {
		return
	}
	restate.Set(ctx, s.stateKey, st)
	restate.ObjectSend(ctx, prwire.RepoObjectServiceName, RolloutIndexKey, prwire.RepoHandlerRolloutIndex).Send(ev)
}

// RolloutIndexEvent is one PRObject rollout-index membership update.
type RolloutIndexEvent struct {
	Repo     string `json:"repo"`
	PRNumber int    `json:"pr_number"`
	Seq      uint64 `json:"seq"`
	Active   bool   `json:"active"`
}

// RolloutIndexEntry is one indexed PR: its last applied sequence, whether it
// is active, and its insertion order for eviction.
type RolloutIndexEntry struct {
	Seq    uint64 `json:"seq"`
	Active bool   `json:"active"`
	Order  uint64 `json:"order"`
}

// RolloutIndexState is the cross-repo rollout index. Inactive entries stay
// as sequence tombstones so a delayed older activation cannot resurrect a
// finished rollout; the bound evicts the oldest entries first.
type RolloutIndexState struct {
	Entries   map[string]RolloutIndexEntry `json:"entries"`
	NextOrder uint64                       `json:"next_order"`
}

// Apply records ev unless an equal or newer sequence for the PR is already
// recorded, and reports whether the index changed.
func (s *RolloutIndexState) Apply(ev RolloutIndexEvent) bool {
	repo := strings.TrimSpace(ev.Repo)
	if s == nil || repo == "" || ev.PRNumber <= 0 {
		return false
	}
	if s.Entries == nil {
		s.Entries = make(map[string]RolloutIndexEntry)
	}
	key := prwire.PRObjectKey(repo, ev.PRNumber)
	cur, ok := s.Entries[key]
	if ok && cur.Seq >= ev.Seq {
		return false
	}
	s.NextOrder++
	s.Entries[key] = RolloutIndexEntry{Seq: ev.Seq, Active: ev.Active, Order: s.NextOrder}
	s.evict()
	return true
}

func (s *RolloutIndexState) evict() {
	for len(s.Entries) > MaxRolloutIndexEntries {
		oldest, oldestOrder := "", uint64(0)
		for k, e := range s.Entries {
			if oldest == "" || e.Order < oldestOrder || (e.Order == oldestOrder && k < oldest) {
				oldest, oldestOrder = k, e.Order
			}
		}
		delete(s.Entries, oldest)
	}
}

// Active returns the active rollouts sorted by repo then PR number.
func (s *RolloutIndexState) Active() []RolloutRef {
	if s == nil {
		return nil
	}
	var out []RolloutRef
	for key, e := range s.Entries {
		if !e.Active {
			continue
		}
		repo, pr, ok := splitPRObjectKey(key)
		if ok {
			out = append(out, RolloutRef{Repo: repo, PRNumber: pr})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Repo != out[j].Repo {
			return out[i].Repo < out[j].Repo
		}
		return out[i].PRNumber < out[j].PRNumber
	})
	return out
}

// splitPRObjectKey parses repo#pr.
func splitPRObjectKey(key string) (string, int, bool) {
	i := strings.LastIndex(key, "#")
	if i <= 0 {
		return "", 0, false
	}
	pr, err := strconv.Atoi(key[i+1:])
	if err != nil || pr <= 0 {
		return "", 0, false
	}
	return key[:i], pr, true
}

// HandleRolloutIndex applies one rollout-index update. Only the
// RolloutIndexKey object holds the index; any other key ignores it.
func (*RepoObjectService) HandleRolloutIndex(ctx restate.ObjectContext, ev RolloutIndexEvent) error {
	if restate.Key(ctx) != RolloutIndexKey {
		return nil
	}
	st, err := restate.Get[*RolloutIndexState](ctx, rolloutIndexStateKey)
	if err != nil {
		return err
	}
	if st == nil {
		st = &RolloutIndexState{}
	}
	if st.Apply(ev) {
		restate.Set(ctx, rolloutIndexStateKey, st)
	}
	return nil
}

// ListRolloutsRequest is the empty ListRollouts input.
type ListRolloutsRequest struct{}

// ListRolloutsResponse is the active rollout set.
type ListRolloutsResponse struct {
	Rollouts []RolloutRef `json:"rollouts"`
}

// ListRollouts returns the active rollouts (shared, read-only).
func (*RepoObjectService) ListRollouts(ctx restate.ObjectSharedContext, _ ListRolloutsRequest) (ListRolloutsResponse, error) {
	st, err := restate.Get[*RolloutIndexState](ctx, rolloutIndexStateKey)
	if err != nil {
		return ListRolloutsResponse{}, err
	}
	return ListRolloutsResponse{Rollouts: st.Active()}, nil
}

// SplitDeployStartedFacts separates the deploy_started facts, which fire
// only when the carrying event transitioned the PR to deploying, from the
// rest.
func SplitDeployStartedFacts(facts []pubsub.PRLifecycleEvent) (started, rest []pubsub.PRLifecycleEvent) {
	for _, f := range facts {
		if f.Event == eventDeployStarted {
			started = append(started, f)
			continue
		}
		rest = append(rest, f)
	}
	return started, rest
}

// eventDeployStarted is the deploy_started lifecycle event name.
const eventDeployStarted = "deploy_started"

// ApplyDeployObserved applies one deploy-alert observation: the announced
// expected jobs, then the started job's deploying transition. It reports
// whether the PR transitioned to deploying.
func (s *PRState) ApplyDeployObserved(ev PREvent) bool {
	s.RecordExpectedJobs(ev.ExpectedJobs)
	job := strings.TrimSpace(ev.JobName)
	if job == "" {
		return false
	}
	if s.TransitionDeploying([]string{job}) {
		s.DisarmDeployFailure()
		return true
	}
	return false
}

// HandleDeployObserved applies one deploy-alert observation (a deploy-phase
// event, so merged PRs apply it) and publishes the carried facts:
// deploy_started only when the PR transitioned, every other fact always.
func (s *PRObjectService) HandleDeployObserved(ctx restate.ObjectContext, ev PREvent) error {
	started, rest := SplitDeployStartedFacts(ev.Facts)
	var transitioned bool
	ev.Facts = nil
	var committed *PRState
	if err := s.mutateRolloutEvent(ctx, ev, "HandleDeployObserved", func(st *PRState) {
		transitioned = st.ApplyDeployObserved(ev)
		committed = st
	}); err != nil || committed == nil {
		return err
	}
	s.runFacts(ctx, committed, gatedStartedFacts(started, rest, transitioned))
	return nil
}

// gatedStartedFacts returns the facts an event publishes: every fact, with
// the deploy_started ones only when the event transitioned the PR.
func gatedStartedFacts(started, rest []pubsub.PRLifecycleEvent, transitioned bool) []pubsub.PRLifecycleEvent {
	if !transitioned {
		return rest
	}
	out := make([]pubsub.PRLifecycleEvent, 0, len(started)+len(rest))
	return append(append(out, started...), rest...)
}

// ReconcileLostKey keys one lost-reconcile re-patch: the Kustomization and
// the observed lastHandledReconcileAt, in the current generation, so each
// distinct lost observation re-patches once.
func ReconcileLostKey(s *PRState, name, observedAt string) EffectKey {
	return s.EffectKeyFor(prwire.EffectFluxReconcile, "", "repatch:"+strings.TrimSpace(name)+"@"+strings.TrimSpace(observedAt), "")
}

// HandleReconcileLost re-patches one recorded reconcile request whose
// annotation Flux lost (the watcher saw a Kustomization handle a value older
// than the recorded request): it patches only that Kustomization with its
// recorded requestedAt. A PR no longer in rollout, or a name with no
// recorded request, does nothing. A retryable failure schedules the delayed reconcile retry.
func (s *PRObjectService) HandleReconcileLost(ctx restate.ObjectContext, ev PREvent) error {
	name := strings.TrimSpace(ev.JobName)
	if strings.TrimSpace(ev.Repo) == "" || ev.PRNumber <= 0 || name == "" {
		return nil
	}
	st, err := restate.Get[*PRState](ctx, s.stateKey)
	if err != nil || st == nil || !st.InRollout() || st.Untracked() {
		return err
	}
	req, ok := st.KustomizationRequests[name]
	if !ok {
		return nil
	}
	key := ReconcileLostKey(st, name, ev.ObservedReconcileAt)
	s.runReconcile(ctx, st, reconcileRun{key: key, match: key.MatchKey(), requests: map[string]KustomizationRequest{name: req}})
	return nil
}

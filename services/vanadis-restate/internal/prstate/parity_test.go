package prstate

// Cross-module parity goldens: every scenario under
// testdata/prlifecycle/parity/ holds one PR lifecycle as the event stream
// synapse sends Restate, the effect multiset
// legacy synapse performs for the same lifecycle (by kind and
// generation-free match key), and the hand-derived final pr_registry rows.
// This test replays the stream through the real PRObject and RepoObject
// handlers on a hermetic in-memory Restate: K/V round-trips through JSON
// like the real journal, journaled runs execute once, and every one-way
// send (index updates, forwards, projection writes, delayed deadlines) is
// delivered after the invocation that made it, delayed sends once the
// immediate ones drain. Fakes stand in for the fact bus, eri, GitHub,
// Kubernetes and the pr_registry writer. synapse's restate_parity_test.go
// drives the legacy router over the same files and must land on the same
// effects and rows.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/x/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/pkg/pubsub"
)

// parityFixtureDir holds one golden lifecycle per file.
const parityFixtureDir = prlifecycleFixtureDir + "parity/"

// parityInvocation is one Restate invocation synapse sends.
type parityInvocation struct {
	Object  string          `json:"object"`
	Handler string          `json:"handler"`
	Event   json.RawMessage `json:"event"`
}

// parityEffect is one effect by kind and generation-free match key.
type parityEffect struct {
	Kind string `json:"kind"`
	Key  string `json:"key"`
}

// parityFactState is the rollout state one published fact carried, keyed
// by the fact's generation-free match key.
type parityFactState struct {
	Key          string                 `json:"key"`
	RolloutState *pubsub.PRRolloutState `json:"rollout_state"`
}

// parityRow is the pr_registry row a lifecycle ends on.
type parityRow struct {
	Repo     string             `json:"repo"`
	PRNumber int                `json:"pr_number"`
	Branch   string             `json:"branch"`
	HeadSHA  string             `json:"head_sha"`
	MergeSHA string             `json:"merge_sha"`
	TargetID string             `json:"target_id"`
	Status   string             `json:"status"`
	Title    string             `json:"title"`
	Metadata ProjectionMetadata `json:"metadata"`
}

// parityScenario is one golden file; synapse-only fields are ignored here.
type parityScenario struct {
	Name              string             `json:"name"`
	Restate           []parityInvocation `json:"restate"`
	Effects           []parityEffect     `json:"effects"`
	FactRolloutStates []parityFactState  `json:"fact_rollout_states"`
	Rows              []parityRow        `json:"rows"`
}

func loadParityScenarios(t *testing.T) []parityScenario {
	t.Helper()
	paths, err := filepath.Glob(parityFixtureDir + "*.json")
	require.NoError(t, err)
	require.Len(t, paths, 12, "one golden per lifecycle scenario")
	sort.Strings(paths)
	out := make([]parityScenario, 0, len(paths))
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		require.NoError(t, err)
		var sc parityScenario
		require.NoError(t, json.Unmarshal(raw, &sc), p)
		require.NotEmpty(t, sc.Restate, p)
		out = append(out, sc)
	}
	return out
}

// simCall is one queued Restate invocation.
type simCall struct {
	service string
	key     string
	handler string
	input   []byte
}

// simHandler invokes one handler with a JSON input.
type simHandler func(ctx restate.ObjectContext, input []byte) error

func decodeInput[I any](input []byte) (I, error) {
	var in I
	err := json.Unmarshal(input, &in)
	return in, err
}

func bindErr[I any](f func(restate.ObjectContext, I) error) simHandler {
	return func(ctx restate.ObjectContext, input []byte) error {
		in, err := decodeInput[I](input)
		if err != nil {
			return err
		}
		return f(ctx, in)
	}
}

func bindOut[I, O any](f func(restate.ObjectContext, I) (O, error)) simHandler {
	return func(ctx restate.ObjectContext, input []byte) error {
		in, err := decodeInput[I](input)
		if err != nil {
			return err
		}
		_, err = f(ctx, in)
		return err
	}
}

// paritySim is the hermetic Restate plus every effect boundary.
type paritySim struct {
	t        *testing.T
	handlers map[string]simHandler
	kv       map[string][]byte
	pending  []simCall
	delayed  []simCall
	current  simCall
	seen     map[string]bool
	effects  []parityEffect
	facts    []parityFactState
	rows     map[string]ProjectionSnapshot
}

func newParitySim(t *testing.T) *paritySim {
	t.Helper()
	sim := &paritySim{t: t, kv: map[string][]byte{}, rows: map[string]ProjectionSnapshot{}}
	pr := NewPRObjectService().WithProjection(sim).WithFactPublisher(sim).WithEriEffector(sim).WithGitHub(sim).WithKube(sim)
	repo := NewRepoObjectService().WithGitHub(sim).WithKube(sim)
	sim.handlers = map[string]simHandler{
		"PRObject/" + "Register":                      bindOut(pr.Register),
		"PRObject/" + PRHandlerProject:                bindErr(pr.Project),
		"PRObject/" + PRHandlerCheckFailed:            bindErr(pr.HandleCheckFailed),
		"PRObject/" + PRHandlerCheckPassed:            bindErr(pr.HandleCheckPassed),
		"PRObject/" + PRHandlerCIRecovered:            bindOut(pr.HandleCIRecovered),
		"PRObject/" + PRHandlerCheckStatus:            bindErr(pr.HandleCheckStatus),
		"PRObject/" + PRHandlerConflict:               bindOut(pr.HandleConflict),
		"PRObject/" + "HandleConflictResolved":        bindErr(pr.HandleConflictResolved),
		"PRObject/" + PRHandlerSynchronized:           bindErr(pr.HandleSynchronized),
		"PRObject/" + PRHandlerMerged:                 bindErr(pr.HandleMerged),
		"PRObject/" + "HandleClosed":                  bindErr(pr.HandleClosed),
		"PRObject/" + "HandleReopened":                bindErr(pr.HandleReopened),
		"PRObject/" + PRHandlerOpened:                 bindErr(pr.HandleOpened),
		"PRObject/" + PRHandlerSelfRegistered:         bindErr(pr.HandleSelfRegistered),
		"PRObject/" + PRHandlerMainAdvanced:           bindErr(pr.HandleMainAdvanced),
		"PRObject/" + PRHandlerKustomizationRequested: bindErr(pr.HandleKustomizationRequested),
		"PRObject/" + PRHandlerRolloutProgress:        bindErr(pr.HandleRolloutProgress),
		"PRObject/" + PRHandlerDeploySuccess:          bindOut(pr.HandleDeploySuccess),
		"PRObject/" + PRHandlerDeployAttemptFailed:    bindErr(pr.HandleDeployAttemptFailed),
		"PRObject/" + PRHandlerDeployFailureDeadline:  bindErr(pr.HandleDeployFailureDeadline),
		"PRObject/" + PRHandlerDeployFailed:           bindErr(pr.HandleDeployFailed),
		"PRObject/" + "HandleDeployObserved":          bindErr(pr.HandleDeployObserved),
		"PRObject/" + "HandleReadinessRetry":          bindErr(pr.HandleReadinessRetry),
		"PRObject/" + "HandleDeploySuccessPublished":  bindOut(pr.HandleDeploySuccessPublished),
		"RepoObject/" + RepoHandlerPRIndex:            bindErr(repo.HandlePRIndex),
		"RepoObject/" + "HandleMainPush":              bindErr(repo.HandleMainPush),
		"RepoObject/" + RepoHandlerAutoUpdate:         bindErr(repo.HandleAutoUpdate),
		"RepoObject/" + RepoHandlerGitPush:            bindErr(repo.HandleGitPush),
		"RepoObject/" + RepoHandlerImagePromotion:     bindErr(repo.HandleImagePromotion),
		"RepoObject/" + "HandleKustomizationStatus":   bindErr(repo.HandleKustomizationStatus),
		"RepoObject/" + RepoHandlerRolloutIndex:       bindErr(repo.HandleRolloutIndex),
		"RepoObject/" + "HandlePushBySHA":             bindErr(repo.HandlePushBySHA),
		"RepoObject/" + "HandleHeldReplay":            bindErr(repo.HandleHeldReplay),
		"RepoObject/" + "HandleHeldExpiry":            bindErr(repo.HandleHeldExpiry),
	}
	return sim
}

// objectKey derives the object key synapse addresses an invocation to.
func objectKey(t *testing.T, inv parityInvocation) string {
	t.Helper()
	var addr struct {
		Repo     string `json:"repo"`
		PRNumber int    `json:"pr_number"`
	}
	require.NoError(t, json.Unmarshal(inv.Event, &addr))
	if inv.Object == RepoObjectServiceName {
		return RepoObjectKey(addr.Repo)
	}
	return PRObjectKey(addr.Repo, addr.PRNumber)
}

// send runs one top-level invocation and everything it causes.
func (s *paritySim) send(inv parityInvocation) {
	s.pending = append(s.pending, simCall{service: inv.Object, key: objectKey(s.t, inv), handler: inv.Handler, input: inv.Event})
	for steps := 0; len(s.pending)+len(s.delayed) > 0; steps++ {
		require.Less(s.t, steps, 500, "invocation cascade did not settle")
		if len(s.pending) == 0 {
			s.pending, s.delayed = s.delayed, nil
		}
		call := s.pending[0]
		s.pending = s.pending[1:]
		s.invoke(call)
	}
}

// invoke runs one queued call on a mock context backed by the sim K/V and
// requires it to succeed.
func (s *paritySim) invoke(call simCall) {
	require.NoError(s.t, s.run(call), "%s/%s on %s", call.service, call.handler, call.key)
}

// run runs one call on a mock context backed by the sim K/V and returns
// the handler error; only a successful call queues its sends.
func (s *paritySim) run(call simCall) error {
	h, ok := s.handlers[call.service+"/"+call.handler]
	require.True(s.t, ok, "unbound handler %s/%s", call.service, call.handler)
	slot := call.service + "/" + call.key
	mc := mocks.NewMockContext(s.t)
	sends := captureSends(s.t, mc)
	stubParityRun(mc)
	mc.On("Key").Return(call.key).Maybe()
	// Each object key has one K/V slot per state key ("state", plus the
	// rollout index slot on its RepoObject key).
	mc.On("Get", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		if raw, ok := s.kv[slot+"/"+args.String(0)]; ok {
			require.NoError(s.t, json.Unmarshal(raw, args.Get(1)))
		}
	}).Return(true, nil).Maybe()
	mc.On("Set", mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
		raw, err := json.Marshal(args.Get(1))
		require.NoError(s.t, err)
		s.kv[slot+"/"+args.String(0)] = raw
	}).Maybe()
	s.current, s.seen = call, map[string]bool{}
	if err := h(restate.WithMockContext(mc), call.input); err != nil {
		return err
	}
	for _, sent := range sends.snapshot() {
		raw, err := json.Marshal(sent.input)
		require.NoError(s.t, err)
		next := simCall{service: sent.service, key: sent.key, handler: sent.method, input: raw}
		if sent.delayed {
			s.delayed = append(s.delayed, next)
			continue
		}
		s.pending = append(s.pending, next)
	}
	return nil
}

// stubParityRun executes every journaled restate.Run once, whatever run
// options the step passes; a step error returns as terminal, standing in for
// retries exhausted.
func stubParityRun(mc *mocks.MockContext) {
	run := func(fn func(restate.RunContext) (any, error), output any, _ ...restate.RunOption) restate.TerminalError {
		res, err := fn(projRunContext{Context: context.Background()})
		if err != nil {
			return restate.TerminalErrorf("%v", err)
		}
		if out := reflect.ValueOf(output); res != nil && out.Kind() == reflect.Pointer && !out.IsNil() {
			out.Elem().Set(reflect.ValueOf(res))
		}
		return nil
	}
	for arity := 2; arity <= 6; arity++ {
		args := make([]any, arity)
		for i := range args {
			args[i] = mock.Anything
		}
		mc.On("Run", args...).Return(run).Maybe()
	}
}

// record appends one effect, once per key within an invocation: a
// promotion of several images or a reconcile of several Kustomizations is
// one keyed effect, exactly as synapse logs it.
func (s *paritySim) record(kind, key string) {
	if s.seen[kind+" "+key] {
		return
	}
	s.seen[kind+" "+key] = true
	s.effects = append(s.effects, parityEffect{Kind: kind, Key: key})
}

// currentRepoKey keys a repo effect on the RepoObject event being handled.
func (s *paritySim) currentRepoKey(kind string) string {
	ev, err := decodeInput[RepoEvent](s.current.input)
	require.NoError(s.t, err)
	return RepoEffectKey(kind, ev).MatchKey()
}

// currentPRKey keys a PR-scoped effect on the PRObject event being handled.
func (s *paritySim) currentPRKey(kind string) string {
	ev, err := decodeInput[PREvent](s.current.input)
	require.NoError(s.t, err)
	return EffectKey{Repo: NormalizeRepo(ev.Repo), PRNumber: ev.PRNumber, HeadSHA: usableHeadSHA(ev.HeadSHA), Kind: kind}.MatchKey()
}

func (s *paritySim) PublishFact(_ context.Context, _ string, f pubsub.PRLifecycleEvent) error {
	head := usableHeadSHA(f.HeadSHA)
	if head == "" {
		head = usableHeadSHA(f.MergeSHA)
	}
	key := EffectKey{Repo: f.Repo, PRNumber: f.PRNumber, HeadSHA: head, Kind: FactEffectKind(f.Event), JobName: f.JobName, CheckName: f.CheckName}
	s.record(key.Kind, key.MatchKey())
	s.facts = append(s.facts, parityFactState{Key: key.MatchKey(), RolloutState: f.RolloutState})
	return nil
}

func (s *paritySim) DeliverEri(_ context.Context, _ string, key EffectKey, _ EriEffect) error {
	s.record(key.Kind, key.MatchKey())
	return nil
}

func (s *paritySim) UpsertProjection(_ context.Context, snap ProjectionSnapshot) error {
	s.rows[PRObjectKey(snap.Repo, snap.PRNumber)] = snap
	return nil
}

func (s *paritySim) ListOpenPRs(context.Context, string) ([]GitHubPR, *GitHubError) {
	s.record(EffectGitHubAutoUpdate, s.currentRepoKey(EffectGitHubAutoUpdate))
	return nil, nil
}

func (*paritySim) HeadChecksBlocked(context.Context, string, string) bool { return false }

func (s *paritySim) UpdateBranch(context.Context, string, int) (UpdateBranchOutcome, *GitHubError) {
	s.record("unexpected_update_branch", s.current.key)
	return UpdateBranchOutcome(""), nil
}

func (s *paritySim) FetchPR(context.Context, string, int) (GitHubPR, *GitHubError) {
	s.record("unexpected_fetch_pr", s.current.key)
	return GitHubPR{}, nil
}

func (s *paritySim) FetchPRMergeability(context.Context, string, int) (GitHubPR, *GitHubError) {
	s.record(EffectGitHubConflictCheck, s.currentPRKey(EffectGitHubConflictCheck))
	return GitHubPR{}, nil
}

func (s *paritySim) RerunFailedJobs(context.Context, string, int64) *GitHubError {
	s.record(EffectGitHubRerun, s.currentRepoKey(EffectGitHubRerun))
	return nil
}

func (s *paritySim) DispatchGitPush(context.Context, GitPushRequest) *KubeError {
	s.record(EffectFluxGitPush, s.currentRepoKey(EffectFluxGitPush))
	return nil
}

func (s *paritySim) PromoteImageTag(context.Context, ImagePromotionRequest) (string, *KubeError) {
	s.record(EffectImagePromote, s.currentRepoKey(EffectImagePromote))
	return "", nil
}

func (s *paritySim) ReconcileKustomization(context.Context, string, string, string) *KubeError {
	if s.current.service == RepoObjectServiceName {
		s.record(EffectFluxReconcile, s.currentRepoKey(EffectFluxReconcile))
		return nil
	}
	s.record(EffectFluxReconcile, s.currentPRKey(EffectFluxReconcile))
	return nil
}

// sortedEffects orders a multiset for comparison.
func sortedEffects(in []parityEffect) []parityEffect {
	out := append([]parityEffect{}, in...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind != out[j].Kind {
			return out[i].Kind < out[j].Kind
		}
		return out[i].Key < out[j].Key
	})
	return out
}

// rowOf projects a written snapshot onto the golden row shape.
func rowOf(snap ProjectionSnapshot) parityRow {
	return parityRow{
		Repo: snap.Repo, PRNumber: snap.PRNumber, Branch: snap.Branch, HeadSHA: snap.HeadSHA, MergeSHA: snap.MergeSHA,
		TargetID: snap.TargetID, Status: snap.Status, Title: snap.Title, Metadata: snap.Metadata,
	}
}

// TestRestateOwnershipParity replays every golden lifecycle and asserts PRObject/RepoObject perform exactly the
// legacy effect multiset, publish facts with the legacy rollout states and
// write exactly the legacy final rows.
func TestRestateOwnershipParity(t *testing.T) {
	t.Parallel()
	tests := loadParityScenarios(t)
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			sim := newParitySim(t)
			for _, inv := range tc.Restate {
				sim.send(inv)
			}
			require.Equal(t, sortedEffects(tc.Effects), sortedEffects(sim.effects))
			require.ElementsMatch(t, tc.FactRolloutStates, sim.facts, "published fact rollout states")
			wantRows := make(map[string]parityRow, len(tc.Rows))
			for _, r := range tc.Rows {
				wantRows[PRObjectKey(r.Repo, r.PRNumber)] = r
			}
			gotRows := make(map[string]parityRow, len(sim.rows))
			for key, snap := range sim.rows {
				gotRows[key] = rowOf(snap)
				require.Equal(t, strings.TrimSpace(firstOwner(snap.Metadata)), snap.Agent, "row owner column follows metadata")
			}
			require.Equal(t, wantRows, gotRows)
		})
	}
}

// firstOwner is the owner a row's metadata names (agent, else bot_id).
func firstOwner(m ProjectionMetadata) string {
	if m.Agent != "" {
		return m.Agent
	}
	return m.BotID
}

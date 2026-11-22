package prstate

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"sync"
	"testing"

	restate "github.com/restatedev/sdk-go"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
)

// prlifecycleFixtureDir is the shared cross-module golden fixture directory
// at the aerial repo root; synapse's restateclient reads the same files.
const prlifecycleFixtureDir = "../../../../testdata/prlifecycle/"

func readPRLifecycleFixture(t *testing.T, name string, out any) {
	t.Helper()
	raw, err := os.ReadFile(prlifecycleFixtureDir + name)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, out))
}

// TestEffectKeyMatchKey pins the generation-free key against the shared
// golden fixture synapse's EffectMatchKey test reads: the generation never
// appears, whatever it is.
func TestEffectKeyMatchKey(t *testing.T) {
	t.Parallel()
	var fx struct {
		Cases []struct {
			Name      string `json:"name"`
			Repo      string `json:"repo"`
			PRNumber  int    `json:"pr_number"`
			HeadSHA   string `json:"head_sha"`
			Kind      string `json:"kind"`
			JobName   string `json:"job_name"`
			CheckName string `json:"check_name"`
			Want      string `json:"want"`
		} `json:"cases"`
	}
	readPRLifecycleFixture(t, "eri_effect_match_keys.json", &fx)
	require.NotEmpty(t, fx.Cases)
	for _, tc := range fx.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			key := EffectKey{Repo: tc.Repo, PRNumber: tc.PRNumber, HeadSHA: tc.HeadSHA, GateGeneration: 9, Kind: tc.Kind, JobName: tc.JobName, CheckName: tc.CheckName}
			require.Equal(t, tc.Want, key.MatchKey())
		})
	}
}

// TestPREventEriEffectsDecode pins that the golden synapse payload decodes
// into PREvent with its rendered effect and owner stamp intact.
func TestPREventEriEffectsDecode(t *testing.T) {
	t.Parallel()
	var fx struct {
		Event json.RawMessage `json:"event"`
	}
	readPRLifecycleFixture(t, "eri_effect_wire.json", &fx)
	tests := []struct {
		name        string
		raw         []byte
		wantEffects []EriEffect
	}{
		{
			name: "golden collaborator DM event", raw: fx.Event,
			wantEffects: []EriEffect{{Kind: EffectEriDirectMessage, ChannelID: "111111111111111111", Content: "<@42> CI check lint failed on mirrormere#1010"}},
		},
		{name: "event without effects", raw: []byte(`{"repo":"azylman/aerial","pr_number":1,"head_sha":"h1"}`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var ev PREvent
			require.NoError(t, json.Unmarshal(tc.raw, &ev))
			require.Equal(t, tc.wantEffects, ev.EriEffects)
		})
	}
}

const (
	testChannel = "111111111111111111"
	testTarget  = "222222222222222222"
	testDMKey   = "azylman/aerial#101@h1:gen0:eri_direct_message"
)

// effectPR seeds an open PR on head h1 owned by agent-alpha with a thread target.
func effectPR() *PRState {
	st := svcRow("h1", "", RegistryStatusOpen, TerminalNone)
	st.Agent = "agent-alpha"
	st.TargetID = testTarget
	return st
}

func dmEffect() EriEffect {
	return EriEffect{Kind: EffectEriDirectMessage, ChannelID: testChannel, Content: "hello"}
}

func dmKey(head string, gen uint64, check string) EffectKey {
	return EffectKey{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: head, GateGeneration: gen, Kind: EffectEriDirectMessage, CheckName: check}
}

// TestPlanEriEffects pins effect normalization, keying, owner addressing,
// target fallback, collapse of same-key duplicates, and generation dedup.
func TestPlanEriEffects(t *testing.T) {
	t.Parallel()
	dispatched := effectPR()
	dispatched.MarkEffectDispatched(dmKey("h1", 0, ""))
	unowned := effectPR()
	unowned.Agent = ""
	untargeted := effectPR()
	untargeted.TargetID = ""
	tests := []struct {
		name  string
		state *PRState
		ev    PREvent
		want  []PlannedEriEffect
	}{
		{name: "nil state plans nothing", state: nil, ev: PREvent{EriEffects: []EriEffect{dmEffect()}}},
		{name: "no effects plans nothing", state: effectPR(), ev: PREvent{HeadSHA: "h1"}},
		{
			name: "DM keyed on event head and check for the stored owner", state: effectPR(),
			ev:   PREvent{HeadSHA: " h2 ", CheckName: "Lint", EriEffects: []EriEffect{{Kind: " eri_direct_message ", ChannelID: " " + testChannel + " ", Content: " hello "}}},
			want: []PlannedEriEffect{{Effect: dmEffect(), Key: dmKey("h2", 0, "Lint"), Agent: "agent-alpha"}},
		},
		{
			name: "empty event head falls back to stored head", state: effectPR(),
			ev:   PREvent{EriEffects: []EriEffect{dmEffect()}},
			want: []PlannedEriEffect{{Effect: dmEffect(), Key: dmKey("h1", 0, ""), Agent: "agent-alpha"}},
		},
		{
			name: "prompt without channel lands in the stored target", state: effectPR(),
			ev: PREvent{HeadSHA: "h1", JobName: "eri", EriEffects: []EriEffect{{Kind: EffectEriPrompt, Content: "wake"}}},
			want: []PlannedEriEffect{{
				Effect: EriEffect{Kind: EffectEriPrompt, ChannelID: testTarget, Content: "wake"},
				Key:    EffectKey{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", Kind: EffectEriPrompt, JobName: "eri"},
				Agent:  "agent-alpha",
			}},
		},
		{name: "prompt with no channel and no target is dropped", state: untargeted, ev: PREvent{HeadSHA: "h1", EriEffects: []EriEffect{{Kind: EffectEriPrompt, Content: "wake"}}}},
		{
			name: "headless background dispatch is kept", state: untargeted,
			ev: PREvent{HeadSHA: "h1", EriEffects: []EriEffect{{Kind: EffectEriBackground, Content: "fix", Effort: " medium ", SkipIfActive: true}}},
			want: []PlannedEriEffect{{
				Effect: EriEffect{Kind: EffectEriBackground, Content: "fix", Effort: "medium", SkipIfActive: true},
				Key:    EffectKey{Repo: "azylman/aerial", PRNumber: 101, HeadSHA: "h1", Kind: EffectEriBackground},
				Agent:  "agent-alpha",
			}},
		},
		{name: "unknown kind is dropped", state: effectPR(), ev: PREvent{HeadSHA: "h1", EriEffects: []EriEffect{{Kind: "discord_alert", ChannelID: testChannel, Content: "x"}}}},
		{name: "empty content is dropped", state: effectPR(), ev: PREvent{HeadSHA: "h1", EriEffects: []EriEffect{{Kind: EffectEriDirectMessage, ChannelID: testChannel, Content: "  "}}}},
		{
			name: "same-key duplicates collapse to the first", state: effectPR(),
			ev:   PREvent{HeadSHA: "h1", EriEffects: []EriEffect{dmEffect(), {Kind: EffectEriDirectMessage, ChannelID: testChannel, Content: "again"}}},
			want: []PlannedEriEffect{{Effect: dmEffect(), Key: dmKey("h1", 0, ""), Agent: "agent-alpha"}},
		},
		{name: "already fired key is skipped", state: dispatched, ev: PREvent{HeadSHA: "h1", EriEffects: []EriEffect{dmEffect()}}},
		{
			name: "missing owner addresses the default agent", state: unowned,
			ev:   PREvent{HeadSHA: "h1", EriEffects: []EriEffect{dmEffect()}},
			want: []PlannedEriEffect{{Effect: dmEffect(), Key: dmKey("h1", 0, ""), Agent: "aerial"}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, PlanEriEffects(tc.state, tc.ev))
		})
	}
}

// TestEffectFingerprints pins the bounded idempotency key and payload
// digest against externally computed sha256 literals.
func TestEffectFingerprints(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		key      EffectKey
		wantIdem string
	}{
		{name: "collaborator DM", key: dmKey("h1", 0, ""), wantIdem: "prstate-54d500d389704715f4c085c744f8e1c8"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, testDMKey, tc.key.String())
			require.Equal(t, tc.wantIdem, EffectIdempotencyKey(tc.key))
		})
	}
}

// eriCall is one recorded EriEffector delivery.
type eriCall struct {
	agent string
	key   string
	eff   EriEffect
}

// fakeEriEffector is the hermetic EriEffector boundary.
type fakeEriEffector struct {
	mu    sync.Mutex
	err   error
	calls []eriCall
}

func (f *fakeEriEffector) DeliverEri(_ context.Context, agent string, key EffectKey, eff EriEffect) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, eriCall{agent: agent, key: key.String(), eff: eff})
	return f.err
}

func (f *fakeEriEffector) Calls() []eriCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]eriCall(nil), f.calls...)
}

var errFakeEri = errors.New("eri unavailable")

// TestPRObjectServiceEriEffects pins effect ownership on the collaborator
// alert handler: restate events deliver to the stored owner and commit the
// key, a failed or unconfigured delivery schedules a delayed retry without
// committing, and a fired key never re-fires.
func TestPRObjectServiceEriEffects(t *testing.T) {
	t.Parallel()
	dispatched := func() *PRState {
		st := effectPR()
		st.MarkEffectDispatched(dmKey("h1", 0, ""))
		return st
	}
	retrySend := []sentCall{{service: "PRObject", key: "azylman/aerial", method: "RetryEriEffect", input: EriEffectRetry{Effect: dmEffect(), HeadSHA: "h1", Attempt: 1}, delayed: true}}
	tests := []struct {
		name           string
		state          *PRState
		eri            *fakeEriEffector
		wantCalls      []eriCall
		wantRun        bool
		wantDispatched bool
		wantErr        bool
		wantSends      []sentCall
	}{
		{
			name: "restate owner delivers to the stored owner", state: effectPR(), eri: &fakeEriEffector{},
			wantCalls: []eriCall{{agent: "agent-alpha", key: testDMKey, eff: dmEffect()}}, wantRun: true, wantDispatched: true,
		},
		{
			name: "failed delivery schedules a retry", state: effectPR(), eri: &fakeEriEffector{err: errFakeEri},
			wantCalls: []eriCall{{agent: "agent-alpha", key: testDMKey, eff: dmEffect()}}, wantRun: true, wantSends: retrySend,
		},
		{name: "unconfigured effector schedules a retry", state: effectPR(), wantRun: true, wantSends: retrySend},
		{name: "fired key never re-fires", state: dispatched(), eri: &fakeEriEffector{}, wantDispatched: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mc, rec := newStateMock(t, tc.state)
			run := stubRun(mc)
			svc := NewPRObjectService()
			if tc.eri != nil {
				svc = svc.WithEriEffector(tc.eri)
			}
			err := svc.HandleCollaboratorAlert(restate.WithMockContext(mc), PREvent{Repo: "aerial", PRNumber: 101, HeadSHA: "h1", EriEffects: []EriEffect{dmEffect()}})
			require.Equal(t, tc.wantErr, restate.IsTerminalError(err), "err = %v", err)
			require.Equal(t, !tc.wantErr, rec.setCalled)
			if tc.eri != nil {
				require.Equal(t, tc.wantCalls, tc.eri.Calls())
			}
			require.Equal(t, tc.wantRun, run.ran)
			require.Equal(t, tc.wantDispatched, rec.saved.WasEffectDispatched(dmKey("h1", 0, "")))
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}

// TestPRObjectServiceRetryEriEffect pins the delayed re-delivery: it fires
// to the current owner and commits, re-schedules on failure until the
// retry budget is spent, and drops retries for gone PRs, moved generations,
// fired keys, and unusable effects.
func TestPRObjectServiceRetryEriEffect(t *testing.T) {
	t.Parallel()
	moved := effectPR()
	moved.GateGeneration = 1
	fired := effectPR()
	fired.MarkEffectDispatched(dmKey("h1", 0, ""))
	retry := func(attempt int) EriEffectRetry {
		return EriEffectRetry{Effect: dmEffect(), HeadSHA: "h1", Attempt: attempt}
	}
	tests := []struct {
		name           string
		state          *PRState
		failGet        bool
		req            EriEffectRetry
		eri            *fakeEriEffector
		wantErr        bool
		wantCalls      int
		wantRun        bool
		wantSet        bool
		wantDispatched bool
		wantSends      []sentCall
	}{
		{name: "kv failure surfaces", failGet: true, req: retry(1), eri: &fakeEriEffector{}, wantErr: true},
		{name: "gone PR drops", state: nil, req: retry(1), eri: &fakeEriEffector{}},
		{name: "moved generation drops", state: moved, req: retry(1), eri: &fakeEriEffector{}},
		{name: "fired key drops", state: fired, req: retry(1), eri: &fakeEriEffector{}},
		{name: "unusable effect drops", state: effectPR(), req: EriEffectRetry{Effect: EriEffect{Kind: EffectEriDirectMessage}, HeadSHA: "h1"}, eri: &fakeEriEffector{}},
		{name: "delivery commits the key", state: effectPR(), req: retry(1), eri: &fakeEriEffector{}, wantCalls: 1, wantRun: true, wantSet: true, wantDispatched: true},
		{
			name: "failure re-schedules the next attempt", state: effectPR(), req: retry(2), eri: &fakeEriEffector{err: errFakeEri}, wantCalls: 1, wantRun: true,
			wantSends: []sentCall{{service: "PRObject", key: "azylman/aerial", method: "RetryEriEffect", input: retry(3), delayed: true}},
		},
		{name: "spent budget drops", state: effectPR(), req: retry(6), eri: &fakeEriEffector{err: errFakeEri}, wantCalls: 1, wantRun: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if tc.failGet {
				mc := newStateErrorMock(t)
				err := NewPRObjectService().WithEriEffector(tc.eri).RetryEriEffect(restate.WithMockContext(mc), tc.req)
				require.Error(t, err)
				return
			}
			mc, rec := newStateMock(t, tc.state)
			run := stubRun(mc)
			err := NewPRObjectService().WithEriEffector(tc.eri).RetryEriEffect(restate.WithMockContext(mc), tc.req)
			require.Equal(t, tc.wantErr, err != nil)
			require.Len(t, tc.eri.Calls(), tc.wantCalls)
			require.Equal(t, tc.wantRun, run.ran)
			require.Equal(t, tc.wantSet, rec.setCalled)
			if tc.wantSet {
				require.Equal(t, tc.wantDispatched, rec.saved.WasEffectDispatched(dmKey("h1", 0, "")))
			}
			require.Equal(t, tc.wantSends, rec.sends.snapshot())
		})
	}
}

// sessionCall is one recorded EriSessionClient call.
type sessionCall struct {
	method   string
	agent    string
	channel  string
	content  string
	key      string
	dispatch ericlient.BrainDispatchRequest
}

// fakeSessionClient is the hermetic EriSessionClient boundary.
type fakeSessionClient struct {
	mu    sync.Mutex
	err   error
	calls []sessionCall
}

func (f *fakeSessionClient) record(c sessionCall) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, c)
	return f.err
}

func (f *fakeSessionClient) PromptAgent(_ context.Context, agent, threadID, prompt, idempotencyKey string) error {
	return f.record(sessionCall{method: "prompt", agent: agent, channel: threadID, content: prompt, key: idempotencyKey})
}

func (f *fakeSessionClient) DispatchRestate(_ context.Context, req ericlient.BrainDispatchRequest) error {
	return f.record(sessionCall{method: "dispatch", dispatch: req})
}

func (f *fakeSessionClient) MessageAgent(_ context.Context, agent, channelID, content string) error {
	return f.record(sessionCall{method: "message", agent: agent, channel: channelID, content: content})
}

// TestSessionEriEffectorDeliverEri pins routing of each eri kind onto the
// agent-scoped Restate session surface with the EffectKey-derived
// idempotency key, busy skip-if-active dispatches counting as delivered,
// and unknown kinds or a missing client failing.
func TestSessionEriEffectorDeliverEri(t *testing.T) {
	t.Parallel()
	key := dmKey("h1", 0, "")
	idem := "prstate-54d500d389704715f4c085c744f8e1c8"
	tests := []struct {
		name      string
		nilClient bool
		clientErr error
		eff       EriEffect
		wantErr   bool
		wantCalls []sessionCall
	}{
		{
			name: "prompt enqueues the owner thread turn", eff: EriEffect{Kind: EffectEriPrompt, ChannelID: testTarget, Content: "wake"},
			wantCalls: []sessionCall{{method: "prompt", agent: "agent-alpha", channel: testTarget, content: "wake", key: idem}},
		},
		{
			name: "background dispatches on the owner session", eff: EriEffect{Kind: EffectEriBackground, ChannelID: testTarget, Content: "fix", Effort: "medium", SkipIfActive: true},
			wantCalls: []sessionCall{{method: "dispatch", dispatch: ericlient.BrainDispatchRequest{ID: idem, Agent: "agent-alpha", Source: "prstate", TargetID: testTarget, Prompt: "fix", Effort: "medium", SkipIfActive: true}}},
		},
		{
			name: "busy skip-if-active dispatch counts as delivered", clientErr: ericlient.ErrDispatchActive, eff: EriEffect{Kind: EffectEriBackground, Content: "fix", SkipIfActive: true},
			wantCalls: []sessionCall{{method: "dispatch", dispatch: ericlient.BrainDispatchRequest{ID: idem, Agent: "agent-alpha", Source: "prstate", Prompt: "fix", SkipIfActive: true}}},
		},
		{
			name: "dispatch failure surfaces", clientErr: errFakeEri, eff: EriEffect{Kind: EffectEriBackground, Content: "fix"}, wantErr: true,
			wantCalls: []sessionCall{{method: "dispatch", dispatch: ericlient.BrainDispatchRequest{ID: idem, Agent: "agent-alpha", Source: "prstate", Prompt: "fix"}}},
		},
		{
			name: "direct message uses the owner sender", eff: dmEffect(),
			wantCalls: []sessionCall{{method: "message", agent: "agent-alpha", channel: testChannel, content: "hello"}},
		},
		{name: "unknown kind fails", eff: EriEffect{Kind: "discord_alert", Content: "x"}, wantErr: true},
		{name: "missing client fails", nilClient: true, eff: dmEffect(), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := &fakeSessionClient{err: tc.clientErr}
			effector := NewSessionEriEffector(client)
			if tc.nilClient {
				effector = NewSessionEriEffector(nil)
			}
			err := effector.DeliverEri(context.Background(), "agent-alpha", key, tc.eff)
			require.Equal(t, tc.wantErr, err != nil, "err = %v", err)
			require.Equal(t, tc.wantCalls, client.calls)
		})
	}
}

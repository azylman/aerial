package app

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
)

var drainTestFullAlpha = []string{
	"ThreadSession-agent-alpha",
	"DispatchSession-agent-alpha",
	"VoiceSession-agent-alpha",
	"SessionManager-agent-alpha",
	"TurnRunner-agent-alpha",
	"LLM-agent-alpha",
}

func drainTestServices(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, n := range names {
		quoted = append(quoted, `{"name":`+fmt.Sprintf("%q", n)+`}`)
	}
	return strings.Join(quoted, ",")
}

var drainTestFullAerial = []string{
	"ThreadSession",
	"DispatchSession",
	"VoiceSession",
	"SessionManager",
	"TurnRunner",
	"LLM",
}

const (
	drainTestSelf = "eri-alpha-restate-45977f4fb5"
	drainTestPrev = "eri-alpha-restate-7c975b8446"
	drainTestNext = "eri-alpha-restate-9d1e2f3a4b"
)

func drainTestURI(host string) string {
	return "http://" + host + ".default.svc.cluster.local:9080/"
}

// drainTestDeployment renders one admin deployment whose uri host label is host
// (the deployment ID is "dep-" + host).
func drainTestDeployment(host, created string, services []string) string {
	return `{"id":` + fmt.Sprintf("%q", "dep-"+host) + `,"uri":` + fmt.Sprintf("%q", drainTestURI(host)) + `,"created_at":` + fmt.Sprintf("%q", created) + `,"services":[` + drainTestServices(services) + `]}`
}

func drainTestPayload(deps ...string) string {
	return `{"deployments":[` + strings.Join(deps, ",") + `]}`
}

type drainDepSpec struct {
	id       string
	host     string
	created  string
	services []string
}

type decideDrainCase struct {
	name              string
	selfName          string
	want              []string
	deps              []drainDepSpec
	prior             int
	wantConfirmations int
	wantDrain         bool
}

func drainTestDeps(t *testing.T, specs []drainDepSpec) []deploymentInfo {
	t.Helper()
	deps := make([]deploymentInfo, 0, len(specs))
	for _, spec := range specs {
		deps = append(deps, deploymentInfo{ID: spec.id, HostLabel: spec.host, CreatedAt: drainTestTime(t, spec.created), Services: drainTestSet(spec.services)})
	}
	return deps
}

func runDecideDrainCase(t *testing.T, tc decideDrainCase) {
	t.Helper()
	gotConfirmations, gotDrain := decideDrain(tc.selfName, tc.want, drainTestDeps(t, tc.deps), tc.prior)
	if gotConfirmations != tc.wantConfirmations || gotDrain != tc.wantDrain {
		t.Errorf("decideDrain() = (%d, %v), want (%d, %v)",
			gotConfirmations, gotDrain, tc.wantConfirmations, tc.wantDrain)
	}
}

func drainTestTime(t *testing.T, raw string) time.Time {
	t.Helper()
	if raw == "" {
		return time.Time{}
	}
	ts, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("bad test timestamp %q: %v", raw, err)
	}
	return ts
}

func drainTestSet(names []string) map[string]bool {
	set := make(map[string]bool, len(names))
	for _, n := range names {
		set[n] = true
	}
	return set
}

func TestDecideDrain_Suite(t *testing.T) {
	t.Parallel()
	older := "2026-01-01T00:00:00Z"
	newer := "2026-06-01T00:00:00Z"
	full := drainTestFullAlpha
	self := drainDepSpec{id: "dep-self", host: drainTestSelf, created: older, services: full}
	selfNewest := drainDepSpec{id: "dep-self", host: drainTestSelf, created: newer, services: full}
	selfReregistered := drainDepSpec{id: "dep-self-fresh", host: drainTestSelf, created: newer, services: full}
	tied := drainDepSpec{id: "dep-tied", host: drainTestNext, created: older, services: full}
	ghost := drainDepSpec{id: "dep-ghost", created: newer, services: full}
	prev := drainDepSpec{id: "dep-prev", host: drainTestPrev, created: older, services: full}
	next := drainDepSpec{id: "dep-next", host: drainTestNext, created: newer, services: full}
	tests := []decideDrainCase{
		{name: "single deployment is healthy primary", selfName: drainTestSelf, want: full, deps: []drainDepSpec{self}, wantConfirmations: 0, wantDrain: false},
		{name: "regression: self not registered yet, only previous version listed", selfName: drainTestSelf, want: full, deps: []drainDepSpec{prev}, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "regression: self registered as newest never drains", selfName: drainTestSelf, want: full, deps: []drainDepSpec{prev, selfNewest}, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "regression: same replicaset listed twice first sighting resets", selfName: drainTestSelf, want: full, deps: []drainDepSpec{self, selfReregistered}, wantConfirmations: 0, wantDrain: false},
		{name: "regression: same replicaset listed twice second sighting still resets", selfName: drainTestSelf, want: full, deps: []drainDepSpec{self, selfReregistered}, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "equal created_at with other deployment resets", selfName: drainTestSelf, want: full, deps: []drainDepSpec{self, tied}, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "newer full-set deployment without uri never drains", selfName: drainTestSelf, want: full, deps: []drainDepSpec{self, ghost}, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "genuine supersede first sighting", selfName: drainTestSelf, want: full, deps: []drainDepSpec{self, next}, prior: 0, wantConfirmations: 1, wantDrain: false},
		{name: "genuine supersede second poll latches", selfName: drainTestSelf, want: full, deps: []drainDepSpec{self, next}, prior: 1, wantConfirmations: 2, wantDrain: true},
		{name: "third consecutive poll stays latched", selfName: drainTestSelf, want: full, deps: []drainDepSpec{self, next}, prior: 2, wantConfirmations: 3, wantDrain: true},
		{name: "self name prefix is not an exact match", selfName: "eri-alpha-restate", want: full, deps: []drainDepSpec{self, next}, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "deployment without uri host is never self", selfName: drainTestSelf, want: full, deps: []drainDepSpec{{id: "dep-self", created: older, services: full}, next}, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "empty self name resets", selfName: "", want: full, deps: []drainDepSpec{self, next}, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "empty wanted set resets", selfName: drainTestSelf, deps: []drainDepSpec{self, next}, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "partial newer set resets", selfName: drainTestSelf, want: full, deps: []drainDepSpec{self, {id: "dep-next", host: drainTestNext, created: newer, services: full[:5]}}, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "empty listing resets", selfName: drainTestSelf, want: full, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "self with partial set is inconclusive", selfName: drainTestSelf, want: full, deps: []drainDepSpec{{id: "dep-self", host: drainTestSelf, created: older, services: full[:5]}, next}, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "zero self timestamp resets", selfName: drainTestSelf, want: full, deps: []drainDepSpec{{id: "dep-self", host: drainTestSelf, services: full}, next}, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "zero other timestamp resets", selfName: drainTestSelf, want: full, deps: []drainDepSpec{self, {id: "dep-next", host: drainTestNext, services: full}}, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "unrelated deployments skipped", selfName: drainTestSelf, want: full, deps: []drainDepSpec{self, {id: "dep-other", host: "other-svc", created: newer, services: []string{"OtherStack-svc"}}}, wantConfirmations: 0, wantDrain: false},
		{name: "aerial default agent uses unscoped names", selfName: "eri-restate-1a2b3c4d5e", want: drainTestFullAerial, deps: []drainDepSpec{{id: "dep-a", host: "eri-restate-1a2b3c4d5e", created: older, services: drainTestFullAerial}, {id: "dep-b", host: "eri-restate-6f7a8b9c0d", created: newer, services: drainTestFullAerial}}, prior: 1, wantConfirmations: 2, wantDrain: true},
		{name: "aerial default agent ignores newer agent-alpha stack", selfName: "eri-restate-1a2b3c4d5e", want: drainTestFullAerial, deps: []drainDepSpec{{id: "dep-a", host: "eri-restate-1a2b3c4d5e", created: older, services: drainTestFullAerial}, next}, prior: 1, wantConfirmations: 0, wantDrain: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDecideDrainCase(t, tc)
		})
	}
}

func TestDrainSelfName_Suite(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		hostname string
		want     string
	}{
		{name: "pod name strips suffix", hostname: "eri-alpha-restate-45977f4fb5-24mgs", want: "eri-alpha-restate-45977f4fb5"},
		{name: "surrounding space trimmed", hostname: "  eri-restate-1a2b3c4d5e-abcde\n", want: "eri-restate-1a2b3c4d5e"},
		{name: "single dash", hostname: "pod-x", want: "pod"},
		{name: "no dash", hostname: "localhost", want: ""},
		{name: "trailing dash", hostname: "eri-restate-", want: ""},
		{name: "leading dash only", hostname: "-abc", want: ""},
		{name: "empty", hostname: "", want: ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := drainSelfName(tc.hostname); got != tc.want {
				t.Errorf("drainSelfName(%q) = %q, want %q", tc.hostname, got, tc.want)
			}
		})
	}
}

func TestDrainServiceNames_Suite(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		agentName string
		want      string
	}{
		{name: "agent-alpha scoped", agentName: "agent-alpha", want: "ThreadSession-agent-alpha,DispatchSession-agent-alpha,VoiceSession-agent-alpha,SessionManager-agent-alpha,TurnRunner-agent-alpha,LLM-agent-alpha"},
		{name: "aerial default unscoped", agentName: "aerial", want: "ThreadSession,DispatchSession,VoiceSession,SessionManager,TurnRunner,LLM"},
		{name: "empty agent unscoped", agentName: "", want: "ThreadSession,DispatchSession,VoiceSession,SessionManager,TurnRunner,LLM"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := strings.Join(drainServiceNames(tc.agentName), ","); got != tc.want {
				t.Errorf("drainServiceNames(%q) = %q, want %q", tc.agentName, got, tc.want)
			}
		})
	}
}

type fakeDrainTarget struct {
	calls atomic.Int32
}

func (f *fakeDrainTarget) BeginDrain() {
	f.calls.Add(1)
}

type drainStepCase struct {
	name        string
	payloads    []string
	steps       int
	wantLatched bool
	wantDrains  int
}

func runDrainStepCase(t *testing.T, tc drainStepCase) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/deployments" {
			t.Errorf("unexpected poll path %q", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		idx := int(calls.Add(1)) - 1
		if idx >= len(tc.payloads) {
			idx = len(tc.payloads) - 1
		}
		body := tc.payloads[idx]
		if body == "ERR500" {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	state := newDrainPollState(drainTestSelf, drainTestFullAlpha)
	fakes := []*fakeDrainTarget{{}, {}}
	targets := []drainTarget{fakes[0], fakes[1]}
	for range tc.steps {
		state.step(context.Background(), srv.URL, 5*time.Second, targets)
	}
	if state.latched != tc.wantLatched {
		t.Errorf("latched = %v, want %v (confirmations=%d self=%q)", state.latched, tc.wantLatched, state.confirmations, state.selfName)
	}
	for i, fake := range fakes {
		if got := int(fake.calls.Load()); got != tc.wantDrains {
			t.Errorf("target %d BeginDrain calls = %d, want %d", i, got, tc.wantDrains)
		}
	}
}

func TestDrainPollState_Step_Suite(t *testing.T) {
	t.Parallel()
	older := "2026-01-01T00:00:00Z"
	newer := "2026-06-01T00:00:00Z"
	selfOnly := drainTestPayload(drainTestDeployment(drainTestSelf, older, drainTestFullAlpha))
	prevOnly := drainTestPayload(drainTestDeployment(drainTestPrev, older, drainTestFullAlpha))
	prevThenSelf := drainTestPayload(
		drainTestDeployment(drainTestPrev, older, drainTestFullAlpha),
		drainTestDeployment(drainTestSelf, newer, drainTestFullAlpha),
	)
	both := drainTestPayload(
		drainTestDeployment(drainTestSelf, older, drainTestFullAlpha),
		drainTestDeployment(drainTestNext, newer, drainTestFullAlpha),
	)
	partial := drainTestPayload(
		drainTestDeployment(drainTestSelf, older, drainTestFullAlpha),
		drainTestDeployment(drainTestNext, newer, drainTestFullAlpha[:5]),
	)
	tests := []drainStepCase{
		{name: "healthy primary never latches", payloads: []string{selfOnly, selfOnly}, steps: 2, wantLatched: false, wantDrains: 0},
		{name: "rollout race: self registers after first poll never latches", payloads: []string{prevOnly, prevThenSelf, prevThenSelf, prevThenSelf}, steps: 4, wantLatched: false, wantDrains: 0},
		{name: "latches after two consecutive newer polls", payloads: []string{both, both}, steps: 2, wantLatched: true, wantDrains: 1},
		{name: "single newer sighting does not latch", payloads: []string{selfOnly, both}, steps: 2, wantLatched: false, wantDrains: 0},
		{name: "error resets confirmations", payloads: []string{both, "ERR500", both, both}, steps: 4, wantLatched: true, wantDrains: 1},
		{name: "partial set never latches", payloads: []string{selfOnly, partial, partial, partial}, steps: 4, wantLatched: false, wantDrains: 0},
		{name: "unreachable admin never latches", payloads: []string{"ERR500", "ERR500"}, steps: 2, wantLatched: false, wantDrains: 0},
		{name: "empty listing never latches", payloads: []string{`{"deployments":[]}`}, steps: 1, wantLatched: false, wantDrains: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDrainStepCase(t, tc)
		})
	}
}

type parseDrainCase struct {
	name       string
	body       string
	wantIDs    string
	wantLabels string
	wantErr    bool
}

func runParseDrainCase(t *testing.T, tc parseDrainCase) {
	t.Helper()
	deps, err := parseDrainDeployments([]byte(tc.body))
	if tc.wantErr {
		if err == nil {
			t.Fatalf("expected decode error, got %+v", deps)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected decode error: %v", err)
	}
	var ids, labels []string
	for _, d := range deps {
		ids = append(ids, d.ID)
		labels = append(labels, d.HostLabel)
	}
	if got := strings.Join(ids, ","); got != tc.wantIDs {
		t.Errorf("deployment ids = %q, want %q", got, tc.wantIDs)
	}
	if got := strings.Join(labels, ","); got != tc.wantLabels {
		t.Errorf("deployment host labels = %q, want %q", got, tc.wantLabels)
	}
}

func TestParseDrainDeployments_Suite(t *testing.T) {
	t.Parallel()
	older := "2026-01-01T00:00:00Z"
	newer := "2026-06-01T00:00:00Z"
	tests := []parseDrainCase{
		{name: "wrapped shape", body: drainTestPayload(drainTestDeployment("eri-alpha-restate-aaa", older, drainTestFullAlpha), drainTestDeployment("eri-alpha-restate-bbb", newer, drainTestFullAlpha)), wantIDs: "dep-eri-alpha-restate-aaa,dep-eri-alpha-restate-bbb", wantLabels: "eri-alpha-restate-aaa,eri-alpha-restate-bbb"},
		{name: "bare array shape", body: `[` + drainTestDeployment("eri-restate-aaa", older, drainTestFullAlpha) + `]`, wantIDs: "dep-eri-restate-aaa", wantLabels: "eri-restate-aaa"},
		{name: "live uri shape", body: `[{"id":"dp_1","uri":"http://eri-alpha-restate-45977f4fb5.default.svc.cluster.local:9080/","created_at":"` + older + `","services":[]}]`, wantIDs: "dp_1", wantLabels: "eri-alpha-restate-45977f4fb5"},
		{name: "bare host uri", body: `[{"id":"dp_1","uri":"http://eri-restate:9080","created_at":"` + older + `","services":[]}]`, wantIDs: "dp_1", wantLabels: "eri-restate"},
		{name: "missing uri has empty label", body: `[{"id":"dp_1","created_at":"` + older + `","services":[]}]`, wantIDs: "dp_1", wantLabels: ""},
		{name: "unparseable uri has empty label", body: `[{"id":"dp_1","uri":"http://[::1","created_at":"` + older + `","services":[]}]`, wantIDs: "dp_1", wantLabels: ""},
		{name: "empty wrapped list", body: `{"deployments":[]}`, wantIDs: "", wantLabels: ""},
		{name: "empty bare array", body: `[]`, wantIDs: "", wantLabels: ""},
		{name: "invalid json errors", body: `not json`, wantErr: true},
		{name: "missing id errors", body: drainTestPayload(`{"created_at":"` + older + `","services":[]}`), wantErr: true},
		{name: "empty object errors", body: `{}`, wantErr: true},
		{name: "null deployments errors", body: `{"deployments":null}`, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runParseDrainCase(t, tc)
		})
	}
}

type fetchDrainCase struct {
	name      string
	status    int
	body      string
	wantCount int
	wantErr   bool
}

func runFetchDrainCase(t *testing.T, tc fetchDrainCase) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(tc.status)
		_, _ = w.Write([]byte(tc.body))
	}))
	defer srv.Close()
	target := srv.URL
	deps, err := fetchDrainDeployments(context.Background(), target, 5*time.Second)
	if tc.wantErr {
		if err == nil {
			t.Fatalf("expected fetch error, got %+v", deps)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected fetch error: %v", err)
	}
	if len(deps) != tc.wantCount {
		t.Errorf("deployment count = %d, want %d", len(deps), tc.wantCount)
	}
}

func TestFetchDrainDeployments_Suite(t *testing.T) {
	t.Parallel()
	older := "2026-01-01T00:00:00Z"
	tests := []fetchDrainCase{
		{name: "ok wrapped listing", status: http.StatusOK, body: drainTestPayload(drainTestDeployment("dep-a", older, drainTestFullAlpha), drainTestDeployment("dep-b", older, drainTestFullAlpha)), wantCount: 2},
		{name: "server error fails fail-open", status: http.StatusInternalServerError, body: "boom", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runFetchDrainCase(t, tc)
		})
	}
}

type signalDrainTarget struct {
	calls atomic.Int32
	once  sync.Once
	done  chan struct{}
}

func newSignalDrainTarget() *signalDrainTarget {
	return &signalDrainTarget{done: make(chan struct{})}
}

func (f *signalDrainTarget) BeginDrain() {
	f.calls.Add(1)
	f.once.Do(func() { close(f.done) })
}

func TestDrainPollState_Step_Guards(t *testing.T) {
	t.Parallel()
	empty := newDrainPollState(drainTestSelf, drainTestFullAlpha)
	empty.step(context.Background(), "   ", time.Second, nil)
	if empty.confirmations != 0 || empty.latched {
		t.Errorf("empty admin URL must be a no-op, got %+v", empty)
	}

	latched := newDrainPollState(drainTestSelf, drainTestFullAlpha)
	latched.latched = true
	fake := newSignalDrainTarget()
	latched.step(context.Background(), "http://127.0.0.1:1", time.Second, []drainTarget{fake})
	if fake.calls.Load() != 0 {
		t.Errorf("latched state must not poll or drain, got %d calls", fake.calls.Load())
	}
}

type drainPollerRunCase struct {
	name     string
	bodies   []string
	interval time.Duration
	timeout  time.Duration
}

func runDrainPollerRunCase(t *testing.T, tc drainPollerRunCase) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		idx := int(calls.Add(1)) - 1
		if idx >= len(tc.bodies) {
			idx = len(tc.bodies) - 1
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(tc.bodies[idx]))
	}))
	defer srv.Close()

	appCfg := config.NewFromData(&config.ConfigData{RestateAdminURL: srv.URL})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := newSignalDrainTarget()
	startDrainPollerWithInterval(ctx, appCfg, drainTestSelf, drainTestFullAlpha, []drainTarget{fake}, tc.interval, tc.timeout)

	select {
	case <-fake.done:
	case <-time.After(5 * time.Second):
		t.Fatalf("poller did not latch drain")
	}
	if got := fake.calls.Load(); got < 1 {
		t.Errorf("expected at least one BeginDrain call, got %d", got)
	}
}

func TestStartDrainPoller_Run_Suite(t *testing.T) {
	t.Parallel()
	older := "2026-01-01T00:00:00Z"
	newer := "2026-06-01T00:00:00Z"
	selfOnly := drainTestPayload(drainTestDeployment(drainTestSelf, older, drainTestFullAlpha))
	superseded := drainTestPayload(
		drainTestDeployment(drainTestSelf, older, drainTestFullAlpha),
		drainTestDeployment(drainTestNext, newer, drainTestFullAlpha),
	)
	tests := []drainPollerRunCase{
		{name: "superseded primary latches drain", bodies: []string{selfOnly, superseded}, interval: 10 * time.Millisecond, timeout: 2 * time.Second},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDrainPollerRunCase(t, tc)
		})
	}
}

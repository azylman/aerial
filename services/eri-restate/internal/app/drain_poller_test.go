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

var drainTestFullSuletta = []string{
	"ThreadSession-suletta",
	"DispatchSession-suletta",
	"VoiceSession-suletta",
	"SessionManager-suletta",
	"TurnRunner-suletta",
	"LLM-suletta",
}

func drainTestServices(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, n := range names {
		quoted = append(quoted, `{"name":`+fmt.Sprintf("%q", n)+`}`)
	}
	return strings.Join(quoted, ",")
}

func drainTestDeployment(id, created string, services []string) string {
	return `{"id":` + fmt.Sprintf("%q", id) + `,"created_at":` + fmt.Sprintf("%q", created) + `,"services":[` + drainTestServices(services) + `]}`
}

func drainTestPayload(deps ...string) string {
	return `{"deployments":[` + strings.Join(deps, ",") + `]}`
}

type decideDrainCase struct {
	name              string
	selfID            string
	omitSelf          bool
	selfCreated       string
	selfServices      []string
	otherID           string
	otherCreated      string
	otherServices     []string
	prior             int
	wantConfirmations int
	wantDrain         bool
}

func runDecideDrainCase(t *testing.T, tc decideDrainCase) {
	t.Helper()
	var deps []deploymentInfo
	if !tc.omitSelf && tc.selfID != "" {
		deps = append(deps, deploymentInfo{ID: tc.selfID, CreatedAt: drainTestTime(t, tc.selfCreated), Services: drainTestSet(tc.selfServices)})
	}
	if tc.otherID != "" {
		deps = append(deps, deploymentInfo{ID: tc.otherID, CreatedAt: drainTestTime(t, tc.otherCreated), Services: drainTestSet(tc.otherServices)})
	}
	gotConfirmations, gotDrain := decideDrain(tc.selfID, deps, tc.prior)
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
	self := "dep-self"
	older := "2026-01-01T00:00:00Z"
	newer := "2026-06-01T00:00:00Z"
	tests := []decideDrainCase{
		{name: "single deployment is healthy primary", selfID: self, selfCreated: older, selfServices: drainTestFullSuletta, prior: 0, wantConfirmations: 0, wantDrain: false},
		{name: "newer superseding deployment first sighting", selfID: self, selfCreated: older, selfServices: drainTestFullSuletta, otherID: "dep-new", otherCreated: newer, otherServices: drainTestFullSuletta, prior: 0, wantConfirmations: 1, wantDrain: false},
		{name: "two consecutive newer polls latch", selfID: self, selfCreated: older, selfServices: drainTestFullSuletta, otherID: "dep-new", otherCreated: newer, otherServices: drainTestFullSuletta, prior: 1, wantConfirmations: 2, wantDrain: true},
		{name: "third consecutive poll stays latched", selfID: self, selfCreated: older, selfServices: drainTestFullSuletta, otherID: "dep-new", otherCreated: newer, otherServices: drainTestFullSuletta, prior: 2, wantConfirmations: 3, wantDrain: true},
		{name: "older deployment never drains", selfID: "dep-new", selfCreated: newer, selfServices: drainTestFullSuletta, otherID: self, otherCreated: older, otherServices: drainTestFullSuletta, prior: 0, wantConfirmations: 0, wantDrain: false},
		{name: "missing self resets", selfID: "dep-ghost", omitSelf: true, otherID: self, otherCreated: older, otherServices: drainTestFullSuletta, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "empty self resets", selfID: "", otherID: self, otherCreated: older, otherServices: drainTestFullSuletta, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "partial newer set resets", selfID: self, selfCreated: older, selfServices: drainTestFullSuletta, otherID: "dep-new", otherCreated: newer, otherServices: drainTestFullSuletta[:5], prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "empty listing resets", selfID: self, omitSelf: true, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "self with partial set is inconclusive", selfID: self, selfCreated: older, selfServices: drainTestFullSuletta[:5], otherID: "dep-new", otherCreated: newer, otherServices: drainTestFullSuletta, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "zero timestamp resets", selfID: self, selfCreated: older, selfServices: drainTestFullSuletta, otherID: "dep-new", otherServices: drainTestFullSuletta, prior: 1, wantConfirmations: 0, wantDrain: false},
		{name: "unrelated deployments skipped", selfID: self, selfCreated: older, selfServices: drainTestFullSuletta, otherID: "dep-other", otherCreated: newer, otherServices: []string{"OtherStack-svc"}, wantConfirmations: 0, wantDrain: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDecideDrainCase(t, tc)
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

	state := newDrainPollState()
	fakes := []*fakeDrainTarget{{}, {}}
	targets := []drainTarget{fakes[0], fakes[1]}
	for range tc.steps {
		state.step(context.Background(), srv.URL, 5*time.Second, targets)
	}
	if state.latched != tc.wantLatched {
		t.Errorf("latched = %v, want %v (confirmations=%d self=%q)", state.latched, tc.wantLatched, state.confirmations, state.selfID)
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
	selfOnly := drainTestPayload(drainTestDeployment("dep-self", older, drainTestFullSuletta))
	both := drainTestPayload(
		drainTestDeployment("dep-self", older, drainTestFullSuletta),
		drainTestDeployment("dep-new", newer, drainTestFullSuletta),
	)
	partial := drainTestPayload(
		drainTestDeployment("dep-self", older, drainTestFullSuletta),
		drainTestDeployment("dep-new", newer, drainTestFullSuletta[:5]),
	)
	tests := []drainStepCase{
		{name: "healthy primary never latches", payloads: []string{selfOnly, selfOnly}, steps: 2, wantLatched: false, wantDrains: 0},
		{name: "latches after two consecutive newer polls", payloads: []string{selfOnly, both, both}, steps: 3, wantLatched: true, wantDrains: 1},
		{name: "single newer sighting does not latch", payloads: []string{selfOnly, both}, steps: 2, wantLatched: false, wantDrains: 0},
		{name: "error resets confirmations", payloads: []string{selfOnly, both, "ERR500", both, both}, steps: 5, wantLatched: true, wantDrains: 1},
		{name: "partial set never latches", payloads: []string{selfOnly, partial, partial, partial}, steps: 4, wantLatched: false, wantDrains: 0},
		{name: "unreachable admin never latches", payloads: []string{"ERR500", "ERR500"}, steps: 2, wantLatched: false, wantDrains: 0},
		{name: "empty listing leaves baseline unresolved", payloads: []string{`{"deployments":[]}`}, steps: 1, wantLatched: false, wantDrains: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runDrainStepCase(t, tc)
		})
	}
}

type parseDrainCase struct {
	name    string
	body    string
	wantIDs string
	wantErr bool
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
	var ids []string
	for _, d := range deps {
		ids = append(ids, d.ID)
	}
	if got := strings.Join(ids, ","); got != tc.wantIDs {
		t.Errorf("deployment ids = %q, want %q", got, tc.wantIDs)
	}
}

func TestParseDrainDeployments_Suite(t *testing.T) {
	t.Parallel()
	older := "2026-01-01T00:00:00Z"
	newer := "2026-06-01T00:00:00Z"
	tests := []parseDrainCase{
		{name: "wrapped shape", body: drainTestPayload(drainTestDeployment("dep-a", older, drainTestFullSuletta), drainTestDeployment("dep-b", newer, drainTestFullSuletta)), wantIDs: "dep-a,dep-b"},
		{name: "bare array shape", body: `[` + drainTestDeployment("dep-a", older, drainTestFullSuletta) + `]`, wantIDs: "dep-a"},
		{name: "empty wrapped list", body: `{"deployments":[]}`, wantIDs: ""},
		{name: "empty bare array", body: `[]`, wantIDs: ""},
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

type drainDepSpec struct {
	id       string
	created  string
	services []string
}

type resolveBaselineCase struct {
	name string
	deps []drainDepSpec
	want string
}

func runResolveBaselineCase(t *testing.T, tc resolveBaselineCase) {
	t.Helper()
	deps := make([]deploymentInfo, 0, len(tc.deps))
	for _, spec := range tc.deps {
		deps = append(deps, deploymentInfo{ID: spec.id, CreatedAt: drainTestTime(t, spec.created), Services: drainTestSet(spec.services)})
	}
	if got := resolveDrainBaseline(deps); got != tc.want {
		t.Errorf("resolveDrainBaseline() = %q, want %q", got, tc.want)
	}
}

func TestResolveDrainBaseline_Suite(t *testing.T) {
	t.Parallel()
	older := "2026-01-01T00:00:00Z"
	newer := "2026-06-01T00:00:00Z"
	tests := []resolveBaselineCase{
		{name: "empty listing has no baseline", want: ""},
		{name: "single full set is baseline", deps: []drainDepSpec{{id: "dep-a", created: older, services: drainTestFullSuletta}}, want: "dep-a"},
		{name: "newest full set wins", deps: []drainDepSpec{{id: "dep-a", created: older, services: drainTestFullSuletta}, {id: "dep-b", created: newer, services: drainTestFullSuletta}}, want: "dep-b"},
		{name: "zero timestamp skipped", deps: []drainDepSpec{{id: "dep-a", created: "", services: drainTestFullSuletta}, {id: "dep-b", created: older, services: drainTestFullSuletta}}, want: "dep-b"},
		{name: "partial set skipped", deps: []drainDepSpec{{id: "dep-a", created: newer, services: drainTestFullSuletta[:5]}, {id: "dep-b", created: older, services: drainTestFullSuletta}}, want: "dep-b"},
		{name: "no full set has no baseline", deps: []drainDepSpec{{id: "dep-a", created: newer, services: drainTestFullSuletta[:5]}}, want: ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runResolveBaselineCase(t, tc)
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
		{name: "ok wrapped listing", status: http.StatusOK, body: drainTestPayload(drainTestDeployment("dep-a", older, drainTestFullSuletta), drainTestDeployment("dep-b", older, drainTestFullSuletta)), wantCount: 2},
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
	empty := newDrainPollState()
	empty.step(context.Background(), "   ", time.Second, nil)
	if empty.selfID != "" || empty.confirmations != 0 || empty.latched {
		t.Errorf("empty admin URL must be a no-op, got %+v", empty)
	}

	latched := newDrainPollState()
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
	startDrainPollerWithInterval(ctx, appCfg, []drainTarget{fake}, tc.interval, tc.timeout)

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
	selfOnly := drainTestPayload(drainTestDeployment("dep-self", older, drainTestFullSuletta))
	superseded := drainTestPayload(
		drainTestDeployment("dep-self", older, drainTestFullSuletta),
		drainTestDeployment("dep-new", newer, drainTestFullSuletta),
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

package router

// Authoritative deploy-decision tests: when restate_authoritative is on, the
// Restate PRObject decision — never the Postgres deployed projection —
// decides whether synapse publishes deploy_success. The Postgres transition
// and deployed-projection reads stay only on the legacy fallback path.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	publishDecisionBody = `{"disposition":"apply","should_publish":true,"deduped":false,"effect_key":"k"}`
	dedupedDecisionBody = `{"disposition":"apply","should_publish":false,"deduped":true,"effect_key":"k"}`
)

// decisionTestServer routes Restate ingress calls by handler: the decision
// endpoint answers with the configured status/body, the publish-recorded ack
// endpoint answers with the ack status. Call counts prove which path ran.
func decisionTestServer(t *testing.T, decisionCalls, ackCalls *atomic.Int64, decisionStatus int, decisionBody string, ackStatus int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/HandleDeployDispatched"):
			decisionCalls.Add(1)
			w.WriteHeader(decisionStatus)
			_, _ = io.WriteString(w, decisionBody)
		case strings.HasSuffix(r.URL.Path, "/HandleDeploySuccessPublished"):
			ackCalls.Add(1)
			w.WriteHeader(ackStatus)
		default:
			w.WriteHeader(http.StatusAccepted)
		}
	}))
}

func countDeploySuccess(bus *mockEventBus) int {
	bus.mu.Lock()
	defer bus.mu.Unlock()
	n := 0
	for _, ev := range bus.publishedPREvents {
		if ev.Event == eventDeploySuccess {
			n++
		}
	}
	return n
}

func TestResolveDeployDispatch(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name              string
		authoritative     bool
		capture           bool
		decisionStatus    int
		decisionBody      string
		prNumber          int
		wantErr           bool
		wantCalls         int64
		wantShouldPublish bool
		wantDeduped       bool
	}{
		{name: "authoritative should-publish decision", authoritative: true, capture: true, decisionStatus: 202, decisionBody: publishDecisionBody, prNumber: 42, wantCalls: 1, wantShouldPublish: true},
		{name: "authoritative deduped decision", authoritative: true, capture: true, decisionStatus: 202, decisionBody: dedupedDecisionBody, prNumber: 42, wantCalls: 1, wantDeduped: true},
		{name: "authoritative ingress failure surfaces", authoritative: true, capture: true, decisionStatus: 500, decisionBody: "boom", prNumber: 42, wantErr: true, wantCalls: 1},
		{name: "authoritative malformed body surfaces", authoritative: true, capture: true, decisionStatus: 202, decisionBody: "not-json{", prNumber: 42, wantErr: true, wantCalls: 1},
		{name: "authoritative without url surfaces misconfiguration", authoritative: true, prNumber: 42, wantErr: true},
		{name: "legacy returns zero without calls", capture: true, decisionStatus: 202, decisionBody: publishDecisionBody, prNumber: 42},
		{name: "non-pr dispatch returns zero without calls", authoritative: true, capture: true, decisionStatus: 202, decisionBody: publishDecisionBody},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var decisionCalls atomic.Int64
			var ackCalls atomic.Int64
			url := ""
			if tc.capture {
				srv := decisionTestServer(t, &decisionCalls, &ackCalls, tc.decisionStatus, tc.decisionBody, 202)
				t.Cleanup(srv.Close)
				url = srv.URL
			}
			srv := NewRouterServer(fixedConfig(Config{RestateAuthoritative: tc.authoritative, RestateURL: url}), nil)
			got, err := srv.resolveDeployDispatch(context.Background(), "azylman/aerial", "brain", "sha123", "sha123", tc.prNumber)
			if (err != nil) != tc.wantErr {
				t.Fatalf("resolveDeployDispatch() error = %v, wantErr %v", err, tc.wantErr)
			}
			if n := decisionCalls.Load(); n != tc.wantCalls {
				t.Fatalf("decision calls = %d, want %d", n, tc.wantCalls)
			}
			if !tc.wantErr && (got.ShouldPublish != tc.wantShouldPublish || got.Deduped != tc.wantDeduped) {
				t.Fatalf("ShouldPublish/Deduped = %v/%v, want %v/%v", got.ShouldPublish, got.Deduped, tc.wantShouldPublish, tc.wantDeduped)
			}
		})
	}
}

func TestPublishUnownedDeployEventAuthoritative(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		authoritative  bool
		deployed       bool
		decisionStatus int
		decisionBody   string
		ackStatus      int
		wantErr        bool
		wantPublishes  int
		wantDecision   int64
		wantAck        int64
	}{
		{name: "authoritative publishes despite deployed projection", authoritative: true, deployed: true, decisionStatus: 202, decisionBody: publishDecisionBody, ackStatus: 202, wantPublishes: 1, wantDecision: 1, wantAck: 1},
		{name: "authoritative deduped decision skips publish", authoritative: true, decisionStatus: 202, decisionBody: dedupedDecisionBody, ackStatus: 202, wantDecision: 1},
		{name: "authoritative decision failure surfaces before publish", authoritative: true, decisionStatus: 500, decisionBody: "boom", ackStatus: 202, wantErr: true, wantDecision: 1},
		{name: "authoritative ack failure surfaces after publish", authoritative: true, decisionStatus: 202, decisionBody: publishDecisionBody, ackStatus: 500, wantErr: true, wantPublishes: 1, wantDecision: 1, wantAck: 1},
		{name: "legacy fallback suppresses on deployed projection", deployed: true, wantPublishes: 0},
		{name: "legacy fallback publishes when not deployed", wantPublishes: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var decisionCalls atomic.Int64
			var ackCalls atomic.Int64
			cfg := lifecycleCfg()
			cfg.RestateAuthoritative = tc.authoritative
			if tc.authoritative {
				srv := decisionTestServer(t, &decisionCalls, &ackCalls, tc.decisionStatus, tc.decisionBody, tc.ackStatus)
				t.Cleanup(srv.Close)
				cfg.RestateURL = srv.URL
			}
			bus := &mockEventBus{}
			reg := &mockPRRegistry{resolveTargetID: lifecycleTargetID, deployedRes: tc.deployed}
			srv := newLifecycleServer(cfg, bus, reg, &mockOutboundDispatcher{})
			evt := DeployEvent{Event: eventDeploySuccess, Repo: "azylman/aerial", PRNumber: 42, JobName: "brain", CommitSHA: "sha123", TargetID: lifecycleTargetID, Timestamp: time.Now().UTC()}
			if _, err := srv.publishUnownedDeployEvent(context.Background(), evt, false); (err != nil) != tc.wantErr {
				t.Fatalf("publishUnownedDeployEvent() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got := len(bus.publishedPREvents); got != tc.wantPublishes {
				t.Fatalf("published lifecycle events = %d, want %d", got, tc.wantPublishes)
			}
			if n := decisionCalls.Load(); n != tc.wantDecision {
				t.Fatalf("decision calls = %d, want %d", n, tc.wantDecision)
			}
			if n := ackCalls.Load(); n != tc.wantAck {
				t.Fatalf("ack calls = %d, want %d", n, tc.wantAck)
			}
		})
	}
}

func TestRecordDeployPublished(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		authoritative bool
		capture       bool
		ackStatus     int
		prNumber      int
		wantErr       bool
		wantAck       int64
	}{
		{name: "legacy ack is a no-op without calls", capture: true, ackStatus: 202, prNumber: 42},
		{name: "non-pr ack is a no-op without calls", authoritative: true, capture: true, ackStatus: 202},
		{name: "authoritative ack records publish", authoritative: true, capture: true, ackStatus: 202, prNumber: 42, wantAck: 1},
		{name: "authoritative ack failure surfaces", authoritative: true, capture: true, ackStatus: 500, prNumber: 42, wantErr: true, wantAck: 1},
		{name: "authoritative ack without url surfaces misconfiguration", authoritative: true, prNumber: 42, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var decisionCalls atomic.Int64
			var ackCalls atomic.Int64
			url := ""
			if tc.capture {
				fake := decisionTestServer(t, &decisionCalls, &ackCalls, 202, publishDecisionBody, tc.ackStatus)
				t.Cleanup(fake.Close)
				url = fake.URL
			}
			srv := NewRouterServer(fixedConfig(Config{RestateAuthoritative: tc.authoritative, RestateURL: url}), nil)
			if err := srv.recordDeployPublished(context.Background(), "azylman/aerial", "brain", "sha123", "sha123", tc.prNumber); (err != nil) != tc.wantErr {
				t.Fatalf("recordDeployPublished() error = %v, wantErr %v", err, tc.wantErr)
			}
			if n := ackCalls.Load(); n != tc.wantAck {
				t.Fatalf("ack calls = %d, want %d", n, tc.wantAck)
			}
		})
	}
}

func TestSyncDeployedProjection(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		nilRegistry   bool
		markErr       bool
		updateErr     bool
		wantErrSub    string
		wantMarkCalls int
	}{
		{name: "nil registry skips sync", nilRegistry: true},
		{name: "sync marks deployed and clears pending", wantMarkCalls: 1},
		{name: "mark failure surfaces", markErr: true, wantErrSub: "mark aerial-core deployed", wantMarkCalls: 1},
		{name: "clear failure surfaces", updateErr: true, wantErrSub: "clear deploy_success_pending", wantMarkCalls: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mock *mockPRRegistry
			var reg PRRegistryUpdater
			if !tc.nilRegistry {
				mock = &mockPRRegistry{resolveTargetID: lifecycleTargetID}
				if tc.markErr {
					mock.markKustomizationsErr = errors.New("db mark fail")
				}
				if tc.updateErr {
					mock.updatePRStatusErr = errors.New("db clear fail")
				}
				reg = mock
			}
			srv := newTestRouter(Config{}, nil, reg, &mockOutboundDispatcher{})
			pr := DeployingPR{Repo: "azylman/aerial", PRNumber: 42, HeadSHA: "sha123", TargetID: lifecycleTargetID}
			err := srv.syncDeployedProjection(context.Background(), pr, "aerial-core")
			assertExpectedErrorSubstring(t, err, tc.wantErrSub)
			if mock != nil {
				if got := len(mock.markKustomizationsCalls); got != tc.wantMarkCalls {
					t.Fatalf("markKustomizationsCalls = %d, want %d", got, tc.wantMarkCalls)
				}
			}
		})
	}
}

func TestCompleteKustomizationRequestAuthoritative(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		deployed       bool
		decisionStatus int
		decisionBody   string
		ackStatus      int
		wantErr        bool
		wantPublishes  int
		wantMarkCalls  int
		wantClear      bool
		wantDecision   int64
		wantAck        int64
	}{
		{name: "authoritative publishes despite deployed projection and syncs projection", deployed: true, decisionStatus: 202, decisionBody: publishDecisionBody, ackStatus: 202, wantPublishes: 1, wantMarkCalls: 1, wantClear: true, wantDecision: 1, wantAck: 1},
		{name: "authoritative deduped skips publish but syncs projection", decisionStatus: 202, decisionBody: dedupedDecisionBody, ackStatus: 202, wantMarkCalls: 1, wantClear: true, wantDecision: 1},
		{name: "authoritative decision failure surfaces before projection sync", decisionStatus: 500, decisionBody: "boom", ackStatus: 202, wantErr: true, wantDecision: 1},
		{name: "authoritative ack failure surfaces after publish", decisionStatus: 202, decisionBody: publishDecisionBody, ackStatus: 500, wantErr: true, wantPublishes: 1, wantDecision: 1, wantAck: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var decisionCalls atomic.Int64
			var ackCalls atomic.Int64
			fake := decisionTestServer(t, &decisionCalls, &ackCalls, tc.decisionStatus, tc.decisionBody, tc.ackStatus)
			t.Cleanup(fake.Close)
			cfg := Config{RestateAuthoritative: true, RestateURL: fake.URL, SystemChannelID: lifecycleTargetID}
			bus := &mockEventBus{}
			reg := &mockPRRegistry{resolveTargetID: lifecycleTargetID, deployedRes: tc.deployed, markKustomizationsRes: true}
			disp := &mockOutboundDispatcher{}
			srv := newTestRouter(cfg, nil, reg, disp, bus)
			pr := DeployingPR{Repo: "azylman/aerial", PRNumber: 42, HeadSHA: "sha123", MergeSHA: "sha-merge", TargetID: lifecycleTargetID}
			if err := srv.completeKustomizationRequest(context.Background(), pr, "aerial-core"); (err != nil) != tc.wantErr {
				t.Fatalf("completeKustomizationRequest() error = %v, wantErr %v", err, tc.wantErr)
			}
			if got := countDeploySuccess(bus); got != tc.wantPublishes {
				t.Fatalf("deploy_success publishes = %d, want %d", got, tc.wantPublishes)
			}
			if got := len(reg.markKustomizationsCalls); got != tc.wantMarkCalls {
				t.Fatalf("markKustomizationsCalls = %d, want %d", got, tc.wantMarkCalls)
			}
			if tc.wantClear {
				assertKustomizationPendingCleared(t, reg.updatePRStatusCalls)
			}
			if n := decisionCalls.Load(); n != tc.wantDecision {
				t.Fatalf("decision calls = %d, want %d", n, tc.wantDecision)
			}
			if n := ackCalls.Load(); n != tc.wantAck {
				t.Fatalf("ack calls = %d, want %d", n, tc.wantAck)
			}
		})
	}
}

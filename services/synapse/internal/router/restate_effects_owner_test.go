package router

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

// policyJobMsg records the ack decision handleJetStreamJob makes.
type policyJobMsg struct {
	numDelivered uint64
	termed       bool
	nakDelay     time.Duration
	naked        bool
}

func (*policyJobMsg) Data() []byte           { return []byte(`{}`) }
func (*policyJobMsg) Subject() string        { return "aerial.jobs.synapse.github" }
func (*policyJobMsg) Header() nats.Header    { return nil }
func (m *policyJobMsg) NumDelivered() uint64 { return m.numDelivered }
func (*policyJobMsg) Ack() error             { return nil }
func (m *policyJobMsg) NakWithDelay(d time.Duration) error {
	m.naked = true
	m.nakDelay = d
	return nil
}
func (m *policyJobMsg) Term() error              { m.termed = true; return nil }
func (*policyJobMsg) Snooze(time.Duration) error { return nil }
func (*policyJobMsg) InProgress() error          { return nil }

func TestRestateOwnedBackoffDelay(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		numDelivered uint64
		want         time.Duration
	}{
		{name: "first delivery", numDelivered: 1, want: time.Second},
		{name: "zero delivery", numDelivered: 0, want: time.Second},
		{name: "fourth delivery doubles", numDelivered: 4, want: 8 * time.Second},
		{name: "sixth delivery", numDelivered: 6, want: 32 * time.Second},
		{name: "seventh delivery caps", numDelivered: 7, want: time.Minute},
		{name: "late delivery stays capped", numDelivered: 44, want: time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := restateOwnedBackoffDelay(tc.numDelivered); got != tc.want {
				t.Errorf("restateOwnedBackoffDelay(%d) = %v, want %v", tc.numDelivered, got, tc.want)
			}
		})
	}
}

func TestGitHubJobFinalAttempt(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		owner        string
		numDelivered uint64
		want         bool
	}{
		{name: "unstamped legacy before budget", owner: "", numDelivered: 4, want: false},
		{name: "unstamped legacy at budget", owner: "", numDelivered: 5, want: true},
		{name: "legacy past budget", owner: "legacy", numDelivered: 6, want: true},
		{name: "restate at legacy budget never final", owner: "restate", numDelivered: 5, want: false},
		{name: "restate late delivery never final", owner: "restate", numDelivered: 44, want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := githubJobFinalAttempt(tc.owner, tc.numDelivered); got != tc.want {
				t.Errorf("githubJobFinalAttempt(%q, %d) = %v, want %v", tc.owner, tc.numDelivered, got, tc.want)
			}
		})
	}
}

func TestHandleJetStreamJobRestatePolicy(t *testing.T) {
	t.Parallel()
	retryable := &restateOwnedSendError{handler: "HandleMerged", err: errors.New("status 503")}
	terminal := &restateOwnedSendError{handler: "HandleMerged", terminal: true, err: errors.New("status 404")}
	tests := []struct {
		name         string
		numDelivered uint64
		processErr   error
		wantErr      bool
		wantTerm     bool
		wantNak      bool
		wantNakDelay time.Duration
	}{
		{name: "success acks", numDelivered: 1, processErr: nil},
		{name: "legacy error naks on short backoff", numDelivered: 2, processErr: errors.New("db down"), wantErr: true, wantNak: true, wantNakDelay: 2 * time.Second},
		{name: "legacy error terms at legacy budget", numDelivered: 5, processErr: errors.New("db down"), wantTerm: true},
		{name: "restate retryable outlives legacy budget", numDelivered: 5, processErr: retryable, wantErr: true, wantNak: true, wantNakDelay: 16 * time.Second},
		{name: "restate retryable caps backoff", numDelivered: 20, processErr: retryable, wantErr: true, wantNak: true, wantNakDelay: time.Minute},
		{name: "restate retryable terms at restate budget", numDelivered: 45, processErr: retryable, wantTerm: true},
		{name: "wrapped restate terminal drops job", numDelivered: 1, processErr: errors.Join(errors.New("other"), terminal), wantTerm: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			msg := &policyJobMsg{numDelivered: tc.numDelivered}
			err := handleJetStreamJob(context.Background(), msg, maxDeliverGitHubWebhook, "github", func(struct{}) error { return tc.processErr })
			if (err != nil) != tc.wantErr {
				t.Fatalf("handleJetStreamJob() err = %v, wantErr %v", err, tc.wantErr)
			}
			if msg.termed != tc.wantTerm {
				t.Errorf("termed = %v, want %v", msg.termed, tc.wantTerm)
			}
			if msg.naked != tc.wantNak || msg.nakDelay != tc.wantNakDelay {
				t.Errorf("nak = %v/%v, want %v/%v", msg.naked, msg.nakDelay, tc.wantNak, tc.wantNakDelay)
			}
		})
	}
}

func TestStampEffectsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		preStamp   string
		preStamped bool
		want       string
	}{
		{name: "unstamped context snapshots legacy", want: "legacy"},
		{name: "earlier restate stamp is kept", preStamp: "restate", preStamped: true, want: "restate"},
		{name: "earlier legacy stamp is kept", preStamp: "legacy", preStamped: true, want: "legacy"},
		{name: "earlier garbage stamp normalizes to legacy", preStamp: "both", preStamped: true, want: "legacy"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			if tc.preStamped {
				ctx = withEffectsOwner(ctx, tc.preStamp)
			}
			if got := effectsOwnerFromContext(stampEffectsOwner(ctx)); got != tc.want {
				t.Errorf("effectsOwnerFromContext() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestEventIdentity(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		source string
		id     string
	}{
		{name: "empty id has no identity", source: "github", id: ""},
		{name: "blank whitespace id has no identity", source: "github", id: "   "},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := eventIdentity(tc.source, tc.id); got != "" {
				t.Errorf("eventIdentity(%q, %q) = %q, want empty", tc.source, tc.id, got)
			}
		})
	}
}

type capturedIngress struct {
	mu     sync.Mutex
	paths  []string
	bodies []string
	keys   []string
}

func capturingIngress(t *testing.T, status int, got *capturedIngress) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got.mu.Lock()
		got.paths = append(got.paths, r.URL.EscapedPath())
		got.bodies = append(got.bodies, string(body))
		got.keys = append(got.keys, r.Header.Get("Idempotency-Key"))
		got.mu.Unlock()
		w.WriteHeader(status)
	}))
}

func TestRouteEventEffectsOwner(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		authoritative bool
		noURL         bool
		status        int
		owner         string
		identity      string
		wantErr       bool
		wantRestate   bool
		wantTerminal  bool
		wantPath      string
		wantBodySub   string
		wantKeyPrefix string
	}{
		{name: "shadow send is one-way and stamped legacy", status: 202, identity: "github:d1", wantPath: "/PRObject/azylman%2Faerial%237/HandleMerged/send", wantBodySub: `"effects_owner":"legacy"`, wantKeyPrefix: "HandleMerged:github:d1:"},
		{name: "shadow failure is counted not returned", status: 503, owner: "legacy", wantPath: "/PRObject/azylman%2Faerial%237/HandleMerged/send", wantBodySub: `"effects_owner":"legacy"`, wantKeyPrefix: "HandleMerged:"},
		{name: "authoritative legacy failure keeps plain error", authoritative: true, status: 503, owner: "legacy", wantErr: true, wantPath: "/PRObject/azylman%2Faerial%237/HandleMerged/send", wantBodySub: `"effects_owner":"legacy"`, wantKeyPrefix: "HandleMerged:"},
		{name: "authoritative restate success carries stamp", authoritative: true, status: 202, owner: "restate", identity: "deploy:k1", wantPath: "/PRObject/azylman%2Faerial%237/HandleMerged/send", wantBodySub: `"effects_owner":"restate"`, wantKeyPrefix: "HandleMerged:deploy:k1:"},
		{name: "authoritative restate 5xx is retryable", authoritative: true, status: 503, owner: "restate", wantErr: true, wantRestate: true, wantPath: "/PRObject/azylman%2Faerial%237/HandleMerged/send", wantBodySub: `"effects_owner":"restate"`, wantKeyPrefix: "HandleMerged:"},
		{name: "authoritative restate 4xx is terminal", authoritative: true, status: 404, owner: "restate", wantErr: true, wantRestate: true, wantTerminal: true, wantPath: "/PRObject/azylman%2Faerial%237/HandleMerged/send", wantBodySub: `"effects_owner":"restate"`, wantKeyPrefix: "HandleMerged:"},
		{name: "authoritative restate without url is retryable", authoritative: true, noURL: true, owner: "restate", wantErr: true, wantRestate: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var got capturedIngress
			url := ""
			if !tc.noURL {
				ingress := capturingIngress(t, tc.status, &got)
				t.Cleanup(ingress.Close)
				url = ingress.URL
			}
			srv := NewRouterServer(fixedConfig(Config{RestateAuthoritative: tc.authoritative, RestateURL: url}), nil)
			ctx := context.Background()
			if tc.owner != "" {
				ctx = withEffectsOwner(ctx, tc.owner)
			}
			ctx = restateclient.WithEventIdentity(ctx, tc.identity)
			err := srv.routePREvent(ctx, "HandleMerged", restateclient.PREvent{Repo: "azylman/aerial", PRNumber: 7, MergeSHA: shadowTestHeadSHA(), Agent: "aerial"})
			if (err != nil) != tc.wantErr {
				t.Fatalf("routePREvent() err = %v, wantErr %v", err, tc.wantErr)
			}
			restateOwned, terminal := restateOwnedSendFailure(err)
			if restateOwned != tc.wantRestate || terminal != tc.wantTerminal {
				t.Errorf("restateOwnedSendFailure() = %v/%v, want %v/%v", restateOwned, terminal, tc.wantRestate, tc.wantTerminal)
			}
			if tc.wantPath == "" {
				return
			}
			got.mu.Lock()
			defer got.mu.Unlock()
			if len(got.paths) != 1 {
				t.Fatalf("ingress calls = %d, want 1", len(got.paths))
			}
			if got.paths[0] != tc.wantPath {
				t.Errorf("path = %q, want %q", got.paths[0], tc.wantPath)
			}
			if !strings.Contains(got.bodies[0], tc.wantBodySub) {
				t.Errorf("body = %q, want substring %q", got.bodies[0], tc.wantBodySub)
			}
			if !strings.HasPrefix(got.keys[0], tc.wantKeyPrefix) || len(got.keys[0]) != len(tc.wantKeyPrefix)+16 {
				t.Errorf("idempotency key = %q, want prefix %q plus 16 hex digest", got.keys[0], tc.wantKeyPrefix)
			}
		})
	}
}

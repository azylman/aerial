package restateclient

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/restateingress"
	"github.com/azylman/aerial/pkg/restateingresstest"
)

// testOwner is the GitHub owner the client qualifies bare repos with.
const testOwner = "acme"

func checkSendResult(t *testing.T, err error, recs []restateingresstest.Call, wantErr, wantPath string, wantBodySubs []string, wantRequests int, wantIdemPrefix string) {
	t.Helper()
	if wantErr == "" && err != nil {
		t.Fatalf("send() = %v, want nil", err)
	}
	if wantErr != "" && (err == nil || !strings.Contains(err.Error(), wantErr)) {
		t.Fatalf("send() = %v, want error containing %q", err, wantErr)
	}
	if len(recs) != wantRequests {
		t.Fatalf("requests = %d, want %d", len(recs), wantRequests)
	}
	if wantPath == "" {
		return
	}
	if recs[0].Method != http.MethodPost {
		t.Errorf("method = %q, want POST", recs[0].Method)
	}
	if recs[0].Path != wantPath {
		t.Errorf("path = %q, want %q", recs[0].Path, wantPath)
	}
	// Every one-way send carries a key: prefix plus a 16-hex payload digest.
	// The digest value itself is pinned by TestSendIdempotencyKey.
	if !strings.HasPrefix(recs[0].IdempotencyKey, wantIdemPrefix) || len(recs[0].IdempotencyKey) != len(wantIdemPrefix)+16 {
		t.Errorf("idempotencyKey = %q, want prefix %q plus 16 hex digest", recs[0].IdempotencyKey, wantIdemPrefix)
	}
	for _, sub := range wantBodySubs {
		if !strings.Contains(string(recs[0].Body), sub) {
			t.Errorf("body %q missing %q", recs[0].Body, sub)
		}
	}
}

// resolveBaseURL maps a row's baseURL to the client endpoint: "" is the
// fake ingress, "unconfigured" is no endpoint, "refused" is a closed
// server, anything else is used verbatim.
func resolveBaseURL(t *testing.T, srv *restateingresstest.Server, override string) string {
	t.Helper()
	switch override {
	case "":
		return srv.URL()
	case "unconfigured":
		return ""
	case "refused":
		closed := httptest.NewServer(http.NotFoundHandler())
		closed.Close()
		return closed.URL
	}
	return override
}

func TestSendPREvent(t *testing.T) {
	t.Parallel()
	ev := prwire.PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123"}
	const failedPath = "/PRObject/acme/aerial#101/HandleCheckFailed/send"
	tests := []struct {
		name           string
		baseURL        string
		owner          string
		handler        string
		event          prwire.PREvent
		serverStatus   int
		identity       string
		wantErr        string
		wantPath       string
		wantBodySubs   []string
		wantRequests   int
		wantIdemPrefix string
	}{
		{name: "unmanaged check failed with build id routes to PRObject", owner: testOwner, handler: "HandleCheckFailed", event: prwire.PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "lint", BuildID: "build-1", Unmanaged: true}, wantPath: failedPath, wantBodySubs: []string{`"head_sha":"abc123"`, `"build_id":"build-1"`, `"unmanaged":true`}, wantRequests: 1, wantIdemPrefix: "HandleCheckFailed:"},
		{name: "idempotency key propagated when set", owner: testOwner, handler: "HandleCheckFailed", event: ev, identity: "gh:delivery-1", wantPath: failedPath, wantBodySubs: []string{`"head_sha":"abc123"`}, wantRequests: 1, wantIdemPrefix: "HandleCheckFailed:gh:delivery-1:"},
		{name: "bare repo normalizes with owner prefix", owner: testOwner, handler: "HandleCheckPassed", event: prwire.PREvent{Event: "check_passed", Repo: "mirrormere", PRNumber: 7, HeadSHA: "def456"}, serverStatus: 202, wantPath: "/PRObject/acme/mirrormere#7/HandleCheckPassed/send", wantBodySubs: []string{`"repo":"acme/mirrormere"`}, wantRequests: 1, wantIdemPrefix: "HandleCheckPassed:"},
		{name: "owned merged carries merge sha", owner: testOwner, handler: "HandleMerged", event: prwire.PREvent{Event: "merged", Repo: "acme/aerial", PRNumber: 101, MergeSHA: "merge999", BuildID: "build-9"}, wantPath: "/PRObject/acme/aerial#101/HandleMerged/send", wantBodySubs: []string{`"merge_sha":"merge999"`}, wantRequests: 1, wantIdemPrefix: "HandleMerged:"},
		{name: "agent never changes the PRObject target", owner: testOwner, handler: "HandleCheckPassed", event: prwire.PREvent{Repo: "aerial", PRNumber: 1, Agent: "agent-alpha"}, wantPath: "/PRObject/acme/aerial#1/HandleCheckPassed/send", wantBodySubs: []string{`"agent":"agent-alpha"`}, wantRequests: 1, wantIdemPrefix: "HandleCheckPassed:"},
		{name: "qualified repo keeps its owner", owner: testOwner, handler: "HandleCheckPassed", event: prwire.PREvent{Event: "check_passed", Repo: "x/y", PRNumber: 3}, wantPath: "/PRObject/x/y#3/HandleCheckPassed/send", wantBodySubs: []string{`"repo":"x/y"`}, wantRequests: 1, wantIdemPrefix: "HandleCheckPassed:"},
		{name: "bare repo without owner sends nothing", handler: "HandleCheckPassed", event: prwire.PREvent{Event: "check_passed", Repo: "aerial", PRNumber: 3}, wantErr: "github_owner is unset"},
		{name: "qualified repo needs no owner", handler: "HandleCheckPassed", event: prwire.PREvent{Event: "check_passed", Repo: "acme/aerial", PRNumber: 3}, wantPath: "/PRObject/acme/aerial#3/HandleCheckPassed/send", wantRequests: 1, wantIdemPrefix: "HandleCheckPassed:"},
		{name: "unknown handler sends nothing", owner: testOwner, handler: "RoutePREvent", event: prwire.PREvent{Event: "merged", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123"}, wantErr: "unknown pr handler"},
		{name: "empty repo sends nothing", owner: testOwner, handler: "HandleClosed", event: prwire.PREvent{Event: "closed", PRNumber: 101, HeadSHA: "abc123"}, wantErr: "repo is required"},
		{name: "bad pr number sends nothing", owner: testOwner, handler: "HandleClosed", event: prwire.PREvent{Event: "closed", Repo: "aerial", HeadSHA: "abc123"}, wantErr: "pr_number is required"},
		{name: "unconfigured endpoint sends nothing", owner: testOwner, baseURL: "unconfigured", handler: "HandleCheckFailed", event: ev, wantErr: "restate is not configured"},
		{name: "refused endpoint surfaces error", owner: testOwner, baseURL: "refused", handler: "HandleCheckFailed", event: ev, wantErr: "restate ingress request failed"},
		{name: "malformed endpoint surfaces error", owner: testOwner, baseURL: "http://restate bad", handler: "HandleCheckFailed", event: ev, wantErr: "restate ingress request failed"},
		{name: "server error surfaces status", owner: testOwner, handler: "HandleCheckFailed", event: ev, serverStatus: 500, wantErr: "returned HTTP 500", wantPath: failedPath, wantRequests: 1, wantIdemPrefix: "HandleCheckFailed:"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := restateingresstest.New(t)
			srv.Routes = map[string]restateingresstest.Response{tc.wantPath: {Status: tc.serverStatus}}
			c := NewClient(resolveBaseURL(t, srv, tc.baseURL), tc.owner, "", "", nil)
			ctx := t.Context()
			if tc.identity != "" {
				ctx = WithEventIdentity(ctx, tc.identity)
			}
			err := c.SendPREvent(ctx, tc.handler, tc.event)
			checkSendResult(t, err, srv.Calls(), tc.wantErr, tc.wantPath, tc.wantBodySubs, tc.wantRequests, tc.wantIdemPrefix)
		})
	}
}

func TestSendRepoEvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		baseURL        string
		handler        string
		event          prwire.RepoEvent
		identity       string
		wantErr        string
		wantPath       string
		wantBodySubs   []string
		wantRequests   int
		wantIdemPrefix string
	}{
		{name: "main push routes to RepoObject", handler: "HandleMainPush", event: prwire.RepoEvent{Repo: "aerial", MainSHA: "abc123"}, wantPath: "/RepoObject/acme/aerial/HandleMainPush/send", wantBodySubs: []string{`"main_sha":"abc123"`}, wantRequests: 1, wantIdemPrefix: "HandleMainPush:"},
		{name: "idempotency key propagated when set", handler: "HandleMainPush", event: prwire.RepoEvent{Repo: "aerial", MainSHA: "abc123"}, identity: "gh:delivery-2", wantPath: "/RepoObject/acme/aerial/HandleMainPush/send", wantBodySubs: []string{`"main_sha":"abc123"`}, wantRequests: 1, wantIdemPrefix: "HandleMainPush:gh:delivery-2:"},
		{name: "unconfigured endpoint sends nothing", baseURL: "unconfigured", handler: "HandleMainPush", event: prwire.RepoEvent{Repo: "aerial", MainSHA: "abc123"}, wantErr: "restate is not configured"},
		{name: "fanout carries pr and head sha", handler: "HandleFanout", event: prwire.RepoEvent{Repo: "acme/mirrormere", PRNumber: 7, HeadSHA: "def456"}, wantPath: "/RepoObject/acme/mirrormere/HandleFanout/send", wantBodySubs: []string{`"pr_number":7`, `"head_sha":"def456"`}, wantRequests: 1, wantIdemPrefix: "HandleFanout:"},
		{name: "kustomization status routes to RepoObject", handler: "HandleKustomizationStatus", event: prwire.RepoEvent{Repo: "aerial", Generation: 3, PRNumber: 101, HeadSHA: "abc123"}, wantPath: "/RepoObject/acme/aerial/HandleKustomizationStatus/send", wantBodySubs: []string{`"generation":3`}, wantRequests: 1, wantIdemPrefix: "HandleKustomizationStatus:"},
		{name: "unknown handler sends nothing", handler: "RoutePREvent", event: prwire.RepoEvent{Repo: "aerial", MainSHA: "abc123"}, wantErr: "unknown repo handler"},
		{name: "pr handler rejected for repo event", handler: "HandleCheckFailed", event: prwire.RepoEvent{Repo: "aerial", MainSHA: "abc123"}, wantErr: "unknown repo handler"},
		{name: "empty repo sends nothing", handler: "HandleMainPush", event: prwire.RepoEvent{MainSHA: "abc123"}, wantErr: "repo is required"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := restateingresstest.New(t)
			c := NewClient(resolveBaseURL(t, srv, tc.baseURL), testOwner, "", "", nil)
			ctx := t.Context()
			if tc.identity != "" {
				ctx = WithEventIdentity(ctx, tc.identity)
			}
			err := c.SendRepoEvent(ctx, tc.handler, tc.event)
			checkSendResult(t, err, srv.Calls(), tc.wantErr, tc.wantPath, tc.wantBodySubs, tc.wantRequests, tc.wantIdemPrefix)
		})
	}
}

func TestSendIdempotencyKey(t *testing.T) {
	t.Parallel()
	// Digests are the first 8 bytes of sha256 over the payload, derived
	// independently with sha256sum.
	tests := []struct {
		name     string
		identity string
		handler  string
		payload  string
		want     string
	}{
		{name: "identity scopes handler and payload digest", identity: "gh:delivery-1", handler: "HandleMerged", payload: `{"repo":"acme/aerial"}`, want: "HandleMerged:gh:delivery-1:2f800cb1c1b440ac"},
		{name: "blank identity falls back to payload digest", identity: "  ", handler: "HandleMainPush", payload: `{"repo":"acme/aerial"}`, want: "HandleMainPush:2f800cb1c1b440ac"},
		{name: "empty payload still digests", identity: "deploy:k1", handler: "HandleRolloutProgress", payload: "", want: "HandleRolloutProgress:deploy:k1:e3b0c44298fc1c14"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := SendIdempotencyKey(tc.identity, tc.handler, []byte(tc.payload)); got != tc.want {
				t.Errorf("SendIdempotencyKey() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestIsTerminal(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil is not terminal", err: nil, want: false},
		{name: "invalid event is terminal", err: fmt.Errorf("%w: repo is required", ErrInvalidEvent), want: true},
		{name: "bad request is terminal", err: &restateingress.HTTPError{Status: 400}, want: true},
		{name: "wrapped not found is terminal", err: fmt.Errorf("send: %w", &restateingress.HTTPError{Status: 404, Body: "no service"}), want: true},
		{name: "request timeout retries", err: &restateingress.HTTPError{Status: 408}, want: false},
		{name: "too many requests retries", err: &restateingress.HTTPError{Status: 429}, want: false},
		{name: "server error retries", err: &restateingress.HTTPError{Status: 503}, want: false},
		{name: "transport error retries", err: errors.New("restate ingress request failed: dial tcp: refused"), want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsTerminal(tc.err); got != tc.want {
				t.Errorf("IsTerminal(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

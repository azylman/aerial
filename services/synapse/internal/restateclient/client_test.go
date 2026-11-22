package restateclient

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/restateingress"
)

type recordedRequest struct {
	method         string
	path           string
	body           string
	idempotencyKey string
}

func newRecordedServer(t *testing.T, status int, truncate bool, recs *[]recordedRequest) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		*recs = append(*recs, recordedRequest{
			method:         r.Method,
			path:           r.URL.EscapedPath(),
			body:           string(body),
			idempotencyKey: r.Header.Get("Idempotency-Key"),
		})
		if truncate {
			w.Header().Set("Content-Length", "100")
			w.WriteHeader(status)
			_, _ = w.Write([]byte("short"))
			return
		}
		w.WriteHeader(status)
	}))
}

func TestClient(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		baseURL       string
		prService     string
		repoService   string
		wantBaseURL   string
		wantPRService string
		wantRepoSvc   string
	}{
		{name: "trims trailing slash and keeps custom services", baseURL: "http://restate.aerial:8080/", prService: "PRObject", repoService: "RepoObject", wantBaseURL: "http://restate.aerial:8080", wantPRService: "PRObject", wantRepoSvc: "RepoObject"},
		{name: "defaults empty service names to frozen objects", baseURL: "http://restate.aerial:8080", prService: "", repoService: "", wantBaseURL: "http://restate.aerial:8080", wantPRService: "PRObject", wantRepoSvc: "RepoObject"},
		{name: "empty base url stays disabled", baseURL: "", prService: "", repoService: "", wantBaseURL: "", wantPRService: "PRObject", wantRepoSvc: "RepoObject"},
		{name: "custom service names preserved", baseURL: "http://restate.aerial:8080", prService: "CustomPR", repoService: "CustomRepo", wantBaseURL: "http://restate.aerial:8080", wantPRService: "CustomPR", wantRepoSvc: "CustomRepo"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := NewClient(tc.baseURL, tc.prService, tc.repoService, nil)
			if c.baseURL != tc.wantBaseURL {
				t.Errorf("baseURL = %q, want %q", c.baseURL, tc.wantBaseURL)
			}
			if c.prService != tc.wantPRService {
				t.Errorf("prService = %q, want %q", c.prService, tc.wantPRService)
			}
			if c.repoService != tc.wantRepoSvc {
				t.Errorf("repoService = %q, want %q", c.repoService, tc.wantRepoSvc)
			}
			if c.httpClient == nil {
				t.Errorf("httpClient is nil, want default client")
			}
		})
	}
}

func checkSendResult(t *testing.T, err error, recs []recordedRequest, wantErr, wantPath string, wantBodySubs []string, wantRequests int, wantIdemPrefix string) {
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
	if recs[0].method != http.MethodPost {
		t.Errorf("method = %q, want POST", recs[0].method)
	}
	if recs[0].path != wantPath {
		t.Errorf("path = %q, want %q", recs[0].path, wantPath)
	}
	// Every one-way send carries a key: prefix plus a 16-hex payload digest.
	// The digest value itself is pinned by TestSendIdempotencyKey.
	if !strings.HasPrefix(recs[0].idempotencyKey, wantIdemPrefix) || len(recs[0].idempotencyKey) != len(wantIdemPrefix)+16 {
		t.Errorf("idempotencyKey = %q, want prefix %q plus 16 hex digest", recs[0].idempotencyKey, wantIdemPrefix)
	}
	for _, sub := range wantBodySubs {
		if !strings.Contains(recs[0].body, sub) {
			t.Errorf("body %q missing %q", recs[0].body, sub)
		}
	}
}

func resolveBaseURL(srv *httptest.Server, override string) string {
	if override == "unconfigured" {
		return ""
	}
	if override != "" {
		return override
	}
	return srv.URL
}

func TestSendPREvent(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		baseURL        string
		handler        string
		event          prwire.PREvent
		serverStatus   int
		truncate       bool
		identity       string
		wantErr        string
		wantPath       string
		wantBodySubs   []string
		wantRequests   int
		wantIdemPrefix string
	}{
		{name: "owned check failed routes to PRObject", handler: "HandleCheckFailed", event: prwire.PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "lint", BuildID: "build-1"}, serverStatus: 200, wantPath: "/PRObject/azylman%2Faerial%23101/HandleCheckFailed/send", wantBodySubs: []string{`"head_sha":"abc123"`, `"build_id":"build-1"`}, wantRequests: 1, wantIdemPrefix: "HandleCheckFailed:"},
		{name: "idempotency key propagated when set", handler: "HandleCheckFailed", event: prwire.PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123", CheckName: "lint"}, identity: "gh:delivery-1", serverStatus: 200, wantPath: "/PRObject/azylman%2Faerial%23101/HandleCheckFailed/send", wantBodySubs: []string{`"head_sha":"abc123"`}, wantRequests: 1, wantIdemPrefix: "HandleCheckFailed:gh:delivery-1:"},
		{name: "bare repo normalizes with owner prefix", handler: "HandleCheckPassed", event: prwire.PREvent{Event: "check_passed", Repo: "mirrormere", PRNumber: 7, HeadSHA: "def456"}, serverStatus: 202, wantPath: "/PRObject/azylman%2Fmirrormere%237/HandleCheckPassed/send", wantBodySubs: []string{`"repo":"azylman/mirrormere"`}, wantRequests: 1, wantIdemPrefix: "HandleCheckPassed:"},
		{name: "owned merged carries merge sha", handler: "HandleMerged", event: prwire.PREvent{Event: "merged", Repo: "azylman/aerial", PRNumber: 101, MergeSHA: "merge999", BuildID: "build-9"}, serverStatus: 200, wantPath: "/PRObject/azylman%2Faerial%23101/HandleMerged/send", wantBodySubs: []string{`"merge_sha":"merge999"`}, wantRequests: 1, wantIdemPrefix: "HandleMerged:"},
		{name: "unknown handler sends nothing", handler: "RoutePREvent", event: prwire.PREvent{Event: "merged", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123"}, serverStatus: 200, wantErr: "unknown pr handler", wantRequests: 0},
		{name: "empty repo sends nothing", handler: "HandleClosed", event: prwire.PREvent{Event: "closed", Repo: "", PRNumber: 101, HeadSHA: "abc123"}, serverStatus: 200, wantErr: "repo is required", wantRequests: 0},
		{name: "bad pr number sends nothing", handler: "HandleClosed", event: prwire.PREvent{Event: "closed", Repo: "aerial", PRNumber: 0, HeadSHA: "abc123"}, serverStatus: 200, wantErr: "pr_number is required", wantRequests: 0},
		{name: "unconfigured endpoint sends nothing", baseURL: "unconfigured", handler: "HandleCheckFailed", event: prwire.PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123"}, serverStatus: 200, wantErr: "restate is not configured", wantRequests: 0},
		{name: "refused endpoint surfaces error", baseURL: "http://127.0.0.1:1", handler: "HandleCheckFailed", event: prwire.PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123"}, serverStatus: 200, wantErr: "restate ingress request failed", wantRequests: 0},
		{name: "malformed endpoint surfaces error", baseURL: "http://restate bad", handler: "HandleCheckFailed", event: prwire.PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123"}, serverStatus: 200, wantErr: "restate ingress request failed", wantRequests: 0},
		{name: "server error surfaces status", handler: "HandleCheckFailed", event: prwire.PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123"}, serverStatus: 500, wantErr: "returned HTTP 500", wantRequests: 1},
		{name: "truncated success body surfaces read error", handler: "HandleCheckFailed", event: prwire.PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123"}, serverStatus: 200, truncate: true, wantErr: "restate ingress request failed", wantRequests: 1},
		{name: "unreadable error body surfaces read error", handler: "HandleCheckFailed", event: prwire.PREvent{Event: "check_failed", Repo: "aerial", PRNumber: 101, HeadSHA: "abc123"}, serverStatus: 500, truncate: true, wantErr: "restate ingress request failed", wantRequests: 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var recs []recordedRequest
			srv := newRecordedServer(t, tc.serverStatus, tc.truncate, &recs)
			defer srv.Close()
			c := NewClient(resolveBaseURL(srv, tc.baseURL), "", "", nil)
			ctx := t.Context()
			if tc.identity != "" {
				ctx = WithEventIdentity(ctx, tc.identity)
			}
			err := c.SendPREvent(ctx, tc.handler, tc.event)
			checkSendResult(t, err, recs, tc.wantErr, tc.wantPath, tc.wantBodySubs, tc.wantRequests, tc.wantIdemPrefix)
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
		serverStatus   int
		identity       string
		wantErr        string
		wantPath       string
		wantBodySubs   []string
		wantRequests   int
		wantIdemPrefix string
	}{
		{name: "main push routes to RepoObject", handler: "HandleMainPush", event: prwire.RepoEvent{Repo: "aerial", MainSHA: "abc123"}, serverStatus: 200, wantPath: "/RepoObject/azylman%2Faerial/HandleMainPush/send", wantBodySubs: []string{`"main_sha":"abc123"`}, wantRequests: 1, wantIdemPrefix: "HandleMainPush:"},
		{name: "idempotency key propagated when set", handler: "HandleMainPush", event: prwire.RepoEvent{Repo: "aerial", MainSHA: "abc123"}, identity: "gh:delivery-2", serverStatus: 200, wantPath: "/RepoObject/azylman%2Faerial/HandleMainPush/send", wantBodySubs: []string{`"main_sha":"abc123"`}, wantRequests: 1, wantIdemPrefix: "HandleMainPush:gh:delivery-2:"},
		{name: "unconfigured endpoint sends nothing", baseURL: "unconfigured", handler: "HandleMainPush", event: prwire.RepoEvent{Repo: "aerial", MainSHA: "abc123"}, serverStatus: 200, wantErr: "restate is not configured", wantRequests: 0},
		{name: "fanout carries pr and head sha", handler: "HandleFanout", event: prwire.RepoEvent{Repo: "azylman/mirrormere", PRNumber: 7, HeadSHA: "def456"}, serverStatus: 202, wantPath: "/RepoObject/azylman%2Fmirrormere/HandleFanout/send", wantBodySubs: []string{`"pr_number":7`, `"head_sha":"def456"`}, wantRequests: 1, wantIdemPrefix: "HandleFanout:"},
		{name: "kustomization status routes to RepoObject", handler: "HandleKustomizationStatus", event: prwire.RepoEvent{Repo: "aerial", Generation: 3, PRNumber: 101, HeadSHA: "abc123"}, serverStatus: 200, wantPath: "/RepoObject/azylman%2Faerial/HandleKustomizationStatus/send", wantBodySubs: []string{`"generation":3`}, wantRequests: 1, wantIdemPrefix: "HandleKustomizationStatus:"},
		{name: "unknown handler sends nothing", handler: "RoutePREvent", event: prwire.RepoEvent{Repo: "aerial", MainSHA: "abc123"}, serverStatus: 200, wantErr: "unknown repo handler", wantRequests: 0},
		{name: "pr handler rejected for repo event", handler: "HandleCheckFailed", event: prwire.RepoEvent{Repo: "aerial", MainSHA: "abc123"}, serverStatus: 200, wantErr: "unknown repo handler", wantRequests: 0},
		{name: "empty repo sends nothing", handler: "HandleMainPush", event: prwire.RepoEvent{Repo: "", MainSHA: "abc123"}, serverStatus: 200, wantErr: "repo is required", wantRequests: 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var recs []recordedRequest
			srv := newRecordedServer(t, tc.serverStatus, false, &recs)
			defer srv.Close()
			c := NewClient(resolveBaseURL(srv, tc.baseURL), "", "", nil)
			ctx := t.Context()
			if tc.identity != "" {
				ctx = WithEventIdentity(ctx, tc.identity)
			}
			err := c.SendRepoEvent(ctx, tc.handler, tc.event)
			checkSendResult(t, err, recs, tc.wantErr, tc.wantPath, tc.wantBodySubs, tc.wantRequests, tc.wantIdemPrefix)
		})
	}
}

func TestUnmanagedGuard(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		event       prwire.PREvent
		wantBodySub string
	}{
		{name: "unmanaged without build id stays in PRObject", event: prwire.PREvent{Event: "collaborator_alert", Repo: "mirrormere", PRNumber: 12, HeadSHA: "abc123", Unmanaged: true}, wantBodySub: `"unmanaged":true`},
		{name: "unmanaged flag wins over build id", event: prwire.PREvent{Event: "collaborator_alert", Repo: "mirrormere", PRNumber: 12, HeadSHA: "abc123", Unmanaged: true, BuildID: "build-1"}, wantBodySub: `"build_id":"build-1"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var recs []recordedRequest
			srv := newRecordedServer(t, 200, false, &recs)
			defer srv.Close()
			c := NewClient(srv.URL, "", "", nil)
			if err := c.SendPREvent(t.Context(), "HandleCollaboratorAlert", tc.event); err != nil {
				t.Fatalf("SendPREvent() = %v, want nil", err)
			}
			if len(recs) != 1 {
				t.Fatalf("requests = %d, want exactly 1", len(recs))
			}
			if !strings.Contains(recs[0].path, "/PRObject/") {
				t.Errorf("path = %q, want PRObject ingress path", recs[0].path)
			}
			if strings.Contains(recs[0].path, "BuildRouter") || strings.Contains(recs[0].body, "BuildRouter") {
				t.Errorf("request references BuildRouter, want PRObject only: %q %q", recs[0].path, recs[0].body)
			}
			if !strings.Contains(recs[0].body, tc.wantBodySub) {
				t.Errorf("body %q missing %q", recs[0].body, tc.wantBodySub)
			}
		})
	}
}

func TestSingleObjectServiceRouting(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		agent        string
		wantPRPath   string
		wantRepoPath string
	}{
		{name: "empty agent defaults to standard services", agent: "", wantPRPath: "/PRObject/azylman%2Faerial%231/HandleCheckPassed/send", wantRepoPath: "/RepoObject/azylman%2Faerial/HandleMainPush/send"},
		{name: "aerial agent defaults to standard services", agent: "aerial", wantPRPath: "/PRObject/azylman%2Faerial%231/HandleCheckPassed/send", wantRepoPath: "/RepoObject/azylman%2Faerial/HandleMainPush/send"},
		{name: "agent-alpha agent still targets the single services", agent: "agent-alpha", wantPRPath: "/PRObject/azylman%2Faerial%231/HandleCheckPassed/send", wantRepoPath: "/RepoObject/azylman%2Faerial/HandleMainPush/send"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var recs []recordedRequest
			srv := newRecordedServer(t, 200, false, &recs)
			defer srv.Close()
			c := NewClient(srv.URL, "", "", nil)

			if err := c.SendPREvent(t.Context(), "HandleCheckPassed", prwire.PREvent{Repo: "aerial", PRNumber: 1, Agent: tc.agent}); err != nil {
				t.Fatalf("SendPREvent failed: %v", err)
			}
			if recs[0].path != tc.wantPRPath {
				t.Errorf("PR path = %q, want %q", recs[0].path, tc.wantPRPath)
			}

			if err := c.SendRepoEvent(t.Context(), "HandleMainPush", prwire.RepoEvent{Repo: "aerial"}); err != nil {
				t.Fatalf("SendRepoEvent failed: %v", err)
			}
			if recs[1].path != tc.wantRepoPath {
				t.Errorf("Repo path = %q, want %q", recs[1].path, tc.wantRepoPath)
			}
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
		{name: "identity scopes handler and payload digest", identity: "gh:delivery-1", handler: "HandleMerged", payload: `{"repo":"azylman/aerial"}`, want: "HandleMerged:gh:delivery-1:d0732f62781ebd07"},
		{name: "blank identity falls back to payload digest", identity: "  ", handler: "HandleMainPush", payload: `{"repo":"azylman/aerial"}`, want: "HandleMainPush:d0732f62781ebd07"},
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

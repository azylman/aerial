package restateclient

// Register and GetBuildLifecycle tests: request-response PRObject calls the
// register handler and the buildmeta consumer use. They carry no
// Idempotency-Key, surface ingress status codes, and decode the verdict.

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/pubsub"
)

type registryCall struct {
	path  string
	body  string
	idKey string
}

func newRegistryServer(t *testing.T, status int, body string, calls *[]registryCall, mu *sync.Mutex) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		*calls = append(*calls, registryCall{path: r.URL.EscapedPath(), body: string(raw), idKey: r.Header.Get("Idempotency-Key")})
		mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func assertErrSub(t *testing.T, op string, err error, wantSub string) {
	t.Helper()
	if wantSub == "" && err != nil {
		t.Fatalf("%s() = %v, want nil", op, err)
	}
	if wantSub != "" && (err == nil || !strings.Contains(err.Error(), wantSub)) {
		t.Fatalf("%s() = %v, want error containing %q", op, err, wantSub)
	}
}

// assertRegistryCall checks the call count and that the first call hit
// wantPath with no Idempotency-Key and a body containing wantBodySub.
func assertRegistryCall(t *testing.T, calls []registryCall, mu *sync.Mutex, wantCalls int, wantPath, wantBodySub string) {
	t.Helper()
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != wantCalls {
		t.Fatalf("calls = %d, want %d", len(calls), wantCalls)
	}
	if wantCalls > 0 && (calls[0].path != wantPath || calls[0].idKey != "" || !strings.Contains(calls[0].body, wantBodySub)) {
		t.Fatalf("call = %+v, want path %q, no idempotency key, body containing %q", calls[0], wantPath, wantBodySub)
	}
}

func TestClientRegister(t *testing.T) {
	t.Parallel()
	const okBody = `{"target_id":"123456789012345678","agent":"aerial","status":"open"}`
	tests := []struct {
		name         string
		unconfigured bool
		req          prwire.RegisterRequest
		status       int
		body         string
		wantErrSub   string
		wantTerminal bool
		wantCalls    int
		wantPath     string
		wantBodySub  string
		want         prwire.RegisterResponse
	}{
		{name: "register round-trips", req: prwire.RegisterRequest{Repo: "aerial", PRNumber: 7, Branch: "feat", HeadSHA: "h1", TargetID: "headless:x", FallbackTargetID: "123456789012345678", EnforceOwner: true, ExpectedOwner: "aerial"}, status: 200, body: okBody, wantCalls: 1, wantPath: "/PRObject/azylman%2Faerial%237/Register", wantBodySub: `"expected_owner":"aerial"`, want: prwire.RegisterResponse{TargetID: "123456789012345678", Agent: "aerial", Status: "open"}},
		{name: "owner mismatch is terminal", req: prwire.RegisterRequest{Repo: "aerial", PRNumber: 7}, status: 409, body: "owner", wantErrSub: "HTTP 409", wantTerminal: true, wantCalls: 1, wantPath: "/PRObject/azylman%2Faerial%237/Register"},
		{name: "undecodable body surfaces", req: prwire.RegisterRequest{Repo: "aerial", PRNumber: 7}, status: 200, body: "nope{", wantErrSub: "decode restate response", wantCalls: 1, wantPath: "/PRObject/azylman%2Faerial%237/Register"},
		{name: "missing pr rejected without request", req: prwire.RegisterRequest{Repo: "aerial"}, status: 200, body: okBody, wantErrSub: "required", wantTerminal: true},
		{name: "unconfigured fails closed", unconfigured: true, req: prwire.RegisterRequest{Repo: "aerial", PRNumber: 7}, wantErrSub: "not configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls []registryCall
			var mu sync.Mutex
			baseURL := ""
			if !tc.unconfigured {
				baseURL = newRegistryServer(t, tc.status, tc.body, &calls, &mu)
			}
			got, err := NewClient(baseURL, "", "", nil).Register(t.Context(), tc.req)
			assertErrSub(t, "Register", err, tc.wantErrSub)
			if tc.wantErrSub != "" && IsTerminal(err) != tc.wantTerminal {
				t.Fatalf("IsTerminal = %v, want %v", IsTerminal(err), tc.wantTerminal)
			}
			if got != tc.want {
				t.Fatalf("Register() = %+v, want %+v", got, tc.want)
			}
			assertRegistryCall(t, calls, &mu, tc.wantCalls, tc.wantPath, tc.wantBodySub)
		})
	}
}

func TestClientGetBuildLifecycle(t *testing.T) {
	t.Parallel()
	const lifecycleBody = `{"found":true,"status":"merged","head_sha":"h1","merge_sha":"m1","started":true,"has_build_metadata":true,"build_metadata":{"version":1,"kind":"final","repository":"azylman/aerial","run_id":9,"source_sha":"m1","pr_number":null,"selected":null}}`
	tests := []struct {
		name         string
		unconfigured bool
		repo         string
		pr           int
		status       int
		body         string
		wantErrSub   string
		wantCalls    int
		want         prwire.BuildLifecycle
	}{
		{name: "lifecycle round-trips", repo: "aerial", pr: 7, status: 200, body: lifecycleBody, wantCalls: 1, want: prwire.BuildLifecycle{Found: true, Status: "merged", HeadSHA: "h1", MergeSHA: "m1", Started: true, HasBuildMetadata: true, BuildMetadata: &prwire.BuildMetadata{Version: 1, Kind: "final", Repository: "azylman/aerial", RunID: 9, SourceSHA: "m1"}}},
		{name: "unknown pr is zero", repo: "aerial", pr: 7, status: 200, body: `{"found":false,"status":"","head_sha":"","merge_sha":"","started":false,"has_build_metadata":false}`, wantCalls: 1},
		{name: "ingress 500 surfaces", repo: "aerial", pr: 7, status: 500, body: "boom", wantErrSub: "HTTP 500", wantCalls: 1},
		{name: "empty repo rejected without request", pr: 7, wantErrSub: "required"},
		{name: "unconfigured fails closed", unconfigured: true, repo: "aerial", pr: 7, wantErrSub: "not configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls []registryCall
			var mu sync.Mutex
			baseURL := ""
			if !tc.unconfigured {
				baseURL = newRegistryServer(t, tc.status, tc.body, &calls, &mu)
			}
			got, err := NewClient(baseURL, "", "", nil).GetBuildLifecycle(t.Context(), tc.repo, tc.pr)
			assertErrSub(t, "GetBuildLifecycle", err, tc.wantErrSub)
			if got.Found != tc.want.Found || got.Status != tc.want.Status || got.HeadSHA != tc.want.HeadSHA || got.MergeSHA != tc.want.MergeSHA || got.Started != tc.want.Started || got.HasBuildMetadata != tc.want.HasBuildMetadata {
				t.Fatalf("GetBuildLifecycle() = %+v, want %+v", got, tc.want)
			}
			if (got.BuildMetadata == nil) != (tc.want.BuildMetadata == nil) || (got.BuildMetadata != nil && (got.BuildMetadata.RunID != tc.want.BuildMetadata.RunID || got.BuildMetadata.SourceSHA != tc.want.BuildMetadata.SourceSHA)) {
				t.Fatalf("BuildMetadata = %+v, want %+v", got.BuildMetadata, tc.want.BuildMetadata)
			}
			assertRegistryCall(t, calls, &mu, tc.wantCalls, "/PRObject/azylman%2Faerial%237/GetBuildLifecycle", "{}")
		})
	}
}

func TestClientRetryReadiness(t *testing.T) {
	t.Parallel()
	const wantBody = `{"merge_sha":"m1","enforce_owner":true,"expected_owner":"aerial"}`
	req := prwire.RetryReadinessRequest{MergeSHA: "m1", EnforceOwner: true, ExpectedOwner: "aerial"}
	tests := []struct {
		name         string
		unconfigured bool
		repo         string
		pr           int
		status       int
		body         string
		wantErrSub   string
		wantCalls    int
		wantStatus   int
		wantRequests int
	}{
		{name: "accepted decision round-trips", repo: "aerial", pr: 7, status: 200, body: `{"status":202,"kustomization_requests":{"core":{"namespace":"flux-system","requested_at":"t1"}},"effect_key":"k"}`, wantCalls: 1, wantStatus: 202, wantRequests: 1},
		{name: "conflict decision round-trips", repo: "aerial", pr: 7, status: 200, body: `{"status":409}`, wantCalls: 1, wantStatus: 409},
		{name: "owner fence surfaces as a terminal error", repo: "aerial", pr: 7, status: 409, body: "owner", wantErrSub: "HTTP 409", wantCalls: 1},
		{name: "zero pr rejected without request", repo: "aerial", wantErrSub: "required"},
		{name: "unconfigured fails closed", unconfigured: true, repo: "aerial", pr: 7, wantErrSub: "not configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls []registryCall
			var mu sync.Mutex
			baseURL := ""
			if !tc.unconfigured {
				baseURL = newRegistryServer(t, tc.status, tc.body, &calls, &mu)
			}
			got, err := NewClient(baseURL, "", "", nil).RetryReadiness(t.Context(), tc.repo, tc.pr, req)
			assertErrSub(t, "RetryReadiness", err, tc.wantErrSub)
			if got.Status != tc.wantStatus || len(got.Requests) != tc.wantRequests {
				t.Fatalf("RetryReadiness() = %+v, want status %d with %d requests", got, tc.wantStatus, tc.wantRequests)
			}
			assertRegistryCall(t, calls, &mu, tc.wantCalls, "/PRObject/azylman%2Faerial%237/RetryReadiness", wantBody)
		})
	}
}

func TestIdempotencyPayload(t *testing.T) {
	t.Parallel()
	req := func(ns, at string) map[string]prwire.KustomizationRequest {
		return map[string]prwire.KustomizationRequest{"aerial-core": {Namespace: ns, RequestedAt: at}}
	}
	base := prwire.PREvent{Repo: "azylman/aerial", PRNumber: 7, Jobs: []string{"eri"}, KustomizationRequests: req("flux-system", "2026-11-20T00:00:00Z")}
	retried := base
	retried.KustomizationRequests = req("flux-system", "2026-11-20T00:05:00Z")
	otherNS := base
	otherNS.KustomizationRequests = req("apps", "2026-11-20T00:00:00Z")
	fact := func(at time.Time, status string) prwire.PREvent {
		return prwire.PREvent{Repo: "azylman/aerial", PRNumber: 7, Facts: []pubsub.PRLifecycleEvent{{Event: "conflict", HeadSHA: "h1", Status: status, OccurredAt: at}}}
	}
	t0 := time.Date(2026, 11, 20, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		a, b     any
		wantSame bool
	}{
		{name: "retry with new requested_at dedups", a: base, b: retried, wantSame: true},
		{name: "different namespace does not dedup", a: base, b: otherNS, wantSame: false},
		{name: "repo events digest as sent", a: prwire.RepoEvent{Repo: "azylman/aerial", MainSHA: "a"}, b: prwire.RepoEvent{Repo: "azylman/aerial", MainSHA: "b"}, wantSame: false},
		{name: "events without requests digest as sent", a: prwire.PREvent{Repo: "azylman/aerial", PRNumber: 7}, b: prwire.PREvent{Repo: "azylman/aerial", PRNumber: 7}, wantSame: true},
		{name: "fact retry with new occurred_at dedups", a: fact(t0, "conflict"), b: fact(t0.Add(time.Minute), "conflict"), wantSame: true},
		{name: "different fact body does not dedup", a: fact(t0, "conflict"), b: fact(t0, "open"), wantSame: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			keyA, errA := json.Marshal(idempotencyPayload(tc.a))
			keyB, errB := json.Marshal(idempotencyPayload(tc.b))
			if errA != nil || errB != nil {
				t.Fatalf("marshal errors = %v, %v", errA, errB)
			}
			same := SendIdempotencyKey("id", "HandleKustomizationRequested", keyA) == SendIdempotencyKey("id", "HandleKustomizationRequested", keyB)
			if same != tc.wantSame {
				t.Fatalf("same key = %v, want %v", same, tc.wantSame)
			}
		})
	}
}

func TestDigestShape(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		payload any
		want    string
	}{
		{name: "pr event digests in the pre-prwire field order", payload: prwire.PREvent{Repo: "azylman/aerial", PRNumber: 7, Agent: "agent-alpha", Jobs: []string{"eri"}, AutoUpdate: &prwire.AutoUpdatePolicy{DefaultOwner: true}}, want: `{"event":"","repo":"azylman/aerial","pr_number":7,"head_sha":"","merge_sha":"","check_name":"","job_name":"","branch":"","title":"","gate_generation":0,"reopen_epoch":0,"unmanaged":false,"build_id":"","agent":"agent-alpha","jobs":["eri"],"auto_update":{"default_owner":true}}`},
		{name: "sha event wraps the pre-prwire pr event with an empty agent key", payload: prwire.SHAEvent{Repo: "azylman/aerial", SHA: "abc", Handler: "HandleMerged", Event: prwire.PREvent{Repo: "azylman/aerial", MergeSHA: "abc"}}, want: `{"repo":"azylman/aerial","sha":"abc","handler":"HandleMerged","event":{"event":"","repo":"azylman/aerial","pr_number":0,"head_sha":"","merge_sha":"abc","check_name":"","job_name":"","branch":"","title":"","gate_generation":0,"reopen_epoch":0,"unmanaged":false,"build_id":"","agent":""}}`},
		{name: "repo event digests as sent", payload: prwire.RepoEvent{Repo: "azylman/aerial", MainSHA: "a"}, want: `{"repo":"azylman/aerial","main_sha":"a","generation":0,"pr_number":0,"head_sha":""}`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := json.Marshal(digestShape(tc.payload))
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("digest bytes = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestClientSendSHAEvent(t *testing.T) {
	t.Parallel()
	const sha = "0123456789abcdef0123456789abcdef01234567"
	tests := []struct {
		name         string
		unconfigured bool
		handler      string
		ev           prwire.SHAEvent
		status       int
		wantErrSub   string
		wantTerminal bool
		wantCalls    int
		wantPath     string
		wantBodySub  string
	}{
		{name: "check run routes by sha", handler: "HandleCheckRunBySHA", ev: prwire.SHAEvent{Repo: "aerial", SHA: sha, Handler: "HandleCheckFailed", Event: prwire.PREvent{HeadSHA: sha, CheckName: "ci"}}, status: 202, wantCalls: 1, wantPath: "/RepoObject/azylman%2Faerial/HandleCheckRunBySHA/send", wantBodySub: `"event":{"event":"","repo":"azylman/aerial","pr_number":0`},
		{name: "push routes merged by sha", handler: "HandlePushBySHA", ev: prwire.SHAEvent{Repo: "aerial", SHA: sha, Handler: "HandleMerged", Event: prwire.PREvent{MergeSHA: sha}}, status: 202, wantCalls: 1, wantPath: "/RepoObject/azylman%2Faerial/HandlePushBySHA/send", wantBodySub: `"merge_sha":"0123456789abcdef0123456789abcdef01234567"`},
		{name: "unknown sha handler rejected without request", handler: "HandleMainPush", ev: prwire.SHAEvent{Repo: "aerial", SHA: sha, Handler: "HandleMerged"}, wantErrSub: "unknown repo sha handler", wantTerminal: true},
		{name: "unroutable pr handler rejected without request", handler: "HandleCheckRunBySHA", ev: prwire.SHAEvent{Repo: "aerial", SHA: sha, Handler: "HandleCheckStatus"}, wantErrSub: "not routable", wantTerminal: true},
		{name: "zero sha rejected without request", handler: "HandlePushBySHA", ev: prwire.SHAEvent{Repo: "aerial", SHA: "0000000", Handler: "HandleMerged"}, wantErrSub: "sha is required", wantTerminal: true},
		{name: "empty repo rejected without request", handler: "HandlePushBySHA", ev: prwire.SHAEvent{SHA: sha, Handler: "HandleMerged"}, wantErrSub: "repo is required", wantTerminal: true},
		{name: "ingress 500 surfaces", handler: "HandleWorkflowRunBySHA", ev: prwire.SHAEvent{Repo: "aerial", SHA: sha, Handler: "HandleCheckPassed"}, status: 500, wantErrSub: "HTTP 500", wantCalls: 1, wantPath: "/RepoObject/azylman%2Faerial/HandleWorkflowRunBySHA/send"},
		{name: "unconfigured fails closed", unconfigured: true, handler: "HandlePushBySHA", ev: prwire.SHAEvent{Repo: "aerial", SHA: sha, Handler: "HandleMerged"}, wantErrSub: "not configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls []registryCall
			var mu sync.Mutex
			baseURL := ""
			if !tc.unconfigured {
				baseURL = newRegistryServer(t, tc.status, "", &calls, &mu)
			}
			err := NewClient(baseURL, "", "", nil).SendSHAEvent(t.Context(), tc.handler, tc.ev)
			assertErrSub(t, "SendSHAEvent", err, tc.wantErrSub)
			if tc.wantErrSub != "" && IsTerminal(err) != tc.wantTerminal {
				t.Fatalf("IsTerminal = %v, want %v", IsTerminal(err), tc.wantTerminal)
			}
			mu.Lock()
			defer mu.Unlock()
			if len(calls) != tc.wantCalls {
				t.Fatalf("calls = %d, want %d", len(calls), tc.wantCalls)
			}
			if tc.wantCalls > 0 && (calls[0].path != tc.wantPath || calls[0].idKey == "" || !strings.Contains(calls[0].body, tc.wantBodySub)) {
				t.Fatalf("call = %+v, want path %q, an idempotency key, body containing %q", calls[0], tc.wantPath, tc.wantBodySub)
			}
		})
	}
}

func TestClientResolvePRBySHA(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		unconfigured bool
		repo         string
		sha          string
		status       int
		body         string
		wantErrSub   string
		wantCalls    int
		want         prwire.ResolveSHAResponse
	}{
		{name: "indexed sha resolves", repo: "aerial", sha: "abc", status: 200, body: `{"pr_number":7,"branch":"feat","found":true}`, wantCalls: 1, want: prwire.ResolveSHAResponse{PRNumber: 7, Branch: "feat", Found: true}},
		{name: "unknown sha is zero", repo: "aerial", sha: "abc", status: 200, body: `{"pr_number":0,"branch":"","found":false}`, wantCalls: 1},
		{name: "ingress 500 surfaces", repo: "aerial", sha: "abc", status: 500, body: "boom", wantErrSub: "HTTP 500", wantCalls: 1},
		{name: "empty sha rejected without request", repo: "aerial", wantErrSub: "required"},
		{name: "unconfigured fails closed", unconfigured: true, repo: "aerial", sha: "abc", wantErrSub: "not configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls []registryCall
			var mu sync.Mutex
			baseURL := ""
			if !tc.unconfigured {
				baseURL = newRegistryServer(t, tc.status, tc.body, &calls, &mu)
			}
			got, err := NewClient(baseURL, "", "", nil).ResolvePRBySHA(t.Context(), tc.repo, tc.sha)
			assertErrSub(t, "ResolvePRBySHA", err, tc.wantErrSub)
			if got != tc.want {
				t.Fatalf("ResolvePRBySHA() = %+v, want %+v", got, tc.want)
			}
			assertRegistryCall(t, calls, &mu, tc.wantCalls, "/RepoObject/azylman%2Faerial/ResolvePRBySHA", `"sha":"abc"`)
		})
	}
}

func TestClientGetDeployView(t *testing.T) {
	t.Parallel()
	const viewBody = `{"found":true,"repo":"azylman/aerial","pr_number":7,"status":"deploying","branch":"feat/x","head_sha":"h1","merge_sha":"m1","target_id":"123456789012345678","kustomization_requests":{"core":{"namespace":"flux-system","requested_at":"t1"}},"deployed_kustomizations":["core"],"has_expected_deploy_jobs":true}`
	tests := []struct {
		name         string
		unconfigured bool
		repo         string
		pr           int
		status       int
		body         string
		wantErrSub   string
		wantCalls    int
		want         prwire.DeployView
	}{
		{
			name: "view round-trips", repo: "aerial", pr: 7, status: 200, body: viewBody, wantCalls: 1,
			want: prwire.DeployView{
				Found: true, Repo: "azylman/aerial", PRNumber: 7, Status: "deploying", Branch: "feat/x", HeadSHA: "h1", MergeSHA: "m1", TargetID: "123456789012345678",
				KustomizationRequests: map[string]prwire.KustomizationRequest{"core": {Namespace: "flux-system", RequestedAt: "t1"}}, DeployedKustomizations: []string{"core"}, HasExpectedDeployJobs: true,
			},
		},
		{name: "ingress 500 surfaces", repo: "aerial", pr: 7, status: 500, body: "boom", wantErrSub: "HTTP 500", wantCalls: 1},
		{name: "missing pr rejected without request", repo: "aerial", wantErrSub: "required"},
		{name: "unconfigured fails closed", unconfigured: true, repo: "aerial", pr: 7, wantErrSub: "not configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls []registryCall
			var mu sync.Mutex
			baseURL := ""
			if !tc.unconfigured {
				baseURL = newRegistryServer(t, tc.status, tc.body, &calls, &mu)
			}
			got, err := NewClient(baseURL, "", "", nil).GetDeployView(t.Context(), tc.repo, tc.pr)
			assertErrSub(t, "GetDeployView", err, tc.wantErrSub)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("GetDeployView() = %+v, want %+v", got, tc.want)
			}
			assertRegistryCall(t, calls, &mu, tc.wantCalls, "/PRObject/azylman%2Faerial%237/GetDeployView", "{}")
		})
	}
}

func TestClientListRollouts(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		unconfigured bool
		status       int
		body         string
		wantErrSub   string
		wantCalls    int
		want         []prwire.RolloutRef
	}{
		{name: "rollouts round-trip", status: 200, body: `{"rollouts":[{"repo":"azylman/aerial","pr_number":7}]}`, wantCalls: 1, want: []prwire.RolloutRef{{Repo: "azylman/aerial", PRNumber: 7}}},
		{name: "empty index", status: 200, body: `{"rollouts":null}`, wantCalls: 1},
		{name: "undecodable body surfaces", status: 200, body: `nope`, wantErrSub: "decode", wantCalls: 1},
		{name: "unconfigured fails closed", unconfigured: true, wantErrSub: "not configured"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls []registryCall
			var mu sync.Mutex
			baseURL := ""
			if !tc.unconfigured {
				baseURL = newRegistryServer(t, tc.status, tc.body, &calls, &mu)
			}
			got, err := NewClient(baseURL, "", "", nil).ListRollouts(t.Context())
			assertErrSub(t, "ListRollouts", err, tc.wantErrSub)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ListRollouts() = %+v, want %+v", got, tc.want)
			}
			assertRegistryCall(t, calls, &mu, tc.wantCalls, "/RepoObject/_rollouts/ListRollouts", "{}")
		})
	}
}

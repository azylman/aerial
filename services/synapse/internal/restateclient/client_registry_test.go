package restateclient

// Register and GetBuildLifecycle tests: request-response PRObject calls the
// register handler and the buildmeta consumer use. They carry no
// Idempotency-Key, surface ingress status codes, and decode the verdict.

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/azylman/aerial/services/synapse/internal/model"
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
		req          RegisterRequest
		status       int
		body         string
		wantErrSub   string
		wantTerminal bool
		wantCalls    int
		wantPath     string
		wantBodySub  string
		want         RegisterResponse
	}{
		{name: "register round-trips", req: RegisterRequest{Repo: "aerial", PRNumber: 7, Branch: "feat", HeadSHA: "h1", TargetID: "headless:x", FallbackTargetID: "123456789012345678", EnforceOwner: true, ExpectedOwner: "aerial"}, status: 200, body: okBody, wantCalls: 1, wantPath: "/PRObject/azylman%2Faerial%237/Register", wantBodySub: `"expected_owner":"aerial"`, want: RegisterResponse{TargetID: "123456789012345678", Agent: "aerial", Status: "open"}},
		{name: "owner mismatch is terminal", req: RegisterRequest{Repo: "aerial", PRNumber: 7}, status: 409, body: "owner", wantErrSub: "status 409", wantTerminal: true, wantCalls: 1, wantPath: "/PRObject/azylman%2Faerial%237/Register"},
		{name: "undecodable body surfaces", req: RegisterRequest{Repo: "aerial", PRNumber: 7}, status: 200, body: "nope{", wantErrSub: "decode restate response", wantCalls: 1, wantPath: "/PRObject/azylman%2Faerial%237/Register"},
		{name: "missing pr rejected without request", req: RegisterRequest{Repo: "aerial"}, status: 200, body: okBody, wantErrSub: "required", wantTerminal: true},
		{name: "unconfigured fails closed", unconfigured: true, req: RegisterRequest{Repo: "aerial", PRNumber: 7}, wantErrSub: "not configured"},
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
		want         BuildLifecycle
	}{
		{name: "lifecycle round-trips", repo: "aerial", pr: 7, status: 200, body: lifecycleBody, wantCalls: 1, want: BuildLifecycle{Found: true, Status: "merged", HeadSHA: "h1", MergeSHA: "m1", Started: true, HasBuildMetadata: true, BuildMetadata: &model.BuildMetadata{Version: 1, Kind: "final", Repository: "azylman/aerial", RunID: 9, SourceSHA: "m1"}}},
		{name: "unknown pr is zero", repo: "aerial", pr: 7, status: 200, body: `{"found":false,"status":"","head_sha":"","merge_sha":"","started":false,"has_build_metadata":false}`, wantCalls: 1},
		{name: "ingress 500 surfaces", repo: "aerial", pr: 7, status: 500, body: "boom", wantErrSub: "status 500", wantCalls: 1},
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

func TestIdempotencyPayload(t *testing.T) {
	t.Parallel()
	req := func(ns, at string) map[string]model.KustomizationRequest {
		return map[string]model.KustomizationRequest{"aerial-core": {Namespace: ns, RequestedAt: at}}
	}
	base := PREvent{Repo: "azylman/aerial", PRNumber: 7, Jobs: []string{"eri"}, KustomizationRequests: req("flux-system", "2026-11-20T00:00:00Z")}
	retried := base
	retried.KustomizationRequests = req("flux-system", "2026-11-20T00:05:00Z")
	otherNS := base
	otherNS.KustomizationRequests = req("apps", "2026-11-20T00:00:00Z")
	tests := []struct {
		name     string
		a, b     any
		wantSame bool
	}{
		{name: "retry with new requested_at dedups", a: base, b: retried, wantSame: true},
		{name: "different namespace does not dedup", a: base, b: otherNS, wantSame: false},
		{name: "repo events digest as sent", a: RepoEvent{Repo: "azylman/aerial", MainSHA: "a"}, b: RepoEvent{Repo: "azylman/aerial", MainSHA: "b"}, wantSame: false},
		{name: "events without requests digest as sent", a: PREvent{Repo: "azylman/aerial", PRNumber: 7}, b: PREvent{Repo: "azylman/aerial", PRNumber: 7}, wantSame: true},
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

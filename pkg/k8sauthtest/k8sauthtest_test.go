package k8sauthtest_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/azylman/aerial/pkg/hermetictest"
	"github.com/azylman/aerial/pkg/k8sauthtest"
)

func TestMain(m *testing.M) {
	hermetictest.Main(m)
}

const (
	trPath  = "/apis/authentication.k8s.io/v1/tokenreviews"
	sarPath = "/apis/authorization.k8s.io/v1/subjectaccessreviews"
	sarReq  = `{"spec":{"resourceAttributes":{"group":"aerial.io","resource":"prs","verb":"create","name":"alpha"}}}`
	allowed = `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":true}}`
	denied  = `{"apiVersion":"authorization.k8s.io/v1","kind":"SubjectAccessReview","status":{"allowed":false}}`
)

func TestRoundTripper(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		rt         *k8sauthtest.RoundTripper
		path       string
		body       string
		bodyErr    error
		wantStatus int
		wantBody   string
		wantErr    string
	}{
		{name: "token review default status", rt: &k8sauthtest.RoundTripper{AuthBody: "tr"}, path: trPath, wantStatus: 200, wantBody: "tr"},
		{name: "token review status", rt: &k8sauthtest.RoundTripper{AuthStatus: 503, AuthBody: "down"}, path: trPath, wantStatus: 503, wantBody: "down"},
		{name: "sar default denied", rt: &k8sauthtest.RoundTripper{}, path: sarPath, body: sarReq, wantStatus: 200, wantBody: denied},
		{name: "sar body and status", rt: &k8sauthtest.RoundTripper{SARStatus: 403, SARBody: "nope"}, path: sarPath, wantStatus: 403, wantBody: "nope"},
		{name: "sar rule exact match", rt: &k8sauthtest.RoundTripper{SARRules: map[string]bool{k8sauthtest.SARKey("prs", "create", "alpha"): true}}, path: sarPath, body: sarReq, wantStatus: 200, wantBody: allowed},
		{name: "sar rule wildcard name", rt: &k8sauthtest.RoundTripper{SARRules: map[string]bool{k8sauthtest.SARKey("prs", "create", ""): true}}, path: sarPath, body: sarReq, wantStatus: 200, wantBody: allowed},
		{name: "sar rule other name denied", rt: &k8sauthtest.RoundTripper{SARRules: map[string]bool{k8sauthtest.SARKey("prs", "create", "beta"): true}}, path: sarPath, body: sarReq, wantStatus: 200, wantBody: denied},
		{name: "sar rule false denied", rt: &k8sauthtest.RoundTripper{SARRules: map[string]bool{k8sauthtest.SARKey("", "", ""): false}}, path: sarPath, body: sarReq, wantStatus: 200, wantBody: denied},
		{name: "sar malformed rule key denied", rt: &k8sauthtest.RoundTripper{SARRules: map[string]bool{"prs": true}}, path: sarPath, body: sarReq, wantStatus: 200, wantBody: denied},
		{name: "sar rules override body", rt: &k8sauthtest.RoundTripper{SARBody: "ignored", SARRules: map[string]bool{k8sauthtest.SARKey("prs", "", ""): true}}, path: sarPath, body: sarReq, wantStatus: 200, wantBody: allowed},
		{name: "sar malformed body denied", rt: &k8sauthtest.RoundTripper{SARRules: map[string]bool{k8sauthtest.SARKey("", "", ""): true}}, path: sarPath, body: "{", wantStatus: 200, wantBody: denied},
		{name: "unreadable body", rt: &k8sauthtest.RoundTripper{}, path: trPath, bodyErr: errors.New("read failed"), wantErr: "read failed"},
		{name: "unknown path", rt: &k8sauthtest.RoundTripper{}, path: "/api/v1/namespaces", wantStatus: 200, wantBody: `{}`},
		{name: "transport error", rt: &k8sauthtest.RoundTripper{Err: errors.New("boom")}, path: trPath, wantErr: "boom"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotStatus, gotBody, gotErr := roundTrip(t, tc.rt, tc.path, tc.body, tc.bodyErr)
			if gotStatus != tc.wantStatus || gotBody != tc.wantBody || gotErr != tc.wantErr {
				t.Fatalf("got (%d, %q, err %q), want (%d, %q, err %q)", gotStatus, gotBody, gotErr, tc.wantStatus, tc.wantBody, tc.wantErr)
			}
			reqs := tc.rt.Requests()
			if len(reqs) != 1 || reqs[0].Path != tc.path || reqs[0].Body != tc.body {
				t.Fatalf("Requests() = %+v, want one {%s %q}", reqs, tc.path, tc.body)
			}
		})
	}
}

// roundTrip posts body (or a reader failing with bodyErr) to path through rt and
// flattens the outcome for declarative comparison.
func roundTrip(t *testing.T, rt *k8sauthtest.RoundTripper, path, body string, bodyErr error) (status int, respBody, errText string) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	if bodyErr != nil {
		r = iotest.ErrReader(bodyErr)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "https://k8s.example.com"+path, r)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := rt.RoundTrip(req)
	if err != nil {
		return 0, "", err.Error()
	}
	defer func() { _ = resp.Body.Close() }()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, string(got), ""
}

// TestAuthenticator drives the real k8sauth client against the fake, proving
// the builders and SARRules keys match what k8sauth sends and parses.
func TestAuthenticator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		rt        *k8sauthtest.RoundTripper
		target    string
		wantAgent string
		wantErr   string
		wantPaths []string
	}{
		{
			name:      "authenticated and authorized",
			rt:        &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:eri-alpha"), SARRules: map[string]bool{k8sauthtest.SARKey("prs", "create", "alpha"): true}},
			target:    "alpha",
			wantAgent: "alpha",
			wantPaths: []string{trPath, sarPath},
		},
		{
			name:      "authenticated but forbidden",
			rt:        &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReview("system:serviceaccount:default:eri-alpha"), SARBody: k8sauthtest.SAR(false, "no rbac")},
			target:    "beta",
			wantAgent: "alpha",
			wantErr:   "no rbac",
			wantPaths: []string{trPath, sarPath},
		},
		{
			name:      "token denied",
			rt:        &k8sauthtest.RoundTripper{AuthBody: k8sauthtest.TokenReviewDenied()},
			wantErr:   "token not authenticated",
			wantPaths: []string{trPath},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			auth := k8sauthtest.Authenticator(t, tc.rt)
			if !auth.Enabled() {
				t.Fatal("Authenticator must be enabled")
			}
			caller, err := auth.Authenticate(context.Background(), "caller-token")
			if err == nil {
				err = auth.Authorize(context.Background(), caller, "prs", "create", tc.target)
			}
			gotErr := ""
			if err != nil {
				gotErr = err.Error()
			}
			if !strings.Contains(gotErr, tc.wantErr) || (tc.wantErr == "") != (gotErr == "") {
				t.Fatalf("err = %q, want containing %q", gotErr, tc.wantErr)
			}
			if caller.CallerAgent != tc.wantAgent {
				t.Fatalf("CallerAgent = %q, want %q", caller.CallerAgent, tc.wantAgent)
			}
			var paths []string
			for _, r := range tc.rt.Requests() {
				paths = append(paths, r.Path)
			}
			if strings.Join(paths, ",") != strings.Join(tc.wantPaths, ",") {
				t.Fatalf("paths = %v, want %v", paths, tc.wantPaths)
			}
		})
	}
}

var _ http.RoundTripper = (*k8sauthtest.RoundTripper)(nil)

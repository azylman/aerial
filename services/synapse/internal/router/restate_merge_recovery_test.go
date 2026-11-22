package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/restateingresstest"
	"github.com/azylman/aerial/services/synapse/internal/github"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
)

const (
	recoveryHead  = "c0de232700000000000000000000000000002327"
	recoveryMerge = "353b1b0500000000000000000000000000002327"
)

// recoveryRepo and recoveryPR are the decoded Restate object path prefixes
// the recovery path calls: the RepoObject SHA index and the PRObject.
const (
	recoveryRepo = "/RepoObject/azylman/aerial/"
	recoveryPR   = "/PRObject/azylman/aerial#2327/"
)

// recoverySends returns the one-way sends the ingress received, in order.
func recoverySends(in *restateingresstest.Server) []restateingresstest.Call {
	var sends []restateingresstest.Call
	for _, c := range in.Calls() {
		if strings.HasSuffix(c.Path, "/send") {
			sends = append(sends, c)
		}
	}
	return sends
}

// recoveryHandler is the Restate handler a send targeted.
func recoveryHandler(c restateingresstest.Call) string {
	path := strings.TrimSuffix(c.Path, "/send")
	return path[strings.LastIndex(path, "/")+1:]
}

// recoveryGitHub serves the commits/<sha>/pulls lookup with code and body
// and an empty open-PR list.
func recoveryGitHub(t *testing.T, code int, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/commits/") {
			w.WriteHeader(code)
			_, _ = w.Write([]byte(body))
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func TestShouldRecoverMerge(t *testing.T) {
	t.Parallel()
	merged := github.CommitPR{Number: 2327, HeadSHA: recoveryHead, Merged: true, MergeCommitSHA: recoveryMerge}
	tests := []struct {
		name string
		pr   github.CommitPR
		sha  string
		view prwire.DeployView
		want bool
	}{
		{name: "open tracked pr merged on the commit recovers", pr: merged, sha: recoveryMerge, view: prwire.DeployView{Found: true, Status: "open"}, want: true},
		{name: "merge sha match ignores case and space", pr: merged, sha: " " + strings.ToUpper(recoveryMerge) + " ", view: prwire.DeployView{Found: true}, want: true},
		{name: "pr merged on another commit in restate still recovers", pr: merged, sha: recoveryMerge, view: prwire.DeployView{Found: true, MergeSHA: recoveryHead}, want: true},
		{name: "already merged on the commit sends nothing", pr: merged, sha: recoveryMerge, view: prwire.DeployView{Found: true, MergeSHA: strings.ToUpper(recoveryMerge)}},
		{name: "untracked pr sends nothing", pr: merged, sha: recoveryMerge},
		{name: "unmerged pr sends nothing", pr: github.CommitPR{Number: 2327, HeadSHA: recoveryHead, MergeCommitSHA: recoveryMerge}, sha: recoveryMerge, view: prwire.DeployView{Found: true}},
		{name: "merge commit other than the sha sends nothing", pr: merged, sha: recoveryHead, view: prwire.DeployView{Found: true}},
		{name: "blank sha sends nothing", pr: merged, view: prwire.DeployView{Found: true}},
		{name: "no pr sends nothing", pr: github.CommitPR{Merged: true, MergeCommitSHA: recoveryMerge}, sha: recoveryMerge, view: prwire.DeployView{Found: true}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := shouldRecoverMerge(tc.pr, tc.sha, tc.view); got != tc.want {
				t.Errorf("shouldRecoverMerge() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestResolveOwnedSHAPRMergeRecovery drives the restate-owned resolve and
// recover path. Rows run serially because they share the global
// merge-recovered counter.
func TestResolveOwnedSHAPRMergeRecovery(t *testing.T) {
	mergedPR := `[{"number":2327,"state":"closed","head":{"ref":"feat/redaction-purge","sha":"` + recoveryHead + `"},"merged_at":"2026-11-21T08:38:21Z","merge_commit_sha":"` + recoveryMerge + `"}]`
	openView := `{"found":true,"repo":"azylman/aerial","pr_number":2327,"status":"open","head_sha":"` + recoveryHead + `"}`
	tests := []struct {
		name         string
		index        string
		ghCode       int
		ghBody       string
		view         string
		viewCode     int
		sendCode     int
		wantPR       int
		wantSends    []string
		wantRecorded float64
	}{
		{name: "send failure records no recovery", index: `{"found":false}`, ghCode: 200, ghBody: mergedPR, view: openView, sendCode: 500, wantPR: 2327, wantSends: []string{"HandleMerged"}},
		{name: "index hit sends no merge", index: `{"pr_number":2327,"branch":"feat/redaction-purge","found":true}`, ghCode: 200, ghBody: mergedPR, view: openView, wantPR: 2327},
		{name: "github fallback on a merged pr recovers the merge", index: `{"found":false}`, ghCode: 200, ghBody: mergedPR, view: openView, wantPR: 2327, wantSends: []string{"HandleMerged"}, wantRecorded: 1},
		{name: "restate already merged on the sha sends nothing", index: `{"found":false}`, ghCode: 200, ghBody: mergedPR, view: `{"found":true,"status":"deploying","merge_sha":"` + recoveryMerge + `"}`, wantPR: 2327},
		{name: "unmerged pr sends nothing", index: `{"found":false}`, ghCode: 200, ghBody: `[{"number":2327,"state":"open","head":{"ref":"feat/redaction-purge","sha":"` + recoveryHead + `"},"merge_commit_sha":"` + recoveryMerge + `"}]`, view: openView, wantPR: 2327},
		{name: "merge commit other than the sha sends nothing", index: `{"found":false}`, ghCode: 200, ghBody: `[{"number":2327,"state":"closed","head":{"ref":"feat/redaction-purge","sha":"` + recoveryHead + `"},"merged_at":"2026-11-21T08:38:21Z","merge_commit_sha":"` + recoveryHead + `"}]`, view: openView, wantPR: 2327},
		{name: "untracked pr sends nothing", index: `{"found":false}`, ghCode: 200, ghBody: mergedPR, view: `{"found":false}`, wantPR: 2327},
		{name: "deploy view failure sends nothing", index: `{"found":false}`, ghCode: 200, ghBody: mergedPR, view: "boom", viewCode: 500, wantPR: 2327},
		{name: "github error resolves nothing and sends nothing", index: `{"found":false}`, ghCode: 500, ghBody: "boom", view: openView},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ingress := restateingresstest.New(t)
			ingress.Routes = map[string]restateingresstest.Response{
				recoveryRepo + "ResolvePRBySHA":  {Body: tc.index},
				recoveryPR + "GetDeployView":     {Status: tc.viewCode, Body: tc.view},
				recoveryPR + "HandleMerged/send": {Status: tc.sendCode},
			}
			cfg := Config{RestateURL: ingress.URL(), GitHubAPIURL: recoveryGitHub(t, tc.ghCode, tc.ghBody)}
			srv := NewRouterServer(fixedConfig(cfg), nil)
			ctx := context.Background()
			counter := metrics.RestateMergeRecoveredTotal.WithLabelValues(mergeRecoveryPathOwnedDeploy)
			before := testutil.ToFloat64(counter)
			prNum, _ := srv.resolveOwnedSHAPR(ctx, "azylman/aerial", recoveryMerge)
			if prNum != tc.wantPR {
				t.Errorf("resolveOwnedSHAPR() pr = %d, want %d", prNum, tc.wantPR)
			}
			sends := recoverySends(ingress)
			handlers := make([]string, 0, len(sends))
			for _, s := range sends {
				handlers = append(handlers, recoveryHandler(s))
			}
			if strings.Join(handlers, ",") != strings.Join(tc.wantSends, ",") {
				t.Errorf("sends = %v, want %v", handlers, tc.wantSends)
			}
			if got := testutil.ToFloat64(counter) - before; got != tc.wantRecorded {
				t.Errorf("merge recovered delta = %v, want %v", got, tc.wantRecorded)
			}
		})
	}
}

// TestOwnedDeployEventRecoversMergeFirst pins the order on the restate-owned
// deploy path: the recovered HandleMerged is sent before the deploy event,
// under the deterministic merge-recovery idempotency identity.
func TestOwnedDeployEventRecoversMergeFirst(t *testing.T) {
	mergedPR := `[{"number":2327,"state":"closed","head":{"ref":"feat/redaction-purge","sha":"` + recoveryHead + `"},"merged_at":"2026-11-21T08:38:21Z","merge_commit_sha":"` + recoveryMerge + `"}]`
	tests := []struct {
		name        string
		evt         DeployEvent
		wantFirst   string
		wantSecond  string
		wantKeyPart string
	}{
		{
			name:        "deploy failure on the merge commit",
			evt:         DeployEvent{Event: eventDeployFailed, Repo: "azylman/aerial", CommitSHA: recoveryMerge, JobName: "aerial-core", Status: "failed"},
			wantFirst:   "HandleMerged",
			wantSecond:  "HandleDeployFailed",
			wantKeyPart: "merge-recovery:azylman/aerial#2327@" + recoveryMerge,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ingress := restateingresstest.New(t)
			ingress.Routes = map[string]restateingresstest.Response{
				recoveryRepo + "ResolvePRBySHA": {Body: `{"found":false}`},
				recoveryPR + "GetDeployView":    {Body: `{"found":true,"repo":"azylman/aerial","pr_number":2327,"status":"open","target_id":"100000000000000003","head_sha":"` + recoveryHead + `"}`},
			}
			cfg := Config{RestateURL: ingress.URL(), GitHubAPIURL: recoveryGitHub(t, 200, mergedPR)}
			srv := newTestRouter(cfg, nil, &mockOutboundDispatcher{}, &mockEventBus{})
			ctx := context.Background()
			if _, err := srv.processOwnedDeployEvent(ctx, tc.evt); err != nil {
				t.Fatalf("processOwnedDeployEvent() err = %v", err)
			}
			sends := recoverySends(ingress)
			if len(sends) != 2 || recoveryHandler(sends[1]) != tc.wantSecond {
				t.Fatalf("sends = %+v, want the recovered merge then the deploy event", sends)
			}
			if recoveryHandler(sends[0]) != tc.wantFirst || !strings.Contains(sends[0].IdempotencyKey, tc.wantKeyPart) {
				t.Errorf("first send = %s key %q, want %s keyed %q", recoveryHandler(sends[0]), sends[0].IdempotencyKey, tc.wantFirst, tc.wantKeyPart)
			}
			var ev prwire.PREvent
			if err := json.Unmarshal(sends[0].Body, &ev); err != nil {
				t.Fatal(err)
			}
			if ev.HeadSHA != recoveryHead || ev.MergeSHA != recoveryMerge {
				t.Errorf("recovered merge = %+v, want head %s merge %s", ev, recoveryHead, recoveryMerge)
			}
		})
	}
}

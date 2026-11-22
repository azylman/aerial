package router

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/services/synapse/internal/github"
	"github.com/azylman/aerial/services/synapse/internal/metrics"
)

const (
	recoveryHead  = "c0de232700000000000000000000000000002327"
	recoveryMerge = "353b1b0500000000000000000000000000002327"
)

// recoveryIngress is a hermetic Restate ingress: request-response calls
// answer from bodies by handler (code from codes, default 200) and one-way
// sends are captured with their Idempotency-Key.
type recoveryIngress struct {
	mu       sync.Mutex
	bodies   map[string]string
	codes    map[string]int
	sendCode int
	sends    []recoverySend
}

// recoverySend is one captured one-way send.
type recoverySend struct {
	handler string
	key     string
	body    string
}

func (in *recoveryIngress) serve(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		parts := strings.Split(strings.TrimSuffix(r.URL.EscapedPath(), "/send"), "/")
		handler := parts[len(parts)-1]
		in.mu.Lock()
		defer in.mu.Unlock()
		if strings.HasSuffix(r.URL.EscapedPath(), "/send") {
			in.sends = append(in.sends, recoverySend{handler: handler, key: r.Header.Get("Idempotency-Key"), body: string(raw)})
			code := http.StatusAccepted
			if in.sendCode != 0 {
				code = in.sendCode
			}
			w.WriteHeader(code)
			return
		}
		code := http.StatusOK
		if c, ok := in.codes[handler]; ok {
			code = c
		}
		w.WriteHeader(code)
		_, _ = w.Write([]byte(in.bodies[handler]))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func (in *recoveryIngress) captured() []recoverySend {
	in.mu.Lock()
	defer in.mu.Unlock()
	return append([]recoverySend(nil), in.sends...)
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
			ingress := &recoveryIngress{
				bodies:   map[string]string{"ResolvePRBySHA": tc.index, "GetDeployView": tc.view},
				codes:    map[string]int{},
				sendCode: tc.sendCode,
			}
			if tc.viewCode != 0 {
				ingress.codes["GetDeployView"] = tc.viewCode
			}
			cfg := Config{RestateURL: ingress.serve(t), GitHubAPIURL: recoveryGitHub(t, tc.ghCode, tc.ghBody)}
			srv := NewRouterServer(fixedConfig(cfg), nil)
			ctx := context.Background()
			counter := metrics.RestateMergeRecoveredTotal.WithLabelValues(mergeRecoveryPathOwnedDeploy)
			before := testutil.ToFloat64(counter)
			prNum, _ := srv.resolveOwnedSHAPR(ctx, "azylman/aerial", recoveryMerge)
			if prNum != tc.wantPR {
				t.Errorf("resolveOwnedSHAPR() pr = %d, want %d", prNum, tc.wantPR)
			}
			sends := ingress.captured()
			handlers := make([]string, 0, len(sends))
			for _, s := range sends {
				handlers = append(handlers, s.handler)
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
			ingress := &recoveryIngress{
				bodies: map[string]string{
					"ResolvePRBySHA": `{"found":false}`,
					"GetDeployView":  `{"found":true,"repo":"azylman/aerial","pr_number":2327,"status":"open","target_id":"100000000000000003","head_sha":"` + recoveryHead + `"}`,
				},
			}
			cfg := Config{RestateURL: ingress.serve(t), GitHubAPIURL: recoveryGitHub(t, 200, mergedPR)}
			srv := newTestRouter(cfg, nil, &mockOutboundDispatcher{}, &mockEventBus{})
			ctx := context.Background()
			if _, err := srv.processOwnedDeployEvent(ctx, tc.evt); err != nil {
				t.Fatalf("processOwnedDeployEvent() err = %v", err)
			}
			sends := ingress.captured()
			if len(sends) != 2 || sends[1].handler != tc.wantSecond {
				t.Fatalf("sends = %+v, want the recovered merge then the deploy event", sends)
			}
			if sends[0].handler != tc.wantFirst || !strings.Contains(sends[0].key, tc.wantKeyPart) {
				t.Errorf("first send = %s key %q, want %s keyed %q", sends[0].handler, sends[0].key, tc.wantFirst, tc.wantKeyPart)
			}
			var ev prwire.PREvent
			if err := json.Unmarshal([]byte(sends[0].body), &ev); err != nil {
				t.Fatal(err)
			}
			if ev.HeadSHA != recoveryHead || ev.MergeSHA != recoveryMerge {
				t.Errorf("recovered merge = %+v, want head %s merge %s", ev, recoveryHead, recoveryMerge)
			}
		})
	}
}

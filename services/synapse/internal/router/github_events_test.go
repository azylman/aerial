package router

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/pkg/pubsub"
	"github.com/azylman/aerial/services/synapse/internal/model"
	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

const (
	ownedHead   = "1111111111111111111111111111111111111111"
	ownedBefore = "2222222222222222222222222222222222222222"
	ownedMerge  = "3333333333333333333333333333333333333333"
	ownedThread = "111111111111111111"
	ownedSystem = "999999999999999999"
	ownedPRKey  = "/PRObject/azylman%2Faerial%2342/"
	ownedRepoK  = "/RepoObject/azylman%2Faerial/"
	ownedOther  = "/PRObject/azylman%2Fother-repo%2342/"
	ownedOtherR = "/RepoObject/azylman%2Fother-repo/"
)

// ownedIngress is the hermetic Restate ingress: one-way sends answer 202,
// request-response handlers answer the configured status and body, or the
// per-handler body named by the path's last segment.
type ownedIngress struct {
	mu            sync.Mutex
	paths         []string
	bodies        []string
	sendCode      int
	callCode      int
	callBody      string
	handlerBodies map[string]string
	callPaths     []string
	callBods      []string
}

func (in *ownedIngress) serve(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		path := r.URL.EscapedPath()
		in.mu.Lock()
		defer in.mu.Unlock()
		if !strings.HasSuffix(path, "/send") {
			in.callPaths = append(in.callPaths, path)
			in.callBods = append(in.callBods, string(raw))
			body := in.callBody
			if hb, ok := in.handlerBodies[path[strings.LastIndex(path, "/")+1:]]; ok {
				body = hb
			}
			w.WriteHeader(in.callCode)
			_, _ = w.Write([]byte(body))
			return
		}
		in.paths = append(in.paths, path)
		in.bodies = append(in.bodies, string(raw))
		w.WriteHeader(in.sendCode)
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func (in *ownedIngress) sends() ([]string, []string) {
	in.mu.Lock()
	defer in.mu.Unlock()
	return append([]string(nil), in.paths...), append([]string(nil), in.bodies...)
}

func (in *ownedIngress) calls() []string {
	in.mu.Lock()
	defer in.mu.Unlock()
	return append([]string(nil), in.callPaths...)
}

func (in *ownedIngress) callBodies() []string {
	in.mu.Lock()
	defer in.mu.Unlock()
	return append([]string(nil), in.callBods...)
}

// ownedGitHub serves every GitHub REST read with one body.
func ownedGitHub(t *testing.T, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// ownedHarness wires a server whose every effect boundary records.
type ownedHarness struct {
	srv     *RouterServer
	ingress *ownedIngress
	bus     *mockEventBus
	disp    *mockOutboundDispatcher
}

func newOwnedHarness(t *testing.T, cfg Config, ingress *ownedIngress, ghBody string) *ownedHarness {
	t.Helper()
	cfg.RestateURL = ingress.serve(t)
	if cfg.GitHubAPIURL == "" {
		cfg.GitHubAPIURL = ownedGitHub(t, ghBody)
	}
	h := &ownedHarness{srv: NewRouterServer(fixedConfig(cfg), nil), ingress: ingress, bus: &mockEventBus{}, disp: &mockOutboundDispatcher{}}
	h.srv.SetEventBus(h.bus)
	h.srv.SetDispatcher(h.disp)
	return h
}

// effectCount counts lifecycle publishes and outbound dispatches.
func (h *ownedHarness) effectCount() int {
	h.bus.mu.Lock()
	n := 0
	for _, subject := range h.bus.subjects {
		if strings.HasPrefix(subject, pubsub.SubjectAerialPRPrefix) {
			n++
		}
	}
	h.bus.mu.Unlock()
	h.disp.mu.Lock()
	defer h.disp.mu.Unlock()
	return n + len(h.disp.directMessageCalls)
}

// triggerEvents joins the event-trigger publishes as "source:event_type".
func (h *ownedHarness) triggerEvents() string {
	h.bus.mu.Lock()
	defer h.bus.mu.Unlock()
	got := make([]string, 0, len(h.bus.events))
	for _, ev := range h.bus.events {
		got = append(got, ev.Source+":"+ev.EventType)
	}
	return strings.Join(got, ",")
}

// assertEdgeOnly checks the exact Restate sends (each body containing its
// substring) and that synapse performed no effect.
func (h *ownedHarness) assertEdgeOnly(t *testing.T, wantPaths, wantBodies []string) {
	t.Helper()
	paths, bodies := h.ingress.sends()
	if strings.Join(paths, ",") != strings.Join(wantPaths, ",") {
		t.Fatalf("sends = %v, want %v", paths, wantPaths)
	}
	for i, sub := range wantBodies {
		if !strings.Contains(bodies[i], sub) {
			t.Errorf("send %d body = %s, want substring %s", i, bodies[i], sub)
		}
	}
	if got := h.effectCount(); got != 0 {
		t.Errorf("lifecycle effects = %d, want 0", got)
	}
}

func ownedCtx() context.Context {
	return restateclient.WithEventIdentity(context.Background(), "github:d1")
}

func prPayload(repo, action string, merged bool, branch, body, mergeableState string) []byte {
	mergeable := "null"
	if mergeableState == "dirty" {
		mergeable = "false"
	}
	return fmt.Appendf(nil, `{"action":%q,"number":42,"before":%q,"pull_request":{"number":42,"title":"Fix it","state":"open","merged":%t,"merge_commit_sha":%q,"mergeable":%s,"mergeable_state":%q,"body":%q,"user":{"login":"dependabot[bot]"},"head":{"ref":%q,"sha":%q}},"repository":{"full_name":%q},"sender":{"login":"dependabot[bot]"}}`,
		action, ownedBefore, merged, ownedMerge, mergeable, mergeableState, body, branch, ownedHead, repo)
}

func checkRunPayload(name, status, conclusion string, withPR bool) []byte {
	return checkRunPayloadTitled(name, status, conclusion, "boom", withPR)
}

func checkRunPayloadTitled(name, status, conclusion, title string, withPR bool) []byte {
	prs := "[]"
	if withPR {
		prs = fmt.Sprintf(`[{"number":42,"head":{"ref":"feat","sha":%q}}]`, ownedHead)
	}
	return fmt.Appendf(nil, `{"action":"completed","check_run":{"name":%q,"head_sha":%q,"status":%q,"conclusion":%q,"html_url":"https://ci/run","output":{"title":%q},"pull_requests":%s},"repository":{"full_name":"azylman/other-repo"}}`,
		name, ownedHead, status, conclusion, title, prs)
}

func workflowRunPayloadAttempt(branch, conclusion string, attempt int, withPR bool) []byte {
	prs := "[]"
	if withPR {
		prs = fmt.Sprintf(`[{"number":42,"head":{"ref":%q,"sha":%q}}]`, branch, ownedHead)
	}
	return fmt.Appendf(nil, `{"action":"completed","workflow_run":{"id":7,"run_attempt":%d,"name":"CI","head_sha":%q,"head_branch":%q,"status":"completed","conclusion":%q,"html_url":"https://ci/wf","pull_requests":%s},"repository":{"full_name":"azylman/other-repo"}}`,
		attempt, ownedHead, branch, conclusion, prs)
}

func workflowRunPayload(branch, conclusion string, withPR bool) []byte {
	return workflowRunPayloadAttempt(branch, conclusion, 1, withPR)
}

func pushPayload(ref, message string) []byte {
	return fmt.Appendf(nil, `{"ref":%q,"before":%q,"after":%q,"head_commit":{"id":%q,"message":%q},"repository":{"full_name":"azylman/aerial"}}`,
		ref, ownedBefore, ownedMerge, ownedMerge, message)
}

func aerialBody(agent, target string) string {
	return fmt.Sprintf("Body\n<!-- aerial-pr: {\"target_id\":%q,\"bot_id\":%q,\"agent\":%q} -->", target, agent, agent)
}

// TestProcessGitHubEventRestateOwned proves the restate-owned GitHub path is
// a Postgres-free edge: each webhook becomes its PRObject or RepoObject
// sends carrying the rendered facts, with zero registry calls, zero
// lifecycle publishes and zero outbound dispatches.
func TestProcessGitHubEventRestateOwned(t *testing.T) {
	t.Parallel()
	for _, tc := range restateOwnedGitHubCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			code := tc.sendCode
			if code == 0 {
				code = http.StatusAccepted
			}
			ghBody := tc.ghBody
			if ghBody == "" {
				ghBody = "{}"
			}
			h := newOwnedHarness(t, tc.cfg, &ownedIngress{sendCode: code, callCode: http.StatusOK, callBody: "{}"}, ghBody)
			res, err := h.srv.ProcessGitHubEvent(ownedCtx(), tc.event, "d1", tc.body)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ProcessGitHubEvent() err = %v, wantErr %v", err, tc.wantErr)
			}
			if tc.sendCode >= 500 {
				if failed, terminal := restateSendFailure(err); !failed || terminal {
					t.Fatalf("restateSendFailure() = %v/%v, want retryable", failed, terminal)
				}
			}
			h.assertEdgeOnly(t, tc.wantPaths, tc.wantBodies)
			if !tc.wantErr && res.PRNumber != tc.wantPRNumber {
				t.Errorf("PRNumber = %d, want %d", res.PRNumber, tc.wantPRNumber)
			}
			if got := h.triggerEvents(); got != tc.wantBusEvent {
				t.Errorf("trigger events = %q, want %q", got, tc.wantBusEvent)
			}
		})
	}
}

// TestRestateBuildMetadataStore pins the buildmeta store restate-owned
// events use: reads come from PRObject.GetBuildLifecycle, records are
// HandleBuildMetadata sends carrying the optional fact.
func TestRestateBuildMetadataStore(t *testing.T) {
	t.Parallel()
	const lifecycleBody = `{"found":true,"status":"open","head_sha":"h1","merge_sha":"","started":true,"has_build_metadata":true,"build_metadata":{"version":1,"kind":"final","repository":"azylman/aerial","run_id":9,"source_sha":"h1","pr_number":42,"selected":null}}`
	fact := pubsub.PRLifecycleEvent{Event: "check_updated", HeadSHA: "h1"}
	tests := []struct {
		name          string
		noURL         bool
		callCode      int
		callBody      string
		fact          *pubsub.PRLifecycleEvent
		wantLifecycle model.PRBuildLifecycle
		wantMetaRun   int64
		wantMetaFound bool
		wantReadErr   bool
		wantRecordErr bool
		wantSendSub   string
	}{
		{name: "found lifecycle and metadata map through", callCode: 200, callBody: lifecycleBody, fact: &fact, wantLifecycle: model.PRBuildLifecycle{Status: "open", HeadSHA: "h1", Started: true}, wantMetaRun: 9, wantMetaFound: true, wantSendSub: `"facts":[{"event":"check_updated"`},
		{name: "unknown pr reads zero", callCode: 200, callBody: `{"found":false}`, wantSendSub: `"build_metadata":{"version":0`},
		{name: "read failure surfaces", callCode: 500, callBody: "boom", wantReadErr: true, wantSendSub: `"build_metadata":{"version":0`},
		{name: "no restate url fails every call", noURL: true, wantReadErr: true, wantRecordErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ingress := &ownedIngress{sendCode: http.StatusAccepted, callCode: tc.callCode, callBody: tc.callBody}
			cfg := Config{}
			if !tc.noURL {
				cfg.RestateURL = ingress.serve(t)
			}
			srv := NewRouterServer(fixedConfig(cfg), nil)
			store := restateBuildMetadataStore{srv: srv, fact: tc.fact}
			ctx := ownedCtx()
			lc, lcErr := store.LoadPRBuildLifecycle(ctx, "azylman/aerial", 42)
			m, found, mErr := store.LoadPRBuildMetadata(ctx, "azylman/aerial", 42)
			if (lcErr != nil) != tc.wantReadErr || (mErr != nil) != tc.wantReadErr {
				t.Fatalf("read errs = %v / %v, want err %v", lcErr, mErr, tc.wantReadErr)
			}
			if lc != tc.wantLifecycle || found != tc.wantMetaFound || m.RunID != tc.wantMetaRun {
				t.Fatalf("reads = %+v / %v run %d, want %+v / %v run %d", lc, found, m.RunID, tc.wantLifecycle, tc.wantMetaFound, tc.wantMetaRun)
			}
			updated, err := store.RecordPRBuildMetadata(ctx, "azylman/aerial", 42, prwire.BuildMetadata{SourceSHA: "h1"})
			if (err != nil) != tc.wantRecordErr || updated == tc.wantRecordErr {
				t.Fatalf("RecordPRBuildMetadata() = %v, %v, want err %v", updated, err, tc.wantRecordErr)
			}
			if tc.wantSendSub == "" {
				return
			}
			paths, bodies := ingress.sends()
			if len(paths) != 1 || paths[0] != ownedPRKey+"HandleBuildMetadata/send" || !strings.Contains(bodies[0], tc.wantSendSub) {
				t.Fatalf("sends = %v %v, want HandleBuildMetadata with %s", paths, bodies, tc.wantSendSub)
			}
		})
	}
}

// TestRestateOwnedBuildMetadataPaths pins the restate-owned metadata entry
// points that resolve no PR or no producer workflow: they send nothing.
func TestRestateOwnedBuildMetadataPaths(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		event string
		body  []byte
	}{
		{name: "metadata check for a repo without a producer workflow", event: ghEventCheckRun, body: []byte(`{"action":"completed","check_run":{"name":"` + model.BuildMetadataCheckName + `","head_sha":"` + ownedHead + `","pull_requests":[{"number":42}]},"repository":{"full_name":"azylman/other-repo"}}`)},
		{name: "metadata check with no resolvable pr", event: ghEventCheckRun, body: []byte(`{"action":"completed","check_run":{"name":"` + model.BuildMetadataCheckName + `","head_sha":"` + ownedHead + `","pull_requests":[]},"repository":{"full_name":"azylman/aerial"}}`)},
		{name: "metadata check whose pr restate does not know", event: ghEventCheckRun, body: []byte(`{"action":"completed","check_run":{"name":"` + model.BuildMetadataCheckName + `","head_sha":"` + ownedHead + `","pull_requests":[{"number":42}]},"repository":{"full_name":"azylman/aerial"}}`)},
		{name: "producer workflow run of an unknown pr", event: ghEventWorkflowRun, body: []byte(`{"action":"in_progress","workflow_run":{"id":7,"run_attempt":2,"name":"Docker","path":".github/workflows/docker-publish.yml","head_sha":"` + ownedHead + `","head_branch":"feat","status":"in_progress","pull_requests":[{"number":42}]},"repository":{"full_name":"azylman/aerial"}}`)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newOwnedHarness(t, Config{}, &ownedIngress{sendCode: http.StatusAccepted, callCode: http.StatusOK, callBody: `{"found":false}`}, "[]")
			if _, err := h.srv.ProcessGitHubEvent(ownedCtx(), tc.event, "d1", tc.body); err != nil {
				t.Fatalf("ProcessGitHubEvent() err = %v", err)
			}
			if paths, bodies := h.ingress.sends(); len(paths) != 0 || len(bodies) != 0 {
				t.Fatalf("sends = %v %v, want none", paths, bodies)
			}
		})
	}
}

// restateOwnedGitHubCase is one restate-owned webhook and the exact Restate
// sends it must produce.
type restateOwnedGitHubCase struct {
	name       string
	cfg        Config
	event      string
	body       []byte
	ghBody     string
	sendCode   int
	wantErr    bool
	wantPaths  []string
	wantBodies []string
	// wantPRNumber is the resolved PR of a successful event.
	wantPRNumber int
	// wantBusEvent is the exact "source:event_type" trigger list fired on
	// the event bus ("" when no trigger fires).
	wantBusEvent string
}

func restateOwnedGitHubCases() []restateOwnedGitHubCase {
	return append(restateOwnedPullRequestCases(), restateOwnedCheckAndPushCases()...)
}

// restateOwnedPullRequestCases covers pull_request rows.
func restateOwnedPullRequestCases() []restateOwnedGitHubCase {
	return []restateOwnedGitHubCase{
		{
			name: "opened claims the body owner and carries the opened fact", wantPRNumber: 42, event: ghEventPullRequest,
			body:      prPayload("azylman/aerial", ghActionOpened, false, "feat", aerialBody("aerial", ownedThread), ""),
			wantPaths: []string{ownedPRKey + "HandleSelfRegistered/send", ownedPRKey + "HandleOpened/send"},
			wantBodies: []string{
				`"title":"Fix it","gate_generation":0,"reopen_epoch":0,"unmanaged":false,"build_id":"","target_id":"` + ownedThread + `"}`,
				`"pr_number":42,"agent":"aerial","head_sha"`,
			},
		},
		{
			name: "opened fact renders the target and title", wantPRNumber: 42, event: ghEventPullRequest,
			body:      prPayload("azylman/aerial", ghActionOpened, false, "feat", aerialBody("aerial", ownedThread), ""),
			wantPaths: []string{ownedPRKey + "HandleSelfRegistered/send", ownedPRKey + "HandleOpened/send"}, wantBodies: []string{`"target_id":"` + ownedThread + `"`, `"event":"opened","repo":"azylman/aerial","pr_number":42,"branch":"feat","head_sha":"` + ownedHead + `","title":"Fix it","target_id":"` + ownedThread + `","status":"open"`},
		},
		{
			name: "body-free pr with branch target self-registers and carries opened fact", wantPRNumber: 42, event: ghEventPullRequest, cfg: Config{DefaultOwner: true},
			body:      prPayload("azylman/aerial", ghActionOpened, false, "feat/aerial-pr-t111111111111111111-a1b2c3", "Body without trailer", ""),
			wantPaths: []string{ownedPRKey + "HandleSelfRegistered/send", ownedPRKey + "HandleOpened/send"},
			wantBodies: []string{
				`"branch":"feat/aerial-pr-t111111111111111111-a1b2c3","title":"Fix it","gate_generation":0,"reopen_epoch":0,"unmanaged":false,"build_id":"","target_id":"111111111111111111"}`,
				`"facts":[{"event":"opened","repo":"azylman/aerial","pr_number":42,"branch":"feat/aerial-pr-t111111111111111111-a1b2c3","head_sha":"` + ownedHead + `","title":"Fix it","target_id":"111111111111111111"`,
			},
		},
		{
			name: "body-free chunk pr without branch target falls back to system channel", wantPRNumber: 42, event: ghEventPullRequest, cfg: Config{DefaultOwner: true, SystemChannelID: ownedSystem},
			body:      prPayload("azylman/aerial", ghActionOpened, false, "feat/aerial-pr-"+("van"+"adis")+"-c1-a1b2c3", "Body without trailer", ""),
			wantPaths: []string{ownedPRKey + "HandleSelfRegistered/send", ownedPRKey + "HandleOpened/send"},
			wantBodies: []string{
				`"branch":"feat/aerial-pr-` + ("van" + "adis") + `-c1-a1b2c3","title":"Fix it","gate_generation":0,"reopen_epoch":0,"unmanaged":false,"build_id":"","target_id":"` + ownedSystem + `"}`,
				`"facts":[{"event":"opened","repo":"azylman/aerial","pr_number":42,"branch":"feat/aerial-pr-` + ("van" + "adis") + `-c1-a1b2c3","head_sha":"` + ownedHead + `","title":"Fix it","target_id":"` + ownedSystem + `"`,
			},
		},
		{
			name: "default-owned dependabot pr gets the system channel target", wantPRNumber: 42, event: ghEventPullRequest, cfg: Config{DefaultOwner: true, SystemChannelID: ownedSystem},
			body:       prPayload("azylman/aerial", "reopened", false, "dependabot/go/x", "", ""),
			wantPaths:  []string{ownedPRKey + "HandleSelfRegistered/send", ownedPRKey + "HandleReopened/send"},
			wantBodies: []string{`"title":"Fix it","gate_generation":0,"reopen_epoch":0,"unmanaged":false,"build_id":"","target_id":"` + ownedSystem + `"}`, `"title":"Fix it","gate_generation":0,"reopen_epoch":0,"unmanaged":false,"build_id":""}`},
		},
		{
			name: "default-owned dependabot opened registers with the system channel and title", wantPRNumber: 42, event: ghEventPullRequest, cfg: Config{DefaultOwner: true, SystemChannelID: ownedSystem},
			body:      prPayload("azylman/aerial", ghActionOpened, false, "dependabot/go/x", "", ""),
			wantPaths: []string{ownedPRKey + "HandleSelfRegistered/send", ownedPRKey + "HandleOpened/send"},
			wantBodies: []string{
				`"branch":"dependabot/go/x","title":"Fix it","gate_generation":0,"reopen_epoch":0,"unmanaged":false,"build_id":"","target_id":"` + ownedSystem + `"}`,
				`"title":"","gate_generation":0,"reopen_epoch":0,"unmanaged":false,"build_id":"","facts":[{"event":"opened","repo":"azylman/aerial","pr_number":42,"branch":"dependabot/go/x","head_sha":"` + ownedHead + `","title":"Fix it","target_id":"` + ownedSystem + `"`,
			},
		},
		{
			name: "self-registration send failure retries before the opened event", event: ghEventPullRequest, cfg: Config{DefaultOwner: true, SystemChannelID: ownedSystem}, sendCode: http.StatusServiceUnavailable, wantErr: true,
			body:      prPayload("azylman/aerial", ghActionOpened, false, "dependabot/go/x", "", ""),
			wantPaths: []string{ownedPRKey + "HandleSelfRegistered/send"},
		},
		{
			name: "default-owned dependabot merge does not register", wantPRNumber: 42, wantBusEvent: "github:pr_merged", event: ghEventPullRequest, cfg: Config{DefaultOwner: true, SystemChannelID: ownedSystem},
			body:      prPayload("azylman/aerial", ghActionClosed, true, "dependabot/go/x", "", ""),
			wantPaths: []string{ownedPRKey + "HandleMerged/send"}, wantBodies: []string{`"title":"","gate_generation":0`},
		},
		{
			name: "metadata merge registers before the merge", wantPRNumber: 42, wantBusEvent: "github:pr_merged", event: ghEventPullRequest,
			body:      prPayload("azylman/aerial", ghActionClosed, true, "feat", aerialBody("aerial", ownedThread), ""),
			wantPaths: []string{ownedPRKey + "HandleSelfRegistered/send", ownedPRKey + "HandleMerged/send"}, wantBodies: []string{`"target_id":"` + ownedThread + `"`, `"title":"","gate_generation":0`},
		},
		{
			name: "metadata unmerged close neither registers nor fills the title", wantPRNumber: 42, event: ghEventPullRequest,
			body:      prPayload("azylman/aerial", ghActionClosed, false, "feat", aerialBody("aerial", ownedThread), ""),
			wantPaths: []string{ownedPRKey + "HandleClosed/send"}, wantBodies: []string{`"title":"","gate_generation":0`},
		},
		{
			name: "metadata labeled action still registers", wantPRNumber: 42, event: ghEventPullRequest,
			body:      prPayload("azylman/aerial", "labeled", false, "feat", aerialBody("aerial", ownedThread), ""),
			wantPaths: []string{ownedPRKey + "HandleSelfRegistered/send"}, wantBodies: []string{`"target_id":"` + ownedThread + `"`},
		},
		{
			name: "headless metadata without a system channel sends no target", wantPRNumber: 42, event: ghEventPullRequest,
			body:      prPayload("azylman/aerial", "reopened", false, "feat", aerialBody("aerial", "headless:abc"), ""),
			wantPaths: []string{ownedPRKey + "HandleReopened/send"}, wantBodies: []string{`"pr_number":42,"agent":"aerial","head_sha"`},
		},
		{
			name: "synchronize carries the compare-and-set head and fact", wantPRNumber: 42, event: ghEventPullRequest,
			body:      prPayload("azylman/aerial", ghActionSynchronize, false, "feat", "", ""),
			wantPaths: []string{ownedPRKey + "HandleSynchronized/send"}, wantBodies: []string{`"before_sha":"` + ownedBefore + `"`},
		},
		{
			name: "merged close carries the merge sha and merged fact", wantPRNumber: 42, wantBusEvent: "github:pr_merged", event: ghEventPullRequest,
			body:      prPayload("azylman/aerial", ghActionClosed, true, "feat", "", ""),
			wantPaths: []string{ownedPRKey + "HandleMerged/send"}, wantBodies: []string{`"event":"merged","repo":"azylman/aerial","pr_number":42,"branch":"feat","head_sha":"` + ownedHead + `","merge_sha":"` + ownedMerge + `"`},
		},
		{
			name: "unmerged close carries the closed fact", wantPRNumber: 42, event: ghEventPullRequest,
			body:      prPayload("azylman/aerial", ghActionClosed, false, "feat", "", ""),
			wantPaths: []string{ownedPRKey + "HandleClosed/send"}, wantBodies: []string{`"event":"closed_unmerged"`},
		},
		{
			name: "reopened without a head sends nothing", wantPRNumber: 42, event: ghEventPullRequest,
			body: []byte(`{"action":"reopened","number":42,"pull_request":{"number":42,"head":{"ref":"feat","sha":""}},"repository":{"full_name":"azylman/aerial"}}`),
		},
		{
			name: "foreign bot pr sends nothing", wantPRNumber: 42, event: ghEventPullRequest, cfg: Config{Bots: map[string]BotConfig{"aerial": {}}},
			body: prPayload("azylman/aerial", ghActionOpened, false, "feat", aerialBody("other-bot", ownedThread), ""),
		},
		{
			name: "edited action sends nothing", wantPRNumber: 42, event: ghEventPullRequest,
			body: prPayload("azylman/aerial", "edited", false, "feat", "", ""),
		},
		{
			name: "auto merge conflict sends the conflict fact", wantPRNumber: 42, event: ghEventPullRequest,
			body:      prPayload("azylman/aerial", "auto_merge_disabled", false, "feat", "", "dirty"),
			wantPaths: []string{ownedPRKey + "HandleConflict/send"}, wantBodies: []string{`"event":"conflict"`},
		},
		{
			name: "collaborator auto merge conflict carries the rendered dm", wantPRNumber: 42, event: ghEventPullRequest,
			cfg:       Config{CollaboratorCIAlerts: []CollaboratorCIAlertRule{{Repo: "azylman/aerial", ChannelID: "555555555555555555", Authors: map[string]string{"dependabot[bot]": "<@1>"}}}},
			body:      prPayload("azylman/aerial", "auto_merge_disabled", false, "feat", "", "dirty"),
			wantPaths: []string{ownedPRKey + "HandleCollaboratorAlert/send"}, wantBodies: []string{`"kind":"eri_direct_message","channel_id":"555555555555555555"`},
		},
		{
			name: "collaborator failure sends the failure and the rendered dm", wantPRNumber: 42, wantBusEvent: "github:ci_failed", event: ghEventCheckRun,
			cfg:       Config{CollaboratorCIAlerts: []CollaboratorCIAlertRule{{Repo: "azylman/other-repo", ChannelID: "555555555555555555", Authors: map[string]string{"alice": "<@1>"}}}},
			ghBody:    `{"number":42,"state":"open","title":"T","user":{"login":"alice"},"head":{"ref":"feat","sha":"` + ownedHead + `"}}`,
			body:      checkRunPayload("lint", "completed", "failure", true),
			wantPaths: []string{ownedOther + "HandleCheckFailed/send", ownedOtherR + "HandleAutoUpdate/send", ownedOther + "HandleCollaboratorAlert/send"}, wantBodies: []string{`"event":"ci_failed"`, `"auto_update":`, `"unmanaged":true`},
		},
		{
			name: "auto merge without a conflict sends nothing", wantPRNumber: 42, event: ghEventPullRequest,
			body: prPayload("azylman/aerial", "auto_merge_disabled", false, "feat", "", "clean"),
		},
	}
}

// restateOwnedCheckAndPushCases covers check_run, workflow_run, push and
// the generic webhook rows.
func restateOwnedCheckAndPushCases() []restateOwnedGitHubCase {
	return append(restateOwnedCheckCases(), restateOwnedPushCases()...)
}

func restateOwnedCheckCases() []restateOwnedGitHubCase {
	return []restateOwnedGitHubCase{
		{
			name: "check failure on a pr carries the ci_failed fact", wantPRNumber: 42, wantBusEvent: "github:ci_failed", event: ghEventCheckRun,
			body:      checkRunPayload("lint", "completed", "failure", true),
			wantPaths: []string{ownedOther + "HandleCheckFailed/send", ownedOtherR + "HandleAutoUpdate/send"}, wantBodies: []string{`"event":"ci_failed","repo":"azylman/other-repo","pr_number":42,"branch":"feat","head_sha":"` + ownedHead + `"`},
		},
		{
			name: "check success records status, recovery and pass", wantPRNumber: 42, wantBusEvent: "github:ci_passed", event: ghEventCheckRun,
			body:       checkRunPayload("lint", "completed", "success", true),
			wantPaths:  []string{ownedOther + "HandleCheckStatus/send", ownedOther + "HandleCIRecovered/send", ownedOther + "HandleCheckPassed/send"},
			wantBodies: []string{`"check_state":"passed"`, `"event":"synchronize"`, `"check_name":"lint"`},
		},
		{
			name: "recovered check success carries the auto-update policy for PRObject", wantPRNumber: 42, wantBusEvent: "github:ci_passed", event: ghEventCheckRun, cfg: Config{AutoUpdatePRAuthors: []string{"dependabot[bot]"}},
			body:       checkRunPayload("lint", "completed", "success", true),
			wantPaths:  []string{ownedOther + "HandleCheckStatus/send", ownedOther + "HandleCIRecovered/send", ownedOther + "HandleCheckPassed/send"},
			wantBodies: []string{`"check_state":"passed"`, `"auto_update":{"authors":["dependabot[bot]"]}`, `"check_name":"lint"`},
		},
		{
			name: "gate success triggers the auto-update cascade directly", wantPRNumber: 42, wantBusEvent: "github:ci_passed", event: ghEventCheckRun,
			body:       checkRunPayload("CI Gate", "completed", "success", true),
			wantPaths:  []string{ownedOther + "HandleCIRecovered/send", ownedOther + "HandleCheckPassed/send", ownedOtherR + "HandleAutoUpdate/send"},
			wantBodies: []string{`"facts":[{"event":"synchronize"`, `"check_name":"CI Gate"`, `"pr_number":42,"head_sha":"` + ownedHead + `","check_name":"CI Gate"`},
		},
		{
			name: "pr workflow success never triggers the auto-update cascade", wantPRNumber: 42, wantBusEvent: "github:ci_passed", event: ghEventWorkflowRun,
			body:       workflowRunPayload("feat", "success", true),
			wantPaths:  []string{ownedOther + "HandleCIRecovered/send", ownedOther + "HandleCheckPassed/send"},
			wantBodies: []string{`"event":"synchronize"`, `"check_name":"CI"`},
		},
		{
			name: "pending check records only its status", wantPRNumber: 42, event: ghEventCheckRun,
			body:      checkRunPayload("lint", "in_progress", "", true),
			wantPaths: []string{ownedOther + "HandleCheckStatus/send"}, wantBodies: []string{`"event":"check_updated"`},
		},
		{
			name: "silent check failure renders the failure prompt", wantPRNumber: 42, wantBusEvent: "github:ci_failed", event: ghEventCheckRun,
			body:      checkRunPayloadTitled("lint", "completed", "failure", "", true),
			wantPaths: []string{ownedOther + "HandleCheckFailed/send", ownedOtherR + "HandleAutoUpdate/send"}, wantBodies: []string{`"check_url":"https://ci/run","details":"`},
		},
		{
			name: "silent roborev failure renders the review prompt", wantPRNumber: 42, wantBusEvent: "github:ci_failed", event: ghEventCheckRun, ghBody: "[]",
			body:      checkRunPayloadTitled("roborev", "completed", "failure", "", true),
			wantPaths: []string{ownedOther + "HandleCheckFailed/send", ownedOtherR + "HandleAutoUpdate/send"}, wantBodies: []string{`"check_name":"roborev","check_state":"failed"`},
		},
		{
			name: "zero head check sends nothing", wantBusEvent: "github:ci_failed", event: ghEventCheckRun,
			body: []byte(`{"action":"completed","check_run":{"name":"lint","head_sha":"0000000000000000000000000000000000000000","status":"completed","conclusion":"failure","pull_requests":[]},"repository":{"full_name":"azylman/other-repo"}}`),
		},
		{
			name: "sha-only check failure rides the repo sha route", wantBusEvent: "github:ci_failed", event: ghEventCheckRun,
			body:      checkRunPayload("lint", "completed", "failure", false),
			wantPaths: []string{ownedOtherR + "HandleCheckRunBySHA/send"}, wantBodies: []string{`"sha":"` + ownedHead + `","handler":"HandleCheckFailed"`},
		},
		{
			name: "gate aggregator failure sends nothing", wantPRNumber: 42, wantBusEvent: "github:ci_failed", event: ghEventCheckRun,
			body: checkRunPayload("CI Gate", "completed", "failure", true),
		},
		{
			name: "pr workflow failure carries the workflow fact", wantPRNumber: 42, wantBusEvent: "github:ci_failed", event: ghEventWorkflowRun,
			body:      workflowRunPayload("feat", "failure", true),
			wantPaths: []string{ownedOther + "HandleCheckFailed/send", ownedOtherR + "HandleAutoUpdate/send"}, wantBodies: []string{`Workflow \"CI\" concluded with failure`},
		},
		{
			name: "attempt-1 workflow failure with roborev-override triggers workflow rerun", wantPRNumber: 42, wantBusEvent: "github:ci_failed", event: ghEventWorkflowRun,
			ghBody:    `[{"body":"## roborev-override for ` + ownedHead + `"}]`,
			body:      workflowRunPayloadAttempt("feat", "failure", 1, true),
			wantPaths: []string{ownedOtherR + "HandleWorkflowRerun/send"},
			wantBodies: []string{
				`"repo":"azylman/other-repo","main_sha":"","generation":0,"pr_number":42,"head_sha":"` + ownedHead + `","check_name":"CI","run_id":7`,
			},
		},
		{
			name: "attempt-2 workflow failure with roborev-override flows to check failed", wantPRNumber: 42, wantBusEvent: "github:ci_failed", event: ghEventWorkflowRun,
			ghBody:     `[{"body":"## roborev-override for ` + ownedHead + `"}]`,
			body:       workflowRunPayloadAttempt("feat", "failure", 2, true),
			wantPaths:  []string{ownedOther + "HandleCheckFailed/send", ownedOtherR + "HandleAutoUpdate/send"},
			wantBodies: []string{`Workflow \"CI\" concluded with failure`},
		},
		{
			name: "dependabot failure self-registration send failure retries", event: ghEventWorkflowRun, cfg: Config{DefaultOwner: true, SystemChannelID: ownedSystem}, sendCode: http.StatusServiceUnavailable, wantErr: true,
			body:      workflowRunPayload("dependabot/go/x", "failure", true),
			wantPaths: []string{ownedOther + "HandleSelfRegistered/send"},
		},
		{
			name: "default-owned dependabot failure self-registers first", wantPRNumber: 42, wantBusEvent: "github:ci_failed", event: ghEventWorkflowRun, cfg: Config{DefaultOwner: true, SystemChannelID: ownedSystem},
			body:       workflowRunPayload("dependabot/go/x", "failure", true),
			wantPaths:  []string{ownedOther + "HandleSelfRegistered/send", ownedOther + "HandleCheckFailed/send", ownedOtherR + "HandleAutoUpdate/send"},
			wantBodies: []string{`"branch":"dependabot/go/x","title":"","gate_generation":0,"reopen_epoch":0,"unmanaged":false,"build_id":"","target_id":"` + ownedSystem + `"}`, `Workflow \"CI\" concluded with failure`},
		},
		{
			name: "sha-only workflow success rides the repo sha route", wantBusEvent: "github:ci_passed", event: ghEventWorkflowRun,
			body:      workflowRunPayload("feat", "success", false),
			wantPaths: []string{ownedOtherR + "HandleWorkflowRunBySHA/send"}, wantBodies: []string{`"handler":"HandleCheckPassed"`},
		},
		{
			name: "main workflow run on a non-default owner sends nothing", wantBusEvent: "github:ci_passed", event: ghEventWorkflowRun,
			body: workflowRunPayload("main", "success", false),
		},
	}
}

func restateOwnedPushCases() []restateOwnedGitHubCase {
	return []restateOwnedGitHubCase{
		{
			name: "squash push records main and merges the parsed pr", wantPRNumber: 42, event: "push", ghBody: "[]",
			body:       pushPayload("refs/heads/main", "Fix it (#42)"),
			wantPaths:  []string{ownedRepoK + "HandleMainPush/send", ownedPRKey + "HandleMerged/send", ownedRepoK + "HandleAutoUpdate/send"},
			wantBodies: []string{`"main_sha":"` + ownedMerge + `"`, `"event":"merged","repo":"azylman/aerial","pr_number":42,"head_sha":"` + ownedMerge + `","merge_sha":"` + ownedMerge + `"`},
		},
		{
			name: "commits api resolves an unparsed push", wantPRNumber: 42, event: "push", ghBody: `[{"number":42,"state":"closed","head":{"ref":"feat","sha":"` + ownedHead + `"}}]`,
			body:       pushPayload("refs/heads/main", "Merge things"),
			wantPaths:  []string{ownedRepoK + "HandleMainPush/send", ownedPRKey + "HandleMerged/send", ownedRepoK + "HandleAutoUpdate/send"},
			wantBodies: []string{`"main_sha"`, `"branch":"feat"`},
		},
		{
			name: "unresolved push merge rides the repo sha route", event: "push", ghBody: "[]",
			body:       pushPayload("refs/heads/main", "Direct commit"),
			wantPaths:  []string{ownedRepoK + "HandleMainPush/send", ownedRepoK + "HandlePushBySHA/send", ownedRepoK + "HandleAutoUpdate/send"},
			wantBodies: []string{`"main_sha"`, `"handler":"HandleMerged"`},
		},
		{
			name: "default owner direct commit dispatches the git push with no pr", event: "push", ghBody: "[]", cfg: Config{DefaultOwner: true},
			body:       pushPayload("refs/heads/main", "Direct commit"),
			wantPaths:  []string{ownedRepoK + "HandleMainPush/send", ownedRepoK + "HandlePushBySHA/send", ownedRepoK + "HandleAutoUpdate/send", ownedRepoK + "HandleGitPush/send"},
			wantBodies: []string{`"main_sha"`, `"handler":"HandleMerged"`, `"auto_update":{"default_owner":true}`, `"main_sha":"` + ownedMerge + `","generation":0,"pr_number":0,"head_sha":"","ref":"refs/heads/main"`},
		},
		{
			name: "default owner squash push dispatches the git push for the pr", wantPRNumber: 42, event: "push", ghBody: "[]", cfg: Config{DefaultOwner: true},
			body:       pushPayload("refs/heads/main", "Fix it (#42)"),
			wantPaths:  []string{ownedRepoK + "HandleMainPush/send", ownedPRKey + "HandleMerged/send", ownedRepoK + "HandleAutoUpdate/send", ownedRepoK + "HandleGitPush/send"},
			wantBodies: []string{`"main_sha"`, `"pr_number":42`, `"auto_update":{"default_owner":true}`, `"pr_number":42,"head_sha":"","ref":"refs/heads/main"`},
		},
		{name: "branch push sends nothing", event: "push", body: pushPayload("refs/heads/feat", "wip")},
		{name: "tag push sends nothing", event: "push", body: pushPayload("refs/tags/v1", "tag")},
		{name: "deleted branch sends nothing", event: "push", body: []byte(`{"ref":"refs/heads/main","deleted":true,"after":"0000000000000000000000000000000000000000","repository":{"full_name":"azylman/aerial"}}`)},
		{name: "ping sends nothing", event: "ping", body: []byte(`{"zen":"hi","hook_id":1,"repository":{"full_name":"azylman/aerial"}}`)},
		{name: "unhandled event sends nothing", event: "issues", body: []byte(`{}`)},
		{
			name: "ingress failure surfaces as a retryable restate-owned error", event: ghEventPullRequest, sendCode: 503, wantErr: true,
			body:      prPayload("azylman/aerial", ghActionClosed, false, "feat", "", ""),
			wantPaths: []string{ownedPRKey + "HandleClosed/send"}, wantBodies: []string{`"pr_number":42`},
		},
		{
			name: "merge whose send fails fires no trigger", event: ghEventPullRequest, sendCode: 503, wantErr: true,
			body:      prPayload("azylman/aerial", ghActionClosed, true, "feat", "", ""),
			wantPaths: []string{ownedPRKey + "HandleMerged/send"},
		},
		{name: "build-metadata transport check skips pr routing", event: ghEventCheckRun, body: checkRunPayload("build-metadata", "completed", "failure", true)},
		{name: "build-metadata-result transport check skips pr routing", event: ghEventCheckRun, body: checkRunPayload("build-metadata-result", "completed", "cancelled", true)},
		{name: "malformed pull_request fails", event: ghEventPullRequest, body: []byte(`{`), wantErr: true},
		{name: "malformed check_run fails", event: ghEventCheckRun, body: []byte(`{`), wantErr: true},
		{name: "malformed workflow_run fails", event: ghEventWorkflowRun, body: []byte(`{`), wantErr: true},
		{name: "malformed push fails", event: "push", body: []byte(`{`), wantErr: true},
	}
}

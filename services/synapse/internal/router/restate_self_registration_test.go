package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/azylman/aerial/services/synapse/internal/restateclient"
)

const (
	selfRegSystem = "100000000000000009"
	selfRegThread = "100000000000000002"
	selfRegStored = "100000000000000003"
)

// selfRegPayload decodes one pull_request payload for the registration rules.
func selfRegPayload(t *testing.T, action string, merged bool, branch, body, user string) GitHubPullRequestPayload {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"action": action, "number": 42,
		"pull_request": map[string]any{
			"number": 42, "title": "Fix it", "merged": merged, "body": body,
			"user": map[string]any{"login": user},
			"head": map[string]any{"ref": branch, "sha": ownedHead},
		},
		"repository": map[string]any{"full_name": "azylman/aerial"},
		"sender":     map[string]any{"login": user},
	})
	if err != nil {
		t.Fatal(err)
	}
	var p GitHubPullRequestPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

// fillTarget is the fill-only target rule EnsurePRRegistered and
// PRObject.HandleSelfRegistered share: a stored target always wins.
func fillTarget(stored string, reg prSelfRegistration, ok bool) string {
	if stored != "" || !ok {
		return stored
	}
	return reg.TargetID
}

// TestPullRequestSelfRegistration pins legacy's self-registration rule and
// that the restate-owned resolver lands on the same stored target: equal
// registrations without a stored target, and with one both sides keep it.
func TestPullRequestSelfRegistration(t *testing.T) {
	t.Parallel()
	dependabotCfg := Config{DefaultOwner: true, SystemChannelID: selfRegSystem}
	botsCfg := Config{SystemChannelID: selfRegSystem, Bots: map[string]BotConfig{"aerial": {}, "miorine": {}}}
	tests := []struct {
		name         string
		cfg          Config
		action       string
		merged       bool
		branch, body string
		user         string
		stored       string
		wantLegacy   prSelfRegistration
		wantOwned    prSelfRegistration
		wantOK       bool
		wantStored   string
	}{
		{
			name: "default-owner dependabot opened registers with the system channel", cfg: dependabotCfg, action: ghActionOpened, branch: "dependabot/go/x", user: "dependabot[bot]",
			wantLegacy: prSelfRegistration{TargetID: selfRegSystem, Title: "Fix it"}, wantOwned: prSelfRegistration{TargetID: selfRegSystem, Title: "Fix it"}, wantOK: true, wantStored: selfRegSystem,
		},
		{
			name: "stored target wins on both sides", cfg: dependabotCfg, action: ghActionSynchronize, branch: "dependabot/go/x", user: "dependabot[bot]", stored: selfRegStored,
			wantLegacy: prSelfRegistration{TargetID: selfRegStored, Title: "Fix it"}, wantOwned: prSelfRegistration{TargetID: selfRegSystem, Title: "Fix it"}, wantOK: true, wantStored: selfRegStored,
		},
		{
			name: "dependabot reopened registers", cfg: dependabotCfg, action: ghActionReopened, branch: "dependabot/go/x", user: "dependabot[bot]",
			wantLegacy: prSelfRegistration{TargetID: selfRegSystem, Title: "Fix it"}, wantOwned: prSelfRegistration{TargetID: selfRegSystem, Title: "Fix it"}, wantOK: true, wantStored: selfRegSystem,
		},
		{name: "dependabot labeled does not register", cfg: dependabotCfg, action: "labeled", branch: "dependabot/go/x", user: "dependabot[bot]"},
		{name: "dependabot merge does not register", cfg: dependabotCfg, action: ghActionClosed, merged: true, branch: "dependabot/go/x", user: "dependabot[bot]"},
		{name: "dependabot without default owner does not register", cfg: Config{SystemChannelID: selfRegSystem}, action: ghActionOpened, branch: "dependabot/go/x", user: "dependabot[bot]"},
		{name: "dependabot branch by a human does not register", cfg: dependabotCfg, action: ghActionOpened, branch: "dependabot/go/x", user: "octocat"},
		{name: "dependabot without a system channel does not register", cfg: Config{DefaultOwner: true}, action: ghActionOpened, branch: "dependabot/go/x", user: "dependabot[bot]"},
		{
			name: "bot metadata registers under the body target and owner", cfg: botsCfg, action: ghActionOpened, branch: "feat", body: aerialBody("Miorine", selfRegThread), user: "octocat",
			wantLegacy: prSelfRegistration{TargetID: selfRegThread, Title: "Fix it", Agent: "miorine"}, wantOwned: prSelfRegistration{TargetID: selfRegThread, Title: "Fix it", Agent: "miorine"}, wantOK: true, wantStored: selfRegThread,
		},
		{
			name: "headless metadata registers under the headless target", cfg: botsCfg, action: ghActionSynchronize, branch: "feat", body: aerialBody("aerial", "dispatch-uuid-1"), user: "octocat",
			wantLegacy: prSelfRegistration{TargetID: selfRegSystem, Title: "Fix it", Agent: "aerial"}, wantOwned: prSelfRegistration{TargetID: selfRegSystem, Title: "Fix it", Agent: "aerial"}, wantOK: true, wantStored: selfRegSystem,
		},
		{
			name: "headless metadata with a stored target keeps it", cfg: botsCfg, action: "labeled", branch: "feat", body: aerialBody("aerial", "dispatch-uuid-1"), user: "octocat", stored: selfRegStored,
			wantLegacy: prSelfRegistration{TargetID: selfRegStored, Title: "Fix it", Agent: "aerial"}, wantOwned: prSelfRegistration{TargetID: selfRegSystem, Title: "Fix it", Agent: "aerial"}, wantOK: true, wantStored: selfRegStored,
		},
		{name: "headless metadata without a system channel does not register", cfg: Config{Bots: map[string]BotConfig{"aerial": {}}}, action: ghActionOpened, branch: "feat", body: aerialBody("aerial", "dispatch-uuid-1"), user: "octocat"},
		{
			name: "metadata merge registers", cfg: botsCfg, action: ghActionClosed, merged: true, branch: "feat", body: aerialBody("aerial", selfRegThread), user: "octocat",
			wantLegacy: prSelfRegistration{TargetID: selfRegThread, Title: "Fix it", Agent: "aerial"}, wantOwned: prSelfRegistration{TargetID: selfRegThread, Title: "Fix it", Agent: "aerial"}, wantOK: true, wantStored: selfRegThread,
		},
		{name: "metadata unmerged close does not register", cfg: botsCfg, action: ghActionClosed, branch: "feat", body: aerialBody("aerial", selfRegThread), user: "octocat"},
		{name: "foreign bot does not register", cfg: botsCfg, action: ghActionOpened, branch: "feat", body: aerialBody("gundam", selfRegThread), user: "octocat"},
		{name: "invalid body target does not register", cfg: botsCfg, action: ghActionOpened, branch: "feat", body: aerialBody("aerial", "not-a-target"), user: "octocat"},
		{name: "target-only body needs a default owner", cfg: Config{}, action: ghActionOpened, branch: "feat", body: `<!-- aerial-pr: {"target_id":"` + selfRegThread + `"} -->`, user: "octocat"},
		{
			name: "target-only body on a default owner registers without an owner", cfg: Config{DefaultOwner: true}, action: ghActionOpened, branch: "feat", body: `<!-- aerial-pr: {"target_id":"` + selfRegThread + `"} -->`, user: "octocat",
			wantLegacy: prSelfRegistration{TargetID: selfRegThread, Title: "Fix it"}, wantOwned: prSelfRegistration{TargetID: selfRegThread, Title: "Fix it"}, wantOK: true, wantStored: selfRegThread,
		},
		{name: "plain PR does not register", cfg: dependabotCfg, action: ghActionOpened, branch: "feat", body: "Plain PR", user: "octocat"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newTestRouter(tc.cfg, nil, &mockPRRegistry{resolveTargetID: tc.stored}, &mockOutboundDispatcher{})
			p := selfRegPayload(t, tc.action, tc.merged, tc.branch, tc.body, tc.user)
			res := &ResolvedGitHubEvent{}
			populatePullRequestEvent(&p, res)
			legacy, legacyOK := r.pullRequestSelfRegistration(context.Background(), res, p, r.resolveHeadlessPRTargetID)
			owned, ownedOK := r.pullRequestSelfRegistration(context.Background(), res, p, r.ownedHeadlessTarget)
			if legacy != tc.wantLegacy || legacyOK != tc.wantOK {
				t.Errorf("legacy = %+v/%v, want %+v/%v", legacy, legacyOK, tc.wantLegacy, tc.wantOK)
			}
			if owned != tc.wantOwned || ownedOK != tc.wantOK {
				t.Errorf("owned = %+v/%v, want %+v/%v", owned, ownedOK, tc.wantOwned, tc.wantOK)
			}
			if got := fillTarget(tc.stored, legacy, legacyOK); got != tc.wantStored {
				t.Errorf("legacy stored target = %q, want %q", got, tc.wantStored)
			}
			if got := fillTarget(tc.stored, owned, ownedOK); got != tc.wantStored {
				t.Errorf("owned stored target = %q, want %q", got, tc.wantStored)
			}
		})
	}
}

// newStatusIngress is a hermetic fake Restate ingress answering every
// request with code and counting the requests.
func newStatusIngress(code int) (*shadowCapture, *httptest.Server) {
	rec := &shadowCapture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.paths = append(rec.paths, r.URL.EscapedPath())
		rec.bodies = append(rec.bodies, json.RawMessage(`{}`))
		rec.mu.Unlock()
		w.WriteHeader(code)
	}))
	return rec, srv
}

// selfRegSendPaths lists the journaled HandleSelfRegistered paths.
func selfRegSendPaths(shot *shadowCapture) []string {
	shot.mu.Lock()
	defer shot.mu.Unlock()
	var out []string
	for _, p := range shot.paths {
		if strings.Contains(p, "/"+restateclient.HandlerSelfRegistered+"/") {
			out = append(out, p)
		}
	}
	return out
}

// TestProcessPullRequestSelfRegistrationJournal pins the legacy
// pull_request path: every EnsurePRRegistered write is journaled into
// PRObject first, and an authoritative journal failure skips the write.
func TestProcessPullRequestSelfRegistrationJournal(t *testing.T) {
	t.Parallel()
	const selfRegPath = "/PRObject/azylman%2Faerial%2342/HandleSelfRegistered/send"
	tests := []struct {
		name          string
		cfg           Config
		code          int
		action        string
		branch, body  string
		user          string
		wantErr       bool
		wantPaths     []string
		wantEnsure    []string
		wantJournaled string
	}{
		{
			name: "default-owner dependabot opened journals the headless target and title", cfg: Config{DefaultOwner: true, SystemChannelID: selfRegSystem}, code: http.StatusAccepted,
			action: ghActionOpened, branch: "dependabot/go/x", user: "dependabot[bot]", wantPaths: []string{selfRegPath},
			wantEnsure: []string{selfRegSystem + "|Fix it|"}, wantJournaled: `"target_id":"` + selfRegSystem + `"`,
		},
		{
			name: "bot metadata opened journals the metadata target and owner", cfg: Config{SystemChannelID: selfRegSystem, Bots: map[string]BotConfig{"aerial": {}, "miorine": {}}}, code: http.StatusAccepted,
			action: ghActionOpened, branch: "feat", body: aerialBody("miorine", selfRegThread), user: "octocat", wantPaths: []string{selfRegPath},
			wantEnsure: []string{selfRegThread + "|Fix it|miorine"}, wantJournaled: `"agent":"miorine"`,
		},
		{
			name: "headless metadata journals the resolved headless target", cfg: Config{SystemChannelID: selfRegSystem, Bots: map[string]BotConfig{"aerial": {}}}, code: http.StatusAccepted,
			action: ghActionOpened, branch: "feat", body: aerialBody("aerial", "dispatch-uuid-1"), user: "octocat", wantPaths: []string{selfRegPath},
			wantEnsure: []string{selfRegSystem + "|Fix it|aerial"}, wantJournaled: `"target_id":"` + selfRegSystem + `"`,
		},
		{
			name: "foreign bot journals and writes nothing", cfg: Config{SystemChannelID: selfRegSystem, Bots: map[string]BotConfig{"aerial": {}}}, code: http.StatusAccepted,
			action: ghActionOpened, branch: "feat", body: aerialBody("gundam", selfRegThread), user: "octocat",
		},
		{
			name: "plain PR journals and writes nothing", cfg: Config{DefaultOwner: true, SystemChannelID: selfRegSystem}, code: http.StatusAccepted,
			action: ghActionOpened, branch: "feat", body: "Plain PR", user: "octocat",
		},
		{
			name: "authoritative metadata journal failure skips the write and retries", cfg: Config{RestateAuthoritative: true, Bots: map[string]BotConfig{"aerial": {}}}, code: http.StatusInternalServerError,
			action: ghActionOpened, branch: "feat", body: aerialBody("aerial", selfRegThread), user: "octocat", wantErr: true, wantPaths: []string{selfRegPath},
		},
		{
			name: "authoritative dependabot journal failure skips the write", cfg: Config{RestateAuthoritative: true, DefaultOwner: true, SystemChannelID: selfRegSystem}, code: http.StatusInternalServerError,
			action: ghActionOpened, branch: "dependabot/go/x", user: "dependabot[bot]", wantPaths: []string{selfRegPath},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shot, srv := newStatusIngress(tc.code)
			if tc.code == http.StatusAccepted {
				shot, srv = newShadowCapture()
			}
			defer srv.Close()
			cfg := tc.cfg
			cfg.RestateURL = srv.URL
			reg := &mockPRRegistry{}
			r := newTestRouter(cfg, srv.Client(), reg, &mockOutboundDispatcher{}, &mockEventBus{})
			r.SetRestateShadow(newShadowRestateReporter(cfg, srv.Client()))
			body, err := json.Marshal(selfRegPayload(t, tc.action, false, tc.branch, tc.body, tc.user))
			if err != nil {
				t.Fatal(err)
			}
			_, err = r.processGitHubPullRequestEvent(context.Background(), "d1", body, &ResolvedGitHubEvent{})
			if (err != nil) != tc.wantErr {
				t.Fatalf("processGitHubPullRequestEvent err = %v, wantErr %v", err, tc.wantErr)
			}
			if got := selfRegSendPaths(shot); !reflect.DeepEqual(got, tc.wantPaths) {
				t.Errorf("self-registration sends = %v, want %v", got, tc.wantPaths)
			}
			if !reflect.DeepEqual(reg.ensureWrites, tc.wantEnsure) {
				t.Errorf("EnsurePRRegistered writes = %v, want %v", reg.ensureWrites, tc.wantEnsure)
			}
			if tc.wantJournaled != "" && (len(shot.bodies) == 0 || !strings.Contains(string(shot.bodies[0]), tc.wantJournaled)) {
				t.Errorf("journaled bodies = %s, want first with substring %s", shot.bodies, tc.wantJournaled)
			}
		})
	}
}

// TestHealDependabotPRJournal pins the CI-failure self-heal: the
// registration is journaled before the write, and a journal failure skips
// the write.
func TestHealDependabotPRJournal(t *testing.T) {
	t.Parallel()
	const selfRegPath = "/PRObject/azylman%2Faerial%2342/HandleSelfRegistered/send"
	tests := []struct {
		name       string
		cfg        Config
		code       int
		noRestate  bool
		branch     string
		wantPaths  []string
		wantEnsure []string
	}{
		{
			name: "restate off writes without a journal", cfg: Config{DefaultOwner: true, SystemChannelID: selfRegSystem}, code: http.StatusAccepted, noRestate: true, branch: "dependabot/go/x",
			wantEnsure: []string{selfRegSystem + "||"},
		},
		{
			name: "default-owner dependabot failure journals then writes", cfg: Config{DefaultOwner: true, SystemChannelID: selfRegSystem}, code: http.StatusAccepted, branch: "dependabot/go/x",
			wantPaths: []string{selfRegPath}, wantEnsure: []string{selfRegSystem + "||"},
		},
		{name: "non-dependabot branch does nothing", cfg: Config{DefaultOwner: true, SystemChannelID: selfRegSystem}, code: http.StatusAccepted, branch: "feat"},
		{name: "no default owner does nothing", cfg: Config{SystemChannelID: selfRegSystem}, code: http.StatusAccepted, branch: "dependabot/go/x"},
		{
			name: "authoritative journal failure skips the write", cfg: Config{RestateAuthoritative: true, DefaultOwner: true, SystemChannelID: selfRegSystem}, code: http.StatusInternalServerError, branch: "dependabot/go/x",
			wantPaths: []string{selfRegPath},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			shot, srv := newStatusIngress(tc.code)
			defer srv.Close()
			cfg := tc.cfg
			if !tc.noRestate {
				cfg.RestateURL = srv.URL
			}
			reg := &mockPRRegistry{}
			r := newTestRouter(cfg, srv.Client(), reg, &mockOutboundDispatcher{})
			r.SetRestateShadow(newShadowRestateReporter(cfg, srv.Client()))
			r.healDependabotPR(context.Background(), &ResolvedGitHubEvent{Repo: "azylman/aerial", PRNumber: 42, Branch: tc.branch, HeadSHA: ownedHead})
			if got := selfRegSendPaths(shot); !reflect.DeepEqual(got, tc.wantPaths) {
				t.Errorf("self-registration sends = %v, want %v", got, tc.wantPaths)
			}
			if !reflect.DeepEqual(reg.ensureWrites, tc.wantEnsure) {
				t.Errorf("EnsurePRRegistered writes = %v, want %v", reg.ensureWrites, tc.wantEnsure)
			}
		})
	}
}

// dependabotOpenedCase is the synapse side of dependabot-opened.json.
type dependabotOpenedCase struct {
	Name        string `json:"name"`
	PullRequest struct {
		Repo     string `json:"repo"`
		PRNumber int    `json:"pr_number"`
		Action   string `json:"action"`
		Branch   string `json:"branch"`
		HeadSHA  string `json:"head_sha"`
		Title    string `json:"title"`
		User     string `json:"user"`
		Body     string `json:"body"`
	} `json:"pull_request"`
	Legacy struct {
		Ensure []string `json:"ensure"`
	} `json:"legacy"`
	OpenedShadow  []shadowGoldenSend `json:"opened_shadow"`
	OpenedRestate []shadowGoldenSend `json:"opened_restate"`
}

// TestShadowDependabotOpenedGolden pins the opened webhook of
// dependabot-opened.json on both synapse paths: the legacy path journals
// exactly opened_shadow before its EnsurePRRegistered writes, the
// restate-owned path sends exactly opened_restate.
func TestShadowDependabotOpenedGolden(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile(shadowGoldenDir + "dependabot-opened.json")
	if err != nil {
		t.Fatal(err)
	}
	var g struct {
		SystemChannelID string                 `json:"system_channel_id"`
		Cases           []dependabotOpenedCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatal(err)
	}
	if len(g.Cases) == 0 {
		t.Fatal("no cases in dependabot-opened.json")
	}
	tests := g.Cases
	for _, tc := range tests {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			pr := tc.PullRequest
			body, err := json.Marshal(map[string]any{
				"action": pr.Action, "number": pr.PRNumber,
				"pull_request": map[string]any{
					"number": pr.PRNumber, "title": pr.Title, "body": pr.Body,
					"user": map[string]any{"login": pr.User},
					"head": map[string]any{"ref": pr.Branch, "sha": pr.HeadSHA},
				},
				"repository": map[string]any{"full_name": pr.Repo},
				"sender":     map[string]any{"login": pr.User},
			})
			if err != nil {
				t.Fatal(err)
			}
			legacyShot, legacySrv := newShadowCapture()
			defer legacySrv.Close()
			cfg := Config{DefaultOwner: true, SystemChannelID: g.SystemChannelID, RestateURL: legacySrv.URL, BotID: "aerial"}
			reg := &mockPRRegistry{}
			r := newTestRouter(cfg, legacySrv.Client(), reg, &mockOutboundDispatcher{}, &mockEventBus{})
			r.SetRestateShadow(newShadowRestateReporter(cfg, legacySrv.Client()))
			if _, err := r.processGitHubPullRequestEvent(context.Background(), "d1", body, &ResolvedGitHubEvent{}); err != nil {
				t.Fatalf("legacy processGitHubPullRequestEvent err = %v", err)
			}
			if !reflect.DeepEqual(reg.ensureWrites, tc.Legacy.Ensure) {
				t.Errorf("EnsurePRRegistered writes = %v, want %v", reg.ensureWrites, tc.Legacy.Ensure)
			}
			assertShadowWire(t, journaledShadowSends(t, legacyShot), tc.OpenedShadow)

			ownedShot, ownedSrv := newShadowCapture()
			defer ownedSrv.Close()
			ownedCfg := cfg
			ownedCfg.RestateURL = ownedSrv.URL
			owned := newTestRouter(ownedCfg, ownedSrv.Client(), nil, &mockOutboundDispatcher{})
			owned.SetRestateShadow(newShadowRestateReporter(ownedCfg, ownedSrv.Client()))
			if _, err := owned.processOwnedPullRequest(ownedCtx(), "d1", body, &ResolvedGitHubEvent{}); err != nil {
				t.Fatalf("processOwnedPullRequest err = %v", err)
			}
			assertShadowWire(t, journaledShadowSends(t, ownedShot), tc.OpenedRestate)
		})
	}
}

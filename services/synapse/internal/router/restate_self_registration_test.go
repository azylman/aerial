package router

import (
	"encoding/json"
	"testing"
)

const (
	selfRegSystem = "100000000000000009"
	selfRegThread = "100000000000000002"
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
		want         prSelfRegistration
		wantOK       bool
	}{
		{
			name: "default-owner dependabot opened registers with the system channel", cfg: dependabotCfg, action: ghActionOpened, branch: "dependabot/go/x", user: "dependabot[bot]",
			want: prSelfRegistration{TargetID: selfRegSystem, Title: "Fix it"}, wantOK: true,
		},
		{
			name: "synchronize registers with the system channel", cfg: dependabotCfg, action: ghActionSynchronize, branch: "dependabot/go/x", user: "dependabot[bot]",
			want: prSelfRegistration{TargetID: selfRegSystem, Title: "Fix it"}, wantOK: true,
		},
		{
			name: "dependabot reopened registers", cfg: dependabotCfg, action: ghActionReopened, branch: "dependabot/go/x", user: "dependabot[bot]",
			want: prSelfRegistration{TargetID: selfRegSystem, Title: "Fix it"}, wantOK: true,
		},
		{name: "dependabot labeled does not register", cfg: dependabotCfg, action: "labeled", branch: "dependabot/go/x", user: "dependabot[bot]"},
		{name: "dependabot merge does not register", cfg: dependabotCfg, action: ghActionClosed, merged: true, branch: "dependabot/go/x", user: "dependabot[bot]"},
		{name: "dependabot without default owner does not register", cfg: Config{SystemChannelID: selfRegSystem}, action: ghActionOpened, branch: "dependabot/go/x", user: "dependabot[bot]"},
		{name: "dependabot branch by a human does not register", cfg: dependabotCfg, action: ghActionOpened, branch: "dependabot/go/x", user: "octocat"},
		{name: "dependabot without a system channel does not register", cfg: Config{DefaultOwner: true}, action: ghActionOpened, branch: "dependabot/go/x", user: "dependabot[bot]"},
		{
			name: "bot metadata registers under the body target and owner", cfg: botsCfg, action: ghActionOpened, branch: "feat", body: aerialBody("Miorine", selfRegThread), user: "octocat",
			want: prSelfRegistration{TargetID: selfRegThread, Title: "Fix it", Agent: "miorine"}, wantOK: true,
		},
		{
			name: "headless metadata registers under the headless target", cfg: botsCfg, action: ghActionSynchronize, branch: "feat", body: aerialBody("aerial", "dispatch-uuid-1"), user: "octocat",
			want: prSelfRegistration{TargetID: selfRegSystem, Title: "Fix it", Agent: "aerial"}, wantOK: true,
		},
		{
			name: "headless metadata registers on any action", cfg: botsCfg, action: "labeled", branch: "feat", body: aerialBody("aerial", "dispatch-uuid-1"), user: "octocat",
			want: prSelfRegistration{TargetID: selfRegSystem, Title: "Fix it", Agent: "aerial"}, wantOK: true,
		},
		{name: "headless metadata without a system channel does not register", cfg: Config{Bots: map[string]BotConfig{"aerial": {}}}, action: ghActionOpened, branch: "feat", body: aerialBody("aerial", "dispatch-uuid-1"), user: "octocat"},
		{
			name: "metadata merge registers", cfg: botsCfg, action: ghActionClosed, merged: true, branch: "feat", body: aerialBody("aerial", selfRegThread), user: "octocat",
			want: prSelfRegistration{TargetID: selfRegThread, Title: "Fix it", Agent: "aerial"}, wantOK: true,
		},
		{name: "metadata unmerged close does not register", cfg: botsCfg, action: ghActionClosed, branch: "feat", body: aerialBody("aerial", selfRegThread), user: "octocat"},
		{name: "foreign bot does not register", cfg: botsCfg, action: ghActionOpened, branch: "feat", body: aerialBody("gundam", selfRegThread), user: "octocat"},
		{name: "invalid body target does not register", cfg: botsCfg, action: ghActionOpened, branch: "feat", body: aerialBody("aerial", "not-a-target"), user: "octocat"},
		{name: "target-only body needs a default owner", cfg: Config{}, action: ghActionOpened, branch: "feat", body: `<!-- aerial-pr: {"target_id":"` + selfRegThread + `"} -->`, user: "octocat"},
		{
			name: "target-only body on a default owner registers without an owner", cfg: Config{DefaultOwner: true}, action: ghActionOpened, branch: "feat", body: `<!-- aerial-pr: {"target_id":"` + selfRegThread + `"} -->`, user: "octocat",
			want: prSelfRegistration{TargetID: selfRegThread, Title: "Fix it"}, wantOK: true,
		},
		{name: "plain PR does not register", cfg: dependabotCfg, action: ghActionOpened, branch: "feat", body: "Plain PR", user: "octocat"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := newTestRouter(tc.cfg, nil, &mockOutboundDispatcher{})
			p := selfRegPayload(t, tc.action, tc.merged, tc.branch, tc.body, tc.user)
			res := &ResolvedGitHubEvent{}
			populatePullRequestEvent(&p, res)
			got, ok := r.pullRequestSelfRegistration(res, p)
			if got != tc.want || ok != tc.wantOK {
				t.Errorf("pullRequestSelfRegistration() = %+v/%v, want %+v/%v", got, ok, tc.want, tc.wantOK)
			}
		})
	}
}

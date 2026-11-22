package router

import "testing"

// collabAlertRouter is a router with one collaborator CI alert rule for
// acme/mirrormere that mentions @user for author "user".
func collabAlertRouter() *RouterServer {
	return &RouterServer{cfgSrc: fixedConfig(Config{
		GitHubOwner:          "acme",
		CollaboratorCIAlerts: []CollaboratorCIAlertRule{{Repo: "acme/mirrormere", ChannelID: "123", Authors: map[string]string{"user": "@user"}}},
	})}
}

func TestHasCollaboratorCIAlertsForRepo(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		srv  *RouterServer
		repo string
		want bool
	}{
		{"nil router", nil, "mirrormere", false},
		{"nil config source", &RouterServer{}, "mirrormere", false},
		{"configured repo", collabAlertRouter(), "mirrormere", true},
		{"unconfigured repo", collabAlertRouter(), "aerial", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.srv.hasCollaboratorCIAlertsForRepo(tc.repo); got != tc.want {
				t.Errorf("hasCollaboratorCIAlertsForRepo(%q) = %v, want %v", tc.repo, got, tc.want)
			}
		})
	}
}

func TestLookupCollaboratorCIAlert(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		srv         *RouterServer
		author      string
		wantChannel string
		wantMention string
		wantOK      bool
	}{
		{name: "nil router", srv: nil, author: "user"},
		{name: "nil config source", srv: &RouterServer{}, author: "user"},
		{name: "configured author", srv: collabAlertRouter(), author: "user", wantChannel: "123", wantMention: "@user", wantOK: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ch, mention, ok := tc.srv.lookupCollaboratorCIAlert("mirrormere", tc.author)
			if ch != tc.wantChannel || mention != tc.wantMention || ok != tc.wantOK {
				t.Errorf("lookupCollaboratorCIAlert = (%q, %q, %v), want (%q, %q, %v)", ch, mention, ok, tc.wantChannel, tc.wantMention, tc.wantOK)
			}
		})
	}
}

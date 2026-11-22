package router

import (
	"reflect"
	"testing"

	"github.com/azylman/aerial/pkg/prwire"
	"github.com/azylman/aerial/services/synapse/internal/config"
)

func TestAutoUpdatePolicy(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  Config
		repo string
		want *prwire.AutoUpdatePolicy
	}{
		{
			name: "authors, default owner, and the repo's collaborators sorted by author",
			cfg: Config{AutoUpdatePRAuthors: []string{"bot"}, DefaultOwner: true, CollaboratorCIAlerts: []config.CollaboratorCIAlertRule{
				{Repo: "azylman/aerial", ChannelID: "111111111111111111", Authors: map[string]string{"zed": "<@2>", "amy": "<@1>", "blank": " "}},
				{Repo: "azylman/mirrormere", ChannelID: "333333333333333333", Authors: map[string]string{"other": "<@3>"}},
			}},
			repo: "azylman/aerial",
			want: &prwire.AutoUpdatePolicy{Authors: []string{"bot"}, DefaultOwner: true, Collaborators: []prwire.CollaboratorAlert{
				{Author: "amy", ChannelID: "111111111111111111", Mention: "<@1>"},
				{Author: "zed", ChannelID: "111111111111111111", Mention: "<@2>"},
			}},
		},
		{name: "empty config renders an empty policy", repo: "azylman/aerial", want: &prwire.AutoUpdatePolicy{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := NewRouterServer(fixedConfig(tc.cfg), nil)
			if got := srv.autoUpdatePolicy(tc.repo); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("autoUpdatePolicy() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

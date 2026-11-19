package main

import (
	"testing"
)

func TestRouterServerNilConfigGuards(t *testing.T) {
	t.Parallel()
	var nilSrv *RouterServer
	if nilSrv.hasCollaboratorCIAlertsForRepo("mirrormere") {
		t.Fatal("nil router should return false")
	}
	if _, _, ok := nilSrv.lookupCollaboratorCIAlert("mirrormere", "user"); ok {
		t.Fatal("nil router should return false")
	}

	nilCfgSrv := &RouterServer{}
	if nilCfgSrv.hasCollaboratorCIAlertsForRepo("mirrormere") {
		t.Fatal("nil cfgSrc router should return false")
	}
	if _, _, ok := nilCfgSrv.lookupCollaboratorCIAlert("mirrormere", "user"); ok {
		t.Fatal("nil cfgSrc router should return false")
	}

	liveSrv := &RouterServer{
		cfgSrc: fixedConfig(Config{
			CollaboratorCIAlerts: []CollaboratorCIAlertRule{
				{Repo: "azylman/mirrormere", ChannelID: "123", Authors: map[string]string{"user": "@user"}},
			},
		}),
	}
	if !liveSrv.hasCollaboratorCIAlertsForRepo("mirrormere") {
		t.Fatal("expected true for configured repo")
	}
	if ch, mention, ok := liveSrv.lookupCollaboratorCIAlert("mirrormere", "user"); !ok || ch != "123" || mention != "@user" {
		t.Fatalf("unexpected lookup: %q, %q, %v", ch, mention, ok)
	}
}

package githubauth

import (
	"context"
	"testing"
)

func TestSelectingTokenSource(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		choice     string
		nilChoose  bool
		wantValue  string
		wantSource string
	}{
		{name: "app selects the app source", choice: "app", wantValue: "ghs_app", wantSource: SourceApp},
		{name: "pat selects the pat source", choice: "pat", wantValue: "ghp_pat", wantSource: SourcePAT},
		{name: "pat is case and space insensitive", choice: " PAT ", wantValue: "ghp_pat", wantSource: SourcePAT},
		{name: "unset selects app", choice: "", wantValue: "ghs_app", wantSource: SourceApp},
		{name: "invalid selects app", choice: "oauth", wantValue: "ghs_app", wantSource: SourceApp},
		{name: "nil chooser selects app", nilChoose: true, wantValue: "ghs_app", wantSource: SourceApp},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			app := &fakeSource{value: "ghs_app", source: SourceApp}
			pat := &fakeSource{value: "ghp_pat", source: SourcePAT}
			var choose func() string
			if !tc.nilChoose {
				choice := tc.choice
				choose = func() string { return choice }
			}
			src := NewSelectingTokenSource(app, pat, choose)
			value, source, err := src.Token(context.Background())
			if err != nil {
				t.Fatalf("Token error: %v", err)
			}
			if value != tc.wantValue || source != tc.wantSource {
				t.Fatalf("Token = (%q, %q), want (%q, %q)", value, source, tc.wantValue, tc.wantSource)
			}
			src.Invalidate(value)
			if len(app.invalidated) != 1 || app.invalidated[0] != value || len(pat.invalidated) != 1 || pat.invalidated[0] != value {
				t.Fatalf("invalidate not forwarded to both sources: app=%v pat=%v", app.invalidated, pat.invalidated)
			}
		})
	}
}

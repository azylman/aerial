package pubsub

import "testing"

func TestSanitizeSubjectToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		token string
		want  string
	}{
		{name: "empty becomes unknown", token: "", want: "unknown"},
		{name: "blank becomes unknown", token: "   ", want: "unknown"},
		{name: "only symbols becomes unknown", token: "!!!", want: "unknown"},
		{name: "only underscores becomes unknown", token: "__", want: "unknown"},
		{name: "plain token unchanged", token: "aerial", want: "aerial"},
		{name: "hyphen underscore and digits kept", token: "Agent-Alpha_v1", want: "Agent-Alpha_v1"},
		{name: "surrounding space trimmed", token: "  valid_token  ", want: "valid_token"},
		{name: "dot and hash replaced", token: "my.topic#name", want: "my_topic_name"},
		{name: "slash and colon replaced", token: "foo/bar:baz", want: "foo_bar_baz"},
		{name: "inner spaces replaced", token: "Mixed Case 123", want: "Mixed_Case_123"},
		{name: "wildcards replaced", token: "a*b>c", want: "a_b_c"},
		{name: "non-ascii rune replaced once", token: "café", want: "caf_"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := SanitizeSubjectToken(tc.token); got != tc.want {
				t.Errorf("SanitizeSubjectToken(%q) = %q, want %q", tc.token, got, tc.want)
			}
		})
	}
}

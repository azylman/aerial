package transcriptwire

import "testing"

func TestSanitizeText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: ""},
		{name: "clean text unchanged", in: "hello wörld", want: "hello wörld"},
		{name: "nul bytes stripped", in: "a\x00b\x00", want: "ab"},
		{name: "invalid utf8 replaced", in: "a\xffb", want: "a�b"},
		{name: "invalid run collapses to one replacement", in: "a\xff\xfeb", want: "a�b"},
		{name: "nul between invalid bytes merges run", in: "\xff\x00\xfe", want: "�"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := SanitizeText(tc.in); got != tc.want {
				t.Errorf("SanitizeText(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

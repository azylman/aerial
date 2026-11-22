package strutil

import "testing"

func TestFirstNonEmpty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		vals []string
		want string
	}{
		{name: "no values", vals: nil, want: ""},
		{name: "all blank", vals: []string{"", "  ", "\t\n"}, want: ""},
		{name: "first value wins", vals: []string{"alpha", "beta"}, want: "alpha"},
		{name: "skips blank and trims", vals: []string{" ", "  beta  ", "gamma"}, want: "beta"},
		{name: "last value used", vals: []string{"", "", "gamma"}, want: "gamma"},
		{name: "whitespace-only value is skipped not returned", vals: []string{"", "  ", "first", "second"}, want: "first"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := FirstNonEmpty(tc.vals...); got != tc.want {
				t.Errorf("FirstNonEmpty(%q) = %q, want %q", tc.vals, got, tc.want)
			}
		})
	}
}

func TestTruncateRunes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		s        string
		maxRunes int
		want     string
	}{
		{name: "empty", s: "", maxRunes: 3, want: ""},
		{name: "shorter than max", s: "ab", maxRunes: 3, want: "ab"},
		{name: "exactly max", s: "abc", maxRunes: 3, want: "abc"},
		{name: "longer than max", s: "abcdef", maxRunes: 3, want: "abc... (truncated)"},
		{name: "counts runes not bytes", s: "héllo wörld", maxRunes: 5, want: "héllo... (truncated)"},
		{name: "multibyte within max", s: "日本語", maxRunes: 3, want: "日本語"},
		{name: "zero max", s: "a", maxRunes: 0, want: "... (truncated)"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := TruncateRunes(tc.s, tc.maxRunes); got != tc.want {
				t.Errorf("TruncateRunes(%q, %d) = %q, want %q", tc.s, tc.maxRunes, got, tc.want)
			}
		})
	}
}

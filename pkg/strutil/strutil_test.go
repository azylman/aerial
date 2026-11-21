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

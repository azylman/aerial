package github

import (
	"testing"
)

func TestCanonicalFailedCheckKey(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		checkName string
		want      string
	}{
		{name: "roborev standard", checkName: "roborev", want: "roborev"},
		{name: "roborev long name", checkName: "RoboRev Code Review", want: "roborev"},
		{name: "other check uppercase", checkName: "Build-Docker-Image", want: "build-docker-image"},
		{name: "whitespace padded", checkName: "  test / unit  ", want: "test / unit"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := CanonicalFailedCheckKey(tc.checkName)
			if got != tc.want {
				t.Errorf("CanonicalFailedCheckKey(%q) = %q, want %q", tc.checkName, got, tc.want)
			}
		})
	}
}

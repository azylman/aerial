package main

import "testing"

func TestRedactDSN(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		dsn  string
		want string
	}{
		{"query removed", "postgresql://app:hunter2@db:5432/app?sslmode=disable&password=hunter2", "postgresql://app:xxxxx@db:5432/app"},
		{"empty", "", "[redacted]"},
		{"key value dsn", "host=db password=hunter2", "[redacted]"},
		{"whitespace trimmed", "  postgres://app:hunter2@db:5432/app#frag \n", "postgres://app:xxxxx@db:5432/app"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := RedactDSN(tc.dsn); got != tc.want {
				t.Errorf("RedactDSN(%q) = %q, want %q", tc.dsn, got, tc.want)
			}
		})
	}
}

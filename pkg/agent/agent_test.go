package agent

import "testing"

func TestClean(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty", input: "", want: ""},
		{name: "whitespace only", input: "   ", want: ""},
		{name: "mixed case trimmed", input: "  Miorine ", want: "miorine"},
		{name: "unsafe chars kept", input: "Foo Bar", want: "foo bar"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := Clean(tc.input); got != tc.want {
				t.Errorf("Clean(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestNormalize(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty stays empty", input: "", want: ""},
		{name: "whitespace stays empty", input: " \t ", want: ""},
		{name: "default", input: "aerial", want: "aerial"},
		{name: "mixed case lowercases", input: " MiOrine ", want: "miorine"},
		{name: "digits dash underscore", input: "agent_2-b", want: "agent_2-b"},
		{name: "space falls back to default", input: "Foo Bar", want: "aerial"},
		{name: "slash falls back to default", input: "a/b", want: "aerial"},
		{name: "non ascii falls back to default", input: "ägent", want: "aerial"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := Normalize(tc.input); got != tc.want {
				t.Errorf("Normalize(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestScopedServiceName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		base  string
		agent string
		want  string
	}{
		{name: "empty agent", base: "ThreadSession", agent: "", want: "ThreadSession"},
		{name: "default agent", base: "ThreadSession", agent: "aerial", want: "ThreadSession"},
		{name: "default agent mixed case", base: "DispatchSession", agent: " Aerial ", want: "DispatchSession"},
		{name: "named agent", base: "DispatchSession", agent: "Miorine", want: "DispatchSession-miorine"},
		{name: "unsafe agent routes to default", base: "ThreadSession", agent: "bad name", want: "ThreadSession"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ScopedServiceName(tc.base, tc.agent); got != tc.want {
				t.Errorf("ScopedServiceName(%q, %q) = %q, want %q", tc.base, tc.agent, got, tc.want)
			}
		})
	}
}

package discordutil

import "testing"

func TestIsSnowflake(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  bool
	}{
		{name: "empty", input: "", want: false},
		{name: "digits", input: "123456789012345678", want: true},
		{name: "single digit", input: "0", want: true},
		{name: "leading space not trimmed", input: " 123", want: false},
		{name: "trailing newline not trimmed", input: "123\n", want: false},
		{name: "letters", input: "12a3", want: false},
		{name: "negative sign", input: "-123", want: false},
		{name: "non ascii digit", input: "١٢٣", want: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsSnowflake(tc.input); got != tc.want {
				t.Errorf("IsSnowflake(%q) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}

func TestNormalizeThreadID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{name: "empty", input: "", want: ""},
		{name: "bare id", input: "123456789012345678", want: "123456789012345678"},
		{name: "padded id", input: "  123  ", want: "123"},
		{name: "discord prefix", input: "discord-123", want: "123"},
		{name: "channel prefix", input: "discord:123", want: "123"},
		{name: "last colon wins", input: "a:b: discord-456 ", want: "456"},
		{name: "trailing colon", input: "discord:", want: ""},
		{name: "prefix only stripped once", input: "discord-discord-7", want: "discord-7"},
		{name: "non numeric kept", input: "thread-abc", want: "thread-abc"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := NormalizeThreadID(tc.input); got != tc.want {
				t.Errorf("NormalizeThreadID(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestIsHeadlessSessionKey(t *testing.T) {
	t.Parallel()
	type testCase struct {
		name      string
		sessionID string
		expected  bool
	}
	tc := func(name, sessionID string, expected bool) testCase {
		return testCase{name: name, sessionID: sessionID, expected: expected}
	}
	tests := []testCase{
		tc("empty", "", false),
		tc("bare dispatch", "dispatch-123", true),
		tc("scoped dispatch", "pool:dispatch-123", true),
		tc("heartbeat", "heartbeat-abc", true),
		tc("worker prefix", "worker-xyz", true),
		tc("worker infix", "sub-worker-task", true),
		tc("regular snowflake", "123456789012345678", false),
		tc("regular channel", "general", false),
	}
	for _, c := range tests {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := IsHeadlessSessionKey(c.sessionID); got != c.expected {
				t.Errorf("IsHeadlessSessionKey(%q) = %v, want %v", c.sessionID, got, c.expected)
			}
		})
	}
}

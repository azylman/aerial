package model

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

func TestDedupeTrimmedStrings(t *testing.T) {
	t.Parallel()
	in := []string{" a ", "b", "a", "", "  ", "c", "b "}
	want := []string{"a", "b", "c"}
	got := DedupeTrimmedStrings(in)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DedupeTrimmedStrings(%v) = %v, want %v", in, got, want)
	}
}

func TestCleanRepoName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in    string
		clean string
	}{
		{"aerial", "aerial"},
		{" acme/aerial ", "aerial"},
		{"other/repo", "repo"},
		{"", ""},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			if got := CleanRepoName(tc.in); got != tc.clean {
				t.Fatalf("CleanRepoName(%q) = %q, want %q", tc.in, got, tc.clean)
			}
		})
	}
}

func TestDiscordValidationHelpers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		id          string
		norm        string
		isHeadless  bool
		isSnowflake bool
		isChannel   bool
	}{
		{"123456789012345678", "123456789012345678", false, true, true},
		{"discord-123456789012345678", "123456789012345678", false, false, true},
		{"pool:discord-123456789012345678", "123456789012345678", false, false, false},
		{"dispatch-abc-123", "dispatch-abc-123", true, false, true},
		{"heartbeat-daily", "heartbeat-daily", true, false, true},
		{"worker-0", "worker-0", true, false, true},
		{"discord:medium-worker-0", "medium-worker-0", true, false, false},
		{"general", "general", false, false, true},
		{"#general-chat", "#general-chat", false, false, true},
		{"invalid channel name with spaces", "invalid channel name with spaces", false, false, false},
	}
	for _, tc := range tests {
		t.Run(tc.id, func(t *testing.T) {
			t.Parallel()
			if got := NormalizeDiscordTargetID(tc.id); got != tc.norm {
				t.Errorf("NormalizeDiscordTargetID(%q) = %q, want %q", tc.id, got, tc.norm)
			}
			if got := IsHeadlessSessionKey(tc.id); got != tc.isHeadless {
				t.Errorf("IsHeadlessSessionKey(%q) = %v, want %v", tc.id, got, tc.isHeadless)
			}
			if got := IsValidDiscordSnowflake(tc.id); got != tc.isSnowflake {
				t.Errorf("IsValidDiscordSnowflake(%q) = %v, want %v", tc.id, got, tc.isSnowflake)
			}
			if got := IsValidDiscordChannel(tc.id); got != tc.isChannel {
				t.Errorf("IsValidDiscordChannel(%q) = %v, want %v", tc.id, got, tc.isChannel)
			}
		})
	}
}

func TestIs4xxClientError(t *testing.T) {
	t.Parallel()
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("request failed with status 404"), true},
		{errors.New("server error status 500"), false},
		{errors.New("invalid discord snowflake provided"), true},
		{errors.New("generic error"), false},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprint(tc.err), func(t *testing.T) {
			t.Parallel()
			if got := Is4xxClientError(tc.err); got != tc.want {
				t.Fatalf("Is4xxClientError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestRandomNonce(t *testing.T) {
	t.Parallel()
	n1 := RandomNonce()
	n2 := RandomNonce()
	if n1 == "" || n2 == "" || n1 == n2 {
		t.Fatalf("RandomNonce() returned non-unique or empty nonces: n1=%q, n2=%q", n1, n2)
	}
}

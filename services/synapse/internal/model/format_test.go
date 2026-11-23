package model

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestRepoHelpers(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in       string
		clean    string
		prefixPR int
		dmPrefix string
	}{
		{"aerial", "aerial", 42, "**(PR: #42, repo: aerial)** "},
		{" acme/aerial ", "aerial", 0, "**(repo: aerial)** "},
		{"other/repo", "repo", 10, "**(PR: #10, repo: repo)** "},
		{"", "", 5, "**(PR: #5)** "},
		{"", "", 0, ""},
	}
	for _, tc := range tests {
		t.Run(tc.in+"_pr"+strconv.Itoa(tc.prefixPR), func(t *testing.T) {
			t.Parallel()
			if got := CleanRepoName(tc.in); got != tc.clean {
				t.Fatalf("CleanRepoName(%q) = %q, want %q", tc.in, got, tc.clean)
			}
			if got := FormatDirectMessagePrefix(tc.prefixPR, tc.in); got != tc.dmPrefix {
				t.Fatalf("FormatDirectMessagePrefix(%d, %q) = %q, want %q", tc.prefixPR, tc.in, got, tc.dmPrefix)
			}
		})
	}
}

func TestDedupeTrimmedStrings(t *testing.T) {
	t.Parallel()
	in := []string{" a ", "b", "a", "", "  ", "c", "b "}
	want := []string{"a", "b", "c"}
	got := DedupeTrimmedStrings(in)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("DedupeTrimmedStrings(%v) = %v, want %v", in, got, want)
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

func TestTruncatePromptDetails(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want string
	}{
		{name: "short text unchanged", in: "short text", want: "short text"},
		{name: "backtick fences neutralized", in: "code ```block```", want: "code '''block'''"},
		{name: "long text truncated to the rune cap", in: strings.Repeat("x", 2000), want: strings.Repeat("x", 1500) + "... (truncated)"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := TruncatePromptDetails(tc.in); got != tc.want {
				t.Errorf("TruncatePromptDetails() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestFormatErrorDetailsBlock(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name                               string
		header, details, extraLine, footer string
		want                               string
	}{
		{name: "all parts", header: "Header:", details: "details", extraLine: "Extra info", footer: "Footer.", want: "Header:\nError details:\n```\ndetails\n```\nExtra info\nFooter."},
		{name: "header only", header: "Header:", want: "Header:"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := FormatErrorDetailsBlock(tc.header, tc.details, tc.extraLine, tc.footer); got != tc.want {
				t.Errorf("FormatErrorDetailsBlock() = %q, want %q", got, tc.want)
			}
		})
	}
}

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
		norm     string
		clean    string
		prefixPR int
		dmPrefix string
	}{
		{"aerial", "azylman/aerial", "aerial", 42, "**(PR: #42, repo: aerial)** "},
		{"azylman/aerial", "azylman/aerial", "aerial", 0, "**(repo: aerial)** "},
		{"other/repo", "other/repo", "other/repo", 10, "**(PR: #10, repo: other/repo)** "},
		{"", "", "", 5, "**(PR: #5)** "},
		{"", "", "", 0, ""},
	}
	for _, tc := range tests {
		t.Run(tc.in+"_pr"+strconv.Itoa(tc.prefixPR), func(t *testing.T) {
			t.Parallel()
			if got := NormalizeRepo(tc.in); got != tc.norm {
				t.Fatalf("NormalizeRepo(%q) = %q, want %q", tc.in, got, tc.norm)
			}
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

func TestPromptFormattingAndTruncation(t *testing.T) {
	t.Parallel()
	short := "short text"
	if TruncatePromptDetails(short) != short {
		t.Fatalf("TruncatePromptDetails modified short string")
	}

	backtickStr := "code ```block```"
	if !strings.Contains(TruncatePromptDetails(backtickStr), "'''block'''") {
		t.Fatalf("TruncatePromptDetails did not replace backticks: %s", TruncatePromptDetails(backtickStr))
	}

	longStr := strings.Repeat("x", 2000)
	truncated := TruncatePromptDetails(longStr)
	if !strings.HasSuffix(truncated, "... (truncated)") || len([]rune(truncated)) != 1515 {
		t.Fatalf("unexpected truncation result: %s", truncated)
	}

	block := FormatErrorDetailsBlock("Header:", "details", "Extra info", "Footer.")
	if !strings.Contains(block, "Header:") || !strings.Contains(block, "```\ndetails\n```") || !strings.Contains(block, "Extra info") || !strings.Contains(block, "Footer.") {
		t.Fatalf("unexpected FormatErrorDetailsBlock result: %s", block)
	}

	blockMinimal := FormatErrorDetailsBlock("Header:", "", "", "")
	if blockMinimal != "Header:" {
		t.Fatalf("unexpected FormatErrorDetailsBlock minimal: %q", blockMinimal)
	}
}

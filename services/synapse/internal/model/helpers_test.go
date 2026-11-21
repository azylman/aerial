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

func TestIsZeroSHA(t *testing.T) {
	t.Parallel()
	tests := []struct {
		sha  string
		want bool
	}{
		{"", true},
		{"0", true},
		{"0000000000000000000000000000000000000000", true},
		{"0000000000000000000000000000000000000001", false},
		{"abc123", false},
	}
	for _, tc := range tests {
		t.Run(tc.sha, func(t *testing.T) {
			t.Parallel()
			if got := IsZeroSHA(tc.sha); got != tc.want {
				t.Fatalf("IsZeroSHA(%q) = %v, want %v", tc.sha, got, tc.want)
			}
		})
	}
}

func TestFirstNonEmptyAndRandomNonce(t *testing.T) {
	t.Parallel()
	if got := FirstNonEmpty("", "  ", "first", "second"); got != "  " {
		t.Fatalf("FirstNonEmpty got %q, want '  '", got)
	}
	if got := FirstNonEmpty("", "", "value"); got != "value" {
		t.Fatalf("FirstNonEmpty got %q, want 'value'", got)
	}
	if got := FirstNonEmpty(); got != "" {
		t.Fatalf("FirstNonEmpty() got %q, want ''", got)
	}

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
	truncated := TruncatePromptRunes(longStr, 100)
	if !strings.HasSuffix(truncated, "... (truncated)") || len([]rune(truncated)) != 115 {
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

func TestResolveImageTag(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		repo         string
		imageRef     string
		wantTagKey   string
		wantImage    string
		wantWorkload string
	}{
		{
			name:         "empty ref",
			repo:         "aerial",
			imageRef:     "",
			wantTagKey:   "",
			wantImage:    "",
			wantWorkload: "",
		},
		{
			name:         "none ref",
			repo:         "aerial",
			imageRef:     "none",
			wantTagKey:   "",
			wantImage:    "",
			wantWorkload: "",
		},
		{
			name:         "aerial service",
			repo:         "azylman/aerial",
			imageRef:     "engine",
			wantTagKey:   "TAG_AERIAL_ENGINE",
			wantImage:    "aerial-engine",
			wantWorkload: "engine",
		},
		{
			name:         "aerial service already prefixed",
			repo:         "azylman/aerial",
			imageRef:     "ghcr.io/azylman/aerial-engine:v1",
			wantTagKey:   "TAG_AERIAL_ENGINE",
			wantImage:    "aerial-engine",
			wantWorkload: "aerial-engine",
		},
		{
			name:         "aerial-sidecars service",
			repo:         "azylman/aerial-sidecars",
			imageRef:     "camera",
			wantTagKey:   "TAG_AERIAL_SIDECAR_CAMERA",
			wantImage:    "aerial-sidecar-camera",
			wantWorkload: "camera",
		},
		{
			name:         "mirrormere service",
			repo:         "azylman/mirrormere",
			imageRef:     "kiosk",
			wantTagKey:   "TAG_MIRRORMERE_KIOSK",
			wantImage:    "mirrormere-kiosk",
			wantWorkload: "kiosk",
		},
		{
			name:         "mirrormere already prefixed",
			repo:         "azylman/mirrormere",
			imageRef:     "mirrormere-kiosk",
			wantTagKey:   "TAG_MIRRORMERE_KIOSK",
			wantImage:    "mirrormere-kiosk",
			wantWorkload: "mirrormere-kiosk",
		},
		{
			name:         "other repo fallback",
			repo:         "other/service",
			imageRef:     "worker-daemon",
			wantTagKey:   "TAG_WORKER_DAEMON",
			wantImage:    "worker-daemon",
			wantWorkload: "worker-daemon",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			gotTagKey, gotImage, gotWorkload := ResolveImageTag(tc.repo, tc.imageRef)
			if gotTagKey != tc.wantTagKey || gotImage != tc.wantImage || gotWorkload != tc.wantWorkload {
				t.Fatalf("ResolveImageTag(%q, %q) = (%q, %q, %q), want (%q, %q, %q)",
					tc.repo, tc.imageRef, gotTagKey, gotImage, gotWorkload, tc.wantTagKey, tc.wantImage, tc.wantWorkload)
			}
		})
	}
}

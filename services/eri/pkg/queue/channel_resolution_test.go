package queue

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/azylman/aerial/services/eri/pkg/config"
	"github.com/bwmarrin/discordgo"
)

func (p *WorkerPool) SetCreateThreadFunc(fn func(channelID, title string) (string, error)) {
	if p == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.createThreadFn = fn
}

func TestFormatThreadTitle_TableDriven(t *testing.T) {
	t.Parallel()
	testTime := time.Date(2026, time.August, 28, 20, 0, 0, 0, time.UTC)
	tests := []struct {
		name     string
		prefix   string
		prompt   string
		expected string
		maxRunes int
	}{
		{"empty prefix and prompt", "", "", "Scheduled Routine – Aug 28, 2026", 100},
		{"whitespace prefix", "   \t\n  ", "", "Scheduled Routine – Aug 28, 2026", 100},
		{"custom prefix", "Weekly Meal Plan", "", "Weekly Meal Plan – Aug 28, 2026", 100},
		{"empty prefix with prompt", "", "Check system health", "Check system health – Aug 28, 2026", 100},
		{"whitespace prefix with prompt", "   ", "Disk cleanup required", "Disk cleanup required – Aug 28, 2026", 100},
		{"long ASCII prefix exceeding 100 runes", strings.Repeat("A", 120), "", strings.Repeat("A", 97) + "...", 100},
		{"multi-byte unicode prefix exceeding 100 runes", strings.Repeat("🚀", 120), "", strings.Repeat("🚀", 97) + "...", 100},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := FormatThreadTitle(tc.prefix, tc.prompt, testTime)
			runes := []rune(got)
			if len(runes) > tc.maxRunes {
				t.Errorf("expected <= %d runes, got %d", tc.maxRunes, len(runes))
			}
			if tc.expected != "" && !strings.HasSuffix(tc.expected, "...") {
				if got != tc.expected {
					t.Errorf("expected %q, got %q", tc.expected, got)
				}
			} else {
				if !strings.HasSuffix(got, "...") || len(runes) != 100 {
					t.Errorf("expected 100 runes ending in '...', got %d: %q", len(runes), got)
				}
			}
		})
	}
}

func TestShouldCreateThread_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		isAlreadyThread bool
		channelMode     string
		expected        bool
	}{
		{"target is already a thread", true, "threads", false},
		{"channel policy is channel mode", false, "channel", false},
		{"channel policy is CHANNEL (case insensitive)", false, " CHANNEL ", false},
		{"valid thread mode", false, "threads", true},
		{"default/empty policy mode", false, "", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ShouldCreateThread(tc.isAlreadyThread, tc.channelMode); got != tc.expected {
				t.Errorf("expected %v, got %v", tc.expected, got)
			}
		})
	}
}

func TestIsValidThreadKey_TableDriven(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		key      string
		expected bool
	}{
		{"empty key", "", false},
		{"single char", "a", true},
		{"alphanumeric with hyphen and underscore", "valid-key_123", true},
		{"all digits", "1234567890", true},
		{"all uppercase", "SESSION-ABC", true},
		{"128 chars valid", strings.Repeat("a", 128), true},
		{"129 chars invalid", strings.Repeat("a", 129), false},
		{"contains colon", "prefix:key", false},
		{"contains space", "key with space", false},
		{"contains slash", "key/slash", false},
		{"contains dot", "key.dot", false},
		{"contains exclamation", "key!alert", false},
		{"contains emoji", "heartbeat🚀", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := IsValidThreadKey(tc.key); got != tc.expected {
				t.Errorf("IsValidThreadKey(%q) = %v, want %v", tc.key, got, tc.expected)
			}
		})
	}
}

func TestResolveTargetThread_TableDriven(t *testing.T) {
	tests := []struct {
		name            string
		isNilPool       bool
		channelID       string
		title           string
		prompt          string
		cachedChannel   *discordgo.Channel
		policyMode      string
		omitPolicyFn    bool
		hasHook         bool
		hookThreadID    string
		hookErr         error
		withDiscordSess bool
		discordThreadID string
		discordErr      error
		expected        string
	}{
		{
			name:      "nil pool returns channelID",
			isNilPool: true,
			channelID: "11223344",
			expected:  "11223344",
		},
		{
			name:      "empty channelID returns empty",
			channelID: "",
			expected:  "",
		},
		{
			name:      "channel is already a thread",
			channelID: "99887766",
			cachedChannel: &discordgo.Channel{
				ID:   "99887766",
				Type: discordgo.ChannelTypeGuildPublicThread,
				Name: "existing-thread",
			},
			policyMode: "threads",
			expected:   "99887766",
		},
		{
			name:      "policy mode is channel",
			channelID: "55667788",
			cachedChannel: &discordgo.Channel{
				ID:   "55667788",
				Type: discordgo.ChannelTypeGuildText,
				Name: "general",
			},
			policyMode: "channel",
			expected:   "55667788",
		},
		{
			name:      "createThreadFn hook creates thread successfully",
			channelID: "44332211",
			title:     "System Health",
			prompt:    "Check disk and CPU",
			cachedChannel: &discordgo.Channel{
				ID:   "44332211",
				Type: discordgo.ChannelTypeGuildText,
				Name: "alerts",
			},
			policyMode:   "threads",
			hasHook:      true,
			hookThreadID: "77889900",
			expected:     "77889900",
		},
		{
			name:      "createThreadFn hook returns error fallback to channelID",
			channelID: "44332211",
			cachedChannel: &discordgo.Channel{
				ID:   "44332211",
				Type: discordgo.ChannelTypeGuildText,
				Name: "alerts",
			},
			policyMode: "threads",
			hasHook:    true,
			hookErr:    errors.New("discord rate limited"),
			expected:   "44332211",
		},
		{
			name:      "createThreadFn hook returns empty thread ID fallback",
			channelID: "44332211",
			cachedChannel: &discordgo.Channel{
				ID:   "44332211",
				Type: discordgo.ChannelTypeGuildText,
				Name: "alerts",
			},
			policyMode:   "threads",
			hasHook:      true,
			hookThreadID: "",
			expected:     "44332211",
		},
		{
			name:      "default policy resolver and discord session thread start success",
			channelID: "44332211",
			cachedChannel: &discordgo.Channel{
				ID:   "44332211",
				Type: discordgo.ChannelTypeGuildText,
				Name: "alerts",
			},
			omitPolicyFn:    true,
			withDiscordSess: true,
			discordThreadID: "88990011",
			expected:        "88990011",
		},
		{
			name:      "discord session thread start error falls back to channelID",
			channelID: "44332211",
			cachedChannel: &discordgo.Channel{
				ID:   "44332211",
				Type: discordgo.ChannelTypeGuildText,
				Name: "alerts",
			},
			policyMode:      "threads",
			withDiscordSess: true,
			discordErr:      errors.New("thread start failed"),
			expected:        "44332211",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.cachedChannel != nil {
				CacheDiscordChannel(tc.cachedChannel)
			}
			var pool *WorkerPool
			if !tc.isNilPool {
				pool = &WorkerPool{}
				if !tc.omitPolicyFn {
					pool.cfg.ResolveChannelPolicy = func(channelID, channelName string) config.ChannelPolicy {
						return config.ChannelPolicy{Mode: tc.policyMode}
					}
				}
				if tc.withDiscordSess {
					dg, _ := discordgo.New("Bot test-token")
					tid, derr := tc.discordThreadID, tc.discordErr
					dg.Client.Transport = roundTripperFunc(func(req *http.Request) (*http.Response, error) {
						if derr != nil {
							return nil, derr
						}
						b, _ := json.Marshal(&discordgo.Channel{ID: tid, Type: discordgo.ChannelTypeGuildPublicThread, Name: "auto-thread"})
						return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(b))}, nil
					})
					pool.cfg.DiscordSession = dg
				}
				if tc.hasHook {
					pool.SetCreateThreadFunc(func(cID, title string) (string, error) {
						return tc.hookThreadID, tc.hookErr
					})
				}
			}

			got := pool.ResolveTargetThread(tc.channelID, tc.title, tc.prompt, time.Now())
			if got != tc.expected {
				t.Errorf("ResolveTargetThread() = %q, want %q", got, tc.expected)
			}
		})
	}
}

package widget

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/azylman/aerial/services/vanadis-restate/internal/ericlient"
)

func TestDelivererChannelRedirection(t *testing.T) {
	t.Parallel()
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	configContent := `
system_channel: "aerial-dev"
channels:
  default:
    no_build_updates: false
  lounge:
    no_build_updates: true
  general:
    no_build_updates: false
  "1564150557705248880":
    no_build_updates: true
`
	require.NoError(t, os.WriteFile(configPath, []byte(configContent), 0o600))

	badSyntaxPath := filepath.Join(tmpDir, "bad.yaml")
	require.NoError(t, os.WriteFile(badSyntaxPath, []byte(":: bad yaml ::"), 0o600))

	noSysChanPath := filepath.Join(tmpDir, "no_sys.yaml")
	require.NoError(t, os.WriteFile(noSysChanPath, []byte("channels:\n  lounge:\n    no_build_updates: true\n"), 0o600))

	nonexistentPath := filepath.Join(tmpDir, "missing.yaml")

	tests := []struct {
		name          string
		cfgPath       string
		channelID     string
		resolverSysID string
		resolverErr   error
		expectTarget  string
	}{
		{
			name:         "regular channel without no_build_updates stays on target",
			cfgPath:      configPath,
			channelID:    "general",
			expectTarget: "general",
		},
		{
			name:          "named channel with no_build_updates redirects to resolved system channel",
			cfgPath:       configPath,
			channelID:     "lounge",
			resolverSysID: "1564150557705248879",
			expectTarget:  "1564150557705248879",
		},
		{
			name:          "snowflake channel with no_build_updates redirects to resolved system channel",
			cfgPath:       configPath,
			channelID:     "1564150557705248880",
			resolverSysID: "1564150557705248879",
			expectTarget:  "1564150557705248879",
		},
		{
			name:         "unresolvable system channel falls back to target channel",
			cfgPath:      configPath,
			channelID:    "lounge",
			resolverErr:  errors.New("channel not found"),
			expectTarget: "lounge",
		},
		{
			name:          "resolver returning non-snowflake name falls back to target channel",
			cfgPath:       configPath,
			channelID:     "lounge",
			resolverSysID: "aerial-dev",
			expectTarget:  "lounge",
		},
		{
			name:         "missing config file does not redirect",
			cfgPath:      nonexistentPath,
			channelID:    "lounge",
			expectTarget: "lounge",
		},
		{
			name:         "bad syntax config file does not redirect",
			cfgPath:      badSyntaxPath,
			channelID:    "lounge",
			expectTarget: "lounge",
		},
		{
			name:         "config file without system channel does not redirect",
			cfgPath:      noSysChanPath,
			channelID:    "lounge",
			expectTarget: "lounge",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockSender := &mockWidgetSender{sendMsg: &discordgo.Message{ID: "msg-123"}}
			d, err := NewDiscordWidgetDeliverer("", "", nil, tc.cfgPath)
			require.NoError(t, err)
			d.SetAgentSender("aerial", mockSender)
			if tc.resolverSysID != "" || tc.resolverErr != nil {
				d.channelResolver = func(_ string) (string, error) {
					return tc.resolverSysID, tc.resolverErr
				}
			}

			req := ericlient.StatusWidgetRequest{
				ChannelID: tc.channelID,
				Title:     "Build Title",
				Status:    "running",
			}
			msgID, dErr := d.DeliverStatusWidget(context.Background(), req)
			require.NoError(t, dErr)
			assert.Equal(t, "msg-123", msgID)
			assert.Equal(t, tc.expectTarget, mockSender.lastSendChannelID)
		})
	}
}

func TestDelivererChannelRedirectionCustomPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		channelID     string
		noBuild       bool
		sysChan       string
		expectChannel string
	}{
		{
			name:          "custom resolver no_build_updates false",
			channelID:     "chan-normal",
			noBuild:       false,
			sysChan:       "sys-123",
			expectChannel: "chan-normal",
		},
		{
			name:          "custom resolver no_build_updates true with snowflake",
			channelID:     "chan-quiet",
			noBuild:       true,
			sysChan:       "1564150557705248879",
			expectChannel: "1564150557705248879",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockSender := &mockWidgetSender{sendMsg: &discordgo.Message{ID: "m-1"}}
			d, err := NewDiscordWidgetDeliverer("", "", nil, filepath.Join(t.TempDir(), "config.yaml"))
			require.NoError(t, err)
			d.SetAgentSender("aerial", mockSender)
			d.channelPolicyResolver = func(_ string) (bool, string) {
				return tc.noBuild, tc.sysChan
			}

			req := ericlient.StatusWidgetRequest{ChannelID: tc.channelID, Title: "Build"}
			_, dErr := d.DeliverStatusWidget(context.Background(), req)
			require.NoError(t, dErr)
			assert.Equal(t, tc.expectChannel, mockSender.lastSendChannelID)
		})
	}
}

func TestChannelPolicyAndResolutionCaching(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")

	initialYAML := `
system_channel: "sys-chan-target"
channels:
  chan-test:
    no_build_updates: true
`
	require.NoError(t, os.WriteFile(cfgPath, []byte(initialYAML), 0o644))

	resolveCount := 0
	d, err := NewDiscordWidgetDeliverer("", "", nil, cfgPath)
	require.NoError(t, err)
	d.channelResolver = func(target string) (string, error) {
		resolveCount++
		if target == "sys-chan-target" {
			return "999888777666555444", nil
		}
		return "", errors.New("not found")
	}

	t0 := time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)

	// First call: reads file and resolves target channel
	res1 := d.redirectChannelIfNoBuildUpdates("chan-test", t0)
	assert.Equal(t, "999888777666555444", res1)
	assert.Equal(t, 1, resolveCount)

	// Delete config file on disk to prove cache is used
	require.NoError(t, os.Remove(cfgPath))

	// Second call at t0 + 10s (before 1m TTL): does not hit disk or resolver
	t1 := t0.Add(10 * time.Second)
	res2 := d.redirectChannelIfNoBuildUpdates("chan-test", t1)
	assert.Equal(t, "999888777666555444", res2)
	assert.Equal(t, 1, resolveCount)

	// Repeated uncached thread policy lookup is cached in channelMetaCache
	resUnmatched1 := d.redirectChannelIfNoBuildUpdates("uncached-thread-999", t1)
	assert.Equal(t, "uncached-thread-999", resUnmatched1)
	_, metaCached := d.getCachedChannelMeta("uncached-thread-999", t1)
	assert.True(t, metaCached)
	resUnmatched2 := d.redirectChannelIfNoBuildUpdates("uncached-thread-999", t1)
	assert.Equal(t, "uncached-thread-999", resUnmatched2)

	// Direct resolveChannelID caching check
	r1 := d.resolveChannelID("sys-chan-target", "fallback", t1)
	assert.Equal(t, "999888777666555444", r1)
	assert.Equal(t, 1, resolveCount)

	// After resolution cache expires at t0 + 6m: resolver called again
	t2 := t0.Add(6 * time.Minute)
	r2 := d.resolveChannelID("sys-chan-target", "fallback", t2)
	assert.Equal(t, "999888777666555444", r2)
	assert.Equal(t, 2, resolveCount)
}

func TestSessionStateChannelResolution(t *testing.T) {
	t.Parallel()
	sess, err := discordgo.New("Bot fake-token")
	require.NoError(t, err)

	guild := &discordgo.Guild{
		ID: "g-1",
		Channels: []*discordgo.Channel{
			{ID: "c-aerial-dev", Name: "aerial-dev"},
			{ID: "c-lounge", Name: "lounge"},
			{ID: "c-thread-1", Name: "thread-1", Type: discordgo.ChannelTypeGuildPublicThread, ParentID: "c-lounge"},
		},
	}
	sess.State = discordgo.NewState()
	require.NoError(t, sess.State.GuildAdd(guild))
	for _, ch := range guild.Channels {
		require.NoError(t, sess.State.ChannelAdd(ch))
	}

	mockSender := &mockWidgetSender{sendMsg: &discordgo.Message{ID: "m-res"}}
	d := &DiscordWidgetDeliverer{
		session:      sess,
		agentSenders: map[string]DiscordWidgetSender{"aerial": mockSender},
	}

	tests := []struct {
		name          string
		channels      map[string]channelConfigData
		channelID     string
		targetSysChan string
		expectChannel string
	}{
		{
			name: "resolves thread via parent in state",
			channels: map[string]channelConfigData{
				"lounge": {NoBuildUpdates: true},
			},
			channelID:     "c-thread-1",
			targetSysChan: "aerial-dev",
			expectChannel: "c-aerial-dev",
		},
		{
			name: "resolves channel directly in state",
			channels: map[string]channelConfigData{
				"lounge": {NoBuildUpdates: true},
			},
			channelID:     "c-lounge",
			targetSysChan: "aerial-dev",
			expectChannel: "c-aerial-dev",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			noBuild := d.checkNoBuildUpdates(tc.channelID, tc.channels)
			assert.True(t, noBuild)
			resolved := d.resolveChannelID(tc.targetSysChan, tc.channelID)
			assert.Equal(t, tc.expectChannel, resolved)
		})
	}
}

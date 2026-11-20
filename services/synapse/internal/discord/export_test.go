package discord

import (
	"github.com/bwmarrin/discordgo"
)

// SetGatewayOpenerForTest overrides the session opener function for testing and returns a cleanup func.
func SetGatewayOpenerForTest(fn func(*discordgo.Session) error) func() {
	gatewayOpenerMu.Lock()
	orig := gatewayOpener
	gatewayOpener = fn
	gatewayOpenerMu.Unlock()
	return func() {
		gatewayOpenerMu.Lock()
		gatewayOpener = orig
		gatewayOpenerMu.Unlock()
	}
}

// ResetWatermarksForTest clears all tracked channel watermarks.
func ResetWatermarksForTest() {
	channelWatermarksMu.Lock()
	defer channelWatermarksMu.Unlock()
	channelWatermarks = make(map[string]string)
}

// SetSweepRunningForTest forces the sweep running state for testing concurrent sweep prevention.
func SetSweepRunningForTest(running bool) func() {
	if running {
		agentSweepsRunning.Store("default", true)
		return func() {
			agentSweepsRunning.Delete("default")
		}
	}
	agentSweepsRunning.Delete("default")
	return func() {}
}

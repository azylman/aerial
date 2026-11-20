package app

// Deadcode anchors for app options and helper functions.
//
// WithDiscordSession and WithEventBus provide programmatic entry points for
// embedding eri-restate with injected Discord sessions and pubsub event buses.
// defaultReloadLogger provides a default fallback logger for config reload triggers.
// To prevent deadcode analysis from reporting them as unreachable in this service,
// they are anchored here.
func keepLive(_ ...any) {}

func init() {
	keepLive(
		WithDiscordSession,
		WithEventBus,
		defaultReloadLogger,
	)
}
